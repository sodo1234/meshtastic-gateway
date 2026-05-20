#!/usr/bin/env python3
"""
LoRa Zigbee Supervisor - v38 (Calendar + Anomaly Dashboard + Anti-Collision)

Schedule Sync: via CalendarTransfer (CRC + retry + zlib compression)
  SUP→GW: cal_transfer.start_send(gw, "cal", compact) → sch_b/sch_c/sch_e/sch_a
  GW→SUP: cal_transfer.start_send(id, "gw_push", compact) → same protocol
  Compact: [[start_min, dur_min, mode],...] — 14 slots = 104B = 1 chunk = 4 packets
  Mode: event name → CONFIG mode_names (auto-derived) → number (PRODUKCJA→1, PRZERWA→2, SERWIS→3)

Other v5.1 features:
- Multi-Interface Routing: send_text_to(gw) via routed antenna
- Rolling window 14 days: only future events synced
- GLOBAL + GW merge: GW-specific overrides GLOBAL
- Dedup on add_slot: same start+end → update, not duplicate
- No LoRa flood: mode transition notifications only via _schedule_loop
- Anomaly batch, size guard, disc_vio split
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import json, time, threading, os, zlib, base64, hashlib, random, string, traceback, glob
import urllib.request
from datetime import datetime
from collections import deque
from enum import IntEnum
import paho.mqtt.client as mqtt
import meshtastic, meshtastic.serial_interface
from pubsub import pub

SCHEDULE_FILE = os.environ.get('LORA_SCHEDULE_FILE',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schedules.json'))
PARAMS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'supervisor_params.json')

CONFIG = {
    "id": "G0",
    "mesh_ports": [
        {"port": "/dev/ttyUSB0", "enabled": True,  "label": "ANT-1", "gateways": ["G1"]},
        #{"port": "/dev/ttyUSB1", "enabled": False, "label": "ANT-2", "gateways": ["G2"]},
        #{"port": "/dev/ttyACM0", "enabled": False, "label": "ANT-3", "gateways": ["G3"]},
    ],
    "mesh_reconnect": {"enabled": True, "interval": 15, "max_backoff": 120},
    "mqtt": {"host": "localhost", "port": 1883, "user": "mqtt", "pass": "REPLACE_ME"},
    "ha_prefix": "homeassistant",
    "state_prefix": "lora",
    "gateways": ["G1", "G2", "G3"],
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},
    "heartbeat_interval": 1200,
    "z2m_stale_threshold": 900,
    "production_schedule": {"enabled": True, "pre_notify_seconds": [30, 5]},
    "anomaly": {
        "auto_clear": {
            "device_offline": True, "low_battery": True, "critical_battery": True,
            "temp_high": True, "temp_low": True, "hum_high": True, "hum_low": True,
            "stagnation": True
        }
    },
    "calendar": {
        "chunk_size": 140, "transfer_timeout": 60, "retry_max": 2, "chunk_delay": 6.0,
        "enabled_gateways": ["G1", "G2", "G3"],
    },
    "mode_names": {
        0: "BRAK PRODUKCJI",
        1: "PRODUKCJA",
        2: "PRZERWA",
        3: "SERWIS",
    },
    "ha_api": {
        "url": "http://localhost:8123",
        "token": "",
    },
    "gateway_override": [],
    "log": {"file": "/tmp/supervisor.log", "max_bytes": 5_000_000, "backup_count": 3},
    "sync": {
        "interval": 1200,
        "sun_entity": "sun.sun",
        "drift_threshold": 120,
    },
    # v34: Params — UI hints for supervisor dashboard number entities.
    # Actual values come from gateways (bidirectional sync).
    "params": {
        "p1": {"name": "P1", "icon": "mdi:numeric-1-circle", "min": 0, "max": 1000, "step": 1, "unit": "", "default": 5},
        "p2": {"name": "P2", "icon": "mdi:numeric-2-circle", "min": 0, "max": 1000, "step": 1, "unit": "", "default": 10},
        "p3": {"name": "P3", "icon": "mdi:numeric-3-circle", "min": 0, "max": 1000, "step": 1, "unit": "", "default": 15},
    },
}


# =====================================================
# v25: FLOW PRIORITY — 3-level (matches Gateway)
# =====================================================
class FlowPriority(IntEnum):
    SYSTEM    = 0   # P0: system protocol (pong, disc, sch, cfg, ping)
    PRIORITY  = 1   # P1: priority device data
    MONITORED = 2   # P2: regular monitored device data

Priority = FlowPriority

PRIORITY_LABELS = {
    FlowPriority.SYSTEM:    "🔧 [P0:SYS]",
    FlowPriority.PRIORITY:  "🚨 [P1:PRIO]",
    FlowPriority.MONITORED: "📦 [P2:MON]",
}


class Logger:
    ICONS = {'DEBUG': '🔍', 'INFO': '✅', 'WARN': '⚠️ ', 'ERROR': '❌'}
    COLORS = {'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARN': '\033[33m', 'ERROR': '\033[31m'}
    RESET = '\033[0m'
    COMP = {'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'HA': '🏠', 'PING': '🏓',
            'DISC': '🔭', 'CMD': '⚡', 'STATUS': '📤', 'CFG': '⚙️', 'ANOMALY': '🚨',
            'SCHED': '📅', 'CAL': '📆', 'SYNC': '🔄', 'ANT': '📡', 'VBTN': '🔘',
            'BATCH': '📦'}
    def __init__(self):
        from logging.handlers import RotatingFileHandler
        log_cfg = CONFIG.get('log', {})
        self._handler = RotatingFileHandler(
            log_cfg.get('file', '/tmp/supervisor.log'),
            maxBytes=log_cfg.get('max_bytes', 5_000_000),
            backupCount=log_cfg.get('backup_count', 3))
        self._handler.setFormatter(logging.Formatter('%(message)s'))
        self._flog = logging.getLogger('sup_file')
        self._flog.addHandler(self._handler)
        self._flog.setLevel(logging.DEBUG)
    def _log(self, lvl, comp, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"{ts} {self.ICONS.get(lvl,'')} {self.COLORS.get(lvl,'')}[{comp}]{self.RESET} {self.COMP.get(comp,'•')} {msg}")
        self._flog.info(f"{ts} [{lvl}] [{comp}] {msg}")
    def debug(self, c, m): self._log('DEBUG', c, m)
    def info(self, c, m): self._log('INFO', c, m)
    def warn(self, c, m): self._log('WARN', c, m)
    def error(self, c, m): self._log('ERROR', c, m)
    def close(self):
        try: self._handler.close()
        except: pass

log = Logger()
now_str = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')
now_ts = lambda: time.time()


# =====================================================
# CALENDAR: Schedule Manager with per-gateway storage (unchanged)
# =====================================================
class ScheduleManager:
    BASE_EPOCH = 1767225600  # 2026-01-01 00:00 UTC — compact encoding base

    def __init__(self, filepath=SCHEDULE_FILE):
        self.filepath = filepath
        self.lock = threading.Lock()
        self.data = {"GLOBAL": [], **{gw: [] for gw in CONFIG['gateways']}}
        self._load()

    def _load(self):
        try:
            with open(self.filepath, 'r') as f:
                raw = json.load(f)
            for k in self.data:
                if k in raw: self.data[k] = raw[k]
            log.info('CAL', f"📂 Loaded: " + ", ".join(f"{k}={len(v)}" for k, v in self.data.items() if v))
        except FileNotFoundError:
            log.info('CAL', "📂 No schedule file — clean start")
        except Exception as e:
            log.warn('CAL', f"📂 Load error: {e}")

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.filepath) or '.', exist_ok=True)
            with open(self.filepath, 'w') as f:
                json.dump({**self.data, "_saved": now_str()}, f, indent=2)
        except Exception as e:
            log.error('CAL', f"💾 Save error: {e}")

    def _gen_id(self):
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

    @staticmethod
    def _parse_ts(val):
        """Parse ISO string or epoch to epoch float."""
        if isinstance(val, str):
            return datetime.fromisoformat(val).timestamp()
        return float(val)

    def add_slot(self, target, start, end, mode=None, note=""):
        """Add slot with auto mode detection from event name.
        If mode not specified, detect from note using mode_names (auto-inverted).
        Dedup: skip if same start+end already exists (update mode if different)."""
        # Auto-detect mode from event name
        if mode is None:
            mode = self._detect_mode(note)
        else:
            mode = int(mode)
        mode_name = CONFIG.get('mode_names', {}).get(mode, note[:30] if note else "")
        try:
            new_st = self._parse_ts(start)
            new_et = self._parse_ts(end)
        except: return None
        with self.lock:
            if target not in self.data: self.data[target] = []
            # Dedup: same start+end → update mode if different, skip if same
            for existing in self.data[target]:
                try:
                    ex_st = self._parse_ts(existing['start'])
                    ex_et = self._parse_ts(existing['end'])
                    if abs(ex_st - new_st) < 60 and abs(ex_et - new_et) < 60:
                        if existing.get('mode') != mode:
                            existing['mode'] = mode
                            existing['note'] = mode_name
                            existing['updated_ts'] = int(now_ts())
                            self._save()
                            log.info('CAL', f"🔄 [{target}] Updated mode={mode} ({mode_name})")
                        return existing['id']
                except: continue
            slot = {"id": self._gen_id(), "start": start, "end": end, "mode": mode,
                    "note": mode_name, "updated_ts": int(now_ts()), "origin": "supervisor"}
            self.data[target].append(slot); self._save()
        log.info('CAL', f"➕ [{target}] {start}→{end} mode={mode} ({mode_name})")
        return slot['id']

    def _detect_mode(self, text):
        """Detect mode number from event name/summary using CONFIG keywords."""
        if not text: return 1  # Default: PRODUKCJA
        text_upper = text.upper()
        for ch in ['🟢', '⚪', '🔧', '⏸']: text_upper = text_upper.replace(ch, '').strip()
        # v38: Include mode 0 (BRAK PRODUKCJI) — was skipped with `if num > 0`
        schedule_modes = {name.upper(): num for num, name in CONFIG.get('mode_names', {}).items()}
        # Sort by keyword length desc — longer matches first ("BRAK PRODUKCJI" before "PRODUKCJA")
        for keyword, mode_num in sorted(schedule_modes.items(), key=lambda x: -len(x[0])):
            if keyword in text_upper:
                return mode_num
        return 1  # Default: PRODUKCJA

    def remove_slot(self, target, slot_id):
        with self.lock:
            before = len(self.data.get(target, []))
            self.data[target] = [s for s in self.data.get(target, []) if s['id'] != slot_id]
            self._save()
        return before > len(self.data.get(target, []))

    def clear_slots(self, target):
        with self.lock: self.data[target] = []; self._save()
        log.info('CAL', f"🧹 Cleared [{target}]")

    def list_slots(self, target):
        with self.lock: return list(self.data.get(target, []))

    def get_all(self):
        with self.lock: return {k: list(v) for k, v in self.data.items() if not k.startswith('_')}

    def get_effective_schedule(self, gw):
        with self.lock:
            return list(self.data.get(gw, [])) + list(self.data.get("GLOBAL", []))

    def compute_now_and_next(self, gw):
        now = now_ts(); slots = self.get_effective_schedule(gw)
        active = None; next_change = 0
        for s in sorted(slots, key=lambda x: x.get('updated_ts', 0), reverse=True):
            try:
                st = self._parse_ts(s['start']); et = self._parse_ts(s['end'])
            except: continue
            if st <= now < et and active is None:
                src = "override" if s['id'] in [x['id'] for x in self.data.get(gw, [])] else "global"
                active = (s['mode'], src)
            for tv in [st, et]:
                if tv > now and (next_change == 0 or tv < next_change): next_change = tv
        if active: return active[0], int(next_change), active[1]
        return 0, int(next_change), "none"

    def merge_schedule(self, gw, incoming):
        with self.lock:
            local = {s['id']: s for s in self.data.get(gw, [])}; changes = 0
            for slot in incoming:
                sid = slot['id']
                if sid not in local or slot.get('updated_ts', 0) > local[sid].get('updated_ts', 0):
                    local[sid] = slot; changes += 1
            self.data[gw] = sorted(local.values(), key=lambda s: s.get('start', '')); self._save()
        log.info('CAL', f"🔄 Merged [{gw}]: {changes} changes, {len(self.data[gw])} total")
        return changes

    def force_sync_all_to_supervisor(self):
        with self.lock:
            gl = list(self.data.get("GLOBAL", []))
            for gw in CONFIG['gateways']:
                self.data[gw] = [dict(s, updated_ts=int(now_ts()), origin='supervisor') for s in gl]
            self._save()
        log.info('CAL', f"📅 SYNC ALL: GLOBAL → all gateways ({len(gl)} slots)")

    # ── Compact encoding for LoRa transfer ──
    def prepare_compact(self, gw, window_days=14):
        """Prepare compact schedule for LoRa transfer.
        Format: [start_min, dur_min, mode]
        Mode number maps to name via CONFIG mode_names on both sides."""
        from datetime import timedelta
        now = datetime.now()
        window_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        window_end = window_start + timedelta(days=window_days)
        ws = window_start.timestamp(); we = window_end.timestamp()

        global_slots = self.list_slots('GLOBAL')
        gw_slots = self.list_slots(gw)

        seen = {}  # (start_min, dur_min) → mode
        for slot in global_slots + gw_slots:
            try:
                st = self._parse_ts(slot['start']); et = self._parse_ts(slot['end'])
            except: continue
            if et < ws or st > we: continue
            start_min = int((st - self.BASE_EPOCH) / 60)
            dur_min = int((et - st) / 60)
            seen[(start_min, dur_min)] = slot.get('mode', 1)

        compact = sorted([[k[0], k[1], v] for k, v in seen.items()])
        h = hashlib.sha256(json.dumps(compact, separators=(',',':')).encode()).hexdigest()[:12]
        log.info('CAL', f"📅 Compact {gw}: {len(global_slots)}g + {len(gw_slots)}gw → {len(compact)} (window: {window_start.date()} → {window_end.date()}, hash={h})")
        return compact, h

    @staticmethod
    def expand_compact(compact_list):
        """Decode compact slots → full format. Mode number → name from CONFIG."""
        BASE = ScheduleManager.BASE_EPOCH
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
        result = []
        for item in compact_list:
            if not isinstance(item, list) or len(item) < 3: continue
            start_min, dur_min, mode = item[0], item[1], item[2]
            st = BASE + start_min * 60
            et = st + dur_min * 60
            result.append({
                "id": f"s{start_min}",
                "start": datetime.fromtimestamp(st).strftime('%Y-%m-%d %H:%M:%S'),
                "end": datetime.fromtimestamp(et).strftime('%Y-%m-%d %H:%M:%S'),
                "mode": mode, "note": mode_names.get(mode, f"MODE_{mode}"),
                "updated_ts": int(now_ts()), "origin": "supervisor"
            })
        return result

    def _to_compact(self, slots):
        """Convert full slots list to compact format (no windowing)."""
        compact = []
        for slot in slots:
            try:
                st = self._parse_ts(slot['start']); et = self._parse_ts(slot['end'])
                compact.append([int((st - self.BASE_EPOCH) / 60), int((et - st) / 60), slot.get('mode', 1)])
            except: continue
        compact.sort()
        return compact


# =====================================================
# CALENDAR: Chunked LoRa Transfer Protocol
# v5.2 [K2]: cal_chunk_delay between chunks (6s default)
# v5.2 [K3]: get() instead of pop() — session kept until success/retry_max
# =====================================================
class CalendarTransfer:
    def __init__(self, queue_fn, chunk_size=140):
        self.queue_fn = queue_fn; self.chunk_size = chunk_size
        self.incoming = {}; self.outgoing = {}; self.lock = threading.Lock()

    @staticmethod
    def gen_tid(): return ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    @staticmethod
    def serialize(slots):
        raw = json.dumps(slots, separators=(',', ':')).encode()
        return base64.b64encode(zlib.compress(raw, 9)).decode()
    @staticmethod
    def deserialize(b64):
        return json.loads(zlib.decompress(base64.b64decode(b64)))
    @staticmethod
    def crc16(data): return hashlib.md5(data.encode()).hexdigest()[:4]

    def _chunk_delay(self):
        """v31: Delay between chunks — from CONFIG['calendar']['chunk_delay']."""
        return CONFIG['calendar'].get('chunk_delay', 6.0)

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'dir': direction, 'retries': 0, 'ts': now_ts(), 'ack_received': False}
        self.queue_fn({"t":"cal_begin","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, FlowPriority.SYSTEM)
        log.info('SYNC', f"📤 Begin {direction} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")
        def _send():
            delay = self._chunk_delay()  # [K2]
            for i, chunk in enumerate(chunks):
                time.sleep(delay)
                self.queue_fn({"t":"cal_chunk","tid":tid,"s":i,"d":chunk}, FlowPriority.SYSTEM)
            # [K4] Send sch_e with retry — LoRa packet loss recovery
            # If sch_e is lost in air, gateway never responds → deadlock without this retry
            SCH_E_RETRIES = 3
            SCH_E_WAIT = 15  # seconds to wait for any ACK/NACK after each sch_e
            for attempt in range(SCH_E_RETRIES):
                time.sleep(delay)
                with self.lock:
                    if tid not in self.outgoing:
                        return  # Session already completed (ACK ok=1 or retry exhausted)
                self.queue_fn({"t":"cal_end","tid":tid}, FlowPriority.SYSTEM)
                if attempt > 0:
                    log.warn('SYNC', f"⚠️ sch_e retry tid={tid} attempt {attempt+1}/{SCH_E_RETRIES}")
                # Wait and check for response
                time.sleep(SCH_E_WAIT)
                with self.lock:
                    info = self.outgoing.get(tid)
                    if info is None:
                        return  # Session removed → done (success or retry_max exhausted)
                    if info.get('ack_received'):
                        return  # NACK received → handle_ack spawned its own retransmit thread
            # All sch_e retries exhausted — no response at all
            log.error('SYNC', f"❌ tid={tid} no response after {SCH_E_RETRIES} sch_e attempts — giving up")
            with self.lock: self.outgoing.pop(tid, None)
        threading.Thread(target=_send, daemon=True).start()
        return tid

    def handle_begin(self, data):
        tid = data.get('tid')
        with self.lock:
            self.incoming[tid] = {'chunks': {}, 'total': data.get('n',0), 'crc': data.get('crc',''),
                                  'gw': data.get('g','?'), 'dir': data.get('dir','push'), 'ts': now_ts()}
        log.info('SYNC', f"📥 Begin {data.get('dir')} [{data.get('g')}] tid={tid} expect={data.get('n')}")

    def handle_chunk(self, data):
        tid = data.get('tid')
        with self.lock:
            if tid in self.incoming:
                self.incoming[tid]['chunks'][data.get('s',0)] = data.get('d','')
                self.incoming[tid]['ts'] = now_ts()  # [K3] Refresh ts — prevent cleanup_stale during active transfer

    def handle_end(self, data):
        tid = data.get('tid')
        # [K3] Use get() — keep session alive for possible NACK→retransmit
        with self.lock: info = self.incoming.get(tid)
        if not info: return None
        missing = [i for i in range(info['total']) if i not in info['chunks']]
        if missing:
            log.warn('SYNC', f"⚠️ tid={tid} missing chunks: {missing[:5]}")
            with self.lock: self.incoming[tid]['ts'] = now_ts()  # [K3] Refresh ts for retry window
            self.queue_fn({"t":"cal_ack","tid":tid,"ok":0,"miss":missing[:5]}, FlowPriority.SYSTEM)
            return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            log.warn('SYNC', f"⚠️ tid={tid} CRC mismatch")
            with self.lock: self.incoming[tid]['ts'] = now_ts()  # [K3] Refresh ts for retry window
            self.queue_fn({"t":"cal_ack","tid":tid,"ok":0,"miss":[]}, FlowPriority.SYSTEM)
            return None
        # Success — NOW remove the session [K3]
        with self.lock: self.incoming.pop(tid, None)
        self.queue_fn({"t":"cal_ack","tid":tid,"ok":1,"miss":[]}, FlowPriority.SYSTEM)
        try:
            slots = self.deserialize(b64)
            log.info('SYNC', f"✅ Received {len(slots)} slots [{info['gw']}] dir={info['dir']}")
            return (info['gw'], info['dir'], slots)
        except Exception as e:
            log.error('SYNC', f"Deserialize: {e}"); return None

    def handle_ack(self, data):
        tid = data.get('tid'); ok = data.get('ok',0); miss = data.get('miss',[])
        # [K3] Use get() — keep session until success or retry_max exhausted
        with self.lock: info = self.outgoing.get(tid)
        if not info: return
        # [K4] Signal to _send thread that we got a response (stops sch_e retry loop)
        with self.lock: self.outgoing[tid]['ack_received'] = True
        if ok:
            # Success — NOW remove the session [K3]
            with self.lock: self.outgoing.pop(tid, None)
            log.info('SYNC', f"✅ ACK tid={tid}")
        elif miss and info['retries'] < CONFIG['calendar']['retry_max']:
            info['retries'] += 1
            with self.lock:
                self.outgoing[tid]['retries'] = info['retries']
                self.outgoing[tid]['ts'] = now_ts()  # [K3] Refresh ts
            delay = self._chunk_delay()  # [K2] Use cal_chunk_delay for retransmit too
            def _r():
                for s in miss:
                    if s < len(info['chunks']):
                        time.sleep(delay)
                        self.queue_fn({"t":"cal_chunk","tid":tid,"s":s,"d":info['chunks'][s]}, FlowPriority.SYSTEM)
                time.sleep(delay)
                self.queue_fn({"t":"cal_end","tid":tid}, FlowPriority.SYSTEM)
            threading.Thread(target=_r, daemon=True).start()
        else:
            # Retry exhausted — remove session [K3]
            with self.lock: self.outgoing.pop(tid, None)
            log.error('SYNC', f"❌ Transfer failed tid={tid} after {info['retries']} retries")

    def cleanup_stale(self, max_age=120):
        now = now_ts()
        with self.lock:
            for st in [self.incoming, self.outgoing]:
                for tid in [t for t, i in st.items() if now - i.get('ts',0) > max_age]: del st[tid]


# =====================================================
# HOME ASSISTANT MQTT DISCOVERY (unchanged + v5.0 additions)
# =====================================================
class HomeAssistant:
    def __init__(self, mqtt_client):
        self.mqtt = mqtt_client; self.registered = set(); self.registered_anomalies = set()

    def _safe(self, s): return s.replace(' ', '_').lower()

    def _dev_info(self, gw, dev, model="Zigbee Device"):
        return {"identifiers": [f"lora_{gw.lower()}_{self._safe(dev)}"], "name": f"LoRa {dev}",
                "model": model, "manufacturer": "LoRa Gateway", "via_device": f"lora_gateway_{gw.lower()}"}

    def _gw_info(self, gw):
        return {"identifiers": [f"lora_gateway_{gw.lower()}"], "name": f"LoRa Gateway {gw}",
                "model": "LoRa Zigbee Gateway", "manufacturer": "Custom"}

    def _sup_info(self):
        return {"identifiers": ["lora_supervisor"], "name": "LoRa Supervisor", "model": "Supervisor", "manufacturer": "Custom"}

    def reg_gateway(self, gw):
        if f"gw_{gw}" in self.registered: return
        di, gl, st, p = self._gw_info(gw), gw.lower(), f"{CONFIG['state_prefix']}/gw/{gw.lower()}/status", CONFIG['ha_prefix']
        for eid, name, tpl, icon in [
            ("status", "Status", "{{ value_json.state }}", None),
            ("uptime", "Uptime", "{{ value_json.uptime | default(0) }}", "mdi:timer"),
            ("last_seen", "Last Seen", "{{ value_json.last_seen | default('--') }}", "mdi:clock-outline"),
            ("devices_total", "Total", "{{ value_json.devices_total | default(0) }}", "mdi:devices"),
            ("devices_monitored", "Monitored", "{{ value_json.devices_monitored | default(0) }}", "mdi:eye"),
            ("devices_offline", "Offline", "{{ value_json.devices_offline | default(0) }}", "mdi:close-circle"),
            ("devices_low_battery", "Low Battery", "{{ value_json.devices_low_battery | default(0) }}", "mdi:battery-low"),
            ("devices_anomaly", "Anomaly", "{{ value_json.devices_anomaly | default(0) }}", "mdi:alert"),
            # v5.0: Additional gateway diagnostics
            ("airtime", "Airtime %", "{{ value_json.airtime | default(0) }}", "mdi:radio-tower"),
            ("rssi", "RSSI", "{{ value_json.rssi | default('--') }}", "mdi:signal"),
            ("disc_hash", "Config Hash", "{{ value_json.disc_hash | default('--') }}", "mdi:identifier"),
            ("z2m_age", "Z2M Age", "{{ value_json.z2m_age | default(-1) }}", "mdi:zigbee"),
            ("z2m_stale", "Z2M Stale", "{{ 'ON' if value_json.z2m_stale else 'OFF' }}", "mdi:alert-outline"),
            # v6.3: Batch latency diagnostic
            ("batch_latency", "Batch Latency", "{{ value_json.batch_latency | default(0) }}", "mdi:timer-sand"),
            # v30: Context-aware fields + priority count
            ("devices_priority", "Priority", "{{ value_json.devices_priority | default(0) }}", "mdi:alert-octagon"),
            ("context_mode", "Context Mode", "{{ value_json.context_mode | default('?') }}", "mdi:weather-sunny"),
            ("sync_age", "Sync Age", "{{ value_json.sync_age | default(-1) }}", "mdi:clock-alert"),
            ("time_offset", "Time Offset", "{{ value_json.time_offset | default(0) }}", "mdi:clock-fast"),
            ("queue_depth", "Queue Depth", "{{ value_json.queue_depth | default(0) }}", "mdi:tray-full"),
        ]:
            if eid == "status":
                self.mqtt.publish(f"{p}/binary_sensor/lora_gw_{gl}_{eid}/config", json.dumps({
                    "name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                    "state_topic": st, "value_template": tpl, "payload_on": "online", "payload_off": "offline",
                    "device_class": "connectivity", "device": di}), retain=True)
            elif eid == "z2m_stale":
                # v5.0: Binary sensor for Z2M staleness
                self.mqtt.publish(f"{p}/binary_sensor/lora_gw_{gl}_{eid}/config", json.dumps({
                    "name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                    "state_topic": st, "value_template": tpl, "payload_on": "ON", "payload_off": "OFF",
                    "device_class": "problem", "device": di, "icon": icon}), retain=True)
            else:
                cfg = {"name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                       "state_topic": st, "value_template": tpl, "device": di}
                if icon: cfg["icon"] = icon
                if eid in ("uptime", "z2m_age"): cfg["unit_of_measurement"] = "s"
                elif eid == "airtime": cfg["unit_of_measurement"] = "%"
                elif eid == "rssi": cfg["unit_of_measurement"] = "dBm"
                elif eid == "batch_latency": cfg["unit_of_measurement"] = "s"
                elif eid in ("sync_age",): cfg["unit_of_measurement"] = "s"
                elif eid == "time_offset": cfg["unit_of_measurement"] = "s"
                self.mqtt.publish(f"{p}/sensor/lora_gw_{gl}_{eid}/config", json.dumps(cfg), retain=True)
        for eid, name, icon in [("ping","Ping","mdi:lan-connect"),("discovery","Discovery","mdi:magnify"),
                                ("clear_anomalies","Clear Anomalies","mdi:bell-off"),("dump_anom","Dump Anomalies","mdi:alert-circle-outline"),
                                # v30: Per-gateway config + params
                                ("send_config","Send Config","mdi:cog-sync"),("send_params","Send Params","mdi:tune-variant"),
        ]:
            self.mqtt.publish(f"{p}/button/lora_gw_{gl}_{eid}/config", json.dumps({
                "name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                "command_topic": f"{CONFIG['state_prefix']}/gw/{gl}/cmd/{eid}", "device": di, "icon": icon}), retain=True)
        # v37: Per-gateway anomaly list sensors (dashboard reads from json_attributes)
        for cat, cname, cicon in [("offline","Offline Anomalies","mdi:lan-disconnect"),
                                   ("battery","Battery Anomalies","mdi:battery-alert"),
                                   ("other","Other Anomalies","mdi:alert")]:
            at = f"{CONFIG['state_prefix']}/gw/{gl}/anomaly_{cat}"
            self.mqtt.publish(f"{p}/sensor/lora_an_{gl}_{cat}/config", json.dumps({
                "name": f"GW {gw} {cname}", "object_id": f"lora_an_{gl}_{cat}",
                "unique_id": f"lora_an_{gl}_{cat}",
                "state_topic": at, "value_template": "{{ value_json.count | default(0) }}",
                "json_attributes_topic": at, "device": di, "icon": cicon}), retain=True)
        # v30: Per-gateway custom param number entities (editable from dashboard)
        params_cfg = CONFIG.get('params', {})
        for pid in ['p1', 'p2', 'p3']:
            pcfg = params_cfg.get(pid, {})
            pname = pcfg.get('name', pid.upper())
            picon = pcfg.get('icon', 'mdi:tune-variant')
            pmin = pcfg.get('min', 0)
            pmax = pcfg.get('max', 1000)
            pstep = pcfg.get('step', 1)
            punit = pcfg.get('unit', '')
            pdefault = pcfg.get('default', 0)
            eid = f"param_{pid}"
            self.mqtt.publish(f"{p}/number/lora_gw_{gl}_{eid}/config", json.dumps({
                "name": f"GW {gw} {pname}",
                "object_id": f"lora_gw_{gl}_{eid}",
                "unique_id": f"lora_gw_{gl}_{eid}",
                "state_topic": f"{CONFIG['state_prefix']}/gw/{gl}/param/{pid}",
                "command_topic": f"{CONFIG['state_prefix']}/gw/{gl}/param/{pid}/set",
                "min": pmin, "max": pmax, "step": pstep,
                "unit_of_measurement": punit if punit else None,
                "device": di, "icon": picon
            }), retain=True)
            # Publish default value
            self.mqtt.publish(f"{CONFIG['state_prefix']}/gw/{gl}/param/{pid}", str(pdefault), retain=True)
        self.registered.add(f"gw_{gw}")

    def reg_supervisor(self):
        if "supervisor" in self.registered: return
        di, p = self._sup_info(), CONFIG['ha_prefix']
        for eid, name, icon in [
            ("send_config","Send Config","mdi:cog-sync"),("ping_all","Ping All","mdi:lan-connect"),
            ("discovery_all","Discovery All","mdi:magnify"),("clear_all_anomalies","Clear All","mdi:bell-off"),
            ("clear_offline","Clear Offline","mdi:close-circle-outline"),("clear_low_battery","Clear Low Battery","mdi:battery-off"),
            ("clear_other","Clear Other Anomalies","mdi:alert-remove"),("dump_anom_all","Dump All Anomalies","mdi:alert-circle-outline"),
            ("prune_anomalies","Prune Ghost Anomalies","mdi:ghost-off"),
            # v30: Send params (global broadcast)
            ("send_params","Send Params","mdi:tune-variant"),
            # v32: Manual sync broadcast
            ("send_sync","Send Sync","mdi:clock-sync"),
        ]:
            self.mqtt.publish(f"{p}/button/lora_supervisor_{eid}/config", json.dumps({
                "name": f"LoRa {name}", "object_id": f"lora_supervisor_{eid}", "unique_id": f"lora_supervisor_{eid}",
                "command_topic": f"{CONFIG['state_prefix']}/supervisor/cmd/{eid}", "device": di, "icon": icon}), retain=True)
        for eid, name, default in [
            ("timeout_switch","Timeout Switch",CONFIG['timeout']['switch']),
            ("timeout_sensor","Timeout Sensor",CONFIG['timeout']['sensor']),
            ("timeout_binary","Timeout Binary",CONFIG['timeout']['binary_sensor'])]:
            self.mqtt.publish(f"{p}/number/lora_{eid}/config", json.dumps({
                "name": f"LoRa {name}", "object_id": f"lora_{eid}", "unique_id": f"lora_{eid}",
                "state_topic": f"{CONFIG['state_prefix']}/config/{eid}",
                "command_topic": f"{CONFIG['state_prefix']}/config/{eid}/set",
                "min":1,"max":999999,"step":1,"unit_of_measurement":"s","device": di,"icon":"mdi:timer-cog"}), retain=True)
            self.mqtt.publish(f"{CONFIG['state_prefix']}/config/{eid}", str(default), retain=True)
        self.registered.add("supervisor")

    def reg_switch(self, gw, dev, dtype='switch'):
        key = f"{gw}_{dev}"
        if key in self.registered: return
        safe, gl, di = self._safe(dev), gw.lower(), self._dev_info(gw, dev, dtype.capitalize())
        st, av, p = f"{CONFIG['state_prefix']}/{gl}/{safe}/state", f"{CONFIG['state_prefix']}/{gl}/{safe}/available", CONFIG['ha_prefix']
        self.mqtt.publish(f"{p}/switch/lora_{gl}_{safe}/config", json.dumps({
            "name": f"LoRa {dev}", "object_id": f"lora_{gl}_{safe}", "unique_id": f"lora_{gl}_{safe}_switch",
            "state_topic": st, "command_topic": f"{CONFIG['state_prefix']}/{gl}/{safe}/set",
            "value_template": "{{ value_json.state }}", "state_on":"ON","state_off":"OFF",
            "payload_on":"ON","payload_off":"OFF", "device": di}), retain=True)
        self.mqtt.publish(f"{p}/sensor/lora_{gl}_{safe}_last_seen/config", json.dumps({
            "name": f"{dev} Last Seen", "object_id": f"lora_{gl}_{safe}_last_seen", "unique_id": f"lora_{gl}_{safe}_last_seen",
            "state_topic": st, "value_template": "{{ value_json.last_seen | default('--') }}", "device": di, "icon":"mdi:clock-outline"}), retain=True)
        self.mqtt.publish(f"{p}/binary_sensor/lora_{gl}_{safe}_available/config", json.dumps({
            "name": f"{dev} Available", "object_id": f"lora_{gl}_{safe}_available", "unique_id": f"lora_{gl}_{safe}_available",
            "state_topic": av, "payload_on":"online","payload_off":"offline","device_class":"connectivity","device": di}), retain=True)
        self.mqtt.publish(f"{p}/button/lora_{gl}_{safe}_refresh/config", json.dumps({
            "name": f"{dev} Refresh", "object_id": f"lora_{gl}_{safe}_refresh", "unique_id": f"lora_{gl}_{safe}_refresh",
            "command_topic": f"{CONFIG['state_prefix']}/{gl}/{safe}/refresh", "device": di, "icon":"mdi:refresh"}), retain=True)
        self.registered.add(key)

    def reg_sensor(self, gw, dev, caps):
        key = f"{gw}_{dev}"
        if key in self.registered: return
        safe, gl, di = self._safe(dev), gw.lower(), self._dev_info(gw, dev, "Sensor")
        st, av, p = f"{CONFIG['state_prefix']}/{gl}/{safe}/state", f"{CONFIG['state_prefix']}/{gl}/{safe}/available", CONFIG['ha_prefix']
        for cap, tpl, dc, unit in [('temperature',"{{ value_json.temperature | default('--') }}",'temperature','°C'),
                                   ('humidity',"{{ value_json.humidity | default('--') }}",'humidity','%'),
                                   ('battery',"{{ value_json.battery | default(0) }}",'battery','%')]:
            if cap in caps:
                self.mqtt.publish(f"{p}/sensor/lora_{gl}_{safe}_{cap[:4]}/config", json.dumps({
                    "name": f"{dev} {cap.title()}", "object_id": f"lora_{gl}_{safe}_{cap[:4]}", "unique_id": f"lora_{gl}_{safe}_{cap[:4]}",
                    "state_topic": st, "value_template": tpl, "unit_of_measurement": unit, "device_class": dc, "device": di}), retain=True)
        self.mqtt.publish(f"{p}/sensor/lora_{gl}_{safe}_last_seen/config", json.dumps({
            "name": f"{dev} Last Seen", "object_id": f"lora_{gl}_{safe}_last_seen", "unique_id": f"lora_{gl}_{safe}_last_seen",
            "state_topic": st, "value_template": "{{ value_json.last_seen | default('--') }}", "device": di, "icon":"mdi:clock-outline"}), retain=True)
        self.mqtt.publish(f"{p}/binary_sensor/lora_{gl}_{safe}_available/config", json.dumps({
            "name": f"{dev} Available", "object_id": f"lora_{gl}_{safe}_available", "unique_id": f"lora_{gl}_{safe}_available",
            "state_topic": av, "payload_on":"online","payload_off":"offline","device_class":"connectivity","device": di}), retain=True)
        self.mqtt.publish(f"{p}/button/lora_{gl}_{safe}_refresh/config", json.dumps({
            "name": f"{dev} Refresh", "object_id": f"lora_{gl}_{safe}_refresh", "unique_id": f"lora_{gl}_{safe}_refresh",
            "command_topic": f"{CONFIG['state_prefix']}/{gl}/{safe}/refresh", "device": di, "icon":"mdi:refresh"}), retain=True)
        self.registered.add(key)

    def reg_binary(self, gw, dev, caps):
        key = f"{gw}_{dev}"
        if key in self.registered: return
        safe, gl, di = self._safe(dev), gw.lower(), self._dev_info(gw, dev, "Binary Sensor")
        st, av, p = f"{CONFIG['state_prefix']}/{gl}/{safe}/state", f"{CONFIG['state_prefix']}/{gl}/{safe}/available", CONFIG['ha_prefix']
        for cap, tpl, dc in [('contact',"{{ 'OFF' if value_json.contact else 'ON' }}",'door'),
                             ('occupancy',"{{ 'ON' if value_json.occupancy else 'OFF' }}",'motion'),
                             ('water_leak',"{{ 'ON' if value_json.water_leak else 'OFF' }}",'moisture'),
                             ('smoke',"{{ 'ON' if value_json.smoke else 'OFF' }}",'smoke')]:
            if cap in caps:
                self.mqtt.publish(f"{p}/binary_sensor/lora_{gl}_{safe}_{cap}/config", json.dumps({
                    "name": f"LoRa {dev}", "object_id": f"lora_{gl}_{safe}_{cap}", "unique_id": f"lora_{gl}_{safe}_{cap}",
                    "state_topic": st, "value_template": tpl, "device_class": dc, "device": di}), retain=True)
                break
        if 'battery' in caps:
            self.mqtt.publish(f"{p}/sensor/lora_{gl}_{safe}_batt/config", json.dumps({
                "name": f"{dev} Battery", "object_id": f"lora_{gl}_{safe}_batt", "unique_id": f"lora_{gl}_{safe}_batt",
                "state_topic": st, "value_template":"{{ value_json.battery | default(0) }}","unit_of_measurement":"%",
                "device_class":"battery","device": di}), retain=True)
        self.mqtt.publish(f"{p}/sensor/lora_{gl}_{safe}_last_seen/config", json.dumps({
            "name": f"{dev} Last Seen", "object_id": f"lora_{gl}_{safe}_last_seen", "unique_id": f"lora_{gl}_{safe}_last_seen",
            "state_topic": st, "value_template":"{{ value_json.last_seen | default('--') }}","device": di,"icon":"mdi:clock-outline"}), retain=True)
        self.mqtt.publish(f"{p}/binary_sensor/lora_{gl}_{safe}_available/config", json.dumps({
            "name": f"{dev} Available", "object_id": f"lora_{gl}_{safe}_available", "unique_id": f"lora_{gl}_{safe}_available",
            "state_topic": av, "payload_on":"online","payload_off":"offline","device_class":"connectivity","device": di}), retain=True)
        self.mqtt.publish(f"{p}/button/lora_{gl}_{safe}_refresh/config", json.dumps({
            "name": f"{dev} Refresh", "object_id": f"lora_{gl}_{safe}_refresh", "unique_id": f"lora_{gl}_{safe}_refresh",
            "command_topic": f"{CONFIG['state_prefix']}/{gl}/{safe}/refresh", "device": di, "icon":"mdi:refresh"}), retain=True)
        self.registered.add(key)

    def reg_anomaly(self, anomaly_id, gw, dev, atype, value, detected_at=None):
        """v37: Always publish discovery config (idempotent). Ensures entity exists in HA."""
        safe_id, p = self._safe(anomaly_id), CONFIG['ha_prefix']
        st = f"{CONFIG['state_prefix']}/anomaly/{safe_id}"
        icon = {"low_battery":"mdi:battery-low","critical_battery":"mdi:battery-alert",
                "device_offline":"mdi:lan-disconnect","temp_high":"mdi:thermometer-high",
                "temp_low":"mdi:thermometer-low","stagnation":"mdi:timer-sand",
                "hum_high":"mdi:water-percent","hum_low":"mdi:water-percent-alert"}.get(atype, "mdi:alert")
        self.mqtt.publish(f"{p}/sensor/lora_an_{safe_id}/config", json.dumps({
            "name": f"{gw} | {dev} | {atype}", "object_id": f"lora_an_{safe_id}", "unique_id": f"lora_an_{safe_id}",
            "state_topic": st, "value_template":"{{ value_json.value if value_json.value else 'active' }}",
            "json_attributes_topic": st, "icon": icon}), retain=True)
        self.mqtt.publish(f"{p}/button/lora_an_{safe_id}_clear/config", json.dumps({
            "name": f"Clear {dev}", "object_id": f"lora_an_{safe_id}_clear", "unique_id": f"lora_an_{safe_id}_clear",
            "command_topic": f"{CONFIG['state_prefix']}/anomaly/{safe_id}/clear", "icon":"mdi:close-circle"}), retain=True)
        self.registered_anomalies.add(anomaly_id)

    def pub_anomaly(self, a):
        self.mqtt.publish(f"{CONFIG['state_prefix']}/anomaly/{self._safe(a['id'])}", json.dumps(a), retain=True)

    def remove_anomaly(self, aid):
        s, p = self._safe(aid), CONFIG['ha_prefix']
        for t in [f"{p}/sensor/lora_an_{s}/config",f"{p}/button/lora_an_{s}_clear/config",f"{CONFIG['state_prefix']}/anomaly/{s}"]:
            self.mqtt.publish(t, "", retain=True)
        self.registered_anomalies.discard(aid)

    def pub_gw_status(self, gw, data):
        self.mqtt.publish(f"{CONFIG['state_prefix']}/gw/{gw.lower()}/status", json.dumps(data), retain=True)

    def pub_dev_state(self, gw, dev, data):
        self.mqtt.publish(f"{CONFIG['state_prefix']}/{gw.lower()}/{self._safe(dev)}/state", json.dumps(data), retain=True)

    def pub_dev_available(self, gw, dev, available):
        self.mqtt.publish(f"{CONFIG['state_prefix']}/{gw.lower()}/{self._safe(dev)}/available", "online" if available else "offline", retain=True)

    # v5.0: Virtual I/O registration from disc_meta
    def reg_vswitch(self, gw, vid, name, value=0):
        """Register a virtual switch entity in HA (from gateway config via discovery)."""
        key = f"vsw_{gw}_{vid}"
        if key in self.registered: return
        gl, p = gw.lower(), CONFIG['ha_prefix']
        di = {"identifiers": [f"lora_vio_{gl}"], "name": f"LoRa Virtual I/O {gw}",
              "model": "Virtual I/O", "manufacturer": "LoRa Gateway"}
        st = f"{CONFIG['state_prefix']}/{gl}/vio/{vid}/state"
        self.mqtt.publish(f"{p}/switch/lora_{gl}_vsw_{vid}/config", json.dumps({
            "name": f"LoRa {name}", "object_id": f"lora_{gl}_vsw_{vid}", "unique_id": f"lora_{gl}_vsw_{vid}",
            "state_topic": st, "command_topic": f"{CONFIG['state_prefix']}/{gl}/vio/{vid}/set",
            "payload_on": "ON", "payload_off": "OFF", "device": di, "icon": "mdi:toggle-switch"}), retain=True)
        self.mqtt.publish(st, "ON" if value else "OFF", retain=True)
        self.registered.add(key)

    def reg_vbutton(self, gw, vid, name):
        """Register a virtual button entity in HA (from gateway config via discovery)."""
        key = f"vbtn_{gw}_{vid}"
        if key in self.registered: return
        gl, p = gw.lower(), CONFIG['ha_prefix']
        di = {"identifiers": [f"lora_vio_{gl}"], "name": f"LoRa Virtual I/O {gw}",
              "model": "Virtual I/O", "manufacturer": "LoRa Gateway"}
        self.mqtt.publish(f"{p}/button/lora_{gl}_vbtn_{vid}/config", json.dumps({
            "name": f"LoRa {name}", "object_id": f"lora_{gl}_vbtn_{vid}", "unique_id": f"lora_{gl}_vbtn_{vid}",
            "command_topic": f"{CONFIG['state_prefix']}/{gl}/vio/{vid}/press", "device": di, "icon": "mdi:gesture-tap-button"}), retain=True)
        self.registered.add(key)


# =====================================================
# MULTI-ANTENNA MANAGER (unchanged)
# =====================================================
class AntennaManager:
    def __init__(self, on_receive):
        self.on_receive = on_receive
        self.interfaces = {}
        self.lock = threading.Lock()
        self._seen_msgs = deque(maxlen=200)
        self._reconnect_backoff = {}
        self.running = True
        # v5.1: Gateway → interface label routing
        self.gw_routes = {}  # {"G1": "ANT-1", "G2": "ANT-2", ...}
        # v5.2 [K1]: Dedicated TX queue + thread — sendText() no longer blocks main loop
        self._tx_queue = deque()
        self._tx_thread = None

    def connect_all(self):
        pub.subscribe(self._mesh_rx, "meshtastic.receive.text")
        for acfg in CONFIG['mesh_ports']:
            if acfg.get('enabled', False):
                self._connect_one(acfg)
                # v5.1: Build gateway routing table
                for gw in acfg.get('gateways', []):
                    self.gw_routes[gw] = acfg['label']
        n = len(self.interfaces)
        log.info('ANT', f"📡 {n} antenna(s) connected, routes: {self.gw_routes}")
        if n == 0:
            log.warn('ANT', "⚠️ No antennas connected! Will retry...")
        # v5.2 [K1]: Start dedicated TX thread
        self._tx_thread = threading.Thread(target=self._tx_loop, daemon=True, name="ant-tx")
        self._tx_thread.start()

    def _connect_one(self, acfg):
        label, port = acfg['label'], acfg['port']
        try:
            iface = meshtastic.serial_interface.SerialInterface(devPath=port)
            with self.lock: self.interfaces[label] = iface
            self._reconnect_backoff[label] = 0
            log.info('ANT', f"✅ {label} ({port}) connected")
            return True
        except Exception as e:
            log.error('ANT', f"❌ {label} ({port}): {e}")
            return False

    def _mesh_rx(self, packet, interface):
        try:
            text = packet.get('decoded', {}).get('text') if isinstance(packet, dict) else None
            if not text: return
            h = hashlib.md5(text.encode()).hexdigest()[:8]
            if h in self._seen_msgs: return
            self._seen_msgs.append(h)
            log.info('LORA', f"📥 {text[:120]}")
            self.on_receive(text)
        except: pass

    # v5.2 [K1]: Dedicated TX loop — executes sendText() with timeout, never blocks main loop
    def _tx_loop(self):
        """Dedicated TX thread. Pops jobs from _tx_queue and calls iface.sendText()
        with an 8s timeout. If sendText() blocks beyond timeout, the interface is
        declared dead and removed so the main loop is never stuck."""
        TX_TIMEOUT = 8  # seconds
        while self.running:
            if not self._tx_queue:
                time.sleep(0.05)
                continue
            job = self._tx_queue.popleft()
            text = job['text']
            target_label = job.get('label')  # None = broadcast all
            if target_label:
                # Unicast via specific interface
                with self.lock: iface = self.interfaces.get(target_label)
                if iface:
                    self._tx_with_timeout(target_label, iface, text, TX_TIMEOUT)
                else:
                    # Fallback: broadcast if routed interface gone
                    log.warn('ANT', f"⚠️ {target_label} gone, fallback broadcast")
                    self._tx_broadcast(text, TX_TIMEOUT)
            else:
                self._tx_broadcast(text, TX_TIMEOUT)

    def _tx_broadcast(self, text, timeout):
        """Send via ALL connected antennas."""
        with self.lock: ifaces = list(self.interfaces.items())
        sent = False
        for label, iface in ifaces:
            if self._tx_with_timeout(label, iface, text, timeout):
                sent = True
        if not sent:
            log.error('ANT', "❌ No antenna available for TX!")

    def _tx_with_timeout(self, label, iface, text, timeout):
        """Execute iface.sendText() in a sub-thread with timeout.
        Returns True on success, False on failure/timeout."""
        result = [False]
        error = [None]
        def _do():
            try:
                iface.sendText(text)
                result[0] = True
            except Exception as e:
                error[0] = e
        t = threading.Thread(target=_do, daemon=True)
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            log.error('ANT', f"❌ TX {label}: sendText() blocked >{timeout}s — force-closing antenna")
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                try: dead.close()
                except: pass
            return False
        if error[0]:
            log.error('ANT', f"❌ TX {label}: {error[0]}")
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                try: dead.close()
                except: pass
            return False
        return result[0]

    def send_text(self, text):
        """Send via ALL connected antennas (broadcast). Non-blocking — enqueues to TX thread."""
        self._tx_queue.append({'text': text, 'label': None})
        return True

    def send_text_to(self, gw, text):
        """v5.1: Send via specific antenna routed to target gateway (unicast).
        Non-blocking — enqueues to TX thread. Falls back to broadcast if no route."""
        label = self.gw_routes.get(gw)
        if label:
            with self.lock: has_iface = label in self.interfaces
            if has_iface:
                self._tx_queue.append({'text': text, 'label': label})
                log.debug('ANT', f"📡 Unicast TX queued via {label} → {gw}")
                return True
        log.warn('ANT', f"⚠️ No route for {gw}, fallback to broadcast")
        self._tx_queue.append({'text': text, 'label': None})
        return True

    def _force_release_port(self, port):
        """v38: Force-release serial port lock. Handles stale Meshtastic locks
        after USB-Ethernet disconnect/reconnect cycles."""
        try:
            # Close any Python file descriptors for this port
            pid = os.getpid()
            for fd_link in glob.glob(f'/proc/{pid}/fd/*'):
                try:
                    target = os.readlink(fd_link)
                    if port in target:
                        fd_num = int(os.path.basename(fd_link))
                        os.close(fd_num)
                        log.info('ANT', f"🔓 Closed stale fd {fd_num} → {port}")
                except: pass
        except: pass
        try:
            # Release lockfile if exists
            lockfile = f'/tmp/pyserial.{port.replace("/","_")}.lock'
            if os.path.exists(lockfile):
                os.unlink(lockfile)
                log.info('ANT', f"🔓 Removed lockfile {lockfile}")
        except: pass
        time.sleep(1)  # Let OS release resources

    def reconnect_loop(self):
        while self.running:
            time.sleep(5)
            if not CONFIG['mesh_reconnect'].get('enabled', True): continue
            for acfg in CONFIG['mesh_ports']:
                if not acfg.get('enabled', False): continue
                label = acfg['label']
                port = acfg['port']
                with self.lock: connected = label in self.interfaces
                if connected:
                    try:
                        iface = self.interfaces.get(label)
                        if iface and hasattr(iface, 'localNode'):
                            _ = iface.localNode
                    except:
                        log.warn('ANT', f"⚠️ {label} unhealthy, force-closing")
                        # v38: Force-close the old interface before removing
                        with self.lock: old_iface = self.interfaces.pop(label, None)
                        if old_iface:
                            try: old_iface.close()
                            except: pass
                        self._force_release_port(port)
                else:
                    backoff = self._reconnect_backoff.get(label, 0)
                    interval = min(CONFIG['mesh_reconnect']['interval'] * (2 ** backoff),
                                   CONFIG['mesh_reconnect']['max_backoff'])
                    time.sleep(interval)
                    # v38: Check if port exists before trying
                    if not os.path.exists(port):
                        log.debug('ANT', f"⚠️ {label} port {port} not found (USB disconnected)")
                        self._reconnect_backoff[label] = min(backoff + 1, 6)
                        continue
                    log.info('ANT', f"🔄 Reconnecting {label} ({port})...")
                    # v38: Force-release port before reconnect attempt
                    self._force_release_port(port)
                    if self._connect_one(acfg):
                        self._reconnect_backoff[label] = 0
                    else:
                        self._reconnect_backoff[label] = min(backoff + 1, 6)

    def get_status(self):
        result = {}
        with self.lock:
            for acfg in CONFIG['mesh_ports']:
                label = acfg['label']
                result[label] = {
                    "port": acfg['port'],
                    "enabled": acfg.get('enabled', False),
                    "connected": label in self.interfaces
                }
        return result

    def close_all(self):
        self.running = False
        with self.lock:
            for label, iface in list(self.interfaces.items()):
                try: iface.close()
                except: pass
            self.interfaces.clear()


# =====================================================
# SUPERVISOR MAIN CLASS
# =====================================================
class Supervisor:
    def __init__(self):
        self.mqtt_client = self.ha = None
        self.antenna_mgr = None
        self.gateways = {gw: {
            'online': False, 'last_seen': None, 'last_seen_ts': 0, 'uptime': 0,
            'devices_total': 0, 'devices_monitored': 0, 'devices_priority': 0,
            'airtime': 0, 'rssi': -999, 'disc_hash': '',
            'z2m_age': -1, 'z2m_stale': False,
            'batch_latency': 0,
            # v26: Context fields
            'context_mode': '?', 'sync_age': -1, 'time_offset': 0, 'queue_depth': 0,
        } for gw in CONFIG['gateways']}
        self.devices = {}
        self.device_states = {}
        self.anomaly_list = []
        # v25: 3-priority queues (maxlen=200)
        self.queue = {p: deque(maxlen=200) for p in FlowPriority}
        self.last_tx = 0
        self.running = True
        self.lock = threading.Lock()

        # Calendar
        self.scheduler = ScheduleManager()
        self.cal_transfer = CalendarTransfer(self._add_queue, CONFIG['calendar']['chunk_size'])

        # Stale cleanup
        self._retained_anomaly_ids = set()
        self._dump_received_ids = set()
        self._startup_phase = True
        self._dump_start_ts = 0

        # Orphan tracking
        self._gw_devices = {gw: set() for gw in CONFIG['gateways']}
        self._disc_refresh_gw = set()

        # v5.0: Discovery hash cache per gateway
        self._gw_disc_hash = {gw: '' for gw in CONFIG['gateways']}

        # v5.0: Short ID → device name map per gateway (from disc_resp)
        self._dev_short_map = {gw: {} for gw in CONFIG['gateways']}  # {gw: {0: "Temp 1", 1: "Leak 1"}}

        # v5.0: Virtual I/O definitions per gateway (from disc_meta)
        self._gw_vio = {gw: [] for gw in CONFIG['gateways']}

        # v5.0: Last ping time for 30-min heartbeat
        self._last_ping_ts = 0

        # Command sequence counter — prevents out-of-order execution on gateway
        self._cmd_seq = 0

        # v5.0: Virtual switch states per gateway (from heartbeat/vsw_st)
        self._gw_vswitch_states = {gw: {} for gw in CONFIG['gateways']}

        # v27: Ping retry tracking — mark offline if no pong after retries
        self._pending_pings = {}  # {gw: {"ts": time, "retries": 0}}
        self._ping_retry_max = 3
        self._ping_retry_timeout = 30  # seconds to wait for pong

        # v26: Time + context sync
        self._sync_seq = 0           # monotonic sequence for out-of-order rejection
        self._sun_state = "day"      # current sun state from HA
        self._last_sync_broadcast = 0

        # v30: Custom param values per gateway
        self._gw_params = {gw: {
            f: CONFIG.get('params', {}).get(f, {}).get('default', 0)
            for f in ['p1', 'p2', 'p3']
        } for gw in CONFIG['gateways']}

        # v32: Calendar hash per gateway — for auto resync detection
        self._gw_cal_hash = {gw: '' for gw in CONFIG['gateways']}
        self._gw_last_rx = {}  # v37: safe window — timestamp of last RX from each gateway
        # v33: Expected cal hash (what supervisor sent) — for mismatch detection
        # v37: Compute from saved schedules.json on startup (not empty)
        self._gw_expected_cal_hash = {}
        for gw in CONFIG['gateways']:
            try:
                _, h = self.scheduler.prepare_compact(gw)
                self._gw_expected_cal_hash[gw] = h
            except:
                self._gw_expected_cal_hash[gw] = ''

        # v33: Load persisted params
        self._load_params()

    # --- Helpers ---
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly', {}).get('auto_clear', {}).get(atype, True)

    def _save_params(self):
        """v33: Persist gateway params to disk."""
        try:
            with open(PARAMS_FILE, 'w') as f:
                json.dump(self._gw_params, f)
        except Exception as e: log.debug('CFG', f"Save params: {e}")

    def _load_params(self):
        """v33: Load persisted gateway params from disk."""
        try:
            with open(PARAMS_FILE, 'r') as f:
                saved = json.load(f)
            for gw in CONFIG['gateways']:
                if gw in saved:
                    self._gw_params[gw].update(saved[gw])
            log.info('CFG', f"⚙️ Loaded params: {PARAMS_FILE}")
        except FileNotFoundError: pass
        except Exception as e: log.debug('CFG', f"Load params: {e}")

    # =====================================================
    # ANOMALY MANAGEMENT (unchanged logic)
    # =====================================================
    def _add_anomaly(self, gw, dev, atype, value, ts=None):
        anomaly_id = f"{gw}_{dev}_{atype}".replace(' ', '_').lower()

        if atype == 'battery_ok':
            if self._is_auto_clear_enabled('low_battery'):
                self._remove_anomalies_for_device(gw, dev, ['low_battery', 'critical_battery'])
            if value is not None: self._update_device_state_value(gw, dev, 'battery', value)
            return
        if atype == 'device_online':
            if self._is_auto_clear_enabled('device_offline'):
                self._remove_anomalies_for_device(gw, dev, ['device_offline'])
                self.ha.pub_dev_available(gw, dev, True)
            return
        if atype == 'temp_ok':
            tc = [t for t in ['temp_high','temp_low'] if self._is_auto_clear_enabled(t)]
            if tc: self._remove_anomalies_for_device(gw, dev, tc)
            if value is not None: self._update_device_state_value(gw, dev, 'temperature', value)
            return
        if atype == 'hum_ok':
            hc = [t for t in ['hum_high','hum_low'] if self._is_auto_clear_enabled(t)]
            if hc: self._remove_anomalies_for_device(gw, dev, hc)
            if value is not None: self._update_device_state_value(gw, dev, 'humidity', value)
            return
        if atype == 'stagnation_clear':
            if self._is_auto_clear_enabled('stagnation'):
                self._remove_anomalies_for_device(gw, dev, ['stagnation'])
            return

        if atype in ['low_battery', 'critical_battery']:
            self._remove_anomalies_for_device(gw, dev, ['low_battery', 'critical_battery'])

        if ts is None: ts = int(now_ts())
        with self.lock:
            existing = next((a for a in self.anomaly_list if a['id'] == anomaly_id), None)
            if existing:
                # v37: Update value + re-publish (entity might not exist in HA)
                existing['value'] = value
                if self._startup_phase: self._dump_received_ids.add(anomaly_id)
                anomaly = existing
            else:
                anomaly = {"id": anomaly_id, "gw": gw, "dev": dev, "type": atype, "value": value,
                           "time": now_str(), "since_ts": ts,
                           "detected_at": datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')}
                self.anomaly_list.append(anomaly)
                if self._startup_phase: self._dump_received_ids.add(anomaly_id)
        # v37: ALWAYS register + publish (idempotent — ensures entity exists in HA)
        self.ha.reg_anomaly(anomaly_id, gw, dev, atype, value, anomaly.get('detected_at'))
        self.ha.pub_anomaly(anomaly)
        if not existing:
            self._save_anomaly_ids()
        self._publish_gw_stats(gw); self._publish_ha_anomaly_topics()
        if atype == 'device_offline':
            self.ha.pub_dev_available(gw, dev, False)
            self._publish_offline_values(gw, dev)
        vmap = {'low_battery':'battery','critical_battery':'battery','temp_high':'temperature','temp_low':'temperature','hum_high':'humidity','hum_low':'humidity'}
        sk = vmap.get(atype)
        if sk and value is not None: self._update_device_state_value(gw, dev, sk, value)
        log.info('ANOMALY', f"🔔 {gw}/{dev}: {atype}={value}")

    def _publish_offline_values(self, gw, dev):
        """v37: Publish state with availability=false but KEEP last known sensor values.
        Dashboard shows last readings + offline indicator instead of '--'."""
        sk = f"{gw}:{dev}"
        with self.lock:
            if sk not in self.device_states: self.device_states[sk] = {}
            cached = dict(self.device_states[sk])
        cached['last_seen'] = cached.get('last_seen', '--')
        self.ha.pub_dev_state(gw, dev, cached)

    def _remove_anomalies_for_device(self, gw, dev, types):
        with self.lock: to_rm = [a for a in self.anomaly_list if a['gw']==gw and a['dev']==dev and a['type'] in types]
        for a in to_rm:
            with self.lock: self.anomaly_list = [x for x in self.anomaly_list if x['id'] != a['id']]
            self.ha.remove_anomaly(a['id'])
            if a['type'] == 'device_offline': self.ha.pub_dev_available(gw, dev, True)
        if to_rm:
            self._save_anomaly_ids()  # v37: persist after auto-clear
            self._publish_gw_stats(gw); self._publish_ha_anomaly_topics()

    def _save_anomaly_ids(self):
        """v35: Persist anomaly IDs to disk for startup cleanup."""
        try:
            with self.lock: ids = [a['id'] for a in self.anomaly_list]
            with open('/tmp/lora_anomaly_ids.json', 'w') as f: json.dump(ids, f)
        except: pass

    def _clear_anomaly_if_exists(self, gw, dev, atype):
        """v33: Clear anomaly only if it exists — silent if not found."""
        aid = f"{gw}_{dev}_{atype}".replace(' ', '_').lower()
        with self.lock: found = any(a['id'] == aid for a in self.anomaly_list)
        if found: self._remove_anomalies_for_device(gw, dev, [atype])

    def _update_device_state_value(self, gw, dev, key, value):
        sk = f"{gw}:{dev}"
        with self.lock:
            if sk not in self.device_states: self.device_states[sk] = {}
            self.device_states[sk][key] = value
            cached = dict(self.device_states[sk])
        self.ha.pub_dev_state(gw, dev, cached)

    def _remove_anomaly_by_id(self, anomaly_id, send_clear=True):
        anomaly_id = anomaly_id.replace(' ', '_').lower()
        anomaly = None
        with self.lock:
            for a in list(self.anomaly_list):
                if a['id'] == anomaly_id: anomaly = a.copy(); self.anomaly_list.remove(a); break
        if anomaly:
            self.ha.remove_anomaly(anomaly_id)
            self._save_anomaly_ids()
            if anomaly['type'] == 'device_offline': self.ha.pub_dev_available(anomaly['gw'], anomaly['dev'], True)
            self._publish_gw_stats(anomaly['gw']); self._publish_ha_anomaly_topics()
            with self.lock:
                gwd = self._gw_devices.get(anomaly['gw'], set())
                exists = anomaly['dev'] in gwd if gwd else anomaly['dev'] in self.devices
            if send_clear and exists:
                self._add_queue({"t":"an_clr","g":anomaly['gw'],"d":anomaly['dev'],"a":anomaly['type']}, FlowPriority.SYSTEM)
            elif send_clear and not exists:
                log.info('ANOMALY', f"👻 Skip an_clr — {anomaly['dev']} gone")

    def _clear_anomalies_by_gateway(self, gw):
        with self.lock: to_clear = [a for a in self.anomaly_list if a['gw']==gw]
        self._batch_remove_anomalies(to_clear)

    def _clear_anomalies_by_type(self, types):
        if isinstance(types, str): types = [types]
        with self.lock: to_clear = [a for a in self.anomaly_list if a['type'] in types]
        self._batch_remove_anomalies(to_clear)

    def _clear_all_anomalies(self):
        with self.lock: to_clear = list(self.anomaly_list)
        self._batch_remove_anomalies(to_clear)

    def _clear_other_anomalies(self):
        skip = ['device_offline','low_battery','critical_battery']
        with self.lock: to_clear = [a for a in self.anomaly_list if a['type'] not in skip]
        self._batch_remove_anomalies(to_clear)

    def _batch_remove_anomalies(self, anomalies):
        """v5.0: Remove multiple anomalies and send batch clear (ac_b) instead of N individual an_clr.
        Groups clears by gateway → one ac_b packet per gateway."""
        if not anomalies: return
        # Group by gateway
        by_gw = {}
        for a in anomalies:
            gw = a['gw']
            if gw not in by_gw: by_gw[gw] = []
            by_gw[gw].append(a)
        # Remove locally + collect LoRa clears
        for a in anomalies:
            with self.lock: self.anomaly_list = [x for x in self.anomaly_list if x['id'] != a['id']]
            self.ha.remove_anomaly(a['id'])
            if a['type'] == 'device_offline': self.ha.pub_dev_available(a['gw'], a['dev'], True)
        # Send batch clear per gateway
        for gw, gw_anoms in by_gw.items():
            # Check which devices still exist (skip ghost clears)
            gwd = self._gw_devices.get(gw, set())
            items = []
            for a in gw_anoms:
                exists = a['dev'] in gwd if gwd else a['dev'] in self.devices
                if exists:
                    items.append([a['dev'], a['type']])
                else:
                    log.info('ANOMALY', f"👻 Skip clear — {a['dev']} gone")
            if items:
                # Split into 220B packets if needed
                max_sz = CONFIG['lora']['max_size']
                packets = []
                current = []
                for item in items:
                    test = current + [item]
                    test_s = json.dumps({"t":"ac_b","g":gw,"d":test}, separators=(',',':'))
                    if len(test_s) > max_sz and current:
                        packets.append({"t":"ac_b","g":gw,"d":current})
                        current = [item]
                    else:
                        current = test
                if current:
                    packets.append({"t":"ac_b","g":gw,"d":current})
                for pkt in packets:
                    self._add_queue(pkt, FlowPriority.SYSTEM)
                log.info('ANOMALY', f"🔄 Batch clear {gw}: {len(items)} items → {len(packets)} ac_b packet(s)")
        # Update stats
        self._save_anomaly_ids()  # v37: persist after batch clear
        gws = {a['gw'] for a in anomalies}
        for gw in gws: self._publish_gw_stats(gw)
        self._publish_ha_anomaly_topics()

    # --- Ghost anomaly prune ---
    def _prune_anomalies(self):
        has_data = any(len(d) > 0 for d in self._gw_devices.values())
        if not has_data:
            log.warn('ANOMALY', "👻 Prune: no discovery data — run Discovery first"); return
        with self.lock:
            orphans = [a for a in self.anomaly_list
                       if self._gw_devices.get(a['gw'], set()) and a['dev'] not in self._gw_devices.get(a['gw'], set())]
        if not orphans:
            log.info('ANOMALY', "👻 Prune: no ghosts found"); return
        log.info('ANOMALY', f"👻 Pruning {len(orphans)} ghost anomalies")
        for a in orphans:
            with self.lock: self.anomaly_list = [x for x in self.anomaly_list if x['id'] != a['id']]
            self.ha.remove_anomaly(a['id'])
            log.info('ANOMALY', f"👻 Pruned: {a['gw']}/{a['dev']}/{a['type']}")
        gws = {a['gw'] for a in orphans}
        for gw in gws: self._publish_gw_stats(gw)
        self._publish_ha_anomaly_topics()

    # --- Stats ---
    def _get_gw_anomaly_counts(self, gw):
        with self.lock: ga = [a for a in self.anomaly_list if a['gw']==gw]
        off = sum(1 for a in ga if a['type']=='device_offline')
        bat = sum(1 for a in ga if a['type'] in ['low_battery','critical_battery'])
        return off, bat, len(ga)-off-bat

    def _publish_gw_stats(self, gw):
        off, bat, oth = self._get_gw_anomaly_counts(gw)
        with self.lock:
            g = self.gateways[gw]
            rssi_v = g.get('rssi', -999)
            data = {"state":"online" if g['online'] else "offline", "uptime":g['uptime'],
                    "last_seen":g['last_seen'] or "--", "devices_total":g['devices_total'],
                    "devices_monitored":g['devices_monitored'],
                    "devices_priority": g.get('devices_priority', 0),
                    "devices_offline":off,
                    "devices_low_battery":bat, "devices_anomaly":oth,
                    "airtime": g.get('airtime', 0),
                    "rssi": rssi_v if rssi_v > -999 else None,
                    "disc_hash": g.get('disc_hash', '--'),
                    "z2m_age": g.get('z2m_age', -1),
                    "z2m_stale": g.get('z2m_stale', False),
                    "batch_latency": g.get('batch_latency', 0),
                    "context_mode": g.get('context_mode', '?'),
                    "sync_age": g.get('sync_age', -1),
                    "time_offset": g.get('time_offset', 0),
                    "queue_depth": g.get('queue_depth', 0),
                    "sup_contact": g.get('sup_contact', -1),
                    }
        self.ha.pub_gw_status(gw, data)

    def _publish_ha_anomaly_topics(self):
        with self.lock: anom = list(self.anomaly_list)
        counts = {"offline":0,"battery_low":0,"battery_critical":0,"stagnation":0,"smoke":0,"water_leak":0,"other":0}
        km = {"device_offline":"offline","low_battery":"battery_low","critical_battery":"battery_critical","stagnation":"stagnation"}
        for a in anom: counts[km.get(a['type'],"other")] += 1
        self.mqtt_client.publish("ha/lora/anomaly_counts", json.dumps(counts), retain=True)
        active = [{"gw":a['gw'],"device":a['dev'],"kind":a['type'],
                   "level":"critical" if a['type'] in ['smoke','water_leak','critical_battery'] else "warning",
                   "value":a.get('value'),"since":a.get('since_ts',0)} for a in anom]
        self.mqtt_client.publish("ha/lora/anomaly_list", json.dumps({"ts":int(now_ts()),"active":active}), retain=True)
        # v37: Per-gateway anomaly lists (dashboard reads from sensor attributes)
        pf = CONFIG['state_prefix']
        offline_types = {'device_offline'}
        battery_types = {'low_battery', 'critical_battery'}
        for gw in CONFIG['gateways']:
            gl = gw.lower()
            gw_anom = [a for a in anom if a['gw'] == gw]
            off_items = [{"id":a['id'],"dev":a['dev'],"type":a['type'],"value":a.get('value'),
                          "detected_at":a.get('detected_at','--')} for a in gw_anom if a['type'] in offline_types]
            bat_items = [{"id":a['id'],"dev":a['dev'],"type":a['type'],"value":a.get('value'),
                          "detected_at":a.get('detected_at','--')} for a in gw_anom if a['type'] in battery_types]
            oth_items = [{"id":a['id'],"dev":a['dev'],"type":a['type'],"value":a.get('value'),
                          "detected_at":a.get('detected_at','--')} for a in gw_anom if a['type'] not in offline_types | battery_types]
            self.mqtt_client.publish(f"{pf}/gw/{gl}/anomaly_offline", json.dumps({"count":len(off_items),"items":off_items}), retain=True)
            self.mqtt_client.publish(f"{pf}/gw/{gl}/anomaly_battery", json.dumps({"count":len(bat_items),"items":bat_items}), retain=True)
            self.mqtt_client.publish(f"{pf}/gw/{gl}/anomaly_other", json.dumps({"count":len(oth_items),"items":oth_items}), retain=True)

    # =====================================================
    # REQ-5: Gateway offline → ALL devices offline (unchanged)
    # =====================================================
    def _gateway_went_offline(self, gw):
        """v33: Gateway offline — mark devices unavailable + single gateway anomaly."""
        log.warn('PING', f"💀 {gw} OFFLINE — marking all devices unavailable")
        self._mark_gw_devices_unavailable(gw)
        self._add_anomaly(gw, f"gateway_{gw}", 'gateway_offline', f"{len(self._gw_devices.get(gw, set()))} devices", int(now_ts()))
        self._publish_gw_stats(gw)

    def _mark_gw_devices_unavailable(self, gw):
        """v35: Mark ALL devices of a gateway as unavailable in HA."""
        with self.lock:
            devs = [(dev, info) for dev, info in self.devices.items() if info.get('gateway') == gw]
        for dev, info in devs:
            self.ha.pub_dev_available(gw, dev, False)
            self._publish_offline_values(gw, dev)

    def _startup_mark_all_offline(self):
        """v37: On supervisor restart, mark ALL gateways offline + ALL known devices unavailable.
        Loads saved device list from disk (self.devices is empty at startup)."""
        # v37: Load saved device list from previous run
        dev_file = '/tmp/lora_gw_devices.json'
        saved_devs = {}
        try:
            with open(dev_file) as f: saved_devs = json.load(f)
        except: pass
        for gw in CONFIG['gateways']:
            gl = gw.lower()
            self.ha.pub_gw_status(gw, {
                "uptime": 0, "last_seen": "--",
                "devices_total": 0, "devices_monitored": 0, "devices_priority": 0,
                "devices_offline": 0, "devices_low_battery": 0, "devices_anomaly": 0,
                "airtime": 0, "rssi": "--", "disc_hash": "--",
                "z2m_age": -1, "z2m_stale": False, "batch_latency": 0,
                "context_mode": "?", "sync_age": -1, "time_offset": 0, "queue_depth": 0,
            })
            self.mqtt_client.publish(f"{CONFIG['state_prefix']}/gw/{gl}/status_bin", "OFF", retain=True)
            # v37: Use saved device list (self.devices is empty at this point)
            gw_dev_list = saved_devs.get(gw, [])
            for dev in gw_dev_list:
                self.ha.pub_dev_available(gw, dev, False)
            if gw_dev_list:
                log.info('MAIN', f"🔄 {gw}: {len(gw_dev_list)} devices marked unavailable (from saved list)")
        # Clear stale anomaly entities
        anom_file = '/tmp/lora_anomaly_ids.json'
        stale_ids = set()
        try:
            with open(anom_file) as f: stale_ids = set(json.load(f))
        except: pass
        with self.lock:
            for a in list(self.anomaly_list): stale_ids.add(a['id'])
            self.anomaly_list.clear()
        for aid in stale_ids:
            self.ha.remove_anomaly(aid)
        if stale_ids:
            log.info('MAIN', f"🧹 Cleared {len(stale_ids)} stale anomaly entities")
        try:
            with open(anom_file, 'w') as f: json.dump([], f)
        except: pass
        log.info('MAIN', f"🔄 Startup: all {len(CONFIG['gateways'])} gateways marked offline")

    def _save_gw_devices(self):
        """v37: Persist device list per gateway for startup cleanup."""
        try:
            data = {gw: list(devs) for gw, devs in self._gw_devices.items()}
            with open('/tmp/lora_gw_devices.json', 'w') as f: json.dump(data, f)
        except: pass

    def _gateway_came_online(self, gw):
        """v37: Gateway online — staggered: cfg → wait → disc → wait → dump_anom.
        Each command separated by tx_cooldown to avoid burst."""
        def _staggered():
            log.info('PING', f"✅ {gw} ONLINE — staggered init (cfg → disc → dump)")
            self._send_cfg(gw)
            time.sleep(5)
            self._send_discovery(gw, force=True)
            time.sleep(10)  # Wait for discovery to complete
            self._add_queue({"t": "dump_anom", "g": gw}, FlowPriority.SYSTEM)
        threading.Thread(target=_staggered, daemon=True).start()

    # =====================================================
    # VIRTUAL BUTTONS (unchanged)
    # =====================================================
    def _next_seq(self):
        """Get next command sequence number (monotonically increasing)."""
        self._cmd_seq += 1
        return self._cmd_seq

    def _send_virtual_button(self, gw, vid, action="single"):
        """Send virtual button press to gateway via LoRa."""
        msg = {"t":"vbtn","g":gw,"id":vid,"act":action,"seq":self._next_seq()}
        self._add_queue(msg, FlowPriority.SYSTEM)
        log.info('VBTN', f"🔘 {gw}/{vid} → {action}")

    def _send_vswitch(self, gw, vid, val):
        """Send virtual switch command to gateway via LoRa."""
        msg = {"t":"vsw","g":gw,"id":vid,"v":val,"seq":self._next_seq()}
        self._add_queue(msg, FlowPriority.SYSTEM)
        log.info('CFG', f"🔘 VSwitch → {gw}/{vid} = {'ON' if val else 'OFF'}")

    # =====================================================
    # DUMP-BASED STALE CLEANUP (unchanged)
    # =====================================================
    def _start_dump_cleanup(self):
        self._dump_start_ts = now_ts(); self._dump_received_ids.clear()
        self._send_dump_anom_all()
        def _finish():
            time.sleep(20); self._startup_phase = False
            with self.lock: active = {a['id'].replace(' ','_').lower() for a in self.anomaly_list}
            stale = self._retained_anomaly_ids - self._dump_received_ids - active
            if stale:
                log.info('ANOMALY', f"🧹 Cleaning {len(stale)} stale entities")
                p, pf = CONFIG['ha_prefix'], CONFIG['state_prefix']
                for sid in stale:
                    # v27: Clean ALL related topics — config + state + attributes
                    for t in [f"{p}/sensor/lora_an_{sid}/config",
                              f"{p}/button/lora_an_{sid}_clear/config",
                              f"{pf}/anomaly/{sid}",
                              f"{p}/sensor/lora_an_{sid}/state"]:
                        self.mqtt_client.publish(t, "", retain=True)
            self._retained_anomaly_ids.clear(); self._dump_received_ids.clear()
            self._prune_anomalies()
            # v27: Aggressive scan — clean any anomaly topic not in active list
            self._scan_and_clean_zombie_anomalies(active)
        threading.Thread(target=_finish, daemon=True).start()

    def _scan_and_clean_zombie_anomalies(self, active_ids):
        """v27: Scan retained MQTT anomaly topics and clean zombies.
        Publishes empty retained payloads for any anomaly entity not in active list."""
        p, pf = CONFIG['ha_prefix'], CONFIG['state_prefix']
        cleaned = 0
        # Check all known anomaly patterns for each gateway
        for gw in CONFIG['gateways']:
            gw_lower = gw.lower()
            # Check device registry for known devices
            for dev_name in list(self.devices.keys()):
                dev_safe = dev_name.replace(' ', '_').lower()
                for atype in ['device_offline', 'low_battery', 'critical_battery',
                              'temp_high', 'temp_low', 'hum_high', 'hum_low',
                              'stagnation', 'smoke', 'water_leak']:
                    aid = f"{gw_lower}_{dev_safe}_{atype}"
                    if aid not in active_ids:
                        # Not active — ensure it's cleaned
                        for t in [f"{p}/sensor/lora_an_{aid}/config",
                                  f"{p}/button/lora_an_{aid}_clear/config",
                                  f"{pf}/anomaly/{aid}"]:
                            self.mqtt_client.publish(t, "", retain=True)
                        cleaned += 1
        if cleaned > 0:
            log.info('ANOMALY', f"🧹 Aggressive cleanup: cleared {cleaned} potential zombie topics")

    # =====================================================
    # CALENDAR STATUS (unchanged)
    # =====================================================
    def _publish_calendar_status(self):
        status = {}
        for gw in CONFIG['gateways']:
            mode, nxt, src = self.scheduler.compute_now_and_next(gw)
            status[gw] = {"mode": mode, "next_ts": nxt, "src": src}
        all_data = self.scheduler.get_all()
        self.mqtt_client.publish("ha/lora/schedule/status", json.dumps({
            "ts": int(now_ts()), "gateways": status,
            "global_slots": len(all_data.get('GLOBAL', [])),
            **{gw: len(all_data.get(gw, [])) for gw in CONFIG['gateways']}
        }), retain=True)
        self.mqtt_client.publish("ha/lora/schedule/list", json.dumps({"ts":int(now_ts()),"schedules":all_data}), retain=True)
        # NO write-back to HA calendar — HA Local Calendar is the source of truth

    def _handle_recurring_schedule(self, data):
        """Generate multiple slots from a recurring pattern.
        Payload: {
            "target": "GLOBAL",
            "time_start": "06:00",
            "time_end": "22:00",
            "days": [1,2,3,4,5],    // 1=Mon..7=Sun (ISO weekday)
            "weeks": 4,             // how many weeks ahead
            "mode": 1,
            "note": "Produkcja"
        }
        """
        target = data.get('target', 'GLOBAL')
        time_start = data.get('time_start', '06:00')
        time_end = data.get('time_end', '22:00')
        days = data.get('days', [1,2,3,4,5])  # default Mon-Fri
        weeks = min(data.get('weeks', 4), 52)  # max 1 year
        mode = data.get('mode', 1)
        note = data.get('note', 'Produkcja' if mode == 1 else 'Przerwa')
        from datetime import timedelta
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        count = 0
        for day_offset in range(weeks * 7):
            d = today + timedelta(days=day_offset)
            # Python isoweekday: 1=Mon..7=Sun — matches our format
            if d.isoweekday() in days:
                start = f"{d.strftime('%Y-%m-%d')} {time_start}:00"
                end = f"{d.strftime('%Y-%m-%d')} {time_end}:00"
                self.scheduler.add_slot(target, start, end, mode, note[:30])
                count += 1
        self._publish_calendar_status()
        self._publish_calendar_status()
        log.info('CAL', f"📅 Recurring: {count} slots ({note}) {time_start}-{time_end} days={days} weeks={weeks} → {target}")

    # =====================================================
    # CALENDAR SYNC — via CalendarTransfer (CRC + retransmit, PROVEN)
    # =====================================================
    def _targeted_calendar_sync(self, gateways=None):
        """Send compact schedule to gateways via CalendarTransfer.
        Override: if GW in gateway_override → use calendar.lora_{gw} data (from schedules.json[GW])
        Default: use prepare_compact(gw) = merge GLOBAL + GW"""
        enabled = CONFIG.get('calendar', {}).get('enabled_gateways', CONFIG['gateways'])
        overrides = CONFIG.get('gateway_override', [])
        targets = gateways or enabled

        def _sync_thread():
            for gw in targets:
                if gw not in enabled:
                    log.info('SYNC', f"⏭️ {gw} excluded"); continue
                with self.lock:
                    if not self.gateways.get(gw, {}).get('online', False):
                        log.warn('SYNC', f"⏭️ {gw} offline"); continue
                if gw in overrides:
                    # Override: send GW-specific calendar (NOT global)
                    gw_slots = self.scheduler.list_slots(gw)
                    if not gw_slots:
                        log.info('SYNC', f"📅 {gw} override but no GW slots"); continue
                    compact = self.scheduler._to_compact(gw_slots)
                    h = hashlib.sha256(json.dumps(compact, separators=(',',':')).encode()).hexdigest()[:12]
                    log.info('SYNC', f"📤 {gw} OVERRIDE: {len(compact)} slots (hash={h})")
                else:
                    # Default: merge GLOBAL + GW
                    compact, h = self.scheduler.prepare_compact(gw)
                if not compact:
                    log.info('SYNC', f"📅 {gw} — no events"); continue
                log.info('SYNC', f"📤 {gw}: sending {len(compact)} slots (hash={h})")
                self.cal_transfer.start_send(gw, "cal", compact)
                # v33: Store expected hash — compare with gateway's reported hash
                self._gw_expected_cal_hash[gw] = h
                time.sleep(15)
            log.info('SYNC', f"🏁 Sync complete for {targets}")

        threading.Thread(target=_sync_thread, daemon=True, name="cal-sync").start()

    def _sync_to_ha_calendar(self, gw, full_slots):
        """Write slots to HA Local Calendar via REST API.
        Writes to calendar.lora_{gw} on supervisor HA.
        Called after gw_push (reverse sync: gateway → supervisor)."""
        ha = CONFIG.get('ha_api', {})
        token = ha.get('token', '')
        if not token:
            log.info('CAL', f"📅 {gw} → MQTT only (no HA token)")
            return
        base_url = ha.get('url', 'http://localhost:8123').rstrip('/')
        cal_entity = f"calendar.lora_{gw.lower()}"
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})

        def _do():
            try:
                # 1. GET existing events
                from datetime import timedelta
                now = datetime.now()
                start_s = now.strftime('%Y-%m-%dT00:00:00')
                end_s = (now + timedelta(days=90)).strftime('%Y-%m-%dT23:59:59')
                req = urllib.request.Request(
                    f"{base_url}/api/calendars/{cal_entity}?start={start_s}&end={end_s}", headers=headers)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    existing = json.loads(resp.read())

                # 2. DELETE old events
                deleted = 0
                for evt in existing:
                    uid = evt.get('uid', '')
                    if not uid: continue
                    try:
                        dr = urllib.request.Request(
                            f"{base_url}/api/calendars/{cal_entity}/{uid}", method='DELETE', headers=headers)
                        urllib.request.urlopen(dr, timeout=5)
                        deleted += 1
                    except: pass

                # 3. POST new events
                created = 0
                for slot in full_slots:
                    mode = slot.get('mode', 0)
                    body = json.dumps({
                        "summary": mode_names.get(mode, f"MODE_{mode}"),
                        "dtstart": slot.get('start', '').replace(' ', 'T'),
                        "dtend": slot.get('end', '').replace(' ', 'T'),
                    }).encode()
                    try:
                        pr = urllib.request.Request(
                            f"{base_url}/api/calendars/{cal_entity}", data=body, headers=headers, method='POST')
                        urllib.request.urlopen(pr, timeout=5)
                        created += 1
                    except Exception as e:
                        log.warn('CAL', f"POST {cal_entity}: {e}")
                log.info('CAL', f"✅ HA API: {cal_entity} — deleted {deleted}, created {created}")
            except Exception as e:
                log.error('CAL', f"❌ HA API {cal_entity}: {e}")

        threading.Thread(target=_do, daemon=True, name=f"ha-cal-{gw}").start()

    def fetch_ha_calendar(self, calendar_id='calendar.lora_global', target_gw='GLOBAL'):
        """v5.1 Placeholder: Fetch events from HA REST API.
        Requires ha_api.token in CONFIG. Returns list of slot dicts.
        Override or complete when HA long-lived token is configured."""
        api = CONFIG.get('ha_api', {})
        url = api.get('url', 'http://localhost:8123')
        token = api.get('token', '')
        if not token:
            log.warn('CAL', f"⚠️ HA API token not configured — cannot fetch {calendar_id}")
            return []
        try:
            import urllib.request
            now = datetime.now()
            start = now.strftime('%Y-%m-%dT00:00:00')
            end = (now + __import__('datetime').timedelta(days=90)).strftime('%Y-%m-%dT23:59:59')
            req_url = f"{url}/api/calendars/{calendar_id}?start={start}&end={end}"
            req = urllib.request.Request(req_url, headers={
                'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                events = json.loads(resp.read())
            slots = []
            for evt in events:
                mode = 1 if '🟢' in evt.get('summary', '') or 'PRODUKCJA' in evt.get('summary', '').upper() else 0
                slots.append({
                    "id": evt.get('uid', f"ha_{len(slots)}"),
                    "start": evt.get('start', {}).get('dateTime', evt.get('start', {}).get('date', '')),
                    "end": evt.get('end', {}).get('dateTime', evt.get('end', {}).get('date', '')),
                    "mode": mode,
                    "note": evt.get('summary', '')[:30],
                    "updated_ts": int(now_ts()),
                    "origin": "ha_api"
                })
            log.info('CAL', f"📅 Fetched {len(slots)} events from HA API ({calendar_id})")
            return slots
        except Exception as e:
            log.error('CAL', f"❌ HA API fetch error: {e}")
            return []

    # =====================================================
    # WATCHDOG + SCHEDULE + HEARTBEAT LOOPS
    # =====================================================
    def _watchdog_loop(self):
        """v33: Periodic housekeeping — stats, prune, antenna, pending pings.
        Offline detection moved to heartbeat_loop (2.5× interval → ping → retry → offline)."""
        orphan_ctr = 0; ant_ctr = 0
        while self.running:
            time.sleep(30)
            for gw in self.gateways: self._publish_gw_stats(gw)
            orphan_ctr += 1
            if orphan_ctr >= 10:
                orphan_ctr = 0
                if not self._startup_phase: self._prune_anomalies()
            self.cal_transfer.cleanup_stale()
            ant_ctr += 1
            if ant_ctr >= 4:
                ant_ctr = 0; self._publish_antenna_status()
            # v27: Ping retry — check for expired pings, retry or mark offline
            self._check_pending_pings()

    def _check_pending_pings(self):
        """v27: Check for pings without pong response. Retry or mark gateway offline."""
        now = time.time()
        expired = []
        with self.lock:
            for gw, info in list(self._pending_pings.items()):
                if now - info['ts'] > self._ping_retry_timeout:
                    expired.append((gw, info['retries']))
        for gw, retries in expired:
            if retries >= self._ping_retry_max:
                # Max retries exhausted → mark offline
                with self.lock:
                    del self._pending_pings[gw]
                    if self.gateways[gw]['online']:
                        self.gateways[gw]['online'] = False
                        log.error('PING', f"🏓❌ {gw} no PONG after {retries} retries — marking OFFLINE")
                        threading.Thread(target=self._gateway_went_offline, args=(gw,), daemon=True).start()
                    else:
                        # v35: Already offline — ensure devices still unavailable (covers restart edge case)
                        self._mark_gw_devices_unavailable(gw)
                        log.warn('PING', f"🏓⚠️ {gw} no PONG (already offline — devices confirmed unavailable)")
            else:
                # Retry ping
                log.warn('PING', f"🏓🔄 {gw} no PONG ({retries+1}/{self._ping_retry_max}) — retrying")
                self._send_ping(gw)

    def _schedule_loop(self):
        """v6.3 [M1, M2]: Monitor schedule for mode transitions.
        - Poll every 1s (was 30s) to enable precise pre_notify
        - pre_notify 5s before mode change → gateway can prepare
        - Correctly handles all modes: PRODUKCJA(1), PRZERWA(2), SERWIS(3)
        - Mode transition sch packet sent ONLY when mode actually changes
        """
        last_modes = {}
        pre_notified = {}  # {gw: next_ts} — track which transitions we pre-notified
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})

        while self.running:
            time.sleep(1)
            now = now_ts()
            pre_secs = CONFIG.get('production_schedule', {}).get('pre_notify_seconds', [5])
            for gw in CONFIG['gateways']:
                mode, nxt, src = self.scheduler.compute_now_and_next(gw)
                prev = last_modes.get(gw)

                # v33: pre_notify at each threshold from CONFIG
                if nxt > 0:
                    remaining = nxt - now
                    for sec in pre_secs:
                        if 0 < remaining <= sec:
                            notify_key = f"{gw}_{nxt}_{sec}"
                            if notify_key not in pre_notified:
                                pre_notified[notify_key] = True
                                next_mode = 0
                                for s in self.scheduler.get_effective_schedule(gw):
                                    try:
                                        st = self.scheduler._parse_ts(s['start'])
                                        if abs(st - nxt) < 60:
                                            next_mode = s.get('mode', 1); break
                                    except: continue
                                next_name = mode_names.get(next_mode, f"MODE_{next_mode}")
                                self._add_queue({
                                    "t": "cal_pre", "g": gw, "mode": next_mode,
                                    "in_sec": int(remaining), "next_ts": int(nxt)
                                }, FlowPriority.SYSTEM)
                                log.info('CAL', f"📅 Pre-notify {gw}: {next_name} in {int(remaining)}s (threshold={sec}s)")

                # Mode ACTUALLY changed — send transition notification
                if prev is not None and prev != mode:
                    cur_name = mode_names.get(mode, f"MODE_{mode}")
                    prev_name = mode_names.get(prev, f"MODE_{prev}")
                    self._add_queue({
                        "t": "cal_mode", "g": gw, "mode": mode,
                        "from_ts": int(now_ts()), "next_ts": nxt, "src": src
                    }, FlowPriority.SYSTEM)
                    log.info('CAL', f"📅 Mode transition {gw}: {prev_name}({prev})→{cur_name}({mode})")
                    self._publish_calendar_status()

                last_modes[gw] = mode

    # =====================================================
    # v26: TIME + CONTEXT SYNC
    # =====================================================
    def _send_sync(self, target_gw=None):
        """v26: Broadcast time + sun state to all gateways (or specific one).
        Packet: {"t":"sync","sec":<unix_ts>,"sun":"day"|"night","seq":<int>}"""
        self._sync_seq += 1
        msg = {
            "t": "sync",
            "sec": int(time.time()),
            "sun": self._sun_state,
            "seq": self._sync_seq,
        }
        if target_gw:
            msg["g"] = target_gw
        self._add_queue(msg, FlowPriority.SYSTEM)
        self._last_sync_broadcast = time.time()
        sun_icon = '☀️' if self._sun_state == 'day' else '🌙'
        tgt = f" → {target_gw}" if target_gw else " (broadcast)"
        log.info('SYNC', f"🔄 {sun_icon} Sync TX{tgt}: ts={msg['sec']} sun={self._sun_state} seq={self._sync_seq}")

    def _sync_broadcast_loop(self):
        """v37: Periodic sync broadcast. First sync delayed 60s to avoid startup burst."""
        sync_interval = CONFIG.get('sync', {}).get('interval', 1200)
        time.sleep(60)  # v37: delayed startup (was 10s)
        self._poll_sun_state()
        self._send_sync()
        while self.running:
            time.sleep(60)
            self._poll_sun_state()
            if time.time() - self._last_sync_broadcast >= sync_interval:
                self._send_sync()

    def _poll_sun_state(self):
        """v26: Check HA sun.sun entity via REST API. Update _sun_state if changed."""
        api = CONFIG.get('ha_api', {})
        token = api.get('token', '')
        url = api.get('url', 'http://localhost:8123').rstrip('/')
        sun_entity = CONFIG.get('sync', {}).get('sun_entity', 'sun.sun')
        if not token: return
        try:
            req = urllib.request.Request(
                f"{url}/api/states/{sun_entity}",
                headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            ha_state = data.get('state', 'above_horizon')
            new_sun = 'night' if ha_state == 'below_horizon' else 'day'
            if new_sun != self._sun_state:
                self._handle_sun_change(new_sun)
        except Exception as e:
            log.debug('SYNC', f"🔄 Sun poll failed: {e}")

    def _handle_sync_request(self, data):
        """v26: Gateway requests sync (s_req). Respond with targeted sync."""
        gw = data.get('g')
        if gw:
            log.info('SYNC', f"🔄 s_req from {gw} — sending targeted sync")
            self._send_sync(target_gw=gw)
        else:
            self._send_sync()

    def _handle_sun_change(self, new_state):
        """v26: Called when sun.sun changes in HA → immediate sync broadcast."""
        old = self._sun_state
        self._sun_state = new_state
        if old != new_state:
            sun_icon = '☀️' if new_state == 'day' else '🌙'
            log.info('SYNC', f"🔄 {sun_icon} Sun state changed: {old} → {new_state} — broadcasting sync")
            self._send_sync()

    def _heartbeat_loop(self):
        """v35: HB timeout + periodic cal sync + periodic dump_anom reconciliation."""
        _cal_check_ts = 0
        _dump_anom_ts = now_ts()  # v37: start from now, not 0 (prevent immediate fire)
        while self.running:
            time.sleep(60)
            now = now_ts()
            hb_interval = CONFIG.get('heartbeat_interval', 1200)
            timeout = hb_interval * 2.5

            for gw, info in self.gateways.items():
                with self.lock:
                    ls_ts = info.get('last_seen_ts', 0)
                    is_online = info.get('online', False)

                # HB timeout → fallback ping
                if is_online and ls_ts > 0 and now - ls_ts > timeout:
                    log.warn('PING', f"🏓 {gw} no HB for {int(now - ls_ts)}s (>{int(timeout)}s) — fallback PING")
                    self._send_ping(gw)

                # Online but no discovery after 5 min → force disc
                if is_online and ls_ts > 0 and now - ls_ts < timeout:
                    gw_devs = self._gw_devices.get(gw, set())
                    if len(gw_devs) == 0 and now - ls_ts > 300:
                        log.warn('DISC', f"🔭 {gw} online but 0 devices after 5min — forcing discovery")
                        self._send_discovery(gw, force=True)

            # v35: Periodic dump_anom every 30 min — reconcile lost packets
            if now - _dump_anom_ts > 1800:
                _dump_anom_ts = now
                for gw, info in self.gateways.items():
                    with self.lock: is_online = info.get('online', False)
                    if is_online:
                        self._add_queue({"t": "dump_anom", "g": gw}, FlowPriority.SYSTEM)
                        log.info('ANOMALY', f"📋 Periodic dump_anom → {gw}")

            # Periodic calendar sync check every 6h
            if now - _cal_check_ts > 21600:
                _cal_check_ts = now
                for gw, info in self.gateways.items():
                    with self.lock: is_online = info.get('online', False)
                    if not is_online: continue
                    gw_cal = self._gw_cal_hash.get(gw, '')
                    if not gw_cal:
                        log.info('CAL', f"📅 {gw} cal hash empty — scheduling periodic sync")
                        self._targeted_calendar_sync([gw])

    # =====================================================
    # TX QUEUE + LOOP
    # =====================================================
    def _add_queue(self, msg, priority): self.queue[priority].append(msg)

    def _send_lora(self, msg, priority=None):
        """v6.0: Returns True on success, False on failure (for H2 Zero Loss)."""
        try:
            s = json.dumps(msg, separators=(',',':'))
            max_sz = CONFIG['lora']['max_size']
            if len(s) > max_sz:
                log.error('LORA', f"❌ Packet too big ({len(s)}B > {max_sz}B) — DROPPED: {s[:80]}...")
                return False
            plabel = PRIORITY_LABELS.get(priority, "[P?]") if priority is not None else ""
            log.info('LORA', f"📤 TX {plabel} ({len(s)}B): {s[:120]}")
            if self.antenna_mgr:
                self.antenna_mgr.send_text(s)
            self.last_tx = time.time()
            return True
        except Exception as e:
            log.error('LORA', f"TX error: {e}")
            return False

    def _send_lora_to(self, gw, msg):
        """v5.1: Targeted unicast send to specific gateway via routed antenna."""
        try:
            s = json.dumps(msg, separators=(',',':'))
            max_sz = CONFIG['lora']['max_size']
            if len(s) > max_sz:
                log.error('LORA', f"❌ Packet too big ({len(s)}B > {max_sz}B) → {gw} — DROPPED: {s[:80]}...")
                return
            log.info('LORA', f"📤 TX → {gw} ({len(s)}B): {s[:120]}")
            if self.antenna_mgr:
                self.antenna_mgr.send_text_to(gw, s)
            self.last_tx = time.time()
        except Exception as e: log.error('LORA', f"TX → {gw} error: {e}")

    def _flush_gw_pending_cmds(self, gw):
        """v37: Safe window — gateway just RX'd, send any queued cmds for it NOW.
        Extracts gw-targeted msgs from P0 queue and sends with 0.5s delay."""
        extracted = []
        with self.lock:
            new_q = deque(maxlen=200)
            for msg in self.queue[FlowPriority.SYSTEM]:
                if isinstance(msg, dict) and msg.get('g') == gw and msg.get('t') in ('cmd','vsw','params','cfg','disc','dump_anom','ping','sync'):
                    extracted.append(msg)
                else:
                    new_q.append(msg)
            self.queue[FlowPriority.SYSTEM] = new_q
        if not extracted: return
        def _send_delayed():
            time.sleep(0.5)  # Let mesh settle after RX
            for msg in extracted:
                cooldown = CONFIG['lora']['tx_cooldown']
                while time.time() - self.last_tx < cooldown:
                    time.sleep(0.1)
                self._send_lora(msg, priority=FlowPriority.SYSTEM)
        threading.Thread(target=_send_delayed, daemon=True).start()
        log.debug('LORA', f"📤 Safe window {gw}: {len(extracted)} cmds queued for immediate TX")

    def _send_ping(self, gw=None):
        msg = {"t":"ping"}
        if gw:
            msg["g"] = gw
            # v27: Track pending ping for retry/offline detection
            with self.lock:
                if gw not in self._pending_pings:
                    self._pending_pings[gw] = {"ts": time.time(), "retries": 0}
                else:
                    self._pending_pings[gw]["ts"] = time.time()
                    self._pending_pings[gw]["retries"] += 1
        self._add_queue(msg, FlowPriority.SYSTEM)

    def _send_discovery(self, gw=None, force=False):
        """Send discovery request. force=True skips hash. 30s cooldown per gateway."""
        target = gw or 'ALL'
        # v37: Cooldown — max 1 disc per gateway per 30s
        cooldown_key = f'_disc_cooldown_{target}'
        last = getattr(self, cooldown_key, 0)
        if time.time() - last < 30:
            log.debug('DISC', f"🔭 {target} disc cooldown (skip)")
            return
        setattr(self, cooldown_key, time.time())
        msg = {"t":"disc"}
        if gw:
            msg["g"] = gw
            if not force:
                known_hash = self._gw_disc_hash.get(gw, '')
                if known_hash: msg["hash"] = known_hash
            self._disc_refresh_gw.add(gw)
        else:
            self._disc_refresh_gw.update(CONFIG['gateways'])
        self._add_queue(msg, FlowPriority.SYSTEM)  # v37: P0 not P2

    def _send_cfg(self, gw=None):
        msg = {"t":"cfg","to":{"sw":CONFIG['timeout']['switch'],"lt":CONFIG['timeout']['light'],
                               "sn":CONFIG['timeout']['sensor'],"bs":CONFIG['timeout']['binary_sensor']}}
        if gw: msg["g"]=gw
        self._add_queue(msg, FlowPriority.SYSTEM)
        tgt = f" → {gw}" if gw else " (broadcast)"
        log.info('CFG', f"⚙️ Config sent{tgt}")

    def _send_params(self, gw=None, params=None):
        """v30: Send custom params to gateway(s).
        If params not given, reads stored values from _gw_params (dashboard edits).
        Per-gateway: reads that gateway's stored values.
        Broadcast: reads default values from CONFIG."""
        msg = {"t": "params"}
        if gw:
            msg["g"] = gw
            if params is None:
                params = self._gw_params.get(gw, {})
        else:
            if params is None:
                # Broadcast: use defaults from CONFIG
                params = {f: CONFIG.get('params', {}).get(f, {}).get('default', 0) for f in ['p1', 'p2', 'p3']}
        for field in ['p1', 'p2', 'p3']:
            if field in params:
                msg[field] = params[field]
        self._add_queue(msg, FlowPriority.SYSTEM)
        tgt = f" → {gw}" if gw else " (broadcast)"
        log.info('CFG', f"⚙️📦 Params sent{tgt}: p1={msg.get('p1','?')} p2={msg.get('p2','?')} p3={msg.get('p3','?')}")

    def _handle_param_update(self, data):
        """v34: Gateway reports local param change → update supervisor stored value + HA entity."""
        gw = data.get('g')
        if not gw or gw not in self.gateways: return
        if gw not in self._gw_params: self._gw_params[gw] = {}
        updated = []
        for pid in ['p1', 'p2', 'p3']:
            if pid in data:
                val = data[pid]
                self._gw_params[gw][pid] = val
                self.mqtt_client.publish(f"{CONFIG['state_prefix']}/gw/{gw.lower()}/param/{pid}", str(val), retain=True)
                updated.append(f"{pid}={val}")
        if updated:
            self._save_params()
            log.info('CFG', f"⚙️📦 {gw} param update (gateway→supervisor): {', '.join(updated)}")

    def _send_dump_anom_all(self):
        def _do():
            for gw in CONFIG['gateways']:
                self._add_queue({"t":"dump_anom","g":gw}, FlowPriority.SYSTEM); time.sleep(2)
        threading.Thread(target=_do, daemon=True).start()

    def _loop(self):
        """v25: 3-priority dispatch with Zero Loss for P0/P1."""
        if time.time()-self.last_tx < CONFIG['lora']['tx_cooldown']: return
        for p in sorted(FlowPriority):
            if self.queue[p]:
                msg = self.queue[p].popleft()
                success = self._send_lora(msg, priority=p)
                if not success and p in (FlowPriority.SYSTEM, FlowPriority.PRIORITY):
                    self.queue[p].appendleft(msg)
                    log.warn('LORA', f"📻 {PRIORITY_LABELS[p]} TX failed — requeued")
                return

    def _cleanup(self):
        self.running = False
        if self.antenna_mgr: self.antenna_mgr.close_all()
        if self.mqtt_client: self.mqtt_client.loop_stop(); self.mqtt_client.disconnect()
        log.info('MAIN', "Stopped")
        log.close()

    # =====================================================
    # LIFECYCLE
    # =====================================================
    def start(self):
        log.info('MAIN', "="*50)
        log.info('MAIN', f"🚀 SUPERVISOR {CONFIG['id']} v38 (Calendar + Anomaly Dashboard + Anti-Collision)")
        log.info('MAIN', "="*50)
        self._setup_mqtt(); self.ha = HomeAssistant(self.mqtt_client); time.sleep(3)
        for gw in CONFIG['gateways']: self.ha.reg_gateway(gw)
        self.ha.reg_supervisor()
        self.antenna_mgr = AntennaManager(self._on_lora_receive)
        self.antenna_mgr.connect_all()
        threading.Thread(target=self.antenna_mgr.reconnect_loop, daemon=True, name="ant-reconnect").start()
        self._publish_antenna_status()
        log.info('MAIN', "✅ Ready!")
        # v26: Passive startup — send config only, gateways self-announce
        time.sleep(2)
        self._send_cfg()
        self._last_ping_ts = now_ts()
        self._start_dump_cleanup()
        self._publish_calendar_status()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        threading.Thread(target=self._schedule_loop, daemon=True, name="schedule").start()
        # v26: Sync broadcast loop — time + sun state to gateways
        threading.Thread(target=self._sync_broadcast_loop, daemon=True, name="sync-broadcast").start()
        log.info('MAIN', f"📡 Waiting for gateways... (sync every {CONFIG.get('sync',{}).get('interval',1200)}s)")
        # v35: Startup — mark ALL gateways offline + ALL devices unavailable
        # Prevents stale retained MQTT showing devices as "online" after supervisor restart
        # Devices go back online when gateway connects → discovery → device states
        self._startup_mark_all_offline()
        try:
            while self.running: self._loop(); time.sleep(0.05)
        except KeyboardInterrupt: pass
        finally: self._cleanup()

    def _publish_antenna_status(self):
        status = self.antenna_mgr.get_status() if self.antenna_mgr else {}
        self.mqtt_client.publish("ha/lora/antenna_status", json.dumps({"ts":int(now_ts()), "antennas": status}), retain=True)

    def _setup_mqtt(self):
        self.mqtt_client = mqtt.Client(client_id=f"supervisor_{int(time.time())}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.username_pw_set(CONFIG['mqtt']['user'], CONFIG['mqtt']['pass'])
        self.mqtt_client.on_connect = self._on_connect; self.mqtt_client.on_message = self._on_message
        self.mqtt_client.connect(CONFIG['mqtt']['host'], CONFIG['mqtt']['port'], 60); self.mqtt_client.loop_start()
        log.info('MQTT', "✅ Connected")

    def _on_connect(self, client, ud, flags, rc, props):
        if rc == 0:
            pf = CONFIG['state_prefix']
            for t in [f"{pf}/+/+/set", f"{pf}/+/+/refresh", f"{pf}/gw/+/cmd/#", f"{pf}/supervisor/cmd/#",
                      f"{pf}/anomaly/+/clear", f"{pf}/config/+/set",
                      f"{pf}/+/vio/+/set", f"{pf}/+/vio/+/press",
                      # v30: Per-gateway param editing from dashboard
                      f"{pf}/gw/+/param/+/set",
                      "ha/lora/schedule/#", "ha/lora/anom/#",
                      f"{CONFIG['ha_prefix']}/sensor/+/config"]:
                client.subscribe(t)

    def _on_message(self, client, ud, msg):
        try:
            topic, payload = msg.topic, msg.payload.decode()
            parts = topic.split('/')

            # Retained anomaly collection
            ha_pfx = CONFIG['ha_prefix']
            if topic.startswith(f"{ha_pfx}/sensor/") and topic.endswith('/config'):
                sid = parts[2] if len(parts)>=4 else ''
                if sid.startswith('lora_an_') and payload and self._startup_phase:
                    try:
                        oid = json.loads(payload).get('object_id','')
                        if oid.startswith('lora_an_'): self._retained_anomaly_ids.add(oid[8:])
                    except: pass
                return

            # Config set
            if '/config/' in topic and topic.endswith('/set'):
                try:
                    v = int(float(payload)); cn = parts[-2]
                    if cn == 'timeout_switch': CONFIG['timeout']['switch'] = CONFIG['timeout']['light'] = v
                    elif cn == 'timeout_sensor': CONFIG['timeout']['sensor'] = v
                    elif cn == 'timeout_binary': CONFIG['timeout']['binary_sensor'] = v
                    self.mqtt_client.publish(f"{CONFIG['state_prefix']}/config/{cn}", str(v), retain=True)
                except: pass
                return

            # v30: Per-gateway param set from dashboard
            # Topic: lora/gw/{gw}/param/{pid}/set → store + publish state
            if '/gw/' in topic and '/param/' in topic and topic.endswith('/set'):
                try:
                    gw = parts[2].upper()
                    pid = parts[4]  # p1, p2, p3
                    val = float(payload)
                    if val == int(val): val = int(val)
                    if gw in self._gw_params and pid in self._gw_params[gw]:
                        self._gw_params[gw][pid] = val
                        # Publish state back (HA slider update)
                        self.mqtt_client.publish(f"{CONFIG['state_prefix']}/gw/{gw.lower()}/param/{pid}", str(val), retain=True)
                        log.info('CFG', f"⚙️📦 {gw} {pid}={val} (dashboard edit)")
                        self._save_params()
                except Exception as e: log.debug('CFG', f"Param set error: {e}")
                return

            # --- CALENDAR MQTT from HA ---
            if topic == "ha/lora/schedule/add":
                try:
                    d = json.loads(payload)
                    self.scheduler.add_slot(d.get('target','GLOBAL'), d['start'], d['end'], d.get('mode'), d.get('note',''))
                    self._publish_calendar_status()
                except Exception as e: log.error('CAL', f"add: {e}")
                return
            # Recurring schedule: generate multiple slots from pattern
            if topic == "ha/lora/schedule/add_recurring":
                try:
                    self._handle_recurring_schedule(json.loads(payload))
                except Exception as e: log.error('CAL', f"add_recurring: {e}")
                return
            # Quick production toggle
            if topic == "ha/lora/schedule/production_set":
                try:
                    d = json.loads(payload)
                    mode = d.get('mode', 0)
                    target = d.get('target', 'GLOBAL')
                    # Create 12h slot from now
                    start = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    end = datetime.fromtimestamp(now_ts() + 43200).strftime('%Y-%m-%d %H:%M:%S')
                    note = "Produkcja" if mode == 1 else "Przerwa"
                    if target == 'GLOBAL':
                        for gw in CONFIG['gateways']:
                            self.scheduler.add_slot(gw, start, end, mode, note)
                    else:
                        self.scheduler.add_slot(target, start, end, mode, note)
                    self._publish_calendar_status()
                except Exception as e: log.error('CAL', f"production_set: {e}")
                return
            if topic == "ha/lora/schedule/remove":
                try:
                    d = json.loads(payload)
                    self.scheduler.remove_slot(d.get('target','GLOBAL'), d['id'])
                    self._publish_calendar_status()
                except: pass
                return
            if topic == "ha/lora/schedule/clear":
                try:
                    d = json.loads(payload) if payload.strip() else {}
                    self.scheduler.clear_slots(d.get('target','GLOBAL'))
                    self._publish_calendar_status()
                except: pass
                return
            if topic == "ha/lora/schedule/list_req": self._publish_calendar_status(); return
            if topic == "ha/lora/schedule/sync_push":
                # Request gateway to push its schedule to supervisor
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._add_queue({"t":"cal_pull","g":gw}, FlowPriority.SYSTEM)
                    log.info('SYNC', f"📥 Requesting push from {gw}")
                except: pass
                return
            if topic == "ha/lora/schedule/sync_pull":
                # Send supervisor schedule to specific gateway (compact)
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._targeted_calendar_sync([gw])
                except: pass
                return
            if topic == "ha/lora/schedule/sync_bidir":
                # Pull from GW then push to GW
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._add_queue({"t":"cal_pull","g":gw}, FlowPriority.SYSTEM)
                    time.sleep(5)  # Wait for push data
                    self._targeted_calendar_sync([gw])
                except: pass
                return
            if topic == "ha/lora/schedule/sync_all":
                self._targeted_calendar_sync()
                return
            # v5.1: Targeted calendar sync (unicast, sequential, delta)
            if topic == "ha/lora/schedule/sync_targeted":
                try:
                    d = json.loads(payload) if payload.strip() else {}
                    targets = d.get('gateways', None)  # None = all enabled
                    self._targeted_calendar_sync(targets)
                except Exception as e: log.error('SYNC', f"targeted sync: {e}")
                return
            # Override toggle: add/remove GW from override list
            if topic == "ha/lora/schedule/override":
                try:
                    d = json.loads(payload)
                    gw = d.get('gw', '').upper()
                    on = d.get('on', False)
                    overrides = CONFIG.get('gateway_override', [])
                    if on and gw not in overrides:
                        overrides.append(gw)
                    elif not on and gw in overrides:
                        overrides.remove(gw)
                    CONFIG['gateway_override'] = overrides
                    log.info('CAL', f"📅 Override {'ON' if on else 'OFF'} for {gw} (overrides: {overrides})")
                except: pass
                return

            # --- VIO COMMAND ROUTING: HA dashboard → LoRa → Gateway ---
            # Topics: lora/{gw}/vio/{vid}/set (switch), lora/{gw}/vio/{vid}/press (button)
            pf = CONFIG['state_prefix']
            if '/vio/' in topic and len(parts) >= 4:
                gw_lower = parts[1]
                vid = parts[3]
                gw = gw_lower.upper()
                if topic.endswith('/set'):
                    # Virtual switch: forward as vsw command to gateway
                    val = 1 if payload.upper() in ('ON', '1') else 0
                    self._send_vswitch(gw, vid, val)
                    # Immediately update local state topic for dashboard responsiveness
                    self.mqtt_client.publish(f"{pf}/{gw_lower}/vio/{vid}/state",
                                             "ON" if val else "OFF", retain=True)
                    return
                elif topic.endswith('/press'):
                    # Virtual button: forward as vbtn command to gateway
                    self._send_virtual_button(gw, vid, 'single')
                    return

            # --- GHOST PRUNE ---
            if topic == "ha/lora/anom/prune": self._prune_anomalies(); return

            # Anomaly clear
            if '/anomaly/' in topic and topic.endswith('/clear'):
                self._remove_anomaly_by_id(parts[-2], send_clear=True); return

            # Supervisor commands
            if '/supervisor/cmd/' in topic:
                a = parts[-1]
                if a == 'send_config': self._send_cfg()
                elif a == 'ping_all': self._send_ping()
                elif a == 'discovery_all': self._send_discovery(force=True)
                elif a == 'clear_all_anomalies': self._clear_all_anomalies()
                elif a == 'clear_offline': self._clear_anomalies_by_type('device_offline')
                elif a == 'clear_low_battery': self._clear_anomalies_by_type(['low_battery','critical_battery'])
                elif a == 'clear_other': self._clear_other_anomalies()
                elif a == 'dump_anom_all': self._send_dump_anom_all()
                elif a == 'prune_anomalies': self._prune_anomalies()
                # v30: Global params command (payload: {"p1":val,"p2":val,"p3":val})
                elif a == 'send_params':
                    try:
                        params = json.loads(payload) if payload else {}
                        self._send_params(params=params)
                    except: self._send_params()
                # v32: Manual sync broadcast
                elif a == 'send_sync': self._send_sync()
                return

            # Per-gateway commands
            if '/gw/' in topic and '/cmd/' in topic:
                gw = parts[2].upper(); a = parts[-1]
                if a == 'ping': self._send_ping(gw)
                elif a == 'discovery': self._send_discovery(gw, force=True)
                elif a == 'clear_anomalies': self._clear_anomalies_by_gateway(gw)
                elif a == 'dump_anom': self._add_queue({"t":"dump_anom","g":gw}, FlowPriority.SYSTEM)
                elif a == 'send_config': self._send_cfg(gw)
                elif a == 'send_params':
                    try:
                        params = json.loads(payload) if payload else {}
                        self._send_params(gw=gw, params=params)
                    except: self._send_params(gw=gw)
                # v32: Per-gateway sync
                elif a == 'send_sync': self._send_sync(target_gw=gw)
                return

            # Device set/refresh
            if len(parts)>=4 and parts[-1] in ['set','refresh']:
                gw, dev = parts[1].upper(), self._find_device_name(parts[1].upper(), parts[2])
                if parts[-1]=='set':
                    self._add_queue({"t":"cmd","g":gw,"d":dev,"c":"state","v":payload}, FlowPriority.SYSTEM)
                else:
                    self._add_queue({"t":"req","g":gw,"d":dev}, FlowPriority.SYSTEM)
        except Exception as e:
            log.error('MQTT', f"Error: {e}")

    # =====================================================
    # LoRa HANDLING
    # =====================================================
    def _find_device_name(self, gw, dev_safe):
        with self.lock:
            for name, info in self.devices.items():
                if name.replace(' ','_').lower() == dev_safe and info.get('gateway') == gw: return name
        return dev_safe.replace('_',' ').title()

    def _on_lora_receive(self, text):
        try:
            data = json.loads(text)
            threading.Thread(target=self._handle_lora, args=(data,), daemon=True).start()
        except: pass

    def _handle_lora(self, data):
        try:
            t, gw = data.get('t'), data.get('g')
            if gw and gw not in self.gateways:
                log.debug('LORA', f"📥 Ignored packet from unknown gateway: {gw}")
                return
            if gw and gw in self.gateways:
                self._update_gw_last_seen(gw)
                self._gw_last_rx[gw] = time.time()  # v37: safe window tracking

            # Packet dispatch
            if t == 'b': self._handle_batch(data)
            elif t == 'db': self._handle_discovery_batch(data)
            elif t == 'ab': self._handle_anomaly_batch(data)
            elif t == 'disc_meta': self._handle_disc_meta(data)
            elif t == 'disc_vio': self._handle_disc_vio(data)
            elif t == 'hb': self._handle_heartbeat(data)
            elif t == 'pong': self._handle_pong(data)
            elif t == 'disc_ack': self._handle_disc_ack(data)
            elif t == 'vsw_st': self._handle_vswitch_status(data)
            elif t == 'cal_begin': self.cal_transfer.handle_begin(data)
            elif t == 'cal_chunk': self.cal_transfer.handle_chunk(data)
            elif t == 'cal_end':
                result = self.cal_transfer.handle_end(data)
                if result: self._process_calendar_transfer(*result)
            elif t == 'cal_ack': self.cal_transfer.handle_ack(data)
            elif t == 's_req': self._handle_sync_request(data)
            elif t == 'param_upd': self._handle_param_update(data)
            else:
                log.debug('LORA', f"📥 Unknown packet type: t={t} from {gw or '?'}")

            # v37: Safe window — gateway just finished TX, send pending cmds now
            if gw and gw in self.gateways:
                self._flush_gw_pending_cmds(gw)
        except Exception as e:
            log.error('LORA', f"Error: {e}")

    def _process_calendar_transfer(self, gw, direction, slots):
        """Handle completed CalendarTransfer.
        direction='push'/'bidir': legacy full slot dicts
        direction='gw_push': compact [[sm,dm,mode],...] from gateway reverse sync
        """
        if direction == 'push':
            self.scheduler.merge_schedule(gw, slots); self._publish_calendar_status()
        elif direction == 'bidir':
            self.scheduler.merge_schedule(gw, slots)
            self.cal_transfer.start_send(gw, "pull", self.scheduler.get_effective_schedule(gw))
            self._publish_calendar_status()
        elif direction == 'gw_push':
            # Gateway sent compact data → decode → save to schedules.json[GW]
            full_slots = ScheduleManager.expand_compact(slots)
            with self.scheduler.lock:
                self.scheduler.data[gw] = full_slots
                self.scheduler._save()
            log.info('SYNC', f"📥 {gw} push: {len(full_slots)} slots saved")
            self._publish_calendar_status()
            # Publish MQTT sensor for dashboard
            mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
            mode_icons = {0: '⚪', 1: '🟢', 2: '⏸', 3: '🔧'}
            events = [{"start": s['start'], "end": s['end'], "mode": s.get('mode', 0),
                       "name": mode_names.get(s.get('mode', 0), '?'),
                       "icon": mode_icons.get(s.get('mode', 0), '📅')} for s in full_slots]
            self.mqtt_client.publish(f"ha/lora/schedule/{gw.lower()}/events", json.dumps({
                "ts": int(now_ts()), "count": len(events), "events": events
            }), retain=True)
            # Write to HA Local Calendar via REST API
            self._sync_to_ha_calendar(gw, full_slots)

    def _update_gw_last_seen(self, gw):
        was_offline = False
        with self.lock:
            if gw in self.gateways:
                was_offline = not self.gateways[gw]['online']
                self.gateways[gw].update({'last_seen':now_str(),'last_seen_ts':now_ts(),'online':True})
        self._publish_gw_stats(gw)
        if was_offline:
            self._gateway_came_online(gw)

    # =====================================================
    # BATCH PACKET HANDLER — batch-level ts + dedup-safe
    # =====================================================
    def _handle_batch(self, data):
        """v25: Unpack batch with enhanced industrial logging.
        Format: {"t":"b","g":"G1","ts":<epoch>,"d":[[sid,{payload}],...]}
        Anomaly markers: _an (type), _av (value) — routed to anomaly system."""
        gw = data.get('g')
        batch_ts = data.get('ts', int(now_ts()))
        try:
            batch_ls = datetime.fromtimestamp(batch_ts).strftime('%Y-%m-%d %H:%M:%S')
        except: batch_ls = now_str()
        items = data.get('d', [])
        sid_map = self._dev_short_map.get(gw, {})
        resolved = 0; unresolved = 0
        log_lines = []
        for item in items:
            if not isinstance(item, list) or len(item) < 2: continue
            raw_id, payload = item[0], item[1]
            if not isinstance(payload, dict): continue
            if isinstance(raw_id, int):
                dev = sid_map.get(raw_id)
                if dev is None:
                    unresolved += 1; continue
                resolved += 1
            else:
                dev = raw_id; resolved += 1
            # v25: Check for anomaly marker in payload
            an_type = payload.pop('_an', None)
            an_val = payload.pop('_av', None)
            # Process status
            status_data = {'t': 'st', 'g': gw, 'd': dev, 'ls': batch_ls}
            status_data.update(payload)
            self._handle_status(status_data)
            # v25: Route anomaly if present
            if an_type:
                self._add_anomaly(gw, dev, an_type, an_val, batch_ts)
            # v25: Enhanced debug line
            ico = self._device_log_icon(dev, payload, an_type)
            log_lines.append(f"  {ico} ID:{raw_id} [{dev}] {self._payload_summary(payload, an_type)}")
        # Latency tracking
        latency_s = round((now_ts() - batch_ts), 1) if batch_ts > 0 else 0
        with self.lock:
            if gw in self.gateways:
                self.gateways[gw]['batch_latency'] = latency_s
        log.info('BATCH', f"📦 From {gw}: {resolved} items, {unresolved} unknown | latency={latency_s}s")
        for line in log_lines:
            log.info('BATCH', line)
        if unresolved > 0:
            log.warn('BATCH', f"⚠️ {gw} unknown sids — forcing discovery")
            self._gw_disc_hash[gw] = ''
            self._send_discovery(gw, force=True)

    def _device_log_icon(self, dev, payload, an_type=None):
        """v27: Pick icon for debug log based on device data."""
        if an_type in ('smoke', 'water_leak'): return '🔴'
        if an_type in ('device_offline',): return '💀'
        if an_type: return '🟡'
        if 's' in payload: return '🟢' if payload['s'] else '⚫'
        if 't' in payload: return '🌡️'
        if 'w' in payload: return '💧' if payload['w'] else '💧'
        if 'k' in payload: return '🔥' if payload['k'] else '✅'
        if 'c' in payload: return '🚪' if not payload['c'] else '🔒'
        if 'o' in payload: return '🏃' if payload['o'] else '👁️'
        return '📋'

    def _payload_summary(self, payload, an_type=None):
        """v27: Human-readable payload — c0=OPEN, c1=CLOSED, w0=DRY, w1=LEAK."""
        parts = []
        if an_type: parts.append(f"⚠️{an_type}")
        if 's' in payload: parts.append(f"{'ON' if payload['s'] else 'OFF'}")
        if 't' in payload: parts.append(f"{payload['t']}°C")
        if 'h' in payload: parts.append(f"{payload['h']}%")
        if 'b' in payload: parts.append(f"🔋{payload['b']}%")
        # v27: Poprawne mapowanie binary_sensor
        if 'c' in payload: parts.append(f"{'CLOSED' if payload['c'] else 'OPEN'}")
        if 'w' in payload: parts.append(f"{'LEAK!' if payload['w'] else 'DRY'}")
        if 'k' in payload: parts.append(f"{'SMOKE!' if payload['k'] else 'OK'}")
        if 'o' in payload: parts.append(f"{'MOTION' if payload['o'] else 'CLEAR'}")
        if 'a' in payload: parts.append(f"av={'Y' if payload['a'] else 'N'}")
        return ' | '.join(parts) if parts else str(payload)

    # =====================================================
    # v6.1: ENHANCED PONG HANDLER with Differential Sync
    # =====================================================
    def _handle_pong(self, data):
        """v32: PONG — clear ping tracking + process unified diag."""
        gw = data.get('g')
        with self.lock:
            if gw in self._pending_pings:
                retries = self._pending_pings[gw].get('retries', 0)
                del self._pending_pings[gw]
                if retries > 0:
                    log.info('PING', f"🏓 {gw} responded after {retries} retries")
        self._process_diag(data, source='pong')

    # =====================================================
    # v5.0: DISCOVERY HASH ACK
    # =====================================================

    def _handle_disc_ack(self, data):
        """Gateway confirmed hash matches — no full discovery needed."""
        gw = data.get('g')
        h = data.get('hash', '')
        n = data.get('n', 0)
        self._gw_disc_hash[gw] = h
        with self.lock:
            self.gateways[gw]['disc_hash'] = h
        log.info('DISC', f"🔭 {gw} disc_ack: hash={h} devices={n} (config unchanged)")

    # =====================================================
    # v5.0: DISC_META — gateway config metadata + virtual I/O
    # =====================================================
    def _handle_disc_meta(self, data):
        """Slim packet: {"t":"disc_meta","g":"G1","hash":"...","dev_n":N}
        VIO arrives separately as disc_vio."""
        gw = data.get('g')
        h = data.get('hash', '')
        dev_n = data.get('dev_n', 0)
        self._gw_disc_hash[gw] = h
        with self.lock:
            self.gateways[gw]['disc_hash'] = h
        # Legacy compat: if vio still embedded in disc_meta
        vio = data.get('vio', [])
        if vio:
            self._process_vio_list(gw, vio)
        log.info('DISC', f"🔭 {gw} disc_meta: hash={h} dev_n={dev_n}")

    def _handle_disc_vio(self, data):
        """Compact VIO packet: {"t":"disc_vio","g":"G1","d":[[id,type_char,name,value?],...]}
        Registers virtual switches/buttons as HA entities."""
        gw = data.get('g')
        items = data.get('d', [])
        # Convert compact array to dict format for _process_vio_list
        vio = []
        for item in items:
            if not isinstance(item, list) or len(item) < 3: continue
            entry = {'id': item[0], 'tp': item[1], 'n': item[2]}
            if len(item) > 3: entry['v'] = item[3]  # switch value
            vio.append(entry)
        self._process_vio_list(gw, vio)
        log.info('DISC', f"🔭 {gw} disc_vio: {len(vio)} virtual I/O registered")

    def _process_vio_list(self, gw, vio):
        """Register VIO entries as HA entities. Shared by disc_meta (legacy) and disc_vio."""
        if gw not in self._gw_vio: self._gw_vio[gw] = []
        self._gw_vio[gw] = vio
        for vi in vio:
            vid, tp = vi.get('id'), vi.get('tp')
            name = vi.get('n', vid)
            if tp == 's':
                self.ha.reg_vswitch(gw, vid, name, vi.get('v', 0))
            elif tp == 'b':
                self.ha.reg_vbutton(gw, vid, name)

    # =====================================================
    # v5.0: DISCOVERY BATCH — replaces N individual disc_resp
    # =====================================================
    # v27: Reverse maps for compact discovery
    _TYPE_REV = {'S':'sensor','W':'switch','L':'light','B':'binary_sensor'}
    _CAPS_REV = {'t':'temperature','h':'humidity','b':'battery','s':'state',
                 'r':'brightness','c':'contact','o':'occupancy','w':'water_leak','k':'smoke'}

    def _decode_caps(self, caps_str):
        """'thb' → ['temperature','humidity','battery']"""
        if isinstance(caps_str, list): return caps_str  # Old format compat
        return [self._CAPS_REV.get(c, c) for c in caps_str]

    def _decode_type(self, type_str):
        """'S' → 'sensor', 'switch' → 'switch' (backward compat)"""
        return self._TYPE_REV.get(type_str, type_str)

    def _handle_discovery_batch(self, data):
        """v27: Compact discovery — NO inline state.
        Format: {"t":"db","g":"G1","d":[[sid,"name","S","thb"], ...]}
        Type: 1-char (S/W/L/B). Caps: string ("thb").
        State arrives separately via batcher (t:b)."""
        gw = data.get('g')
        items = data.get('d', [])
        first_in_refresh = gw in self._disc_refresh_gw
        for item in items:
            if not isinstance(item, list) or len(item) < 4: continue
            sid, dev = item[0], item[1]
            dtype = self._decode_type(item[2])
            caps = self._decode_caps(item[3])
            # Register HA entities
            if dtype in ['switch', 'light']: self.ha.reg_switch(gw, dev, dtype)
            elif dtype == 'sensor': self.ha.reg_sensor(gw, dev, caps)
            elif dtype == 'binary_sensor': self.ha.reg_binary(gw, dev, caps)
            # Update device registry
            with self.lock:
                self.devices[dev] = {'gateway': gw, 'type': dtype, 'caps': caps, 'available': True, 'last_seen': now_str()}
            if gw in self._dev_short_map:
                self._dev_short_map[gw][sid] = dev
            if first_in_refresh:
                self._gw_devices[gw] = {dev}
                self._disc_refresh_gw.discard(gw)
                first_in_refresh = False
            else:
                if gw not in self._gw_devices: self._gw_devices[gw] = set()
                self._gw_devices[gw].add(dev)
            self.ha.pub_dev_available(gw, dev, True)
        log.info('DISC', f"🔭 {gw} db: {len(items)} devices registered (state follows via batcher)")
        self._save_gw_devices()  # v37: persist for startup cleanup

    # =====================================================
    # v5.0: ANOMALY BATCH — replaces N individual 'an' from dump
    # =====================================================
    # v34: Anomaly type short codes (must match gateway)
    _ANOM_REV = {
        'do':'device_offline', 'lb':'low_battery', 'cb':'critical_battery',
        'th':'temp_high', 'tl':'temp_low', 'hh':'hum_high', 'hl':'hum_low',
        'sg':'stagnation', 'sk':'smoke', 'wl':'water_leak',
        'to':'temp_ok', 'bo':'battery_ok', 'ho':'hum_ok',
        'dn':'device_online', 'sc':'stagnation_clear',
    }

    def _handle_anomaly_batch(self, data):
        """v34: Compact anomaly batch with short IDs + short type codes.
        Format: {"t":"ab","g":"G1","ts":epoch,"d":[[sid,"th",32.6],...]]}
        Also accepts old format: [["dev","atype",value,ts], ...] (dump_anom)."""
        gw = data.get('g')
        items = data.get('d', [])
        pkt_ts = data.get('ts', int(now_ts()))
        sid_map = self._dev_short_map.get(gw, {})
        processed = 0
        for item in items:
            if not isinstance(item, list) or len(item) < 2: continue
            raw_id, raw_type = item[0], item[1]
            value = item[2] if len(item) > 2 else None
            # Resolve device name
            if isinstance(raw_id, int):
                dev = sid_map.get(raw_id)
                if dev is None:
                    log.debug('ANOMALY', f"⚠️ {gw} unknown sid {raw_id} in ab"); continue
            else:
                dev = raw_id
            # Resolve anomaly type (short → full)
            atype = self._ANOM_REV.get(raw_type, raw_type)
            # Timestamp: per-item (old format) or per-packet (new)
            ts = item[3] if len(item) > 3 else pkt_ts
            self._add_anomaly(gw, dev, atype, value, ts)
            processed += 1
        log.info('ANOMALY', f"📋 {gw} ab: {processed}/{len(items)} anomalies processed")

    # =====================================================
    # v5.0: HEARTBEAT HANDLER (gateway self-reports)
    # =====================================================
    def _handle_heartbeat(self, data):
        """v32: HB — process unified diag (identical to PONG)."""
        self._process_diag(data, source='hb')

    def _process_diag(self, data, source='hb'):
        """v32: Unified diagnostic processor — shared by HB and PONG.
        Updates gateway stats, checks cal hash, detects time drift, publishes entities."""
        gw = data.get('g')
        if not gw or gw not in self.gateways: return
        z2m_age = data.get('z2m', -1)
        z2m_stale = z2m_age > CONFIG.get('z2m_stale_threshold', 900) if z2m_age >= 0 else False
        mode_char = data.get('m', '?')
        sync_age = data.get('sa', -1)
        gw_offset = data.get('o', 0)
        q_depth = data.get('q', 0)
        sup_contact = data.get('sc', -1)
        gw_ts = data.get('ts', 0)  # v32: gateway system timestamp

        with self.lock:
            g = self.gateways[gw]
            g['uptime'] = data.get('up', g['uptime'])
            if 'dev' in data: g['devices_total'] = data['dev']
            if 'mon' in data: g['devices_monitored'] = data['mon']
            if 'pri' in data: g['devices_priority'] = data['pri']
            if 'air' in data: g['airtime'] = data['air']
            if 'rssi' in data and data['rssi'] is not None: g['rssi'] = data['rssi']
            g['z2m_age'] = z2m_age
            g['z2m_stale'] = z2m_stale
            g['context_mode'] = mode_char
            g['sync_age'] = sync_age
            g['time_offset'] = gw_offset
            g['queue_depth'] = q_depth
            g['sup_contact'] = sup_contact
            # Device hash diff sync
            new_hash = data.get('hash', '')
            if new_hash:
                old_hash = self._gw_disc_hash.get(gw, '')
                self._gw_disc_hash[gw] = new_hash
                g['disc_hash'] = new_hash
                if not old_hash:
                    log.info('DISC', f"🔭 {gw} first hash: {new_hash} — requesting discovery")
                    self._send_discovery(gw, force=True)
                elif old_hash != new_hash:
                    log.info('DISC', f"🔭 {gw} hash changed → resync")
                    def _resync():
                        time.sleep(1); self._send_discovery(gw, force=True)
                        time.sleep(2); self._send_cfg(gw)
                    threading.Thread(target=_resync, daemon=True).start()

        # v33: Calendar hash comparison — compare with supervisor's expected hash
        gw_cal_hash = data.get('cal', '')
        if gw_cal_hash:
            expected = self._gw_expected_cal_hash.get(gw, '')
            if expected and expected != gw_cal_hash:
                # Gateway has different calendar than what supervisor sent
                last_sync = getattr(self, f'_last_cal_sync_{gw}', 0)
                if time.time() - last_sync > 300:
                    log.info('CAL', f"📅 {gw} cal mismatch: gw={gw_cal_hash[:8]} exp={expected[:8]} — resync")
                    self._targeted_calendar_sync([gw])
                    setattr(self, f'_last_cal_sync_{gw}', time.time())
            self._gw_cal_hash[gw] = gw_cal_hash

        # v33: Auto time sync + anomaly if gateway clock drift > threshold
        drift_threshold = CONFIG.get('sync', {}).get('drift_threshold', 120)
        if gw_ts > 0:
            drift = abs(gw_ts - time.time())
            if drift > drift_threshold:
                log.warn('SYNC', f"🔄⚠️ {gw} clock drift {drift:.0f}s > {drift_threshold}s — sync + anomaly")
                self._send_sync(target_gw=gw)
                self._add_anomaly(gw, f"gateway_{gw}", 'time_drift', f"{drift:.0f}s", int(now_ts()))
            else:
                # Drift OK — auto-clear if previously reported
                self._clear_anomaly_if_exists(gw, f"gateway_{gw}", 'time_drift')

        # VSwitch states
        vs = data.get('vs')
        if vs and isinstance(vs, dict):
            self._gw_vswitch_states[gw] = vs

        # Failsafe → immediate sync
        if mode_char == 'F':
            log.warn('SYNC', f"🔄⚠️ {gw} in FAILSAFE — sending targeted sync")
            self._send_sync(target_gw=gw)

        # Z2M stale warning
        if z2m_stale:
            log.warn('PING', f"⚠️ {gw} Z2M stale ({z2m_age}s)")

        # Publish sync entity + stats
        self._publish_gw_sync_entity(gw, data)
        self._publish_gw_stats(gw)

        # Log
        mode_icons = {'D': '☀️', 'N': '🌙', 'F': '⚠️'}
        mico = mode_icons.get(mode_char, '❓')
        rssi_v = g.get('rssi', -999)
        rssi_str = str(rssi_v) if rssi_v > -999 else '--'
        src_icon = '🏓' if source == 'pong' else '💓'
        src_label = 'PONG' if source == 'pong' else 'HB'
        log.info('PING', f"{src_icon} {src_label} {gw}: {mico}{mode_char} dev={data.get('dev','?')} mon={data.get('mon','?')} pri={data.get('pri','?')}" +
                 f" z2m={z2m_age}s sync={sync_age}s q={q_depth} sc={sup_contact}s" +
                 (f" ⚠️Z2M_STALE" if z2m_stale else "") +
                 f" air={data.get('air',0)}% rssi={rssi_str}" +
                 (f" cal={gw_cal_hash[:8]}" if gw_cal_hash else ""))

    def _publish_gw_sync_entity(self, gw, hb_data):
        """v27: Publish sync status MQTT entity per gateway for dashboard."""
        mode_char = hb_data.get('m', '?')
        if mode_char == 'F': mode_str = "FAILSAFE"
        elif mode_char == 'N': mode_str = "NIGHT"
        elif mode_char == 'D': mode_str = "DAY"
        else: mode_str = "UNKNOWN"
        sync_age = hb_data.get('sa', -1)
        offset = hb_data.get('o', 0)
        drift_threshold = CONFIG.get('sync', {}).get('drift_threshold', 120)
        data = {
            "mode": mode_str,
            "sync_age_sec": sync_age,
            "offset_sec": offset,
            "drift_ok": abs(offset) <= drift_threshold if offset else True,
            "queue_depth": hb_data.get('q', 0),
            "production": hb_data.get('pm', 0),
        }
        pf = CONFIG.get('state_prefix', 'lora')
        self.mqtt_client.publish(f"{pf}/gw/{gw.lower()}/sync", json.dumps(data), retain=True)

    # =====================================================
    # v5.0: VIRTUAL SWITCH STATUS (confirmation from gateway)
    # =====================================================
    def _handle_vswitch_status(self, data):
        """Gateway confirmed vswitch state change.
        Packet: {"t":"vsw_st","g":"G1","id":"vs_prod","v":1}
        """
        gw = data.get('g')
        vid = data.get('id')
        val = data.get('v', 0)
        if not gw or not vid:
            return
        self._gw_vswitch_states.setdefault(gw, {})[vid] = val
        # Publish to HA MQTT using VIO topic — keeps dashboard in sync
        safe = vid.replace(' ', '_').lower()
        pf = CONFIG['state_prefix']
        self.mqtt_client.publish(f"{pf}/{gw.lower()}/vio/{safe}/state",
                                 "ON" if val else "OFF", retain=True)
        log.info('CFG', f"🔘 {gw} vswitch {vid} = {'ON' if val else 'OFF'}")

    def _handle_status(self, data):
        """v6.2: Parse status with Short JSON keys (backward compatible).
        New 1-char keys: a=av, s=st, l=ls, t=tmp, h=hum, b=bat, r=bri, c=con, o=occ, w=wtr, k=smk
        Old 2-3 char keys still accepted for backward compatibility."""
        gw, dev = data.get('g'), data.get('d')
        if not dev: return
        # v6.2: Accept both old 'ls' and new 'l' for last_seen
        ls = data.get('ls') or data.get('l') or now_str()
        # v6.2: Accept both old 'av' and new 'a' for availability
        raw_av = data.get('av') if 'av' in data else data.get('a', True)
        av = bool(raw_av) if isinstance(raw_av, int) else raw_av
        sd = {'last_seen': ls}
        # v6.2: Short JSON key map — (full_name, [new_short, old_short])
        key_pairs = [
            ('state',       's',  'st'),
            ('temperature', 't',  'tmp'),
            ('humidity',    'h',  'hum'),
            ('battery',     'b',  'bat'),
            ('contact',     'c',  'con'),
            ('occupancy',   'o',  'occ'),
            ('water_leak',  'w',  'wtr'),
            ('smoke',       'k',  'smk'),
            ('brightness',  'r',  'bri'),
        ]
        for full, new_key, old_key in key_pairs:
            v = data.get(new_key) if new_key in data else data.get(old_key)
            if v is not None:
                # Decompress 0/1 → ON/OFF for state, True/False for booleans
                if full == 'state' and isinstance(v, int):
                    v = 'ON' if v else 'OFF'
                elif full in ('contact','occupancy','water_leak','smoke') and isinstance(v, int):
                    v = bool(v)
                sd[full] = v
        if not av:
            if 'temperature' not in sd: sd['temperature'] = '--'
            if 'humidity' not in sd: sd['humidity'] = '--'
        sk = f"{gw}:{dev}"
        with self.lock:
            if dev not in self.devices: self.devices[dev] = {}
            bat_val = data.get('b') if 'b' in data else data.get('bat')
            self.devices[dev].update({'gateway':gw,'available':av,'last_seen':ls,'battery':bat_val})
            if sk not in self.device_states: self.device_states[sk] = {}
            self.device_states[sk].update(sd)
            cached = dict(self.device_states[sk])
        self.ha.pub_dev_state(gw, dev, cached)
        self.ha.pub_dev_available(gw, dev, av)


if __name__ == "__main__":
    Supervisor().start()
