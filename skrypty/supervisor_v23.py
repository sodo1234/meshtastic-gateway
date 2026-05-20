#!/usr/bin/env python3
"""
LoRa Zigbee Supervisor - v6.3 (ETAP 4: Diagnostics & Precision)

Schedule Sync: via CalendarTransfer (CRC + retry + zlib compression)
  SUP→GW: cal_transfer.start_send(gw, "cal", compact) → sch_b/sch_c/sch_e/sch_a
  GW→SUP: cal_transfer.start_send(id, "gw_push", compact) → same protocol
  Compact: [[start_min, dur_min, mode],...] — 14 slots = 104B = 1 chunk = 4 packets
  Mode: event name → CONFIG schedule_modes → number (PRODUKCJA→1, PRZERWA→2, SERWIS→3)

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

import json, time, threading, os, zlib, base64, hashlib, random, string, traceback
import urllib.request
from datetime import datetime
from collections import deque
from enum import IntEnum
import paho.mqtt.client as mqtt
import meshtastic, meshtastic.serial_interface
from pubsub import pub

SCHEDULE_FILE = os.environ.get('LORA_SCHEDULE_FILE',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schedules.json'))

CONFIG = {
    "id": "G0",
    "mesh_ports": [
        {"port": "/dev/ttyUSB0", "enabled": True,  "label": "ANT-1", "gateways": ["G1"]},
        #{"port": "/dev/ttyUSB1", "enabled": False, "label": "ANT-2", "gateways": ["G2"]},
        #{"port": "/dev/ttyACM0", "enabled": False, "label": "ANT-3", "gateways": ["G3"]},
        # v5.1: Up to 8 interfaces supported
        # {"port": "/dev/ttyUSB2", "enabled": False, "label": "ANT-4", "gateways": ["G1","G2"]},
        # {"port": "/dev/ttyUSB3", "enabled": False, "label": "ANT-5", "gateways": ["G3"]},
    ],
    "mesh_reconnect": {"enabled": True, "interval": 15, "max_backoff": 120},
    "mqtt": {"host": "localhost", "port": 1883, "user": "mqtt", "pass": "REPLACE_ME"},
    "ha_prefix": "homeassistant",
    "state_prefix": "lora",
    "gateways": ["G1", "G2", "G3"],
    "lora": {"max_size": 220, "tx_cooldown": 2.5, "cal_chunk_delay": 6.0},
    # Timeout: device considered offline after this many seconds of silence
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},
    "gateway_timeout": 300,
    # Heartbeat: gateway self-reports; this is the silence threshold
    "heartbeat_timeout": 480,      # 8 min = 1.6× gateway's 5-min heartbeat interval
    "z2m_stale_threshold": 900,    # 15 min — if gateway's Z2M hasn't updated, flag stale
    # Fallback ping: supervisor PINGs only on demand or as heartbeat fallback
    "ping_interval": 1800,
    "production_schedule": {"enabled": True, "pre_notify_seconds": [30, 5]},
    "anomaly": {
        "auto_clear": {
            "device_offline": True, "low_battery": True, "critical_battery": True,
            "temp_high": True, "temp_low": True, "hum_high": True, "hum_low": True,
            "stagnation": True
        }
    },
    "calendar": {"chunk_size": 140, "transfer_timeout": 60, "retry_max": 2},
    # v5.1: Calendar sync settings
    "calendar_sync": {
        "rolling_window_extra_days": 7,
        "sync_timeout_per_gw": 30,
        "enabled_gateways": ["G1", "G2", "G3"],
    },
    # Schedule mode keywords: HA event name → mode number
    # User creates event named "PRODUKCJA" → mode=1
    # No event at given time → mode=0 (BRAK PRODUKCJI)
    "schedule_modes": {
        "PRODUKCJA": 1,
        "PRZERWA": 2,
        "SERWIS": 3,
        # Add more: "KONSERWACJA": 4, "TESTOWANIE": 5, etc.
    },
    "mode_names": {
        0: "BRAK PRODUKCJI",
        1: "PRODUKCJA",
        2: "PRZERWA",
        3: "SERWIS",
    },
    # HA REST API — for writing to local calendars (calendar.lora_g1/g2/g3)
    # Generate token: HA → Profile → Long-Lived Access Tokens → Create
    "ha_api": {
        "url": "http://localhost:8123",
        "token": "",  # PASTE YOUR LONG-LIVED TOKEN HERE
    },
    # Per-gateway calendar override:
    # If gateway listed here → use calendar.lora_{gw} instead of calendar.lora_global
    # Controlled by input_boolean.lora_override_{gw} on dashboard
    "gateway_override": [],  # e.g. ["G1"] → G1 gets its own calendar, G2/G3 get GLOBAL
    # Log rotation: max 5MB per file, keep 3 rotated copies
    "log": {"file": "/tmp/supervisor.log", "max_bytes": 5_000_000, "backup_count": 3},
}


# =====================================================
# v6.0: FLOW PRIORITY — ETAP 1 hierarchy (matches Gateway)
# =====================================================
class FlowPriority(IntEnum):
    ALARM_PRIO  = 0   # P0: Priority device state changes + safety anomalies
    COMMAND     = 1   # P1: Commands t:cmd, system responses
    DIAGNOSTIC  = 2   # P2: Anomalies/offline for regular monitored devices
    BATCH       = 3   # P3: Cyclic reports, discovery batches

Priority = FlowPriority

PRIORITY_LABELS = {
    FlowPriority.ALARM_PRIO: "[P0:ALARM_PRIO]",
    FlowPriority.COMMAND:    "[P1:CMD]",
    FlowPriority.DIAGNOSTIC: "[P2:DIAG]",
    FlowPriority.BATCH:      "[P3:BATCH]",
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
        If mode not specified, detect from note using schedule_modes CONFIG.
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
        # Strip emoji prefixes
        for ch in ['🟢', '⚪', '🔧', '⏸']: text_upper = text_upper.replace(ch, '').strip()
        for keyword, mode_num in CONFIG.get('schedule_modes', {}).items():
            if keyword.upper() in text_upper:
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
        """[K2] Delay between chunks — uses cal_chunk_delay (default 6s) instead of tx_cooldown+0.5."""
        return CONFIG['lora'].get('cal_chunk_delay', 6.0)

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'dir': direction, 'retries': 0, 'ts': now_ts(), 'ack_received': False}
        self.queue_fn({"t":"sch_b","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, FlowPriority.COMMAND)
        log.info('SYNC', f"📤 Begin {direction} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")
        def _send():
            delay = self._chunk_delay()  # [K2]
            for i, chunk in enumerate(chunks):
                time.sleep(delay)
                self.queue_fn({"t":"sch_c","tid":tid,"s":i,"d":chunk}, FlowPriority.COMMAND)
            # [K4] Send sch_e with retry — LoRa packet loss recovery
            # If sch_e is lost in air, gateway never responds → deadlock without this retry
            SCH_E_RETRIES = 3
            SCH_E_WAIT = 15  # seconds to wait for any ACK/NACK after each sch_e
            for attempt in range(SCH_E_RETRIES):
                time.sleep(delay)
                with self.lock:
                    if tid not in self.outgoing:
                        return  # Session already completed (ACK ok=1 or retry exhausted)
                self.queue_fn({"t":"sch_e","tid":tid}, FlowPriority.COMMAND)
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
            self.queue_fn({"t":"sch_a","tid":tid,"ok":0,"miss":missing[:5]}, FlowPriority.COMMAND)
            return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            log.warn('SYNC', f"⚠️ tid={tid} CRC mismatch")
            with self.lock: self.incoming[tid]['ts'] = now_ts()  # [K3] Refresh ts for retry window
            self.queue_fn({"t":"sch_a","tid":tid,"ok":0,"miss":[]}, FlowPriority.COMMAND)
            return None
        # Success — NOW remove the session [K3]
        with self.lock: self.incoming.pop(tid, None)
        self.queue_fn({"t":"sch_a","tid":tid,"ok":1,"miss":[]}, FlowPriority.COMMAND)
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
                        self.queue_fn({"t":"sch_c","tid":tid,"s":s,"d":info['chunks'][s]}, FlowPriority.COMMAND)
                time.sleep(delay)
                self.queue_fn({"t":"sch_e","tid":tid}, FlowPriority.COMMAND)
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
                self.mqtt.publish(f"{p}/sensor/lora_gw_{gl}_{eid}/config", json.dumps(cfg), retain=True)
        for eid, name, icon in [("ping","Ping","mdi:lan-connect"),("discovery","Discovery","mdi:magnify"),
                                ("clear_anomalies","Clear Anomalies","mdi:bell-off"),("dump_anom","Dump Anomalies","mdi:alert-circle-outline")]:
            self.mqtt.publish(f"{p}/button/lora_gw_{gl}_{eid}/config", json.dumps({
                "name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                "command_topic": f"{CONFIG['state_prefix']}/gw/{gl}/cmd/{eid}", "device": di, "icon": icon}), retain=True)
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
        if anomaly_id in self.registered_anomalies: return
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
            # sendText() blocked beyond timeout — interface is dead
            log.error('ANT', f"❌ TX {label}: sendText() blocked >{timeout}s — removing antenna")
            with self.lock: self.interfaces.pop(label, None)
            return False
        if error[0]:
            log.error('ANT', f"❌ TX {label}: {error[0]}")
            with self.lock: self.interfaces.pop(label, None)
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

    def reconnect_loop(self):
        while self.running:
            time.sleep(5)
            if not CONFIG['mesh_reconnect'].get('enabled', True): continue
            for acfg in CONFIG['mesh_ports']:
                if not acfg.get('enabled', False): continue
                label = acfg['label']
                with self.lock: connected = label in self.interfaces
                if connected:
                    try:
                        iface = self.interfaces.get(label)
                        if iface and hasattr(iface, 'localNode'):
                            _ = iface.localNode
                    except:
                        log.warn('ANT', f"⚠️ {label} unhealthy, removing")
                        with self.lock: self.interfaces.pop(label, None)
                else:
                    backoff = self._reconnect_backoff.get(label, 0)
                    interval = min(CONFIG['mesh_reconnect']['interval'] * (2 ** backoff),
                                   CONFIG['mesh_reconnect']['max_backoff'])
                    time.sleep(interval)
                    log.info('ANT', f"🔄 Reconnecting {label} ({acfg['port']})...")
                    if self._connect_one(acfg):
                        self._reconnect_backoff[label] = 0
                    else:
                        self._reconnect_backoff[label] = backoff + 1

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
            'devices_total': 0, 'devices_monitored': 0,
            # v5.0: Additional diagnostics from heartbeat/PONG
            'airtime': 0, 'rssi': None, 'anom_blocked': 0, 'disc_hash': '',
            'z2m_age': -1, 'z2m_stale': False,
            # v6.3: Latency tracking — batch transit time
            'batch_latency': 0,
        } for gw in CONFIG['gateways']}
        self.devices = {}
        self.device_states = {}
        self.anomaly_list = []
        # v6.0: Flow priority queues (M4: maxlen=200)
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

        # v6.2 [Implicit ACK]: track sent commands, mark unavailable after 60s without batch confirmation
        self._pending_cmd_acks = {}  # {"gw:dev": {"ts": timestamp, "gw": gw, "dev": dev}}
        self._implicit_ack_timeout = 60  # 3 slot cycles × 20s = 60s

    # --- Helpers ---
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly', {}).get('auto_clear', {}).get(atype, True)

    # =====================================================
    # v6.2 [Implicit ACK]: Command confirmation via batch
    # =====================================================
    def _record_cmd_ack(self, gw, dev):
        """Record that a command was sent — wait for batch confirmation within 60s."""
        key = f"{gw}:{dev}"
        with self.lock:
            self._pending_cmd_acks[key] = {"ts": now_ts(), "gw": gw, "dev": dev}
        log.debug('CMD', f"⏳ [ImplicitACK] Tracking {gw}/{dev} — 60s window")

    def _confirm_cmd_ack(self, gw, dev):
        """Called from _handle_status when batch/status arrives — confirms device responded."""
        key = f"{gw}:{dev}"
        with self.lock:
            if key in self._pending_cmd_acks:
                elapsed = now_ts() - self._pending_cmd_acks[key]['ts']
                del self._pending_cmd_acks[key]
                log.debug('CMD', f"✅ [ImplicitACK] {gw}/{dev} confirmed in {elapsed:.1f}s")

    def _check_expired_cmd_acks(self):
        """Check for commands without batch confirmation after 60s → mark unavailable.
        Called from _watchdog_loop."""
        now = now_ts()
        expired = []
        with self.lock:
            for key, info in list(self._pending_cmd_acks.items()):
                if now - info['ts'] > self._implicit_ack_timeout:
                    expired.append(info)
                    del self._pending_cmd_acks[key]
        for info in expired:
            gw, dev = info['gw'], info['dev']
            log.warn('CMD', f"⚠️ [ImplicitACK] {gw}/{dev} no batch confirmation after {self._implicit_ack_timeout}s — marking unavailable")
            self.ha.pub_dev_available(gw, dev, False)

    # =====================================================
    # ANOMALY MANAGEMENT (unchanged logic)
    # =====================================================
    def _add_anomaly(self, gw, dev, atype, value, ts=None):
        anomaly_id = f"{gw}_{dev}_{atype}".replace(' ', '_').lower()
        is_recovery = atype in ('battery_ok', 'device_online', 'temp_ok', 'hum_ok', 'stagnation_clear')

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
            if any(a['id'] == anomaly_id for a in self.anomaly_list):
                if self._startup_phase: self._dump_received_ids.add(anomaly_id)
                return
            anomaly = {"id": anomaly_id, "gw": gw, "dev": dev, "type": atype, "value": value,
                       "time": now_str(), "since_ts": ts,
                       "detected_at": datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')}
            self.anomaly_list.append(anomaly)
        if self._startup_phase: self._dump_received_ids.add(anomaly_id)
        self.ha.reg_anomaly(anomaly_id, gw, dev, atype, value, anomaly.get('detected_at'))
        self.ha.pub_anomaly(anomaly)
        self._publish_gw_stats(gw); self._publish_ha_anomaly_topics()
        if atype == 'device_offline':
            self.ha.pub_dev_available(gw, dev, False)
            self._publish_offline_values(gw, dev)
        vmap = {'low_battery':'battery','critical_battery':'battery','temp_high':'temperature','temp_low':'temperature','hum_high':'humidity','hum_low':'humidity'}
        sk = vmap.get(atype)
        if sk and value is not None: self._update_device_state_value(gw, dev, sk, value)
        log.info('ANOMALY', f"🔔 {gw}/{dev}: {atype}={value}")

    def _publish_offline_values(self, gw, dev):
        sk = f"{gw}:{dev}"
        with self.lock:
            if sk not in self.device_states: self.device_states[sk] = {}
            self.device_states[sk]['temperature'] = '--'
            self.device_states[sk]['humidity'] = '--'
            cached = dict(self.device_states[sk])
        cached['last_seen'] = cached.get('last_seen', '--')
        self.ha.pub_dev_state(gw, dev, cached)

    def _remove_anomalies_for_device(self, gw, dev, types):
        with self.lock: to_rm = [a for a in self.anomaly_list if a['gw']==gw and a['dev']==dev and a['type'] in types]
        for a in to_rm:
            with self.lock: self.anomaly_list = [x for x in self.anomaly_list if x['id'] != a['id']]
            self.ha.remove_anomaly(a['id'])
            if a['type'] == 'device_offline': self.ha.pub_dev_available(gw, dev, True)
        if to_rm: self._publish_gw_stats(gw); self._publish_ha_anomaly_topics()

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
            if anomaly['type'] == 'device_offline': self.ha.pub_dev_available(anomaly['gw'], anomaly['dev'], True)
            self._publish_gw_stats(anomaly['gw']); self._publish_ha_anomaly_topics()
            with self.lock:
                gwd = self._gw_devices.get(anomaly['gw'], set())
                exists = anomaly['dev'] in gwd if gwd else anomaly['dev'] in self.devices
            if send_clear and exists:
                self._add_queue({"t":"an_clr","g":anomaly['gw'],"d":anomaly['dev'],"a":anomaly['type']}, FlowPriority.COMMAND)
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
                    self._add_queue(pkt, FlowPriority.COMMAND)
                log.info('ANOMALY', f"🔄 Batch clear {gw}: {len(items)} items → {len(packets)} ac_b packet(s)")
        # Update stats
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
            data = {"state":"online" if g['online'] else "offline", "uptime":g['uptime'],
                    "last_seen":g['last_seen'] or "--", "devices_total":g['devices_total'],
                    "devices_monitored":g['devices_monitored'], "devices_offline":off,
                    "devices_low_battery":bat, "devices_anomaly":oth,
                    # v5.0: Extended diagnostics
                    "airtime": g.get('airtime', 0),
                    "rssi": g.get('rssi', '--'),
                    "disc_hash": g.get('disc_hash', '--'),
                    "z2m_age": g.get('z2m_age', -1),
                    "z2m_stale": g.get('z2m_stale', False),
                    # v6.3: Latency tracking
                    "batch_latency": g.get('batch_latency', 0),
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

    # =====================================================
    # REQ-5: Gateway offline → ALL devices offline (unchanged)
    # =====================================================
    def _gateway_went_offline(self, gw):
        log.warn('PING', f"💀 {gw} OFFLINE — marking all devices unavailable")
        with self.lock:
            devs = [(dev, info) for dev, info in self.devices.items() if info.get('gateway') == gw]
        for dev, info in devs:
            self.ha.pub_dev_available(gw, dev, False)
            self._publish_offline_values(gw, dev)

    def _gateway_came_online(self, gw):
        """v6.1: Smart recovery when gateway comes back online.
        1. Always send req_st (H4) — gateway sends full state snapshot
        2. Differential Sync: only send discovery if hash unknown or changed
        3. Only send config if discovery is needed (new/changed config)
        4. Always send dump_anom to reconcile anomaly state
        """
        log.info('PING', f"✅ {gw} ONLINE — starting recovery sequence")

        # H4: Request full state snapshot first (highest priority)
        self._add_queue({"t": "req_st", "g": gw}, FlowPriority.COMMAND)
        log.info('STATUS', f"📥 [H4] Sent req_st → {gw} (state recovery)")

        # Differential Sync: check if we have a cached hash for this gateway
        cached_hash = self._gw_disc_hash.get(gw, '')
        if cached_hash:
            # We have a hash — send discovery WITH hash (gateway will disc_ack if match)
            log.info('DISC', f"🔭 [DiffSync] {gw} cached hash={cached_hash} — sending discovery with hash check")
            self._send_discovery(gw, force=False)
        else:
            # No cached hash — force full discovery + config
            log.info('DISC', f"🔭 [DiffSync] {gw} no cached hash — forcing full discovery + config")
            self._send_discovery(gw, force=True)
            time.sleep(2)
            self._send_cfg(gw)

        # Always reconcile anomalies
        time.sleep(2)
        self._add_queue({"t": "dump_anom", "g": gw}, FlowPriority.COMMAND)

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
        self._add_queue(msg, FlowPriority.COMMAND)
        log.info('VBTN', f"🔘 {gw}/{vid} → {action}")

    def _send_vswitch(self, gw, vid, val):
        """Send virtual switch command to gateway via LoRa."""
        msg = {"t":"vsw","g":gw,"id":vid,"v":val,"seq":self._next_seq()}
        self._add_queue(msg, FlowPriority.COMMAND)
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
                    for t in [f"{p}/sensor/lora_an_{sid}/config",f"{p}/button/lora_an_{sid}_clear/config",f"{pf}/anomaly/{sid}"]:
                        self.mqtt_client.publish(t,"",retain=True)
            self._retained_anomaly_ids.clear(); self._dump_received_ids.clear()
            self._prune_anomalies()
        threading.Thread(target=_finish, daemon=True).start()

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

    def _evaluate_and_send_schedule(self):
        """Update MQTT status ONLY. LoRa mode notifications happen ONLY via _schedule_loop on transitions.
        Full calendar data goes through _targeted_calendar_sync."""
        # No LoRa broadcast here — prevents flood on add/clear/modify
        pass

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
        self._evaluate_and_send_schedule()
        self._publish_calendar_status()
        log.info('CAL', f"📅 Recurring: {count} slots ({note}) {time_start}-{time_end} days={days} weeks={weeks} → {target}")

    # =====================================================
    # CALENDAR SYNC — via CalendarTransfer (CRC + retransmit, PROVEN)
    # =====================================================
    def _targeted_calendar_sync(self, gateways=None):
        """Send compact schedule to gateways via CalendarTransfer.
        Override: if GW in gateway_override → use calendar.lora_{gw} data (from schedules.json[GW])
        Default: use prepare_compact(gw) = merge GLOBAL + GW"""
        enabled = CONFIG.get('calendar_sync', {}).get('enabled_gateways', CONFIG['gateways'])
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
        orphan_ctr = 0; ant_ctr = 0
        while self.running:
            time.sleep(30); now = now_ts()
            with self.lock:
                for gw, info in self.gateways.items():
                    # v5.0: Use heartbeat_timeout (8 min) instead of old gateway_timeout (5 min)
                    hb_timeout = CONFIG.get('heartbeat_timeout', 480)
                    if info['online'] and info['last_seen_ts']>0 and now-info['last_seen_ts']>hb_timeout:
                        info['online'] = False
                        threading.Thread(target=self._gateway_went_offline, args=(gw,), daemon=True).start()
            for gw in self.gateways: self._publish_gw_stats(gw)
            orphan_ctr += 1
            if orphan_ctr >= 10:
                orphan_ctr = 0
                if not self._startup_phase: self._prune_anomalies()
            self.cal_transfer.cleanup_stale()
            # v6.2 [Implicit ACK]: check for expired command confirmations
            self._check_expired_cmd_acks()
            ant_ctr += 1
            if ant_ctr >= 4:
                ant_ctr = 0; self._publish_antenna_status()

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
            time.sleep(1)  # v6.3 [M1]: 1s polling for pre_notify precision
            now = now_ts()
            for gw in CONFIG['gateways']:
                mode, nxt, src = self.scheduler.compute_now_and_next(gw)
                prev = last_modes.get(gw)

                # v6.3 [M1]: pre_notify 5s before mode transition
                if nxt > 0 and (nxt - now) <= 5 and (nxt - now) > 0:
                    if pre_notified.get(gw) != nxt:
                        pre_notified[gw] = nxt
                        # Determine what mode comes next (after nxt timestamp)
                        next_mode = 0  # default: no production
                        # Check if nxt is a slot start or end
                        for s in self.scheduler.get_effective_schedule(gw):
                            try:
                                st = self.scheduler._parse_ts(s['start'])
                                et = self.scheduler._parse_ts(s['end'])
                                if abs(st - nxt) < 60:
                                    next_mode = s.get('mode', 1)
                                    break
                            except: continue
                        next_name = mode_names.get(next_mode, f"MODE_{next_mode}")
                        self._add_queue({
                            "t": "sch_pre", "g": gw, "mode": next_mode,
                            "in_sec": int(nxt - now), "next_ts": int(nxt)
                        }, FlowPriority.COMMAND)
                        log.info('CAL', f"📅 [M1] Pre-notify {gw}: {next_name} (mode={next_mode}) in {int(nxt - now)}s")

                # Mode ACTUALLY changed — send transition notification
                if prev is not None and prev != mode:
                    cur_name = mode_names.get(mode, f"MODE_{mode}")
                    prev_name = mode_names.get(prev, f"MODE_{prev}")
                    self._add_queue({
                        "t": "sch", "g": gw, "mode": mode,
                        "from_ts": int(now_ts()), "next_ts": nxt, "src": src
                    }, FlowPriority.COMMAND)
                    log.info('CAL', f"📅 [M2] Mode transition {gw}: {prev_name}({prev})→{cur_name}({mode}) (next: {nxt})")
                    self._publish_calendar_status()
                    pre_notified.pop(gw, None)  # Reset pre-notify tracker

                last_modes[gw] = mode

    def _heartbeat_loop(self):
        """v6.1: Passive heartbeat monitor + M6 Bootstrap.
        - Fallback PING if online gateway goes silent for 3× expected interval
        - M6 Bootstrap: PING gateways that never checked in (last_seen_ts == 0)
          so gateways defined in CONFIG are discovered even on first boot.
        """
        # M6: Initial bootstrap delay — give gateways time to start up
        _bootstrap_sent = set()
        _bootstrap_interval = 120  # re-ping unseen gateways every 2 min
        _last_bootstrap_ts = {gw: 0 for gw in CONFIG['gateways']}

        while self.running:
            time.sleep(60)
            now = now_ts()
            expected_interval = 300  # gateway heartbeat_interval
            fallback_threshold = expected_interval * 3  # 15 min — very conservative

            for gw, info in self.gateways.items():
                with self.lock:
                    ls_ts = info.get('last_seen_ts', 0)
                    is_online = info.get('online', False)

                # Existing: fallback PING for online-but-silent gateways
                if is_online and ls_ts > 0 and now - ls_ts > fallback_threshold:
                    log.warn('PING', f"🏓 {gw} no heartbeat for {int(now - ls_ts)}s — sending fallback PING")
                    self._add_queue({"t": "ping", "g": gw}, FlowPriority.COMMAND)

                # M6 Bootstrap: PING gateways that never checked in
                elif ls_ts == 0 and not is_online:
                    if now - _last_bootstrap_ts.get(gw, 0) >= _bootstrap_interval:
                        _last_bootstrap_ts[gw] = now
                        log.info('PING', f"🏓 [M6] Bootstrap PING → {gw} (never seen)")
                        self._add_queue({"t": "ping", "g": gw}, FlowPriority.COMMAND)

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

    def _send_ping(self, gw=None):
        msg = {"t":"ping"}
        if gw: msg["g"] = gw
        self._add_queue(msg, FlowPriority.COMMAND)

    def _send_discovery(self, gw=None, force=False):
        """Send discovery request. force=True skips hash → gateway always sends full response."""
        msg = {"t":"disc"}
        if gw:
            msg["g"] = gw
            if not force:
                known_hash = self._gw_disc_hash.get(gw, '')
                if known_hash: msg["hash"] = known_hash
            self._disc_refresh_gw.add(gw)
        else:
            self._disc_refresh_gw.update(CONFIG['gateways'])
            # Broadcast disc without hash = always full
        self._add_queue(msg, FlowPriority.BATCH)

    def _send_cfg(self, gw=None):
        msg = {"t":"cfg","to":{"sw":CONFIG['timeout']['switch'],"lt":CONFIG['timeout']['light'],
                               "sn":CONFIG['timeout']['sensor'],"bs":CONFIG['timeout']['binary_sensor']}}
        if gw: msg["g"]=gw
        self._add_queue(msg, FlowPriority.COMMAND)

    def _send_dump_anom_all(self):
        def _do():
            for gw in CONFIG['gateways']:
                self._add_queue({"t":"dump_anom","g":gw}, FlowPriority.COMMAND); time.sleep(2)
        threading.Thread(target=_do, daemon=True).start()

    def _loop(self):
        """v6.0: Priority dispatch with H2 Zero Loss — P0/P1 requeued on TX failure."""
        if time.time()-self.last_tx < CONFIG['lora']['tx_cooldown']: return
        for p in sorted(FlowPriority):
            if self.queue[p]:
                msg = self.queue[p].popleft()
                success = self._send_lora(msg, priority=p)
                # H2: On TX failure, P0/P1 return to front of queue
                if not success and p in (FlowPriority.ALARM_PRIO, FlowPriority.COMMAND):
                    self.queue[p].appendleft(msg)
                    log.warn('LORA', f"⚠️ {PRIORITY_LABELS[p]} TX failed — requeued (H2 Zero Loss)")
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
        log.info('MAIN', f"🚀 SUPERVISOR {CONFIG['id']} v6.3 (ETAP 4: Diagnostics & Precision)")
        log.info('MAIN', "="*50)
        self._setup_mqtt(); self.ha = HomeAssistant(self.mqtt_client); time.sleep(3)
        for gw in CONFIG['gateways']: self.ha.reg_gateway(gw)
        self.ha.reg_supervisor()
        self.antenna_mgr = AntennaManager(self._on_lora_receive)
        self.antenna_mgr.connect_all()
        threading.Thread(target=self.antenna_mgr.reconnect_loop, daemon=True, name="ant-reconnect").start()
        self._publish_antenna_status()
        log.info('MAIN', "✅ Ready!"); time.sleep(2)
        self._send_ping(); time.sleep(5); self._send_cfg(); time.sleep(5); self._send_discovery(force=True); time.sleep(5)
        self._last_ping_ts = now_ts()
        self._start_dump_cleanup()
        self._evaluate_and_send_schedule(); self._publish_calendar_status()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        threading.Thread(target=self._schedule_loop, daemon=True, name="schedule").start()
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
                      # v5.0: VIO command routing from HA dashboard
                      f"{pf}/+/vio/+/set", f"{pf}/+/vio/+/press",
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

            # --- CALENDAR MQTT from HA ---
            if topic == "ha/lora/schedule/add":
                try:
                    d = json.loads(payload)
                    self.scheduler.add_slot(d.get('target','GLOBAL'), d['start'], d['end'], d.get('mode',1), d.get('note',''))
                    self._evaluate_and_send_schedule(); self._publish_calendar_status()
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
                    self._evaluate_and_send_schedule(); self._publish_calendar_status()
                except Exception as e: log.error('CAL', f"production_set: {e}")
                return
            if topic == "ha/lora/schedule/remove":
                try:
                    d = json.loads(payload)
                    self.scheduler.remove_slot(d.get('target','GLOBAL'), d['id'])
                    self._evaluate_and_send_schedule(); self._publish_calendar_status()
                except: pass
                return
            if topic == "ha/lora/schedule/clear":
                try:
                    d = json.loads(payload) if payload.strip() else {}
                    self.scheduler.clear_slots(d.get('target','GLOBAL'))
                    self._evaluate_and_send_schedule(); self._publish_calendar_status()
                except: pass
                return
            if topic == "ha/lora/schedule/list_req": self._publish_calendar_status(); return
            if topic == "ha/lora/schedule/sync_push":
                # Request gateway to push its schedule to supervisor
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._add_queue({"t":"sch_pull_req","g":gw}, FlowPriority.COMMAND)
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
                    self._add_queue({"t":"sch_pull_req","g":gw}, FlowPriority.COMMAND)
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
                elif a == 'discovery_all': self._send_discovery(force=True)  # Manual = always full
                elif a == 'clear_all_anomalies': self._clear_all_anomalies()
                elif a == 'clear_offline': self._clear_anomalies_by_type('device_offline')
                elif a == 'clear_low_battery': self._clear_anomalies_by_type(['low_battery','critical_battery'])
                elif a == 'clear_other': self._clear_other_anomalies()
                elif a == 'dump_anom_all': self._send_dump_anom_all()
                elif a == 'prune_anomalies': self._prune_anomalies()
                return

            # Per-gateway commands
            if '/gw/' in topic and '/cmd/' in topic:
                gw = parts[2].upper(); a = parts[-1]
                if a == 'ping': self._add_queue({"t":"ping","g":gw}, FlowPriority.COMMAND)
                elif a == 'discovery': self._send_discovery(gw, force=True)  # Manual = always full
                elif a == 'clear_anomalies': self._clear_anomalies_by_gateway(gw)
                elif a == 'dump_anom': self._add_queue({"t":"dump_anom","g":gw}, FlowPriority.COMMAND)
                return

            # Device set/refresh
            if len(parts)>=4 and parts[-1] in ['set','refresh']:
                gw, dev = parts[1].upper(), self._find_device_name(parts[1].upper(), parts[2])
                if parts[-1]=='set':
                    self._add_queue({"t":"cmd","g":gw,"d":dev,"c":"state","v":payload}, FlowPriority.COMMAND)
                    # v6.2 [Implicit ACK]: track command, wait 60s for batch confirmation
                    self._record_cmd_ack(gw, dev)
                else:
                    self._add_queue({"t":"req","g":gw,"d":dev}, FlowPriority.COMMAND)
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
            if gw and gw in self.gateways: self._update_gw_last_seen(gw)

            # v5.0: Batch packets
            if t == 'b': self._handle_batch(data)
            elif t == 'b_st': self._handle_batch_state_recovery(data)  # v6.1 [H4]
            elif t == 'db': self._handle_discovery_batch(data)
            elif t == 'ab': self._handle_anomaly_batch(data)
            elif t == 'disc_meta': self._handle_disc_meta(data)
            elif t == 'disc_vio': self._handle_disc_vio(data)
            elif t == 'hb': self._handle_heartbeat(data)
            elif t == 'pong': self._handle_pong(data)
            elif t == 'st': self._handle_status(data)
            elif t == 'an': self._handle_anomaly(data)
            elif t == 'disc_resp': self._handle_discovery_resp(data)  # legacy compat
            elif t == 'disc_ack': self._handle_disc_ack(data)
            elif t == 'vsw_st': self._handle_vswitch_status(data)
            elif t == 'sch_event': self._handle_schedule_event(data)
            elif t == 'sch_b': self.cal_transfer.handle_begin(data)
            elif t == 'sch_c': self.cal_transfer.handle_chunk(data)
            elif t == 'sch_e':
                result = self.cal_transfer.handle_end(data)
                if result: self._process_calendar_transfer(*result)
            elif t == 'sch_a': self.cal_transfer.handle_ack(data)
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
        """Unpack batch packet and process each item as a status update.
        Format: {"t":"b","g":"G1","ts":<epoch>,"d":[[sid,{payload}],...]}
        - ts: batch-level timestamp → used as last_seen for ALL devices in batch
        - sid: short integer ID → resolved via _dev_short_map
        - Payloads have compressed values (0/1) → decompressed by _handle_status
        """
        gw = data.get('g')
        batch_ts = data.get('ts', int(now_ts()))
        # Convert epoch to last_seen string
        try:
            batch_ls = datetime.fromtimestamp(batch_ts).strftime('%Y-%m-%d %H:%M:%S')
        except: batch_ls = now_str()
        items = data.get('d', [])
        sid_map = self._dev_short_map.get(gw, {})
        resolved = 0; unresolved = 0
        for item in items:
            if not isinstance(item, list) or len(item) < 2:
                continue
            raw_id, payload = item[0], item[1]
            if not isinstance(payload, dict):
                continue
            # Resolve short ID → full device name
            if isinstance(raw_id, int):
                dev = sid_map.get(raw_id)
                if dev is None:
                    unresolved += 1
                    continue
                resolved += 1
            else:
                dev = raw_id
                resolved += 1
            # Inject batch-level ls if not already in payload
            status_data = {'t': 'st', 'g': gw, 'd': dev, 'ls': batch_ls}
            status_data.update(payload)
            self._handle_status(status_data)
        # v6.3 [Latency Tracking]: compute batch transit delay
        latency_ms = int((now_ts() - batch_ts) * 1000) if batch_ts > 0 else 0
        latency_s = round(latency_ms / 1000, 1)
        with self.lock:
            if gw in self.gateways:
                self.gateways[gw]['batch_latency'] = latency_s
        log.info('BATCH', f"📦 Batch {gw}: {resolved} ok, {unresolved} unknown (ts={batch_ts}, latency={latency_s}s)")
        if unresolved > 0:
            log.warn('BATCH', f"⚠️ {gw} unknown sids — forcing discovery")
            self._gw_disc_hash[gw] = ''
            self._send_discovery(gw, force=True)

    # =====================================================
    # v6.1 [H4]: STATE RECOVERY BATCH — full snapshot from gateway
    # =====================================================
    def _handle_batch_state_recovery(self, data):
        """H4: Process full state snapshot after gateway comes back online.
        Format: {"t":"b_st","g":"G1","ts":<epoch>,"d":[[sid,{payload}],...]}
        Same processing as regular batch, but logs as recovery and restores
        availability for all devices in the snapshot.
        """
        gw = data.get('g')
        batch_ts = data.get('ts', int(now_ts()))
        try:
            batch_ls = datetime.fromtimestamp(batch_ts).strftime('%Y-%m-%d %H:%M:%S')
        except: batch_ls = now_str()
        items = data.get('d', [])
        sid_map = self._dev_short_map.get(gw, {})
        resolved = 0; unresolved = 0
        for item in items:
            if not isinstance(item, list) or len(item) < 2:
                continue
            raw_id, payload = item[0], item[1]
            if not isinstance(payload, dict):
                continue
            # Resolve short ID → full device name
            if isinstance(raw_id, int):
                dev = sid_map.get(raw_id)
                if dev is None:
                    unresolved += 1
                    continue
                resolved += 1
            else:
                dev = raw_id
                resolved += 1
            # Inject batch-level ls if not already in payload
            status_data = {'t': 'st', 'g': gw, 'd': dev, 'ls': payload.get('ls', batch_ls)}
            status_data.update(payload)
            self._handle_status(status_data)
        log.info('STATUS', f"📥 [H4] State recovery {gw}: {resolved} devices restored, {unresolved} unknown (ts={batch_ts})")
        if unresolved > 0 and resolved == 0:
            log.warn('STATUS', f"⚠️ [H4] {gw} all sids unknown — need discovery first")
            self._gw_disc_hash[gw] = ''
            self._send_discovery(gw, force=True)

    # =====================================================
    # v6.1: ENHANCED PONG HANDLER with Differential Sync
    # =====================================================
    def _handle_pong(self, data):
        gw = data.get('g')
        with self.lock:
            self.gateways[gw].update({
                'uptime': data.get('up', 0),
                'devices_total': data.get('dev_total', 0),
                'devices_monitored': data.get('dev_mon', 0),
                'airtime': data.get('air', 0),
                'anom_blocked': data.get('anom_blk', 0),
                'rssi': data.get('rssi', None),
            })
            # v6.1 Differential Sync in PONG (same logic as heartbeat)
            new_hash = data.get('hash', '')
            if new_hash:
                old_hash = self._gw_disc_hash.get(gw, '')
                self._gw_disc_hash[gw] = new_hash
                self.gateways[gw]['disc_hash'] = new_hash
                if old_hash and old_hash != new_hash:
                    log.info('DISC', f"🔭 [DiffSync] {gw} PONG hash changed: {old_hash} → {new_hash} — resync")
                    self._send_discovery(gw, force=True)
                elif old_hash and old_hash == new_hash:
                    log.debug('DISC', f"🔭 [DiffSync] {gw} PONG hash match — skip disc+cfg")
        self._publish_gw_stats(gw)
        air = data.get('air', 0)
        rssi = data.get('rssi', '--')
        anom_blk = data.get('anom_blk', 0)
        log.info('PING', f"🏓 PONG from {gw}: up={data.get('up',0)}s air={air}% rssi={rssi} anom_blk={anom_blk} hash={data.get('hash','?')}")

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
    def _handle_discovery_batch(self, data):
        """Packet: {"t":"db","g":"G1","d":[[sid,"name","type",["cap1",...]], ...]}
        Processes all devices from a single batch packet."""
        gw = data.get('g')
        items = data.get('d', [])
        first_in_refresh = gw in self._disc_refresh_gw
        for item in items:
            if not isinstance(item, list) or len(item) < 4: continue
            sid, dev, dtype, caps = item[0], item[1], item[2], item[3]
            # Register HA entities
            if dtype in ['switch', 'light']: self.ha.reg_switch(gw, dev, dtype)
            elif dtype == 'sensor': self.ha.reg_sensor(gw, dev, caps)
            elif dtype == 'binary_sensor': self.ha.reg_binary(gw, dev, caps)
            # Update device registry
            with self.lock:
                self.devices[dev] = {'gateway': gw, 'type': dtype, 'caps': caps, 'available': True, 'last_seen': now_str()}
            # Store short ID mapping
            if gw in self._dev_short_map:
                self._dev_short_map[gw][sid] = dev
            # Track devices per gateway
            if first_in_refresh:
                self._gw_devices[gw] = {dev}
                self._disc_refresh_gw.discard(gw)
                first_in_refresh = False
            else:
                if gw not in self._gw_devices: self._gw_devices[gw] = set()
                self._gw_devices[gw].add(dev)
            self.ha.pub_dev_state(gw, dev, {'last_seen': now_str()})
            self.ha.pub_dev_available(gw, dev, True)
        log.info('DISC', f"🔭 {gw} db: {len(items)} devices registered")

    # =====================================================
    # v5.0: ANOMALY BATCH — replaces N individual 'an' from dump
    # =====================================================
    def _handle_anomaly_batch(self, data):
        """Packet: {"t":"ab","g":"G1","d":[["dev","atype",value,ts], ...]}
        Processes dump_anom results in one batch instead of N individual packets."""
        gw = data.get('g')
        items = data.get('d', [])
        for item in items:
            if not isinstance(item, list) or len(item) < 3: continue
            dev, atype = item[0], item[1]
            value = item[2] if len(item) > 2 else None
            ts = item[3] if len(item) > 3 else int(now_ts())
            self._add_anomaly(gw, dev, atype, value, ts)
        log.info('ANOMALY', f"📋 {gw} ab: {len(items)} anomalies from dump")

    # =====================================================
    # v5.0: HEARTBEAT HANDLER (gateway self-reports)
    # =====================================================
    def _handle_heartbeat(self, data):
        """v6.1: Process proactive heartbeat from gateway.
        Differential Sync: hash match → skip discovery+config (save LoRa bandwidth).
        Hash change → trigger discovery + config resend.
        """
        gw = data.get('g')
        if not gw or gw not in self.gateways:
            return
        z2m_age = data.get('z2m', -1)
        z2m_stale = z2m_age > CONFIG.get('z2m_stale_threshold', 900) if z2m_age >= 0 else False

        with self.lock:
            self.gateways[gw].update({
                'uptime': data.get('up', 0),
                'devices_total': data.get('dev', 0),
                'devices_monitored': data.get('mon', 0),
                'airtime': data.get('air', 0),
                'anom_blocked': data.get('abl', 0),
                'rssi': data.get('rssi', None),
                'z2m_age': z2m_age,
                'z2m_stale': z2m_stale,
            })
            # v6.1 Differential Sync: compare hash from heartbeat with cache
            new_hash = data.get('hash', '')
            if new_hash:
                old_hash = self._gw_disc_hash.get(gw, '')
                self._gw_disc_hash[gw] = new_hash
                self.gateways[gw]['disc_hash'] = new_hash
                if not old_hash:
                    # First heartbeat — store hash, no action (online handler already ran)
                    log.info('DISC', f"🔭 [DiffSync] {gw} first hash cached: {new_hash}")
                elif old_hash != new_hash:
                    # Hash CHANGED — config/devices changed on gateway side
                    log.info('DISC', f"🔭 [DiffSync] {gw} hash changed: {old_hash} → {new_hash} — resync")
                    def _resync():
                        time.sleep(1)
                        self._send_discovery(gw, force=True)
                        time.sleep(2)
                        self._send_cfg(gw)
                    threading.Thread(target=_resync, daemon=True).start()
                else:
                    # Hash matches — skip discovery + config (bandwidth saved)
                    log.debug('DISC', f"🔭 [DiffSync] {gw} hash={new_hash} — match, skip disc+cfg")

        # Update vswitch states from heartbeat
        vs = data.get('vs')
        if vs and isinstance(vs, dict):
            self._gw_vswitch_states[gw] = vs

        # Log
        rssi = data.get('rssi', '--')
        log.info('PING', f"💓 HB {gw}: up={data.get('up',0)}s z2m_age={z2m_age}s" +
                 (f" ⚠️Z2M STALE" if z2m_stale else "") +
                 f" air={data.get('air',0)}% rssi={rssi}")

        # If Z2M stale, mark devices with stale warning
        if z2m_stale:
            log.warn('PING', f"⚠️ {gw} Z2M stale ({z2m_age}s) — device data may be frozen")

        self._publish_gw_stats(gw)

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
        # v6.2 [Implicit ACK]: clear pending command ACK for this device
        self._confirm_cmd_ack(gw, dev)

    def _handle_anomaly(self, data):
        self._add_anomaly(data.get('g'), data.get('d'), data.get('a'), data.get('v'), data.get('ts', int(now_ts())))

    def _handle_discovery_resp(self, data):
        gw, dev, dtype, caps = data.get('g'), data.get('d'), data.get('dt'), data.get('cap',[])
        ls = data.get('ls','unknown')
        sid = data.get('sid')  # v5.0: short ID from gateway
        if dtype in ['switch','light']: self.ha.reg_switch(gw, dev, dtype)
        elif dtype == 'sensor': self.ha.reg_sensor(gw, dev, caps)
        elif dtype == 'binary_sensor': self.ha.reg_binary(gw, dev, caps)
        with self.lock: self.devices[dev] = {'gateway':gw,'type':dtype,'caps':caps,'available':True,'last_seen':ls}
        # v5.0: Store short ID mapping for batch decoding
        if sid is not None and gw in self._dev_short_map:
            self._dev_short_map[gw][sid] = dev
            log.debug('DISC', f"🔢 {gw} sid={sid} → {dev}")
        if gw in self._disc_refresh_gw:
            self._gw_devices[gw] = {dev}; self._disc_refresh_gw.discard(gw)
            # v5.0: Clear old short ID map on full refresh
            if gw in self._dev_short_map and sid is not None:
                self._dev_short_map[gw] = {sid: dev}
        else:
            if gw not in self._gw_devices: self._gw_devices[gw] = set()
            self._gw_devices[gw].add(dev)
        self.ha.pub_dev_state(gw, dev, {'last_seen':ls}); self.ha.pub_dev_available(gw, dev, True)

    def _handle_schedule_event(self, data):
        gw, mode = data.get('g'), data.get('mode', 0)
        start = datetime.now().isoformat()
        end = datetime.fromtimestamp(now_ts()+43200).isoformat()
        self.scheduler.add_slot(gw, start, end, mode, f"GW {gw} override")
        self._evaluate_and_send_schedule(); self._publish_calendar_status()


if __name__ == "__main__":
    Supervisor().start()
