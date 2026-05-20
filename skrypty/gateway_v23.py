#!/usr/bin/env python3
"""
LoRa Zigbee Gateway - v6.3 (ETAP 4: Diagnostics & Precision)

Schedule Sync: via CalendarTransfer (CRC + retry + zlib compression)
  SUP→GW: sch_b → sch_c×N → sch_e → sch_a (compact: [[sm,dm,mode],...])
  GW→SUP: same (direction='gw_push')
  14 slots = 104B compressed = 1 chunk = 4 LoRa packets
  Mode system: 0=BRAK, 1=PRODUKCJA, 2=PRZERWA, 3=SERWIS (CONFIG)

Other v5.1 features:
- Anomaly batching: non-critical → ab batch, critical → immediate P0
- disc_meta split: VIO in separate disc_vio packet
- Size guard: packets >220B dropped before TX
- Antenna crash recovery: mesh.close() on TX error
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import json, time, random, threading, os, zlib, base64, hashlib, string
import urllib.request, urllib.error
from datetime import datetime
from collections import deque
from enum import IntEnum
import paho.mqtt.client as mqtt
import meshtastic, meshtastic.serial_interface
from pubsub import pub

PERSIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gw_state.json')
SCHEDULE_FILE = os.environ.get('LORA_SCHEDULE_FILE',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schedule.json'))

CONFIG = {
    "id": "G1",
    "mesh_port": "/dev/ttyUSB1",
    "mesh_reconnect": {"enabled": True, "interval": 10, "max_backoff": 120},
    "mqtt": {"host": "172.17.0.1", "port": 1883, "user": "mqtt", "pass": "REPLACE_ME"},
    "lora": {"max_size": 220, "tx_cooldown": 2.5, "cal_chunk_delay": 6.0},
    # Slot system — 20s windows: G1(0-19s), G2(20-39s), G3(40-59s)
    "slot": {"enabled": False, "index": 0, "count": 3, "window": 20},
    # Batcher: collects status/measurements, flushes every 30s
    "batcher": {"enabled": True, "interval": 30, "max_payload": 220,
                "max_items": 12},  # v6.3 [M3]: flush when buffer reaches 12 items
    # v6.3 [Airtime Guard]: block TX if airtime exceeds threshold
    "airtime_guard": {"enabled": True, "max_seconds_per_min": 10, "block_duration": 30},
    # Cyclic report intervals per device type (seconds)
    "report_intervals": {"switch": 300, "light": 300, "sensor": 300, "binary_sensor": 300},
    "report_mode": {"switch": "event", "light": "event", "sensor": "cyclic", "binary_sensor": "event"},
    # Timeout: device considered offline after this many seconds of silence
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},
    # Monitored devices — only these are reported via LoRa
    "monitored": ["Test 1", "Test 2", "Temp 1", "Temp 2", "Temp 3", "Temp 4"],
    # Priority devices — auto-monitored + bypass slot/batcher, instant CRITICAL TX
    "priority_devices": ["Door 1", "Leak 1"],
    # Command retry for mains-powered devices
    "cmd_retry": {"enabled": True, "timeout": 5, "retries": 1},
    # Delta reporting: skip cyclic report if value unchanged beyond threshold
    # Reduces LoRa traffic 5-10x for stable environments
    "delta": {
        "enabled": False,
        "temperature": 0.2,    # °C — report only if changed by this much
        "humidity": 2,         # % — report only if changed by this much
        "battery": 5,          # % — report only if changed by this much
        "brightness": 10,      # units — report only if changed by this much
    },
    # Anomaly detection config (gateway-only)
    "anomaly": {
        "check_interval": 60,
        "detection": {
            "low_battery": {"enabled": True, "threshold": 25},
            "critical_battery": {"enabled": True, "threshold": 15},
            "device_offline": {"enabled": True},
            "temp_high": {"enabled": True, "threshold": 30},
            "temp_low": {"enabled": True, "threshold": -10},
            "hum_high": {"enabled": True, "threshold": 90},
            "hum_low": {"enabled": True, "threshold": 5},
            "stagnation": {"enabled": True, "hours": 48}
        },
        "auto_clear": {
            "device_offline": True, "low_battery": True, "critical_battery": True,
            "temp_high": True, "temp_low": True, "hum_high": False, "hum_low": True,
            "stagnation": True
        }
    },
    "critical_alarm": {"enabled": True, "types": ["smoke","water_leak"], "repeats": 3, "repeat_delay": 2.0, "cooldown": 10},
    "calendar": {"chunk_size": 140, "transfer_timeout": 60, "retry_max": 2},
    # Proactive heartbeat — gateway self-reports every 5 min
    "heartbeat_interval": 300,
    # Log rotation: max 5MB per file, keep 3 rotated copies
    "log": {"file": "/tmp/gateway.log", "max_bytes": 5_000_000, "backup_count": 3},
    # Schedule mode names — must match supervisor CONFIG
    # mode 0 = default when no event active
    "mode_names": {
        0: "BRAK PRODUKCJI",
        1: "PRODUKCJA",
        2: "PRZERWA",
        3: "SERWIS",
    },
    # Virtual I/O — switches (stateful ON/OFF) + buttons (fire event)
    # Defined HERE (gateway only). Supervisor learns via disc_meta.
    "virtual_io": [
        {"id": "vs_prod",  "type": "switch", "name": "Tryb Produkcji", "default": 0},
        {"id": "vs_night", "type": "switch", "name": "Tryb Nocny",     "default": 0},
        {"id": "vb_hall",  "type": "button", "name": "Hol Włącz"},
        {"id": "vb_panic", "type": "button", "name": "Alarm Ręczny"},
    ],
    # HA REST API — for direct calendar management (no automation middleman)
    # Generate token: HA → Profile → Long-Lived Access Tokens → Create
    "ha_api": {
        "url": "http://localhost:8123",
        "token": "",  # PASTE YOUR LONG-LIVED TOKEN HERE
        "calendar_entity": "",  # Auto: "calendar.lora_{gw_id}" if empty
        # ICS file path — BEST METHOD (direct file write, zero API issues)
        # Find it: ls /config/.storage/local_calendar/
        # Typical: /config/.storage/local_calendar/LoRa G1.ics
        "ics_path": "/var/lib/homeassistant/homeassistant/.storage/local_calendar.g1.ics",  # e.g. "/config/.storage/local_calendar/LoRa G1.ics"
    },
}


# =====================================================
# v6.0: FLOW PRIORITY — ETAP 1 hierarchy
# P0: Safety alarms + priority device state changes
# P1: Commands from supervisor, system responses
# P2: Anomalies/offline for regular monitored devices
# P3: Cyclic reports (temp, battery) via Batcher
# ALL priorities respect slot windows (no bypass)
# =====================================================
class FlowPriority(IntEnum):
    ALARM_PRIO  = 0   # P0: Priority device state changes + safety anomalies
    COMMAND     = 1   # P1: Commands t:cmd, system responses (pong, disc, vsw_st)
    DIAGNOSTIC  = 2   # P2: Anomalies/offline for regular monitored devices
    BATCH       = 3   # P3: Cyclic reports via Batcher

# Keep old Priority name for backward compat in internal logic
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
    COMP = {'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'ZIGBEE': '📶', 'PING': '🏓',
            'DISC': '🔭', 'CMD': '⚡', 'STATUS': '📤', 'CFG': '⚙️', 'SLOT': '⏱️',
            'OFFLINE': '💀', 'ANOMALY': '🚨', 'RETRY': '🔄', 'CAL': '📆', 'SYNC': '🔄',
            'VBTN': '🔘', 'BATCH': '📦'}
    def __init__(self):
        from logging.handlers import RotatingFileHandler
        log_cfg = CONFIG.get('log', {})
        self._handler = RotatingFileHandler(
            log_cfg.get('file', '/tmp/gateway.log'),
            maxBytes=log_cfg.get('max_bytes', 5_000_000),
            backupCount=log_cfg.get('backup_count', 3))
        self._handler.setFormatter(logging.Formatter('%(message)s'))
        self._flog = logging.getLogger('gw_file')
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
def parse_datetime(s):
    try: return datetime.strptime(s, '%Y-%m-%d %H:%M:%S').timestamp()
    except: return 0


def batch_split(packet_type, gw_id, items, max_payload=220):
    """Split a list of items into batch packets under max_payload bytes.
    Returns list of dicts: [{"t": packet_type, "g": gw_id, "d": [...items...]}, ...]
    """
    if not items: return []
    packets = []; current = []
    for item in items:
        test = current + [item]
        test_pkt = json.dumps({"t": packet_type, "g": gw_id, "d": test}, separators=(',', ':'))
        if len(test_pkt) > max_payload and current:
            packets.append({"t": packet_type, "g": gw_id, "d": current})
            current = [item]
        else:
            current = test
    if current:
        packets.append({"t": packet_type, "g": gw_id, "d": current})
    return packets


# =====================================================
# BATCHER — v6.3 [M3] Adaptive flush + dedup + batch-level timestamp
# =====================================================
class Batcher:
    """v6.3: Adaptive batch flush.
    Flush triggers (whichever comes first):
    1. Timer: 30s since last flush (unchanged)
    2. Item count: buffer reaches max_items (12) [M3]
    3. Priority: data from priority device arrives [M3]
    Batch format: {"t":"b","g":"G1","ts":<epoch>,"d":[[dev,{payload}],...]}
    """
    def __init__(self, gw_id, flush_callback, interval=30, max_payload=220,
                 max_items=12, priority_check_fn=None):
        self.gw_id = gw_id
        self.flush_callback = flush_callback
        self.interval = interval
        self.max_payload = max_payload
        self.max_items = max_items
        self._is_priority = priority_check_fn  # v6.3: callable(dev) → bool
        self.buffer = {}  # {dev_id: payload_dict} — DEDUP: last wins
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.running = True
        self._stats_batched = 0
        self._stats_flushed = 0
        self._stats_deduped = 0

    def add(self, dev, payload):
        """Add to buffer. Triggers immediate flush for priority devices [M3]."""
        flush_now = False
        with self.lock:
            if dev in self.buffer:
                self._stats_deduped += 1
            self.buffer[dev] = payload
            self._stats_batched += 1
            # M3: Check immediate flush triggers
            if len(self.buffer) >= self.max_items:
                flush_now = True
            elif self._is_priority and self._is_priority(dev):
                flush_now = True
        if flush_now:
            self.flush()

    def start(self):
        threading.Thread(target=self._flush_loop, daemon=True, name="batcher").start()

    def _flush_loop(self):
        while self.running:
            time.sleep(1)
            if time.time() - self.last_flush >= self.interval:
                self.flush()

    def flush(self):
        """Flush buffer: build batch packets with batch-level ts, deduped."""
        with self.lock:
            items = list(self.buffer.items())  # [(dev_id, payload), ...]
            self.buffer.clear()
            self.last_flush = time.time()
        if not items:
            return
        packets = self._split_into_packets(items)
        for pkt in packets:
            self.flush_callback(pkt)
            self._stats_flushed += 1
        log.info('BATCH', f"📦 Flushed {len(items)} items → {len(packets)} pkt(s)")

    def _split_into_packets(self, items):
        packets = []; current = []
        for item in items:
            test = current + [item]
            if len(self._serialize(test)) > self.max_payload and current:
                packets.append(self._build_dict(current))
                current = [item]
            else:
                current = test
        if current:
            packets.append(self._build_dict(current))
        return packets

    def _build_dict(self, items):
        return {"t": "b", "g": self.gw_id, "ts": int(time.time()),
                "d": [[dev, payload] for dev, payload in items]}

    def _serialize(self, items):
        return json.dumps({"t":"b","g":self.gw_id,"ts":0,
                           "d":[[d,p] for d,p in items]}, separators=(',',':'))

    def pending_count(self):
        with self.lock: return len(self.buffer)

    @property
    def stats(self):
        return {"batched": self._stats_batched, "flushed": self._stats_flushed,
                "deduped": self._stats_deduped, "pending": self.pending_count()}


# =====================================================
# LOCAL SCHEDULE MANAGER (unchanged from v4.1)
# =====================================================
class LocalSchedule:
    def __init__(self, filepath=SCHEDULE_FILE):
        self.filepath = filepath; self.lock = threading.Lock(); self.slots = []; self._load()
    def _load(self):
        try:
            with open(self.filepath, 'r') as f: self.slots = json.load(f).get('slots', [])
            log.info('CAL', f"📂 Loaded {len(self.slots)} local slots")
        except FileNotFoundError: log.info('CAL', "📂 No local schedule")
        except Exception as e: log.warn('CAL', f"📂 Load: {e}")
    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.filepath) or '.', exist_ok=True)
            with open(self.filepath, 'w') as f: json.dump({"slots": self.slots, "_saved": now_str()}, f, indent=2)
        except Exception as e: log.error('CAL', f"💾 Save: {e}")
    def _gen_id(self): return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    def add_slot(self, start, end, mode=1, note=""):
        s = {"id": self._gen_id(), "start": start, "end": end, "mode": mode,
             "note": note[:30], "updated_ts": int(now_ts()), "origin": CONFIG['id']}
        with self.lock: self.slots.append(s); self._save()
        return s['id']
    def remove_slot(self, sid):
        with self.lock: b=len(self.slots); self.slots=[s for s in self.slots if s['id']!=sid]; self._save()
        return b > len(self.slots)
    def clear_slots(self):
        with self.lock: self.slots=[]; self._save()
    def get_all(self):
        with self.lock: return list(self.slots)
    def compute_now_and_next(self):
        now = now_ts(); active = None; nxt = 0
        with self.lock: ss = sorted(self.slots, key=lambda x: x.get('updated_ts',0), reverse=True)
        for s in ss:
            try:
                st = datetime.fromisoformat(s['start']).timestamp() if isinstance(s['start'], str) else s['start']
                et = datetime.fromisoformat(s['end']).timestamp() if isinstance(s['end'], str) else s['end']
            except: continue
            if st <= now < et and active is None: active = (s['mode'], s.get('origin','?'))
            for tv in [st, et]:
                if tv > now and (nxt == 0 or tv < nxt): nxt = tv
        return (active[0], int(nxt), active[1]) if active else (0, int(nxt), "none")
    def merge_incoming(self, incoming):
        with self.lock:
            local = {s['id']: s for s in self.slots}; ch = 0
            for s in incoming:
                if s['id'] not in local or s.get('updated_ts',0) > local[s['id']].get('updated_ts',0):
                    local[s['id']] = s; ch += 1
            self.slots = sorted(local.values(), key=lambda s: s.get('start','')); self._save()
        return ch
    def replace_all(self, new_slots):
        with self.lock: self.slots = list(new_slots); self._save()


# =====================================================
# CHUNKED CALENDAR TRANSFER
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
        return base64.b64encode(zlib.compress(json.dumps(slots, separators=(',',':')).encode(), 9)).decode()
    @staticmethod
    def deserialize(b64): return json.loads(zlib.decompress(base64.b64decode(b64)))
    @staticmethod
    def crc16(d): return hashlib.md5(d.encode()).hexdigest()[:4]

    def _chunk_delay(self):
        """[K2] Delay between chunks — uses cal_chunk_delay (default 6s) instead of tx_cooldown+0.5."""
        return CONFIG['lora'].get('cal_chunk_delay', 6.0)

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock: self.outgoing[tid] = {'chunks':chunks,'gw':gw,'dir':direction,'retries':0,'ts':now_ts(),'ack_received':False}
        self.queue_fn({"t":"sch_b","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, FlowPriority.COMMAND)
        def _s():
            delay = self._chunk_delay()  # [K2]
            for i, c in enumerate(chunks):
                time.sleep(delay)
                self.queue_fn({"t":"sch_c","tid":tid,"s":i,"d":c}, FlowPriority.COMMAND)
            # [K4] Send sch_e with retry — LoRa packet loss recovery
            SCH_E_RETRIES = 3
            SCH_E_WAIT = 15
            for attempt in range(SCH_E_RETRIES):
                time.sleep(delay)
                with self.lock:
                    if tid not in self.outgoing:
                        return
                self.queue_fn({"t":"sch_e","tid":tid}, FlowPriority.COMMAND)
                if attempt > 0:
                    log.warn('SYNC', f"⚠️ sch_e retry tid={tid} attempt {attempt+1}/{SCH_E_RETRIES}")
                time.sleep(SCH_E_WAIT)
                with self.lock:
                    info = self.outgoing.get(tid)
                    if info is None:
                        return
                    if info.get('ack_received'):
                        return
            log.error('SYNC', f"❌ tid={tid} no response after {SCH_E_RETRIES} sch_e attempts")
            with self.lock: self.outgoing.pop(tid, None)
        threading.Thread(target=_s, daemon=True).start()
        return tid

    def handle_begin(self, data):
        tid = data.get('tid')
        with self.lock:
            self.incoming[tid] = {'chunks':{},'total':data.get('n',0),'crc':data.get('crc',''),
                                  'gw':data.get('g','?'),'dir':data.get('dir','push'),'ts':now_ts()}

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
        try: return (info['gw'], info['dir'], self.deserialize(b64))
        except: return None

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

    def cleanup_stale(self, age=120):
        now = now_ts()
        with self.lock:
            for st in [self.incoming, self.outgoing]:
                for tid in [t for t, i in st.items() if now-i.get('ts',0)>age]: del st[tid]


# =====================================================
# GATEWAY MAIN CLASS
# =====================================================
class Gateway:
    def __init__(self):
        self.mqtt = self.mesh = None
        self.devices = {}
        self.states = {}
        self.last_seen = {}
        self.z2m_av = {}
        self.pending_cmds = {}
        self.cmd_failed = {}
        self.last_report = {}
        # Delta reporting: last values sent via LoRa (for skip-if-unchanged logic)
        self._last_reported_values = {}  # {"dev_name": {"temperature": 22.5, "humidity": 45}}
        self.last_state_change = {}
        self.battery_state = {}
        self.offline_reported = {}
        self.stagnation_reported = {}
        self.temp_anomaly_reported = {}
        self.hum_anomaly_reported = {}
        self.critical_alarm_cooldown = {}
        # Production mode (from supervisor sch)
        self.production_mode = False
        self.production_next_change_ts = 0
        self.production_source = None
        self.production_last_update_ts = 0
        # Calendar + sync
        self.local_schedule = LocalSchedule()
        self.cal_transfer = None
        # LoRa reconnect
        self._mesh_connected = False
        self._mesh_reconnect_backoff = 0

        # v6.0: Flow priority queues (M4: maxlen=200 prevents unbounded growth)
        self.queue = {p: deque(maxlen=200) for p in FlowPriority}
        self.last_tx = 0
        self.start_time = time.time()
        self.running = True
        self.lock = threading.Lock()
        self.discovery_done = False

        # v5.0: Batcher instance (created in start())
        self.batcher = None

        # v5.0: Airtime tracking
        self._airtime_tx_count = 0
        self._airtime_tx_bytes = 0
        self._airtime_slot_used = 0.0  # seconds used in current slot window
        self._slot_window_start = 0.0

        # v5.0: Blocked anomaly counter (for PONG diagnostics)
        self._blocked_anomaly_count = 0

        # v5.0: Discovery hash cache
        self._disc_hash = ""

        # v5.0: Short ID map — auto-assigned at Z2M discovery
        # "Temp 1" → 0, "Leak 1" → 1, etc.
        self.dev_short_id = {}   # name → int
        self.dev_short_rev = {}  # int → name (reverse)

        # v5.0: Proactive heartbeat
        self._last_heartbeat_ts = 0
        self._last_z2m_ts = 0  # last time ANY Z2M data arrived

        # v5.0: Virtual switches — stateful ON/OFF flags
        self.vswitch_states = {}  # {"vs_prod": 0, "vs_night": 1}
        self._vio_mqtt_registered = set()
        # Command sequence tracking — reject out-of-order commands
        self._last_cmd_seq = {}  # {"vs_prod": 47, "vb_hall": 48}
        # VSwitch debounce — delay execution to absorb rapid clicks
        self._vsw_debounce = {}  # {"vs_prod": (value, timer_thread)}

        # v5.1: Anomaly buffer — non-critical anomalies batched per cycle
        self._anomaly_buffer = []

        # v6.2 [Echo Filter]: suppress LoRa status for 10s after command execution
        self._echo_suppress = {}  # {"dev_name": timestamp} — echo window per device
        self._echo_window = 10    # seconds

        # v6.3 [Airtime Guard]: rolling TX time tracking per minute
        self._airtime_window = deque(maxlen=200)  # [(timestamp, estimated_airtime_s), ...]
        self._airtime_blocked_until = 0  # epoch when TX block expires

    def _is_priority(self, dev): return dev in CONFIG.get('priority_devices', [])
    def _is_monitored(self, dev):
        # Priority devices are automatically monitored (Rule 3)
        if self._is_priority(dev): return True
        m = CONFIG.get('monitored', [])
        return not m or dev in m
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly',{}).get('auto_clear',{}).get(atype, True)

    # =====================================================
    # v5.0: DISCOVERY HASH
    # =====================================================
    def _compute_disc_hash(self):
        """Compute hash of monitored device configs + VIO + short ID map for change detection."""
        monitored_devs = {}
        for dev, info in sorted(self.devices.items()):
            if not self._is_monitored(dev): continue
            sid = self.dev_short_id.get(dev, -1)
            monitored_devs[dev] = {"type": info.get('type'), "caps": sorted(info.get('caps', [])), "sid": sid}
        # Include VIO definitions so adding/removing triggers re-discovery
        vio_defs = [(v['id'], v['type']) for v in CONFIG.get('virtual_io', [])]
        raw = json.dumps({"devs": monitored_devs, "vio": vio_defs}, separators=(',', ':'), sort_keys=True)
        h = hashlib.sha256(raw.encode()).hexdigest()[:12]
        self._disc_hash = h
        return h

    # =====================================================
    # v5.0: SLOT SYSTEM — 20s windows
    # =====================================================
    def _is_my_slot(self):
        """Check if current second falls within this gateway's 20s slot window."""
        cfg = CONFIG.get('slot', {})
        if not cfg.get('enabled', True): return True
        sec = datetime.now().second
        window = cfg.get('window', 20)
        idx = cfg.get('index', 0)
        slot_start = idx * window
        slot_end = slot_start + window
        return slot_start <= sec < slot_end

    def _slot_time_remaining(self):
        """Return (used, total) seconds in current slot window."""
        cfg = CONFIG.get('slot', {})
        if not cfg.get('enabled', True): return (0, 20)
        sec = datetime.now().second
        window = cfg.get('window', 20)
        idx = cfg.get('index', 0)
        slot_start = idx * window
        if slot_start <= sec < slot_start + window:
            used = sec - slot_start
            return (used, window)
        return (0, window)

    def _slot_debug_str(self):
        used, total = self._slot_time_remaining()
        return f"[Slot: {used}s/{total}s used]"

    # =====================================================
    # STATE PERSISTENCE (unchanged)
    # =====================================================
    def _save_state(self):
        try:
            data = {'last_seen':self.last_seen,'battery_state':self.battery_state,
                    'offline_reported':self.offline_reported,'stagnation_reported':self.stagnation_reported,
                    'last_state_change':self.last_state_change,
                    'states':{d:s for d,s in self.states.items()},
                    'temp_anomaly_reported':self.temp_anomaly_reported,
                    'hum_anomaly_reported':self.hum_anomaly_reported,
                    'production_mode':self.production_mode,
                    'production_next_change_ts':self.production_next_change_ts,
                    'production_source':self.production_source,
                    'vswitch_states':self.vswitch_states,
                    'saved_at':now_str()}
            with open(PERSIST_FILE, 'w') as f: json.dump(data, f)
        except Exception as e: log.error('MAIN', f"Save state: {e}")

    def _load_state(self):
        try:
            with open(PERSIST_FILE, 'r') as f: data = json.load(f)
            self.last_seen = data.get('last_seen', {})
            self.battery_state = data.get('battery_state', {})
            self.offline_reported = data.get('offline_reported', {})
            self.stagnation_reported = data.get('stagnation_reported', {})
            self.last_state_change = {k: float(v) for k, v in data.get('last_state_change', {}).items()}
            self.temp_anomaly_reported = data.get('temp_anomaly_reported', {})
            self.hum_anomaly_reported = data.get('hum_anomaly_reported', {})
            self.production_mode = data.get('production_mode', False)
            self.production_next_change_ts = data.get('production_next_change_ts', 0)
            self.production_source = data.get('production_source')
            for d, s in data.get('states', {}).items():
                if d not in self.states: self.states[d] = s
            self.vswitch_states = data.get('vswitch_states', {})
            log.info('MAIN', f"♻️ Loaded state: {len(self.last_seen)} devices, {len(self.vswitch_states)} vswitches")
        except FileNotFoundError: log.info('MAIN', "No saved state")
        except Exception as e: log.warn('MAIN', f"Load state: {e}")

    def _calc_availability(self, dev):
        info = self.devices.get(dev, {})
        dtype = info.get('type', 'sensor')
        is_mains = info.get('mains_powered', dtype in ['switch', 'light'])
        if dev in self.z2m_av and not self.z2m_av[dev]: return False
        if is_mains and self.cmd_failed.get(dev, False): return False
        ls = self.last_seen.get(dev)
        if ls:
            ls_ts = parse_datetime(ls)
            timeout = CONFIG['timeout'].get(dtype, 3600)
            if ls_ts > 0 and (now_ts() - ls_ts) > timeout: return False
        return True

    # =====================================================
    # ANOMALY DETECTION — ALL CONFIG HERE (REQ-7, unchanged logic)
    # =====================================================
    def _handle_anomaly_clear(self, data):
        dev, atype = data.get('d'), data.get('a')
        if not dev: return
        log.info('ANOMALY', f"🔄 Clear: {dev}/{atype}")
        if atype in ['low_battery','critical_battery']: self.battery_state[dev] = "OK"
        elif atype == 'device_offline': self.offline_reported[dev] = False
        elif atype == 'stagnation': self.stagnation_reported[dev] = False; self.last_state_change[dev] = now_ts()
        elif atype in ['temp_high','temp_low']: self.temp_anomaly_reported[dev] = None
        elif atype in ['hum_high','hum_low']: self.hum_anomaly_reported[dev] = None

    def _handle_anomaly_clear_batch(self, data):
        """v5.0: Batch anomaly clear — process multiple clears from one packet.
        Format: {"t":"ac_b","g":"G1","d":[["dev","atype"], ...]}"""
        items = data.get('d', [])
        for item in items:
            if isinstance(item, list) and len(item) >= 2:
                self._handle_anomaly_clear({'d': item[0], 'a': item[1]})
        log.info('ANOMALY', f"🔄 Batch clear: {len(items)} items")

    def _check_battery_anomaly(self, dev, battery):
        if battery is None: return
        det = CONFIG['anomaly']['detection']
        cc, lc = det.get('critical_battery',{}), det.get('low_battery',{})
        if cc.get('enabled',True) and battery < cc.get('threshold',15): new = "CRITICAL"
        elif lc.get('enabled',True) and battery < lc.get('threshold',25): new = "LOW"
        else: new = "OK"
        old = self.battery_state.get(dev, "OK")
        if new == old: return
        self.battery_state[dev] = new
        if new == "LOW": self._send_anomaly(dev, "low_battery", battery)
        elif new == "CRITICAL": self._send_anomaly(dev, "critical_battery", battery)
        elif new == "OK" and old in ["LOW","CRITICAL"]:
            if self._is_auto_clear_enabled('low_battery'): self._send_anomaly(dev, "battery_ok", battery)

    def _check_offline_anomaly(self, dev):
        det = CONFIG['anomaly']['detection'].get('device_offline',{})
        if not det.get('enabled',True): return
        av = self._calc_availability(dev)
        was = self.offline_reported.get(dev, False)
        if not av and not was:
            self.offline_reported[dev] = True; self._send_anomaly(dev, "device_offline", None)
        elif av and was:
            self.offline_reported[dev] = False
            if self._is_auto_clear_enabled('device_offline'): self._send_anomaly(dev, "device_online", None)

    def _check_value_anomalies(self, dev, data):
        det = CONFIG['anomaly']['detection']
        if 'temperature' in data:
            temp, cur = data['temperature'], self.temp_anomaly_reported.get(dev)
            th, tl = det.get('temp_high',{}), det.get('temp_low',{})
            if th.get('enabled',True) and temp >= th.get('threshold',50):
                if cur != 'high': self.temp_anomaly_reported[dev] = 'high'; self._send_anomaly(dev, "temp_high", temp)
            elif tl.get('enabled',True) and temp <= tl.get('threshold',-10):
                if cur != 'low': self.temp_anomaly_reported[dev] = 'low'; self._send_anomaly(dev, "temp_low", temp)
            elif cur:
                self.temp_anomaly_reported[dev] = None
                if self._is_auto_clear_enabled('temp_high') or self._is_auto_clear_enabled('temp_low'):
                    self._send_anomaly(dev, "temp_ok", temp)
        if 'humidity' in data:
            hum, cur = data['humidity'], self.hum_anomaly_reported.get(dev)
            hh, hl = det.get('hum_high',{}), det.get('hum_low',{})
            if hh.get('enabled',True) and hum >= hh.get('threshold',95):
                if cur != 'high': self.hum_anomaly_reported[dev] = 'high'; self._send_anomaly(dev, "hum_high", hum)
            elif hl.get('enabled',True) and hum <= hl.get('threshold',5):
                if cur != 'low': self.hum_anomaly_reported[dev] = 'low'; self._send_anomaly(dev, "hum_low", hum)
            elif cur:
                self.hum_anomaly_reported[dev] = None
                if self._is_auto_clear_enabled('hum_high') or self._is_auto_clear_enabled('hum_low'):
                    self._send_anomaly(dev, "hum_ok", hum)

    def _check_stagnation(self, dev):
        sc = CONFIG['anomaly']['detection'].get('stagnation',{})
        if not sc.get('enabled',True): return
        info = self.devices.get(dev, {})
        if info.get('type') not in ['switch','light']: return
        if self.stagnation_reported.get(dev, False): return
        lc = self.last_state_change.get(dev, self.start_time)
        h = (now_ts() - lc) / 3600
        if h >= sc.get('hours',48):
            self.stagnation_reported[dev] = True; self._send_anomaly(dev, "stagnation", int(h))

    def _send_anomaly(self, dev, atype, value):
        """Route anomaly: true critical (smoke/leak) → immediate P0, rest → buffer for batch."""
        msg_data = [dev, atype, value, int(now_ts())]
        # TRUE CRITICAL: smoke, water_leak → immediate single packet P0 + retransmit
        critical_types = CONFIG.get('critical_alarm', {}).get('types', [])
        if atype in critical_types:
            msg = {"t": "an", "g": CONFIG['id'], "d": dev, "a": atype, "ts": int(now_ts())}
            if value is not None: msg["v"] = value
            self.queue[FlowPriority.ALARM_PRIO].append(msg)
            self._blocked_anomaly_count += 1
            log.warn('ANOMALY', f"🚨 {PRIORITY_LABELS[FlowPriority.ALARM_PRIO]} IMMEDIATE: {dev} {atype}" + (f"={value}" if value is not None else ""))
            self._retransmit_critical(dev, atype, msg)
        else:
            # NON-CRITICAL: offline, stagnation, battery, temp, hum → buffer for batch
            with self.lock:
                self._anomaly_buffer.append(msg_data)
            self._blocked_anomaly_count += 1
            log.warn('ANOMALY', f"🚨 Buffered: {dev} {atype}" + (f"={value}" if value is not None else ""))

    def _flush_anomaly_buffer(self):
        """v6.0: Flush buffered anomalies as 'ab' batch packet(s) to P2 (DIAGNOSTIC).
        Called after anomaly loop cycle."""
        with self.lock:
            items = list(self._anomaly_buffer)
            self._anomaly_buffer.clear()
        if not items:
            return
        packets = batch_split("ab", CONFIG['id'], items, CONFIG['lora']['max_size'])
        for pkt in packets:
            self.queue[FlowPriority.DIAGNOSTIC].append(pkt)
        log.info('ANOMALY', f"🚨 {PRIORITY_LABELS[FlowPriority.DIAGNOSTIC]} Flushed {len(items)} anomalies → {len(packets)} ab packet(s)")

    def _retransmit_critical(self, dev, atype, msg):
        ca = CONFIG.get('critical_alarm',{})
        if not ca.get('enabled') or atype not in ca.get('types',[]): return
        ck = (dev, atype)
        if now_ts() - self.critical_alarm_cooldown.get(ck,0) < ca.get('cooldown',10): return
        self.critical_alarm_cooldown[ck] = now_ts()
        def _do():
            for i in range(1, ca.get('repeats',3)):
                time.sleep(ca.get('repeat_delay',2.0)); self.queue[FlowPriority.ALARM_PRIO].append(dict(msg))
        threading.Thread(target=_do, daemon=True).start()

    # =====================================================
    # REQ-8: dump_anom = LIVE re-check (unchanged logic)
    # =====================================================
    def _handle_dump_anom(self):
        """REQ-8: dump_anom = LIVE re-check. Results sent as batched 'ab' (anomaly batch).
        Format: {"t":"ab","g":"G1","d":[["dev","atype",value,ts], ...]}
        This replaces N individual 'an' packets with 1-2 batch packets."""
        log.info('ANOMALY', f"📋 Dump — LIVE re-check all monitored devices")
        def _do():
            det = CONFIG['anomaly']['detection']
            found = []  # collect all anomalies before sending
            ts = int(now_ts())
            for dev in self.devices:
                if not self._is_monitored(dev): continue
                ds = self.states.get(dev, {})
                bat = ds.get('battery')
                if bat is not None:
                    cc, lc = det.get('critical_battery',{}), det.get('low_battery',{})
                    if cc.get('enabled',True) and bat < cc.get('threshold',15):
                        self.battery_state[dev] = "CRITICAL"
                        found.append([dev, "critical_battery", bat, ts])
                    elif lc.get('enabled',True) and bat < lc.get('threshold',25):
                        self.battery_state[dev] = "LOW"
                        found.append([dev, "low_battery", bat, ts])
                oc = det.get('device_offline',{})
                if oc.get('enabled',True) and not self._calc_availability(dev):
                    self.offline_reported[dev] = True
                    found.append([dev, "device_offline", None, ts])
                sc = det.get('stagnation',{})
                if sc.get('enabled',True) and self.devices.get(dev,{}).get('type') in ['switch','light']:
                    h = (now_ts() - self.last_state_change.get(dev, self.start_time)) / 3600
                    if h >= sc.get('hours',48):
                        self.stagnation_reported[dev] = True
                        found.append([dev, "stagnation", int(h), ts])
                temp = ds.get('temperature')
                if temp is not None:
                    th, tl = det.get('temp_high',{}), det.get('temp_low',{})
                    if th.get('enabled',True) and temp >= th.get('threshold',50):
                        found.append([dev, "temp_high", temp, ts])
                    elif tl.get('enabled',True) and temp <= tl.get('threshold',-10):
                        found.append([dev, "temp_low", temp, ts])
                hum = ds.get('humidity')
                if hum is not None:
                    hh, hl = det.get('hum_high',{}), det.get('hum_low',{})
                    if hh.get('enabled',True) and hum >= hh.get('threshold',95):
                        found.append([dev, "hum_high", hum, ts])
                    elif hl.get('enabled',True) and hum <= hl.get('threshold',5):
                        found.append([dev, "hum_low", hum, ts])
            # Send as batched packets
            if found:
                packets = batch_split("ab", CONFIG['id'], found, CONFIG['lora']['max_size'])
                for pkt in packets:
                    self.queue[FlowPriority.COMMAND].append(pkt)
                log.info('ANOMALY', f"📋 Dump: {len(found)} anomalies → {len(packets)} ab packet(s)")
            else:
                # Send empty dump confirmation so supervisor knows dump completed
                self.queue[FlowPriority.COMMAND].append({"t":"ab","g":CONFIG['id'],"d":[]})
                log.info('ANOMALY', "📋 Dump: 0 anomalies (clean)")
        threading.Thread(target=_do, daemon=True).start()

    # =====================================================
    # SCHEDULE + CALENDAR (unchanged)
    # =====================================================
    def _handle_schedule(self, data):
        msg_ts = data.get('from_ts', 0)
        if msg_ts < self.production_last_update_ts: return
        self.production_mode = bool(data.get('mode', 0))
        self.production_next_change_ts = data.get('next_ts', 0)
        self.production_source = data.get('src', 'supervisor')
        self.production_last_update_ts = msg_ts
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
        mode_name = mode_names.get(data.get('mode', 0), '?')
        log.info('CFG', f"📅 Schedule: mode={data.get('mode',0)} ({mode_name}) next={self.production_next_change_ts}")
        self._publish_schedule_status()

    def _handle_schedule_pre_notify(self, data):
        """v6.3 [M1]: Pre-notify — mode change coming in N seconds.
        Packet: {"t":"sch_pre","g":"G1","mode":1,"in_sec":5,"next_ts":...}
        Gateway publishes MQTT for dashboard warning, no state change yet."""
        mode = data.get('mode', 0)
        in_sec = data.get('in_sec', 5)
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
        mode_name = mode_names.get(mode, f"MODE_{mode}")
        log.info('CFG', f"📅 [M1] Pre-notify: {mode_name} (mode={mode}) in {in_sec}s")
        self.mqtt.publish("ha/lora/schedule/pre_notify", json.dumps({
            "ts": int(now_ts()), "mode": mode, "name": mode_name,
            "in_sec": in_sec, "gw": CONFIG['id']
        }), retain=False)  # Not retained — transient warning

    def _publish_schedule_status(self):
        mode, nxt, src = self.local_schedule.compute_now_and_next()
        slots = self.local_schedule.get_all()
        self.mqtt.publish("ha/lora/schedule/status", json.dumps({
            "ts":int(now_ts()), "mode":mode, "next_ts":nxt, "src":src,
            "slots_count":len(slots), "production_mode":self.production_mode,
            "production_src":self.production_source
        }), retain=True)
        self.mqtt.publish("ha/lora/schedule/slots", json.dumps({"ts":int(now_ts()),"slots":slots}), retain=True)

    # =====================================================
    # SCHEDULE SYNC via CalendarTransfer (proven: CRC + retry + compression)
    # =====================================================
    def _get_schedule_hash(self):
        """Compute hash of local schedule in compact format."""
        compact = self._slots_to_compact()
        if not compact: return ""
        return hashlib.sha256(json.dumps(compact, separators=(',',':')).encode()).hexdigest()[:12]

    def _slots_to_compact(self, slots=None):
        """Convert full slots to compact [[start_min, dur_min, mode], ...]"""
        BASE = 1767225600
        if slots is None: slots = self.local_schedule.get_all()
        compact = []
        for s in slots:
            try:
                st = datetime.fromisoformat(s['start']).timestamp() if isinstance(s['start'], str) else float(s['start'])
                et = datetime.fromisoformat(s['end']).timestamp() if isinstance(s['end'], str) else float(s['end'])
                compact.append([int((st - BASE) / 60), int((et - st) / 60), s.get('mode', 1)])
            except: continue
        compact.sort()
        return compact

    def _push_schedule_to_supervisor(self):
        """Send local schedule to supervisor via CalendarTransfer (reliable, CRC)."""
        compact = self._slots_to_compact()
        if not compact: log.warn('SYNC', "Nothing to push"); return
        self.cal_transfer.start_send(CONFIG['id'], "gw_push", compact)
        log.info('SYNC', f"📤 Push {len(compact)} slots to supervisor via CalendarTransfer")

    def push_to_ha_local_calendar(self):
        """Sync schedule.json → HA Local Calendar.
        Method 1: Direct ICS file write (if ics_path configured — bulletproof)
        Method 2: REST API create_event only (fallback — may accumulate)
        Also publishes MQTT sensor for dashboard."""
        slots = self.local_schedule.get_all()
        gw_id = CONFIG['id'].lower()
        mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
        mode_icons = {0: '⚪', 1: '🟢', 2: '⏸', 3: '🔧'}

        # MQTT sensor (always publish — dashboard reads this)
        events_mqtt = []
        for slot in slots:
            mode = slot.get('mode', 0)
            events_mqtt.append({
                "start": slot.get('start', ''), "end": slot.get('end', ''),
                "mode": mode, "name": mode_names.get(mode, f"MODE_{mode}"),
                "icon": mode_icons.get(mode, '📅')
            })
        self.mqtt.publish(f"ha/lora/schedule/{gw_id}/events", json.dumps({
            "ts": int(now_ts()), "count": len(events_mqtt), "events": events_mqtt
        }), retain=True)

        ha = CONFIG.get('ha_api', {})
        ics_path = ha.get('ics_path', '')

        if ics_path:
            # METHOD 1: Direct ICS file write — atomic replace, zero API issues
            self._write_ics_file(ics_path, slots, mode_names)
        elif ha.get('token', ''):
            # METHOD 2: REST API (create only — delete doesn't work reliably)
            self._sync_via_rest_api(slots, mode_names)
        else:
            if events_mqtt: log.info('CAL', f"📅 {len(events_mqtt)} events → MQTT only (no HA config)")

    def _write_ics_file(self, ics_path, slots, mode_names):
        """Write ICS file + reload HA integration so calendar updates immediately."""
        def _do():
            try:
                lines = [
                    'BEGIN:VCALENDAR',
                    'VERSION:2.0',
                    'PRODID:-//LoRa BMS//Gateway//EN',
                    'X-WR-CALNAME:LoRa ' + CONFIG['id'],
                ]
                for slot in slots:
                    mode = slot.get('mode', 0)
                    name = mode_names.get(mode, f"MODE_{mode}")
                    start = slot.get('start', '').replace('-', '').replace(':', '').replace(' ', 'T')
                    end = slot.get('end', '').replace('-', '').replace(':', '').replace(' ', 'T')
                    sid = slot.get('id', f"s{hash(slot.get('start',''))}")
                    lines.extend([
                        'BEGIN:VEVENT',
                        f'UID:{sid}@lora-bms',
                        f'DTSTART:{start}',
                        f'DTEND:{end}',
                        f'SUMMARY:{name}',
                        f'DESCRIPTION:mode={mode}',
                        'END:VEVENT',
                    ])
                lines.append('END:VCALENDAR')
                ics_content = '\r\n'.join(lines) + '\r\n'

                # Atomic write: temp file → rename
                tmp_path = ics_path + '.tmp'
                with open(tmp_path, 'w', encoding='utf-8') as f:
                    f.write(ics_content)
                os.replace(tmp_path, ics_path)
                log.info('CAL', f"✅ ICS written: {len(slots)} events → {ics_path}")

                # Reload HA local_calendar integration so it picks up the new file
                self._reload_ha_calendar()

            except Exception as e:
                log.error('CAL', f"❌ ICS write failed: {e}")
        threading.Thread(target=_do, daemon=True, name="ics-write").start()

    def _reload_ha_calendar(self):
        """Reload HA local_calendar integration after ICS file write."""
        ha = CONFIG.get('ha_api', {})
        token = ha.get('token', '')
        if not token: return
        base_url = ha.get('url', 'http://localhost:8123').rstrip('/')
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        try:
            # Find local_calendar config entry ID
            req = urllib.request.Request(f"{base_url}/api/config/config_entries/entry", headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                entries = json.loads(resp.read())
            for entry in entries:
                if entry.get('domain') == 'local_calendar':
                    eid = entry['entry_id']
                    # Reload this config entry
                    reload_req = urllib.request.Request(
                        f"{base_url}/api/config/config_entries/entry/{eid}/reload",
                        data=b'', headers=headers, method='POST')
                    urllib.request.urlopen(reload_req, timeout=10)
                    log.info('CAL', f"✅ HA reload: local_calendar entry {eid[:8]}")
        except Exception as e:
            log.warn('CAL', f"⚠️ HA reload failed: {e} — restart HA manually or wait")

    def _sync_via_rest_api(self, slots, mode_names):
        """Fallback: REST API create_event only (if ICS path not configured)."""
        ha = CONFIG.get('ha_api', {})
        base_url = ha.get('url', 'http://localhost:8123').rstrip('/')
        gw_id = CONFIG['id'].lower()
        cal_entity = ha.get('calendar_entity', '') or f"calendar.lora_{gw_id}"
        headers = {'Authorization': f'Bearer {ha["token"]}', 'Content-Type': 'application/json'}

        def _do():
            try:
                created = 0
                for slot in slots:
                    mode = slot.get('mode', 0)
                    name = mode_names.get(mode, f"MODE_{mode}")
                    body = json.dumps({
                        "entity_id": cal_entity,
                        "summary": name,
                        "start_date_time": slot.get('start', ''),
                        "end_date_time": slot.get('end', ''),
                    }).encode()
                    try:
                        req = urllib.request.Request(
                            f"{base_url}/api/services/calendar/create_event",
                            data=body, headers=headers, method='POST')
                        urllib.request.urlopen(req, timeout=5)
                        created += 1
                    except Exception as e:
                        log.warn('CAL', f"Create: {e}")
                log.info('CAL', f"✅ REST API: created {created} in {cal_entity}")
            except Exception as e:
                log.error('CAL', f"❌ REST API: {e}")
        threading.Thread(target=_do, daemon=True, name="ha-rest").start()

    # =====================================================
    # VIRTUAL BUTTON (unchanged)
    # =====================================================
    # =====================================================
    # v5.0: VIRTUAL I/O — switches (stateful) + buttons (events)
    # =====================================================
    def _init_vio(self):
        """Initialize virtual I/O from CONFIG. Switches get stateful defaults, buttons are stateless."""
        for vio in CONFIG.get('virtual_io', []):
            vid, vtype = vio['id'], vio['type']
            if vtype == 'switch':
                if vid not in self.vswitch_states:
                    self.vswitch_states[vid] = vio.get('default', 0)
        sw = [v for v in CONFIG.get('virtual_io', []) if v['type'] == 'switch']
        bt = [v for v in CONFIG.get('virtual_io', []) if v['type'] == 'button']
        log.info('CFG', f"🔘 Virtual I/O: {len(sw)} switches, {len(bt)} buttons")
        if sw:
            log.info('CFG', f"   Switches: " + ", ".join(f"{v['id']}={'ON' if self.vswitch_states.get(v['id'],0) else 'OFF'}" for v in sw))

    def _register_vio_entities(self):
        """Register MQTT discovery entities for ALL virtual I/O (switches + buttons)."""
        p = "homeassistant"
        gw = CONFIG['id']
        di = {"identifiers": [f"lora_vio_{gw.lower()}"],
              "name": f"LoRa Virtual I/O {gw}",
              "model": "Virtual I/O", "manufacturer": "LoRa Gateway"}
        for vio in CONFIG.get('virtual_io', []):
            vid, vtype = vio['id'], vio['type']
            name = vio.get('name', vid)
            safe = vid.replace(' ', '_').lower()
            if safe in self._vio_mqtt_registered:
                continue
            if vtype == 'switch':
                self.mqtt.publish(f"{p}/switch/lora_vio_{safe}/config", json.dumps({
                    "name": f"LoRa {name}",
                    "object_id": f"lora_vio_{safe}",
                    "unique_id": f"lora_vio_{safe}",
                    "state_topic": f"lora/vio/{safe}/state",
                    "command_topic": f"lora/vio/{safe}/set",
                    "payload_on": "ON", "payload_off": "OFF",
                    "state_on": "ON", "state_off": "OFF",
                    "device": di, "icon": "mdi:toggle-switch"
                }), retain=True)
                val = self.vswitch_states.get(vid, 0)
                self.mqtt.publish(f"lora/vio/{safe}/state", "ON" if val else "OFF", retain=True)
                log.info('CFG', f"🔘 VIO switch: {name} = {'ON' if val else 'OFF'}")
            elif vtype == 'button':
                self.mqtt.publish(f"{p}/event/lora_vio_{safe}/config", json.dumps({
                    "name": f"LoRa {name}",
                    "object_id": f"lora_vio_{safe}",
                    "unique_id": f"lora_vio_{safe}",
                    "state_topic": f"lora/vio/{safe}/event",
                    "event_types": ["single", "double", "long"],
                    "device": di, "icon": "mdi:gesture-tap-button"
                }), retain=True)
                # Also register a HA button entity for dashboard press
                self.mqtt.publish(f"{p}/button/lora_vio_{safe}_press/config", json.dumps({
                    "name": f"LoRa {name} Press",
                    "object_id": f"lora_vio_{safe}_press",
                    "unique_id": f"lora_vio_{safe}_press",
                    "command_topic": f"lora/vio/{safe}/press",
                    "device": di, "icon": "mdi:gesture-tap-button"
                }), retain=True)
                log.info('CFG', f"🔘 VIO button: {name}")
            self._vio_mqtt_registered.add(safe)

    def _check_cmd_seq(self, vid, seq):
        """Check command sequence number. Returns True if command should be processed."""
        if seq is None:
            return True  # No seq → legacy/local command, always process
        last = self._last_cmd_seq.get(vid, 0)
        if seq <= last:
            log.warn('CMD', f"⚠️ Stale command {vid} seq={seq} <= last={last}, ignoring")
            return False
        self._last_cmd_seq[vid] = seq
        return True

    def _handle_vswitch(self, data):
        """Handle vswitch command from LoRa (supervisor → gateway).
        Packet: {"t":"vsw","g":"G1","id":"vs_prod","v":1,"seq":47}
        - Seq check: rejects out-of-order commands
        - Debounce 500ms: absorbs rapid clicks, only final state applied
        """
        vid = data.get('id')
        val = data.get('v', 0)
        seq = data.get('seq')
        if vid is None: return
        if not self._check_cmd_seq(vid, seq): return
        # Debounce: cancel previous pending change, schedule new one
        with self.lock:
            prev = self._vsw_debounce.get(vid)
            if prev and prev[1] is not None:
                prev[1].cancel()
        def _apply():
            with self.lock:
                old = self.vswitch_states.get(vid)
                self.vswitch_states[vid] = val
                self._vsw_debounce.pop(vid, None)
            safe = vid.replace(' ', '_').lower()
            self.mqtt.publish(f"lora/vio/{safe}/state", "ON" if val else "OFF", retain=True)
            log.info('CFG', f"🔘 VSwitch {vid}: {'ON' if val else 'OFF'}" +
                     (f" (was {'ON' if old else 'OFF'})" if old is not None else " (new)"))
            self.queue[FlowPriority.COMMAND].append({
                't': 'vsw_st', 'g': CONFIG['id'], 'id': vid, 'v': val
            })
            self._save_state()
        timer = threading.Timer(0.5, _apply)
        timer.daemon = True
        with self.lock:
            self._vsw_debounce[vid] = (val, timer)
        timer.start()

    def _handle_vio_button_lora(self, data):
        """Handle button press from LoRa (supervisor → gateway).
        Packet: {"t":"vbtn","g":"G1","id":"vb_hall","act":"single","seq":48}
        """
        vid = data.get('id', data.get('btn', ''))
        action = data.get('act', 'single')
        seq = data.get('seq')
        if not self._check_cmd_seq(vid, seq): return
        safe = vid.replace(' ', '_').lower()
        log.info('VBTN', f"🔘 VIO button: {vid} → {action}")
        self.mqtt.publish(f"lora/vio/{safe}/event", json.dumps({
            "event_type": action, "btn": vid, "ts": int(now_ts())
        }))

    def _handle_vio_mqtt_set(self, vid, payload):
        """Handle vswitch command from LOCAL MQTT (HA dashboard on gateway side)."""
        val = 1 if payload.upper() in ("ON", "1") else 0
        with self.lock:
            self.vswitch_states[vid] = val
        safe = vid.replace(' ', '_').lower()
        self.mqtt.publish(f"lora/vio/{safe}/state", "ON" if val else "OFF", retain=True)
        log.info('CFG', f"🔘 VSwitch {vid} (MQTT): {'ON' if val else 'OFF'}")
        # Notify supervisor
        self.queue[FlowPriority.COMMAND].append({
            't': 'vsw_st', 'g': CONFIG['id'], 'id': vid, 'v': val
        })
        self._save_state()

    def _handle_vio_mqtt_press(self, vid):
        """Handle button press from LOCAL MQTT (HA dashboard on gateway side)."""
        safe = vid.replace(' ', '_').lower()
        log.info('VBTN', f"🔘 VIO button (MQTT): {vid} → single")
        self.mqtt.publish(f"lora/vio/{safe}/event", json.dumps({
            "event_type": "single", "btn": vid, "ts": int(now_ts())
        }))

    # =====================================================
    # v5.0: PROACTIVE HEARTBEAT (gateway self-reports)
    # =====================================================
    def _heartbeat_loop(self):
        """Self-initiated heartbeat every heartbeat_interval seconds.
        Supervisor detects offline if no heartbeat arrives within 1.5× interval.
        """
        while self.running:
            time.sleep(10)
            interval = CONFIG.get('heartbeat_interval', 300)
            now = now_ts()
            if now - self._last_heartbeat_ts >= interval:
                self._send_heartbeat()
                self._last_heartbeat_ts = now

    def _send_heartbeat(self):
        """Build and queue heartbeat packet with diagnostics."""
        uptime = int(time.time() - self.start_time)
        mon = len([d for d in self.devices if self._is_monitored(d)])

        # Airtime estimate
        elapsed = max(time.time() - self.start_time, 1)
        airtime_pct = round(self._airtime_tx_count * 0.5 / elapsed * 100, 2)

        # RSSI (best effort)
        rssi = -999
        try:
            if self.mesh and hasattr(self.mesh, 'nodes'):
                for node in self.mesh.nodes.values():
                    r = node.get('rssi', None)
                    if r and r > rssi: rssi = r
        except: pass

        # Z2M freshness: seconds since last Z2M data
        z2m_age = int(now_ts() - self._last_z2m_ts) if self._last_z2m_ts > 0 else -1

        hb = {
            't': 'hb', 'g': CONFIG['id'],
            'up': uptime,
            'dev': len(self.devices),
            'mon': mon,
            'air': airtime_pct,
            'z2m': z2m_age,       # seconds since last Z2M data (-1 = never)
            'abl': self._blocked_anomaly_count,
            'hash': self._disc_hash,
        }
        if rssi > -999: hb['rssi'] = rssi
        # Include vswitch states (compact)
        if self.vswitch_states:
            hb['vs'] = self.vswitch_states

        self.queue[FlowPriority.COMMAND].append(hb)
        log.info('PING', f"💓 HB: up={uptime}s z2m_age={z2m_age}s air={airtime_pct}% dev={mon}/{len(self.devices)}")

    # =====================================================
    # MAIN LIFECYCLE
    # =====================================================
    def start(self):
        log.info('MAIN', "="*50)
        log.info('MAIN', f"🚀 GATEWAY {CONFIG['id']} v6.3 (ETAP 4: Diagnostics & Precision)")
        log.info('MAIN', "="*50)
        self._load_state()
        self.cal_transfer = CalendarTransfer(self._add_queue, CONFIG['calendar']['chunk_size'])

        # v6.3 [M3]: Initialize adaptive batcher
        if CONFIG['batcher']['enabled']:
            self.batcher = Batcher(
                gw_id=CONFIG['id'],
                flush_callback=self._on_batch_flush,
                interval=CONFIG['batcher']['interval'],
                max_payload=CONFIG['batcher']['max_payload'],
                max_items=CONFIG['batcher'].get('max_items', 12),
                priority_check_fn=self._is_priority,
            )

        self._setup_mqtt()
        for _ in range(20):
            if self.discovery_done: break
            time.sleep(0.5)

        # v5.0: Init virtual I/O (after load_state, before anomaly loop)
        self._init_vio()

        threading.Thread(target=self._anomaly_loop, daemon=True).start()
        if CONFIG['cmd_retry']['enabled']:
            threading.Thread(target=self._retry_loop, daemon=True).start()
        self._setup_mesh()
        threading.Thread(target=self._mesh_reconnect_loop, daemon=True, name="mesh-reconnect").start()

        # v5.0: Start batcher flush loop
        if self.batcher:
            self.batcher.start()

        # v5.0: Start proactive heartbeat loop
        self._last_heartbeat_ts = now_ts()
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()

        # v5.0: Register VIO MQTT entities (switches + buttons)
        self._register_vio_entities()

        mon = len([d for d in self.devices if self._is_monitored(d)])
        pri = len([d for d in self.devices if self._is_priority(d)])
        self._compute_disc_hash()
        log.info('MAIN', f"✅ Ready! {len(self.devices)} devices, {mon} monitored, {pri} priority")
        log.info('MAIN', f"📋 disc_hash={self._disc_hash}")
        slot_cfg = CONFIG.get('slot', {})
        if slot_cfg.get('enabled'):
            log.info('SLOT', f"⏱️ Slot system: index={slot_cfg['index']}, window={slot_cfg['window']}s, count={slot_cfg['count']}")
        self._publish_schedule_status()
        try:
            while self.running: self._loop(); time.sleep(0.05)
        except KeyboardInterrupt: pass
        finally: self._cleanup()

    def _setup_mqtt(self):
        self.mqtt = mqtt.Client(client_id=f"gw_{CONFIG['id']}_{int(time.time())}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt.username_pw_set(CONFIG['mqtt']['user'], CONFIG['mqtt']['pass'])
        self.mqtt.on_connect = self._on_connect; self.mqtt.on_message = self._on_message
        self.mqtt.connect(CONFIG['mqtt']['host'], CONFIG['mqtt']['port'], 60); self.mqtt.loop_start()
        log.info('MQTT', "✅ Connected")

    def _on_connect(self, client, ud, flags, rc, props):
        if rc == 0:
            client.subscribe("zigbee2mqtt/bridge/devices")
            client.subscribe("zigbee2mqtt/+")
            client.subscribe("zigbee2mqtt/+/availability")
            client.subscribe("ha/lora/schedule/#")
            # v5.0: Subscribe to VIO local commands from HA (switches + buttons)
            client.subscribe("lora/vio/+/set")
            client.subscribe("lora/vio/+/press")

    def _on_message(self, client, ud, msg):
        try:
            topic, payload = msg.topic, msg.payload.decode()
            # Calendar MQTT
            if topic == "ha/lora/schedule/add":
                try:
                    d = json.loads(payload)
                    self.local_schedule.add_slot(d['start'], d['end'], d.get('mode',1), d.get('note',''))
                    self._publish_schedule_status()
                except: pass
                return
            if topic == "ha/lora/schedule/clear": self.local_schedule.clear_slots(); self._publish_schedule_status(); return
            if topic == "ha/lora/schedule/list_req": self._publish_schedule_status(); return
            if topic == "ha/lora/schedule/sync_push": self._sync_push(); return
            if topic == "ha/lora/schedule/sync_pull": self._sync_pull(); return
            if topic == "ha/lora/schedule/sync_bidir": self._sync_bidir(); return

            # v5.0: Virtual I/O command from local HA
            if topic.startswith("lora/vio/") and topic.endswith("/set"):
                safe = topic.split('/')[2]
                for vio in CONFIG.get('virtual_io', []):
                    if vio['id'].replace(' ', '_').lower() == safe and vio['type'] == 'switch':
                        self._handle_vio_mqtt_set(vio['id'], payload)
                        break
                return
            if topic.startswith("lora/vio/") and topic.endswith("/press"):
                safe = topic.split('/')[2]
                for vio in CONFIG.get('virtual_io', []):
                    if vio['id'].replace(' ', '_').lower() == safe and vio['type'] == 'button':
                        self._handle_vio_mqtt_press(vio['id'])
                        break
                return

            # Zigbee2MQTT
            if topic == "zigbee2mqtt/bridge/devices": self._process_z2m_discovery(payload)
            elif topic.endswith("/availability"):
                dev = topic.split('/')[1]
                if dev in self.devices:
                    av = payload.lower() == "online"
                    old = self.z2m_av.get(dev)
                    self.z2m_av[dev] = av
                    if old != av:
                        self._check_offline_anomaly(dev)
                        if self._is_monitored(dev):
                            self._send_status(dev, FlowPriority.ALARM_PRIO if self._is_priority(dev) else FlowPriority.DIAGNOSTIC)
            elif topic.startswith("zigbee2mqtt/") and '/' not in topic[12:]:
                dev = topic.split('/')[1]
                if dev in self.devices:
                    try: self._handle_zigbee_state(dev, json.loads(payload))
                    except: pass
        except Exception as e: log.error('MQTT', f"Error: {e}")

    def _sync_push(self):
        """Push local schedule to supervisor via LoRa compact protocol."""
        slots = self.local_schedule.get_all()
        if not slots: log.warn('SYNC', "Nothing to push"); return
        self._push_schedule_to_supervisor()

    def _sync_pull(self):
        self._add_queue({"t":"sch_req","g":CONFIG['id'],"dir":"pull"}, FlowPriority.COMMAND)

    def _sync_bidir(self):
        slots = self.local_schedule.get_all()
        if slots: self.cal_transfer.start_send(CONFIG['id'], "bidir", slots)
        else: self._add_queue({"t":"sch_req","g":CONFIG['id'],"dir":"bidir"}, FlowPriority.COMMAND)

    def _process_z2m_discovery(self, payload):
        try:
            self._last_z2m_ts = now_ts()  # v5.0: track Z2M freshness
            for dev in json.loads(payload):
                name = dev.get('friendly_name')
                if not name or name == 'Coordinator': continue
                self.devices[name] = self._analyze_device(dev)
                if name not in self.z2m_av: self.z2m_av[name] = True
                if name not in self.battery_state: self.battery_state[name] = "OK"
                if name not in self.offline_reported: self.offline_reported[name] = False
                if name not in self.stagnation_reported: self.stagnation_reported[name] = False
            self.discovery_done = True
            # v5.0: Assign short IDs (stable sorted order)
            self._assign_short_ids()
            self._compute_disc_hash()
            log.info('ZIGBEE', f"Found {len(self.devices)} devices (hash={self._disc_hash}), {len(self.dev_short_id)} short IDs")
        except Exception as e: log.error('ZIGBEE', f"Discovery: {e}")

    def _assign_short_ids(self):
        """Auto-assign short integer IDs to monitored devices (sorted for stability)."""
        monitored = sorted([d for d in self.devices if self._is_monitored(d)])
        self.dev_short_id = {name: idx for idx, name in enumerate(monitored)}
        self.dev_short_rev = {idx: name for name, idx in self.dev_short_id.items()}
        log.info('DISC', f"🔢 Short IDs: " + ", ".join(f"{v}={k}" for k, v in self.dev_short_id.items()))

    def _analyze_device(self, dev):
        info = {'type':'sensor','caps':[],'mains_powered':False}
        pw = dev.get('power_source','')
        if 'Mains' in pw or 'DC' in pw: info['mains_powered'] = True
        for exp in dev.get('definition',{}).get('exposes',[]):
            t = exp.get('type')
            if t == 'switch': info['type']='switch'; info['caps'].append('state'); info['mains_powered']=True
            elif t == 'light':
                info['type']='light'; info['caps'].append('state'); info['mains_powered']=True
                for f in exp.get('features',[]):
                    if f.get('name')=='brightness': info['caps'].append('brightness')
            elif t in ['numeric','binary']:
                n = exp.get('name','').lower()
                for key,dt,cap in [('temperature','sensor','temperature'),('humidity','sensor','humidity'),
                                   ('battery',None,'battery'),('contact','binary_sensor','contact'),
                                   ('occupancy','binary_sensor','occupancy'),('water_leak','binary_sensor','water_leak'),
                                   ('smoke','binary_sensor','smoke')]:
                    if key in n:
                        if dt: info['type']=dt
                        if cap not in info['caps']: info['caps'].append(cap)
                        break
        return info

    def _handle_zigbee_state(self, dev, data):
        info = self.devices.get(dev,{}); dtype = info.get('type','sensor')
        self._last_z2m_ts = now_ts()  # v5.0: track Z2M freshness
        with self.lock:
            old = self.states.get(dev,{}); self.states[dev] = data; self.last_seen[dev] = now_str()
            if dev in self.cmd_failed: self.cmd_failed[dev] = False
            if dev in self.pending_cmds: del self.pending_cmds[dev]
        if dtype in ['switch','light'] and data.get('state') != old.get('state'):
            self.last_state_change[dev] = now_ts()
            if self.stagnation_reported.get(dev, False):
                self.stagnation_reported[dev] = False
                if self._is_auto_clear_enabled('stagnation'): self._send_anomaly(dev, "stagnation_clear", None)
            else: self.stagnation_reported[dev] = False
        self._check_battery_anomaly(dev, data.get('battery'))
        self._check_offline_anomaly(dev); self._check_value_anomalies(dev, data)
        if not self._is_monitored(dev): return
        mode = CONFIG['report_mode'].get(dtype, 'both')
        if mode in ['event','both']:
            changed = is_alarm = False
            if dtype in ['switch','light']:
                if data.get('state') != old.get('state'): changed = True
            elif dtype == 'binary_sensor':
                for k in ['contact','occupancy','water_leak','smoke']:
                    if k in data and data.get(k) != old.get(k):
                        changed = True
                        if k in ['water_leak','smoke'] and data.get(k): is_alarm = True
                if 'battery' in data and self._exceeds_delta(dev, 'battery', data['battery']): changed = True
            elif dtype == 'sensor':
                # Delta reporting: use CONFIG thresholds
                for k in ['temperature','humidity']:
                    if k in data and k in old:
                        if self._exceeds_delta(dev, k, data[k]): changed = True
                if 'battery' in data and self._exceeds_delta(dev, 'battery', data['battery']): changed = True
            if changed:
                if self._is_priority(dev):
                    self._send_status(dev, FlowPriority.ALARM_PRIO)
                else:
                    self._send_status(dev, FlowPriority.ALARM_PRIO if is_alarm else FlowPriority.BATCH)

    def _exceeds_delta(self, dev, key, new_val):
        """Check if new value exceeds delta threshold compared to last REPORTED value.
        Returns True if value should be reported (delta exceeded or delta disabled).
        Always returns True on first report for a device/key."""
        dcfg = CONFIG.get('delta', {})
        if not dcfg.get('enabled', False):
            return True  # Delta disabled — always report
        threshold = dcfg.get(key)
        if threshold is None:
            return True  # No threshold for this key — always report
        last = self._last_reported_values.get(dev, {}).get(key)
        if last is None:
            return True  # First report — always send
        try:
            return abs(float(new_val) - float(last)) >= threshold
        except (TypeError, ValueError):
            return True  # Non-numeric — always report

    @staticmethod
    def _cv(v):
        """Compress value: bool/ON/OFF → 1/0 for minimal payload."""
        if v is True or v == "ON": return 1
        if v is False or v == "OFF": return 0
        return v

    def _build_status_payload(self, dev, include_ls=False):
        """v6.2: Build status payload with Short JSON keys.
        Short key map: a=available, s=state, l=last_seen, t=temperature,
        h=humidity, b=battery, r=brightness, c=contact, o=occupancy,
        w=water_leak, k=smoke.
        include_ls=False for batcher (batch has ts header).
        include_ls=True for direct P0/P1 sends."""
        info = self.devices.get(dev,{})
        if not info: return None
        with self.lock: state = self.states.get(dev,{}); ls = self.last_seen.get(dev, now_str())
        av = self._calc_availability(dev); dtype = info.get('type','sensor')
        msg = {'a': self._cv(av)}
        if include_ls: msg['l'] = ls
        if dtype in ['switch','light']:
            msg['s'] = self._cv(state.get('state','OFF'))
            if 'brightness' in state: msg['r'] = state['brightness']
        elif dtype == 'sensor':
            if av:
                if 'temperature' in state: msg['t'] = round(state['temperature'],1)
                if 'humidity' in state: msg['h'] = int(state['humidity'])
            if 'battery' in state: msg['b'] = state['battery']
        elif dtype == 'binary_sensor':
            for k,short in [('contact','c'),('occupancy','o'),('water_leak','w'),('smoke','k')]:
                if k in state: msg[short] = self._cv(state[k])
            if 'battery' in state: msg['b'] = state['battery']
        return msg

    def _send_status(self, dev, priority=FlowPriority.BATCH):
        """v6.2: Send device status with Echo Filter.
        Echo Filter: suppress status for 10s after cmd execution (Supervisor already knows).
        P0 (ALARM_PRIO) is NEVER suppressed — safety first.
        """
        info = self.devices.get(dev,{})
        if not info: return

        if self._is_priority(dev):
            priority = FlowPriority.ALARM_PRIO

        # v6.2 [Echo Filter]: suppress non-safety status within echo window
        if priority != FlowPriority.ALARM_PRIO:
            echo_ts = self._echo_suppress.get(dev, 0)
            if echo_ts and (time.time() - echo_ts) < self._echo_window:
                log.debug('CMD', f"🔇 Echo suppressed: {dev} ({time.time() - echo_ts:.1f}s < {self._echo_window}s)")
                return

        # Batch mode: P2/P3 regular devices go through batcher (no ls, batch header has ts)
        # P0/P1 bypass batcher for immediate delivery within slot
        use_batch = self.batcher and priority in (FlowPriority.DIAGNOSTIC, FlowPriority.BATCH)
        payload = self._build_status_payload(dev, include_ls=not use_batch)
        if payload is None: return

        # Delta: record reported values for future comparison
        self._track_reported_values(dev, payload)

        if use_batch:
            sid = self.dev_short_id.get(dev, dev)
            self.batcher.add(sid, payload)
            log.debug('BATCH', f"📦 {PRIORITY_LABELS[FlowPriority.BATCH]} Buffered: {dev}(sid={sid}) {self._slot_debug_str()}")
        else:
            msg = {'t':'st','g':CONFIG['id'],'d':dev}
            msg.update(payload)
            self.queue[priority].append(msg)
            log.info('STATUS', f"{PRIORITY_LABELS[priority]} Queued: {dev}")

    def _track_reported_values(self, dev, payload):
        """Record values sent via LoRa for delta comparison on next report."""
        if dev not in self._last_reported_values:
            self._last_reported_values[dev] = {}
        # v6.2: Map short payload keys back to delta config keys
        key_map = {'t': 'temperature', 'h': 'humidity', 'b': 'battery', 'r': 'brightness'}
        for short_key, full_key in key_map.items():
            if short_key in payload:
                self._last_reported_values[dev][full_key] = payload[short_key]

    def _on_batch_flush(self, batch_dict):
        """Callback from Batcher: enqueue a batch packet dict for LoRa TX."""
        self.queue[FlowPriority.BATCH].append(batch_dict)
        size = len(json.dumps(batch_dict, separators=(',', ':')))
        items_n = len(batch_dict.get('d', []))
        log.info('BATCH', f"{PRIORITY_LABELS[FlowPriority.BATCH]} Queued batch ({items_n} items, ~{size}B) {self._slot_debug_str()}")

    def _execute_command(self, dev, cmd, val):
        self.mqtt.publish(f"zigbee2mqtt/{dev}/set", json.dumps({cmd: val}))
        # v6.2 [Echo Filter]: suppress LoRa echo for this device
        self._echo_suppress[dev] = time.time()
        info = self.devices.get(dev,{})
        if info.get('mains_powered', False) and CONFIG['cmd_retry']['enabled']:
            with self.lock: self.pending_cmds[dev] = {'cmd':cmd,'val':val,'time':now_ts(),'retries':0}
        # Note: no immediate status send — Echo Filter blocks for 10s
        # The next batch cycle or periodic report will pick up the new state
        log.info('CMD', f"⚡ {dev} cmd={cmd} val={val} — echo suppressed {self._echo_window}s")

    def _retry_loop(self):
        while self.running:
            time.sleep(1)
            to, mx = CONFIG['cmd_retry']['timeout'], CONFIG['cmd_retry']['retries']
            now = now_ts()
            with self.lock: checks = list(self.pending_cmds.items())
            for dev, info in checks:
                if now - info['time'] > to:
                    if info['retries'] < mx:
                        with self.lock: self.pending_cmds[dev]['retries']+=1; self.pending_cmds[dev]['time']=now
                        self.mqtt.publish(f"zigbee2mqtt/{dev}/set", json.dumps({info['cmd']:info['val']}))
                    else:
                        with self.lock: del self.pending_cmds[dev]; self.cmd_failed[dev]=True
                        self._check_offline_anomaly(dev)
                        if self._is_monitored(dev): self._send_status(dev, FlowPriority.DIAGNOSTIC)

    # =====================================================
    # REQ-9: anomaly loop checks ALL known devices (unchanged logic)
    # =====================================================
    def _anomaly_loop(self):
        sc = 0
        while self.running:
            time.sleep(CONFIG['anomaly']['check_interval'])
            try:
                for dev in list(self.devices.keys()):
                    self._check_stagnation(dev)
                    self._check_offline_anomaly(dev)
                # v5.1: Flush accumulated non-critical anomalies as batch
                self._flush_anomaly_buffer()
                sc += 1
                if sc >= 5: self._save_state(); sc = 0
            except Exception as e: log.error('ANOMALY', f"Loop error: {e}")

    def _check_periodic_reports(self):
        now = time.time()
        for dev, info in self.devices.items():
            if not self._is_monitored(dev): continue
            dtype = info.get('type','sensor')
            mode = CONFIG['report_mode'].get(dtype, 'both')
            if mode == 'event' and not self._is_priority(dev): continue
            interval = CONFIG['report_intervals'].get(dtype, 600)
            last = self.last_report.get(dev, 0)
            if last == 0:
                sd = CONFIG['slot']['index'] * 5 if CONFIG['slot']['enabled'] else 0
                if now - self.start_time > sd + 10:
                    self._send_status(dev, FlowPriority.ALARM_PRIO if self._is_priority(dev) else FlowPriority.BATCH)
                    self.last_report[dev] = now
            elif now - last >= interval:
                # Delta check: skip if no values changed beyond threshold
                if self._has_delta_change(dev):
                    self._send_status(dev, FlowPriority.ALARM_PRIO if self._is_priority(dev) else FlowPriority.BATCH)
                    self.last_report[dev] = now
                else:
                    self.last_report[dev] = now  # Reset timer even if skipped
                    log.debug('BATCH', f"📦 Delta skip: {dev} (no significant change)")

    def _has_delta_change(self, dev):
        """Check if device has any value exceeding delta threshold since last report.
        Returns True if at least one value changed enough to warrant a report.
        Priority devices always return True (never skip).
        First report for a device always returns True."""
        if self._is_priority(dev):
            return True
        dcfg = CONFIG.get('delta', {})
        if not dcfg.get('enabled', False):
            return True  # Delta disabled — always report
        state = self.states.get(dev, {})
        last = self._last_reported_values.get(dev)
        if not last:
            return True  # Never reported — always send
        # Check each trackable value
        for key in ['temperature', 'humidity', 'battery']:
            if key in state:
                threshold = dcfg.get(key)
                if threshold is None:
                    continue
                old_val = last.get(key)
                if old_val is None:
                    return True  # New key appeared
                try:
                    if abs(float(state[key]) - float(old_val)) >= threshold:
                        return True
                except (TypeError, ValueError):
                    return True
        # Check brightness for lights
        if 'brightness' in state:
            threshold = dcfg.get('brightness')
            if threshold:
                old_bri = last.get('brightness')
                if old_bri is None or abs(state['brightness'] - old_bri) >= threshold:
                    return True
        # Check state change for switches/lights
        dtype = self.devices.get(dev, {}).get('type', 'sensor')
        if dtype in ['switch', 'light']:
            return True  # State changes always reported
        return False  # No significant change

    # =====================================================
    # LoRa: SETUP + HOT RECONNECT (unchanged logic)
    # =====================================================
    def _setup_mesh(self):
        log.info('LORA', f"Connecting {CONFIG['mesh_port']}...")
        pub.subscribe(self._mesh_rx, "meshtastic.receive.text")
        try:
            self.mesh = meshtastic.serial_interface.SerialInterface(devPath=CONFIG['mesh_port'])
            self._mesh_connected = True
            self._mesh_reconnect_backoff = 0
            log.info('LORA', "✅ Connected")
        except Exception as e:
            log.error('LORA', f"❌ Connection failed: {e}")
            self._mesh_connected = False
        time.sleep(2)

    def _mesh_reconnect_loop(self):
        while self.running:
            time.sleep(5)
            if not CONFIG['mesh_reconnect'].get('enabled', True): continue
            if self._mesh_connected:
                try:
                    if self.mesh and hasattr(self.mesh, 'localNode'):
                        _ = self.mesh.localNode
                except:
                    log.warn('LORA', "⚠️ Antenna unhealthy — disconnecting")
                    self._mesh_connected = False
                    try: self.mesh.close()
                    except: pass
                    self.mesh = None
            else:
                interval = min(CONFIG['mesh_reconnect']['interval'] * (2 ** self._mesh_reconnect_backoff),
                               CONFIG['mesh_reconnect']['max_backoff'])
                time.sleep(interval)
                log.info('LORA', f"🔄 Reconnecting {CONFIG['mesh_port']}...")
                try:
                    self.mesh = meshtastic.serial_interface.SerialInterface(devPath=CONFIG['mesh_port'])
                    self._mesh_connected = True
                    self._mesh_reconnect_backoff = 0
                    log.info('LORA', "✅ Reconnected!")
                except Exception as e:
                    log.error('LORA', f"❌ Reconnect failed: {e}")
                    self._mesh_reconnect_backoff = min(self._mesh_reconnect_backoff + 1, 5)

    def _mesh_rx(self, packet, interface):
        try:
            text = packet.get('decoded',{}).get('text') if isinstance(packet, dict) else None
            if text:
                log.info('LORA', f"📥 {text[:120]}")
                threading.Thread(target=self._handle_lora, args=(json.loads(text),), daemon=True).start()
        except: pass

    def _handle_lora(self, data):
        try:
            t, target = data.get('t'), data.get('g')
            if target and target != CONFIG['id']: return
            if not target and CONFIG['slot']['enabled']:
                delay = CONFIG['slot']['index'] * 2
                if delay > 0: time.sleep(delay)
            if t == 'cfg': self._handle_config(data)
            elif t == 'cmd': self._handle_command(data)
            elif t == 'req': self._handle_request(data)
            elif t == 'req_st': self._handle_req_states()  # v6.1 [H4]: State recovery
            elif t == 'ping': self._handle_ping()
            elif t == 'disc': self._handle_discovery(data)
            elif t == 'an_clr': self._handle_anomaly_clear(data)
            # v5.0: Batch anomaly clear — supervisor sends multiple clears in one packet
            elif t == 'ac_b': self._handle_anomaly_clear_batch(data)
            elif t == 'dump_anom': self._handle_dump_anom()
            # Calendar: CalendarTransfer (proven: CRC + retransmit)
            elif t == 'sch_pull_req': self._push_schedule_to_supervisor()  # SUP requests our schedule
            elif t == 'sch': self._handle_schedule(data)  # Mode transition notification
            elif t == 'sch_pre': self._handle_schedule_pre_notify(data)  # v6.3 [M1]: pre-notify
            elif t == 'sch_b': self.cal_transfer.handle_begin(data)
            elif t == 'sch_c': self.cal_transfer.handle_chunk(data)
            elif t == 'sch_e':
                result = self.cal_transfer.handle_end(data)
                if result: self._process_calendar_received(*result)
            elif t == 'sch_a': self.cal_transfer.handle_ack(data)
            elif t == 'vbtn': self._handle_vio_button_lora(data)
            elif t == 'vsw': self._handle_vswitch(data)
        except Exception as e: log.error('LORA', f"Error: {e}")

    def _process_calendar_received(self, gw, direction, data):
        """Handle data received from CalendarTransfer.
        direction='cal': compact [[sm,dm,mode],...] from supervisor sync
        direction='gw_push': compact from gateway push (on supervisor side)
        direction='pull'/'bidir': full slot dicts (legacy)
        direction='push': full slot dicts merge (legacy)"""
        if direction == 'cal':
            BASE = 1767225600
            mode_names = CONFIG.get('mode_names', {0: 'BRAK PRODUKCJI', 1: 'PRODUKCJA', 2: 'PRZERWA', 3: 'SERWIS'})
            full_slots = []
            for item in data:
                if not isinstance(item, list) or len(item) < 3: continue
                sm, dm, mode = item[0], item[1], item[2]
                st = BASE + sm * 60; et = st + dm * 60
                full_slots.append({
                    "id": f"s{sm}", "mode": mode,
                    "note": mode_names.get(mode, f"MODE_{mode}"),
                    "start": datetime.fromtimestamp(st).strftime('%Y-%m-%d %H:%M:%S'),
                    "end": datetime.fromtimestamp(et).strftime('%Y-%m-%d %H:%M:%S'),
                    "updated_ts": int(now_ts()), "origin": "supervisor"
                })
            self.local_schedule.replace_all(full_slots)
            log.info('SYNC', f"✅ Cal sync: {len(full_slots)} slots (hash={self._get_schedule_hash()})")
        elif direction in ('pull', 'bidir'):
            self.local_schedule.replace_all(data)
        elif direction == 'push':
            self.local_schedule.merge_incoming(data)
        self._publish_schedule_status()
        self.push_to_ha_local_calendar()


    def _handle_config(self, data):
        to = data.get('to',{})
        if to:
            CONFIG['timeout']['switch'] = to.get('sw', CONFIG['timeout']['switch'])
            CONFIG['timeout']['light'] = to.get('lt', CONFIG['timeout']['light'])
            CONFIG['timeout']['sensor'] = to.get('sn', CONFIG['timeout']['sensor'])
            CONFIG['timeout']['binary_sensor'] = to.get('bs', CONFIG['timeout']['binary_sensor'])

    def _handle_command(self, data):
        dev, cmd, val = data.get('d'), data.get('c','state'), data.get('v')
        if dev in self.devices: self._execute_command(dev, cmd, val)

    def _handle_request(self, data):
        dev = data.get('d')
        if dev in self.devices and self._is_monitored(dev):
            self._send_status(dev, FlowPriority.ALARM_PRIO if self._is_priority(dev) else FlowPriority.COMMAND)

    # =====================================================
    # v6.1 [H4]: STATE RECOVERY — full state dump on req_st
    # After gateway comes back online, supervisor sends t:req_st
    # Gateway responds with b_st batch: all monitored device states
    # =====================================================
    def _handle_req_states(self):
        """H4: Send full state snapshot for all monitored devices.
        Format: {"t":"b_st","g":"G1","ts":<epoch>,"d":[[sid,{payload}],...]}
        Same structure as regular batch 'b', but type 'b_st' so supervisor
        knows it's a recovery snapshot (not incremental)."""
        log.info('STATUS', f"📤 [H4] req_st received — building full state snapshot")
        items = []
        with self.lock:
            for dev in self.devices:
                if not self._is_monitored(dev): continue
                payload = self._build_status_payload(dev, include_ls=True)
                if payload is None: continue
                sid = self.dev_short_id.get(dev, dev)
                items.append([sid, payload])
        if not items:
            log.warn('STATUS', f"⚠️ [H4] No monitored devices to report")
            return
        # Split into packets respecting max_payload
        ts = int(time.time())
        packets = []
        current = []
        for item in items:
            test = current + [item]
            test_pkt = json.dumps({"t": "b_st", "g": CONFIG['id'], "ts": ts, "d": test}, separators=(',', ':'))
            if len(test_pkt) > CONFIG['lora']['max_size'] and current:
                packets.append({"t": "b_st", "g": CONFIG['id'], "ts": ts, "d": current})
                current = [item]
            else:
                current = test
        if current:
            packets.append({"t": "b_st", "g": CONFIG['id'], "ts": ts, "d": current})
        for pkt in packets:
            self.queue[FlowPriority.COMMAND].append(pkt)
        log.info('STATUS', f"📤 {PRIORITY_LABELS[FlowPriority.COMMAND]} [H4] State snapshot: {len(items)} devices → {len(packets)} b_st pkt(s)")

    def _handle_ping(self):
        """v5.0: Enhanced PONG with diagnostics."""
        mon = len([d for d in self.devices if self._is_monitored(d)])
        uptime = int(time.time() - self.start_time)

        # v5.0: Airtime usage estimate (% of theoretical max)
        elapsed = max(time.time() - self.start_time, 1)
        # Assume ~0.5s airtime per TX at SF7/125kHz; duty cycle reference = 1%
        airtime_pct = round(self._airtime_tx_count * 0.5 / elapsed * 100, 2)

        # v5.0: Meshtastic RSSI (best effort)
        rssi = -999
        try:
            if self.mesh and hasattr(self.mesh, 'nodes'):
                for node in self.mesh.nodes.values():
                    snr = node.get('snr', None)
                    r = node.get('rssi', None)
                    if r and r > rssi: rssi = r
        except: pass

        pong = {
            't': 'pong', 'g': CONFIG['id'],
            'up': uptime,
            'dev_total': len(self.devices),
            'dev_mon': mon,
            # v5.0 additions:
            'air': airtime_pct,
            'anom_blk': self._blocked_anomaly_count,
            'rssi': rssi if rssi > -999 else None,
            'hash': self._disc_hash,
            'bat_s': self.batcher.stats if self.batcher else None,
        }
        # Remove None values to save bytes
        pong = {k: v for k, v in pong.items() if v is not None}
        self.queue[FlowPriority.COMMAND].append(pong)
        log.info('PING', f"🏓 PONG: up={uptime}s air={airtime_pct}% anom_blk={self._blocked_anomaly_count} rssi={rssi}")

    def _handle_discovery(self, data):
        """Discovery with hash + batch.
        1. If hash matches → disc_ack (1 packet)
        2. If hash differs → disc_meta (slim) + disc_vio (compact) + batched db
        """
        req_hash = data.get('hash')
        if req_hash and req_hash == self._disc_hash:
            self.queue[FlowPriority.COMMAND].append({
                't': 'disc_ack', 'g': CONFIG['id'], 'hash': self._disc_hash,
                'n': len([d for d in self.devices if self._is_monitored(d)])
            })
            log.info('DISC', f"🔭 Hash match ({self._disc_hash}) — skipping full discovery")
            return

        # Phase 1a: disc_meta (slim — NO vio, fits in <100B)
        meta = {
            't': 'disc_meta', 'g': CONFIG['id'], 'hash': self._disc_hash,
            'dev_n': len([d for d in self.devices if self._is_monitored(d)]),
        }
        self.queue[FlowPriority.COMMAND].append(meta)

        # Phase 1b: disc_vio (compact array format — separate packet)
        vio_list = CONFIG.get('virtual_io', [])
        if vio_list:
            # Format: [id, type_char, name, value_or_null]
            vio_compact = []
            for vs in vio_list:
                entry = [vs['id'], vs['type'][0], vs.get('name', vs['id'])]
                if vs['type'] == 'switch':
                    entry.append(self.vswitch_states.get(vs['id'], vs.get('default', 0)))
                vio_compact.append(entry)
            vio_pkts = batch_split("disc_vio", CONFIG['id'], vio_compact, CONFIG['lora']['max_size'])
            for pkt in vio_pkts:
                self.queue[FlowPriority.COMMAND].append(pkt)

        # Phase 2: batched discovery — all devices in compact format
        items = []
        for dev, info in self.devices.items():
            if not self._is_monitored(dev): continue
            sid = self.dev_short_id.get(dev, -1)
            items.append([sid, dev, info['type'], info['caps']])
        packets = batch_split("db", CONFIG['id'], items, CONFIG['lora']['max_size'])
        for pkt in packets:
            self.queue[FlowPriority.BATCH].append(pkt)
        log.info('DISC', f"🔭 Full discovery: meta + {len(vio_list)} vio + {len(items)} devices in {len(packets)} db pkt(s)")

    def _send_lora(self, msg, priority=None):
        """v6.3: Enhanced send with size guard + Airtime Guard + priority label."""
        if not self._mesh_connected or not self.mesh:
            log.warn('LORA', "⚠️ No antenna — queuing"); return False

        # v6.3 [Airtime Guard]: check if TX is blocked
        now = time.time()
        ag = CONFIG.get('airtime_guard', {})
        if ag.get('enabled', True) and now < self._airtime_blocked_until:
            remaining = int(self._airtime_blocked_until - now)
            log.warn('LORA', f"🛑 [AirtimeGuard] TX blocked for {remaining}s more")
            return False

        try:
            s = json.dumps(msg, separators=(',',':'))
            max_sz = CONFIG['lora']['max_size']
            if len(s) > max_sz:
                log.error('LORA', f"❌ Packet too big ({len(s)}B > {max_sz}B) — DROPPED: {s[:80]}...")
                return False
            plabel = PRIORITY_LABELS.get(priority, "[P?]") if priority is not None else ""
            log.info('LORA', f"📤 TX {plabel} ({len(s)}B) {self._slot_debug_str()}: {s[:120]}")
            self.mesh.sendText(s)
            self.last_tx = time.time()
            self._airtime_tx_count += 1
            self._airtime_tx_bytes += len(s)

            # v6.3 [Airtime Guard]: record TX in rolling window (~0.5s per packet at SF7/125kHz)
            est_airtime = 0.5
            self._airtime_window.append((time.time(), est_airtime))
            if ag.get('enabled', True):
                self._check_airtime_guard(ag)

            return True
        except Exception as e:
            log.error('LORA', f"TX error: {e}")
            self._mesh_connected = False
            try:
                if self.mesh: self.mesh.close()
            except: pass
            self.mesh = None
            return False

    def _check_airtime_guard(self, ag):
        """v6.3: Check rolling 60s airtime window. Block TX if exceeded."""
        now = time.time()
        cutoff = now - 60
        total = sum(at for ts, at in self._airtime_window if ts > cutoff)
        max_s = ag.get('max_seconds_per_min', 10)
        if total > max_s:
            block = ag.get('block_duration', 30)
            self._airtime_blocked_until = now + block
            log.error('LORA', f"🛑 [AirtimeGuard] Airtime {total:.1f}s > {max_s}s/min — TX BLOCKED for {block}s")

    def _add_queue(self, msg, priority): self.queue[priority].append(msg)

    # =====================================================
    # v6.0: MAIN LOOP — deterministic slot-aware priority dispatch
    # ALL priorities respect slot windows (ETAP 1 rule)
    # H2: Zero Loss for P0/P1 on TX failure or slot expiry
    # H3: LIFO dedup at loop start for P2/P3
    # =====================================================
    def _loop(self):
        # H3: Dedup P2/P3 queues at start of every loop cycle
        self._dedup_queues()

        now = time.time()
        if now - self.last_tx < CONFIG['lora']['tx_cooldown']:
            return

        # v6.0: ALL priorities must respect slot — no bypass
        if not self._is_my_slot():
            return

        for p in sorted(FlowPriority):
            if self.queue[p]:
                msg = self.queue[p].popleft()

                # H2: Double-check slot right before TX (may have expired since loop start)
                if not self._is_my_slot():
                    self.queue[p].appendleft(msg)
                    if p in (FlowPriority.ALARM_PRIO, FlowPriority.COMMAND):
                        log.warn('SLOT', f"⚠️ {PRIORITY_LABELS[p]} Slot expired pre-TX — requeued (H2 Zero Loss)")
                    return

                success = self._send_lora(msg, priority=p)

                # H2: On TX failure, P0/P1 return to front of queue (appendleft)
                if not success and p in (FlowPriority.ALARM_PRIO, FlowPriority.COMMAND):
                    self.queue[p].appendleft(msg)
                    log.warn('LORA', f"⚠️ {PRIORITY_LABELS[p]} TX failed — requeued (H2 Zero Loss)")
                return

        # Only check periodic reports if still in our slot and nothing else to send
        self._check_periodic_reports()

    def _dedup_queues(self):
        """v6.0 [H3]: LIFO dedup — keep only latest state per device in P2/P3 queues.
        Called at start of every _loop cycle to minimize stale data in ether."""
        for p in [FlowPriority.DIAGNOSTIC, FlowPriority.BATCH]:
            if not self.queue[p]: continue
            seen = {}
            items = list(self.queue[p])
            deduped = []
            for msg in reversed(items):  # Newest first (LIFO)
                dev = msg.get('d') if isinstance(msg, dict) else None
                # Skip batch packets where 'd' is a list (not a device name)
                if dev and isinstance(dev, str):
                    if dev in seen:
                        continue  # Skip older entry for same device
                    seen[dev] = True
                deduped.append(msg)
            deduped.reverse()
            self.queue[p] = deque(deduped, maxlen=200)

    def _cleanup(self):
        self.running = False
        if self.batcher:
            self.batcher.running = False
            self.batcher.flush()  # Final flush
        self._save_state()
        if self.mqtt: self.mqtt.loop_stop(); self.mqtt.disconnect()
        if self.mesh:
            try: self.mesh.close()
            except: pass
        log.info('MAIN', "Stopped")
        log.close()

if __name__ == "__main__":
    Gateway().start()
