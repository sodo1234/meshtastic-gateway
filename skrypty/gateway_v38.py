#!/usr/bin/env python3
"""
LoRa Zigbee Gateway - v38 (Calendar + Anomaly Dashboard + Anti-Collision)

Schedule Sync: via CalendarTransfer (CRC + retry + zlib compression)
  SUP→GW: sch_b → sch_c×N → sch_e → sch_a (compact: [[sm,dm,mode],...])
  GW→SUP: same (direction='gw_push')
  14 slots = 104B compressed = 1 chunk = 4 LoRa packets
  Mode system: 0=BRAK, 1=PRODUKCJA, 2=PRZERWA, 3=SERWIS (CONFIG)

v35 changes:
- Separate batchers: priority (2s) + monitored (30s)
- Anomaly buffer: compact format + persistent + autoclear fix
- TX jitter: 0-500ms random delay prevents burst collisions
- max_payload: 150B (was 220B), tx_cooldown: 3.0s (was 2.5s)
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
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    "slot": {"enabled": False, "index": 0, "count": 3, "window": 20}, # Slot system — 20s windows
    "batcher": {"enabled": True, "interval": 30, "max_payload": 150, "max_items": 6},
    "airtime_guard": {"enabled": True, "max_seconds_per_min": 10, "block_duration": 30},
    "report_intervals": {"switch": 300, "light": 300, "sensor": 300, "binary_sensor": 300},
    "report_mode": {"switch": "event", "light": "event", "sensor": "cyclic", "binary_sensor": "event"},
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},

    "monitored": ["Test 1", "Test 2", "Temp 1", "Temp 2", "Temp 3", "Temp 4"],

    "priority_devices": ["Leak 1", "Door 1"],

    "operating_mode": "ALWAYS", # ALWAYS | DAY_ONLY | NIGHT_ONLY

    "sync": {
        "timeout": 1800,       # 30 min without sync → request
        "retry_interval": 600, # 10 min between s_req retries
        "max_retries": 3,      # after 3 fails → failsafe mode
        "failsafe_interval": 900,  # 15 min s_req in failsafe
        "drift_threshold": 120,    # 2 min — anomaly if offset > this
    },

    "heartbeat_interval": 1200,  # 20 min
    "cmd_retry": {"enabled": True, "timeout": 5, "retries": 1},
    "delta": {"enabled": False, "temperature": 0.5, "humidity": 2, "battery": 5, "brightness": 10},

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
    "critical_alarm": {"enabled": True, "types": ["smoke","water_leak"]},
    "calendar": {"chunk_size": 140, "transfer_timeout": 60, "retry_max": 2, "chunk_delay": 6.0},
    "log": {"file": "/tmp/gateway.log", "max_bytes": 5_000_000, "backup_count": 3},
    "mode_names": {0: "BRAK PRODUKCJI", 1: "PRODUKCJA", 2: "PRZERWA", 3: "SERWIS"},
    "virtual_io": [
        {"id": "vs_test",  "type": "switch", "name": "Test Switch", "default": 0},
        {"id": "vb_test", "type": "button", "name": "Test Button"},
    ],
    "ha_api": {
        "url": "http://localhost:8123",
        "token": "",
        "ics_path": "/var/lib/homeassistant/homeassistant/.storage/local_calendar.g1.ics",
    },
    # v34: Custom params — editable on gateway dashboard, synced bidirectionally with supervisor
    # Gateway is source of truth. Supervisor mirrors and can push changes back.
    "custom_params": {
        "p1": {"enabled": True,  "key": "param1", "default": 20, "name": "P1", "min": 0, "max": 1000, "step": 1, "unit": "", "icon": "mdi:numeric-1-circle"},
        "p2": {"enabled": False, "key": "param2", "default": 40, "name": "P2", "min": 0, "max": 1000, "step": 1, "unit": "", "icon": "mdi:numeric-2-circle"},
        "p3": {"enabled": False, "key": "param3", "default": 60, "name": "P3", "min": 0, "max": 1000, "step": 1, "unit": "", "icon": "mdi:numeric-3-circle"},
    },
}


# =====================================================
# v35: FLOW PRIORITY — 4-level deterministic SCADA
# ALL priorities respect slot windows + tx_cooldown
# =====================================================
class FlowPriority(IntEnum):
    SYSTEM    = 0   # P0: pong, hb, disc_*, cal_*, vsw_st, params
    PRIORITY  = 1   # P1: priority device status (10s dedup batch)
    ANOMALY   = 2   # P2: anomaly batch (compact ab packets)
    MONITORED = 3   # P3: monitored device status (30s dedup batch)

Priority = FlowPriority

PRIORITY_LABELS = {
    FlowPriority.SYSTEM:    "🔧 [P0:SYS]",
    FlowPriority.PRIORITY:  "🚨 [P1:PRIO]",
    FlowPriority.ANOMALY:   "⚠️ [P2:ANOM]",
    FlowPriority.MONITORED: "📦 [P3:MON]",
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
# v25: UNIVERSAL BATCHER — all device data goes through here
# Format: {"t":"b","g":"G1","ts":<epoch>,"d":[[sid,{payload}],...]}
# Triggers: 30s timer / 12 items / priority device / cmd response
# Dedup: last value wins per device
# =====================================================
class Batcher:
    def __init__(self, gw_id, flush_callback, interval=30, max_payload=150,
                 max_items=6, tag="BATCH"):
        self.gw_id = gw_id
        self.flush_callback = flush_callback
        self.interval = interval
        self.max_payload = max_payload
        self.max_items = max_items
        self.tag = tag
        self.buffer = {}   # {dev_id: payload_dict} — dedup: last wins
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.running = True
        self._stats = {"batched": 0, "flushed": 0, "deduped": 0}

    def add(self, dev, payload, force_flush=False):
        """Add to buffer. force_flush=True for alarm retransmit only."""
        do_flush = force_flush
        with self.lock:
            if dev in self.buffer:
                self._stats["deduped"] += 1
            self.buffer[dev] = payload
            self._stats["batched"] += 1
            if len(self.buffer) >= self.max_items:
                do_flush = True
        if do_flush:
            self.flush()

    def start(self):
        threading.Thread(target=self._flush_loop, daemon=True, name=f"batcher-{self.tag}").start()

    def _flush_loop(self):
        while self.running:
            time.sleep(1)
            if time.time() - self.last_flush >= self.interval:
                self.flush()

    def flush(self):
        with self.lock:
            items = list(self.buffer.items())
            self.buffer.clear()
            self.last_flush = time.time()
        if not items: return []
        packets = self._split_into_packets(items)
        for pkt in packets:
            self.flush_callback(pkt)
            self._stats["flushed"] += 1
        n = len(items)
        log.info('BATCH', f"📦 [{self.tag}] Flushed {n} items → {len(packets)} pkt(s)")
        return packets

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
        with self.lock: return {**self._stats, "pending": len(self.buffer)}


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
        """v31: Delay between chunks — from CONFIG['calendar']['chunk_delay']."""
        return CONFIG['calendar'].get('chunk_delay', 6.0)

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock: self.outgoing[tid] = {'chunks':chunks,'gw':gw,'dir':direction,'retries':0,'ts':now_ts(),'ack_received':False}
        self.queue_fn({"t":"cal_begin","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, FlowPriority.SYSTEM)
        def _s():
            delay = self._chunk_delay()  # [K2]
            for i, c in enumerate(chunks):
                time.sleep(delay)
                self.queue_fn({"t":"cal_chunk","tid":tid,"s":i,"d":c}, FlowPriority.SYSTEM)
            # [K4] Send sch_e with retry — LoRa packet loss recovery
            SCH_E_RETRIES = 3
            SCH_E_WAIT = 15
            for attempt in range(SCH_E_RETRIES):
                time.sleep(delay)
                with self.lock:
                    if tid not in self.outgoing:
                        return
                self.queue_fn({"t":"cal_end","tid":tid}, FlowPriority.SYSTEM)
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
                        self.queue_fn({"t":"cal_chunk","tid":tid,"s":s,"d":info['chunks'][s]}, FlowPriority.SYSTEM)
                time.sleep(delay)
                self.queue_fn({"t":"cal_end","tid":tid}, FlowPriority.SYSTEM)
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
        self._offline_first_seen = {}  # v37: grace period for flapping prevention
        self.stagnation_reported = {}
        self.temp_anomaly_reported = {}
        self.hum_anomaly_reported = {}
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

        # v25: 3-priority queues (maxlen=200)
        self.queue = {p: deque(maxlen=200) for p in FlowPriority}
        self.last_tx = 0
        self.start_time = time.time()
        self.running = True
        self.lock = threading.Lock()
        self.discovery_done = False

        # Batcher instance (created in start())
        self.batcher_prio = None  # v35: set in start()
        self.batcher_mon = None   # v35: set in start()

        # Airtime tracking
        self._airtime_tx_count = 0
        self._airtime_tx_bytes = 0
        self._airtime_window = deque(maxlen=200)
        self._airtime_blocked_until = 0

        # Blocked anomaly counter (for PONG diagnostics)
        self._blocked_anomaly_count = 0

        # Discovery hash cache
        self._disc_hash = ""
        self._saved_disc_hash = ""  # v27: loaded from gw_state.json for change detection

        # Short ID map
        self.dev_short_id = {}
        self.dev_short_rev = {}

        # Proactive heartbeat
        self._last_heartbeat_ts = 0
        self._last_z2m_ts = 0

        # Virtual switches
        self.vswitch_states = {}
        self._vio_mqtt_registered = set()
        self._last_cmd_seq = {}
        self._vsw_debounce = {}

        # Anomaly buffer
        self._anomaly_pending = {}   # v35: {dev:type: [sid,short,val]} — items to send

        # v25 [Echo Filter]: suppress Z2M event echo for 10s after cmd
        self._echo_suppress = {}
        self._echo_window = 10
        self._cmd_flush_pending = False  # v32: cmd response aggregation window
        self._prio_flush_pending = False  # v34: priority dedup flush window

        # v25 [Alarm Retransmit]: track active alarms for priority devices
        self._active_alarms = {}

        # v26 [Context-Aware Sync]: time offset + sun state from supervisor
        self._time_offset = 0.0       # seconds to add to time.time() for synced time
        self._sun_state = "day"       # "day" or "night" — from supervisor sync
        self._is_synced = False       # True after receiving first valid sync
        self._last_sync_ts = 0        # local time.time() of last sync reception
        self._last_sync_seq = -1      # sequence number for out-of-order rejection
        self._sync_retry_count = 0    # retries since last successful sync
        self._failsafe_mode = False   # True when sync lost and retries exhausted
        # v30: General supervisor contact tracking
        self._last_supervisor_contact = 0  # updated on ANY incoming LoRa message

    def _is_priority(self, dev): return dev in CONFIG.get('priority_devices', [])
    def _is_monitored(self, dev):
        # Priority devices are automatically monitored (Rule 3)
        if self._is_priority(dev): return True
        m = CONFIG.get('monitored', [])
        return not m or dev in m
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly',{}).get('auto_clear',{}).get(atype, True)

    # =====================================================
    # v26: SYNCED TIME + CONTEXT-AWARE SUPPRESSION
    # =====================================================
    def _synced_ts(self):
        """Returns corrected unix timestamp (local + offset from supervisor sync)."""
        return time.time() + self._time_offset

    def _synced_str(self):
        """Returns formatted datetime string using synced time."""
        return datetime.fromtimestamp(self._synced_ts()).strftime('%Y-%m-%d %H:%M:%S')

    def _current_context(self):
        """Returns current context: 'day', 'night', or failsafe=ALWAYS."""
        if self._failsafe_mode:
            return 'ALWAYS'
        return self._sun_state

    def _should_suppress(self, dev):
        """v31: Selective suppression based on operating_mode + sun state.
        Priority devices NEVER suppressed. Failsafe = ALWAYS report."""
        if self._is_priority(dev):
            return False
        rtype = CONFIG.get('operating_mode', 'ALWAYS')
        if rtype == 'ALWAYS':
            return False
        ctx = self._current_context()
        if ctx == 'ALWAYS':
            return False  # Failsafe = report everything
        if rtype == 'NIGHT_ONLY' and ctx == 'day':
            return True
        if rtype == 'DAY_ONLY' and ctx == 'night':
            return True
        return False

    def _handle_sync(self, data):
        """v27: Process time+sun sync from supervisor. Publish rich MQTT entity."""
        seq = data.get('seq', 0)
        if seq <= self._last_sync_seq:
            log.debug('SYNC', f"🔄 Stale sync seq={seq} <= {self._last_sync_seq}")
            return
        self._last_sync_seq = seq
        remote_ts = data.get('sec', 0)
        sun = data.get('sun', 'day')
        if remote_ts <= 0: return
        local_ts = time.time()
        self._time_offset = remote_ts - local_ts
        old_sun = self._sun_state
        self._sun_state = sun
        self._last_sync_ts = local_ts
        self._is_synced = True
        self._sync_retry_count = 0
        was_failsafe = self._failsafe_mode
        self._failsafe_mode = False
        # v27: Publish rich sync entity to local MQTT
        self._publish_sync_entity()
        # v27: Check time drift anomaly
        drift = abs(self._time_offset)
        drift_threshold = CONFIG.get('sync', {}).get('drift_threshold', 120)
        if drift > drift_threshold:
            log.warn('SYNC', f"🔄⚠️ Time drift {drift:.0f}s > {drift_threshold}s — anomaly!")
        # Log
        sun_icon = '☀️' if sun == 'day' else '🌙'
        ctx_change = f" (was {old_sun})" if old_sun != sun else ""
        if was_failsafe: ctx_change += " ← RECOVERED"
        log.info('SYNC', f"🔄 {sun_icon} OK: {self._synced_str()} offset={self._time_offset:+.1f}s sun={sun}{ctx_change}")

    def _publish_sync_entity(self):
        """v30: Publish sync status as MQTT entity for dashboard.
        Topic: lora/gw/{id}/sync — shows SCADA time, system time, offset, mode."""
        now_local = datetime.now()
        sync_age = int(time.time() - self._last_sync_ts) if self._last_sync_ts > 0 else -1
        drift_threshold = CONFIG.get('sync', {}).get('drift_threshold', 120)
        drift_ok = abs(self._time_offset) <= drift_threshold
        if self._failsafe_mode: mode_str = "FAILSAFE"
        elif not self._is_synced: mode_str = "AWAITING"
        elif self._sun_state == 'night': mode_str = "NIGHT"
        else: mode_str = "DAY"
        # SCADA time only valid when synced
        if self._is_synced:
            scada_str = datetime.fromtimestamp(self._synced_ts()).strftime('%Y-%m-%d %H:%M:%S')
        else:
            scada_str = "--"
        data = {
            "scada_time": scada_str,
            "system_time": now_local.strftime('%Y-%m-%d %H:%M:%S'),
            "offset_sec": round(self._time_offset, 1),
            "mode": mode_str,
            "sun": self._sun_state,
            "synced": self._is_synced,
            "failsafe": self._failsafe_mode,
            "sync_age_sec": sync_age,
            "drift_ok": drift_ok,
        }
        self.mqtt.publish(f"lora/gw/{CONFIG['id']}/sync", json.dumps(data), retain=True)

    def _sync_watchdog_loop(self):
        """v27: Monitor sync freshness + periodic sync entity update + drift anomaly."""
        scfg = CONFIG.get('sync', {})
        timeout = scfg.get('timeout', 1800)
        retry_iv = scfg.get('retry_interval', 600)
        max_r = scfg.get('max_retries', 3)
        fs_iv = scfg.get('failsafe_interval', 900)
        drift_threshold = scfg.get('drift_threshold', 120)
        while self.running:
            time.sleep(60)
            # v32: Update diag entity every 60s
            self._publish_gw_diag()
            # v27: Update sync entity every 60s (dashboard sees fresh sync_age)
            if self._is_synced or self._failsafe_mode:
                self._publish_sync_entity()
            if not self._is_synced:
                if self._last_sync_ts == 0 and time.time() - self.start_time > 60:
                    self.queue[FlowPriority.SYSTEM].append({"t": "s_req", "g": CONFIG['id']})
                    self._last_sync_ts = time.time()
                    log.info('SYNC', f"🔄 Initial sync request")
                continue
            # v27: Drift anomaly check
            drift = abs(self._time_offset)
            if drift > drift_threshold:
                log.warn('SYNC', f"🔄⚠️ Drift {drift:.0f}s > {drift_threshold}s")
            elapsed = time.time() - self._last_sync_ts
            if self._failsafe_mode:
                if elapsed > fs_iv:
                    self.queue[FlowPriority.SYSTEM].append({"t": "s_req", "g": CONFIG['id']})
                    self._last_sync_ts = time.time()
                    log.warn('SYNC', f"🔄⚠️ Failsafe retry (every {fs_iv}s)")
            elif elapsed > timeout:
                self._sync_retry_count += 1
                if self._sync_retry_count > max_r:
                    self._failsafe_mode = True
                    nsm = 'ALWAYS'
                    log.error('SYNC', f"🔄❌ FAILSAFE: no sync after {max_r} retries — mode={nsm}")
                    self._publish_sync_entity()
                else:
                    self.queue[FlowPriority.SYSTEM].append({"t": "s_req", "g": CONFIG['id']})
                    self._last_sync_ts = time.time()
                    log.warn('SYNC', f"🔄 Sync stale ({int(elapsed)}s) — retry {self._sync_retry_count}/{max_r}")

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
        """v33: Check if current second falls within gateway's slot window.
        Uses synced time (not local clock) so all gateways agree on slot boundaries."""
        cfg = CONFIG.get('slot', {})
        if not cfg.get('enabled', True): return True
        sec = int(self._synced_ts()) % 60
        window = cfg.get('window', 20)
        idx = cfg.get('index', 0)
        slot_start = idx * window
        slot_end = slot_start + window
        # v25 Slot Guard: need >= 3s remaining
        return slot_start <= sec < (slot_end - 3)

    def _slot_time_remaining(self):
        """Return (used, total) seconds in current slot window."""
        cfg = CONFIG.get('slot', {})
        if not cfg.get('enabled', True): return (0, 20)
        sec = int(self._synced_ts()) % 60
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
        """v31: Minimal persist — only data NOT available from Z2M."""
        try:
            data = {
                'vswitch_states': self.vswitch_states,
                'disc_hash': self._disc_hash,
                'production_mode': self.production_mode,
                'saved_at': now_str()
            }
            with open(PERSIST_FILE, 'w') as f: json.dump(data, f)
        except Exception as e: log.error('MAIN', f"Save state: {e}")

    def _load_state(self):
        """v31: Load minimal persist. Z2M provides device states via MQTT retain."""
        try:
            with open(PERSIST_FILE, 'r') as f: data = json.load(f)
            self.vswitch_states = data.get('vswitch_states', {})
            self._saved_disc_hash = data.get('disc_hash', '')
            self.production_mode = data.get('production_mode', False)
            log.info('MAIN', f"♻️ Loaded state: {len(self.vswitch_states)} vswitches, hash={self._saved_disc_hash[:8] or 'none'}")
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
        if not av:
            if not was:
                # v37: Grace period — must be offline for 120s before reporting
                first = self._offline_first_seen.get(dev)
                if first is None:
                    self._offline_first_seen[dev] = now_ts()
                    return  # Start grace period, don't report yet
                if now_ts() - first < 120:
                    return  # Still in grace period
                self.offline_reported[dev] = True
                self._offline_first_seen.pop(dev, None)
                self._send_anomaly(dev, "device_offline", None)
        else:
            self._offline_first_seen.pop(dev, None)  # Reset grace timer
            if was:
                self.offline_reported[dev] = False
                if self._is_auto_clear_enabled('device_offline'):
                    self._send_anomaly(dev, "device_online", None)

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
                # v37: Check auto_clear for the SPECIFIC anomaly that was active
                clear_type = f"temp_{cur}"  # 'temp_high' or 'temp_low'
                self.temp_anomaly_reported[dev] = None
                if self._is_auto_clear_enabled(clear_type):
                    self._send_anomaly(dev, "temp_ok", temp)
        if 'humidity' in data:
            hum, cur = data['humidity'], self.hum_anomaly_reported.get(dev)
            hh, hl = det.get('hum_high',{}), det.get('hum_low',{})
            if hh.get('enabled',True) and hum >= hh.get('threshold',95):
                if cur != 'high': self.hum_anomaly_reported[dev] = 'high'; self._send_anomaly(dev, "hum_high", hum)
            elif hl.get('enabled',True) and hum <= hl.get('threshold',5):
                if cur != 'low': self.hum_anomaly_reported[dev] = 'low'; self._send_anomaly(dev, "hum_low", hum)
            elif cur:
                # v37: Check auto_clear for the SPECIFIC anomaly that was active
                clear_type = f"hum_{cur}"  # 'hum_high' or 'hum_low'
                self.hum_anomaly_reported[dev] = None
                if self._is_auto_clear_enabled(clear_type):
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

    # v34: Anomaly type short codes (2-char)
    _ANOM_MAP = {
        'device_offline':'do', 'low_battery':'lb', 'critical_battery':'cb',
        'temp_high':'th', 'temp_low':'tl', 'hum_high':'hh', 'hum_low':'hl',
        'stagnation':'sg', 'smoke':'sk', 'water_leak':'wl',
        # Clear types
        'temp_ok':'to', 'battery_ok':'bo', 'hum_ok':'ho',
        'device_online':'dn', 'stagnation_clear':'sc',
    }
    _ANOM_REV = {v: k for k, v in _ANOM_MAP.items()}
    _ANOM_CLEAR_TYPES = {'temp_ok','battery_ok','hum_ok','device_online','stagnation_clear'}

    def _send_anomaly(self, dev, atype, value):
        """v35: Add anomaly to buffer → flushed as compact ab at P2.
        Buffer dedup by dev:type key. Flush clears buffer entirely.
        Cyclic re-detection via anomaly_loop handles retransmit."""
        sid = self.dev_short_id.get(dev, dev)
        short_type = self._ANOM_MAP.get(atype, atype[:2])
        with self.lock:
            self._anomaly_pending[f"{dev}:{atype}"] = [sid, short_type, value]
        is_prio = self._is_priority(dev)
        critical_types = CONFIG.get('critical_alarm', {}).get('types', [])
        if atype in critical_types and is_prio:
            self._start_alarm_retransmit(dev, atype)
            log.warn('ANOMALY', f"🚨🔴 CRITICAL: {dev} → {atype}" + (f"={value}" if value is not None else ""))
        else:
            is_clear = atype in self._ANOM_CLEAR_TYPES
            icon = '🔴' if atype in ['device_offline'] else ('✅' if is_clear else '🟡')
            log.info('ANOMALY', f"🚨{icon} {dev} → {atype}" + (f"={value}" if value is not None else ""))

    def _flush_anomaly_buffer(self):
        """v35: Flush buffer as 1+ ab packets. Buffer cleared entirely after flush.
        Anomaly_loop re-detects on next cycle → acts as retransmit if lost."""
        with self.lock:
            if not self._anomaly_pending: return
            items = list(self._anomaly_pending.values())
            self._anomaly_pending.clear()
        max_sz = CONFIG['lora']['max_size']
        ts = int(time.time())
        chunks, current = [], []
        for item in items:
            test = {"t": "ab", "g": CONFIG['id'], "ts": ts, "d": current + [item]}
            if len(json.dumps(test, separators=(',', ':'))) > max_sz and current:
                chunks.append(current); current = [item]
            else:
                current.append(item)
        if current: chunks.append(current)
        for chunk in chunks:
            self.queue[FlowPriority.ANOMALY].append({"t": "ab", "g": CONFIG['id'], "ts": ts, "d": chunk})
        log.info('ANOMALY', f"🚨📤 ab: {len(items)} items → {len(chunks)} pkt(s)")

    def _start_alarm_retransmit(self, dev, atype):
        """v25: Alarm retransmit — flush batcher every 15s × 3 while alarm active.
        Always sends LATEST state (batcher dedup ensures freshness).
        Stops if alarm cleared or after 3 retransmits."""
        key = f"{dev}:{atype}"
        with self.lock:
            self._active_alarms[key] = {"dev": dev, "type": atype, "ts": time.time(), "count": 0}
        def _retransmit():
            for i in range(3):
                time.sleep(15)
                with self.lock:
                    info = self._active_alarms.get(key)
                    if not info:
                        return  # Alarm cleared
                    info['count'] = i + 1
                # Re-read current state and add to batcher
                state = self.states.get(dev, {})
                # Check if alarm still active
                alarm_still_active = False
                if atype == 'smoke' and state.get('smoke'):
                    alarm_still_active = True
                elif atype == 'water_leak' and state.get('water_leak'):
                    alarm_still_active = True
                if not alarm_still_active:
                    with self.lock: self._active_alarms.pop(key, None)
                    log.info('ANOMALY', f"🚨🟢 Alarm cleared: {dev}/{atype} — stopping retransmit")
                    return
                # v35: Re-add to pending for retransmit
                sid = self.dev_short_id.get(dev, dev)
                short_type = self._ANOM_MAP.get(atype, atype[:2])
                with self.lock:
                    self._anomaly_pending[f"{dev}:{atype}"] = [sid, short_type, None]
                self._flush_anomaly_buffer()
                log.warn('ANOMALY', f"🚨🔄 Retransmit {i+1}/3: {dev}/{atype}")
            with self.lock: self._active_alarms.pop(key, None)
        threading.Thread(target=_retransmit, daemon=True, name=f"alarm-{dev}").start()

    def _clear_alarm_retransmit(self, dev, atype):
        """Clear alarm retransmit when device state returns to safe."""
        key = f"{dev}:{atype}"
        with self.lock:
            if key in self._active_alarms:
                del self._active_alarms[key]
                log.info('ANOMALY', f"🚨🟢 Alarm retransmit cancelled: {dev}/{atype}")

    # =====================================================
    # REQ-8: dump_anom = LIVE re-check (unchanged logic)
    # =====================================================
    def _handle_dump_anom(self):
        """v34: dump_anom = LIVE re-check. Compact format: [[sid,"th",value], ...]
        Single ts per packet. Uses short IDs + 2-char type codes."""
        log.info('ANOMALY', f"📋 Dump — LIVE re-check all monitored devices")
        def _do():
            det = CONFIG['anomaly']['detection']
            found = []
            for dev in self.devices:
                sid = self.dev_short_id.get(dev, dev)
                ds = self.states.get(dev, {})
                # v36: offline + battery for ALL devices (not just monitored)
                bat = ds.get('battery')
                if bat is not None:
                    cc, lc = det.get('critical_battery',{}), det.get('low_battery',{})
                    if cc.get('enabled',True) and bat < cc.get('threshold',15):
                        found.append([sid, "cb", bat])
                    elif lc.get('enabled',True) and bat < lc.get('threshold',25):
                        found.append([sid, "lb", bat])
                oc = det.get('device_offline',{})
                if oc.get('enabled',True) and not self._calc_availability(dev):
                    found.append([sid, "do", None])
                # Value anomalies only for monitored devices
                if not self._is_monitored(dev): continue
                sc = det.get('stagnation',{})
                if sc.get('enabled',True) and self.devices.get(dev,{}).get('type') in ['switch','light']:
                    h = (now_ts() - self.last_state_change.get(dev, self.start_time)) / 3600
                    if h >= sc.get('hours',48):
                        found.append([sid, "sg", int(h)])
                temp = ds.get('temperature')
                if temp is not None:
                    th, tl = det.get('temp_high',{}), det.get('temp_low',{})
                    if th.get('enabled',True) and temp >= th.get('threshold',50):
                        found.append([sid, "th", temp])
                    elif tl.get('enabled',True) and temp <= tl.get('threshold',-10):
                        found.append([sid, "tl", temp])
                hum = ds.get('humidity')
                if hum is not None:
                    hh, hl = det.get('hum_high',{}), det.get('hum_low',{})
                    if hh.get('enabled',True) and hum >= hh.get('threshold',95):
                        found.append([sid, "hh", hum])
                    elif hl.get('enabled',True) and hum <= hl.get('threshold',5):
                        found.append([sid, "hl", hum])
            if found:
                pkt = {"t":"ab","g":CONFIG['id'],"ts":int(now_ts()),"d":found}
                s = len(json.dumps(pkt, separators=(',',':')))
                if s <= CONFIG['lora']['max_size']:
                    self.queue[FlowPriority.SYSTEM].append(pkt)
                else:
                    # Split into multiple packets
                    for i in range(0, len(found), 8):
                        chunk = found[i:i+8]
                        self.queue[FlowPriority.SYSTEM].append(
                            {"t":"ab","g":CONFIG['id'],"ts":int(now_ts()),"d":chunk})
                log.info('ANOMALY', f"📋 Dump: {len(found)} anomalies (compact)")
            else:
                self.queue[FlowPriority.SYSTEM].append({"t":"ab","g":CONFIG['id'],"ts":int(now_ts()),"d":[]})
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
        Packet: {"t":"cal_pre","g":"G1","mode":1,"in_sec":5,"next_ts":...}
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
            self._write_ics_file(ics_path, slots, mode_names)
        else:
            if events_mqtt: log.info('CAL', f"📅 {len(events_mqtt)} events → MQTT only (no ics_path)")

    def _write_ics_file(self, ics_path, slots, mode_names):
        """v30: Write ICS file — preserves ownership for Docker HA.
        Detects original file owner and applies same UID:GID after write."""
        def _do():
            try:
                # v30: Capture original file ownership BEFORE writing
                orig_uid, orig_gid = None, None
                if os.path.exists(ics_path):
                    st = os.stat(ics_path)
                    orig_uid, orig_gid = st.st_uid, st.st_gid
                elif os.path.exists(os.path.dirname(ics_path)):
                    st = os.stat(os.path.dirname(ics_path))
                    orig_uid, orig_gid = st.st_uid, st.st_gid

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
                    # v38: UID includes timestamp → HA always sees new events on update
                    uid = f"lora-{sid}-{int(now_ts())}@lora-bms"
                    lines.extend([
                        'BEGIN:VEVENT',
                        f'UID:{uid}',
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
                # v30: Restore original ownership (Docker HA fix)
                if orig_uid is not None:
                    try: os.chown(tmp_path, orig_uid, orig_gid)
                    except: pass
                os.replace(tmp_path, ics_path)
                log.info('CAL', f"✅ ICS written: {len(slots)} events → {ics_path}")

                self._reload_ha_calendar()

            except PermissionError as e:
                log.error('CAL', f"❌ ICS permission denied: {e}")
                log.error('CAL', f"   Fix: sudo chown -R $(id -u):$(id -g) {os.path.dirname(ics_path)}")
                log.error('CAL', f"   Or:  sudo setfacl -R -m u:$(whoami):rwx {os.path.dirname(ics_path)}")
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

    def _register_sync_entity(self):
        """v30: Register MQTT autodiscovery entities for sync dashboard.
        Uses lora/gw/{id}/sync as state_topic (updated every 60s by watchdog)."""
        p = "homeassistant"
        gw = CONFIG['id'].lower()
        st = f"lora/gw/{CONFIG['id']}/sync"
        di = {"identifiers": [f"lora_gw_{gw}"],
              "name": f"LoRa Gateway {CONFIG['id']}",
              "model": "LoRa Gateway", "manufacturer": "SCADA"}
        # SCADA time (synced from supervisor)
        self.mqtt.publish(f"{p}/sensor/lora_gw_{gw}_scada_time/config", json.dumps({
            "name": f"GW {CONFIG['id']} SCADA Time",
            "object_id": f"lora_gw_{gw}_scada_time",
            "unique_id": f"lora_gw_{gw}_scada_time",
            "state_topic": st, "value_template": "{{ value_json.scada_time }}",
            "json_attributes_topic": st,
            "device": di, "icon": "mdi:clock-check"
        }), retain=True)
        # System time (local gateway clock)
        self.mqtt.publish(f"{p}/sensor/lora_gw_{gw}_system_time/config", json.dumps({
            "name": f"GW {CONFIG['id']} System Time",
            "object_id": f"lora_gw_{gw}_system_time",
            "unique_id": f"lora_gw_{gw}_system_time",
            "state_topic": st, "value_template": "{{ value_json.system_time }}",
            "device": di, "icon": "mdi:clock-outline"
        }), retain=True)
        # Mode (DAY/NIGHT/FAILSAFE)
        self.mqtt.publish(f"{p}/sensor/lora_gw_{gw}_sync_mode/config", json.dumps({
            "name": f"GW {CONFIG['id']} Mode",
            "object_id": f"lora_gw_{gw}_sync_mode",
            "unique_id": f"lora_gw_{gw}_sync_mode",
            "state_topic": st, "value_template": "{{ value_json.mode }}",
            "device": di, "icon": "mdi:weather-sunny"
        }), retain=True)
        # Synced binary sensor
        self.mqtt.publish(f"{p}/binary_sensor/lora_gw_{gw}_synced/config", json.dumps({
            "name": f"GW {CONFIG['id']} Synced",
            "object_id": f"lora_gw_{gw}_synced",
            "unique_id": f"lora_gw_{gw}_synced",
            "state_topic": st,
            "value_template": "{{ 'ON' if value_json.synced else 'OFF' }}",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "connectivity", "device": di
        }), retain=True)
        # Time offset sensor
        self.mqtt.publish(f"{p}/sensor/lora_gw_{gw}_time_offset/config", json.dumps({
            "name": f"GW {CONFIG['id']} Time Offset",
            "object_id": f"lora_gw_{gw}_time_offset",
            "unique_id": f"lora_gw_{gw}_time_offset",
            "state_topic": st, "value_template": "{{ value_json.offset_sec }}",
            "unit_of_measurement": "s",
            "device": di, "icon": "mdi:clock-fast"
        }), retain=True)
        # Request Sync button
        self.mqtt.publish(f"{p}/button/lora_gw_{gw}_request_sync/config", json.dumps({
            "name": f"GW {CONFIG['id']} Request Sync",
            "object_id": f"lora_gw_{gw}_request_sync",
            "unique_id": f"lora_gw_{gw}_request_sync",
            "command_topic": f"lora/gw/{CONFIG['id']}/cmd/request_sync",
            "device": di, "icon": "mdi:sync"
        }), retain=True)
        # v32: Gateway diag entities (from lora/gw/{id}/diag topic)
        dt = f"lora/gw/{CONFIG['id']}/diag"
        for eid, name, tpl, icon, unit in [
            ("operating_mode", "Operating Mode", "{{ value_json.operating_mode }}", "mdi:tune", None),
            ("uptime", "Uptime", "{{ value_json.uptime }}", "mdi:timer", "s"),
            ("batcher_pending", "Batcher Pending", "{{ value_json.batcher_pending }}", "mdi:tray-full", None),
            ("batcher_flushed", "Batcher Flushed", "{{ value_json.batcher_flushed }}", "mdi:tray-arrow-up", None),
            ("tx_count", "TX Count", "{{ value_json.tx_count }}", "mdi:radio-tower", None),
        ]:
            cfg = {"name": f"GW {CONFIG['id']} {name}", "object_id": f"lora_gw_{gw}_{eid}",
                   "unique_id": f"lora_gw_{gw}_{eid}", "state_topic": dt,
                   "value_template": tpl, "device": di, "icon": icon}
            if unit: cfg["unit_of_measurement"] = unit
            self.mqtt.publish(f"{p}/sensor/lora_gw_{gw}_{eid}/config", json.dumps(cfg), retain=True)
        # v34: Custom param NUMBER entities (editable from gateway dashboard)
        cp = CONFIG.get('custom_params', {})
        for pid in ['p1', 'p2', 'p3']:
            pcfg = cp.get(pid, {})
            pname = pcfg.get('name', pcfg.get('key', pid))
            picon = pcfg.get('icon', 'mdi:tune-variant')
            self.mqtt.publish(f"{p}/number/lora_gw_{gw}_param_{pid}/config", json.dumps({
                "name": f"GW {CONFIG['id']} {pname}",
                "object_id": f"lora_gw_{gw}_param_{pid}",
                "unique_id": f"lora_gw_{gw}_param_{pid}",
                "state_topic": f"lora/gw/{CONFIG['id']}/param/{pid}",
                "command_topic": f"lora/gw/{CONFIG['id']}/param/{pid}/set",
                "min": pcfg.get('min', 0), "max": pcfg.get('max', 1000),
                "step": pcfg.get('step', 1),
                "unit_of_measurement": pcfg.get('unit', '') or None,
                "device": di, "icon": picon
            }), retain=True)
            # Publish current value
            rp = CONFIG.get('runtime_params', {})
            val = rp.get(pcfg.get('key', pid), pcfg.get('default', 0))
            self.mqtt.publish(f"lora/gw/{CONFIG['id']}/param/{pid}", str(val), retain=True)
        log.info('CFG', f"🔄 Sync + diag + param entities registered for {CONFIG['id']}")

    def _publish_gw_diag(self):
        """v33: Publish gateway diagnostics + params to local MQTT (periodic)."""
        data = {
            "operating_mode": CONFIG.get('operating_mode', 'ALWAYS'),
            "uptime": int(time.time() - self.start_time),
            "batcher_pending": (self.batcher_prio.pending_count() if self.batcher_prio else 0) + (self.batcher_mon.pending_count() if self.batcher_mon else 0),
            "batcher_flushed": (self.batcher_prio.stats.get('flushed', 0) if self.batcher_prio else 0) + (self.batcher_mon.stats.get('flushed', 0) if self.batcher_mon else 0),
            "tx_count": self._airtime_tx_count,
        }
        self.mqtt.publish(f"lora/gw/{CONFIG['id']}/diag", json.dumps(data), retain=True)
        self._publish_params()

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
        v38: No seq check — always apply latest value. Idempotent operation."""
        vid = data.get('id')
        val = data.get('v', 0)
        if vid is None: return
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
            self.queue[FlowPriority.SYSTEM].append({
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
        self.queue[FlowPriority.SYSTEM].append({
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
        """v26: Extended heartbeat every 20-30 min with sync context."""
        while self.running:
            time.sleep(10)
            interval = CONFIG.get('heartbeat_interval', 1200)
            now = now_ts()
            if now - self._last_heartbeat_ts >= interval:
                self._send_heartbeat()
                self._last_heartbeat_ts = now

    def _build_diag_payload(self, pkt_type='hb'):
        """v32: Unified diagnostic payload — identical for HB and PONG.
        Only difference: t='hb' (periodic) vs t='pong' (response to ping)."""
        uptime = int(time.time() - self.start_time)
        pri = len([d for d in self.devices if self._is_priority(d)])
        mon = len([d for d in self.devices if self._is_monitored(d) and not self._is_priority(d)])
        elapsed = max(time.time() - self.start_time, 1)
        air = round(self._airtime_tx_count * 0.5 / elapsed * 100, 2)
        z2m_age = int(now_ts() - self._last_z2m_ts) if self._last_z2m_ts > 0 else -1

        if self._failsafe_mode: mode_char = 'F'
        elif self._sun_state == 'night': mode_char = 'N'
        else: mode_char = 'D'
        sync_age = int(time.time() - self._last_sync_ts) if self._last_sync_ts > 0 else -1
        q_depth = sum(len(self.queue[p]) for p in FlowPriority)
        sup_contact = int(time.time() - self._last_supervisor_contact) if self._last_supervisor_contact > 0 else -1

        msg = {
            't': pkt_type, 'g': CONFIG['id'],
            'up': uptime, 'dev': len(self.devices), 'mon': mon, 'pri': pri,
            'air': air, 'z2m': z2m_age,
            'hash': self._disc_hash,
            'cal': self._compute_cal_hash(),  # v32: calendar hash
            'm': mode_char, 'pm': 1 if self.production_mode else 0,
            'sa': sync_age, 'o': round(self._time_offset, 1),
            'q': q_depth, 'sc': sup_contact,
            'ts': int(time.time()),  # v32: system unix timestamp
        }
        # RSSI
        try:
            if self.mesh and hasattr(self.mesh, 'nodes'):
                for node in self.mesh.nodes.values():
                    r = node.get('rssi', None)
                    if r and r > -999: msg['rssi'] = r; break
        except: pass
        # v33: vs (vswitch states) removed from diag — sent separately via vsw_st
        # Keeps HB/PONG under 220B limit
        return msg

    def _compute_cal_hash(self):
        """v33: Hash of local calendar (sha256, consistent with disc_hash)."""
        slots = self.local_schedule.get_all()
        if not slots: return ""
        raw = json.dumps(sorted([json.dumps(s, sort_keys=True) for s in slots]))
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    def _send_heartbeat(self):
        """v32: HB — uses unified diag payload."""
        msg = self._build_diag_payload('hb')
        self.queue[FlowPriority.SYSTEM].append(msg)
        m = msg.get('m', '?')
        sun_icon = {'D': '☀️', 'N': '🌙', 'F': '⚠️'}.get(m, '?')
        log.info('PING', f"💓 HB: {sun_icon}{m} dev={msg['dev']} mon={msg['mon']} pri={msg['pri']} z2m={msg['z2m']}s air={msg['air']}% q={msg['q']}")

    # =====================================================
    # MAIN LIFECYCLE
    # =====================================================
    def start(self):
        log.info('MAIN', "="*50)
        log.info('MAIN', f"🚀 GATEWAY {CONFIG['id']} v38 (Calendar + Anomaly Dashboard + Anti-Collision)")
        log.info('MAIN', f"   operating_mode={CONFIG.get('operating_mode','ALWAYS')}")
        log.info('MAIN', "="*50)
        self._load_state()
        self.cal_transfer = CalendarTransfer(self._add_queue, CONFIG['calendar']['chunk_size'])

        # v35: Dual batchers — separate queues for priority and monitored devices
        self.batcher_prio = None  # Priority: 2s flush, P1 queue
        self.batcher_mon = None   # Monitored: 30s flush, P2 queue
        if CONFIG['batcher']['enabled']:
            mp = CONFIG['batcher']['max_payload']
            mi = CONFIG['batcher'].get('max_items', 6)
            self.batcher_prio = Batcher(
                gw_id=CONFIG['id'], flush_callback=self._on_prio_flush,
                interval=10, max_payload=mp, max_items=mi, tag="PRIO")
            self.batcher_mon = Batcher(
                gw_id=CONFIG['id'], flush_callback=self._on_mon_flush,
                interval=CONFIG['batcher']['interval'], max_payload=mp, max_items=mi, tag="MON")

        self._setup_mqtt()
        for _ in range(20):
            if self.discovery_done: break
            time.sleep(0.5)

        self._init_vio()

        threading.Thread(target=self._anomaly_loop, daemon=True).start()
        if CONFIG['cmd_retry']['enabled']:
            threading.Thread(target=self._retry_loop, daemon=True).start()
        self._setup_mesh()
        threading.Thread(target=self._mesh_reconnect_loop, daemon=True, name="mesh-reconnect").start()

        if self.batcher_prio:
            self.batcher_prio.start()
            self.batcher_mon.start()

        self._last_heartbeat_ts = now_ts()
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        # v26: Sync watchdog — monitors time sync freshness
        threading.Thread(target=self._sync_watchdog_loop, daemon=True, name="sync-watchdog").start()

        self._register_vio_entities()
        # v30: Register sync time/mode entity for dashboard
        self._register_sync_entity()
        # Publish initial sync state (before first sync from supervisor)
        self._publish_sync_entity()

        mon = len([d for d in self.devices if self._is_monitored(d) and not self._is_priority(d)])
        pri = len([d for d in self.devices if self._is_priority(d)])
        self._compute_disc_hash()
        log.info('MAIN', f"✅ Ready! {len(self.devices)} total, {mon} monitored, {pri} priority")
        log.info('MAIN', f"📋 disc_hash={self._disc_hash}")
        slot_cfg = CONFIG.get('slot', {})
        if slot_cfg.get('enabled'):
            log.info('SLOT', f"⏱️ Slot: idx={slot_cfg['index']}, window={slot_cfg['window']}s")
        self._publish_schedule_status()

        # v35: Startup announce — reload states + HB + discovery + flush
        if self._mesh_connected and self.devices:
            def _startup_announce():
                time.sleep(1)
                # v35: Re-subscribe to get retained MQTT messages NOW that devices are populated
                # (retained messages from initial subscribe were dropped because self.devices was empty)
                self.mqtt.unsubscribe("zigbee2mqtt/+")
                self.mqtt.unsubscribe("zigbee2mqtt/+/availability")
                time.sleep(0.5)
                self.mqtt.subscribe("zigbee2mqtt/+")
                self.mqtt.subscribe("zigbee2mqtt/+/availability")
                log.info('STARTUP', f"🔄 Re-subscribed to Z2M for {len(self.devices)} devices (loading retained states)")
                time.sleep(3)  # Wait for retained messages to arrive
                # Query mains devices (switch/light) via Z2M /get for fresh state
                mains_count = 0
                for dev, info in self.devices.items():
                    if info.get('type') in ['switch', 'light']:
                        self.mqtt.publish(f"zigbee2mqtt/{dev}/get", json.dumps({"state": ""}))
                        mains_count += 1
                if mains_count > 0:
                    log.info('STARTUP', f"📡 Queried {mains_count} mains devices via Z2M /get")
                    time.sleep(2)
                # Log what states we have
                loaded = sum(1 for d in self.devices if self.states.get(d))
                log.info('STARTUP', f"📊 States loaded: {loaded}/{len(self.devices)} devices")
                # Always send HB on startup
                self._send_heartbeat()
                time.sleep(5)  # v37: wait for supervisor to process HB and send disc
                # Discovery if hash changed
                saved_hash = self._saved_disc_hash
                if not saved_hash or saved_hash != self._disc_hash:
                    log.info('DISC', f"🔭📤 Hash changed ({saved_hash or 'none'}→{self._disc_hash}) — sending discovery")
                    self._handle_discovery({"hash": ""})
                    time.sleep(8)  # v37: wait for all disc packets to TX
                else:
                    log.info('DISC', f"🔭 Hash unchanged ({self._disc_hash}) — skip discovery")
                    time.sleep(3)
                # Flush ALL device states with loaded data
                log.info('STARTUP', f"📤 Flushing all states ({loaded} with data)")
                self._flush_all_states()
                # v37: Mark all as reported — prevents _check_periodic_reports burst
                now = time.time()
                for dev in self.devices:
                    if self._is_monitored(dev):
                        self.last_report[dev] = now
            threading.Thread(target=_startup_announce, daemon=True, name="startup-announce").start()
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
            # v30: Gateway cmd topics (request_sync, etc)
            client.subscribe(f"lora/gw/{CONFIG['id']}/cmd/#")
            # v34: Param edits from local HA dashboard
            client.subscribe(f"lora/gw/{CONFIG['id']}/param/+/set")

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
                            self._send_status(dev)
            elif topic.startswith("zigbee2mqtt/") and '/' not in topic[12:]:
                dev = topic.split('/')[1]
                if dev in self.devices:
                    try: self._handle_zigbee_state(dev, json.loads(payload))
                    except: pass
            # v30: Gateway cmd topics (request_sync, etc)
            elif topic.startswith(f"lora/gw/{CONFIG['id']}/cmd/"):
                cmd = topic.split('/')[-1]
                if cmd == 'request_sync':
                    self.queue[FlowPriority.SYSTEM].append({"t": "s_req", "g": CONFIG['id']})
                    log.info('SYNC', f"🔄 Manual sync request from dashboard")
            # v34: Param edit from local HA dashboard
            # Topic: lora/gw/G1/param/p1/set → store + publish + send to supervisor
            elif topic.startswith(f"lora/gw/{CONFIG['id']}/param/") and topic.endswith("/set"):
                parts_p = topic.split('/')
                pid = parts_p[-2]  # p1, p2, p3
                self._handle_local_param_change(pid, payload)
        except Exception as e: log.error('MQTT', f"Error: {e}")

    def _sync_push(self):
        """Push local schedule to supervisor via LoRa compact protocol."""
        slots = self.local_schedule.get_all()
        if not slots: log.warn('SYNC', "Nothing to push"); return
        self._push_schedule_to_supervisor()

    def _sync_pull(self):
        self._add_queue({"t":"cal_req","g":CONFIG['id'],"dir":"pull"}, FlowPriority.SYSTEM)

    def _sync_bidir(self):
        slots = self.local_schedule.get_all()
        if slots: self.cal_transfer.start_send(CONFIG['id'], "bidir", slots)
        else: self._add_queue({"t":"cal_req","g":CONFIG['id'],"dir":"bidir"}, FlowPriority.SYSTEM)

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
        self._last_z2m_ts = now_ts()
        # v35: Track if this is a cmd response confirmation
        was_pending = False
        with self.lock:
            old = self.states.get(dev,{}); self.states[dev] = data; self.last_seen[dev] = now_str()
            if dev in self.cmd_failed: self.cmd_failed[dev] = False
            if dev in self.pending_cmds:
                del self.pending_cmds[dev]
                was_pending = True  # Device confirmed cmd response
        if dtype in ['switch','light'] and data.get('state') != old.get('state'):
            self.last_state_change[dev] = now_ts()
            if self.stagnation_reported.get(dev, False):
                self.stagnation_reported[dev] = False
                if self._is_auto_clear_enabled('stagnation'): self._send_anomaly(dev, "stagnation_clear", None)
            else: self.stagnation_reported[dev] = False
        # v37: Battery + offline anomalies for ALL devices
        self._check_battery_anomaly(dev, data.get('battery'))
        self._check_offline_anomaly(dev)
        if not self._is_monitored(dev): return
        # v37: Value anomalies (temp/hum) only for monitored devices
        self._check_value_anomalies(dev, data)
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
                if is_alarm:
                    self._send_status(dev, force_flush=True)
                elif was_pending:
                    self._send_status(dev, bypass_echo=True)
                    self._schedule_cmd_flush()
                    log.info('CMD', f"✅ {dev} confirmed → status queued")
                else:
                    self._send_status(dev)
            elif was_pending:
                # v36: Device confirmed cmd but state unchanged (e.g. already ON physically)
                # Still send status to sync dashboard
                self._send_status(dev, bypass_echo=True)
                self._schedule_cmd_flush()
                log.info('CMD', f"✅ {dev} confirmed (state unchanged) → sync dashboard")

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

    def _send_status(self, dev, force_flush=False, bypass_echo=False):
        """v34: ALL device data through batcher with dedup.
        Priority: 2s flush window (dedup fast changes, avoid flicker).
        Monitored: 30s batcher interval.
        force_flush=True only for alarm retransmit."""
        info = self.devices.get(dev,{})
        if not info or not self.batcher_mon: return

        is_prio = self._is_priority(dev)

        # Context-aware suppression (priority devices NEVER suppressed)
        if self._should_suppress(dev):
            log.debug('STATUS', f"🔇 Suppressed ({CONFIG.get('operating_mode','?')}/{self._current_context()}): {dev}")
            return

        # Echo filter (bypass for cmd responses and force_flush)
        if not force_flush and not bypass_echo:
            echo_ts = self._echo_suppress.get(dev, 0)
            if echo_ts and (time.time() - echo_ts) < self._echo_window:
                log.debug('CMD', f"🔇 Echo suppressed: {dev}")
                return

        payload = self._build_status_payload(dev, include_ls=False)
        if payload is None: return

        self._track_reported_values(dev, payload)
        sid = self.dev_short_id.get(dev, dev)

        # v35: Route to correct batcher — priority has 2s auto-flush, monitored has 30s
        if is_prio:
            self.batcher_prio.add(sid, payload, force_flush=force_flush)
        else:
            self.batcher_mon.add(sid, payload, force_flush=force_flush)

        prio_tag = "🚨" if is_prio else "📦"
        log.debug('STATUS', f"{prio_tag} {dev}(sid={sid})")

    def _on_prio_flush(self, batch_dict):
        """v35: Priority batcher flush → P1 queue."""
        items = batch_dict.get('d', [])
        self.queue[FlowPriority.PRIORITY].append(batch_dict)
        size = len(json.dumps(batch_dict, separators=(',', ':')))
        log.info('BATCH', f"🚨 [P1:PRIO] Queued ({len(items)} items, ~{size}B)")

    def _on_mon_flush(self, batch_dict):
        """v35: Monitored batcher flush → P2 queue."""
        items = batch_dict.get('d', [])
        self.queue[FlowPriority.MONITORED].append(batch_dict)
        size = len(json.dumps(batch_dict, separators=(',', ':')))
        log.info('BATCH', f"📦 [P3:MON] Queued ({len(items)} items, ~{size}B)")

    def _track_reported_values(self, dev, payload):
        """Record values sent via LoRa for delta comparison on next report."""
        if dev not in self._last_reported_values:
            self._last_reported_values[dev] = {}
        key_map = {'t': 'temperature', 'h': 'humidity', 'b': 'battery', 'r': 'brightness'}
        for short_key, full_key in key_map.items():
            if short_key in payload:
                self._last_reported_values[dev][full_key] = payload[short_key]

    def _execute_command(self, dev, cmd, val):
        """v35: Execute cmd → wait for Z2M confirm or timeout.
        Status sent ONLY when device responds (was_pending in _handle_zigbee_state)
        or on timeout (via _retry_loop → offline status)."""
        self.mqtt.publish(f"zigbee2mqtt/{dev}/set", json.dumps({cmd: val}))
        self._echo_suppress[dev] = time.time()
        info = self.devices.get(dev, {})
        # v35: ALL monitored devices tracked. Switch/light = retry, sensor/binary = single attempt
        retries = CONFIG['cmd_retry']['retries'] if info.get('type') in ['switch', 'light'] else 0
        timeout = CONFIG['cmd_retry']['timeout']
        with self.lock:
            self.pending_cmds[dev] = {'cmd': cmd, 'val': val, 'time': now_ts(), 'retries': 0, 'max': retries}
        log.info('CMD', f"⚡ {dev} → {cmd}={val} (timeout={timeout}s, retry={retries})")

    def _schedule_cmd_flush(self):
        """v35: Schedule both batchers flush 3s from now."""
        with self.lock:
            if self._cmd_flush_pending:
                return
            self._cmd_flush_pending = True
        # Block flush_loop during 3s aggregation window
        if self.batcher_prio: self.batcher_prio.last_flush = time.time()
        if self.batcher_mon: self.batcher_mon.last_flush = time.time()
        def _delayed():
            time.sleep(3)
            with self.lock: self._cmd_flush_pending = False
            if self.batcher_prio: self.batcher_prio.flush()
            if self.batcher_mon: self.batcher_mon.flush()
            log.debug('CMD', f"⚡ Cmd response flush (aggregated)")
        threading.Thread(target=_delayed, daemon=True).start()

    def _retry_loop(self):
        """v35: Check pending commands. Retry or mark offline + send status."""
        while self.running:
            time.sleep(1)
            to = CONFIG['cmd_retry']['timeout']
            now = now_ts()
            with self.lock: checks = list(self.pending_cmds.items())
            for dev, info in checks:
                if now - info['time'] > to:
                    mx = info.get('max', CONFIG['cmd_retry']['retries'])
                    if info['retries'] < mx:
                        with self.lock: self.pending_cmds[dev]['retries'] += 1; self.pending_cmds[dev]['time'] = now
                        self.mqtt.publish(f"zigbee2mqtt/{dev}/set", json.dumps({info['cmd']: info['val']}))
                        log.info('CMD', f"🔄 {dev} retry {info['retries']+1}/{mx}")
                    else:
                        with self.lock: del self.pending_cmds[dev]; self.cmd_failed[dev] = True
                        self._check_offline_anomaly(dev)
                        if self._is_monitored(dev):
                            self._send_status(dev, bypass_echo=True)
                            self._schedule_cmd_flush()
                        log.warn('CMD', f"❌ {dev} timeout after {mx} retries → offline")

    # =====================================================
    # REQ-9: anomaly loop checks ALL known devices (unchanged logic)
    # =====================================================
    def _anomaly_loop(self):
        """v36: Anomaly detection every 60s, flush buffer every 30s.
        Offline + battery: ALL devices. Value anomalies: monitored only."""
        sc = 0; fc = 0
        while self.running:
            time.sleep(30)
            try:
                fc += 1
                if fc % 2 == 0:
                    for dev in list(self.devices.keys()):
                        # v36: offline + battery for ALL devices
                        self._check_offline_anomaly(dev)
                        ds = self.states.get(dev)
                        if ds:
                            self._check_battery_anomaly(dev, ds.get('battery'))
                        # Value anomalies + stagnation only for monitored
                        if self._is_monitored(dev):
                            self._check_stagnation(dev)
                            if ds:
                                self._check_value_anomalies(dev, ds)
                    sc += 1
                    if sc >= 5: self._save_state(); sc = 0
                self._flush_anomaly_buffer()
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
                    self._send_status(dev)  # v34: priority uses 2s dedup window automatically
                    self.last_report[dev] = now
            elif now - last >= interval:
                if self._has_delta_change(dev):
                    self._send_status(dev)
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
                    # v27: Reconnect — flush states only (device list unchanged)
                    def _reconnect_flush():
                        time.sleep(5)
                        log.info('STATUS', f"📤 Reconnect: flushing all states")
                        self._flush_all_states()
                    threading.Thread(target=_reconnect_flush, daemon=True).start()
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
            # v30: Track last supervisor contact — ANY incoming LoRa msg
            self._last_supervisor_contact = time.time()
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
            elif t == 'cal_pull': self._push_schedule_to_supervisor()  # SUP requests our schedule
            elif t == 'cal_mode': self._handle_schedule(data)  # Mode transition notification
            elif t == 'cal_pre': self._handle_schedule_pre_notify(data)  # v6.3 [M1]: pre-notify
            elif t == 'cal_begin': self.cal_transfer.handle_begin(data)
            elif t == 'cal_chunk': self.cal_transfer.handle_chunk(data)
            elif t == 'cal_end':
                result = self.cal_transfer.handle_end(data)
                if result: self._process_calendar_received(*result)
            elif t == 'cal_ack': self.cal_transfer.handle_ack(data)
            elif t == 'vbtn': self._handle_vio_button_lora(data)
            elif t == 'vsw': self._handle_vswitch(data)
            # v26: Time + context sync from supervisor
            elif t == 'sync': self._handle_sync(data)
            elif t == 'params': self._handle_params(data)
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
        """v30: Accept config with optional per-gateway targeting.
        If 'g' present and != our id, ignore. No 'g' = broadcast (accept)."""
        gw = data.get('g')
        if gw and gw != CONFIG['id']: return
        to = data.get('to',{})
        if to:
            CONFIG['timeout']['switch'] = to.get('sw', CONFIG['timeout']['switch'])
            CONFIG['timeout']['light'] = to.get('lt', CONFIG['timeout']['light'])
            CONFIG['timeout']['sensor'] = to.get('sn', CONFIG['timeout']['sensor'])
            CONFIG['timeout']['binary_sensor'] = to.get('bs', CONFIG['timeout']['binary_sensor'])
            tgt = f" (targeted {gw})" if gw else " (broadcast)"
            log.info('CFG', f"⚙️ Timeouts updated{tgt}: sw={CONFIG['timeout']['switch']}s sn={CONFIG['timeout']['sensor']}s")

    def _handle_params(self, data):
        """v34: Params received from supervisor → update local values + HA entities.
        Does NOT echo back to supervisor (prevents loop)."""
        gw = data.get('g')
        if gw and gw != CONFIG['id']: return
        cp = CONFIG.get('custom_params', {})
        if 'runtime_params' not in CONFIG: CONFIG['runtime_params'] = {}
        updated = []
        for pid in ['p1', 'p2', 'p3']:
            if pid in data and cp.get(pid):
                key = cp[pid].get('key', pid)
                val = data[pid]
                CONFIG['runtime_params'][key] = val
                # Publish state to local HA entity (number slider updates)
                self.mqtt.publish(f"lora/gw/{CONFIG['id']}/param/{pid}", str(val), retain=True)
                updated.append(f"{key}={val}")
        if updated:
            log.info('CFG', f"⚙️📦 Params from supervisor: {', '.join(updated)}")

    def _handle_local_param_change(self, pid, payload):
        """v34: Param changed on local gateway HA dashboard → store + send to supervisor."""
        cp = CONFIG.get('custom_params', {})
        pcfg = cp.get(pid)
        if not pcfg: return
        try:
            val = float(payload)
            if val == int(val): val = int(val)
        except: return
        key = pcfg.get('key', pid)
        if 'runtime_params' not in CONFIG: CONFIG['runtime_params'] = {}
        CONFIG['runtime_params'][key] = val
        # Publish state back (HA slider feedback)
        self.mqtt.publish(f"lora/gw/{CONFIG['id']}/param/{pid}", str(val), retain=True)
        # Send to supervisor via LoRa
        msg = {"t": "param_upd", "g": CONFIG['id'], pid: val}
        self.queue[FlowPriority.SYSTEM].append(msg)
        log.info('CFG', f"⚙️📦 Param {key}={val} (local edit → supervisor)")

    def _publish_params(self):
        """v34: Publish current params to per-param MQTT topics."""
        cp = CONFIG.get('custom_params', {})
        rp = CONFIG.get('runtime_params', {})
        for pid in ['p1', 'p2', 'p3']:
            pcfg = cp.get(pid, {})
            key = pcfg.get('key', pid)
            val = rp.get(key, pcfg.get('default', 0))
            self.mqtt.publish(f"lora/gw/{CONFIG['id']}/param/{pid}", str(val), retain=True)

    def _handle_command(self, data):
        dev, cmd, val = data.get('d'), data.get('c','state'), data.get('v')
        if dev in self.devices: self._execute_command(dev, cmd, val)

    def _handle_request(self, data):
        """v27: Active Z2M /get query for fresh data.
        Publishes to zigbee2mqtt/{dev}/get → Z2M responds with cached state
        → _handle_zigbee_state updates self.states → 2s later flush via batcher."""
        dev = data.get('d')
        if dev not in self.devices or not self._is_monitored(dev):
            return
        info = self.devices.get(dev, {})
        caps = info.get('caps', [])
        # Build Z2M get payload — request all known capabilities
        get_payload = {cap: "" for cap in caps}
        if get_payload:
            self.mqtt.publish(f"zigbee2mqtt/{dev}/get", json.dumps(get_payload))
            log.info('CMD', f"🔄 Refresh: {dev} → Z2M /get {list(get_payload.keys())}")
        # Wait for Z2M cached response → flush
        def _delayed_flush():
            time.sleep(2)  # Z2M responds in ~100ms, 2s is safe margin
            self._send_status(dev, force_flush=True)
        threading.Thread(target=_delayed_flush, daemon=True).start()

    # =====================================================
    # v25: STATE RECOVERY — triggers full discovery+state
    # =====================================================
    def _handle_req_states(self):
        """v27: State recovery — flush all states through batcher (no discovery)."""
        log.info('STATUS', f"📤 req_st → flushing all states")
        self._flush_all_states()

    def _flush_all_states(self):
        """v35: Add ALL monitored device states to correct batcher → flush.
        Used after startup, reconnect, and req_st."""
        if not self.batcher_mon: return
        pc, mc = 0, 0
        for dev in self.devices:
            if not self._is_monitored(dev): continue
            payload = self._build_status_payload(dev, include_ls=False)
            if payload is None: continue
            sid = self.dev_short_id.get(dev, dev)
            if self._is_priority(dev):
                self.batcher_prio.add(sid, payload); pc += 1
            else:
                self.batcher_mon.add(sid, payload); mc += 1
        if pc > 0: self.batcher_prio.flush()
        if mc > 0: self.batcher_mon.flush()
        log.info('STATUS', f"📤 Flushed states: {pc} priority + {mc} monitored")

    def _handle_ping(self):
        """v32: PONG — identical payload to HB, only t='pong' differs."""
        msg = self._build_diag_payload('pong')
        self.queue[FlowPriority.SYSTEM].append(msg)
        log.info('PING', f"🏓 PONG: dev={msg['dev']} mon={msg['mon']} pri={msg['pri']} air={msg['air']}% rssi={msg.get('rssi','--')}")

    # =====================================================
    # v27: CAPS ENCODING — compact capability strings
    # =====================================================
    _CAPS_MAP = {'temperature':'t','humidity':'h','battery':'b','state':'s',
                 'brightness':'r','contact':'c','occupancy':'o','water_leak':'w','smoke':'k'}
    _CAPS_REV = {v: k for k, v in _CAPS_MAP.items()}
    _TYPE_MAP = {'sensor':'S','switch':'W','light':'L','binary_sensor':'B'}
    _TYPE_REV = {v: k for k, v in _TYPE_MAP.items()}

    def _encode_caps(self, caps):
        """['temperature','humidity','battery'] → 'thb'"""
        return ''.join(self._CAPS_MAP.get(c, c[0]) for c in caps)

    def _handle_discovery(self, data):
        """v27: Compact discovery — lightweight registration only.
        1. Hash match → disc_ack (1 packet)
        2. Hash differs → disc_meta + disc_vio + compact db (NO state)
        3. State goes through batcher after discovery (separate flush)
        Format: [sid, "name", "S", "thb"]  (~25B per device vs ~100B old)
        """
        req_hash = data.get('hash')
        if req_hash and req_hash == self._disc_hash:
            self.queue[FlowPriority.SYSTEM].append({
                't': 'disc_ack', 'g': CONFIG['id'], 'hash': self._disc_hash,
                'n': len([d for d in self.devices if self._is_monitored(d)])
            })
            log.info('DISC', f"🔭 Hash match ({self._disc_hash}) — skip discovery")
            return

        # Phase 1a: disc_meta
        meta = {'t': 'disc_meta', 'g': CONFIG['id'], 'hash': self._disc_hash,
                'dev_n': len([d for d in self.devices if self._is_monitored(d)])}
        self.queue[FlowPriority.SYSTEM].append(meta)

        # Phase 1b: disc_vio
        vio_list = CONFIG.get('virtual_io', [])
        if vio_list:
            vio_compact = []
            for vs in vio_list:
                entry = [vs['id'], vs['type'][0], vs.get('name', vs['id'])]
                if vs['type'] == 'switch':
                    entry.append(self.vswitch_states.get(vs['id'], vs.get('default', 0)))
                vio_compact.append(entry)
            vio_pkts = batch_split("disc_vio", CONFIG['id'], vio_compact, CONFIG['lora']['max_size'])
            for pkt in vio_pkts:
                self.queue[FlowPriority.SYSTEM].append(pkt)

        # Phase 2: compact db — NO inline state
        items = []
        for dev, info in self.devices.items():
            if not self._is_monitored(dev): continue
            sid = self.dev_short_id.get(dev, -1)
            type_char = self._TYPE_MAP.get(info['type'], '?')
            caps_str = self._encode_caps(info.get('caps', []))
            items.append([sid, dev, type_char, caps_str])
        packets = batch_split("db", CONFIG['id'], items, CONFIG['lora']['max_size'])
        for pkt in packets:
            self.queue[FlowPriority.SYSTEM].append(pkt)
        log.info('DISC', f"🔭 Compact discovery: meta + {len(vio_list)} vio + {len(items)} dev → {len(packets)} db pkt(s)")

    def _send_lora(self, msg, priority=None):
        """v35: Returns True on success or permanent drop. False only on TX error (retriable)."""
        if not self._mesh_connected or not self.mesh:
            log.warn('LORA', "⚠️ No antenna"); return False
        # Airtime guard check
        now = time.time()
        ag = CONFIG.get('airtime_guard', {})
        if ag.get('enabled', True) and now < self._airtime_blocked_until:
            remaining = int(self._airtime_blocked_until - now)
            log.warn('LORA', f"🛑 Airtime blocked for {remaining}s"); return False
        try:
            s = json.dumps(msg, separators=(',',':'))
            max_sz = CONFIG['lora']['max_size']
            if len(s) > max_sz:
                log.error('LORA', f"❌ Packet too big ({len(s)}B > {max_sz}B) — PERMANENT DROP")
                return True  # v35: permanent drop — do NOT requeue
            plabel = PRIORITY_LABELS.get(priority, "[P?]") if priority is not None else ""
            items_n = len(msg.get('d', [])) if isinstance(msg.get('d'), list) else 0
            batch_info = f" ({items_n} items)" if items_n > 0 and msg.get('t') in ('b', 'ab') else ""
            log.info('LORA', f"📤 TX {plabel}{batch_info} ({len(s)}B) | {self._slot_debug_str()} | {s[:100]}")
            self.mesh.sendText(s)
            self.last_tx = time.time() + random.uniform(0, 0.5)
            self._airtime_tx_count += 1
            self._airtime_tx_bytes += len(s)
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
    # v25: MAIN LOOP — 3-priority slot-aware dispatch
    # =====================================================
    def _loop(self):
        self._dedup_queues()
        now = time.time()
        if now - self.last_tx < CONFIG['lora']['tx_cooldown']:
            return
        # v30: Early exit — don't dequeue if TX is impossible
        if not self._mesh_connected or not self.mesh:
            return  # reconnect_loop handles recovery
        ag = CONFIG.get('airtime_guard', {})
        if ag.get('enabled', True) and now < self._airtime_blocked_until:
            return  # wait for airtime block to expire
        if not self._is_my_slot():
            return
        for p in sorted(FlowPriority):
            if self.queue[p]:
                msg = self.queue[p].popleft()
                if not self._is_my_slot():
                    self.queue[p].appendleft(msg)
                    if p in (FlowPriority.SYSTEM, FlowPriority.PRIORITY, FlowPriority.ANOMALY):
                        log.warn('SLOT', f"⏱️ {PRIORITY_LABELS[p]} Slot expired — requeued")
                    return
                success = self._send_lora(msg, priority=p)
                if not success and p in (FlowPriority.SYSTEM, FlowPriority.PRIORITY, FlowPriority.ANOMALY):
                    self.queue[p].appendleft(msg)
                    log.warn('LORA', f"📻 {PRIORITY_LABELS[p]} TX failed — requeued")
                return
        self._check_periodic_reports()

    def _dedup_queues(self):
        """v25: Dedup P2 (MONITORED) — keep only latest batch."""
        p = FlowPriority.MONITORED
        if not self.queue[p] or len(self.queue[p]) <= 1: return
        items = list(self.queue[p])
        self.queue[p] = deque([items[-1]], maxlen=200)
        if len(items) > 1:
            log.debug('BATCH', f"📦 Dedup: kept 1/{len(items)} batches")

    def _cleanup(self):
        self.running = False
        if self.batcher_prio:
            self.batcher_prio.running = False; self.batcher_prio.flush()
        if self.batcher_mon:
            self.batcher_mon.running = False; self.batcher_mon.flush()
        self._save_state()
        if self.mqtt: self.mqtt.loop_stop(); self.mqtt.disconnect()
        if self.mesh:
            try: self.mesh.close()
            except: pass
        log.info('MAIN', "Stopped")
        log.close()

if __name__ == "__main__":
    Gateway().start()
