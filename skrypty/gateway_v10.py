#!/usr/bin/env python3
"""
LoRa Zigbee Gateway - v4.1
NEW v4.1:
- Calendar: local schedule + chunked LoRa sync (push/pull/bidir)
- LoRa hot-reconnect: script survives antenna unplug/replug
- Virtual buttons: supervisor sends button press, gateway creates MQTT entity
- Anomaly CONFIG lives HERE (gateway) — thresholds, detection enable/disable
- Priority devices = monitored subset with ALARM priority + bypass slot
- dump_anom = LIVE re-check (not from memory) — REQ-8
- Offline/stagnation check for ALL known devices, including no-contact — REQ-9
- State persistence across restarts
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import json, time, random, threading, os, zlib, base64, hashlib, string
from datetime import datetime
from collections import deque
from enum import IntEnum
import paho.mqtt.client as mqtt
import meshtastic, meshtastic.serial_interface
from pubsub import pub

PERSIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gw_state.json')
SCHEDULE_FILE = os.environ.get('LORA_SCHEDULE_FILE', '/opt/lora_gateway/schedule.json')

CONFIG = {
    "id": "G1",
    "mesh_port": "/dev/ttyUSB1",
    "mesh_reconnect": {"enabled": True, "interval": 10, "max_backoff": 120},
    "mqtt": {"host": "172.17.0.1", "port": 1883, "user": "mqtt", "pass": "REPLACE_ME"},
    "lora": {"max_size": 220, "tx_cooldown": 2.5},
    "anti_collision": {"enabled": False, "slot_index": 0, "slot_count": 3},
    "report_intervals": {"switch": 300, "light": 300, "sensor": 300, "binary_sensor": 300},
    "report_mode": {"switch": "event", "light": "event", "sensor": "cyclic", "binary_sensor": "event"},
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},
    "monitored": ["Test 1", "Test 2", "Temp 1", "Door 1", "Leak 1", "Light Sensor"],
    # Priority = subset of monitored: ALARM priority, bypass slot, instant reporting
    "priority_devices": ["Leak 1"],
    "cmd_retry": {"enabled": True, "timeout": 5, "retries": 1},
    # REQ-7: ALL anomaly detection config is HERE (gateway only)
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
    "calendar": {"chunk_size": 140, "transfer_timeout": 60, "retry_max": 2}
}

class Priority(IntEnum):
    ALARM = 0; COMMAND = 1; RESPONSE = 2; STATUS = 3; DISCOVERY = 4

class Logger:
    ICONS = {'DEBUG': '🔍', 'INFO': '✅', 'WARN': '⚠️ ', 'ERROR': '❌'}
    COLORS = {'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARN': '\033[33m', 'ERROR': '\033[31m'}
    RESET = '\033[0m'
    COMP = {'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'ZIGBEE': '📶', 'PING': '🏓',
            'DISC': '🔭', 'CMD': '⚡', 'STATUS': '📤', 'CFG': '⚙️', 'SLOT': '⏱️',
            'OFFLINE': '💀', 'ANOMALY': '🚨', 'RETRY': '🔄', 'CAL': '📆', 'SYNC': '🔄', 'VBTN': '🔘'}
    def __init__(self):
        self.file = open('/tmp/gateway.log', 'a')
    def _log(self, lvl, comp, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"{ts} {self.ICONS.get(lvl,'')} {self.COLORS.get(lvl,'')}[{comp}]{self.RESET} {self.COMP.get(comp,'•')} {msg}")
        self.file.write(f"{ts} [{lvl}] [{comp}] {msg}\n"); self.file.flush()
    def debug(self, c, m): self._log('DEBUG', c, m)
    def info(self, c, m): self._log('INFO', c, m)
    def warn(self, c, m): self._log('WARN', c, m)
    def error(self, c, m): self._log('ERROR', c, m)

log = Logger()
now_str = lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S')
now_ts = lambda: time.time()
def parse_datetime(s):
    try: return datetime.strptime(s, '%Y-%m-%d %H:%M:%S').timestamp()
    except: return 0


# =====================================================
# LOCAL SCHEDULE MANAGER
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

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock: self.outgoing[tid] = {'chunks':chunks,'gw':gw,'dir':direction,'retries':0,'ts':now_ts()}
        self.queue_fn({"t":"sch_b","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, Priority.COMMAND)
        def _s():
            for i, c in enumerate(chunks):
                time.sleep(CONFIG['lora']['tx_cooldown']+0.5)
                self.queue_fn({"t":"sch_c","tid":tid,"s":i,"d":c}, Priority.COMMAND)
            time.sleep(CONFIG['lora']['tx_cooldown']+0.5)
            self.queue_fn({"t":"sch_e","tid":tid}, Priority.COMMAND)
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
            if tid in self.incoming: self.incoming[tid]['chunks'][data.get('s',0)] = data.get('d','')

    def handle_end(self, data):
        tid = data.get('tid')
        with self.lock: info = self.incoming.pop(tid, None)
        if not info: return None
        missing = [i for i in range(info['total']) if i not in info['chunks']]
        if missing:
            self.queue_fn({"t":"sch_a","tid":tid,"ok":0,"miss":missing[:5]}, Priority.COMMAND); return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            self.queue_fn({"t":"sch_a","tid":tid,"ok":0,"miss":[]}, Priority.COMMAND); return None
        self.queue_fn({"t":"sch_a","tid":tid,"ok":1,"miss":[]}, Priority.COMMAND)
        try: return (info['gw'], info['dir'], self.deserialize(b64))
        except: return None

    def handle_ack(self, data):
        tid = data.get('tid'); ok = data.get('ok',0); miss = data.get('miss',[])
        with self.lock: info = self.outgoing.pop(tid, None)
        if not info: return
        if ok: log.info('SYNC', f"✅ ACK tid={tid}")
        elif miss and info['retries'] < CONFIG['calendar']['retry_max']:
            info['retries'] += 1
            with self.lock: self.outgoing[tid] = info
            def _r():
                for s in miss:
                    if s < len(info['chunks']):
                        time.sleep(CONFIG['lora']['tx_cooldown'])
                        self.queue_fn({"t":"sch_c","tid":tid,"s":s,"d":info['chunks'][s]}, Priority.COMMAND)
                time.sleep(CONFIG['lora']['tx_cooldown'])
                self.queue_fn({"t":"sch_e","tid":tid}, Priority.COMMAND)
            threading.Thread(target=_r, daemon=True).start()

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
        # Virtual buttons registered from supervisor
        self.vbtn_registered = set()
        # LoRa reconnect
        self._mesh_connected = False
        self._mesh_reconnect_backoff = 0

        self.queue = {p: deque() for p in Priority}
        self.last_tx = 0
        self.start_time = time.time()
        self.running = True
        self.lock = threading.Lock()
        self.discovery_done = False

    def _is_priority(self, dev): return dev in CONFIG.get('priority_devices', [])
    def _is_monitored(self, dev):
        m = CONFIG.get('monitored', [])
        return not m or dev in m
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly',{}).get('auto_clear',{}).get(atype, True)

    # =====================================================
    # STATE PERSISTENCE
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
                    'production_source':self.production_source,'saved_at':now_str()}
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
            log.info('MAIN', f"♻️ Loaded state: {len(self.last_seen)} devices")
        except FileNotFoundError: log.info('MAIN', "No saved state")
        except Exception as e: log.warn('MAIN', f"Load state: {e}")

    def _is_my_slot(self):
        if not CONFIG['anti_collision']['enabled']: return True
        sec = datetime.now().second
        ss = 60 // CONFIG['anti_collision']['slot_count']
        return CONFIG['anti_collision']['slot_index'] * ss <= sec < CONFIG['anti_collision']['slot_index'] * ss + ss - 2

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
    # ANOMALY DETECTION — ALL CONFIG HERE (REQ-7)
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
        msg = {"t":"an","g":CONFIG['id'],"d":dev,"a":atype,"ts":int(now_ts())}
        if value is not None: msg["v"] = value
        self.queue[Priority.ALARM].append(msg)
        log.warn('ANOMALY', f"🚨 TX: {dev} {atype}" + (f"={value}" if value is not None else ""))
        self._retransmit_critical(dev, atype, msg)

    def _retransmit_critical(self, dev, atype, msg):
        ca = CONFIG.get('critical_alarm',{})
        if not ca.get('enabled') or atype not in ca.get('types',[]): return
        ck = (dev, atype)
        if now_ts() - self.critical_alarm_cooldown.get(ck,0) < ca.get('cooldown',10): return
        self.critical_alarm_cooldown[ck] = now_ts()
        def _do():
            for i in range(1, ca.get('repeats',3)):
                time.sleep(ca.get('repeat_delay',2.0)); self.queue[Priority.ALARM].append(dict(msg))
        threading.Thread(target=_do, daemon=True).start()

    # =====================================================
    # REQ-8: dump_anom = LIVE re-check (not from memory)
    # =====================================================
    def _handle_dump_anom(self):
        log.info('ANOMALY', f"📋 Dump — LIVE re-check all monitored devices")
        def _do():
            det = CONFIG['anomaly']['detection']
            for dev in self.devices:
                if not self._is_monitored(dev): continue
                ds = self.states.get(dev, {})
                # Re-check battery LIVE
                bat = ds.get('battery')
                if bat is not None:
                    cc, lc = det.get('critical_battery',{}), det.get('low_battery',{})
                    if cc.get('enabled',True) and bat < cc.get('threshold',15):
                        self.battery_state[dev] = "CRITICAL"
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"critical_battery","v":bat,"ts":int(now_ts())}); time.sleep(0.25)
                    elif lc.get('enabled',True) and bat < lc.get('threshold',25):
                        self.battery_state[dev] = "LOW"
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"low_battery","v":bat,"ts":int(now_ts())}); time.sleep(0.25)
                # Re-check offline LIVE
                oc = det.get('device_offline',{})
                if oc.get('enabled',True) and not self._calc_availability(dev):
                    self.offline_reported[dev] = True
                    self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"device_offline","ts":int(now_ts())}); time.sleep(0.25)
                # Re-check stagnation LIVE
                sc = det.get('stagnation',{})
                if sc.get('enabled',True) and self.devices.get(dev,{}).get('type') in ['switch','light']:
                    h = (now_ts() - self.last_state_change.get(dev, self.start_time)) / 3600
                    if h >= sc.get('hours',48):
                        self.stagnation_reported[dev] = True
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"stagnation","v":int(h),"ts":int(now_ts())}); time.sleep(0.25)
                # Re-check temp LIVE
                temp = ds.get('temperature')
                if temp is not None:
                    th, tl = det.get('temp_high',{}), det.get('temp_low',{})
                    if th.get('enabled',True) and temp >= th.get('threshold',50):
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"temp_high","v":temp,"ts":int(now_ts())}); time.sleep(0.25)
                    elif tl.get('enabled',True) and temp <= tl.get('threshold',-10):
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"temp_low","v":temp,"ts":int(now_ts())}); time.sleep(0.25)
                # Re-check humidity LIVE
                hum = ds.get('humidity')
                if hum is not None:
                    hh, hl = det.get('hum_high',{}), det.get('hum_low',{})
                    if hh.get('enabled',True) and hum >= hh.get('threshold',95):
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"hum_high","v":hum,"ts":int(now_ts())}); time.sleep(0.25)
                    elif hl.get('enabled',True) and hum <= hl.get('threshold',5):
                        self.queue[Priority.ALARM].append({"t":"an","g":CONFIG['id'],"d":dev,"a":"hum_low","v":hum,"ts":int(now_ts())}); time.sleep(0.25)
            log.info('ANOMALY', "📋 Dump complete (LIVE)")
        threading.Thread(target=_do, daemon=True).start()

    # =====================================================
    # SCHEDULE + CALENDAR
    # =====================================================
    def _handle_schedule(self, data):
        msg_ts = data.get('from_ts', 0)
        if msg_ts < self.production_last_update_ts: return
        self.production_mode = bool(data.get('mode', 0))
        self.production_next_change_ts = data.get('next_ts', 0)
        self.production_source = data.get('src', 'supervisor')
        self.production_last_update_ts = msg_ts
        log.info('CFG', f"📅 Schedule: mode={self.production_mode} next={self.production_next_change_ts}")
        self._publish_schedule_status()

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
    # REQ-10: VIRTUAL BUTTON — create local MQTT entity
    # =====================================================
    def _handle_virtual_button(self, data):
        """Supervisor sent vbtn — create/fire MQTT event entity"""
        btn = data.get('btn', 'button1')
        action = data.get('act', 'single')
        safe = btn.replace(' ','_').lower()
        log.info('VBTN', f"🔘 Received: {btn} → {action}")

        # Register MQTT discovery entity if not yet
        if safe not in self.vbtn_registered:
            self.vbtn_registered.add(safe)
            p = "homeassistant"
            # Create event entity for automations
            self.mqtt.publish(f"{p}/event/lora_vbtn_{safe}/config", json.dumps({
                "name": f"LoRa VBtn {btn}",
                "object_id": f"lora_vbtn_{safe}",
                "unique_id": f"lora_vbtn_{safe}",
                "state_topic": f"lora/vbtn/{safe}/event",
                "event_types": ["single", "double", "long"],
                "device": {"identifiers": [f"lora_vbtn_{CONFIG['id']}"],
                           "name": f"LoRa Virtual Buttons {CONFIG['id']}",
                           "model": "Virtual Button", "manufacturer": "LoRa Supervisor"}
            }), retain=True)
            log.info('VBTN', f"📋 Registered entity: event.lora_vbtn_{safe}")

        # Fire event
        self.mqtt.publish(f"lora/vbtn/{safe}/event", json.dumps({
            "event_type": action,
            "btn": btn,
            "ts": int(now_ts())
        }))
        log.info('VBTN', f"🔔 Fired: {btn} = {action}")

    # =====================================================
    # MAIN LIFECYCLE
    # =====================================================
    def start(self):
        log.info('MAIN', "="*50); log.info('MAIN', f"🚀 GATEWAY {CONFIG['id']} v4.1"); log.info('MAIN', "="*50)
        self._load_state()
        self.cal_transfer = CalendarTransfer(self._add_queue, CONFIG['calendar']['chunk_size'])
        self._setup_mqtt()
        for _ in range(20):
            if self.discovery_done: break
            time.sleep(0.5)
        threading.Thread(target=self._anomaly_loop, daemon=True).start()
        if CONFIG['cmd_retry']['enabled']:
            threading.Thread(target=self._retry_loop, daemon=True).start()
        self._setup_mesh()
        # REQ-4: start reconnect loop
        threading.Thread(target=self._mesh_reconnect_loop, daemon=True, name="mesh-reconnect").start()
        mon = len([d for d in self.devices if self._is_monitored(d)])
        pri = len([d for d in self.devices if self._is_priority(d)])
        log.info('MAIN', f"✅ Ready! {len(self.devices)} devices, {mon} monitored, {pri} priority")
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
                            self._send_status(dev, Priority.ALARM if self._is_priority(dev) else Priority.STATUS)
            elif topic.startswith("zigbee2mqtt/") and '/' not in topic[12:]:
                dev = topic.split('/')[1]
                if dev in self.devices:
                    try: self._handle_zigbee_state(dev, json.loads(payload))
                    except: pass
        except Exception as e: log.error('MQTT', f"Error: {e}")

    def _sync_push(self):
        slots = self.local_schedule.get_all()
        if not slots: log.warn('SYNC', "Nothing to push"); return
        self.cal_transfer.start_send(CONFIG['id'], "push", slots)

    def _sync_pull(self):
        self._add_queue({"t":"sch_req","g":CONFIG['id'],"dir":"pull"}, Priority.COMMAND)

    def _sync_bidir(self):
        slots = self.local_schedule.get_all()
        if slots: self.cal_transfer.start_send(CONFIG['id'], "bidir", slots)
        else: self._add_queue({"t":"sch_req","g":CONFIG['id'],"dir":"bidir"}, Priority.COMMAND)

    def _process_z2m_discovery(self, payload):
        try:
            for dev in json.loads(payload):
                name = dev.get('friendly_name')
                if not name or name == 'Coordinator': continue
                self.devices[name] = self._analyze_device(dev)
                if name not in self.z2m_av: self.z2m_av[name] = True
                if name not in self.battery_state: self.battery_state[name] = "OK"
                if name not in self.offline_reported: self.offline_reported[name] = False
                if name not in self.stagnation_reported: self.stagnation_reported[name] = False
            self.discovery_done = True
            log.info('ZIGBEE', f"Found {len(self.devices)} devices")
        except Exception as e: log.error('ZIGBEE', f"Discovery: {e}")

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
                if 'battery' in data and data.get('battery') != old.get('battery'): changed = True
            elif dtype == 'sensor':
                for k in ['temperature','humidity']:
                    if k in data and k in old:
                        if abs(float(data.get(k,0))-float(old.get(k,0))) >= (0.5 if k=='temperature' else 2): changed = True
                if 'battery' in data and data.get('battery') != old.get('battery'): changed = True
            if changed:
                if self._is_priority(dev): self._send_status(dev, Priority.ALARM, bypass_slot=True)
                else: self._send_status(dev, Priority.ALARM if is_alarm else Priority.STATUS, is_alarm)

    def _send_status(self, dev, priority=Priority.STATUS, bypass_slot=False):
        info = self.devices.get(dev,{})
        if not info: return
        with self.lock: state = self.states.get(dev,{}); ls = self.last_seen.get(dev, now_str())
        av = self._calc_availability(dev); dtype = info.get('type','sensor')
        msg = {'t':'st','g':CONFIG['id'],'d':dev,'ls':ls,'av':av}
        if dtype in ['switch','light']:
            msg['st'] = state.get('state','OFF')
            if 'brightness' in state: msg['bri'] = state['brightness']
        elif dtype == 'sensor':
            if av:
                if 'temperature' in state: msg['tmp'] = round(state['temperature'],1)
                if 'humidity' in state: msg['hum'] = int(state['humidity'])
            # REQ-6: if not available, don't send temp/hum (supervisor will show "--")
            if 'battery' in state: msg['bat'] = state['battery']
        elif dtype == 'binary_sensor':
            for k,s in [('contact','con'),('occupancy','occ'),('water_leak','wtr'),('smoke','smk')]:
                if k in state: msg[s] = state[k]
            if 'battery' in state: msg['bat'] = state['battery']
        if self._is_priority(dev): priority = Priority.ALARM; bypass_slot = True
        if priority == Priority.ALARM: time.sleep(random.uniform(0, 0.3))
        elif bypass_slot and CONFIG['anti_collision']['enabled']: time.sleep(random.uniform(0.1, 0.5))
        self.queue[priority].append(msg)

    def _execute_command(self, dev, cmd, val):
        self.mqtt.publish(f"zigbee2mqtt/{dev}/set", json.dumps({cmd: val}))
        info = self.devices.get(dev,{})
        if info.get('mains_powered', False) and CONFIG['cmd_retry']['enabled']:
            with self.lock: self.pending_cmds[dev] = {'cmd':cmd,'val':val,'time':now_ts(),'retries':0}
        def _d():
            time.sleep(1)
            if self._is_monitored(dev):
                self._send_status(dev, Priority.ALARM if self._is_priority(dev) else Priority.RESPONSE,
                                  bypass_slot=self._is_priority(dev))
        threading.Thread(target=_d, daemon=True).start()

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
                        if self._is_monitored(dev): self._send_status(dev, Priority.STATUS, True)

    # =====================================================
    # REQ-9: anomaly loop checks ALL known devices
    # including those gateway knows but has no contact with
    # =====================================================
    def _anomaly_loop(self):
        sc = 0
        while self.running:
            time.sleep(CONFIG['anomaly']['check_interval'])
            try:
                # Check ALL known devices — REQ-9
                for dev in list(self.devices.keys()):
                    self._check_stagnation(dev)
                    self._check_offline_anomaly(dev)
                sc += 1
                if sc >= 5: self._save_state(); sc = 0
            except Exception as e: log.error('ANOMALY', f"Loop error: {e}")

    # =====================================================
    # LoRa: SETUP + HOT RECONNECT (REQ-4)
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
        """REQ-4: Background thread — reconnect antenna on unplug/replug"""
        while self.running:
            time.sleep(5)
            if not CONFIG['mesh_reconnect'].get('enabled', True): continue
            if self._mesh_connected:
                # Health check
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
                # Try reconnect
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
            if not target and CONFIG['anti_collision']['enabled']:
                delay = CONFIG['anti_collision']['slot_index'] * 2
                if delay > 0: time.sleep(delay)
            if t == 'cfg': self._handle_config(data)
            elif t == 'cmd': self._handle_command(data)
            elif t == 'req': self._handle_request(data)
            elif t == 'ping': self._handle_ping()
            elif t == 'disc': self._handle_discovery()
            elif t == 'an_clr': self._handle_anomaly_clear(data)
            elif t == 'dump_anom': self._handle_dump_anom()
            elif t == 'sch': self._handle_schedule(data)
            elif t == 'sch_b': self.cal_transfer.handle_begin(data)
            elif t == 'sch_c': self.cal_transfer.handle_chunk(data)
            elif t == 'sch_e':
                result = self.cal_transfer.handle_end(data)
                if result: self._process_calendar_received(*result)
            elif t == 'sch_a': self.cal_transfer.handle_ack(data)
            elif t == 'sch_req': self._handle_calendar_request(data)
            elif t == 'vbtn': self._handle_virtual_button(data)
        except Exception as e: log.error('LORA', f"Error: {e}")

    def _process_calendar_received(self, gw, direction, slots):
        if direction == 'pull': self.local_schedule.replace_all(slots)
        elif direction == 'bidir': self.local_schedule.replace_all(slots)
        elif direction == 'push': self.local_schedule.merge_incoming(slots)
        self._publish_schedule_status()

    def _handle_calendar_request(self, data):
        direction = data.get('dir', 'push')
        slots = self.local_schedule.get_all()
        self.cal_transfer.start_send(CONFIG['id'], direction, slots)

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
            self._send_status(dev, Priority.ALARM if self._is_priority(dev) else Priority.RESPONSE,
                              bypass_slot=self._is_priority(dev))

    def _handle_ping(self):
        mon = len([d for d in self.devices if self._is_monitored(d)])
        self.queue[Priority.COMMAND].append({'t':'pong','g':CONFIG['id'],'up':int(time.time()-self.start_time),
                                             'dev_total':len(self.devices),'dev_mon':mon})

    def _handle_discovery(self):
        for dev, info in self.devices.items():
            if not self._is_monitored(dev): continue
            self.queue[Priority.DISCOVERY].append({'t':'disc_resp','g':CONFIG['id'],'d':dev,
                                                   'dt':info['type'],'cap':info['caps'],'ls':self.last_seen.get(dev,'unknown')})

    def _send_lora(self, msg):
        if not self._mesh_connected or not self.mesh:
            log.warn('LORA', "⚠️ No antenna — queuing"); return False
        try:
            s = json.dumps(msg, separators=(',',':'))
            log.info('LORA', f"📤 TX ({len(s)}B): {s[:120]}")
            self.mesh.sendText(s); self.last_tx = time.time(); return True
        except Exception as e:
            log.error('LORA', f"TX error: {e}")
            self._mesh_connected = False; return False

    def _add_queue(self, msg, priority): self.queue[priority].append(msg)

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
                sd = CONFIG['anti_collision']['slot_index'] * 5 if CONFIG['anti_collision']['enabled'] else 0
                if now - self.start_time > sd + 10:
                    self._send_status(dev, Priority.ALARM if self._is_priority(dev) else Priority.STATUS)
                    self.last_report[dev] = now
            elif now - last >= interval:
                self._send_status(dev, Priority.ALARM if self._is_priority(dev) else Priority.STATUS)
                self.last_report[dev] = now

    def _loop(self):
        now = time.time()
        if now - self.last_tx >= CONFIG['lora']['tx_cooldown']:
            for p in sorted(Priority):
                if self.queue[p]:
                    msg = self.queue[p].popleft()
                    if p in [Priority.ALARM, Priority.COMMAND, Priority.RESPONSE] or self._is_my_slot():
                        self._send_lora(msg); return
                    else: self.queue[p].appendleft(msg); return
        if self._is_my_slot(): self._check_periodic_reports()

    def _cleanup(self):
        self.running = False; self._save_state()
        if self.mqtt: self.mqtt.loop_stop(); self.mqtt.disconnect()
        if self.mesh:
            try: self.mesh.close()
            except: pass
        log.info('MAIN', "Stopped")

if __name__ == "__main__":
    Gateway().start()
