#!/usr/bin/env python3
"""
LoRa Zigbee Supervisor - v4.1
NEW v4.1:
- Multi-antenna: up to 3 LoRa/Meshtastic radios with enable/disable + hot-reconnect
- Calendar system with chunked LoRa sync (push/pull/bidir)
- Per-gateway + global schedules with merge
- Ghost anomaly prune (devices removed from Z2M)
- Gateway offline → ALL devices become offline; on reconnect re-check
- Sensor offline → "--" for temp/hum on dashboard
- Virtual buttons: supervisor → gateway via LoRa (single/double/long)
- Anomaly CONFIG on gateway only; supervisor just stores/displays
- device_states keyed by "GW:dev"
- Dump-based stale anomaly cleanup + orphan detection
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import json, time, threading, os, zlib, base64, hashlib, random, string, traceback
from datetime import datetime
from collections import deque
from enum import IntEnum
import paho.mqtt.client as mqtt
import meshtastic, meshtastic.serial_interface
from pubsub import pub

SCHEDULE_FILE = os.environ.get('LORA_SCHEDULE_FILE', '/opt/lora_supervisor/schedules.json')

CONFIG = {
    "id": "G0",
    "mesh_ports": [
        {"port": "/dev/ttyUSB0", "enabled": True,  "label": "ANT-1"},
        {"port": "/dev/ttyUSB1", "enabled": False, "label": "ANT-2"},
        {"port": "/dev/ttyACM0", "enabled": False, "label": "ANT-3"},
    ],
    "mesh_reconnect": {"enabled": True, "interval": 15, "max_backoff": 120},
    "mqtt": {"host": "localhost", "port": 1883, "user": "mqtt", "pass": "REPLACE_ME"},
    "ha_prefix": "homeassistant",
    "state_prefix": "lora",
    "gateways": ["G1", "G2", "G3"],
    "lora": {"max_size": 220, "tx_cooldown": 2.5},
    "timeout": {"switch": 1800, "light": 1800, "sensor": 86400, "binary_sensor": 7200},
    "gateway_timeout": 300,
    "production_schedule": {"enabled": True, "pre_notify_seconds": [30, 5]},

    "anomaly": {
        "auto_clear": {
            "device_offline": True, "low_battery": True, "critical_battery": True,
            "temp_high": True, "temp_low": True, "hum_high": True, "hum_low": True,
            "stagnation": True
        }
    },
    
    "calendar": {"chunk_size": 140, "transfer_timeout": 60, "retry_max": 2},
    
    # VIRTUAL BUTTONS: sent from supervisor dashboard to gateway via LoRa
    "virtual_buttons": [
        # {"id": "prod_start", "label": "Start Produkcji", "gw": "G1"},
    ]
}

class Priority(IntEnum):
    ALARM = 0; COMMAND = 1; RESPONSE = 2; STATUS = 3; DISCOVERY = 4

class Logger:
    ICONS = {'DEBUG': '🔍', 'INFO': '✅', 'WARN': '⚠️ ', 'ERROR': '❌'}
    COLORS = {'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARN': '\033[33m', 'ERROR': '\033[31m'}
    RESET = '\033[0m'
    COMP = {'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'HA': '🏠', 'PING': '🏓',
            'DISC': '🔭', 'CMD': '⚡', 'STATUS': '📤', 'CFG': '⚙️', 'ANOMALY': '🚨',
            'SCHED': '📅', 'CAL': '📆', 'SYNC': '🔄', 'ANT': '📡', 'VBTN': '🔘'}
    def __init__(self):
        self.file = open('/tmp/supervisor.log', 'a')
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


# =====================================================
# CALENDAR: Schedule Manager with per-gateway storage
# =====================================================
class ScheduleManager:
    """Manages GLOBAL + per-gateway schedules. JSON persistence.
    Slot: {id, start, end, mode, note, updated_ts, origin}
    """
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

    def add_slot(self, target, start, end, mode=1, note=""):
        slot = {"id": self._gen_id(), "start": start, "end": end, "mode": mode,
                "note": note[:30] if note else "", "updated_ts": int(now_ts()), "origin": "supervisor"}
        with self.lock:
            if target not in self.data: self.data[target] = []
            self.data[target].append(slot); self._save()
        log.info('CAL', f"➕ [{target}] {start}→{end} mode={mode} ({note})")
        return slot['id']

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
                st = datetime.fromisoformat(s['start']).timestamp() if isinstance(s['start'], str) else s['start']
                et = datetime.fromisoformat(s['end']).timestamp() if isinstance(s['end'], str) else s['end']
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


# =====================================================
# CALENDAR: Chunked LoRa Transfer Protocol
# =====================================================
class CalendarTransfer:
    """Chunked calendar over LoRa.
    sch_b=begin, sch_c=chunk, sch_e=end, sch_a=ack, sch_req=pull request
    """
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

    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots); crc = self.crc16(b64)
        chunks = [b64[i:i+self.chunk_size] for i in range(0, len(b64), self.chunk_size)]
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'dir': direction, 'retries': 0, 'ts': now_ts()}
        self.queue_fn({"t":"sch_b","tid":tid,"g":gw,"dir":direction,"n":len(chunks),"crc":crc}, Priority.COMMAND)
        log.info('SYNC', f"📤 Begin {direction} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")
        def _send():
            for i, chunk in enumerate(chunks):
                time.sleep(CONFIG['lora']['tx_cooldown'] + 0.5)
                self.queue_fn({"t":"sch_c","tid":tid,"s":i,"d":chunk}, Priority.COMMAND)
            time.sleep(CONFIG['lora']['tx_cooldown'] + 0.5)
            self.queue_fn({"t":"sch_e","tid":tid}, Priority.COMMAND)
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
        try:
            slots = self.deserialize(b64)
            log.info('SYNC', f"✅ Received {len(slots)} slots [{info['gw']}] dir={info['dir']}")
            return (info['gw'], info['dir'], slots)
        except Exception as e:
            log.error('SYNC', f"Deserialize: {e}"); return None

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

    def cleanup_stale(self, max_age=120):
        now = now_ts()
        with self.lock:
            for st in [self.incoming, self.outgoing]:
                for tid in [t for t, i in st.items() if now - i.get('ts',0) > max_age]: del st[tid]


# =====================================================
# HOME ASSISTANT MQTT DISCOVERY
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
        ]:
            if eid == "status":
                self.mqtt.publish(f"{p}/binary_sensor/lora_gw_{gl}_{eid}/config", json.dumps({
                    "name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                    "state_topic": st, "value_template": tpl, "payload_on": "online", "payload_off": "offline",
                    "device_class": "connectivity", "device": di}), retain=True)
            else:
                cfg = {"name": f"GW {gw} {name}", "object_id": f"lora_gw_{gl}_{eid}", "unique_id": f"lora_gw_{gl}_{eid}",
                       "state_topic": st, "value_template": tpl, "device": di}
                if icon: cfg["icon"] = icon
                if eid == "uptime": cfg["unit_of_measurement"] = "s"
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


# =====================================================
# MULTI-ANTENNA MANAGER
# =====================================================
class AntennaManager:
    """Manages multiple Meshtastic radios with hot-reconnect."""
    def __init__(self, on_receive):
        self.on_receive = on_receive
        self.interfaces = {}   # {label: SerialInterface}
        self.lock = threading.Lock()
        self._seen_msgs = deque(maxlen=200)  # dedup
        self._reconnect_backoff = {}
        self.running = True

    def connect_all(self):
        pub.subscribe(self._mesh_rx, "meshtastic.receive.text")
        for acfg in CONFIG['mesh_ports']:
            if acfg.get('enabled', False):
                self._connect_one(acfg)
        n = len(self.interfaces)
        log.info('ANT', f"📡 {n} antenna(s) connected")
        if n == 0:
            log.warn('ANT', "⚠️ No antennas connected! Will retry...")

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
            # Dedup: hash of text content
            h = hashlib.md5(text.encode()).hexdigest()[:8]
            if h in self._seen_msgs: return
            self._seen_msgs.append(h)
            log.info('LORA', f"📥 {text[:120]}")
            self.on_receive(text)
        except: pass

    def send_text(self, text):
        """Send via ALL connected antennas for reliability"""
        with self.lock: ifaces = list(self.interfaces.items())
        sent = False
        for label, iface in ifaces:
            try:
                iface.sendText(text)
                sent = True
            except Exception as e:
                log.error('ANT', f"❌ TX {label}: {e}")
                with self.lock: self.interfaces.pop(label, None)
        if not sent:
            log.error('ANT', "❌ No antenna available for TX!")
        return sent

    def reconnect_loop(self):
        """Background thread: tries to reconnect disconnected antennas"""
        while self.running:
            time.sleep(5)
            if not CONFIG['mesh_reconnect'].get('enabled', True): continue
            for acfg in CONFIG['mesh_ports']:
                if not acfg.get('enabled', False): continue
                label = acfg['label']
                with self.lock: connected = label in self.interfaces
                if connected:
                    # Check health
                    try:
                        iface = self.interfaces.get(label)
                        if iface and hasattr(iface, 'localNode'):
                            _ = iface.localNode  # basic health check
                    except:
                        log.warn('ANT', f"⚠️ {label} unhealthy, removing")
                        with self.lock: self.interfaces.pop(label, None)
                else:
                    # Try reconnect with backoff
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
        """Return status dict for all antennas"""
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
        self.gateways = {gw: {'online': False, 'last_seen': None, 'last_seen_ts': 0, 'uptime': 0,
            'devices_total': 0, 'devices_monitored': 0} for gw in CONFIG['gateways']}
        self.devices = {}
        self.device_states = {}  # MUST-1: keyed by "GW:dev"
        self.anomaly_list = []
        self.queue = {p: deque() for p in Priority}
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

    # --- Helpers ---
    def _is_auto_clear_enabled(self, atype):
        return CONFIG.get('anomaly', {}).get('auto_clear', {}).get(atype, True)

    # =====================================================
    # ANOMALY MANAGEMENT
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
            # REQ-6: sensor offline → publish "--" for temp/hum
            self._publish_offline_values(gw, dev)
        vmap = {'low_battery':'battery','critical_battery':'battery','temp_high':'temperature','temp_low':'temperature','hum_high':'humidity','hum_low':'humidity'}
        sk = vmap.get(atype)
        if sk and value is not None: self._update_device_state_value(gw, dev, sk, value)
        log.info('ANOMALY', f"🔔 {gw}/{dev}: {atype}={value}")

    def _publish_offline_values(self, gw, dev):
        """REQ-6: When sensor offline, publish '--' for temp/hum"""
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
                self._add_queue({"t":"an_clr","g":anomaly['gw'],"d":anomaly['dev'],"a":anomaly['type']}, Priority.COMMAND)
            elif send_clear and not exists:
                log.info('ANOMALY', f"👻 Skip an_clr — {anomaly['dev']} gone")

    def _clear_anomalies_by_gateway(self, gw):
        with self.lock: ids = [a['id'] for a in self.anomaly_list if a['gw']==gw]
        for aid in ids: self._remove_anomaly_by_id(aid, send_clear=True)

    def _clear_anomalies_by_type(self, types):
        if isinstance(types, str): types = [types]
        with self.lock: ids = [a['id'] for a in self.anomaly_list if a['type'] in types]
        for aid in ids: self._remove_anomaly_by_id(aid, send_clear=True)

    def _clear_all_anomalies(self):
        with self.lock: ids = [a['id'] for a in self.anomaly_list]
        for aid in ids: self._remove_anomaly_by_id(aid, send_clear=True)

    def _clear_other_anomalies(self):
        skip = ['device_offline','low_battery','critical_battery']
        with self.lock: ids = [a['id'] for a in self.anomaly_list if a['type'] not in skip]
        for aid in ids: self._remove_anomaly_by_id(aid, send_clear=True)

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
                    "devices_low_battery":bat, "devices_anomaly":oth}
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
    # REQ-5: Gateway offline → ALL devices offline
    # =====================================================
    def _gateway_went_offline(self, gw):
        """When gateway goes offline, mark ALL its devices as offline"""
        log.warn('PING', f"💀 {gw} OFFLINE — marking all devices unavailable")
        with self.lock:
            devs = [(dev, info) for dev, info in self.devices.items() if info.get('gateway') == gw]
        for dev, info in devs:
            self.ha.pub_dev_available(gw, dev, False)
            self._publish_offline_values(gw, dev)

    def _gateway_came_online(self, gw):
        """When gateway comes back, request discovery + dump to re-check devices"""
        log.info('PING', f"✅ {gw} ONLINE — requesting discovery + dump")
        self._add_queue({"t":"disc","g":gw}, Priority.DISCOVERY)
        time.sleep(2)
        self._add_queue({"t":"dump_anom","g":gw}, Priority.COMMAND)

    # =====================================================
    # VIRTUAL BUTTONS: supervisor → gateway
    # =====================================================
    def _send_virtual_button(self, gw, btn_name, action="single"):
        """Send virtual button press to gateway via LoRa
        action: single | double | long
        """
        msg = {"t":"vbtn","g":gw,"btn":btn_name,"act":action}
        self._add_queue(msg, Priority.COMMAND)
        log.info('VBTN', f"🔘 {gw}/{btn_name} → {action}")

    # =====================================================
    # DUMP-BASED STALE CLEANUP
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
    # CALENDAR STATUS
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

    def _evaluate_and_send_schedule(self):
        for gw in CONFIG['gateways']:
            mode, nxt, src = self.scheduler.compute_now_and_next(gw)
            self._add_queue({"t":"sch","g":gw,"mode":mode,"from_ts":int(now_ts()),"next_ts":nxt,"src":src}, Priority.COMMAND)

    # =====================================================
    # LIFECYCLE
    # =====================================================
    def start(self):
        log.info('MAIN', "="*50); log.info('MAIN', f"🚀 SUPERVISOR {CONFIG['id']} v4.1"); log.info('MAIN', "="*50)
        self._setup_mqtt(); self.ha = HomeAssistant(self.mqtt_client); time.sleep(3)
        for gw in CONFIG['gateways']: self.ha.reg_gateway(gw)
        self.ha.reg_supervisor()
        # Multi-antenna setup
        self.antenna_mgr = AntennaManager(self._on_lora_receive)
        self.antenna_mgr.connect_all()
        threading.Thread(target=self.antenna_mgr.reconnect_loop, daemon=True, name="ant-reconnect").start()
        # Publish antenna status
        self._publish_antenna_status()
        log.info('MAIN', "✅ Ready!"); time.sleep(2)
        self._send_ping(); time.sleep(5); self._send_cfg(); time.sleep(5); self._send_discovery(); time.sleep(5)
        self._start_dump_cleanup()
        self._evaluate_and_send_schedule(); self._publish_calendar_status()
        threading.Thread(target=self._watchdog_loop, daemon=True).start()
        if CONFIG.get('production_schedule',{}).get('enabled',False):
            threading.Thread(target=self._schedule_loop, daemon=True).start()
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
                      "ha/lora/schedule/#", "ha/lora/anom/#", "ha/lora/vbtn/#",
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
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._add_queue({"t":"sch_req","g":gw,"dir":"push"}, Priority.COMMAND)
                except: pass
                return
            if topic == "ha/lora/schedule/sync_pull":
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self.cal_transfer.start_send(gw, "pull", self.scheduler.get_effective_schedule(gw))
                except: pass
                return
            if topic == "ha/lora/schedule/sync_bidir":
                try:
                    d = json.loads(payload); gw = d.get('target','G1')
                    self._add_queue({"t":"sch_req","g":gw,"dir":"bidir"}, Priority.COMMAND)
                except: pass
                return
            if topic == "ha/lora/schedule/sync_all":
                self.scheduler.force_sync_all_to_supervisor()
                for gw in CONFIG['gateways']:
                    self.cal_transfer.start_send(gw, "pull", self.scheduler.get_effective_schedule(gw)); time.sleep(3)
                self._publish_calendar_status(); return

            # --- VIRTUAL BUTTON from HA ---
            if topic.startswith("ha/lora/vbtn/"):
                try:
                    d = json.loads(payload) if payload.strip() else {}
                    self._send_virtual_button(d.get('gw','G1'), d.get('btn','button1'), d.get('act','single'))
                except: pass
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
                elif a == 'discovery_all': self._send_discovery()
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
                if a == 'ping': self._add_queue({"t":"ping","g":gw}, Priority.COMMAND)
                elif a == 'discovery': self._add_queue({"t":"disc","g":gw}, Priority.DISCOVERY)
                elif a == 'clear_anomalies': self._clear_anomalies_by_gateway(gw)
                elif a == 'dump_anom': self._add_queue({"t":"dump_anom","g":gw}, Priority.COMMAND)
                return

            # Device set/refresh
            if len(parts)>=4 and parts[-1] in ['set','refresh']:
                gw, dev = parts[1].upper(), self._find_device_name(parts[1].upper(), parts[2])
                if parts[-1]=='set':
                    self._add_queue({"t":"cmd","g":gw,"d":dev,"c":"state","v":payload}, Priority.COMMAND)
                else:
                    self._add_queue({"t":"req","g":gw,"d":dev}, Priority.RESPONSE)
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
        """Called by AntennaManager when text received"""
        try:
            data = json.loads(text)
            threading.Thread(target=self._handle_lora, args=(data,), daemon=True).start()
        except: pass

    def _handle_lora(self, data):
        try:
            t, gw = data.get('t'), data.get('g')
            if gw and gw in self.gateways: self._update_gw_last_seen(gw)
            if t == 'pong': self._handle_pong(data)
            elif t == 'st': self._handle_status(data)
            elif t == 'an': self._handle_anomaly(data)
            elif t == 'disc_resp': self._handle_discovery_resp(data)
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
        if direction == 'push':
            self.scheduler.merge_schedule(gw, slots); self._publish_calendar_status()
        elif direction == 'bidir':
            self.scheduler.merge_schedule(gw, slots)
            self.cal_transfer.start_send(gw, "pull", self.scheduler.get_effective_schedule(gw))
            self._publish_calendar_status()

    def _update_gw_last_seen(self, gw):
        was_offline = False
        with self.lock:
            if gw in self.gateways:
                was_offline = not self.gateways[gw]['online']
                self.gateways[gw].update({'last_seen':now_str(),'last_seen_ts':now_ts(),'online':True})
        self._publish_gw_stats(gw)
        # REQ-5: if gateway was offline and came back, re-check devices
        if was_offline:
            self._gateway_came_online(gw)

    def _handle_pong(self, data):
        gw = data.get('g')
        with self.lock:
            self.gateways[gw].update({'uptime':data.get('up',0),'devices_total':data.get('dev_total',0),'devices_monitored':data.get('dev_mon',0)})
        self._publish_gw_stats(gw)

    def _handle_status(self, data):
        gw, dev = data.get('g'), data.get('d')
        if not dev: return
        ls, av = data.get('ls', now_str()), data.get('av', True)
        sd = {'last_seen': ls}
        for full, short in [('state','st'),('temperature','tmp'),('humidity','hum'),('battery','bat'),
                            ('contact','con'),('occupancy','occ'),('water_leak','wtr'),('smoke','smk'),('brightness','bri')]:
            if short in data: sd[full] = data[short]
        # REQ-6: if not available, temp/hum = "--"
        if not av:
            if 'temperature' not in sd: sd['temperature'] = '--'
            if 'humidity' not in sd: sd['humidity'] = '--'
        sk = f"{gw}:{dev}"
        with self.lock:
            if dev not in self.devices: self.devices[dev] = {}
            self.devices[dev].update({'gateway':gw,'available':av,'last_seen':ls,'battery':data.get('bat')})
            if sk not in self.device_states: self.device_states[sk] = {}
            self.device_states[sk].update(sd)
            cached = dict(self.device_states[sk])
        self.ha.pub_dev_state(gw, dev, cached)
        self.ha.pub_dev_available(gw, dev, av)

    def _handle_anomaly(self, data):
        self._add_anomaly(data.get('g'), data.get('d'), data.get('a'), data.get('v'), data.get('ts', int(now_ts())))

    def _handle_discovery_resp(self, data):
        gw, dev, dtype, caps = data.get('g'), data.get('d'), data.get('dt'), data.get('cap',[])
        ls = data.get('ls','unknown')
        if dtype in ['switch','light']: self.ha.reg_switch(gw, dev, dtype)
        elif dtype == 'sensor': self.ha.reg_sensor(gw, dev, caps)
        elif dtype == 'binary_sensor': self.ha.reg_binary(gw, dev, caps)
        with self.lock: self.devices[dev] = {'gateway':gw,'type':dtype,'caps':caps,'available':True,'last_seen':ls}
        if gw in self._disc_refresh_gw:
            self._gw_devices[gw] = {dev}; self._disc_refresh_gw.discard(gw)
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

    # =====================================================
    # WATCHDOG + SCHEDULE LOOPS
    # =====================================================
    def _watchdog_loop(self):
        orphan_ctr = 0; ant_ctr = 0
        while self.running:
            time.sleep(30); now = now_ts()
            with self.lock:
                for gw, info in self.gateways.items():
                    if info['online'] and info['last_seen_ts']>0 and now-info['last_seen_ts']>CONFIG['gateway_timeout']:
                        info['online'] = False
                        # REQ-5: gateway offline → all devices offline
                        threading.Thread(target=self._gateway_went_offline, args=(gw,), daemon=True).start()
            for gw in self.gateways: self._publish_gw_stats(gw)
            orphan_ctr += 1
            if orphan_ctr >= 10:
                orphan_ctr = 0
                if not self._startup_phase: self._prune_anomalies()
            self.cal_transfer.cleanup_stale()
            # Publish antenna status every ~2 min
            ant_ctr += 1
            if ant_ctr >= 4:
                ant_ctr = 0; self._publish_antenna_status()

    def _schedule_loop(self):
        pre = CONFIG.get('production_schedule',{}).get('pre_notify_seconds',[30,5])
        last_modes, notified = {}, {}
        while self.running:
            time.sleep(5)
            for gw in CONFIG['gateways']:
                mode, nxt, src = self.scheduler.compute_now_and_next(gw)
                prev = last_modes.get(gw)
                if prev is not None and prev != mode:
                    self._add_queue({"t":"sch","g":gw,"mode":mode,"from_ts":int(now_ts()),"next_ts":nxt,"src":src}, Priority.COMMAND)
                    self._publish_calendar_status()
                last_modes[gw] = mode
                if nxt > 0:
                    rem = nxt - now_ts()
                    if gw not in notified: notified[gw] = set()
                    for sec in pre:
                        if rem <= sec and sec not in notified[gw]:
                            notified[gw].add(sec)
                            self._add_queue({"t":"sch","g":gw,"mode":mode,"from_ts":int(now_ts()),"next_ts":nxt,"src":src}, Priority.COMMAND)
                    if rem <= 0: notified[gw] = set()

    # =====================================================
    # TX QUEUE + LOOP
    # =====================================================
    def _add_queue(self, msg, priority): self.queue[priority].append(msg)

    def _send_lora(self, msg):
        try:
            s = json.dumps(msg, separators=(',',':'))
            log.info('LORA', f"📤 TX ({len(s)}B): {s[:120]}")
            if self.antenna_mgr:
                self.antenna_mgr.send_text(s)
            self.last_tx = time.time()
        except Exception as e: log.error('LORA', f"TX error: {e}")

    def _send_ping(self, gw=None):
        msg = {"t":"ping"}
        if gw: msg["g"] = gw
        self._add_queue(msg, Priority.COMMAND)

    def _send_discovery(self, gw=None):
        msg = {"t":"disc"}
        if gw: msg["g"]=gw; self._disc_refresh_gw.add(gw)
        else: self._disc_refresh_gw.update(CONFIG['gateways'])
        self._add_queue(msg, Priority.DISCOVERY)

    def _send_cfg(self, gw=None):
        msg = {"t":"cfg","to":{"sw":CONFIG['timeout']['switch'],"lt":CONFIG['timeout']['light'],
                               "sn":CONFIG['timeout']['sensor'],"bs":CONFIG['timeout']['binary_sensor']}}
        if gw: msg["g"]=gw
        self._add_queue(msg, Priority.COMMAND)

    def _send_dump_anom_all(self):
        def _do():
            for gw in CONFIG['gateways']:
                self._add_queue({"t":"dump_anom","g":gw}, Priority.COMMAND); time.sleep(2)
        threading.Thread(target=_do, daemon=True).start()

    def _loop(self):
        if time.time()-self.last_tx < CONFIG['lora']['tx_cooldown']: return
        for p in sorted(Priority):
            if self.queue[p]: self._send_lora(self.queue[p].popleft()); return

    def _cleanup(self):
        self.running = False
        if self.antenna_mgr: self.antenna_mgr.close_all()
        if self.mqtt_client: self.mqtt_client.loop_stop(); self.mqtt_client.disconnect()
        log.info('MAIN', "Stopped")

if __name__ == "__main__":
    Supervisor().start()
