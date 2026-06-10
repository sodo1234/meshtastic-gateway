"""ParamSync — bidirectional parameter sync (step 17), gateway = master.

Six params, exposed as HA `number` entities on BOTH brokers (gateway + supervisor):
  P1 stagnation bateryjne [h]   P2 stagnation sieciowe [h]   P3 placeholder
  T1 offline switch/light [min] T2 offline temp/hum [min]    T3 offline door/leak [min]

Push-pull (gateway = source of truth):
  - edit on gateway HA  → master applies → broadcast `param_upd` (confirmed) → supervisor mirror
  - edit on supervisor HA → `params` (proposal) → gateway applies → `param_upd` (confirmed) back
  - supervisor startup   → `params_req` → gateway `push_all`

Functional owners: gateway applies P1/P2/P3 (stagnation); supervisor applies T1/T2/T3
(offline). All six are visible/editable on both sides; values are always the master's.
"""
import hashlib
import json
import os


# key → entity/clamp/display spec
PARAM_DEFS = {
    "P1": {"default": 48,  "min": 1, "max": 720,  "step": 1, "unit": "h",
           "name": "Stagnation bateryjne", "icon": "mdi:battery-clock"},
    "P2": {"default": 24,  "min": 1, "max": 720,  "step": 1, "unit": "h",
           "name": "Stagnation sieciowe", "icon": "mdi:transmission-tower"},
    "P3": {"default": 15,  "min": 1, "max": 1440, "step": 1, "unit": "min",
           "name": "Raportowanie temp", "icon": "mdi:clock-outline"},
    "T1": {"default": 30,  "min": 1, "max": 1440, "step": 1, "unit": "min",
           "name": "Offline switch/light", "icon": "mdi:lightbulb-alert"},
    "T2": {"default": 60,  "min": 1, "max": 1440, "step": 1, "unit": "min",
           "name": "Offline temp/hum", "icon": "mdi:thermometer-alert"},
    "T3": {"default": 120, "min": 1, "max": 1440, "step": 1, "unit": "min",
           "name": "Offline door/leak/motion", "icon": "mdi:door-closed-lock"},
}
PARAM_ORDER = ["P1", "P2", "P3", "T1", "T2", "T3"]
# Dwie grupy wysyłki (przyciski jak v38 send_config) — model edytuj→Send (nie auto):
CONFIG_KEYS = ["P1", "P2", "P3"]      # przycisk „Send Config"  (stagnation + raportowanie)
TIMEOUT_KEYS = ["T1", "T2", "T3"]     # przycisk „Send Timeout" (offline switch/sensor/binary)
SEND_BUTTONS = [
    ("send_config",  "Wyślij Config",  "mdi:cog-sync",  CONFIG_KEYS),
    ("send_timeout", "Wyślij Timeout", "mdi:timer-cog", TIMEOUT_KEYS),
]


class ParamSync:
    def __init__(self, role, gw_id, mqtt, send_fn, logger=None, persist_path=None,
                 ha_prefix="homeassistant", state_prefix="lora", on_change=None):
        assert role in ("gateway", "supervisor")
        self.role = role                 # 'gateway' = master | 'supervisor' = mirror
        self.gw_id = gw_id
        self.mqtt = mqtt
        self.send = send_fn              # callable(dict) → send over LoRa
        self.log = logger
        self.persist_path = persist_path
        self.ha = ha_prefix
        self.sp = state_prefix
        self.on_change = on_change       # callable(key, value) — apply to mechanism
        self.values = {k: PARAM_DEFS[k]["default"] for k in PARAM_ORDER}
        self._load()

    # ── persistence ─────────────────────────────────────
    def _load(self):
        if not self.persist_path or not os.path.exists(self.persist_path):
            return
        try:
            data = json.load(open(self.persist_path, encoding="utf-8"))
            for k in PARAM_ORDER:
                if k in data:
                    self.values[k] = self._clamp(k, data[k])
        except Exception as e:
            if self.log:
                self.log.warn('PARAM', f"load failed: {e}")

    def _save(self):
        if not self.persist_path:
            return
        try:
            json.dump(self.values, open(self.persist_path, "w", encoding="utf-8"),
                      separators=(',', ':'))
        except Exception as e:
            if self.log:
                self.log.warn('PARAM', f"save failed: {e}")

    # ── HA entities ─────────────────────────────────────
    def _eid(self, key):
        if self.role == "gateway":
            return f"lora_{self.gw_id.lower()}_param_{key.lower()}"
        # 'supg1' = świeży uid, by HA utworzył liczniki pod device LoRa Gateway G1
        # (stare lora_sup_param_* utknęły pod LoRa Supervisor — lepki rejestr device)
        return f"lora_supg1_param_{key.lower()}"

    def _device(self):
        """Wszystkie encje (number + przyciski Send) wpięte pod urządzenie
        LoRa Gateway G1 na OBU HA (bramki i supervisora) — user 2026-06-10.
        Na supervisorze gw_id = pierwsza bramka, więc identyfikator ten sam."""
        return {"identifiers": [f"lora_gateway_{self.gw_id.lower()}"]}

    def register_entities(self):
        device = self._device()
        for key in PARAM_ORDER:
            d, uid = PARAM_DEFS[key], self._eid(key)
            cfg = {"name": f"{key} · {d['name']}", "object_id": uid, "unique_id": uid,
                   "command_topic": f"{self.sp}/params/{self.role}/set/{key}",
                   "state_topic": f"{self.sp}/params/{self.role}/state/{key}",
                   "min": d["min"], "max": d["max"], "step": d["step"],
                   "mode": "box", "icon": d["icon"], "device": device}
            if d["unit"]:
                cfg["unit_of_measurement"] = d["unit"]
            self.mqtt.publish(f"{self.ha}/number/{uid}/config",
                              json.dumps(cfg, separators=(',', ':')), retain=True)
        # 2 przyciski Send (jak v38 send_config) — push grupy przez LoRa po naciśnięciu
        for eid, name, icon, _keys in SEND_BUTTONS:
            buid = self._btn_uid(eid)
            self.mqtt.publish(f"{self.ha}/button/{buid}/config", json.dumps({
                "name": f"LoRa {name}", "object_id": buid, "unique_id": buid,
                "command_topic": f"{self.sp}/params/{self.role}/cmd/{eid}",
                "icon": icon, "device": device}, separators=(',', ':')), retain=True)
        self._publish_states()
        if self.log:
            self.log.info('PARAM', f"⚙️ {len(PARAM_ORDER)} number + {len(SEND_BUTTONS)} "
                          f"przyciski Send ({self.role}) + stan")

    def _btn_uid(self, eid):
        if self.role == "gateway":
            return f"lora_{self.gw_id.lower()}_param_{eid}"
        return f"lora_sup_param_{eid}"

    def subscribe(self):
        self.mqtt.subscribe(f"{self.sp}/params/{self.role}/set/+")
        self.mqtt.subscribe(f"{self.sp}/params/{self.role}/cmd/+")    # przyciski Send

    def _publish_states(self):
        for key in PARAM_ORDER:
            self.mqtt.publish(f"{self.sp}/params/{self.role}/state/{key}",
                              str(self.values[key]), retain=True)

    def _publish_state(self, key):
        self.mqtt.publish(f"{self.sp}/params/{self.role}/state/{key}",
                          str(self.values[key]), retain=True)

    # ── local HA edit ───────────────────────────────────
    def on_mqtt_set(self, topic, payload):
        """Wire from app's on_mqtt for topic lora/params/<role>/set/<KEY>."""
        key = topic.rsplit('/', 1)[-1]
        if key not in self.values:
            return
        try:
            val = int(round(float(payload)))
        except (TypeError, ValueError):
            return
        self.handle_local_set(key, val)

    def handle_local_set(self, key, val):
        """Edycja pola w HA = TYLKO lokalnie (zapis + optimistic state). Propagacja do
        drugiej strony dopiero po naciśnięciu przycisku Send (model v38). Engine'y
        czytają wartość na bieżąco przez get(), więc lokalny efekt jest natychmiast."""
        val = self._clamp(key, val)
        changed = self.values.get(key) != val
        self.values[key] = val
        self._save(); self._publish_state(key)
        if changed and self.on_change:
            self.on_change(key, val)
        if self.log:
            self.log.info('PARAM', f"⚙️ {key}={val}{PARAM_DEFS[key]['unit']} "
                          f"(lokalnie — naciśnij Send aby wysłać)")

    # ── wysyłka grupowa (przyciski Send, jak v38 send_config) ──
    def on_cmd(self, topic, payload=None):
        """Wire z app on_mqtt: lora/params/<role>/cmd/<send_config|send_timeout>."""
        eid = topic.rsplit('/', 1)[-1]
        if eid == "send_config":
            self.send_config()
        elif eid == "send_timeout":
            self.send_timeout()

    def send_config(self):
        self._send_group(CONFIG_KEYS, "Config")

    def send_timeout(self):
        self._send_group(TIMEOUT_KEYS, "Timeout")

    def _send_group(self, keys, label):
        d = {k: self.values[k] for k in keys}
        if self.role == "gateway":                       # master → broadcast potwierdzonej prawdy
            self.send({"t": "param_upd", "g": self.gw_id, "d": d})
            if self.log:
                self.log.info('PARAM', f"⚙️➡️ Send {label} (master→broadcast): {d}")
        else:                                            # mirror → proposal do mastera
            self.send({"t": "params", "g": self.gw_id, "d": d})
            if self.log:
                self.log.info('PARAM', f"⚙️➡️ Send {label} (proposal→master): {d}")

    # ── remote (LoRa) ───────────────────────────────────
    def handle_remote(self, data):
        t, d = data.get('t'), (data.get('d') or {})
        if self.role == "gateway" and t == "params":     # proposal from supervisor
            applied = {}
            for key, val in d.items():
                if key in self.values:
                    val = self._clamp(key, val)
                    if self.values.get(key) != val and self.on_change:
                        self.on_change(key, val)
                    self.values[key] = val
                    applied[key] = val
            if applied:
                self._save(); self._publish_states()
                self.send({"t": "param_upd", "g": self.gw_id, "d": applied})
                if self.log:
                    self.log.info('PARAM', f"⚙️ proposal {applied} → zastosowano + confirm")
        elif self.role == "supervisor" and t == "param_upd":   # confirmed from gateway
            for key, val in d.items():
                if key in self.values:
                    self.values[key] = self._clamp(key, val)
                    if self.on_change:
                        self.on_change(key, self.values[key])
            self._save(); self._publish_states()
            if self.log:
                self.log.info('PARAM', f"⚙️ param_upd {d} → mirror zaktualizowany")
        elif self.role == "gateway" and t == "params_req":     # supervisor wants full state
            self.push_all()

    def push_all(self):
        """Gateway → broadcast confirmed full param set (startup / on request)."""
        if self.role == "gateway":
            self.send({"t": "param_upd", "g": self.gw_id, "d": dict(self.values)})
            if self.log:
                self.log.info('PARAM', f"⚙️ push_all → {self.values}")

    def request(self):
        """Supervisor → ask gateway to push the authoritative set."""
        if self.role == "supervisor":
            self.send({"t": "params_req", "g": self.gw_id})

    # ── helpers ─────────────────────────────────────────
    def _clamp(self, key, val):
        d = PARAM_DEFS[key]
        try:
            val = int(round(float(val)))
        except (TypeError, ValueError):
            val = d["default"]
        return max(d["min"], min(d["max"], val))

    def get(self, key):
        return self.values.get(key)

    def params_hash(self):
        """Krótki hash wszystkich wartości — do porównania w HB (sync gdy ≠)."""
        raw = json.dumps({k: self.values[k] for k in PARAM_ORDER},
                         separators=(',', ':'), sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:8]
