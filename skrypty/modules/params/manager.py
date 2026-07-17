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
    # Progi anomalii (gateway = master) — silniki temphum/battery czytają na żywo przez
    # get_thresholds()→param_sync.get(); strojenie z dashboardu zamiast SSH+restart config.py.
    "TH": {"default": 35,  "min": -40, "max": 125, "step": 1, "unit": "°C",
           "name": "Próg temp. wysoka", "icon": "mdi:thermometer-high"},
    "TL": {"default": 10,  "min": -40, "max": 125, "step": 1, "unit": "°C",
           "name": "Próg temp. niska", "icon": "mdi:thermometer-low"},
    "HH": {"default": 70,  "min": 0, "max": 100, "step": 1, "unit": "%",
           "name": "Próg wilg. wysoka", "icon": "mdi:water-percent-alert"},
    "HL": {"default": 20,  "min": 0, "max": 100, "step": 1, "unit": "%",
           "name": "Próg wilg. niska", "icon": "mdi:water-off"},
    "BL": {"default": 25,  "min": 1, "max": 100, "step": 1, "unit": "%",
           "name": "Próg bateria niska", "icon": "mdi:battery-low"},
    "BC": {"default": 15,  "min": 1, "max": 100, "step": 1, "unit": "%",
           "name": "Próg bateria krytyczna", "icon": "mdi:battery-alert"},
    # F3 (2026-07-15): parametry probe'a supervisor-link per-bramka (SupervisorLinkProbe) —
    # czytane na żywo, więc strojenie interwału/timeoutu z dashboardu bez restartu.
    "P4": {"default": 15,  "min": 1, "max": 1440, "step": 1, "unit": "min",
           "name": "Ping sup interwał", "icon": "mdi:timer-sync"},
    "T4": {"default": 25,  "min": 5, "max": 300,  "step": 1, "unit": "s",
           "name": "Ping sup timeout", "icon": "mdi:timer-alert"},
    "PR": {"default": 2,   "min": 0, "max": 10,   "step": 1, "unit": "x",
           "name": "Ping sup retry", "icon": "mdi:repeat"},
}
PARAM_ORDER = ["P1", "P2", "P3", "T1", "T2", "T3", "TH", "TL", "HH", "HL", "BL", "BC",
              "P4", "T4", "PR"]
# Trzy grupy wysyłki (przyciski jak v38 send_config) — model edytuj→Send (nie auto):
CONFIG_KEYS = ["P1", "P2", "P3", "P4"]      # przycisk „Send Config"  (stagnation + raportowanie)
TIMEOUT_KEYS = ["T1", "T2", "T3", "T4"]     # przycisk „Send Timeout" (offline switch/sensor/binary)
THRESHOLD_KEYS = ["TH", "TL", "HH", "HL", "BL", "BC", "PR"]   # „Send Progi" (temp/hum/bateria)
SEND_BUTTONS = [
    ("send_config",    "Wyślij Config",  "mdi:cog-sync",   CONFIG_KEYS),
    ("send_timeout",   "Wyślij Timeout", "mdi:timer-cog",  TIMEOUT_KEYS),
    ("send_threshold", "Wyślij Progi",   "mdi:gauge",      THRESHOLD_KEYS),
]


class ParamSync:
    def __init__(self, role, gw_id, mqtt, send_fn, logger=None, persist_path=None,
                 ha_prefix="homeassistant", state_prefix="lora", on_change=None,
                 seed=None):
        assert role in ("gateway", "supervisor")
        self.role = role                 # 'gateway' = master | 'supervisor' = mirror
        self.gw_id = gw_id
        self.mqtt = mqtt
        self.send = send_fn              # callable(dict) → send over LoRa
        self.log = logger
        # F3 (2026-07-15): topic-role — gateway bez zmian; supervisor per-bramka
        # (lora/params/supervisor/<gl>/...) → jedna instancja ParamSync per bramka zamiast
        # jednej wspólnej (poprzednio timeouty/progi „per-bramka" nie działały naprawdę).
        self.tp = self.role if self.role == "gateway" else f"supervisor/{self.gw_id.lower()}"
        self.persist_path = persist_path
        self.ha = ha_prefix
        self.sp = state_prefix
        self.on_change = on_change       # callable(key, value) — apply to mechanism
        self.values = {k: PARAM_DEFS[k]["default"] for k in PARAM_ORDER}
        # seed: wartości startowe z config (np. progi anomalii) — nadpisują DEFAULT, ale
        # persisted (edycje usera z dashboardu) wygrywają (kolejność: default→seed→_load).
        if seed:
            for k, v in seed.items():
                if k in self.values and v is not None:
                    self.values[k] = self._clamp(k, v)
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
        # F3: de-hardcode — per-bramka uid (dla gw_id="G1" daje identyczny
        # lora_supg1_param_* jak dotąd = zero breakage; G2/G3 dostają własne encje).
        # 'sup{gw}' = świeży uid, by HA utworzył liczniki pod device LoRa Gateway Gx
        # (stare lora_sup_param_* utknęły pod LoRa Supervisor — lepki rejestr device)
        return f"lora_sup{self.gw_id.lower()}_param_{key.lower()}"

    def _device(self):
        """Wszystkie encje (number + przyciski Send) wpięte pod urządzenie
        LoRa Gateway G1 na OBU HA (bramki i supervisora) — user 2026-06-10.
        Na supervisorze gw_id = pierwsza bramka, więc identyfikator ten sam.
        FIX 2026-07-07 (audyt): device block MUSI mieć `name` — HA odrzuca discovery
        tworzące NOWE urządzenie bez nazwy (objaw: 12 configów lora_g2_param_* retained
        w brokerze, zero encji w HA, panel PARAMETRY bramki „Nie znaleziono encji").
        Stare g1 działały, bo device istniał już w registry z wcześniejszej rejestracji."""
        return {"identifiers": [f"lora_gateway_{self.gw_id.lower()}"],
                "name": (f"LoRa {self.gw_id}" if self.role == "supervisor"
                         else f"LoRa Gateway {self.gw_id}"),
                "manufacturer": "LoRa SCADA", "model": "Gateway Params"}

    def register_entities(self):
        device = self._device()
        for key in PARAM_ORDER:
            d, uid = PARAM_DEFS[key], self._eid(key)
            cfg = {"name": f"{key} · {d['name']}", "object_id": uid, "unique_id": uid,
                   "command_topic": f"{self.sp}/params/{self.tp}/set/{key}",
                   "state_topic": f"{self.sp}/params/{self.tp}/state/{key}",
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
                "command_topic": f"{self.sp}/params/{self.tp}/cmd/{eid}",
                "icon": icon, "device": device}, separators=(',', ':')), retain=True)
        self._publish_states()
        if self.log:
            self.log.info('PARAM', f"⚙️ {len(PARAM_ORDER)} number + {len(SEND_BUTTONS)} "
                          f"przyciski Send ({self.role}) + stan")

    def _btn_uid(self, eid):
        if self.role == "gateway":
            return f"lora_{self.gw_id.lower()}_param_{eid}"
        # F3: per-bramka uid (jak _eid) — inaczej przyciski Send N instancji nadpisywałyby
        # się nawzajem (ten sam retained config, różne command_topic → wygrywa ostatni).
        return f"lora_sup{self.gw_id.lower()}_param_{eid}"

    def subscribe(self):
        self.mqtt.subscribe(f"{self.sp}/params/{self.tp}/set/+")
        # cmd/# (nie +): łapie i `cmd/send_x` (broadcast/self) i `cmd/<gw>/send_x` (per-bramka, supervisor).
        self.mqtt.subscribe(f"{self.sp}/params/{self.tp}/cmd/#")    # przyciski Send (per-gw)

    def _publish_states(self):
        for key in PARAM_ORDER:
            self.mqtt.publish(f"{self.sp}/params/{self.tp}/state/{key}",
                              str(self.values[key]), retain=True)

    def _publish_state(self, key):
        self.mqtt.publish(f"{self.sp}/params/{self.tp}/state/{key}",
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
    _GROUPS = None   # lazy: eid → (keys, label)

    def on_cmd(self, topic, payload=None):
        """Wire z app on_mqtt: lora/params/<role>/cmd/[<gw>/]<send_config|send_timeout|send_threshold>.
        SUPERVISOR: opcjonalny segment <gw> → wyślij progi/timeouty TYLKO do tej bramki (wymóg usera:
        params/timeouty per-bramka). Brak segmentu = do self.gw_id (kompat wstecz / przyciski ParamSync)."""
        if ParamSync._GROUPS is None:
            ParamSync._GROUPS = {"send_config": (CONFIG_KEYS, "Config"),
                                 "send_timeout": (TIMEOUT_KEYS, "Timeout"),
                                 "send_threshold": (THRESHOLD_KEYS, "Progi")}
        tail = topic.split(f"/params/{self.tp}/cmd/", 1)[-1].split('/')  # (<gw>,) <eid>
        target_gw = tail[0].upper() if len(tail) == 2 else None
        grp = ParamSync._GROUPS.get(tail[-1])
        if grp:
            self._send_group(grp[0], grp[1], target_gw)

    def send_config(self, target_gw=None):
        self._send_group(CONFIG_KEYS, "Config", target_gw)

    def send_timeout(self, target_gw=None):
        self._send_group(TIMEOUT_KEYS, "Timeout", target_gw)

    def send_threshold(self, target_gw=None):
        self._send_group(THRESHOLD_KEYS, "Progi", target_gw)

    def _send_group(self, keys, label, target_gw=None):
        d = {k: self.values[k] for k in keys}
        if self.role == "gateway":                       # master → broadcast potwierdzonej prawdy
            self.send({"t": "param_upd", "g": self.gw_id, "d": d})
            if self.log:
                self.log.info('PARAM', f"⚙️➡️ Send {label} (master→broadcast): {d}")
        else:                                            # mirror → proposal do KONKRETNEJ bramki (per-gw)
            gw = target_gw or self.gw_id
            self.send({"t": "params", "g": gw, "d": d})
            if self.log:
                self.log.info('PARAM', f"⚙️➡️ Send {label} → {gw} (proposal per-gw): {d}")

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
