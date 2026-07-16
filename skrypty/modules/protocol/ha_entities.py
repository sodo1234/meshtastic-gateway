"""HA MQTT Discovery — registers entities in Home Assistant.

Extracted from supervisor_v38.py:497-634. Constructor injection (mqtt, logger).
Creates entities by publishing JSON configs to homeassistant/<type>/<uid>/config.

All gateway status entities read from one JSON topic via value_template.
Buttons publish to command_topic, HA sends MQTT on press.
"""
import json

HA_PREFIX = "homeassistant"
STATE_PREFIX = "lora"


class HAEntities:
    def __init__(self, mqtt, logger=None):
        self.mqtt = mqtt
        self.log = logger
        self._registered = set()
        self._dev_state = {}       # {(gw,dev): last full fields} — by avail-only nie kasował last_seen/capów

    def _safe(self, s):
        return s.replace(' ', '_').lower()

    def _gw_device(self, gw):
        return {"identifiers": [f"lora_gateway_{gw.lower()}"],
                "name": f"LoRa Gateway {gw}",
                "model": "LoRa Zigbee Gateway", "manufacturer": "Custom"}

    def _sup_device(self):
        return {"identifiers": ["lora_supervisor"],
                "name": "LoRa Supervisor",
                "model": "Supervisor", "manufacturer": "Custom"}

    def _dev_device(self, gw, dev, model="Zigbee Device"):
        return {"identifiers": [f"lora_{gw.lower()}_{self._safe(dev)}"],
                "name": f"LoRa {dev}", "model": model,
                "manufacturer": "LoRa Gateway",
                "via_device": f"lora_gateway_{gw.lower()}"}

    def _pub(self, domain, uid, cfg):
        self.mqtt.publish(f"{HA_PREFIX}/{domain}/{uid}/config",
                          json.dumps(cfg, separators=(',', ':')), retain=True)

    # ── Gateway entities ────────────────────────────────
    def reg_gateway(self, gw):
        key = f"gw_{gw}"
        if key in self._registered:
            return
        gl = gw.lower()
        di = self._gw_device(gw)
        st = f"{STATE_PREFIX}/gw/{gl}/status"

        sensors = [
            ("status", "Status", "{{ value_json.state }}", None, None, "binary_sensor"),
            ("uptime", "Uptime", "{{ value_json.uptime | default(0) }}", "mdi:timer", "s", "sensor"),
            ("last_seen", "Last Seen", "{{ value_json.last_seen | default('--') }}", "mdi:clock-outline", None, "sensor"),
            ("devices_total", "Total", "{{ value_json.devices_total | default(0) }}", "mdi:devices", None, "sensor"),
            ("devices_monitored", "Monitored", "{{ value_json.devices_monitored | default(0) }}", "mdi:eye", None, "sensor"),
            ("devices_priority", "Priority", "{{ value_json.devices_priority | default(0) }}", "mdi:alert-octagon", None, "sensor"),
            ("devices_offline", "Offline", "{{ value_json.devices_offline | default(0) }}", "mdi:close-circle", None, "sensor"),
        ]

        for eid, name, tpl, icon, unit, domain in sensors:
            uid = f"lora_gw_{gl}_{eid}"
            if domain == "binary_sensor":
                self._pub(domain, uid, {
                    "name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                    "state_topic": st, "value_template": tpl,
                    "payload_on": "online", "payload_off": "offline",
                    "device_class": "connectivity", "device": di})
            else:
                cfg = {"name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                       "state_topic": st, "value_template": tpl, "device": di}
                if icon:
                    cfg["icon"] = icon
                if unit:
                    cfg["unit_of_measurement"] = unit
                self._pub(domain, uid, cfg)

        self._registered.add(key)
        if self.log:
            self.log.info('HA', f'🏠 Gateway {gw}: 7 sensors registered')

    def reg_gw_buttons_local(self, gw):
        """Gateway-LOCAL buttons (on the gateway's own broker → gateway acts).
        Command topic is what the gateway subscribes to: lora/gw/<gl>/cmd/<eid>."""
        key = f"gwbtn_local_{gw}"
        if key in self._registered:
            return
        gl, di = gw.lower(), self._gw_device(gw)
        for eid, name, icon in [
            ("ping", "Ping", "mdi:lan-connect"),
            ("discovery", "Discovery", "mdi:magnify"),
            ("dump", "Dump anomalii", "mdi:database-export"),          # F5: reconcyliacja teraz
            ("sync_req", "Sync czasu", "mdi:clock-sync"),               # F5: poproś supervisora o sync
        ]:
            uid = f"lora_gw_{gl}_{eid}"
            self._pub("button", uid, {
                "name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                "command_topic": f"{STATE_PREFIX}/gw/{gl}/cmd/{eid}",
                "device": di, "icon": icon})
        self._registered.add(key)

    def reg_gw_controls(self, gw):
        """Supervisor per-gateway control buttons, attached to the gateway device.
        Publish to lora/supervisor/cmd/<gl>/<action> → supervisor sends targeted LoRa."""
        key = f"gwctl_{gw}"
        if key in self._registered:
            return
        gl, di = gw.lower(), self._gw_device(gw)
        # eid → entity object_id (matches dashboard), action → supervisor cmd topic
        for eid, action, name, icon in [
            ("ping", "ping", "Ping", "mdi:lan-connect"),
            ("discovery", "disc", "Discovery", "mdi:magnify"),
            ("sync", "sync", "Sync Time", "mdi:clock-sync"),
        ]:
            uid = f"lora_gw_{gl}_{eid}"
            self._pub("button", uid, {
                "name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                "command_topic": f"{STATE_PREFIX}/supervisor/cmd/{gl}/{action}",
                "device": di, "icon": icon})
        self._registered.add(key)
        if self.log:
            self.log.info('HA', f'🏠 {gw}: 3 control buttons (ping/disc/sync) w supervisor HA')

    # ── Supervisor entities ─────────────────────────────
    def reg_supervisor(self):
        if "supervisor" in self._registered:
            return
        di = self._sup_device()

        for eid, name, icon in [
            ("ping_all", "Ping All", "mdi:lan-connect"),
            ("discovery_all", "Discovery All", "mdi:magnify"),
            ("sync_all", "Sync All (czas)", "mdi:clock-sync"),
        ]:
            uid = f"lora_supervisor_{eid}"
            self._pub("button", uid, {
                "name": f"LoRa {name}", "object_id": uid, "unique_id": uid,
                "command_topic": f"{STATE_PREFIX}/supervisor/cmd/{eid}",
                "device": di, "icon": icon})

        self._registered.add("supervisor")
        if self.log:
            self.log.info('HA', f'🏠 Supervisor: 3 global buttons (ping_all/discovery_all/sync_all)')

    # ── Publish gateway status JSON ─────────────────────
    def pub_gw_status(self, gw, data):
        self.mqtt.publish(f"{STATE_PREFIX}/gw/{gw.lower()}/status",
                          json.dumps(data, separators=(',', ':')), retain=True)

    # ── Device availability (offline cascade) ───────────
    def pub_device_avail(self, gw, dev, online):
        """Mark a device available/unavailable on the supervisor's HA broker.

        Retained state topic jest WSPÓŁDZIELONY z pub_device_state. Goły {available}
        full-replace kasował last_seen/temp/humi/batt (dashboard pokazywał '--' i tracił
        wartości). FIX: merge `available` w ostatni opublikowany stan — zachowaj capy."""
        avail = "ON" if online else "OFF"
        fields = dict(self._dev_state.get((gw, dev), {}))
        fields['available'] = avail
        self._dev_state[(gw, dev)] = fields
        gl, safe = gw.lower(), self._safe(dev)
        self.mqtt.publish(
            f"{STATE_PREFIX}/{gl}/{safe}/state",
            json.dumps(fields, separators=(',', ':')), retain=True)

    def seed_device_cache(self, gw, dev, fields):
        """Zasiej cache stanu z RETAINED (hydratacja startowa) — by pierwsza zmiana
        availability po restarcie nie skasowała capów/last_seen zanim przyjdzie pierwszy `b`."""
        if isinstance(fields, dict) and fields:
            self._dev_state.setdefault((gw, dev), dict(fields))

    def pub_device_state(self, gw, dev, fields):
        """Merge-ish publish of a device state JSON (step2: full replace)."""
        self._dev_state[(gw, dev)] = dict(fields)       # cache dla pub_device_avail (zachowanie capów)
        gl, safe = gw.lower(), self._safe(dev)
        self.mqtt.publish(
            f"{STATE_PREFIX}/{gl}/{safe}/state",
            json.dumps(fields, separators=(',', ':')), retain=True)

    # ── Virtual I/O entities ────────────────────────────
    def reg_vswitch(self, gw, vid, name, value=0):
        key = f"vsw_{gw}_{vid}"
        if key in self._registered:
            return
        gl = gw.lower()
        safe = self._safe(vid)
        di = {"identifiers": [f"lora_vio_{gl}"],
              "name": f"LoRa Virtual I/O {gw}",
              "model": "Virtual I/O", "manufacturer": "LoRa Gateway"}
        self._pub("switch", f"lora_{gl}_vsw_{safe}", {
            "name": f"LoRa {name}", "object_id": f"lora_{gl}_vsw_{safe}",
            "unique_id": f"lora_{gl}_vsw_{safe}",
            "state_topic": f"{STATE_PREFIX}/{gl}/vio/{safe}/state",
            "command_topic": f"{STATE_PREFIX}/{gl}/vio/{safe}/set",
            "payload_on": "ON", "payload_off": "OFF",
            "state_on": "ON", "state_off": "OFF",
            "device": di, "icon": "mdi:toggle-switch"})
        self.mqtt.publish(f"{STATE_PREFIX}/{gl}/vio/{safe}/state",
                          "ON" if value else "OFF", retain=True)
        self._registered.add(key)
        if self.log:
            self.log.info('HA', f'🔘 VSwitch {vid} ({name}) = {"ON" if value else "OFF"}')

    def reg_vbutton(self, gw, vid, name):
        key = f"vbtn_{gw}_{vid}"
        if key in self._registered:
            return
        gl = gw.lower()
        safe = self._safe(vid)
        di = {"identifiers": [f"lora_vio_{gl}"],
              "name": f"LoRa Virtual I/O {gw}",
              "model": "Virtual I/O", "manufacturer": "LoRa Gateway"}
        self._pub("button", f"lora_{gl}_vbtn_{safe}", {
            "name": f"LoRa {name}", "object_id": f"lora_{gl}_vbtn_{safe}",
            "unique_id": f"lora_{gl}_vbtn_{safe}",
            "command_topic": f"{STATE_PREFIX}/{gl}/vio/{safe}/press",
            "device": di, "icon": "mdi:gesture-tap-button"})
        self._registered.add(key)
        if self.log:
            self.log.info('HA', f'🔘 VButton {vid} ({name})')

    # ── Device entities (from discovery) ────────────────
    def reg_sensor(self, gw, dev, caps):
        key = f"{gw}_{dev}"
        if key in self._registered:
            return
        gl, safe = gw.lower(), self._safe(dev)
        di = self._dev_device(gw, dev, "Sensor")
        st = f"{STATE_PREFIX}/{gl}/{safe}/state"
        cap_map = {'t': ('temp', 'temperature', '°C'), 'h': ('humi', 'humidity', '%'),
                   'b': ('batt', 'battery', '%')}
        for c in caps:
            if c in cap_map:
                eid, name, unit = cap_map[c]
                uid = f"lora_{gl}_{safe}_{eid}"
                self._pub("sensor", uid, {
                    "name": f"{dev} {name.title()}", "object_id": uid, "unique_id": uid,
                    "state_topic": st,
                    "value_template": f"{{{{ value_json.{name} | default('') }}}}",
                    "unit_of_measurement": unit, "device": di})
        uid_ls = f"lora_{gl}_{safe}_last_seen"
        self._pub("sensor", uid_ls, {
            "name": f"{dev} Last Seen", "object_id": uid_ls, "unique_id": uid_ls,
            "state_topic": st, "value_template": "{{ value_json.last_seen | default('--') }}",
            "device": di, "icon": "mdi:clock-outline"})
        uid_av = f"lora_{gl}_{safe}_available"
        self._pub("binary_sensor", uid_av, {
            "name": f"{dev} Available", "object_id": uid_av, "unique_id": uid_av,
            "state_topic": st, "value_template": "{{ value_json.available | default('OFF') }}",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "connectivity", "device": di})
        self._registered.add(key)

    def reg_binary(self, gw, dev, caps):
        key = f"{gw}_{dev}"
        if key in self._registered:
            return
        gl, safe = gw.lower(), self._safe(dev)
        di = self._dev_device(gw, dev, "Binary Sensor")
        st = f"{STATE_PREFIX}/{gl}/{safe}/state"
        cap_map = {'c': ('contact', 'contact'), 'o': ('occupancy', 'occupancy'),
                   'w': ('water_leak', 'moisture'), 'k': ('smoke', 'smoke')}
        for c in caps:
            if c in cap_map:
                name, dc = cap_map[c]
                uid = f"lora_{gl}_{safe}_{name}"
                self._pub("binary_sensor", uid, {
                    "name": f"{dev} {name.replace('_',' ').title()}", "object_id": uid,
                    "unique_id": uid, "state_topic": st,
                    "value_template": f"{{{{ 'ON' if value_json.{name} else 'OFF' }}}}",
                    "device_class": dc, "device": di})
        if 'b' in caps:
            uid = f"lora_{gl}_{safe}_batt"
            self._pub("sensor", uid, {
                "name": f"{dev} Battery", "object_id": uid, "unique_id": uid,
                "state_topic": st, "value_template": "{{ value_json.battery | default('') }}",
                "unit_of_measurement": "%", "device_class": "battery", "device": di})
        uid_av = f"lora_{gl}_{safe}_available"
        self._pub("binary_sensor", uid_av, {
            "name": f"{dev} Available", "object_id": uid_av, "unique_id": uid_av,
            "state_topic": st, "value_template": "{{ value_json.available | default('OFF') }}",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "connectivity", "device": di})
        self._registered.add(key)

    def reg_switch_dev(self, gw, dev):
        key = f"{gw}_{dev}"
        if key in self._registered:
            return
        gl, safe = gw.lower(), self._safe(dev)
        di = self._dev_device(gw, dev, "Switch")
        st = f"{STATE_PREFIX}/{gl}/{safe}/state"
        uid = f"lora_{gl}_{safe}"
        self._pub("switch", uid, {
            "name": f"LoRa {dev}", "object_id": uid, "unique_id": f"{uid}_switch",
            "state_topic": st, "command_topic": f"{STATE_PREFIX}/{gl}/{safe}/set",
            "value_template": "{{ value_json.state }}",
            "state_on": "ON", "state_off": "OFF",
            "payload_on": "ON", "payload_off": "OFF", "device": di})
        # availability + last_seen (dashboard: switch online/offline badge + last_seen)
        uid_av = f"lora_{gl}_{safe}_available"
        self._pub("binary_sensor", uid_av, {
            "name": f"{dev} Available", "object_id": uid_av, "unique_id": uid_av,
            "state_topic": st, "value_template": "{{ value_json.available | default('OFF') }}",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "connectivity", "device": di})
        uid_ls = f"lora_{gl}_{safe}_last_seen"
        self._pub("sensor", uid_ls, {
            "name": f"{dev} Last Seen", "object_id": uid_ls, "unique_id": uid_ls,
            "state_topic": st, "value_template": "{{ value_json.last_seen | default('--') }}",
            "icon": "mdi:clock-outline", "device": di})
        self._registered.add(key)

    # ── Gateway-local stats panel (link do supervisora + sync czasu) ──
    def reg_gw_local_stats(self, gw):
        """Panel statystyk bramki na JEJ lokalnym HA, z lora/<gl>/gwstat."""
        key = f"gwstats_{gw}"
        if key in self._registered:
            return
        gl, di = gw.lower(), self._gw_device(gw)
        st = f"{STATE_PREFIX}/{gl}/gwstat"
        sensors = [
            ("gw_uptime", "Uptime", "{{ value_json.uptime | default(0) }}", "mdi:timer", "s", "sensor"),
            ("gw_monitored", "Monitored", "{{ value_json.monitored | default(0) }}", "mdi:eye", None, "sensor"),
            ("gw_last_hb", "Last HB", "{{ value_json.last_hb | default('--') }}", "mdi:heart-pulse", None, "sensor"),
            # FIX 2026-07-07 (audyt): kafel OFFLINE dashboardu bramki czytał podzbiór urządzeń
            # z generatora (pokazywał 2 vs realnych ~174) — autorytatywna encja z gwstat.offline
            # (liczona z data._alive w publish_gwstat). HA nada entity_id z name → sensor.gw_g2_offline.
            ("gw_offline", "Offline", "{{ value_json.offline | default(0) }}", "mdi:lan-disconnect", None, "sensor"),
            ("sup_link", "Supervisor Link", "{{ value_json.sup_link | default('OFF') }}", None, None, "binary_sensor"),
            ("sup_last_rx", "Supervisor Last RX", "{{ value_json.sup_last_rx | default('--') }}", "mdi:download-network", None, "sensor"),
            ("sup_lost_pong", "Sup Lost Pong", "{{ value_json.sup_lost_pong | default(0) }}", "mdi:sync-alert", None, "sensor"),
            ("time_offset", "Time Offset", "{{ value_json.time_offset | default('--') }}", "mdi:clock-alert", None, "sensor"),
            ("last_sync", "Last Sync", "{{ value_json.last_sync | default('--') }}", "mdi:clock-sync", None, "sensor"),
        ]
        # UWAGA: brak "device" celowo — z device HA włącza has_entity_name i ignoruje
        # object_id (→ entity_id manglowane). Bez device entity_id = object_id (deterministyczne).
        for eid, name, tpl, icon, unit, domain in sensors:
            uid = f"lora_{gl}_{eid}"
            if domain == "binary_sensor":
                self._pub(domain, uid, {
                    "name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                    "state_topic": st, "value_template": tpl,
                    "payload_on": "ON", "payload_off": "OFF",
                    "device_class": "connectivity"})
            else:
                cfg = {"name": f"GW {gw} {name}", "object_id": uid, "unique_id": uid,
                       "state_topic": st, "value_template": tpl}
                if icon:
                    cfg["icon"] = icon
                if unit:
                    cfg["unit_of_measurement"] = unit
                self._pub(domain, uid, cfg)
        self._registered.add(key)
        if self.log:
            self.log.info('HA', f'🏠 {gw}: panel statystyk bramki (7 encji)')

    def pub_gw_stats(self, gw, data):
        self.mqtt.publish(f"{STATE_PREFIX}/{gw.lower()}/gwstat",
                          json.dumps(data, separators=(',', ':')), retain=True)

    @property
    def registered_count(self):
        return len(self._registered)
