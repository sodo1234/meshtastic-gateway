"""AnomalyHAv10Bridge — most: step5 AnomalyStore → encje HA per-anomalia (model v10).

Powód: dashboard v10 (auto-entities + template-entity-row) działa lepiej i ma niezawodny
clear-per-wiersz, bo KAŻDA anomalia to osobna para encji HA:
  - sensor.lora_an_<safe>        (state = wartość | 'active'; json_attributes: dev/gw/type/detected_at)
  - button.lora_an_<safe>_clear  (command_topic {sp}/anomaly/<safe>/clear → usuń tę anomalię)
gdzie <safe> = slug("{gw}_{dev}_{atype}") np. g1_temp_1_temp_high. auto-entities filtruje
button.lora_an_<gl>_*_<atype>_clear → encja reaktywna: po clear znika encja → znika wiersz
(także przy AUTO-CLEAR: recovery dn/to/bo kasuje wpis w store → reconcile usuwa encję).

Backend step5 (detekcja/transport/LoRa-batching/redundancja) NIETKNIĘTY — to wyłącznie warstwa
publikacji encji, spinana w `on_change` store'a + obsługa command_topic clear w dispatchu harnessu.

Liczniki devices_offline/_low_battery/_anomaly i przyciski clear-all już istnieją w step5 —
ten most ich NIE rejestruje (zero kolizji object_id). Reconcyliacja idempotentna per bramka.
"""
import json
import time

# step5 store code → atype v10 (sufiks encji + 'type' dla dashboardu)
CODE_ATYPE = {
    "do": "device_offline",
    "lb": "low_battery", "cb": "critical_battery",
    "th": "temp_high", "tl": "temp_low",
    "hh": "hum_high", "hl": "hum_low",
    "sg": "stagnation", "sk": "smoke", "wl": "water_leak",
}
ICON = {
    "low_battery": "mdi:battery-low", "critical_battery": "mdi:battery-alert",
    "device_offline": "mdi:lan-disconnect", "temp_high": "mdi:thermometer-high",
    "temp_low": "mdi:thermometer-low", "stagnation": "mdi:timer-sand",
    "hum_high": "mdi:water-percent", "hum_low": "mdi:water-percent-alert",
    "smoke": "mdi:smoke-detector", "water_leak": "mdi:water-alert",
}


def _safe(s):
    return str(s).replace(" ", "_").lower()


class AnomalyHAv10Bridge:
    def __init__(self, store, mqtt, ha_prefix, state_prefix, logger=None):
        self.store = store              # AnomalyStore (anomalies: {(gw,dev,cat): {code,value,ts}})
        self.mqtt = mqtt
        self.hp = ha_prefix             # 'homeassistant'
        self.sp = state_prefix          # 'lora'
        self.log = logger
        self.published = {}             # safe_id → (gw, dev, cat)  — encje aktualnie wystawione

    # ── publikacja encji ────────────────────────────────
    def _reg_anomaly(self, safe_id, gw, dev, atype):
        st = f"{self.sp}/anomaly/{safe_id}"
        self.mqtt.publish(f"{self.hp}/sensor/lora_an_{safe_id}/config", json.dumps({
            "name": f"{gw} | {dev} | {atype}", "object_id": f"lora_an_{safe_id}",
            "unique_id": f"lora_an_{safe_id}", "state_topic": st,
            "value_template": "{{ value_json.value if value_json.value else 'active' }}",
            "json_attributes_topic": st, "icon": ICON.get(atype, "mdi:alert")},
            separators=(",", ":")), retain=True)
        self.mqtt.publish(f"{self.hp}/button/lora_an_{safe_id}_clear/config", json.dumps({
            "name": f"Clear {dev}", "object_id": f"lora_an_{safe_id}_clear",
            "unique_id": f"lora_an_{safe_id}_clear",
            "command_topic": f"{st}/clear", "icon": "mdi:close-circle"},
            separators=(",", ":")), retain=True)

    def _remove_entity(self, safe_id):
        for t in (f"{self.hp}/sensor/lora_an_{safe_id}/config",
                  f"{self.hp}/button/lora_an_{safe_id}_clear/config",
                  f"{self.sp}/anomaly/{safe_id}"):
            self.mqtt.publish(t, "", retain=True)

    def publish(self, gw):
        """Reconcyliacja encji per-anomalia dla jednej bramki ze stanu store (add/update/remove)."""
        desired = {}                    # safe_id → (dev, atype, value, ts)
        for (g, dev, cat), a in list(self.store.anomalies.items()):
            if g != gw:
                continue
            atype = CODE_ATYPE.get(a.get("code"), a.get("code") or "other")
            safe_id = _safe(f"{gw}_{dev}_{atype}")
            desired[safe_id] = (dev, atype, a.get("value"), a.get("ts"), cat)
        # usuń encje tej bramki, których już nie ma (clear/auto-clear/zmiana atype np. lb→cb)
        for safe_id, (pg, _pdev, _pcat) in list(self.published.items()):
            if pg == gw and safe_id not in desired:
                self._remove_entity(safe_id)
                del self.published[safe_id]
        # dodaj/aktualizuj
        for safe_id, (dev, atype, value, ts, cat) in desired.items():
            if safe_id not in self.published:
                self._reg_anomaly(safe_id, gw, dev, atype)
                self.published[safe_id] = (gw, dev, cat)
            detected_at = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else "")
            self.mqtt.publish(f"{self.sp}/anomaly/{safe_id}", json.dumps({
                "id": safe_id, "gw": gw, "dev": dev, "type": atype,
                "value": value, "detected_at": detected_at},
                separators=(",", ":")), retain=True)

    # ── clear (klik wiersza → command_topic) ────────────
    def lookup(self, safe_id):
        """safe_id z topicu {sp}/anomaly/<safe_id>/clear → (gw, dev, cat) lub None."""
        return self.published.get(safe_id)

    @staticmethod
    def safe_from_clear_topic(topic):
        """'{sp}/anomaly/<safe_id>/clear' → '<safe_id>' (lub None)."""
        parts = topic.split("/")
        if len(parts) >= 3 and parts[-1] == "clear" and parts[-3] == "anomaly":
            return parts[-2]
        return None
