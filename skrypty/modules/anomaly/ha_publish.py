"""AnomalyBucketPublisher — publikuje anomalie bramki na LOKALNY HA w formacie 1:1 jak
supervisor (reg_anomaly_entities + publish_anomalies): kubełki offline/battery/other
(count + items z NAZWAMI) + liczniki devices_{offline,low_battery,anomaly}.

Bramka = source of truth anomalii → jej HA ma WŁASNĄ zakładkę anomalii identyczną jak
supervisor, na lokalnych encjach. Nazwy urządzeń pochodzą z AnomalyStore.items() (resolve_dev
= discovery.short_rev → nazwa). Format encji/topiców taki sam jak w run_supervisor, więc karty
dashboardu (DASH) działają bez zmian.

Topiki:  {sp}/gw/{gl}/an_{bucket}  = {"count":N,"items":[{dev,gw,type,value,detected_at,since}]}
Encje:   sensor.lora_an_{gl}_{offline,battery,other}         (state=count, attr.items)
         sensor.lora_gw_{gl}_devices_{offline,low_battery,anomaly}   (state=count)
"""
import json

# (bucket, nazwa PL, ikona, licznik devices_*)
_BUCKETS = [
    ('offline', 'Anomalie Offline', 'mdi:lan-disconnect', 'devices_offline'),
    ('battery', 'Anomalie Bateria', 'mdi:battery-alert', 'devices_low_battery'),
    ('other',   'Anomalie Inne',    'mdi:alert-circle',  'devices_anomaly'),
]


class AnomalyBucketPublisher:
    def __init__(self, store, mqtt, ha_prefix, state_prefix, logger=None):
        self.store = store              # AnomalyStore (lokalny, bramki)
        self.mqtt = mqtt
        self.hp = ha_prefix             # 'homeassistant'
        self.sp = state_prefix          # 'lora'
        self.log = logger
        self._regd = set()
        self._ctrl_regd = set()         # bramki z zarejestrowanymi przyciskami clear/diag
        self._diag_regd = set()         # bramki z zarejestrowaną encją diagnostyki

    def register(self, gw):
        if gw in self._regd:
            return
        gl = gw.lower(); di = {"identifiers": [f"lora_gateway_{gl}"]}
        for bucket, nm, icon, cnt in _BUCKETS:
            topic = f"{self.sp}/gw/{gl}/an_{bucket}"
            iuid = f"lora_an_{gl}_{bucket}"                # kubełek (items → popup/lista)
            self.mqtt.publish(f"{self.hp}/sensor/{iuid}/config", json.dumps({
                "name": f"GW {gw} {nm}", "object_id": iuid, "unique_id": iuid,
                "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                "json_attributes_topic": topic, "icon": icon, "device": di},
                separators=(',', ':')), retain=True)
            cuid = f"lora_gw_{gl}_{cnt}"                   # licznik devices_*
            self.mqtt.publish(f"{self.hp}/sensor/{cuid}/config", json.dumps({
                "name": f"GW {gw} {nm} #", "object_id": cuid, "unique_id": cuid,
                "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        self._regd.add(gw)
        if self.log:
            self.log.info('ANOM', f'📋 encje anomalii bramki {gw} zarejestrowane (offline/battery/other + devices_*)')

    def publish(self, gw):
        self.register(gw)
        gl = gw.lower()
        for bucket, _n, _i, _c in _BUCKETS:
            items = self.store.items(gw, bucket)
            self.mqtt.publish(f"{self.sp}/gw/{gl}/an_{bucket}",
                              json.dumps({"count": len(items), "items": items},
                                         separators=(',', ':')), retain=True)

    def register_controls(self, gw):
        """Przyciski na HA BRAMKI (1:1 jak supervisor): Clear All per-kubełek + Diagnostyka anomalii.
        command_topic → {sp}/gw/{gl}/cmd/<cid> (harness subskrybuje i obsługuje)."""
        if gw in self._ctrl_regd:
            return
        gl = gw.lower(); di = {"identifiers": [f"lora_gateway_{gl}"]}
        for cid, nm, icon in [
                ('clear_offline', 'Clear Offline', 'mdi:lan-disconnect'),
                ('clear_battery', 'Clear Battery', 'mdi:battery-alert'),
                ('clear_other',   'Clear Other',   'mdi:alert-circle'),
                ('anomaly_diag',  'Diagnostyka anomalii', 'mdi:stethoscope')]:
            uid = f"lora_{gl}_{cid}"
            self.mqtt.publish(f"{self.hp}/button/{uid}/config", json.dumps({
                "name": f"GW {gw} {nm}", "object_id": uid, "unique_id": uid,
                "command_topic": f"{self.sp}/gw/{gl}/cmd/{cid}",
                "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        self._ctrl_regd.add(gw)
        if self.log:
            self.log.info('ANOM', f'🎛️ przyciski anomalii bramki {gw} (clear offline/battery/other + diagnostyka)')

    def publish_diag(self, gw, report):
        """Wynik diagnostyki AnomalyReconciler → encja HA bramki (state=liczba rozbieżności,
        attr: truth/store/missing/stale/muted/rearmed)."""
        gl = gw.lower(); di = {"identifiers": [f"lora_gateway_{gl}"]}
        uid = f"lora_{gl}_anomaly_diag"
        topic = f"{self.sp}/gw/{gl}/anomaly_diag"
        if gw not in self._diag_regd:
            self.mqtt.publish(f"{self.hp}/sensor/{uid}/config", json.dumps({
                "name": f"GW {gw} Diagnostyka anomalii", "object_id": uid, "unique_id": uid,
                "state_topic": topic, "value_template": "{{ value_json.divergence | default(0) }}",
                "json_attributes_topic": topic, "icon": "mdi:stethoscope", "device": di},
                separators=(',', ':')), retain=True)
            self._diag_regd.add(gw)
        payload = {"divergence": len(report.get('missing', [])) + len(report.get('stale', [])),
                   "truth": report.get('truth_n'), "store": report.get('store_n'),
                   "blob_hash": report.get('blob_hash'), "snapshot_sent": report.get('snapshot_sent'),
                   "missing": report.get('missing'), "stale": report.get('stale'),
                   "muted": report.get('muted'), "rearmed": report.get('rearmed'),
                   "ts": report.get('ts')}
        self.mqtt.publish(topic, json.dumps(payload, separators=(',', ':')), retain=True)
