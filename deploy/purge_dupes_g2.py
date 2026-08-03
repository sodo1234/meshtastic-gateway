#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""C1: skasuj RETAINED konfiguracje duplikatów encji anomalii na brokerze bramki.

Kod przestał je rejestrować (modules/anomaly/ha_publish.py, 2026-08-03), ale HA trzyma encje
dopóki na brokerze leży ich retained config — MQTT discovery kasuje się PUSTYM payloadem.

Co znika: `lora_gw_<gl>_devices_{offline,low_battery,anomaly}` — encje z nazwą „… #" i entity_id
z sufiksem `_2`. Miały ten sam state_topic i tę samą wartość co `lora_an_<gl>_{bucket}`, tylko
bez listy w atrybutach. Dashboard używa tych z listą, więc kasowanie duplikatów jest bezpieczne.

Uruchamiać NA BRAMCE:  venv/bin/python deploy/purge_dupes_g2.py [gl]     (domyślnie g2)
"""
import sys
import time

sys.path.insert(0, '/home/td/meshtastic')
import config                                                    # noqa: E402
import paho.mqtt.client as mqtt                                  # noqa: E402

GL = (sys.argv[1] if len(sys.argv) > 1 else 'g2').lower()
DUPES = [f"lora_gw_{GL}_devices_offline",
         f"lora_gw_{GL}_devices_low_battery",
         f"lora_gw_{GL}_devices_anomaly"]
KEEP = [f"lora_an_{GL}_offline", f"lora_an_{GL}_battery", f"lora_an_{GL}_other"]

m = config.CONFIG['mqtt']
c = mqtt.Client(client_id=f'purge-dupes-{GL}')
c.username_pw_set(m['user'], m['pass'])
c.connect(m['host'], m['port'], 20)
for uid in DUPES:
    c.publish(f"homeassistant/sensor/{uid}/config", payload=b'', retain=True)   # '' = usuń encję
    print(f"🗑️  skasowany retained config: {uid}")
for _ in range(20):
    c.loop(0.1)
time.sleep(1)
c.disconnect()
print(f"✅ zostają (z listą do popupu): {', '.join(KEEP)}")
print("   Sprawdź w HA bramki: encje '… #' / '_2' powinny zniknąć.")
