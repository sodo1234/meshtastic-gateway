#!/usr/bin/env python3
"""Jednorazowy fix: usuń OSIEROCONE wpisy rejestru encji HA dla anomalii per-dev ze starszej
wersji (v10/v23/v38), które zamroziły zły entity_id (sensor.g1_*, button.clear_*) pod tym samym
unique_id co nowy most v10 (object_id lora_an_<safe>). HA dopasowuje po unique_id → trzyma stary
entity_id → dashboard (filtr button.lora_an_g1_*_clear) ich nie łapie.

Krok 1: zbierz retained discovery configi lora_an_g1 z brokera.
Krok 2: WS — usuń z rejestru wpisy gdzie unique_id startswith 'lora_an_g1_' (poza agregatami
        offline/battery/other) a entity_id NIE jest sensor./button.lora_an_g1_* (= stale).
Krok 3: republish zebrane configi → HA odtwarza encje ze świeżym entity_id z object_id.

Uruchom NA supervisorze (czyta config + lokalny broker/HA). Idempotentny.
"""
import asyncio
import json
import time

import paho.mqtt.client as mqtt
import websockets

from config import CONFIG

AGGREGATES = {"lora_an_g1_offline", "lora_an_g1_battery", "lora_an_g1_other"}


def collect_configs():
    """{topic: payload_str} retained homeassistant/.../lora_an_g1_*/config (non-empty)."""
    m = CONFIG["mqtt"]
    got = {}

    def on_c(c, u, f, rc):
        c.subscribe("homeassistant/#")

    def on_m(c, u, msg):
        if "lora_an_g1" in msg.topic and msg.topic.endswith("/config") and msg.payload:
            got[msg.topic] = msg.payload.decode("utf-8")

    cl = mqtt.Client()
    if m.get("username") or m.get("user"):
        cl.username_pw_set(m.get("username") or m.get("user"), m.get("password") or m.get("pass"))
    cl.on_connect = on_c
    cl.on_message = on_m
    cl.connect(m["host"], m["port"], 10)
    cl.loop_start()
    time.sleep(4)
    cl.loop_stop()
    cl.disconnect()
    return got


def republish(configs):
    m = CONFIG["mqtt"]
    cl = mqtt.Client()
    if m.get("username") or m.get("user"):
        cl.username_pw_set(m.get("username") or m.get("user"), m.get("password") or m.get("pass"))
    cl.connect(m["host"], m["port"], 10)
    cl.loop_start()
    for topic, payload in configs.items():
        cl.publish(topic, "", retain=True)            # wyczyść (HA usuwa)
    time.sleep(1.0)
    for topic, payload in configs.items():
        cl.publish(topic, payload, retain=True)       # republish → HA odtwarza z object_id
    time.sleep(1.5)
    cl.loop_stop()
    cl.disconnect()


async def purge_stale():
    T = (CONFIG.get("ha_api") or {}).get("token", "")
    removed = []
    async with websockets.connect("ws://localhost:8123/api/websocket", max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": T}))
        await ws.recv()
        await ws.send(json.dumps({"id": 1, "type": "config/entity_registry/list"}))
        while True:
            r = json.loads(await ws.recv())
            if r.get("id") == 1:
                break
        mid = 1
        for e in r["result"]:
            uid = e.get("unique_id") or ""
            eid = e.get("entity_id") or ""
            if not uid.startswith("lora_an_g1_") or uid in AGGREGATES:
                continue
            if eid.startswith("sensor.lora_an_g1_") or eid.startswith("button.lora_an_g1_"):
                continue                              # już poprawny
            mid += 1
            await ws.send(json.dumps({"id": mid, "type": "config/entity_registry/remove",
                                      "entity_id": eid}))
            resp = None
            while True:
                resp = json.loads(await ws.recv())
                if resp.get("id") == mid:
                    break
            removed.append((eid, uid, resp.get("success")))
    return removed


def main():
    configs = collect_configs()
    print(f"zebrano {len(configs)} retained configów lora_an_g1")
    removed = asyncio.run(purge_stale())
    print(f"usunięto {len(removed)} osieroconych wpisów rejestru:")
    for eid, uid, ok in removed:
        print(f"  {'OK' if ok else 'FAIL'}  {eid}  (uid={uid})")
    if configs:
        republish(configs)
        print(f"republish {len(configs)} configów → HA odtwarza encje ze świeżym entity_id")


if __name__ == "__main__":
    main()
