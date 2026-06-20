#!/usr/bin/env python3
"""Naprawia ISTNIEJĄCE kafelki anomalii w sekcji BRAMKA G1 (dashboard supervisora):

1. Usuwa wcześniej wstrzyknięty przeze mnie panel `_lora_anomalies` (wracamy do
   kafelków usera).
2. W kaflach OFFLINE / LOW BATT / OTHER:
   - podmienia entity tile'a z liczników `devices_*` (źle: supervisorowy OfflineMonitor,
     pokazywał 4) na items-encje `*_anomalies` (state=count = prawda bramki = 2) →
     licznik zgadza się z listą.
   - naprawia entity_id w popupie markdown: user wpisał `sensor.lora_an_g1_*`
     (to OBJECT_ID, nie istnieje jako entity!), realny entity_id po munge HA to
     `sensor.lora_gateway_g1_gw_g1_*_anomalies` → dlatego lista była pusta.

Idempotentny. Token: env HA_SUP_TOKEN albo CONFIG['ha_api']['token'].
"""
import asyncio
import json
import os
import sys
import time

import websockets

HA = "ws://localhost:8123/api/websocket"
URL_PATH = None

# tile entity (stary licznik) → items-encja (count+items, prawda bramki)
TILE_SWAP = {
    "sensor.lora_gw_g1_devices_offline":     "sensor.lora_gateway_g1_gw_g1_offline_anomalies",
    "sensor.lora_gw_g1_devices_low_battery": "sensor.lora_gateway_g1_gw_g1_battery_anomalies",
    "sensor.lora_gw_g1_devices_anomaly":     "sensor.lora_gateway_g1_gw_g1_other_anomalies",
}
# popup markdown: błędny object_id → realny entity_id
POPUP_FIX = {
    "sensor.lora_an_g1_offline": "sensor.lora_gateway_g1_gw_g1_offline_anomalies",
    "sensor.lora_an_g1_battery": "sensor.lora_gateway_g1_gw_g1_battery_anomalies",
    "sensor.lora_an_g1_other":   "sensor.lora_gateway_g1_gw_g1_other_anomalies",
}


def _clean(cards):
    out = []
    for c in cards:
        if isinstance(c, dict):
            if c.get("_lora_anomalies"):          # usuń mój wstrzyknięty panel
                continue
            if isinstance(c.get("cards"), list):
                c["cards"] = _clean(c["cards"])
        out.append(c)
    return out


def _swap_tiles(card, stats):
    """Rekurencyjnie: podmień entity tile'a anomalii na items-encję."""
    if isinstance(card, dict):
        e = card.get("entity")
        if e in TILE_SWAP:
            card["entity"] = TILE_SWAP[e]
            stats["tiles"] += 1
        for c in (card.get("cards") or []):
            _swap_tiles(c, stats)
    return card


def _token():
    t = os.environ.get("HA_SUP_TOKEN")
    if t:
        return t
    try:
        sys.path.insert(0, os.path.expanduser("~/meshtastic"))
        from config import CONFIG
        return (CONFIG.get("ha_api") or {}).get("token", "REPLACE_ME")
    except Exception:
        return "REPLACE_ME"


async def main():
    token = _token()
    if not token or token == "REPLACE_ME":
        print("BRAK TOKENU"); return
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [0]

        async def call(msg):
            mid[0] += 1; msg["id"] = mid[0]
            await ws.send(json.dumps(msg))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0] and r.get("type") == "result":
                    return r

        cfg = (await call({"type": "lovelace/config", "url_path": URL_PATH})).get("result")
        if not cfg:
            print("brak config"); return
        json.dump(cfg, open("/tmp/lovelace_sup_fix_%d.json" % int(time.time()), "w",
                            encoding="utf-8"), ensure_ascii=False)
        # 1) usuń mój panel
        for v in cfg.get("views", []):
            v["cards"] = _clean(v.get("cards", []))
        # 2) popup entity fix — bezpieczne string-replace na całym configu (object_id→entity_id)
        blob = json.dumps(cfg, ensure_ascii=False)
        fixed = 0
        for bad, good in POPUP_FIX.items():
            cnt = blob.count("'" + bad + "'") + blob.count('"' + bad + '"')
            blob = blob.replace(bad, good)
            fixed += cnt
        cfg = json.loads(blob)
        # 3) tile entity swap
        stats = {"tiles": 0}
        for v in cfg.get("views", []):
            for c in v.get("cards", []):
                _swap_tiles(c, stats)
        r = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", r.get("success"), r.get("error", ""))
        print(f"popup entity_id naprawione: {fixed} wystąpień | tile entity swap: {stats['tiles']}")


if __name__ == "__main__":
    asyncio.run(main())
