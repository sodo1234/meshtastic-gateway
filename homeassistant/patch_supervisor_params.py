#!/usr/bin/env python3
"""Wstrzykuje panel PARAMETRY (6 pól + 2 przyciski Send) do dashboardu supervisora.

Pobiera live lovelace (WS), backup do /tmp, dodaje kartę PARAMETRY na początku
pierwszego widoku, usuwa martwą kartę v38 „Timeout (sekundy)" (encje
number.lora_timeout_* już nie istnieją w v39). Idempotentny (marker _lora_params).
Token: env HA_SUP_TOKEN albo CONFIG['ha_api']['token'].
"""
import asyncio
import json
import os
import sys
import time

import websockets

HA = "ws://localhost:8123/api/websocket"
URL_PATH = None  # domyślny dashboard

PARAM_FIELDS = [   # id zaktualizowane 2026-06-10 (przeniesione pod device LoRa Gateway G1)
    ("number.lora_gateway_g1_p1_stagnation_bateryjne",     "P1 · Stagnacja bateryjne [h]"),
    ("number.lora_gateway_g1_p2_stagnation_sieciowe",      "P2 · Stagnacja sieciowe [h]"),
    ("number.lora_gateway_g1_p3_raportowanie_temp",        "P3 · Raportowanie temp [min]"),
    ("number.lora_gateway_g1_t1_offline_switch_light",     "T1 · Offline switch/light [min]"),
    ("number.lora_gateway_g1_t2_offline_temp_hum",         "T2 · Offline temp/hum [min]"),
    ("number.lora_gateway_g1_t3_offline_door_leak_motion", "T3 · Offline door/leak [min]"),
]
SEND_CONFIG = "button.lora_gateway_g1_lora_wyslij_config"
SEND_TIMEOUT = "button.lora_gateway_g1_lora_wyslij_timeout"

ENT_STYLE = ("ha-card{background:#0a0a0a;border:1px solid #1f1f1f;border-radius:12px;"
             "box-shadow:none;padding:4px 6px;}"
             ".card-header{font-size:10px;font-weight:800;letter-spacing:2px;"
             "text-transform:uppercase;color:#525252;padding:10px 10px 6px 10px;}"
             "hui-number-entity-row,hui-generic-entity-row{padding:3px 6px;color:#e5e5e5;}"
             ".text-content{color:#a0a0a0;font-size:12px;}")


def _send_btn(name, eid, icon, color):
    return {"type": "custom:button-card", "name": name, "icon": icon, "show_state": False,
            "tap_action": {"action": "call-service", "service": "button.press",
                           "service_data": {"entity_id": eid}},
            "styles": {"card": [{"background": "#0a0a0a"}, {"border": f"1px solid {color}55"},
                                {"border-radius": "12px"}, {"box-shadow": "none"},
                                {"height": "58px"}, {"padding": "14px 12px"}],
                       "name": [{"font-size": "11px"}, {"font-weight": 800},
                                {"letter-spacing": "1.5px"}, {"text-transform": "uppercase"},
                                {"color": color}, {"white-space": "nowrap"},
                                {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
                       "icon": [{"color": color}, {"width": "20px"}]}}


def _param_card():
    return {"type": "vertical-stack", "_lora_params": True, "cards": [
        {"type": "entities", "title": "PARAMETRY (LoRa Gateway G1)",
         "show_header_toggle": False,
         "entities": [{"entity": e, "name": n} for e, n in PARAM_FIELDS],
         "card_mod": {"style": ENT_STYLE}},
        {"type": "horizontal-stack", "cards": [
            _send_btn("Wyślij Config", SEND_CONFIG, "mdi:cog-sync", "#4ade80"),
            _send_btn("Wyślij Timeout", SEND_TIMEOUT, "mdi:timer-cog", "#22d3ee")]},
    ]}


def _is_dead_timeout_card(card):
    """Karta v38 z encjami number.lora_timeout_* (martwe w v39)."""
    if not isinstance(card, dict):
        return False
    if str(card.get("title", "")).lower().startswith("timeout"):
        ents = card.get("entities") or []
        return any("lora_timeout" in str(e.get("entity", e)) for e in ents
                   if isinstance(e, (dict, str)))
    return False


def _clean(cards):
    """Usuń wcześniej wstrzyknięte (idempotency) i martwe karty timeout v38."""
    out = []
    for c in cards:
        if isinstance(c, dict):
            if c.get("_lora_params"):
                continue
            if _is_dead_timeout_card(c):
                continue
            if isinstance(c.get("cards"), list):
                c["cards"] = _clean(c["cards"])
        out.append(c)
    return out


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
        print("BRAK TOKENU (env HA_SUP_TOKEN)"); return
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
        bk = "/tmp/lovelace_sup_backup_%d.json" % int(time.time())
        json.dump(cfg, open(bk, "w", encoding="utf-8"), ensure_ascii=False)
        print("backup:", bk)
        views = cfg.get("views", [])
        print("views:", [v.get("title") for v in views])
        if not views:
            print("brak widoków"); return
        for v in views:                                   # czyść w każdym widoku
            v["cards"] = _clean(v.get("cards", []))
        # docelowy widok: preferuj „Config"/„Konfiguracja"/„Bramki", fallback = pierwszy
        pref = ["config", "konfiguracja", "bramki", "bramka"]
        target = next((v for p in pref for v in views
                       if str(v.get("title", "")).lower() == p), views[0])
        target.setdefault("cards", []).insert(0, _param_card())
        r = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", r.get("success"), r.get("error", ""))
        print(f"panel PARAMETRY → widok '{target.get('title')}' (góra)")


if __name__ == "__main__":
    asyncio.run(main())
