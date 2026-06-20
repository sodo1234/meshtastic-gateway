#!/usr/bin/env python3
"""Przebudowuje POPUP w 3 istniejących kaflach anomalii (OFFLINE/LOW BATT/OTHER) na
dashboardzie supervisora: dynamiczna lista, WIERSZ = jedna anomalia, KLIK w wiersz =
clear TEJ anomalii (mqtt → lora/supervisor/cmd/clear_anomaly {gw,dev,bucket}).

Wzorzec: stała liczba N wierszy-button-cardów; każdy czyta items[i] z encji; pusty →
ukryty (height 0). Timestamp data+godzina (fallback since→detected_at). Kolory/czcionki
spójne z dashboardem (#0a0a0a/#e5e5e5/#525252). Zachowuje przycisk clear-all u dołu.

Idempotentny po (entity, bucket). Token: env HA_SUP_TOKEN albo CONFIG['ha_api']['token'].
"""
import asyncio
import json
import os
import sys
import time

import websockets

HA = "ws://localhost:8123/api/websocket"
URL_PATH = None
N_ROWS = 10                                   # max wierszy w popupie (pusty → ukryty)

# tile entity → (bucket, tytuł, kolor, clear-all button)
TILES = {
    "sensor.lora_gateway_g1_gw_g1_offline_anomalies":
        ("offline", "OFFLINE", "#f87171", "button.lora_supervisor_clear_offline"),
    "sensor.lora_gateway_g1_gw_g1_battery_anomalies":
        ("battery", "BATERIA", "#fbbf24", "button.lora_supervisor_clear_battery"),
    "sensor.lora_gateway_g1_gw_g1_other_anomalies":
        ("other", "INNE", "#22d3ee", "button.lora_supervisor_clear_other"),
}
TYPE_LABELS = {"offline": "Offline", "do": "Offline", "lb": "Niska bateria",
               "cb": "Krytyczna bateria", "sg": "Stagnacja", "low_battery": "Niska bateria",
               "critical_battery": "Krytyczna bateria", "stagnation": "Stagnacja",
               "temp_high": "Temp. wysoka", "temp_low": "Temp. niska", "water_leak": "Wyciek",
               "smoke": "Dym", "hum_high": "Wilg. wysoka", "hum_low": "Wilg. niska"}


def _row(eid, i, bucket, color):
    """Button-card wiersz i: items[i] → dev · typ · data godz; klik → clear tej anomalii."""
    L = json.dumps(TYPE_LABELS, ensure_ascii=False)
    content = (
        "[[[ "
        "var items=(states['" + eid + "'].attributes.items)||[];"
        "var it=items[" + str(i) + "];"
        "if(!it) return '';"
        "var L=" + L + ";"
        "var typ=L[it.type]||it.type||'';"
        "var ts=it.detected_at?new Date(it.detected_at):(it.since?new Date(it.since*1000):null);"
        "var tss=ts?(('0'+ts.getDate()).slice(-2)+'.'+('0'+(ts.getMonth()+1)).slice(-2)+' '+"
        "('0'+ts.getHours()).slice(-2)+':'+('0'+ts.getMinutes()).slice(-2)):'—';"
        "return `<div style=\"display:flex;justify-content:space-between;align-items:center;width:100%;\">"
        "<div style=\"display:flex;flex-direction:column;gap:2px;\">"
        "<span style=\"color:#e5e5e5;font-weight:700;font-size:13px;\">${it.dev}</span>"
        "<span style=\"color:#737373;font-size:10px;\">${it.gw} · " + "${typ}" + "</span></div>"
        "<div style=\"display:flex;align-items:center;gap:10px;\">"
        "<span style=\"color:#525252;font-size:10px;white-space:nowrap;\">${tss}</span>"
        "<span style=\"color:" + color + ";font-size:14px;font-weight:800;\">✕</span></div></div>`; ]]]")
    # height 0 gdy brak items[i] (ukrycie pustych wierszy)
    height = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
              "return it?'auto':'0px'; ]]]")
    pad = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
           "return it?'10px 12px':'0px'; ]]]")
    border = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
              "return it?'1px solid #1a1a1a':'none'; ]]]")
    # payload clear: {gw,dev,bucket} z items[i]
    payload = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
               "return it?JSON.stringify({gw:it.gw,dev:it.dev,bucket:'" + bucket + "'}):''; ]]]")
    return {
        "type": "custom:button-card", "entity": eid, "show_icon": False,
        "show_name": False, "show_state": False,
        "custom_fields": {"content": content},
        "tap_action": {"action": "call-service", "service": "mqtt.publish",
                       "service_data": {"topic": "lora/supervisor/cmd/clear_anomaly",
                                        "payload": payload}},
        "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"},
                            {"border-bottom": border}, {"border-radius": "0"},
                            {"padding": pad}, {"height": height}, {"overflow": "hidden"}],
                   "custom_fields": {"content": [{"width": "100%"}]}}}


def _popup_content(eid, bucket, title, color, clear_btn):
    rows = [_row(eid, i, bucket, color) for i in range(N_ROWS)]
    empty = {"type": "custom:button-card", "entity": eid, "show_icon": False,
             "show_name": False, "show_state": False,
             "custom_fields": {"content": (
                 "[[[ var n=(states['" + eid + "'].attributes.items||[]).length;"
                 "return n>0?'':'<div style=\"color:#4ade80;font-size:13px;padding:14px;"
                 "text-align:center;\">Brak anomalii w tej kategorii.</div>'; ]]]")},
             "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"},
                                 {"border": "none"}, {"padding": "0"}]}}
    clear_all = {"type": "custom:button-card", "template": "lora_btn",
                 "name": f"WYCZYŚĆ WSZYSTKIE {title}", "icon": "mdi:close-circle",
                 "tap_action": {"action": "call-service",
                                "confirmation": {"text": f"Wyczyścić wszystkie {title}?"},
                                "service": "button.press",
                                "service_data": {"entity_id": clear_btn}},
                 "styles": {"card": [{"border": f"1px solid {color}55"}, {"margin-top": "8px"}],
                            "icon": [{"color": color}], "name": [{"color": color}]}}
    return {"type": "vertical-stack", "cards": [empty] + rows + [clear_all]}


def _patch(card):
    """Jeśli kafel ma entity z TILES → podmień popup content na klikalne wiersze."""
    if not isinstance(card, dict):
        return
    e = card.get("entity")
    if e in TILES and card.get("tap_action", {}).get("action") == "fire-dom-event":
        bucket, title, color, cb = TILES[e]
        data = card["tap_action"].setdefault("browser_mod", {}).setdefault("data", {})
        data["title"] = f"Anomalie — {title}"
        data["content"] = _popup_content(e, bucket, title, color, cb)
        data["dismissable"] = True
        data.setdefault("style", {"--popup-min-width": "460px",
                                  "--popup-background-color": "#0a0a0a",
                                  "--popup-border-radius": "12px"})
        return True
    for c in (card.get("cards") or []):
        _patch(c)


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
        json.dump(cfg, open("/tmp/lovelace_sup_popup_%d.json" % int(time.time()), "w",
                            encoding="utf-8"), ensure_ascii=False)
        cnt = [0]

        def walk(cards):
            for c in cards:
                if _patch(c):
                    cnt[0] += 1
                if isinstance(c, dict) and isinstance(c.get("cards"), list):
                    walk(c["cards"])
        for v in cfg.get("views", []):
            walk(v.get("cards", []))
        r = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", r.get("success"), r.get("error", ""), "| popupy przebudowane:", cnt[0])


if __name__ == "__main__":
    asyncio.run(main())
