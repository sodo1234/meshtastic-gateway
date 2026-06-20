#!/usr/bin/env python3
"""Wstrzykuje panel ANOMALIE (kafelki licznikowe + popup z listą) do dashboardu supervisora.

STEP 5: encje `sensor.lora_gateway_g1_gw_g1_{offline,battery,other}_anomalies` mają
state=count + attr.items=[{gw,dev,type,value,since,detected_at}]. Ten patch dodaje
kompaktowe kafelki licznikowe — KLIK na kafelek → browser_mod popup z DYNAMICZNIE
generowaną listą urządzeń (bramka / nazwa / typ anomalii / czas wykrycia).

Wzorzec 1:1 z patch_supervisor_params.py: pull live lovelace (WS), backup do /tmp,
idempotentny clean po markerze `_lora_anomalies`, insert na górę widoku docelowego,
save. Token: env HA_SUP_TOKEN albo CONFIG['ha_api']['token']. Wymaga browser_mod.
"""
import asyncio
import json
import os
import sys
import time

import websockets

HA = "ws://localhost:8123/api/websocket"
URL_PATH = None  # domyślny dashboard

# (entity, tytuł, ikona, kolor) — komplet step5 dla G1
BUCKETS = [
    ("sensor.lora_gateway_g1_gw_g1_offline_anomalies", "OFFLINE", "mdi:lan-disconnect", "#f87171"),
    ("sensor.lora_gateway_g1_gw_g1_battery_anomalies", "BATERIA", "mdi:battery-alert", "#fbbf24"),
    ("sensor.lora_gateway_g1_gw_g1_other_anomalies",   "INNE",    "mdi:alert-circle",  "#22d3ee"),
]

# czytelne nazwy typów anomalii (kod → label)
TYPE_LABELS = {
    "offline": "Offline", "do": "Offline", "low_battery": "Niska bateria", "lb": "Niska bateria",
    "critical_battery": "Krytyczna bateria", "cb": "Krytyczna bateria", "stagnation": "Stagnacja",
    "sg": "Stagnacja", "temp_high": "Temp. wysoka", "temp_low": "Temp. niska",
    "hum_high": "Wilg. wysoka", "hum_low": "Wilg. niska", "water_leak": "Wyciek", "smoke": "Dym",
}


def _popup_md(eid, title):
    """Zawartość popupu = NATYWNY markdown + Jinja (bez custom:button-card, który w popupie
    bywa nie-renderowany). Dynamiczna tabela z attr.items: urządzenie / bramka / typ / czas."""
    lmap = "{" + ",".join(f"'{k}':'{v}'" for k, v in TYPE_LABELS.items()) + "}"
    tmpl = (
        "{% set L = " + lmap + " %}\n"
        "{% set items = state_attr('" + eid + "','items') %}\n"
        "{% if items %}\n"
        "| Urządzenie | Bramka | Typ | Wykryto |\n"
        "|---|---|---|---|\n"
        "{% for it in items %}"
        "| **{{ it.dev }}** | `{{ it.gw }}` | {{ L.get(it.type, it.type) }}"
        "{% if it.value not in [none, '', 0] %} ({{ it.value }}){% endif %} | "
        "{{ as_timestamp(it.detected_at) | timestamp_custom('%d.%m %H:%M') "
        "if it.detected_at else (it.since | timestamp_custom('%d.%m %H:%M') if it.since else '—') }} |\n"
        "{% endfor %}\n"
        "{% else %}\n_Brak anomalii w tej kategorii._\n{% endif %}"
    )
    return {"type": "markdown", "content": tmpl}


def _count_tile(eid, title, icon, color):
    """Kompaktowy kafelek licznika — klik → popup z listą (browser_mod)."""
    js = (
        "[[[ var e=states['" + eid + "'];var c=e?parseInt(e.state)||0:0;"
        "var col=c>0?'" + color + "':'#525252';"
        "return `<div style=\"display:flex;flex-direction:column;gap:4px;\">"
        "<span style=\"font-size:24px;font-weight:800;font-variant-numeric:tabular-nums;color:${col};\">${c}</span>"
        "<span style=\"font-size:9px;font-weight:700;letter-spacing:2px;color:#525252;\">" + title + "</span>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "entity": eid, "icon": icon,
            "show_name": False, "show_state": False, "show_icon": True,
            "custom_fields": {"content": js},
            "tap_action": {"action": "fire-dom-event", "browser_mod": {
                "service": "browser_mod.popup",
                "data": {"title": f"Anomalie — {title}", "content": _popup_md(eid, title),
                         "dismissable": True}}},
            "styles": {"card": [{"background": "#0a0a0a"}, {"border": "1px solid #1f1f1f"},
                                {"border-radius": "12px"}, {"box-shadow": "none"},
                                {"padding": "14px 16px"}, {"height": "92px"}],
                       "icon": [{"width": "18px"}, {"color": color}, {"position": "absolute"},
                                {"top": "14px"}, {"right": "14px"}],
                       "custom_fields": {"content": [{"justify-self": "start"},
                                                     {"align-self": "end"}]}}}


def _anomaly_card():
    return {"type": "vertical-stack", "_lora_anomalies": True, "cards": [
        {"type": "custom:button-card", "show_state": False, "show_icon": False,
         "name": "ANOMALIE (klik → lista)",
         "styles": {"card": [{"background": "none"}, {"box-shadow": "none"}, {"border": "none"},
                             {"padding": "12px 0 4px 0"}],
                    "name": [{"font-size": "10px"}, {"font-weight": 800}, {"color": "#525252"},
                             {"letter-spacing": "3px"}, {"text-transform": "uppercase"},
                             {"justify-self": "start"}]}},
        {"type": "horizontal-stack", "cards": [_count_tile(*b) for b in BUCKETS]},
    ]}


def _clean(cards):
    """Usuń wcześniej wstrzyknięte (idempotency)."""
    out = []
    for c in cards:
        if isinstance(c, dict):
            if c.get("_lora_anomalies"):
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
        bk = "/tmp/lovelace_sup_anom_backup_%d.json" % int(time.time())
        json.dump(cfg, open(bk, "w", encoding="utf-8"), ensure_ascii=False)
        print("backup:", bk)
        views = cfg.get("views", [])
        print("views:", [v.get("title") for v in views])
        if not views:
            print("brak widoków"); return
        for v in views:
            v["cards"] = _clean(v.get("cards", []))
        pref = ["anomalie", "bramki", "bramka", "config", "konfiguracja"]
        target = next((v for p in pref for v in views
                       if str(v.get("title", "")).lower() == p), views[0])
        target.setdefault("cards", []).insert(0, _anomaly_card())
        r = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", r.get("success"), r.get("error", ""))
        print(f"panel ANOMALIE (kafelki+popup) → widok '{target.get('title')}' (góra)")


if __name__ == "__main__":
    asyncio.run(main())
