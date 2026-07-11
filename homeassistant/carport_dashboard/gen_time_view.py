"""Dodaje styled widok 'Czas & Pora dnia' do carport-dashboard (button-card, styl projektu).
Idempotentnie: usuwa istniejacy widok o path 'carport-time' i dodaje na nowo. Reszta bez zmian.
"""
import asyncio, json, sys
import websockets

HOST = "100.98.155.78"; TOKEN = sys.argv[1]
URL = "carport-dashboard"; VP = "carport-time"

# hero: pora dnia z sun.sun (na zywo) + zegar SCADA bramki
PORA_JS = (
    "[[[ "
    "var s=states['sun.sun'];var day=s&&s.state==='above_horizon';"
    "var ico=day?'☀️':'🌙';var lab=day?'DZIEŃ':'NOC';var col=day?'#fbbf24':'#818cf8';"
    "var t=states['sensor.lora_gw_g1_system_time'];var tv=t?(''+t.state).replace('T',' '):'--';"
    "var nx=day?(s&&s.attributes.next_setting):(s&&s.attributes.next_rising);"
    "var nxl=day?'zachód':'wschód';"
    "var nxt=nx?new Date(nx).toLocaleTimeString('pl-PL',{hour:'2-digit',minute:'2-digit'}):'--';"
    "return `<div style=\"display:flex;align-items:center;gap:20px;width:100%;\">"
    "<div style=\"font-size:52px;\">${ico}</div>"
    "<div style=\"flex:1;\">"
    "<div style=\"font-size:10px;letter-spacing:3px;color:#525252;font-weight:800;\">PORA DNIA</div>"
    "<div style=\"font-size:30px;font-weight:900;color:${col};line-height:1.1;\">${lab}</div>"
    "<div style=\"font-size:11px;color:#737373;margin-top:4px;\">${nxl} ${nxt} · zegar ${tv}</div>"
    "</div></div>`; ]]]"
)


def hero():
    return {"type": "custom:button-card", "template": "lora_base", "entity": "sun.sun",
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"},
            "custom_fields": {"c": PORA_JS},
            "styles": {"card": [{"height": "104px"}, {"padding": "16px 20px"}],
                       "custom_fields": {"c": [{"width": "100%"}]},
                       "grid": [{"grid-template-areas": "\"c\""}]},
            "layout_options": {"grid_columns": 4}}


def hdr(t):
    return {"type": "custom:button-card", "template": "lora_hdr", "name": t,
            "layout_options": {"grid_columns": 4}}


def stat(name, eid):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid, "name": name}


def time_view():
    return {"title": "Czas", "path": VP, "icon": "mdi:clock-star-four-points", "type": "sections",
            "sections": [
                {"type": "grid", "cards": [hdr("CZAS & PORA DNIA"), hero()]},
                {"type": "grid", "cards": [hdr("SYNCHRONIZACJA CZASU"),
                    stat("Zsynchronizowany", "binary_sensor.lora_gw_g1_synced"),
                    stat("Tryb sync", "sensor.lora_gw_g1_sync_mode"),
                    stat("Offset [s]", "sensor.lora_gw_g1_time_offset"),
                    stat("Czas SCADA", "sensor.lora_gw_g1_scada_time"),
                    stat("Czas systemowy", "sensor.lora_gw_g1_system_time")]},
                {"type": "grid", "cards": [hdr("BRAMKA"),
                    stat("Tryb pracy", "sensor.lora_gw_g1_operating_mode"),
                    stat("Uptime [s]", "sensor.lora_gw_g1_uptime"),
                    stat("TX count", "sensor.lora_gw_g1_tx_count")]},
            ]}


async def main():
    async with websockets.connect(f"ws://{HOST}:8123/api/websocket", max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [1]
        async def call(m):
            i = mid[0]; mid[0] += 1
            await ws.send(json.dumps({**m, "id": i}))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == i: return r
        cfg = (await call({"type": "lovelace/config", "url_path": URL}))["result"]
        views = cfg["views"]
        views[:] = [v for v in views if v.get("path") != VP]
        # wstaw po Harmonogramie (indeks 2) jesli jest, inaczej na koniec
        idx = next((i for i, v in enumerate(views) if v.get("path") == "carport-schedule"), len(views) - 1)
        views.insert(idx + 1, time_view())
        r = await call({"type": "lovelace/config/save", "url_path": URL, "config": cfg})
        print("save:", "OK" if r.get("success") else json.dumps(r)[:200])
        print("views:", [v["title"] for v in views])


asyncio.run(main())
