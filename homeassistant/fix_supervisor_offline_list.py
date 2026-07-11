#!/usr/bin/env python3
"""Supervisor: podmienia TYLKO kafel offline (custom:auto-entities filtrujacy encje v10
button.lora_an_g1_*_device_offline_clear — istnieja 2) na liste renderowana z AGREGATU
sensor.*_offline_anomalies.attributes.items (wszystkie offline, tez spoza discovery).

Styl dopasowany do template-entity-row (ikona lan-disconnect, name + secondary, ✕ clear po prawej,
dividery, dark #0a0a0a). Mapa IEEE->friendly (z2m) tlumaczy surowe 0x... nazwy. NIE dodaje przycisku
clear-all (istniejacy standalone zostaje = jeden). Bateria/inne kafle NIETKNIETE.

Idempotentny: znajduje auto-entities z filtrem *_device_offline_clear i zamienia in-place.
"""
import asyncio, json, os, sys, time
import websockets

HA = "ws://localhost:8123/api/websocket"
SENSOR = "sensor.lora_gateway_g1_gw_g1_offline_anomalies"
N_ROWS = 60
COLOR = "#f87171"
MAP_PATH = "/tmp/ieee_friendly.json"
IEEE_MAP = json.load(open(MAP_PATH, encoding="utf-8")) if os.path.exists(MAP_PATH) else {}
MAPJS = json.dumps(IEEE_MAP, ensure_ascii=False)


def _is_offline_card(node):
    incl = (node.get("filter") or {}).get("include") or []
    return any("device_offline_clear" in (i.get("entity_id") or "") for i in incl)


def _row(i):
    content = (
        "[[[ var items=(states['" + SENSOR + "'].attributes.items)||[];var it=items[" + str(i) + "];"
        "if(!it) return '';var MAP=" + MAPJS + ";"
        "var nm=it.dev;if(nm&&nm.indexOf('0x')===0&&MAP[nm])nm=MAP[nm];"
        "var ts=it.detected_at?new Date(it.detected_at):(it.since?new Date(it.since*1000):null);"
        "var tss=ts?(('0'+ts.getDate()).slice(-2)+'.'+('0'+(ts.getMonth()+1)).slice(-2)+' '+"
        "('0'+ts.getHours()).slice(-2)+':'+('0'+ts.getMinutes()).slice(-2)):'—';"
        "return `<div style=\"display:flex;align-items:center;width:100%;box-sizing:border-box;gap:14px;\">"
        "<ha-icon icon=\"mdi:lan-disconnect\" style=\"color:" + COLOR + ";--mdc-icon-size:20px;flex:0 0 auto;\"></ha-icon>"
        "<div style=\"display:flex;flex-direction:column;gap:2px;flex:1 1 auto;min-width:0;\">"
        "<span style=\"color:#e5e5e5;font-weight:600;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\">${nm}</span>"
        "<span style=\"color:#6b7280;font-size:10px;letter-spacing:.4px;\">🏷️ ${it.gw}  ·  🔴 OFFLINE  ·  🕐 ${tss}</span></div>"
        "<span style=\"color:" + COLOR + ";font-size:18px;font-weight:700;flex:0 0 auto;cursor:pointer;\">✕</span>"
        "</div>`; ]]]")
    hh = "[[[ var it=(states['" + SENSOR + "'].attributes.items||[])[" + str(i) + "];return it?'auto':'0px'; ]]]"
    pad = "[[[ var it=(states['" + SENSOR + "'].attributes.items||[])[" + str(i) + "];return it?'11px 16px':'0px'; ]]]"
    bd = "[[[ var it=(states['" + SENSOR + "'].attributes.items||[])[" + str(i) + "];return it?'1px solid #141414':'none'; ]]]"
    payload = ("[[[ var it=(states['" + SENSOR + "'].attributes.items||[])[" + str(i) + "];"
               "return it?JSON.stringify({gw:it.gw,dev:it.dev,bucket:'offline'}):''; ]]]")
    return {"type": "custom:button-card", "entity": SENSOR, "show_icon": False, "show_name": False,
            "show_state": False, "custom_fields": {"content": content},
            "tap_action": {"action": "call-service", "service": "mqtt.publish",
                           "confirmation": {"text": "Usunąć anomalię offline?"},
                           "service_data": {"topic": "lora/supervisor/cmd/clear_anomaly", "payload": payload},
                           "data": {"topic": "lora/supervisor/cmd/clear_anomaly", "payload": payload}},
            "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"}, {"border": "none"},
                                {"border-bottom": bd}, {"border-radius": "0"}, {"width": "100%"},
                                {"padding": pad}, {"height": hh}, {"overflow": "hidden"}],
                       "grid": [{"grid-template-columns": "1fr"}],
                       "custom_fields": {"content": [{"width": "100%"}, {"justify-self": "stretch"}]}}}


def _offline_list_card():
    empty = {"type": "custom:button-card", "entity": SENSOR, "show_icon": False, "show_name": False,
             "show_state": False,
             "custom_fields": {"content": (
                 "[[[ var n=(states['" + SENSOR + "'].attributes.items||[]).length;"
                 "return n>0?'':'<div style=\"color:#4ade80;font-size:13px;padding:16px;text-align:center;\">"
                 "✅ Brak urządzeń offline.</div>'; ]]]")},
             "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"}, {"border": "none"}, {"padding": "0"}]}}
    rows = [_row(i) for i in range(N_ROWS)]
    return {"type": "vertical-stack", "cards": [empty] + rows,
            "card_mod": {"style": "ha-card{background:#0a0a0a !important;border:1px solid #1f1f1f !important;"
                                  "border-radius:12px !important;overflow:hidden;max-height:520px;overflow-y:auto;}"}}


def _swap(node, cnt):
    if isinstance(node, dict):
        if node.get("type") == "custom:auto-entities" and _is_offline_card(node):
            new = _offline_list_card()
            node.clear(); node.update(new); cnt.append(1); return
        for v in list(node.values()):
            _swap(v, cnt)
    elif isinstance(node, list):
        for x in node:
            _swap(x, cnt)


async def main():
    sys.path.insert(0, os.path.expanduser("~/meshtastic"))
    import config as C
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv(); await ws.send(json.dumps({"type": "auth", "access_token": C.HA_TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [0]
        async def cmd(p):
            mid[0] += 1; p["id"] = mid[0]; await ws.send(json.dumps(p))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0]:
                    return r
        cfg = (await cmd({"type": "lovelace/config"})).get("result") or {}
        json.dump(cfg, open("/tmp/lovelace_sup_offlist2_%d.json" % int(time.time()), "w"), ensure_ascii=False)
        cnt = []
        _swap(cfg, cnt)
        r = await cmd({"type": "lovelace/config/save", "config": cfg})
        print("save:", r.get("success"), r.get("error", ""), "| offline cards swapped:", len(cnt),
              "| map entries:", len(IEEE_MAP))


if __name__ == "__main__":
    asyncio.run(main())
