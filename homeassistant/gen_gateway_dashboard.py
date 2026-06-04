#!/usr/bin/env python3
"""Generuje i wdraża (WS API) dashboard gateway 1:1 ze stylem supervisora,
ale na encjach zigbee2mqtt. Dark theme, button-card + apexcharts.

Encje z2m na gateway:
  temp_N: sensor.temp_N_temperature/_humidity/_battery
  door_1: binary_sensor.door_1_contact + sensor.door_1_battery
  leak_1: binary_sensor.leak_1_water_leak + sensor.leak_1_battery
  switche: switch.<ieee> (Test 1/2)
Online z2m = state != 'unavailable'; last_seen = entity.last_changed.
"""
import asyncio, json, os, sys
import websockets

HA = "ws://100.98.155.78:8123/api/websocket"
# Long-lived token z gateway_v38.py (ha_api.token) — podaj przez env HA_GW_TOKEN
TOKEN = os.environ.get("HA_GW_TOKEN", "REPLACE_ME")
URL_PATH = "lora-gw"

TEMPS = [("Temp 1", "temp_1"), ("Temp 2", "temp_2"),
         ("Temp 3", "temp_3"), ("Temp 4", "temp_4")]
SWITCHES = [("Test 1", "switch.0x70c59cfffee2b098"),
            ("Test 2", "switch.0x70c59cfffe8c0be7")]
ALARMS = [("Leak 1", "binary_sensor.leak_1_water_leak", "sensor.leak_1_battery", "leak"),
          ("Door 1", "binary_sensor.door_1_contact", "sensor.door_1_battery", "contact")]

BADGE = ('`<span style="background:#141414;border:1px solid #22d3ee;padding:1px 5px;'
         'border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;">G1</span>`')

# ── button_card_templates (z supervisora) ──
TEMPLATES = {
    "lora_base": {"styles": {"card": [
        {"background": "#0a0a0a"}, {"border-radius": "12px"},
        {"border": "1px solid #1f1f1f"}, {"box-shadow": "none"}, {"overflow": "hidden"}]}},
    "lora_hdr": {"show_icon": False, "show_state": False, "styles": {
        "card": [{"background": "none"}, {"box-shadow": "none"}, {"border": "none"},
                 {"padding": "12px 0 4px 0"}],
        "name": [{"font-size": "10px"}, {"font-weight": 700}, {"color": "#525252"},
                 {"letter-spacing": "3px"}, {"text-transform": "uppercase"},
                 {"justify-self": "start"}]}},
    "lora_stat": {"template": "lora_base", "show_state": True, "styles": {
        "card": [{"padding": "16px"}, {"height": "78px"}],
        "state": [{"font-size": "22px"}, {"font-weight": 800},
                  {"font-variant-numeric": "tabular-nums"}, {"justify-self": "start"},
                  {"color": "#e5e5e5"}],
        "name": [{"font-size": "8px"}, {"font-weight": 700}, {"color": "#525252"},
                 {"letter-spacing": "2px"}, {"justify-self": "start"}],
        "icon": [{"width": "18px"}, {"color": "#525252"}],
        "img_cell": [{"justify-self": "end"}, {"position": "absolute"},
                     {"top": "14px"}, {"right": "14px"}]}},
}


def stat_tile(eid, label, icon):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid,
            "name": label, "icon": icon, "tap_action": {"action": "more-info"}}


def link_card():
    e = "binary_sensor.lora_g1_sup_link"
    content = (
        "[[[ var l=states['" + e + "'];var on=l&&l.state==='on';"
        "var col=on?'#4ade80':'#f87171';var lbl=on?'POŁĄCZONY':'BRAK';"
        "var rx=states['sensor.lora_g1_sup_last_rx'];var rxT=rx?rx.state:'--';"
        "var off=states['sensor.lora_g1_time_offset'];var offT=off?off.state:'--';"
        "var sy=states['sensor.lora_g1_last_sync'];var syT=sy?sy.state:'--';"
        "return `<div style=\"display:flex;flex-direction:column;gap:8px;width:100%;\">"
        "<div style=\"display:flex;align-items:center;justify-content:space-between;\">"
        "<span style=\"font-size:11px;font-weight:700;color:#525252;letter-spacing:2px;\">"
        "SUPERVISOR</span><span style=\"font-size:13px;font-weight:800;color:${col};\">"
        "● ${lbl}</span></div>"
        "<div style=\"font-size:10px;color:#525252;\">ostatni RX: "
        "<span style=\"color:#e5e5e5;\">${rxT}</span></div>"
        "<div style=\"display:flex;justify-content:space-between;font-size:10px;color:#525252;\">"
        "<span>sync: <span style=\"color:#22d3ee;\">${syT}</span></span>"
        "<span>offset: <span style=\"color:#22d3ee;\">${offT}</span></span></div>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": e,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"},
            "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "16px"},
                                {"border": ("[[[ var l=states['" + e + "'];return l&&l.state==='on'?"
                                            "'1px solid #1f3a1f':'1px solid #3a1f1f'; ]]]")}]}}


def online_js(eid):
    return (f"var e=states['{eid}'];var online=e&&e.state!=='unavailable';"
            "var col=online?'#4ade80':'#f87171';var label=online?'online':'offline';")


def batt_js(batt_eid):
    return (f"var b=states['{batt_eid}'];var batt=b&&b.state!=='unavailable'?b.state:'--';"
            "var battColor='#4ade80';if(batt!=='--'){var bv=parseInt(batt);"
            "if(bv<10)battColor='#f87171';else if(bv<25)battColor='#fbbf24';}")


def ls_js(eid):
    return (f"var ent=states['{eid}'];var lsT=ent?new Date(ent.last_changed)."
            "toLocaleString('pl-PL',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'--';")


def _avail_branch(eid, unavail, on, off):
    return ("[[[ var e=states['" + eid + "']; if(e&&e.state==='unavailable') return '"
            + unavail + "'; return e&&e.state==='on'?'" + on + "':'" + off + "'; ]]]")


def switch_card(name, eid):
    info = ("[[[ " + online_js(eid) + ls_js(eid) +
            "return `<div style=\"font-size:10px;color:#525252;margin-top:6px;\">"
            "<span style=\"color:${col};\">● ${label}</span><br/>${lsT}</div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "toggle"},
        "icon": _avail_branch(eid, "mdi:alert-octagon", "mdi:lightbulb-on", "mdi:lightbulb-off-outline"),
        "custom_fields": {
            "badge": "[[[ return " + BADGE + "; ]]]",
            "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f")}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252")}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
            "custom_fields": {"badge": [{"position": "absolute"}, {"top": "10px"},
                                        {"right": "15px"}],
                              "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def alarm_card(name, eid, batt_eid, kind):
    on_icon = "mdi:water-alert" if kind == "leak" else "mdi:door-open"
    off_icon = "mdi:water" if kind == "leak" else "mdi:door-closed"
    info = ("[[[ " + online_js(eid) + batt_js(batt_eid) + ls_js(eid) +
            "return `<div style=\"display:flex;flex-direction:column;gap:2px;font-size:10px;"
            "color:#525252;margin-top:6px;\"><div><span style=\"color:${col};\">● ${label}</span>"
            " · <span style=\"color:${battColor};\">🔋 ${batt}%</span></div>"
            "<div>${lsT}</div></div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "none"},
        "icon": _avail_branch(eid, "mdi:alert-octagon", on_icon, off_icon),
        "custom_fields": {
            "badge": "[[[ return " + BADGE + "; ]]]",
            "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f")}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252")}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
            "custom_fields": {"badge": [{"position": "absolute"}, {"top": "10px"},
                                        {"right": "15px"}],
                              "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def chart(entity, title, color, decimals, ymin=None, ymax=None):
    yax = {"decimals": decimals}
    if ymin is not None:
        yax["min"] = ymin
        yax["max"] = ymax
    base = {
        "type": "custom:apexcharts-card",
        "header": {"show": True, "title": title, "show_states": False, "colorize_states": True},
        "graph_span": "24h", "span": {"end": "minute"}, "yaxis": [yax],
        "apex_config": {"chart": {"height": 160, "toolbar": {"show": False},
                                  "background": "transparent"},
                        "grid": {"show": True, "borderColor": "#1f1f1f"},
                        "stroke": {"width": 2, "curve": "smooth"},
                        "dataLabels": {"enabled": False}, "legend": {"show": False},
                        "tooltip": {"enabled": True, "theme": "dark"}},
        "series": [{"entity": entity, "name": title, "type": "line", "color": color}],
        "card_mod": {"style": ("ha-card{background:transparent!important;border:none!important;"
                               "box-shadow:none!important;}ha-card .header{padding:4px 8px!important;"
                               "min-height:24px!important;}ha-card .header .title{font-size:12px!important;}")},
    }
    # gateway HA nie ma browser_mod → tap = wbudowane more-info (historia encji)
    wrapper = {
        "type": "custom:button-card", "show_icon": False, "show_name": False,
        "show_state": False, "entity": entity,
        "styles": {"card": [{"background": "#0a0a0a"}, {"border-radius": "12px"},
                            {"box-shadow": "none"}, {"padding": 0}, {"overflow": "hidden"},
                            {"height": "auto"}],
                   "custom_fields": {"chart": [{"pointer-events": "none"}]}},
        "custom_fields": {"chart": {"card": base}},
        "tap_action": {"action": "more-info"},
    }
    return wrapper


def temp_stat(name, slug):
    t, h, b = (f"sensor.{slug}_temperature", f"sensor.{slug}_humidity", f"sensor.{slug}_battery")
    content = (
        f"var t=states['{t}'];var h=states['{h}'];var b=states['{b}'];"
        f"var temp=t&&t.state!=='unavailable'?parseFloat(t.state).toFixed(1):'--';"
        f"var hum=h&&h.state!=='unavailable'?Math.round(parseFloat(h.state)):'--';"
        f"var batt=b&&b.state!=='unavailable'?b.state:'--';"
        f"{batt_js(b)}{online_js(t)}{ls_js(t)}"
        "return `<div style=\"display:flex;flex-direction:column;height:100%;width:100%;\">"
        "<div style=\"display:flex;align-items:center;width:100%;margin-bottom:8px;"
        "justify-content:space-between;\"><div style=\"font-size:14px;font-weight:600;"
        f"color:#e5e5e5;\">{name}</div><div>{BADGE[1:-1]}</div></div>"
        "<div style=\"display:flex;justify-content:space-between;align-items:center;"
        "width:100%;flex-grow:1;\"><div style=\"display:flex;align-items:baseline;gap:30px;\">"
        "<div><span style=\"font-size:30px;font-weight:700;color:#22d3ee;\">${temp}</span>"
        "<span style=\"font-size:22px;color:#525252;\">°C</span></div>"
        "<div><span style=\"font-size:30px;font-weight:700;color:#4ade80;\">${hum}</span>"
        "<span style=\"font-size:22px;color:#525252;\">%</span></div></div>"
        "<div style=\"display:flex;flex-direction:column;align-items:flex-end;gap:2px;"
        "font-size:10px;color:#525252;\"><div><span style=\"color:${col};\">● ${label}</span>"
        " · <span style=\"color:${battColor};\">🔋 ${batt}%</span></div><div>${lsT}</div>"
        "</div></div></div>`;")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": t,
        "show_icon": False, "show_state": False, "show_name": False,
        "custom_fields": {"content": "[[[ %s ]]]" % content},
        "styles": {"card": [{"height": "90px"}, {"padding": "16px"},
                            {"border-radius": "12px"}, {"background": "#0a0a0a"}],
                   "icon": [{"display": "none"}], "name": [{"display": "none"}]}}


def build_config():
    control = {"path": "lora-control", "title": "Sterowanie", "icon": "mdi:lightbulb",
               "type": "masonry", "cards": [
                   {"type": "custom:button-card", "template": "lora_hdr", "name": "STEROWANIE"},
                   {"type": "horizontal-stack",
                    "cards": [switch_card(n, e) for n, e in SWITCHES]}]}
    alarms = {"path": "lora-alarms", "title": "Alarmy", "icon": "mdi:fire-alert", "cards": [
        {"type": "custom:button-card", "template": "lora_hdr", "name": "CZUJNIKI"},
        {"type": "horizontal-stack", "cards": [alarm_card(*a) for a in ALARMS]}]}
    sensor_cards = [{"type": "custom:button-card", "template": "lora_hdr", "name": "POMIARY"}]
    for name, slug in TEMPS:
        sensor_cards.append(temp_stat(name, slug))
        sensor_cards.append({"type": "horizontal-stack", "cards": [
            chart(f"sensor.{slug}_temperature", "Temperatura", "#22d3ee", 1),
            chart(f"sensor.{slug}_humidity", "Wilgotność", "#4ade80", 0, 0, 100)]})
    sensors = {"path": "lora-sensors", "title": "Pomiary", "icon": "mdi:thermometer",
               "cards": [{"type": "vertical-stack", "cards": sensor_cards}]}
    gwv = {"path": "lora-gw-stats", "title": "Bramka", "icon": "mdi:radio-tower",
           "type": "masonry", "cards": [
               {"type": "custom:button-card", "template": "lora_hdr", "name": "BRAMKA G1"},
               link_card(),
               {"type": "horizontal-stack", "cards": [
                   stat_tile("sensor.lora_g1_gw_uptime", "UPTIME", "mdi:timer"),
                   stat_tile("sensor.lora_g1_gw_monitored", "MONITORED", "mdi:eye")]},
               stat_tile("sensor.lora_g1_gw_last_hb", "OSTATNI HB", "mdi:heart-pulse")]}
    return {"title": "LoRa Gateway G1", "button_card_templates": TEMPLATES,
            "views": [gwv, control, alarms, sensors]}


async def deploy():
    cfg = build_config()
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()  # auth_required
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        r = json.loads(await ws.recv())
        if r.get("type") != "auth_ok":
            print("AUTH FAIL", r); return
        mid = [0]

        async def call(msg):
            mid[0] += 1
            msg["id"] = mid[0]
            await ws.send(json.dumps(msg))
            while True:
                resp = json.loads(await ws.recv())
                if resp.get("id") == mid[0] and resp.get("type") == "result":
                    return resp

        lst = await call({"type": "lovelace/dashboards/list"})
        exists = any(d.get("url_path") == URL_PATH for d in lst.get("result", []))
        if not exists:
            c = await call({"type": "lovelace/dashboards/create", "url_path": URL_PATH,
                            "title": "LoRa Gateway", "mode": "storage",
                            "show_in_sidebar": True, "icon": "mdi:radio-tower"})
            print("create:", c.get("success"), c.get("error", ""))
        else:
            print("dashboard już istnieje — nadpisuję config")
        s = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", s.get("success"), s.get("error", ""))
        print("views:", [v["title"] for v in cfg["views"]],
              "| switches:", len(SWITCHES), "| temps:", len(TEMPS))


if __name__ == "__main__":
    asyncio.run(deploy())
