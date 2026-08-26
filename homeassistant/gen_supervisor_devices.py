#!/usr/bin/env python3
"""Kafle SWITCH + TEMP (+ALARMY) per-bramka na dashboardzie supervisora, styl 1:1 z
gen_gateway_dashboard.py, ale na encjach SUPERVISORA (schemat lora_<slug>_<slug>_<suf> —
INNY niż z2m-natywny na bramce, bo tu encje tworzy MQTT discovery z LoRa protokołu, nie z2m
bezpośrednio). Import z gen_supervisor_dashboard.py, splice do gw_view().

Devices per gateway — G2 potwierdzone live (2026-07-14, po cleanup do 6 urządzeń testowych).
G1 (carport lighting) jeszcze NIEZNANE — pusta lista dopóki G1 nie będzie osiągalne."""
import json

DEVICES = {
    "G2": {
        "switches": [("Test 1", "test_1"), ("Test 2", "test_2")],
        "temps": [("Temp 1", "temp_1"), ("Temp 2", "temp_2")],
        "alarms": [("Leak 1", "leak_1", "leak"), ("Door 1", "door_1", "contact")],
    },
    "G1": {"switches": [], "temps": [], "alarms": []},  # TODO: carport light (deferred)
}

BADGE_TPL = ('`<span style="background:#141414;border:1px solid #22d3ee;padding:1px 5px;'
             'border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;">{gw}</span>`')

OFFLINE_S = 1920  # = GatewayData.offline_after na bramce (spójność z jej dashboardem)


def _eids(gw, slug):
    """Schemat supervisora: lora_<slug>_<slug>_<suf> (podwójna nazwa — tak tworzy ją
    autodiscovery MQTT z protokołu LoRa, inaczej niż z2m-natywne sensor.<slug>_<suf> na bramce)."""
    p = f"lora_{slug}_{slug}"
    return {
        "lqi": f"sensor.{p}_link_quality",
        "available": f"binary_sensor.{p}_available",
        "last_seen": f"sensor.{p}_last_seen",
        "battery": f"sensor.{p}_battery",
        "temperature": f"sensor.{p}_temperature",
        "humidity": f"sensor.{p}_humidity",
        "switch": f"switch.lora_{slug}_lora_{slug}",
        "water_leak": f"binary_sensor.{p}_water_leak",
        "contact": f"binary_sensor.{p}_contact",
    }


def _fresh_js(lqi_eid):
    return (f"var fe=states['{lqi_eid}'];var fts=fe&&(fe.last_reported||fe.last_updated);"
            f"var fresh=fts&&((Date.now()-new Date(fts).getTime())/1000<{OFFLINE_S});")


def online_js(eid, lqi_eid):
    return (f"var e=states['{eid}'];" + _fresh_js(lqi_eid) +
            "var online=e&&e.state!=='unavailable'&&fresh;"
            "var col=online?'#4ade80':'#f87171';var label=online?'online':'offline';")


def batt_js(batt_eid):
    return (f"var b=states['{batt_eid}'];var batt=b&&b.state!=='unavailable'?b.state:'--';"
            "var battColor='#4ade80';if(batt!=='--'){var bv=parseInt(batt);"
            "if(bv<10)battColor='#f87171';else if(bv<25)battColor='#fbbf24';}")


def ls_js(eid):
    return (f"var ent=states['{eid}'];var lsT=ent?new Date(ent.last_changed)."
            "toLocaleString('pl-PL',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'--';")


def lq_js(lqi_eid):
    return (f"var lqe=states['{lqi_eid}'];"
            "var lq=lqe&&lqe.state!=='unavailable'&&lqe.state!==''&&lqe.state!=='unknown'?parseInt(lqe.state):null;"
            "var lqCol=lq===null?'#525252':(lq>=100?'#4ade80':(lq>=50?'#fbbf24':'#f87171'));"
            "var lqTxt=lq===null?'--':lq;")


def _avail_branch(eid, unavail, on, off, lqi_eid):
    return ("[[[ var e=states['" + eid + "']; " + _fresh_js(lqi_eid)
            + "if(!e||e.state==='unavailable'||!fresh) return '" + unavail + "'; "
            "return e.state==='on'?'" + on + "':'" + off + "'; ]]]")


def switch_card(gw, name, slug):
    e = _eids(gw, slug)
    eid, lqi = e["switch"], e["lqi"]
    badge = BADGE_TPL.format(gw=gw)
    info = ("[[[ " + online_js(eid, lqi) + ls_js(eid) + lq_js(lqi) +
            "return `<div style=\"font-size:10px;color:#525252;margin-top:6px;\">"
            "<span style=\"color:${col};\">● ${label}</span>"
            " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span><br/>${lsT}</div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "toggle"},
        "icon": "[[[ var e=states['" + eid + "']; "
                "return e&&e.state==='on'?'mdi:lightbulb-on':'mdi:lightbulb-off-outline'; ]]]",
        "custom_fields": {"badge": "[[[ return " + badge + "; ]]]", "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f", lqi)}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252", lqi)}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
            "custom_fields": {"badge": [{"position": "absolute"}, {"top": "10px"}, {"right": "15px"}],
                              "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def alarm_card(gw, name, slug, kind):
    e = _eids(gw, slug)
    eid = e["water_leak"] if kind == "leak" else e["contact"]
    batt_eid, lqi = e["battery"], e["lqi"]
    badge = BADGE_TPL.format(gw=gw)
    on_icon = "mdi:water-alert" if kind == "leak" else "mdi:door-open"
    off_icon = "mdi:water" if kind == "leak" else "mdi:door-closed"
    info = ("[[[ " + online_js(eid, lqi) + batt_js(batt_eid) + ls_js(eid) + lq_js(lqi) +
            "return `<div style=\"display:flex;flex-direction:column;gap:2px;font-size:10px;"
            "color:#525252;margin-top:6px;\"><div><span style=\"color:${col};\">● ${label}</span>"
            " · <span style=\"color:${battColor};\">🔋 ${batt}%</span>"
            " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div>"
            "<div>${lsT}</div></div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "none"},
        "icon": _avail_branch(eid, "mdi:alert-octagon", on_icon, off_icon, lqi),
        "custom_fields": {"badge": "[[[ return " + badge + "; ]]]", "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f", lqi)}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252", lqi)}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
            "custom_fields": {"badge": [{"position": "absolute"}, {"top": "10px"}, {"right": "15px"}],
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
        "apex_config": {"chart": {"height": 160, "toolbar": {"show": False}, "background": "transparent"},
                        "grid": {"show": True, "borderColor": "#1f1f1f"},
                        "stroke": {"width": 2, "curve": "smooth"},
                        "dataLabels": {"enabled": False}, "legend": {"show": False},
                        "tooltip": {"enabled": True, "theme": "dark"}},
        "series": [{"entity": entity, "name": title, "type": "line", "color": color}],
        "card_mod": {"style": ("ha-card{background:transparent!important;border:none!important;"
                               "box-shadow:none!important;}ha-card .header{padding:4px 8px!important;"
                               "min-height:24px!important;}ha-card .header .title{font-size:12px!important;}")},
    }
    return {
        "type": "custom:button-card", "show_icon": False, "show_name": False,
        "show_state": False, "entity": entity,
        "styles": {"card": [{"background": "#0a0a0a"}, {"border-radius": "12px"},
                            {"box-shadow": "none"}, {"padding": 0}, {"overflow": "hidden"},
                            {"height": "auto"}],
                   "custom_fields": {"chart": [{"pointer-events": "none"}]}},
        "custom_fields": {"chart": {"card": base}},
        "tap_action": {"action": "more-info"},
    }


def temp_stat(gw, name, slug):
    e = _eids(gw, slug)
    t, h, b, lqi = e["temperature"], e["humidity"], e["battery"], e["lqi"]
    badge = BADGE_TPL.format(gw=gw)
    content = (
        f"var t=states['{t}'];var h=states['{h}'];var b=states['{b}'];"
        f"var temp=t&&t.state!=='unavailable'?parseFloat(t.state).toFixed(1):'--';"
        f"var hum=h&&h.state!=='unavailable'?Math.round(parseFloat(h.state)):'--';"
        f"var batt=b&&b.state!=='unavailable'?b.state:'--';"
        f"{batt_js(b)}{online_js(t, lqi)}{ls_js(t)}{lq_js(lqi)}"
        "return `<div style=\"display:flex;flex-direction:column;height:100%;width:100%;\">"
        "<div style=\"display:flex;align-items:center;width:100%;margin-bottom:8px;"
        "justify-content:space-between;\"><div style=\"font-size:14px;font-weight:600;"
        f"color:#e5e5e5;\">{name}</div><div>{badge[1:-1]}</div></div>"
        "<div style=\"display:flex;justify-content:space-between;align-items:center;"
        "width:100%;flex-grow:1;\"><div style=\"display:flex;align-items:baseline;gap:30px;\">"
        "<div><span style=\"font-size:30px;font-weight:700;color:#22d3ee;\">${temp}</span>"
        "<span style=\"font-size:22px;color:#525252;\">°C</span></div>"
        "<div><span style=\"font-size:30px;font-weight:700;color:#4ade80;\">${hum}</span>"
        "<span style=\"font-size:22px;color:#525252;\">%</span></div></div>"
        "<div style=\"display:flex;flex-direction:column;align-items:flex-end;gap:2px;"
        "font-size:10px;color:#525252;\"><div><span style=\"color:${col};\">● ${label}</span>"
        " · <span style=\"color:${battColor};\">🔋 ${batt}%</span>"
        " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div><div>${lsT}</div>"
        "</div></div></div>`;")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": t,
        "show_icon": False, "show_state": False, "show_name": False,
        "custom_fields": {"content": "[[[ %s ]]]" % content},
        "styles": {"card": [{"height": "112px"}, {"padding": "16px"},
                            {"border-radius": "12px"}, {"background": "#0a0a0a"},
                            {"border": _avail_branch(t, "1px solid #f87171",
                                                     "1px solid #1f1f1f", "1px solid #1f1f1f", lqi)}],
                   "icon": [{"display": "none"}], "name": [{"display": "none"}]}}


def device_section_cards(gw, have):
    """Sekcja SWITCH + TEMP + ALARMY dla danej bramki. Pomija urządzenia, których encje
    jeszcze nie istnieją w `have` (np. G1 dopóki carport light nieznany/nieskonfigurowany)."""
    cfg = DEVICES.get(gw, {"switches": [], "temps": [], "alarms": []})
    cards = []
    sw_cards = []
    for name, slug in cfg["switches"]:
        eid = _eids(gw, slug)["switch"]
        if eid in have:
            sw_cards.append(switch_card(gw, name, slug))
    if sw_cards:
        cards += [{"type": "custom:button-card", "template": "lora_hdr", "name": "PRZEKAŹNIKI"},
                   {"type": "horizontal-stack", "cards": sw_cards}]

    temp_cards = []
    for name, slug in cfg["temps"]:
        e = _eids(gw, slug)
        if e["temperature"] in have:
            temp_cards.append(temp_stat(gw, name, slug))
            temp_cards.append({"type": "horizontal-stack", "cards": [
                chart(e["temperature"], "Temperatura", "#22d3ee", 1),
                chart(e["humidity"], "Wilgotność", "#4ade80", 0, 0, 100)]})
    if temp_cards:
        cards += [{"type": "custom:button-card", "template": "lora_hdr", "name": "TEMPERATURA & WILGOTNOŚĆ"}]
        cards += temp_cards

    alarm_cards = []
    for name, slug, kind in cfg["alarms"]:
        e = _eids(gw, slug)
        eid = e["water_leak"] if kind == "leak" else e["contact"]
        if eid in have:
            alarm_cards.append(alarm_card(gw, name, slug, kind))
    if alarm_cards:
        cards += [{"type": "custom:button-card", "template": "lora_hdr", "name": "CZUJNIKI PRIORITY"},
                   {"type": "horizontal-stack", "cards": alarm_cards}]

    return cards
