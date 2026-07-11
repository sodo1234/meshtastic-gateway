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


def _token():
    """Token: env HA_GW_TOKEN albo CONFIG['ha_api']['token'] (na hoście, w runtime)."""
    t = os.environ.get("HA_GW_TOKEN")
    if t:
        return t
    try:
        sys.path.insert(0, os.path.expanduser("~/meshtastic"))
        from config import CONFIG
        return (CONFIG.get("ha_api") or {}).get("token", "REPLACE_ME")
    except Exception:
        return "REPLACE_ME"


TOKEN = _token()
URL_PATH = "lora-gw"

# tylko fizycznie istniejące czujniki z2m (Temp 3/4 usunięte — martwe kafelki/wykresy)
TEMPS = [("Temp 1", "temp_1"), ("Temp 2", "temp_2")]
SWITCHES = [("Test 1", "switch.0x70c59cfffee2b098"),
            ("Test 2", "switch.0x70c59cfffe8c0be7")]
ALARMS = [("Leak 1", "binary_sensor.leak_1_water_leak", "sensor.leak_1_battery", "leak"),
          ("Door 1", "binary_sensor.door_1_contact", "sensor.door_1_battery", "contact")]

BADGE = ('`<span style="background:#141414;border:1px solid #22d3ee;padding:1px 5px;'
         'border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;">G2</span>`')

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
        "card": [{"padding": "14px 16px"}, {"height": "88px"}],
        # Ikona w OSOBNEJ kolumnie grida (nie absolute) → długie wartości (hash/uptime/
        # timestamp) NIGDY nie wchodzą pod ikonę. Układ: [nazwa | ikona] / [stan | ikona].
        "grid": [{"grid-template-areas": '"n i" "s i"'},
                 {"grid-template-columns": "1fr 22px"},
                 {"grid-template-rows": "auto auto"},
                 {"align-items": "center"}, {"column-gap": "10px"}],
        "state": [{"font-size": "19px"}, {"font-weight": 800},
                  {"font-variant-numeric": "tabular-nums"}, {"justify-self": "start"},
                  {"color": "#e5e5e5"}, {"white-space": "nowrap"}, {"overflow": "hidden"},
                  {"text-overflow": "ellipsis"}, {"max-width": "100%"}, {"line-height": "1.2"}],
        "name": [{"font-size": "8px"}, {"font-weight": 700}, {"color": "#525252"},
                 {"letter-spacing": "2px"}, {"justify-self": "start"}, {"margin-bottom": "6px"},
                 {"white-space": "nowrap"}, {"overflow": "hidden"}, {"text-overflow": "ellipsis"},
                 {"max-width": "100%"}],
        "icon": [{"width": "18px"}, {"color": "#525252"}],
        "img_cell": [{"justify-self": "center"}, {"align-self": "center"}]}},
    # przycisk akcji (Send Config / Send Timeout) — jednolinijkowy, bez nachodzenia
    "lora_btn": {"template": "lora_base", "show_state": False, "styles": {
        "card": [{"padding": "14px 12px"}, {"height": "58px"}],
        "name": [{"font-size": "11px"}, {"font-weight": 800}, {"letter-spacing": "1.5px"},
                 {"text-transform": "uppercase"}, {"white-space": "nowrap"},
                 {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
        "icon": [{"width": "20px"}]}},
}


def stat_tile(eid, label, icon):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid,
            "name": label, "icon": icon, "tap_action": {"action": "more-info"}}


# urządzenia spoza themed-views (Pomiary/Sterowanie/Alarmy) — kompakt w zakładce Bramka:
# nazwa + LQI + bateria. Themed devices mają LQI bezpośrednio na swoich kartach.
# 3. element (opcjonalny) = jawne entity baterii Z2M. LQI tworzymy my (reg_gw_lqi →
# sensor.<slug>_lqi, stabilne), ale baterię publikuje Z2M pod ID z IEEE urządzenia
# (np. sensor.0x385c...e1c9e_battery), więc sensor.<slug>_battery NIE istnieje —
# trzeba podać realne entity, inaczej kafel pokazuje '--'.
OTHER_DEVICES = [("Button", "button", "sensor.0x385cfbfffece1c9e_battery")]


def other_tile(name, slug, batt_eid=None):
    """Kompaktowa karta urządzenia spoza themed-views: nazwa | LQI | bateria."""
    lqe = f"sensor.{slug}_lqi"
    be = batt_eid or f"sensor.{slug}_battery"
    content = (
        "[[[ "
        f"var lqe=states['{lqe}'];var lq=lqe&&lqe.state!=='unavailable'&&lqe.state!==''&&lqe.state!=='unknown'?parseInt(lqe.state):null;"
        "var lqCol=lq===null?'#525252':(lq>=100?'#4ade80':(lq>=50?'#fbbf24':'#f87171'));var lqTxt=lq===null?'--':lq;"
        f"var be=states['{be}'];var bt=be&&be.state!=='unavailable'&&be.state!==''&&be.state!=='unknown'?parseInt(be.state):null;"
        "var btCol=bt===null?'#525252':(bt<10?'#f87171':(bt<25?'#fbbf24':'#4ade80'));var btTxt=bt===null?'--':bt;"
        "return `<div style=\"display:flex;align-items:center;justify-content:space-between;width:100%;\">"
        "<span style=\"font-size:12px;font-weight:700;color:#e5e5e5;\">" + name + "</span>"
        "<div style=\"display:flex;gap:16px;align-items:baseline;font-size:11px;\">"
        "<span style=\"color:#525252;\">📶 <span style=\"color:${lqCol};font-weight:800;\">${lqTxt}</span>"
        "<span style=\"font-size:8px;\">/255</span></span>"
        "<span style=\"color:#525252;\">🔋 <span style=\"color:${btCol};font-weight:800;\">${btTxt}%</span></span>"
        "</div></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": lqe,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"},
            "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "15px 16px"}, {"height": "52px"}]}}


def other_section():
    """Sekcja 'pozostałe urządzenia' (spoza themed-views) w zakładce Bramka."""
    if not OTHER_DEVICES:
        return []
    cards = [{"type": "custom:button-card", "template": "lora_hdr",
              "name": "POZOSTAŁE URZĄDZENIA"}]
    cards += [other_tile(*d) for d in OTHER_DEVICES]   # (name, slug[, batt_eid])
    return cards


def link_card():
    e = "binary_sensor.gw_g2_supervisor_link"
    content = (
        "[[[ var l=states['" + e + "'];var on=l&&l.state==='on';"
        "var col=on?'#4ade80':'#f87171';var lbl=on?'POŁĄCZONY':'BRAK';"
        "var rx=states['sensor.gw_g2_supervisor_last_rx'];var rxT=rx?rx.state:'--';"
        "var off=states['sensor.gw_g2_time_offset'];var offT=off?off.state:'--';"
        "var sy=states['sensor.gw_g2_last_sync'];var syT=sy?sy.state:'--';"
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
                                            "'1px solid #14532d':'1px solid #7f1d1d'; ]]]")}]}}


# offline gdy brak raportu z2m > tyle s (= gateway data.offline_after). Martwy czujnik
# trzyma w Z2M ostatnią wartość (state != unavailable), więc liczymy WIEK ostatniego
# raportu z last_updated — to samo kryterium co gw-side liveness wysyłany do supervisora.
OFFLINE_S = 1920   # = GatewayData.offline_after (z2m-cisza) → dashboard ZGODNY z flagą `a:`
# wysyłaną do supervisora. force_update=True na LQI bumpuje last_reported co raport z2m,
# więc świeżość jest prawdziwa. (Wcześniej 7200 rozjeżdżał dashboard z supervisorem.)


def _avail_eid(lqi_eid):
    """sensor.<slug>_lqi → binary_sensor.<slug>_available (encja publikowana przez bramkę
    z GatewayData.on_avail = TO SAMO źródło co flaga `a:` do supervisora)."""
    slug = lqi_eid[len("sensor."):-len("_lqi")]
    return f"binary_sensor.{slug}_available"


def _fresh_js(lqi_eid):
    # UNIFIKACJA: 'fresh'/online z encji ...available bramki (jedno źródło z `a:` → zgodność
    # z supervisorem; koniec rozjazdu LQI-heurystyka vs supervisor). Fallback: gdy encji jeszcze
    # nie ma (start) → świeżość LQI, by nie pokazywać fałszywego offline.
    ae = _avail_eid(lqi_eid)
    return (f"var ae=states['{ae}'];"
            f"var fe=states['{lqi_eid}'];var fts=fe&&(fe.last_reported||fe.last_updated);"
            f"var lqfresh=fts&&((Date.now()-new Date(fts).getTime())/1000<{OFFLINE_S});"
            "var fresh=(ae&&ae.state!=='unavailable'&&ae.state!=='unknown')?(ae.state==='on'):lqfresh;")


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


def _slug(name):
    """Friendly name → z2m slug (sensor.<slug>_*)."""
    return name.lower().replace(' ', '_')


def lq_js(name):
    """JS: zmienne lq (LQI 0-255) + lqCol (kolor progowy) dla urządzenia po nazwie."""
    eid = f"sensor.{_slug(name)}_lqi"
    return (f"var lqe=states['{eid}'];"
            "var lq=lqe&&lqe.state!=='unavailable'&&lqe.state!==''&&lqe.state!=='unknown'?parseInt(lqe.state):null;"
            "var lqCol=lq===null?'#525252':(lq>=100?'#4ade80':(lq>=50?'#fbbf24':'#f87171'));"
            "var lqTxt=lq===null?'--':lq;")


def _avail_branch(eid, unavail, on, off, lqi_eid):
    # offline = Z2M unavailable LUB LQI nie raportowało > OFFLINE_S (świeżość z LQI = każdy raport).
    return ("[[[ var e=states['" + eid + "']; " + _fresh_js(lqi_eid)
            + "if(!e||e.state==='unavailable'||!fresh) return '" + unavail + "'; "
            "return e.state==='on'?'" + on + "':'" + off + "'; ]]]")


def switch_card(name, eid):
    lqi = f"sensor.{_slug(name)}_lqi"
    info = ("[[[ " + online_js(eid, lqi) + ls_js(eid) + lq_js(name) +
            "return `<div style=\"font-size:10px;color:#525252;margin-top:6px;\">"
            "<span style=\"color:${col};\">● ${label}</span>"
            " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span><br/>${lsT}</div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "toggle"},
        # C: switch ZAWSZE żarówka on/off (sterowalny tap→toggle→Z2M nawet offline);
        # offline sygnalizowane kolorem ikony + ramką (poniżej), nie zmianą na alert.
        "icon": "[[[ var e=states['" + eid + "']; "
                "return e&&e.state==='on'?'mdi:lightbulb-on':'mdi:lightbulb-off-outline'; ]]]",
        "custom_fields": {
            "badge": "[[[ return " + BADGE + "; ]]]",
            "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f", lqi)}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252", lqi)}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
            "custom_fields": {"badge": [{"position": "absolute"}, {"top": "10px"},
                                        {"right": "15px"}],
                              "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def alarm_card(name, eid, batt_eid, kind):
    on_icon = "mdi:water-alert" if kind == "leak" else "mdi:door-open"
    off_icon = "mdi:water" if kind == "leak" else "mdi:door-closed"
    lqi = f"sensor.{_slug(name)}_lqi"
    info = ("[[[ " + online_js(eid, lqi) + batt_js(batt_eid) + ls_js(eid) + lq_js(name) +
            "return `<div style=\"display:flex;flex-direction:column;gap:2px;font-size:10px;"
            "color:#525252;margin-top:6px;\"><div><span style=\"color:${col};\">● ${label}</span>"
            " · <span style=\"color:${battColor};\">🔋 ${batt}%</span>"
            " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div>"
            "<div>${lsT}</div></div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "name": name, "show_state": False, "tap_action": {"action": "none"},
        "icon": _avail_branch(eid, "mdi:alert-octagon", on_icon, off_icon, lqi),
        "custom_fields": {
            "badge": "[[[ return " + BADGE + "; ]]]",
            "info": info},
        "styles": {
            "card": [{"height": "120px"}, {"padding": "16px"},
                     {"border": _avail_branch(eid, "1px solid #f87171",
                                              "1px solid #facc15", "1px solid #1f1f1f", lqi)}],
            "icon": [{"width": "35px"}, {"height": "35px"},
                     {"color": _avail_branch(eid, "#f87171", "#facc15", "#525252", lqi)}],
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
    lqi = f"sensor.{slug}_lqi"
    content = (
        f"var t=states['{t}'];var h=states['{h}'];var b=states['{b}'];"
        f"var temp=t&&t.state!=='unavailable'?parseFloat(t.state).toFixed(1):'--';"
        f"var hum=h&&h.state!=='unavailable'?Math.round(parseFloat(h.state)):'--';"
        f"var batt=b&&b.state!=='unavailable'?b.state:'--';"
        f"{batt_js(b)}{online_js(t, lqi)}{ls_js(t)}{lq_js(name)}"
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
        " · <span style=\"color:${battColor};\">🔋 ${batt}%</span>"
        " · <span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div><div>${lsT}</div>"
        "</div></div></div>`;")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": t,
        "show_icon": False, "show_state": False, "show_name": False,
        "custom_fields": {"content": "[[[ %s ]]]" % content},
        "styles": {"card": [{"height": "112px"}, {"padding": "16px"},
                            {"border-radius": "12px"}, {"background": "#0a0a0a"},
                            {"border": _avail_branch(t, "1px solid #f87171",   # offline → czerwona ramka
                                                     "1px solid #1f1f1f", "1px solid #1f1f1f", lqi)}],
                   "icon": [{"display": "none"}], "name": [{"display": "none"}]}}


# ── PARAMETRY (6 pól edytowalnych + 2 przyciski Send, model v38) ──
# entity_id potwierdzone na żywo 2026-06-10 (HA generuje z nazwy, object_id ignorowany).
PARAM_FIELDS = [
    # FIX 2026-07-07: realne entity_id — HA tworzy je z device-name+entity-name (object_id
    # ignorowane); powstały po naprawie device.name w ParamSync + restarcie HA. + 6 progów.
    ("number.lora_gateway_g2_p1_stagnation_bateryjne",      "P1 · Stagnacja bateryjne [h]"),
    ("number.lora_gateway_g2_p2_stagnation_sieciowe",       "P2 · Stagnacja sieciowe [h]"),
    ("number.lora_gateway_g2_p3_raportowanie_temp",         "P3 · Raportowanie temp [min]"),
    ("number.lora_gateway_g2_t1_offline_switch_light",      "T1 · Offline switch/light [min]"),
    ("number.lora_gateway_g2_t2_offline_temp_hum",          "T2 · Offline temp/hum [min]"),
    ("number.lora_gateway_g2_t3_offline_door_leak_motion",  "T3 · Offline door/leak [min]"),
    ("number.lora_gateway_g2_th_prog_temp_wysoka",          "TH · Próg temp. wysoka [°C]"),
    ("number.lora_gateway_g2_tl_prog_temp_niska",           "TL · Próg temp. niska [°C]"),
    ("number.lora_gateway_g2_hh_prog_wilg_wysoka",          "HH · Próg wilg. wysoka [%]"),
    ("number.lora_gateway_g2_hl_prog_wilg_niska",           "HL · Próg wilg. niska [%]"),
    ("number.lora_gateway_g2_bl_prog_bateria_niska",        "BL · Próg bateria niska [%]"),
    ("number.lora_gateway_g2_bc_prog_bateria_krytyczna",    "BC · Próg bateria kryt. [%]"),
]
SEND_BTN_CONFIG = "button.lora_gateway_g2_lora_wyslij_config"
SEND_BTN_TIMEOUT = "button.lora_gateway_g2_lora_wyslij_timeout"


def send_button(name, eid, icon, color):
    return {"type": "custom:button-card", "template": "lora_btn", "name": name, "icon": icon,
            "tap_action": {"action": "call-service", "service": "button.press",
                           "service_data": {"entity_id": eid}},
            "styles": {"card": [{"border": f"1px solid {color}33"}],
                       "icon": [{"color": color}], "name": [{"color": color}]}}


def param_section():
    """Panel parametrów: 6 edytowalnych pól liczbowych + 2 przyciski wysyłki (push)."""
    return [
        {"type": "custom:button-card", "template": "lora_hdr", "name": "PARAMETRY"},
        {"type": "entities", "show_header_toggle": False,
         "entities": [{"entity": e, "name": n} for e, n in PARAM_FIELDS],
         "card_mod": {"style":
             "ha-card{background:#0a0a0a;border:1px solid #1f1f1f;border-radius:12px;"
             "box-shadow:none;padding:4px 6px;}"
             ".card-content{padding:4px 8px;}"
             "hui-number-entity-row,hui-generic-entity-row{padding:3px 4px;color:#e5e5e5;}"
             ".text-content{color:#a0a0a0;font-size:12px;}"
             "ha-textfield{--mdc-typography-subtitle1-font-size:13px;}"}},
        {"type": "horizontal-stack", "cards": [
            send_button("Wyślij Config", SEND_BTN_CONFIG, "mdi:cog-sync", "#4ade80"),
            send_button("Wyślij Timeout", SEND_BTN_TIMEOUT, "mdi:timer-cog", "#22d3ee")]},
    ]


def offline_count_tile():
    """Licznik OFFLINE z encji ...available (TO SAMO źródło co `a:`/supervisor) — zgodny z
    kaflami i z licznikiem supervisora. Fallback wieku LQI gdy encji jeszcze nie ma (start)."""
    avs = ([f"binary_sensor.{slug}_available" for _, slug in TEMPS]
           + [f"binary_sensor.{_slug(a[0])}_available" for a in ALARMS]
           + [f"binary_sensor.{_slug(n)}_available" for n, _ in SWITCHES])
    lqs = ([f"sensor.{slug}_lqi" for _, slug in TEMPS]
           + [f"sensor.{_slug(a[0])}_lqi" for a in ALARMS]
           + [f"sensor.{_slug(n)}_lqi" for n, _ in SWITCHES])
    content = ("[[[ var av=" + json.dumps(avs) + ";var lq=" + json.dumps(lqs) + ";var off=0;"
               "for(var i=0;i<av.length;i++){var a=states[av[i]];"
               "if(a&&a.state!=='unavailable'&&a.state!=='unknown'){if(a.state!=='on')off++;}"
               "else{var e=states[lq[i]];var ts=e&&(e.last_reported||e.last_updated);"
               "if(!e||!ts||((Date.now()-new Date(ts).getTime())/1000>=" + str(OFFLINE_S) + "))off++;}}"
               "var col=off>0?'#f87171':'#525252';"
               "return `<div style=\"display:flex;flex-direction:column;justify-content:center;height:100%;\">"
               "<span style=\"font-size:19px;font-weight:800;color:${col};\">${off}</span>"
               "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;"
               "margin-top:6px;\">OFFLINE</span></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "show_icon": False,
            "show_name": False, "show_state": False, "entity": avs[0],
            "tap_action": {"action": "none"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "14px 16px"}, {"height": "88px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


# ── HARMONOGRAM / KALENDARZ (STEP 4 — krok 16) ──
# Encje publikowane przez bramkę (test_step4/5): tryb produkcji + next change + hash + sloty,
# przyciski push/pull harmonogramu, oraz lokalny kalendarz odtworzony z ICS.
SCHED_MODE       = "sensor.lora_gateway_g2_gw_g2_tryb"
SCHED_MODE_NEXT  = "sensor.lora_gateway_g2_gw_g2_tryb_nastepna_zmiana"
SCHED_SLOTS      = "sensor.lora_gateway_g2_gw_g2_harmonogram_slotow"
SCHED_HASH       = "sensor.lora_gateway_g2_gw_g2_hash_kalendarza"
SCHED_BTN_PUSH   = "button.lora_gateway_g2_gw_g2_push_schedule_up"
SCHED_BTN_SYNC   = "button.lora_gateway_g2_gw_g2_sync_schedule"
SCHED_CALENDAR   = "calendar.lora_g2"
GW_MODE          = "sensor.lora_gateway_g2_gw_g2_tryb_pracy_bramki"  # STEP 5: day/night/all-time


def gw_mode_card():
    """STEP 5: tryb pracy bramki (Całodobowa/Dzienna/Nocna) + czy aktywna (supresja offline)."""
    content = (
        "[[[ var s=states['" + GW_MODE + "'];var lbl=s?s.state:'--';"
        "var a=s&&s.attributes?s.attributes:{};var ga=a.ga===undefined?1:a.ga;"
        "var act=a.active||(ga?'aktywna':'wstrzymana');var col=ga?'#4ade80':'#fbbf24';"
        "return `<div style=\"display:flex;flex-direction:column;gap:6px;width:100%;\">"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;\">TRYB PRACY BRAMKI</span>"
        "<span style=\"font-size:19px;font-weight:800;color:#e5e5e5;\">${lbl}</span>"
        "<span style=\"font-size:11px;font-weight:700;color:${col};\">● ${act}</span>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": GW_MODE,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "14px 16px"}, {"height": "100px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def schedule_view():
    """Widok Harmonogram: bieżący tryb produkcji + synchronizacja + lokalny kalendarz G2."""
    return {"path": "lora-schedule", "title": "Harmonogram", "icon": "mdi:calendar-clock",
            "cards": [{"type": "vertical-stack", "cards": [
                {"type": "custom:button-card", "template": "lora_hdr", "name": "TRYB PRACY"},
                gw_mode_card(),
                {"type": "custom:button-card", "template": "lora_hdr", "name": "HARMONOGRAM PRODUKCJI"},
                {"type": "horizontal-stack", "cards": [
                    stat_tile(SCHED_MODE, "TRYB", "mdi:factory"),
                    stat_tile(SCHED_MODE_NEXT, "NASTĘPNA ZMIANA", "mdi:clock-outline")]},
                {"type": "horizontal-stack", "cards": [
                    stat_tile(SCHED_SLOTS, "SLOTY", "mdi:calendar-multiple"),
                    stat_tile(SCHED_HASH, "HASH KALENDARZA", "mdi:fingerprint")]},
                {"type": "custom:button-card", "template": "lora_hdr", "name": "SYNCHRONIZACJA HARMONOGRAMU"},
                # PULL = pobierz z supervisora (sup→bramka), PUSH = wyślij w górę (bramka→sup).
                # Oba kierunki na dashboardzie bramki (parytet z supervisorem).
                {"type": "horizontal-stack", "cards": [
                    send_button("⬇ PULL — z supervisora", SCHED_BTN_SYNC, "mdi:calendar-arrow-left", "#4ade80"),
                    send_button("⬆ PUSH — do supervisora", SCHED_BTN_PUSH, "mdi:calendar-arrow-right", "#22d3ee")]},
                {"type": "custom:button-card", "template": "lora_hdr", "name": "KALENDARZ G2 (LOKALNY)"},
                {"type": "calendar", "initial_view": "listWeek", "entities": [SCHED_CALENDAR],
                 "card_mod": {"style": "ha-card{background:#0a0a0a;border:1px solid #1f1f1f;"
                                       "border-radius:12px;box-shadow:none;overflow:hidden;}"}},
            ]}]}


def day_night_card():
    """STEP 5+: WIELKA ikona Dzień/Noc + świt/zmierzch (z sensor.lora_g2_sun_period/_sunrise/_sunset).
    Pora dnia liczona LOKALNIE na bramce (lat/lon + zegar) — niezależnie od HA."""
    P = "sensor.lora_gateway_g2_gw_g2_pora_dnia"
    SR = "sensor.lora_gateway_g2_gw_g2_swit"
    SS = "sensor.lora_gateway_g2_gw_g2_zmierzch"
    content = (
        "[[[ var p=states['" + P + "'];var st=p?p.state:'--';"
        "var day=/Dzie/i.test(st);"
        "var sr=states['" + SR + "'];var ss=states['" + SS + "'];"
        "var srT=sr?sr.state:'--';var ssT=ss?ss.state:'--';"
        "var icon=day?'☀️':'🌙';var col=day?'#fbbf24':'#c084fc';var lbl=day?'DZIEŃ':'NOC';"
        "return `<div style=\"display:flex;align-items:center;gap:18px;width:100%;\">"
        "<div style=\"font-size:54px;line-height:1;filter:drop-shadow(0 0 12px ${col}55);\">${icon}</div>"
        "<div style=\"display:flex;flex-direction:column;gap:5px;\">"
        "<span style=\"font-size:26px;font-weight:800;color:${col};letter-spacing:3px;\">${lbl}</span>"
        "<span style=\"font-size:11px;color:#525252;\">🌅 świt <span style=\"color:#e5e5e5;font-weight:700;\">${srT}</span>"
        "  ·  🌇 zmierzch <span style=\"color:#e5e5e5;font-weight:700;\">${ssT}</span></span>"
        "</div></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": P,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "18px 20px"}, {"height": "108px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def time_compare_card():
    """STEP 5+: porównanie CZAS BRAMKI vs CZAS SUPERVISORA (wyliczony z offsetu) + jakość czasu."""
    LT = "sensor.lora_gateway_g2_gw_g2_czas_bramki"
    OFF = "sensor.gw_g2_time_offset"
    TQ = "sensor.lora_gateway_g2_gw_g2_jakosc_czasu"
    content = (
        "[[[ var lt=states['" + LT + "'];var ltT=lt?lt.state:'--';"
        "var off=states['" + OFF + "'];var offS=off?off.state:'--';"
        "var tq=states['" + TQ + "'];var tqS=tq?tq.state:'--';"
        "var m=/([+-]?\\d+)/.exec(offS||'');var supT='—';"
        "if(m&&ltT.indexOf(':')>=0){var pr=ltT.split(':');var d=new Date();"
        "d.setHours(+pr[0],+pr[1],+pr[2]||0);d=new Date(d.getTime()-(+m[1])*1000);"
        "supT=('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2)+':'+('0'+d.getSeconds()).slice(-2);}"
        "var tqCol=/Zsynchron/i.test(tqS)?'#4ade80':(/Holdover/i.test(tqS)?'#fbbf24':'#f87171');"
        "return `<div style=\"display:flex;flex-direction:column;gap:12px;width:100%;\">"
        "<div style=\"display:flex;justify-content:space-between;gap:14px;\">"
        "<div style=\"display:flex;flex-direction:column;\">"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;\">CZAS BRAMKI</span>"
        "<span style=\"font-size:23px;font-weight:800;color:#22d3ee;font-variant-numeric:tabular-nums;\">${ltT}</span></div>"
        "<div style=\"display:flex;flex-direction:column;text-align:right;\">"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;\">CZAS SUPERVISORA</span>"
        "<span style=\"font-size:23px;font-weight:800;color:#22d3ee;font-variant-numeric:tabular-nums;\">${supT}</span></div></div>"
        "<div style=\"display:flex;justify-content:space-between;font-size:10px;color:#525252;\">"
        "<span>jakość: <span style=\"color:${tqCol};font-weight:700;\">${tqS}</span></span>"
        "<span>Δ offset: <span style=\"color:#22d3ee;font-weight:700;\">${offS}</span></span></div>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": LT,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "16px 20px"}, {"height": "108px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


# ── ANOMALIE (liczniki bramki z atrybutami items — TO SAMO źródło co supervisor) ──
ANOM_OFF = "sensor.lora_gateway_g2_gw_g2_anomalie_offline"
ANOM_BAT = "sensor.lora_gateway_g2_gw_g2_anomalie_bateria"
ANOM_OTH = "sensor.lora_gateway_g2_gw_g2_anomalie_inne"
GW_PING = "button.lora_gateway_g2_gw_g2_ping"
GW_DISC = "button.lora_gateway_g2_gw_g2_discovery"
SEND_BTN_PROGI = "button.lora_gateway_g2_lora_wyslij_progi"
VIO_SW = "switch.lora_virtual_i_o_g2_lora_test_switch"
VIO_BTN = "button.lora_virtual_i_o_g2_lora_test_button"
TQ_EID = "sensor.lora_gateway_g2_gw_g2_jakosc_czasu"


def anom_tile(eid, label, icon):
    """Licznik anomalii: czerwony gdy >0, szary gdy 0 (kolorystyka v10)."""
    content = ("[[[ var s=states['" + eid + "'];var n=s?parseInt(s.state)||0:0;"
               "var col=n>0?'#f87171':'#4ade80';"
               "return `<div style=\"display:flex;flex-direction:column;justify-content:center;height:100%;\">"
               "<span style=\"font-size:19px;font-weight:800;color:${col};\">${n}</span>"
               "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;"
               "margin-top:6px;\">" + label + "</span></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": eid,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "14px 16px"}, {"height": "88px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def anom_list_card():
    """Lista aktywnych anomalii z ATRYBUTÓW items 3 liczników (jedno źródło z MQTT/store).
    Max 25 wierszy + licznik reszty; pusta lista = zielone ✓."""
    ents = json.dumps([ANOM_OFF, ANOM_BAT, ANOM_OTH])
    content = (
        "[[[ var ents=" + ents + ";var rows='';var tot=0;"
        "for(var k=0;k<ents.length;k++){var s=states[ents[k]];if(!s)continue;"
        "var it=(s.attributes&&s.attributes.items)||[];"
        "for(var i=0;i<it.length;i++){tot++;if(tot>25)continue;var r=it[i];"
        "var v=(r.value!==undefined&&r.value!==null)?' · '+r.value:'';"
        "rows+=`<div style=\"display:flex;justify-content:space-between;padding:4px 0;"
        "border-bottom:1px solid #1f1f1f;\">"
        "<span style=\"color:#e5e5e5;font-size:11px;font-weight:700;\">${r.dev||'?'}</span>"
        "<span style=\"color:#f87171;font-size:10px;font-weight:800;letter-spacing:1px;\">"
        "${(r.type||'').toUpperCase()}${v}</span></div>`;}}"
        "if(tot===0)rows='<div style=\"color:#4ade80;font-size:12px;font-weight:700;padding:6px 0;\">"
        "✓ BRAK AKTYWNYCH ANOMALII</div>';"
        "var more=tot>25?`<div style=\"color:#525252;font-size:10px;padding-top:6px;\">… +${tot-25} kolejnych</div>`:'';"
        "return `<div style=\"width:100%;max-height:420px;overflow-y:auto;\">${rows}${more}</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": ANOM_OFF,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "12px 16px"}],
                       "grid": [{"grid-template-columns": "1fr"}],
                       "custom_fields": {"content": [{"justify-self": "stretch"}, {"width": "100%"}]}}}


def entities_dark(rows):
    """Natywna karta entities w ciemnym stylu v10 (spójna z param_section)."""
    return {"type": "entities", "show_header_toggle": False, "entities": rows,
            "card_mod": {"style":
                "ha-card{background:#0a0a0a;border:1px solid #1f1f1f;border-radius:12px;"
                "box-shadow:none;padding:4px 6px;}"
                ".card-content{padding:4px 8px;}"
                "hui-generic-entity-row,hui-toggle-entity-row{padding:3px 4px;color:#e5e5e5;}"
                ".text-content{color:#a0a0a0;font-size:12px;}"}}


def build_config():
    """7 zakładek — KAŻDA = jedna funkcja (2026-07-07, przebudowa na życzenie usera):
    Bramka(status) / Harmonogram / Sterowanie / Pomiary / Alarmy / Parametry / Diagnostyka.
    Wszystkie encje bindowane do REALNYCH id z HA (inwentaryzacja via REST — zero „Nie znaleziono")."""
    # 1) BRAMKA — status łącza + statystyki + hashe (sam status, bez paneli funkcyjnych)
    gwv = {"path": "lora-gw-stats", "title": "Bramka", "icon": "mdi:radio-tower",
           "cards": [{"type": "vertical-stack", "cards": [
               {"type": "custom:button-card", "template": "lora_hdr", "name": "BRAMKA G2"},
               link_card(),
               {"type": "custom:button-card", "template": "lora_hdr", "name": "STATYSTYKI URZĄDZEŃ"},
               {"type": "horizontal-stack", "cards": [
                   stat_tile("sensor.lora_gateway_g2_total", "TOTAL", "mdi:devices"),
                   stat_tile("sensor.gw_g2_monitored", "MONITORED", "mdi:eye"),
                   stat_tile("sensor.lora_gateway_g2_priority", "PRIORITY", "mdi:alert-octagon"),
                   stat_tile("sensor.gw_g2_offline", "OFFLINE", "mdi:lan-disconnect")]},
               {"type": "horizontal-stack", "cards": [
                   stat_tile("sensor.gw_g2_uptime", "UPTIME", "mdi:timer"),
                   stat_tile("sensor.gw_g2_last_hb", "OSTATNI HB", "mdi:heart-pulse")]},
               {"type": "custom:button-card", "template": "lora_hdr", "name": "SYNCHRONIZACJA"},
               {"type": "horizontal-stack", "cards": [
                   stat_tile("sensor.gw_g2_hash_parametrow", "HASH PARAM", "mdi:tune-variant"),
                   stat_tile("sensor.gw_g2_hash_disc", "HASH DISC", "mdi:fingerprint")]},
           ]}]}
    # 3) STEROWANIE — przekaźniki + wirtualne I/O + akcje LoRa (ping/discovery)
    control = {"path": "lora-control", "title": "Sterowanie", "icon": "mdi:lightbulb",
               "cards": [{"type": "vertical-stack", "cards": [
                   {"type": "custom:button-card", "template": "lora_hdr", "name": "PRZEKAŹNIKI"},
                   {"type": "horizontal-stack",
                    "cards": [switch_card(n, e) for n, e in SWITCHES]},
                   {"type": "custom:button-card", "template": "lora_hdr", "name": "WIRTUALNE I/O"},
                   entities_dark([{"entity": VIO_SW, "name": "VSwitch Test (dwustronny)"},
                                  {"entity": VIO_BTN, "name": "VButton Test (impuls)"}]),
                   {"type": "custom:button-card", "template": "lora_hdr", "name": "AKCJE LoRa"},
                   {"type": "horizontal-stack", "cards": [
                       send_button("Ping (test łącza)", GW_PING, "mdi:radar", "#22d3ee"),
                       send_button("Discovery (mapa)", GW_DISC, "mdi:magnify-scan", "#4ade80")]},
               ]}]}
    # 4) POMIARY — temperatury/wilgotność + wykresy + czujniki priority + pozostałe
    sensor_cards = [{"type": "custom:button-card", "template": "lora_hdr", "name": "TEMPERATURA & WILGOTNOŚĆ"}]
    for name, slug in TEMPS:
        sensor_cards.append(temp_stat(name, slug))
        sensor_cards.append({"type": "horizontal-stack", "cards": [
            chart(f"sensor.{slug}_temperature", "Temperatura", "#22d3ee", 1),
            chart(f"sensor.{slug}_humidity", "Wilgotność", "#4ade80", 0, 0, 100)]})
    sensor_cards += [
        {"type": "custom:button-card", "template": "lora_hdr", "name": "CZUJNIKI PRIORITY"},
        {"type": "horizontal-stack", "cards": [alarm_card(*a) for a in ALARMS]},
        *other_section(),
    ]
    sensors = {"path": "lora-sensors", "title": "Pomiary", "icon": "mdi:thermometer",
               "cards": [{"type": "vertical-stack", "cards": sensor_cards}]}
    # 5) ALARMY — liczniki anomalii + żywa lista z atrybutów MQTT (jedno źródło ze store bramki)
    alarms = {"path": "lora-alarms", "title": "Alarmy", "icon": "mdi:fire-alert",
              "cards": [{"type": "vertical-stack", "cards": [
                  {"type": "custom:button-card", "template": "lora_hdr", "name": "LICZNIKI ANOMALII"},
                  {"type": "horizontal-stack", "cards": [
                      anom_tile(ANOM_OFF, "OFFLINE", "mdi:lan-disconnect"),
                      anom_tile(ANOM_BAT, "BATERIA", "mdi:battery-alert"),
                      anom_tile(ANOM_OTH, "INNE (TEMP/WILG/…)", "mdi:alert")]},
                  {"type": "custom:button-card", "template": "lora_hdr", "name": "AKTYWNE ANOMALIE (LISTA)"},
                  anom_list_card(),
              ]}]}
    # 6) PARAMETRY — osobna zakładka: 12 pól + 3 przyciski wysyłki + hash
    params = {"path": "lora-params", "title": "Parametry", "icon": "mdi:tune",
              "cards": [{"type": "vertical-stack", "cards": [
                  *param_section(),
                  {"type": "horizontal-stack", "cards": [
                      send_button("Wyślij Progi", SEND_BTN_PROGI, "mdi:thermometer-alert", "#fbbf24"),
                      stat_tile("sensor.gw_g2_hash_parametrow", "HASH PARAM", "mdi:tune-variant")]},
              ]}]}
    # 7) DIAGNOSTYKA — czas/pora dnia, jakość czasu, łączność z supervisorem
    diag = {"path": "lora-diag", "title": "Diagnostyka", "icon": "mdi:stethoscope",
            "cards": [{"type": "vertical-stack", "cards": [
                {"type": "custom:button-card", "template": "lora_hdr", "name": "CZAS & PORA DNIA"},
                {"type": "horizontal-stack", "cards": [day_night_card(), time_compare_card()]},
                {"type": "custom:button-card", "template": "lora_hdr", "name": "JAKOŚĆ CZASU & SYNC"},
                {"type": "horizontal-stack", "cards": [
                    stat_tile(TQ_EID, "JAKOŚĆ CZASU", "mdi:clock-check"),
                    stat_tile("sensor.gw_g2_time_offset", "OFFSET", "mdi:clock-alert"),
                    stat_tile("sensor.gw_g2_last_sync", "OSTATNI SYNC", "mdi:clock-sync")]},
                {"type": "custom:button-card", "template": "lora_hdr", "name": "ŁĄCZNOŚĆ Z SUPERVISOREM"},
                {"type": "horizontal-stack", "cards": [
                    stat_tile("sensor.gw_g2_supervisor_last_rx", "OSTATNI RX OD SUP", "mdi:download-network"),
                    stat_tile("sensor.gw_g2_last_hb", "OSTATNI HB → SUP", "mdi:heart-pulse")]},
            ]}]}
    return {"title": "LoRa Gateway G2", "button_card_templates": TEMPLATES,
            "views": [gwv, schedule_view(), control, sensors, alarms, params, diag]}


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
