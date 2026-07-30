#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""⚠️ LEGACY — NIE URUCHAMIAJ NA PRODUKCJI BEZ DIFFA (decyzja usera 2026-07-30).
Kanon = stan LIVE w `dashboards_baseline/live_sup_lora-scada-g1.json` (`dash_sync.py pull|push`).
Ten generator odtwarza układ sprzed ręcznych poprawek na żywym HA i by je skasował.

Dashboard SUPERVISORA dla G1 (carport, 2026-07-22) — KLON gen_unified_dashboard.py (GW=G2)
z GW=G1, oddzielny url_path (ZERO ryzyka dla działającego dashboardu G2), + 8. zakładka
"Przekaźniki" (105× ZBMINIR2) z layoutem 1:1 przeniesionym z lokalnego dashboardu bramki G1
(lovelace.carport_dashboard: button_card_templates carport_relay/carport_motion, grupowanie RA-area).

G1 nie ma Test 1/2, Temp 1/2, Leak/Door (SWITCHES/TEMPS/ALARM_SENSORS puste) — te zakładki
istnieją (spójność z G2) ale są prawie puste (brak takich urządzeń na carporcie). Zakładka
Przekaźniki pobiera AKTUALNĄ listę switch.lora_g1_* z HA (get_states przez to samo websocket) —
gdy uruchomisz PO discovery (lista monitored/urządzeń z bramki dotarła), auto-dopasuje się do
prawdziwych encji. Uruchom ponownie w każdej chwili, żeby odświeżyć listę przekaźników.

Wdrożenie: dashboard url_path='lora-scada-g1'. Uruchom NA supervisorze: venv/bin/python gen_g1_dashboard.py
"""
import asyncio, json, os, re, sys
import websockets

sys.path.insert(0, os.path.expanduser("~/meshtastic"))
from config import CONFIG, ROLE          # noqa: E402

HA = "ws://localhost:8123/api/websocket"
TOKEN = os.environ.get("HA_TOKEN") or (CONFIG.get("ha_api") or {}).get("token", "")
URL_PATH = "lora-scada-g1"
GW = "G1"
GL = GW.lower()
N_ROWS = 40                              # wierszy w popupie/liście (reszta: licznik + scroll)

PARAM_LABELS = [
    ("p1_stagnation_bateryjne",     "P1 · Stagnacja bateryjne [h]"),
    ("p2_stagnation_sieciowe",      "P2 · Stagnacja sieciowe [h]"),
    ("p3_raportowanie_temp",        "P3 · Raportowanie temp [min]"),
    ("t1_offline_switch_light",     "T1 · Offline switch/light [min]"),
    ("t2_offline_temp_hum",         "T2 · Offline temp/hum [min]"),
    ("t3_offline_door_leak_motion", "T3 · Offline door/leak [min]"),
    ("th_prog_temp_wysoka",         "TH · Próg temp. wysoka [°C]"),
    ("tl_prog_temp_niska",          "TL · Próg temp. niska [°C]"),
    ("hh_prog_wilg_wysoka",         "HH · Próg wilg. wysoka [%]"),
    ("hl_prog_wilg_niska",          "HL · Próg wilg. niska [%]"),
    ("bl_prog_bateria_niska",       "BL · Próg bateria niska [%]"),
    ("bc_prog_bateria_krytyczna",   "BC · Próg bateria kryt. [%]"),
]

# ── mapa encji/topiców — supervisor patrzący na G1 (jak G2, tylko GL='g1') ──
# G1 (carport produkcyjny) NIE ma Test 1/2 / Temp 1/2 / Leak 1 / Door 1 — puste listy = zakładki
# Sterowanie/Pomiary generyczne zostają (spójność z G2), ale renderują się prawie puste.
E = dict(
    status=f"binary_sensor.lora_gw_{GL}_status",
    last_seen=f"sensor.lora_gw_{GL}_last_seen",
    total=f"sensor.lora_gw_{GL}_devices_total",
    mon=f"sensor.lora_gw_{GL}_devices_monitored",
    pri=f"sensor.lora_gateway_{GL}_gw_{GL}_priority",
    cnt_off=f"sensor.lora_gw_{GL}_devices_offline",
    cnt_bat=f"sensor.lora_gw_{GL}_devices_low_battery",
    cnt_oth=f"sensor.lora_gw_{GL}_devices_anomaly",
    it_off=f"sensor.lora_gateway_{GL}_gw_{GL}_offline_anomalies",
    it_bat=f"sensor.lora_gateway_{GL}_gw_{GL}_battery_anomalies",
    it_oth=f"sensor.lora_gateway_{GL}_gw_{GL}_other_anomalies",
    tq=f"sensor.lora_gateway_{GL}_gw_{GL}_jakosc_czasu",
    pora=f"sensor.lora_gateway_{GL}_gw_{GL}_pora_dnia_tryb",
    tryb_stan=f"sensor.lora_gateway_{GL}_gw_{GL}_tryb_stan",
    hash_disc=f"sensor.lora_gw_{GL}_disc_hash",
    hash_param=f"sensor.lora_gateway_{GL}_gw_{GL}_hash_param_bramka",
    uptime=f"sensor.lora_gw_{GL}_uptime",
    calendar=f"calendar.lora_{GL}",
    vio_sw=f"switch.lora_virtual_i_o_{GL}_lora_test_switch",
    vio_btn=f"button.lora_virtual_i_o_{GL}_lora_test_button",
)
SWITCHES = []          # brak Test 1/2 na G1
TEMPS = []             # brak Temp 1/2 na G1
ALARM_SENSORS = []     # brak Leak/Door na G1
PARAM_FIELDS = []                    # numbery params G1 nie istnieją na sup — edycja na bramce
SEND_BTNS = [("WYŚLIJ CONFIG", "mdi:cog-sync",
              ("mqtt", f"lora/params/supervisor/cmd/{GL}/send_config", ""))]
ACT = [
    ("PING",  "mdi:lan-connect", "#22d3ee", "#164e63",
     ("btn", f"button.lora_gw_{GL}_ping"), None),
    ("DISC",  "mdi:magnify", "#22d3ee", "#164e63",
     ("btn", f"button.lora_gw_{GL}_discovery"), None),
    ("SYNC",  "mdi:calendar-sync", "#22d3ee", "#164e63",
     ("mqtt", f"lora/supervisor/cmd/{GL}/calendar", ""), "Synchronizować kalendarz?"),
    ("DUMP",  "mdi:alert-circle-outline", "#fbbf24", "#713f12",
     ("mqtt", f"lora/supervisor/cmd/{GL}/dump_anom", ""), None),
    ("CLEAR", "mdi:bell-off", "#f87171", "#7f1d1d",
     ("mqtt", "lora/supervisor/cmd/clear_offline", ""), f"Wyczyścić anomalie {GW}?"),
    # 6. przycisk = CZAS na WSZYSTKICH dashboardach (decyzja usera 2026-07-30). CFG zostaje w
    # zakładce Parametry (WYŚLIJ CONFIG). MQTT wprost na topic, NIE button.press: entity_id tej
    # samej encji różni się per bramka (rejestr HA trzyma historyczne id), topic jest stały.
    ("CZAS",  "mdi:clock-sync", "#22d3ee", "#164e63",
     ("mqtt", f"lora/supervisor/cmd/{GL}/sync", ""), None),
]
CAL_ACTS = [("PUSH → BRAMKA", "mdi:calendar-arrow-right", "#22d3ee",
             ("mqtt", f"lora/supervisor/cmd/{GL}/calendar", ""), "Wysłać kalendarz do bramki?"),
            # Symetria per-bramka (decyzja usera 2026-07-30): pull nadpisuje calendar.lora_<gl>
            # na supie stanem bramki (cmd/<gl>/pull_calendar → cal_pull → gw push-up).
            ("PULL ← BRAMKA", "mdi:calendar-arrow-left", "#22d3ee",
             ("mqtt", f"lora/supervisor/cmd/{GL}/pull_calendar", ""),
             f"Pobrać harmonogram z bramki {GW} (nadpisze calendar.lora_{GL})?"),
            ("PUSH ALL", "mdi:calendar-multiple", "#fbbf24",
             ("btn", "button.sync_calendar_all"), "Wypchnąć kalendarz do WSZYSTKICH bramek?")]
DIAG_EXTRA = [("SYNC CZASU", "mdi:clock-check",
               ("mqtt", f"lora/supervisor/cmd/{GL}/sync", ""))]
CLEAR_TOPIC = "lora/supervisor/cmd/clear_anomaly"          # {gw,dev,bucket}
CLEAR_ALL = {"offline": ("mqtt", "lora/supervisor/cmd/clear_offline", ""),
             "battery": ("mqtt", "lora/supervisor/cmd/clear_battery", ""),
             "other":   ("mqtt", "lora/supervisor/cmd/clear_other", "")}


def row_payload_js(items_e, i):
    return (f"[[[ var it=(states['{items_e}'].attributes.items||[])[{i}];"
            f"return it?JSON.stringify({{gw:it.gw||'{GW}',dev:it.dev,bucket:'BUCKET'}}):''; ]]]")


# ── button_card_templates (pierwotny v10 + carport_relay/carport_motion z lokalnego
#    dashboardu bramki G1 — lovelace.carport_dashboard, PRZENIESIONE 1:1, tylko poprawiony
#    wskaźnik offline: lokalnie liczył po entity.state==='unavailable' (Z2M natywne), tu po
#    companion binary_sensor.._available (konwencja LoRa-mirror, patrz sup_switch_card niżej) ──
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
    "lora_btn": {"template": "lora_base", "show_state": False, "styles": {
        "card": [{"padding": "14px 12px"}, {"height": "58px"}],
        "name": [{"font-size": "11px"}, {"font-weight": 800}, {"letter-spacing": "1.5px"},
                 {"text-transform": "uppercase"}, {"white-space": "nowrap"},
                 {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
        "icon": [{"width": "20px"}]}},
    "carport_relay": {
        "template": "lora_base", "show_state": False, "show_label": False,
        "tap_action": {"action": "toggle"},
        "icon": "[[[ return entity.state==='on'?'mdi:lightbulb-on':'mdi:lightbulb-off-outline'; ]]]",
        "custom_fields": {"info": (
            "[[[ var e=entity;var slug=e?e.entity_id.replace(/^switch\\./,''):'';"
            "var av=states['binary_sensor.'+slug+'_available'];"
            "var lqe=states['sensor.'+slug+'_link_quality'];"
            "var lq=(lqe&&lqe.state!=='unknown'&&lqe.state!=='unavailable')?parseInt(lqe.state):null;"
            "var off=(!e||!av||av.state==='off');"
            "var lc=e?new Date(e.last_changed):null;"
            "var t=lc?(('0'+lc.getDate()).slice(-2)+'.'+('0'+(lc.getMonth()+1)).slice(-2)+' '+"
            "('0'+lc.getHours()).slice(-2)+':'+('0'+lc.getMinutes()).slice(-2)):'--';"
            "var lqs=(lq!=null)?lq:'--';"
            "var dot=off?'<span style=\"color:#f87171;font-weight:800;\">● OFFLINE</span>':"
            "'<span style=\"color:#4ade80;\">● online</span>';"
            "return '<div style=\"display:flex;flex-direction:column;gap:2px;font-size:9px;"
            "color:#737373;line-height:1.3;white-space:nowrap;\">'+"
            "'<span>\U0001F550 '+t+'   \U0001F4F6 '+lqs+'</span>'+"
            "'<span>'+dot+'</span></div>'; ]]]")},
        "styles": {
            "card": [{"height": "90px"}, {"padding": "9px 12px"},
                     {"border": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')"
                                "return '1px solid #f87171'; return e.state==='on'?"
                                "'1px solid #facc15':'1px solid #1f1f1f'; ]]]"}],
            "grid": [{"grid-template-areas": '"i n" "i info"'},
                     {"grid-template-columns": "24px 1fr"},
                     {"grid-template-rows": "auto auto"},
                     {"align-items": "center"}, {"column-gap": "10px"}, {"row-gap": "3px"}],
            "icon": [{"width": "22px"}, {"height": "22px"},
                     {"color": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')"
                               "return '#f87171'; return e.state==='on'?'#facc15':'#525252'; ]]]"}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"},
                     {"justify-self": "start"}, {"white-space": "nowrap"}, {"overflow": "hidden"},
                     {"text-overflow": "ellipsis"}],
            "custom_fields": {"info": [{"justify-self": "start"}]}}},
    "carport_motion": {
        "template": "lora_base", "show_state": False, "show_label": False,
        "tap_action": {"action": "more-info"}, "icon": "mdi:motion-sensor",
        "custom_fields": {"info": (
            "[[[ var e=entity;var slug=e?e.entity_id.replace(/^binary_sensor\\./,'')"
            ".replace(/_occupancy$/,''):'';"
            "var av=states['binary_sensor.'+slug+'_available'];"
            "var lqe=states['sensor.'+slug+'_link_quality'];"
            "var lq=(lqe&&lqe.state!=='unknown'&&lqe.state!=='unavailable')?parseInt(lqe.state):null;"
            "var off=(!e||!av||av.state==='off');"
            "var lc=e?new Date(e.last_changed):null;"
            "var t=lc?(('0'+lc.getDate()).slice(-2)+'.'+('0'+(lc.getMonth()+1)).slice(-2)+' '+"
            "('0'+lc.getHours()).slice(-2)+':'+('0'+lc.getMinutes()).slice(-2)):'--';"
            "var lqs=(lq!=null)?lq:'--';"
            "var dot=off?'<span style=\"color:#f87171;font-weight:800;\">● OFFLINE</span>':"
            "'<span style=\"color:#4ade80;\">● online</span>';"
            "var beid='sensor.'+slug+'_batt';var be=states[beid];"
            "var bs=(be&&be.state!=='unknown'&&be.state!=='unavailable')?be.state+'%':'--';"
            "return '<div style=\"display:flex;flex-direction:column;gap:2px;font-size:9px;"
            "color:#737373;line-height:1.3;white-space:nowrap;\">'+"
            "'<span>\U0001F550 '+t+'   \U0001F4F6 '+lqs+'</span>'+"
            "'<span>\U0001F50B '+bs+'  '+dot+'</span></div>'; ]]]")},
        "styles": {
            "card": [{"height": "90px"}, {"padding": "9px 12px"},
                     {"border": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')"
                                "return '1px solid #f87171'; return e.state==='on'?"
                                "'1px solid #4ade80':'1px solid #1f1f1f'; ]]]"}],
            "grid": [{"grid-template-areas": '"i n" "i info"'},
                     {"grid-template-columns": "24px 1fr"},
                     {"grid-template-rows": "auto auto"},
                     {"align-items": "center"}, {"column-gap": "10px"}, {"row-gap": "3px"}],
            "icon": [{"width": "22px"}, {"height": "22px"},
                     {"color": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')"
                               "return '#f87171'; return e.state==='on'?'#4ade80':'#525252'; ]]]"}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"},
                     {"justify-self": "start"}, {"white-space": "nowrap"}, {"overflow": "hidden"},
                     {"text-overflow": "ellipsis"}],
            "custom_fields": {"info": [{"justify-self": "start"}]}}},
}

CARD_MOD_DARK = {"style": "ha-card{background:#0a0a0a;border:1px solid #1f1f1f;"
                          "border-radius:12px;box-shadow:none;overflow:hidden;}"}


def hdr(name):
    return {"type": "custom:button-card", "template": "lora_hdr", "name": name}


def tap_of(action, confirm=None):
    kind = action[0]
    if kind == "btn":
        tap = {"action": "call-service", "service": "button.press",
               "service_data": {"entity_id": action[1]}}
    else:
        tap = {"action": "call-service", "service": "mqtt.publish",
               "service_data": {"topic": action[1], "payload": action[2]}}
    if confirm:
        tap["confirmation"] = {"text": confirm}
    return tap


def gw_header_card():
    content = (
        "[[[ var on = entity.state === 'on';"
        f"var t = states['{E['total']}'];var m = states['{E['mon']}'];"
        f"var p = states['{E['pri']}'];var ls = states['{E['last_seen']}'];"
        "var dC = on ? '#4ade80' : '#f87171';var sT = on ? 'ONLINE' : 'OFFLINE';"
        "var tot = t ? t.state : '0';var mon = m ? m.state : '0';var prio = p ? p.state : '0';"
        "var lsT = ls && ls.state !== 'unavailable' ? ls.state : '--';"
        f"var gwId = '{GW}';"
        "return `<div style=\"display:flex; flex-direction:column; height:100%; width:100%;\">"
        "<div style=\"display:grid; grid-template-columns:0.75fr auto; margin-bottom:5px;\">"
        "<div><span style=\"background:#141414; border:2px solid #22d3ee; padding:2px 15px;"
        " border-radius:5px; font-size:14px; font-weight:800; color:#22d3ee;\">${gwId}</span></div>"
        "<div style=\"display:flex; flex-direction:column; align-items:center;\">"
        "<div style=\"display:flex; align-items:center; gap:4px;\">"
        "<span style=\"color:${dC}; font-size:12px;\">●</span>"
        "<span style=\"font-size:11px; font-weight:700; color:${dC};\">${sT}</span></div>"
        "<span style=\"font-size:10px; color:#525252; margin-top:2px;\">${lsT}</span></div></div>"
        "<div style=\"display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:5px;\">"
        + "".join(
            "<div style=\"background:#141414; border:1px solid #1f1f1f; border-radius:10px;"
            " height:64px; display:flex; flex-direction:column; justify-content:center;"
            " align-items:center;\">"
            f"<div style=\"font-size:24px; font-weight:800; color:{c};\">${{{v}}}</div>"
            f"<div style=\"font-size:9px; color:#525252; margin-top:5px;\">{lbl}</div></div>"
            for v, lbl, c in (("tot", "TOTAL", "#e5e5e5"),
                              ("mon", "MONITORED", "#22d3ee"),
                              ("prio", "PRIORITY", "#edad37")))
        + "</div></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": E["status"],
            "show_name": False, "show_state": False, "show_icon": False,
            "tap_action": {"action": "none"}, "custom_fields": {"content": content},
            "styles": {"grid": [{"grid-template-areas": '"content"'},
                                {"grid-template-columns": "1fr"}, {"grid-template-rows": "1fr"}],
                       "custom_fields": {"content": [{"height": "100%"}, {"width": "100%"}]},
                       "card": [{"height": "130px"}, {"padding": "14px"},
                                {"border-radius": "12px"}, {"background": "#0a0a0a"}]}}


def action_btn(name, icon, col, border, action, confirm):
    return {"type": "custom:button-card", "template": "lora_btn", "name": name, "icon": icon,
            "tap_action": tap_of(action, confirm),
            "styles": {"card": [{"border": f"1px solid {border}"}],
                       "icon": [{"color": col}], "name": [{"color": col}]}}


def actions_grid():
    cols = []
    for pair in (ACT[0:2], ACT[2:4], ACT[4:6]):
        cols.append({"type": "vertical-stack",
                     "cards": [action_btn(*a) for a in pair]})
    return {"type": "horizontal-stack", "cards": cols}


BUCKETS = {
    "offline": dict(cnt=E["cnt_off"], items=E["it_off"], color="#f87171",
                    icon="mdi:lan-disconnect", label="OFFLINE", stat_ic="mdi:lan-disconnect",
                    row_lbl="'🔴 OFFLINE'"),
    "battery": dict(cnt=E["cnt_bat"], items=E["it_bat"], color="#fbbf24",
                    icon="mdi:battery-alert", label="LOW BATT", stat_ic="mdi:battery-low",
                    row_lbl="((it.type||'')==='critical_battery'?'🔴 CRIT BATT':'🟡 LOW BATT')"),
    "other":   dict(cnt=E["cnt_oth"], items=E["it_oth"], color="#c084fc",
                    icon="mdi:alert", label="OTHER", stat_ic="mdi:alert",
                    row_lbl="('🟣 '+String(it.type||'?').toUpperCase())"),
}


def popup_row(bucket, i):
    b = BUCKETS[bucket]
    ie = b["items"]
    content = (
        f"[[[ var items=(states['{ie}'].attributes.items)||[];var it=items[{i}];"
        "if(!it) return '';var nm=it.dev||'?';"
        "var v=(it.value!==undefined&&it.value!==null)?('  ·  📊 '+it.value):'';"
        "var ts=it.detected_at||it.ts||it.since||'';"
        "var tss=ts?String(ts).slice(5,16):'—';"
        f"var lbl={b['row_lbl']};var gw=it.gw||'{GW}';"
        "return `<div style=\"display:flex;align-items:center;width:100%;box-sizing:border-box;gap:14px;\">"
        f"<ha-icon icon=\"{b['icon']}\" style=\"color:{b['color']};--mdc-icon-size:20px;flex:0 0 auto;\"></ha-icon>"
        "<div style=\"display:flex;flex-direction:column;gap:2px;flex:1 1 auto;min-width:0;\">"
        "<span style=\"color:#e5e5e5;font-weight:600;font-size:14px;white-space:nowrap;"
        "overflow:hidden;text-overflow:ellipsis;text-align:left;\">${nm}</span>"
        "<span style=\"color:#6b7280;font-size:10px;letter-spacing:.4px;text-align:left;\">"
        "🏷️ ${gw}  ·  ${lbl}${v}  ·  🕐 ${tss}</span></div>"
        "<span style=\"color:#f87171;font-size:18px;font-weight:700;flex:0 0 auto;"
        "cursor:pointer;\">✕</span></div>`; ]]]")
    exists = (f"[[[ var it=(states['{ie}'].attributes.items||[])[{i}];return it?'W':'N'; ]]]")
    payload = row_payload_js(ie, i).replace("'BUCKET'", f"'{bucket}'")
    return {"type": "custom:button-card", "entity": ie,
            "show_icon": False, "show_name": False, "show_state": False,
            "triggers_update": "all",
            "custom_fields": {"content": content},
            "tap_action": {"action": "call-service", "service": "mqtt.publish",
                           "confirmation": {"text": "Usunąć anomalię?"},
                           "service_data": {"topic": CLEAR_TOPIC, "payload": payload}},
            "styles": {"card": [
                {"background": "#0a0a0a"}, {"box-shadow": "none"}, {"border": "none"},
                {"border-bottom": exists.replace("'W'", "'1px solid #141414'").replace("'N'", "'none'")},
                {"border-radius": "0"}, {"width": "100%"},
                {"padding": exists.replace("'W'", "'11px 16px'").replace("'N'", "'0px'")},
                {"height": exists.replace("'W'", "'auto'").replace("'N'", "'0px'")},
                {"overflow": "hidden"}],
                "grid": [{"grid-template-columns": "1fr"}],
                "custom_fields": {"content": [{"width": "100%"}, {"justify-self": "stretch"}]}}}


def popup_empty(bucket):
    b = BUCKETS[bucket]
    content = (f"[[[ var n=((states['{b['items']}'].attributes.items)||[]).length;"
               "if(n>0) return '';"
               "return `<div style=\"color:#4ade80;font-size:13px;padding:14px;text-align:center;"
               f"width:100%;\">✅ Brak anomalii {b['label']}</div>`; ]]]")
    return {"type": "custom:button-card", "entity": b["items"],
            "show_icon": False, "show_name": False, "show_state": False,
            "triggers_update": "all",
            "custom_fields": {"content": content},
            "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"},
                                {"border": "none"}, {"padding": "0"}]}}


def popup_more(bucket):
    b = BUCKETS[bucket]
    content = (f"[[[ var n=((states['{b['items']}'].attributes.items)||[]).length;"
               f"if(n<={N_ROWS}) return '';"
               f"return `<div style=\"color:#525252;font-size:11px;padding:8px;text-align:center;\">"
               f"… +${{n-{N_ROWS}}} kolejnych (lista pokazuje {N_ROWS})</div>`; ]]]")
    return {"type": "custom:button-card", "entity": b["items"],
            "show_icon": False, "show_name": False, "show_state": False,
            "triggers_update": "all",
            "custom_fields": {"content": content},
            "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"},
                                {"border": "none"}, {"padding": "0"}]}}


def clear_all_btn(bucket):
    b = BUCKETS[bucket]
    kind, topic, payload = CLEAR_ALL[bucket]
    return {"type": "custom:button-card", "template": "lora_btn",
            "name": f"WYCZYŚĆ WSZYSTKIE {b['label']}", "icon": "mdi:close-circle",
            "tap_action": {"action": "call-service", "service": "mqtt.publish",
                           "confirmation": {"text": f"Wyczyścić wszystkie {b['label']}?"},
                           "service_data": {"topic": topic, "payload": payload}},
            "styles": {"card": [{"border": f"1px solid {b['color']}55"}, {"margin-top": "8px"}],
                       "icon": [{"color": b["color"]}], "name": [{"color": b["color"]}]}}


def anom_rows_card(bucket, max_h=520):
    return {"type": "vertical-stack",
            "cards": [popup_empty(bucket)] + [popup_row(bucket, i) for i in range(N_ROWS)],
            "card_mod": {"style":
                "ha-card{background:#0a0a0a !important;border:1px solid #1f1f1f !important;"
                f"border-radius:12px !important;overflow:hidden;max-height:{max_h}px;overflow-y:auto;}}"}}


def anomaly_tile(bucket):
    b = BUCKETS[bucket]
    popup = {"service": "browser_mod.popup",
             "data": {"title": f"{b['label']} — {GW}",
                      "content": {"type": "vertical-stack",
                                  "cards": [anom_rows_card(bucket), popup_more(bucket),
                                            clear_all_btn(bucket)]},
                      "style": {"--popup-background-color": "#0a0a0a",
                                "--popup-border-radius": "12px",
                                "--popup-min-width": "460px"}}}
    cnd = "[[[return parseInt(entity.state)>0?'{on}':'{off}';]]]"
    return {"type": "custom:button-card", "template": "lora_stat", "entity": b["cnt"],
            "name": b["label"], "icon": b["stat_ic"],
            "tap_action": {"action": "fire-dom-event", "browser_mod": popup},
            "styles": {"card": [{"border": cnd.format(on=f"1px solid {b['color']}", off="1px solid #1f1f1f")}],
                       "state": [{"color": cnd.format(on=b["color"], off="#333")}],
                       "icon": [{"color": cnd.format(on=b["color"], off="#333")}]}}


def stat_tile(eid, label, icon):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid,
            "name": label, "icon": icon, "tap_action": {"action": "more-info"}}


def sup_switch_card(name, eid, avail_eid):
    ls_eid = avail_eid.replace("binary_sensor.", "sensor.").replace("_available", "_last_seen")
    lq_eid = avail_eid.replace("binary_sensor.", "sensor.").replace("_available", "_link_quality")
    icon = (f"[[[ var av = states['{avail_eid}']; "
            "if (av && av.state === 'off') return 'mdi:alert-octagon'; "
            "return entity.state === 'on' ? 'mdi:lightbulb-on' : 'mdi:lightbulb-off-outline'; ]]]")
    badge = ("[[[ return `<span style=\"background:#141414;border:1px solid #22d3ee;"
             "padding:1px 5px;border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;\">"
             f"{GW}</span>`; ]]]")
    info = (f"[[[ var av = states['{avail_eid}']; var ls = states['{ls_eid}']; "
            f"var lqe = states['{lq_eid}']; "
            "var lqv = lqe && lqe.state !== 'unavailable' && lqe.state !== 'unknown' && lqe.state !== '' ? parseInt(lqe.state) : null; "
            "var lqCol = lqv === null ? '#525252' : (lqv >= 100 ? '#4ade80' : (lqv >= 50 ? '#fbbf24' : '#f87171')); "
            "var lqTxt = lqv === null ? '--' : lqv; "
            "var on = av && av.state === 'on'; var col = on ? '#4ade80' : '#f87171'; "
            "var label = on ? 'online' : 'offline'; "
            "var t = ls && ls.state !== 'unavailable' ? ls.state : '--'; "
            "return `<div style=\"font-size:10px;color:#525252;margin-top:6px;\">"
            "<span style=\"color:${col};\">● ${label}</span> · "
            "<span style=\"color:${lqCol};\">📶 ${lqTxt}</span><br/>${t}</div>`; ]]]")
    border = (f"[[[ var av = states['{avail_eid}']; "
              "if (av && av.state === 'off') return '1px solid #f87171'; "
              "return entity.state === 'on' ? '1px solid #facc15' : '1px solid #1f1f1f'; ]]]")
    icon_color = (f"[[[ var av = states['{avail_eid}']; "
                 "if (av && av.state === 'off') return '#f87171'; "
                 "return entity.state === 'on' ? '#facc15' : '#525252'; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": eid, "name": name,
            "show_state": False, "tap_action": {"action": "toggle"}, "icon": icon,
            "custom_fields": {"badge": badge, "info": info},
            "styles": {"card": [{"height": "120px"}, {"padding": "16px"}, {"border": border}],
                       "icon": [{"width": "35px"}, {"height": "35px"}, {"color": icon_color}],
                       "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
                       "custom_fields": {
                           "badge": [{"position": "absolute"}, {"top": "10px"}, {"right": "15px"}],
                           "info": [{"justify-self": "start"}, {"padding": 0}]}}}


# ── WIDOKI standardowe (identyczne z gen_unified_dashboard.py, GW=G1) ──
def view_bramka():
    return {"path": "scada-gw", "title": f"Bramka {GW}", "icon": "mdi:radio-tower",
            "cards": [{"type": "vertical-stack", "cards": [
                hdr(f"BRAMKA {GW}"),
                {"type": "horizontal-stack", "cards": [gw_header_card(), actions_grid()]},
                {"type": "horizontal-stack", "cards": [
                    anomaly_tile("offline"), anomaly_tile("battery"), anomaly_tile("other")]},
                hdr("SYNCHRONIZACJA"),
                {"type": "horizontal-stack", "cards": [
                    stat_tile(E["hash_param"], "HASH PARAM", "mdi:tune-variant"),
                    stat_tile(E["hash_disc"], "HASH DISC", "mdi:fingerprint"),
                    stat_tile(E["uptime"], "UPTIME", "mdi:timer")]},
            ]}]}


def view_control():
    # G1: brak Test 1/2 i Leak/Door — karta zredukowana do nagłówka (prawdziwe przekaźniki
    # mają własną zakładkę "Przekaźniki" niżej).
    cards = [hdr("STEROWANIE"),
             {"type": "markdown",
              "content": "ℹ️ Przekaźniki carportu — zakładka **Przekaźniki**. Tu (wzorem G2): "
                         "sterowanie testowe/awaryjne, gdy dojdzie.",
              "card_mod": CARD_MOD_DARK}]
    if SWITCHES:
        cards.insert(1, {"type": "horizontal-stack",
                         "cards": [sup_switch_card(n, e, a) for n, e, a in SWITCHES]})
    return {"path": "scada-ctl", "title": "Sterowanie", "icon": "mdi:toggle-switch",
            "cards": cards}


def view_measure():
    rows = [hdr("POMIARY")]
    if TEMPS:
        for name, t_eid, h_eid, b_eid, _xl, _xi in TEMPS:
            avail_eid = "binary_sensor." + t_eid.split(".", 1)[1].replace("_temperature", "_available")
            ls_eid = t_eid.replace("_temperature", "_last_seen")
            lq_eid = t_eid.replace("_temperature", "_link_quality")
    else:
        rows.append({"type": "markdown",
                     "content": "ℹ️ Brak czujników temp/wilg na G1 (carport = przekaźniki + ruch).",
                     "card_mod": CARD_MOD_DARK})
    return {"path": "scada-meas", "title": "Pomiary", "icon": "mdi:thermometer",
            "cards": [{"type": "vertical-stack", "cards": rows}]}


def view_alarms():
    cols = []
    for bucket in ("offline", "battery", "other"):
        b = BUCKETS[bucket]
        cnd = "[[[return parseInt(entity.state)>0?'" + b["color"] + "':'#333';]]]"
        cols.append({"type": "vertical-stack", "cards": [
            {"type": "custom:button-card", "template": "lora_stat", "entity": b["cnt"],
             "name": b["label"], "icon": b["stat_ic"],
             "tap_action": {"action": "none"},
             "styles": {"state": [{"color": cnd}], "icon": [{"color": cnd}]}},
            anom_rows_card(bucket, max_h=430),
            popup_more(bucket),
            clear_all_btn(bucket)]})
    return {"path": "scada-alarm", "title": "Alarmy", "icon": "mdi:alert-circle",
            "cards": [{"type": "vertical-stack", "cards": [
                hdr(f"ANOMALIE {GW} — KLIK WIERSZA = CLEAR"),
                {"type": "horizontal-stack", "cards": cols}]}]}


def view_schedule():
    acts = [{"type": "custom:button-card", "template": "lora_btn", "name": n, "icon": ic,
             "tap_action": tap_of(a, cf),
             "styles": {"card": [{"border": f"1px solid {col}55"}],
                        "icon": [{"color": col}], "name": [{"color": col}]}}
            for n, ic, col, a, cf in CAL_ACTS]
    rows = [hdr("TRYB & CZAS"),
            {"type": "horizontal-stack", "cards": [
                stat_tile(E["tryb_stan"], "TRYB PRACY", "mdi:factory"),
                stat_tile(E["pora"], "PORA DNIA", "mdi:theme-light-dark"),
                stat_tile(E["tq"], "JAKOŚĆ CZASU", "mdi:clock-check")]},
            hdr("AKCJE KALENDARZA"),
            {"type": "horizontal-stack", "cards": acts},
            hdr(f"KALENDARZ {GW}"),
            {"type": "calendar", "initial_view": "listWeek", "entities": [E["calendar"]],
             "card_mod": CARD_MOD_DARK}]
    return {"path": "scada-cal", "title": "Harmonogram", "icon": "mdi:calendar-clock",
            "cards": [{"type": "vertical-stack", "cards": rows}]}


def view_params():
    rows = [hdr(f"PARAMETRY BRAMKI {GW}")]
    rows.append({"type": "markdown",
                 "content": f"ℹ️ Edycja wartości parametrów — na dashboardzie **bramki {GW}**. "
                            "Tu: hash zgodności + push konfiguracji.",
                 "card_mod": CARD_MOD_DARK})
    rows.append(hdr("SYNC"))
    tiles = [stat_tile(E["hash_param"], "HASH PARAM", "mdi:tune-variant")]
    for n, ic, a in SEND_BTNS:
        tiles.append({"type": "custom:button-card", "template": "lora_btn", "name": n,
                      "icon": ic, "tap_action": tap_of(a, f"{n}?"),
                      "styles": {"card": [{"border": "1px solid #14532d"}, {"height": "88px"}],
                                 "icon": [{"color": "#4ade80"}], "name": [{"color": "#4ade80"}]}})
    rows.append({"type": "horizontal-stack", "cards": tiles})
    return {"path": "scada-param", "title": "Parametry", "icon": "mdi:tune",
            "cards": [{"type": "vertical-stack", "cards": rows}]}


def view_diag():
    ping_btn = {"type": "custom:button-card", "template": "lora_btn",
                "name": "PING", "icon": "mdi:lan-connect",
                "tap_action": tap_of(ACT[0][4]),
                "styles": {"card": [{"border": "1px solid #164e63"}, {"height": "88px"}],
                           "icon": [{"color": "#22d3ee"}], "name": [{"color": "#22d3ee"}]}}
    extra = [{"type": "custom:button-card", "template": "lora_btn", "name": n, "icon": ic,
              "tap_action": tap_of(a),
              "styles": {"card": [{"border": "1px solid #164e63"}, {"height": "88px"}],
                         "icon": [{"color": "#22d3ee"}], "name": [{"color": "#22d3ee"}]}}
             for n, ic, a in DIAG_EXTRA]
    rows = [hdr("ŁĄCZE & CZAS"),
            {"type": "horizontal-stack", "cards": [
                stat_tile(E["status"], "STATUS ŁĄCZA", "mdi:radio-tower"),
                stat_tile(E["last_seen"], "OSTATNI RX", "mdi:clock-in"),
                stat_tile(E["uptime"], "UPTIME", "mdi:timer")]},
            {"type": "horizontal-stack", "cards": [
                stat_tile(E["tq"], "JAKOŚĆ CZASU", "mdi:clock-check"),
                stat_tile(E["pora"], "PORA DNIA", "mdi:theme-light-dark"),
                stat_tile(E["tryb_stan"], "TRYB", "mdi:factory")]},
            hdr("HASHE SPÓJNOŚCI"),
            {"type": "horizontal-stack", "cards": [
                stat_tile(E["hash_disc"], "HASH DISC", "mdi:fingerprint"),
                stat_tile(E["hash_param"], "HASH PARAM", "mdi:tune-variant"),
                ping_btn]}]
    if extra:
        rows.append(hdr("NARZĘDZIA"))
        rows.append({"type": "horizontal-stack", "cards": extra})
    return {"path": "scada-diag", "title": "Diagnostyka", "icon": "mdi:stethoscope",
            "cards": [{"type": "vertical-stack", "cards": rows}]}


# ── 8. zakładka: PRZEKAŹNIKI — layout 1:1 z lokalnego dashboardu bramki G1
#    (lovelace.carport_dashboard), entity_id na żywo z HA (switch.lora_g1_*) ──
_RA_RE = re.compile(r"^(RA\d+|[A-Za-z]+)_?(\d+)$", re.IGNORECASE)


def _group_key(dev_slug):
    """'ra1_1' → ('RA1', 1); 'test_1' → ('TEST', 1); reszta → ('INNE', 0)."""
    m = _RA_RE.match(dev_slug.upper())
    if m:
        return m.group(1), int(m.group(2))
    return "INNE", 0


def view_relays(switch_entities):
    """switch_entities: lista entity_id 'switch.lora_g1_<slug>' aktualnie w HA (get_states)."""
    groups = {}
    for eid in switch_entities:
        slug = eid.split(".", 1)[1]
        if slug.startswith(f"lora_{GL}_"):
            slug = slug[len(f"lora_{GL}_"):]
        area, idx = _group_key(slug)
        groups.setdefault(area, []).append((idx, slug, eid))
    sections = []
    for area in sorted(groups, key=lambda a: (a == "INNE", a)):
        items = sorted(groups[area])
        cards = [{"type": "custom:button-card", "template": "carport_relay",
                  "entity": eid, "name": slug.upper()} for _, slug, eid in items]
        sections.append({"type": "grid", "cards": [
            {"type": "custom:button-card", "template": "lora_hdr",
             "name": f"{area} ({len(items)})", "layout_options": {"grid_columns": 4}},
            {"type": "grid", "columns": 3, "square": False, "cards": cards}]})
    if not sections:
        sections = [{"type": "grid", "cards": [
            {"type": "markdown",
             "content": "ℹ️ Brak jeszcze encji `switch.lora_g1_*` w HA — poczekaj aż discovery "
                        "bramki G1 dotrze do supervisora (devmap/db), potem uruchom ten skrypt "
                        "ponownie, żeby dociągnąć listę 105 przekaźników.",
             "card_mod": CARD_MOD_DARK}]}]
    return {"path": "carport-relays", "title": "Przekaźniki", "icon": "mdi:toggle-switch-variant",
            "type": "sections", "sections": sections}


def build_config(switch_entities):
    return {"title": f"LoRa SCADA ({ROLE}) — {GW}", "button_card_templates": TEMPLATES,
            "views": [view_bramka(), view_control(), view_measure(), view_alarms(),
                      view_schedule(), view_params(), view_diag(),
                      view_relays(switch_entities)]}


async def deploy():
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
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

        # Pobierz aktualne encje switch.lora_g1_* (puste dopóki discovery bramki G1 nie dotrze
        # do supervisora — skrypt jest bezpieczny do wielokrotnego uruchamiania, dociąga listę).
        states = await call({"type": "get_states"})
        prefix = f"switch.lora_{GL}_"
        switch_entities = sorted(
            e["entity_id"] for e in states.get("result", [])
            if e.get("entity_id", "").startswith(prefix))
        print(f"znaleziono {len(switch_entities)} encji {prefix}*")

        cfg = build_config(switch_entities)
        lst = await call({"type": "lovelace/dashboards/list"})
        if not any(d.get("url_path") == URL_PATH for d in lst.get("result", [])):
            c = await call({"type": "lovelace/dashboards/create", "url_path": URL_PATH,
                            "title": f"LoRa SCADA {GW}", "mode": "storage",
                            "show_in_sidebar": True, "icon": "mdi:antenna"})
            print("create:", c.get("success"), c.get("error", ""))
        s = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", s.get("success"), s.get("error", ""),
              "| role:", ROLE, "| views:", [v["title"] for v in cfg["views"]])


if __name__ == "__main__":
    asyncio.run(deploy())
