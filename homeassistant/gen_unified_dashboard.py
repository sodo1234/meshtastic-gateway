#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ZUNIFIKOWANY dashboard LoRa SCADA (2026-07-11) — JEDEN generator, OBIE maszyny, 7 zakładek.

Wzór = pierwotny design v10 usera. Zakładki per FUNKCJA (identyczne bramka/supervisor):
  1. Bramka       — nagłówek wzoru (badge/status/TOTAL/MON/PRI) + akcje 3×2 + kafle anomalii z POPUPAMI
  2. Sterowanie   — przekaźniki Test 1/2 + Virtual IO + akcje
  3. Pomiary      — Temp 1/2 (temp/hum/bat) + wykresy 24h + czujniki leak/door
  4. Alarmy       — pełne listy anomalii (offline/bateria/inne) z clear per-wiersz i clear-all
  5. Harmonogram  — tryb/pora/jakość czasu + akcje PULL/PUSH + karta kalendarza
  6. Parametry    — 12 pól per-bramka (edycja na gw; na sup hash+push) + wyślij
  7. Diagnostyka  — hashe, uptime, link, czas

Rola z config.ROLE; wszystkie encje ZWERYFIKOWANE inwentaryzacją REST (2026-07-11).
Wdrożenie: dashboard url_path='lora-scada'. Uruchom NA maszynie: venv/bin/python gen_unified_dashboard.py
"""
import asyncio, json, os, sys
import websockets

sys.path.insert(0, os.path.expanduser("~/meshtastic"))
from config import CONFIG, ROLE          # noqa: E402

HA = "ws://localhost:8123/api/websocket"
TOKEN = os.environ.get("HA_TOKEN") or (CONFIG.get("ha_api") or {}).get("token", "")
URL_PATH = "lora-scada"
GW = "G2"
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

# ── mapa encji/topiców per rola (ZWERYFIKOWANE REST 2026-07-11) ──
if ROLE == "supervisor":
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
    SWITCHES = [("Test 1", "switch.lora_test_1_lora_test_1",
                 "binary_sensor.lora_test_1_test_1_available"),
                ("Test 2", "switch.lora_test_2_lora_test_2",
                 "binary_sensor.lora_test_2_test_2_available")]
    TEMPS = [("Temp 1", "sensor.lora_temp_1_temp_1_temperature",
              "sensor.lora_temp_1_temp_1_humidity",
              "sensor.lora_temp_1_temp_1_battery", "BAT", "mdi:battery"),
             ("Temp 2", "sensor.lora_temp_2_temp_2_temperature",
              "sensor.lora_temp_2_temp_2_humidity",
              "sensor.lora_temp_2_temp_2_battery", "BAT", "mdi:battery")]
    ALARM_SENSORS = [("Leak 1", "binary_sensor.lora_leak_1_leak_1_water_leak",
                      "sensor.lora_leak_1_leak_1_battery", "💧"),
                     ("Door 1", "binary_sensor.lora_door_1_door_1_contact",
                      "sensor.lora_door_1_door_1_battery", "🚪")]
    PARAM_FIELDS = []                    # numbery params G2 nie istnieją na sup — edycja na bramce
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
        ("CFG",   "mdi:cog-sync", "#4ade80", "#14532d",
         ("mqtt", f"lora/params/supervisor/cmd/{GL}/send_config", ""), None),
    ]
    CAL_ACTS = [("SYNC → BRAMKA", "mdi:calendar-arrow-right", "#22d3ee",
                 ("mqtt", f"lora/supervisor/cmd/{GL}/calendar", ""), "Wysłać kalendarz do bramki?")]
    DIAG_EXTRA = [("SYNC CZASU", "mdi:clock-check",
                   ("btn", f"button.lora_gateway_{GL}_gw_{GL}_sync_time"))]
    CLEAR_TOPIC = "lora/supervisor/cmd/clear_anomaly"          # {gw,dev,bucket}
    CLEAR_ALL = {"offline": ("mqtt", "lora/supervisor/cmd/clear_offline", ""),
                 "battery": ("mqtt", "lora/supervisor/cmd/clear_battery", ""),
                 "other":   ("mqtt", "lora/supervisor/cmd/clear_other", "")}
    def row_payload_js(items_e, i):
        return (f"[[[ var it=(states['{items_e}'].attributes.items||[])[{i}];"
                f"return it?JSON.stringify({{gw:it.gw||'{GW}',dev:it.dev,bucket:'BUCKET'}}):''; ]]]")
else:                                     # ROLE == gateway (na HA bramki)
    E = dict(
        status=f"binary_sensor.gw_{GL}_supervisor_link",
        last_seen=f"sensor.gw_{GL}_supervisor_last_rx",
        total=f"sensor.lora_gateway_{GL}_total",
        mon=f"sensor.gw_{GL}_monitored",
        pri=f"sensor.lora_gateway_{GL}_priority",
        cnt_off=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_offline",
        cnt_bat=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_bateria",
        cnt_oth=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_inne",
        it_off=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_offline",
        it_bat=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_bateria",
        it_oth=f"sensor.lora_gateway_{GL}_gw_{GL}_anomalie_inne",
        tq=f"sensor.lora_gateway_{GL}_gw_{GL}_jakosc_czasu",
        pora=f"sensor.lora_gateway_{GL}_gw_{GL}_pora_dnia",
        tryb_stan=f"sensor.lora_gateway_{GL}_gw_{GL}_tryb_pracy_bramki",
        hash_disc=f"sensor.gw_{GL}_hash_disc",
        hash_param=f"sensor.gw_{GL}_hash_parametrow",
        uptime=f"sensor.gw_{GL}_uptime",
        calendar=f"calendar.lora_{GL}",
        vio_sw=f"switch.lora_virtual_i_o_{GL}_lora_test_switch",
        vio_btn=f"button.lora_virtual_i_o_{GL}_lora_test_button",
    )
    SWITCHES = [("Test 1", "switch.test_1", None),
                ("Test 2", "switch.test_2", None)]
    TEMPS = [("Temp 1", "sensor.temp_1_temperature",
              "sensor.temp_1_humidity", "sensor.temp_1_lqi", "LQI", "mdi:signal"),
             ("Temp 2", "sensor.temp_2_temperature",
              "sensor.temp_2_humidity", "sensor.temp_2_lqi", "LQI", "mdi:signal")]
    ALARM_SENSORS = [("Leak 1", "binary_sensor.leak_1_water_leak",
                      "sensor.leak_1_battery", "💧"),
                     ("Door 1", "binary_sensor.door_1_contact",
                      "sensor.door_1_battery", "🚪")]
    PARAM_FIELDS = [(f"number.lora_gateway_{GL}_{suf}", lbl) for suf, lbl in PARAM_LABELS]
    SEND_BTNS = [("WYŚLIJ CONFIG", "mdi:cog-sync",
                  ("btn", f"button.lora_gateway_{GL}_lora_wyslij_config")),
                 ("WYŚLIJ TIMEOUT", "mdi:timer-cog",
                  ("btn", f"button.lora_gateway_{GL}_lora_wyslij_timeout")),
                 ("WYŚLIJ PROGI", "mdi:tune-vertical",
                  ("btn", f"button.lora_gateway_{GL}_lora_wyslij_progi"))]
    ACT = [
        ("PING",  "mdi:lan-connect", "#22d3ee", "#164e63",
         ("btn", f"button.lora_gateway_{GL}_gw_{GL}_ping"), None),
        ("DISC",  "mdi:magnify", "#22d3ee", "#164e63",
         ("btn", f"button.lora_gateway_{GL}_gw_{GL}_discovery"), None),
        ("SYNC",  "mdi:calendar-arrow-left", "#22d3ee", "#164e63",
         ("btn", f"button.lora_gateway_{GL}_gw_{GL}_sync_schedule"), "Pobrać kalendarz z supervisora?"),
        ("PUSH",  "mdi:calendar-arrow-right", "#fbbf24", "#713f12",
         ("btn", f"button.lora_gateway_{GL}_gw_{GL}_push_schedule_up"), None),
        ("CLEAR", "mdi:bell-off", "#f87171", "#7f1d1d",
         ("mqtt", "lora/gw/cmd/clear_anomaly", '{"bucket":"offline","all":1}'),
         f"Wyczyścić anomalie {GW}?"),
        ("CFG",   "mdi:cog-sync", "#4ade80", "#14532d",
         ("btn", f"button.lora_gateway_{GL}_lora_wyslij_config"), None),
    ]
    CAL_ACTS = [("PULL ← SUPERVISOR", "mdi:calendar-arrow-left", "#22d3ee",
                 ("btn", f"button.lora_gateway_{GL}_gw_{GL}_sync_schedule"),
                 "Pobrać kalendarz z supervisora?"),
                ("PUSH → SUPERVISOR", "mdi:calendar-arrow-right", "#fbbf24",
                 ("btn", f"button.lora_gateway_{GL}_gw_{GL}_push_schedule_up"), None)]
    DIAG_EXTRA = []
    CLEAR_TOPIC = "lora/gw/cmd/clear_anomaly"                  # {dev,bucket}
    CLEAR_ALL = {b: ("mqtt", "lora/gw/cmd/clear_anomaly",
                     json.dumps({"bucket": b, "all": 1})) for b in ("offline", "battery", "other")}
    def row_payload_js(items_e, i):
        return (f"[[[ var it=(states['{items_e}'].attributes.items||[])[{i}];"
                f"return it?JSON.stringify({{dev:it.dev,bucket:'BUCKET'}}):''; ]]]")

# ── button_card_templates (pierwotny v10) ──
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
    """Wzór: badge GW | ● ONLINE/OFFLINE + last_seen | boxy TOTAL/MONITORED/PRIORITY."""
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


def switch_card(name, eid, avail_eid=None):
    """Duży kafel przekaźnika: ikona + nazwa + ON/OFF (kolor) + toggle; kropka offline gdy avail off."""
    avail_js = (f"var av=states['{avail_eid}'];var avOn=av&&av.state==='on';"
                if avail_eid else "var avOn=true;")
    content = (
        "[[[ var on=entity.state==='on';" + avail_js +
        "var c=on?'#4ade80':'#525252';"
        "return `<div style=\"display:flex;flex-direction:column;align-items:center;gap:8px;\">"
        "<ha-icon icon=\"mdi:power-socket-eu\" style=\"color:${c};--mdc-icon-size:30px;\"></ha-icon>"
        f"<span style=\"color:#e5e5e5;font-weight:700;font-size:13px;\">{name}"
        "${avOn?'':' <span style=\\'color:#f87171;font-size:9px;\\'>●OFFLINE</span>'}</span>"
        "<span style=\"color:${c};font-weight:800;font-size:15px;letter-spacing:2px;\">"
        "${on?'ON':'OFF'}</span></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": eid,
            "show_name": False, "show_state": False, "show_icon": False,
            "custom_fields": {"content": content},
            "tap_action": {"action": "call-service", "service": "switch.toggle",
                           "service_data": {"entity_id": eid}},
            "styles": {"card": [{"height": "120px"}, {"padding": "14px"},
                                {"border": "[[[return entity.state==='on'?"
                                           "'1px solid #14532d':'1px solid #1f1f1f';]]]"}]}}


def chart(entity, title, color, ymin=None, ymax=None, decimals=1):
    """Wykres 24h — struktura 1:1 z gen_gateway_dashboard.py (ZWERYFIKOWANA live 07-07)."""
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


def temp_card(name, t_eid, h_eid, x_eid, x_lbl, x_icon):
    return {"type": "horizontal-stack", "cards": [
        stat_tile(t_eid, f"{name} · TEMP", "mdi:thermometer"),
        stat_tile(h_eid, f"{name} · WILG", "mdi:water-percent"),
        stat_tile(x_eid, f"{name} · {x_lbl}", x_icon)]}


def alarm_sensor_card(name, eid, batt_eid, emoji):
    on_border = "[[[return entity.state==='on'?'1px solid #f87171':'1px solid #1f1f1f';]]]"
    content = (
        f"[[[ var on=entity.state==='on';var b=states['{batt_eid}'];"
        "var bt=b?b.state+'%':'--';"
        "return `<div style=\"display:flex;flex-direction:column;align-items:center;gap:6px;\">"
        f"<span style=\"font-size:26px;\">{emoji}</span>"
        f"<span style=\"color:#e5e5e5;font-weight:700;font-size:13px;\">{name}</span>"
        "<span style=\"color:${on?'#f87171':'#4ade80'};font-weight:800;font-size:13px;"
        "letter-spacing:1px;\">${on?'ALARM':'OK'}</span>"
        "<span style=\"color:#525252;font-size:10px;\">🔋 ${bt}</span></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": eid,
            "show_name": False, "show_state": False, "show_icon": False,
            "custom_fields": {"content": content},
            "tap_action": {"action": "more-info"},
            "styles": {"card": [{"height": "130px"}, {"padding": "12px"},
                                {"border": on_border}]}}


# ── zakładki Sterowanie/Pomiary supervisora — WIERNA transkrypcja z
#    supervisor_lovelace_fixed.yaml (source of truth, widoki: Sterowanie/Pomiary).
#    Dotyczy WYŁĄCZNIE ROLE=='supervisor' — gateway (view_control/view_measure
#    else-branch) pozostaje nietknięty. ──
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


def sup_leak_card(name, eid, batt_eid):
    avail_eid = eid.replace("_water_leak", "_available")
    ls_eid = eid.replace("_water_leak", "_last_seen")
    lq_eid = eid.replace("_water_leak", "_link_quality")
    icon = (f"[[[ var av = states['{avail_eid}']; "
            "if (av && av.state === 'off') return 'mdi:alert-octagon'; "
            "return entity.state === 'on' ? 'mdi:water-alert' : 'mdi:water'; ]]]")
    badge = ("[[[ return `<span style=\"background:#141414;border:1px solid #22d3ee;"
             "padding:1px 5px;border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;\">"
             f"{GW}</span>`; ]]]")
    info = (f"[[[ var av = states['{avail_eid}']; var ls = states['{ls_eid}']; "
            f"var lqe = states['{lq_eid}']; "
            "var lqv = lqe && lqe.state !== 'unavailable' && lqe.state !== 'unknown' && lqe.state !== '' ? parseInt(lqe.state) : null; "
            "var lqCol = lqv === null ? '#525252' : (lqv >= 100 ? '#4ade80' : (lqv >= 50 ? '#fbbf24' : '#f87171')); "
            "var lqTxt = lqv === null ? '--' : lqv; "
            f"var batt = states['{batt_eid}']; "
            "var on = av && av.state === 'on'; var col = on ? '#4ade80' : '#f87171'; "
            "var label = on ? 'online' : 'offline'; "
            "var t = ls && ls.state !== 'unavailable' ? ls.state : '--'; "
            "var b = batt && batt.state !== 'unavailable' ? batt.state : '--'; "
            "var battColor = '#4ade80'; if (b !== '--') { var bVal = parseInt(b); "
            "if (bVal < 10) battColor = '#f87171'; else if (bVal < 25) battColor = '#fbbf24'; } "
            "return `<div style=\"display:flex; flex-direction:column; gap:2px; font-size:10px; color:#525252; margin-top:6px;\">"
            "<div><span style=\"color:${col};\">● ${label}</span> · "
            "<span style=\"color:${battColor};\">🔋 ${b}%</span> · "
            "<span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div><div>${t}</div></div>`; ]]]")
    border = (f"[[[ var av = states['{avail_eid}']; "
              "if (av && av.state === 'off') return '1px solid #f87171'; "
              "return entity.state === 'on' ? '1px solid #f87171' : '1px solid #1f1f1f'; ]]]")
    icon_color = (f"[[[ var av = states['{avail_eid}']; "
                 "if (av && av.state === 'off') return '#f87171'; "
                 "return entity.state === 'on' ? '#f87171' : '#525252'; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": eid, "name": name,
            "show_state": False, "tap_action": {"action": "none"}, "icon": icon,
            "custom_fields": {"badge": badge, "info": info},
            "styles": {"card": [{"height": "120px"}, {"padding": "16px"}, {"border": border}],
                       "icon": [{"width": "35px"}, {"height": "35px"}, {"color": icon_color}],
                       "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
                       "custom_fields": {
                           "badge": [{"position": "absolute"}, {"top": "10px"}, {"right": "15px"}],
                           "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def sup_door_card(name, avail_eid, batt_eid):
    """Wzór (YAML): entity KARTY = sam avail_eid — status kontaktu nie jest pokazywany."""
    ls_eid = avail_eid.replace("_available", "_last_seen")
    lq_eid = avail_eid.replace("_available", "_link_quality")
    icon = (f"[[[ var av = states['{avail_eid}']; "
            "if (av && av.state === 'off') return 'mdi:alert-octagon'; "
            "return 'mdi:door-closed'; ]]]")
    badge = ("[[[ return `<span style=\"background:#141414;border:1px solid #22d3ee;"
             "padding:1px 5px;border-radius:4px;font-size:9px;font-weight:700;color:#22d3ee;\">"
             f"{GW}</span>`; ]]]")
    info = (f"[[[ var av = states['{avail_eid}']; var ls = states['{ls_eid}']; "
            f"var lqe = states['{lq_eid}']; "
            "var lqv = lqe && lqe.state !== 'unavailable' && lqe.state !== 'unknown' && lqe.state !== '' ? parseInt(lqe.state) : null; "
            "var lqCol = lqv === null ? '#525252' : (lqv >= 100 ? '#4ade80' : (lqv >= 50 ? '#fbbf24' : '#f87171')); "
            "var lqTxt = lqv === null ? '--' : lqv; "
            f"var batt = states['{batt_eid}']; "
            "var on = av && av.state === 'on'; var col = on ? '#4ade80' : '#f87171'; "
            "var label = on ? 'online' : 'offline'; "
            "var t = ls && ls.state !== 'unavailable' ? ls.state : '--'; "
            "var b = batt && batt.state !== 'unavailable' ? batt.state : '--'; "
            "var battColor = '#4ade80'; if (b !== '--') { var bVal = parseInt(b); "
            "if (bVal < 10) battColor = '#f87171'; else if (bVal < 25) battColor = '#fbbf24'; } "
            "return `<div style=\"display:flex; flex-direction:column; gap:2px; font-size:10px; color:#525252; margin-top:6px;\">"
            "<div><span style=\"color:${col};\">● ${label}</span> · "
            "<span style=\"color:${battColor};\">🔋 ${b}%</span> · "
            "<span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div><div>${t}</div></div>`; ]]]")
    border = (f"[[[ var av = states['{avail_eid}']; "
              "if (av && av.state === 'off') return '1px solid #f87171'; "
              "return '1px solid #1f1f1f'; ]]]")
    icon_color = (f"[[[ var av = states['{avail_eid}']; "
                 "if (av && av.state === 'off') return '#f87171'; return '#525252'; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": avail_eid, "name": name,
            "show_state": False, "tap_action": {"action": "none"}, "icon": icon,
            "custom_fields": {"badge": badge, "info": info},
            "styles": {"card": [{"height": "120px"}, {"padding": "16px"}, {"border": border}],
                       "icon": [{"width": "35px"}, {"height": "35px"}, {"color": icon_color}],
                       "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"}],
                       "custom_fields": {
                           "badge": [{"position": "absolute"}, {"top": "10px"}, {"right": "15px"}],
                           "info": [{"justify-self": "start"}, {"padding": 0}]}}}


def sup_temp_content_card(dev_name, t_eid, h_eid, b_eid, ls_eid, lq_eid, avail_eid):
    content = (
        f"[[[ var t = states['{t_eid}']; var h = states['{h_eid}']; var b = states['{b_eid}']; "
        f"var ls = states['{ls_eid}']; var lqe = states['{lq_eid}']; "
        "var lqv = lqe && lqe.state !== 'unavailable' && lqe.state !== 'unknown' && lqe.state !== '' ? parseInt(lqe.state) : null; "
        "var lqCol = lqv === null ? '#525252' : (lqv >= 100 ? '#4ade80' : (lqv >= 50 ? '#fbbf24' : '#f87171')); "
        "var lqTxt = lqv === null ? '--' : lqv; "
        f"var av = states['{avail_eid}']; "
        "var temp = t && t.state !== 'unavailable' ? parseFloat(t.state).toFixed(1) : '--'; "
        "var hum = h && h.state !== 'unavailable' ? Math.round(parseFloat(h.state)) : '--'; "
        "var batt = b && b.state !== 'unavailable' ? b.state : '--'; "
        "var lsT = ls && ls.state !== 'unavailable' ? ls.state : '--'; "
        "var battColor = '#4ade80'; if (batt !== '--') { var battVal = parseInt(batt); "
        "if (battVal < 10) { battColor = '#f87171'; } else if (battVal < 25) { battColor = '#fbbf24'; } } "
        "var online = av && av.state === 'on'; var col = online ? '#4ade80' : '#f87171'; "
        "var label = online ? 'online' : 'offline'; "
        f"var gw = '{GW}'; var devName = '{dev_name}'; "
        "return `<div style=\"display: flex; flex-direction: column; height: 100%; width: 100%;\">"
        "<div style=\"display: flex; align-items: center; width: 100%; margin-bottom: 8px; "
        "justify-content: space-between; gap: 370px\">"
        "<div style=\"font-size: 14px; font-weight: 600; color: #e5e5e5;\">${devName}</div>"
        "<div><span style=\"background:#141414; border:1px solid #22d3ee; padding:2px 8px; "
        "border-radius:4px; font-size:10px; font-weight:600; color:#22d3ee;\">${gw}</span></div></div>"
        "<div style=\"display: flex; justify-content: space-between; align-items: center; width: 100%; "
        "flex-grow: 1; gap: 180px;\">"
        "<div style=\"display: flex; align-items: baseline; gap: 10px;\">"
        "<div><span style=\"font-size:30px; font-weight:700; color:#22d3ee; line-height:1.1;\">${temp}</span>"
        "<span style=\"font-size:22px; color:#525252;\">°C</span></div>"
        "<div><span style=\"font-size:30px; font-weight:700; color:#4ade80; line-height:1.1;\">${hum}</span>"
        "<span style=\"font-size:22px; color:#525252;\">%</span></div></div>"
        "<div style=\"display: flex; flex-direction: column; align-items: flex-end; gap: 2px; "
        "font-size:10px; color:#525252;\">"
        "<div><span style=\"color:${col};\">● ${label}</span> · "
        "<span style=\"color:${battColor};\">🔋 ${batt}%</span> · "
        "<span style=\"color:${lqCol};\">📶 ${lqTxt}</span></div><div>${lsT}</div></div></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": t_eid,
            "show_icon": False, "show_state": False, "show_name": False,
            "tap_action": {"action": "none"},
            "custom_fields": {"content": content},
            "styles": {"card": [{"height": "90px"}, {"padding": "16px"},
                                {"border-radius": "12px"}, {"background": "#0a0a0a"}],
                       "icon": [{"display": "none"}], "name": [{"display": "none"}]}}


def sup_apex_mini(entity, title, color, yaxis):
    return {"type": "custom:apexcharts-card",
            "header": {"show": True, "title": title, "show_states": False, "colorize_states": True},
            "graph_span": "24h", "span": {"end": "minute"}, "yaxis": [yaxis],
            "apex_config": {"chart": {"height": 160, "toolbar": {"show": False},
                                      "background": "transparent"},
                            "grid": {"show": True, "borderColor": "#1f1f1f"},
                            "stroke": {"width": 2, "curve": "smooth"},
                            "dataLabels": {"enabled": False}, "legend": {"show": False},
                            "tooltip": {"enabled": True, "theme": "dark",
                                       "x": {"format": "dd.MM HH:mm"}}},
            "series": [{"entity": entity, "name": title, "type": "line", "color": color}],
            "card_mod": {"style": (
                "ha-card { background: transparent !important; border: none !important; "
                "box-shadow: none !important; } "
                "ha-card .header { padding: 4px 8px !important; min-height: 24px !important; } "
                "ha-card .header .title { font-size: 12px !important; } "
                "ha-card .header .states { font-size: 10px !important; }")}}


def sup_apex_popup_span(entity, name, color, span, yaxis):
    return {"type": "custom:apexcharts-card",
            "header": {"show": True, "title": span, "show_states": True, "colorize_states": True},
            "graph_span": span, "span": {"end": "minute"}, "yaxis": [yaxis],
            "apex_config": {"chart": {"height": 200, "toolbar": {"show": False}},
                            "grid": {"show": True, "borderColor": "#1f1f1f"},
                            "stroke": {"width": 2, "curve": "smooth"},
                            "dataLabels": {"enabled": False},
                            "tooltip": {"enabled": True, "theme": "dark",
                                       "x": {"format": "dd.MM HH:mm"}}},
            "series": [{"entity": entity, "name": name, "type": "line", "color": color}],
            "card_mod": {"style": ("ha-card { background: #141414 !important; border: none !important; "
                                   "border-radius: 12px !important; box-shadow: none !important; }")}}


def sup_chart_tile(entity, popup_title, mini_name, popup_name, color, yaxis):
    mini = sup_apex_mini(entity, mini_name, color, yaxis)
    popup_cards = [sup_apex_popup_span(entity, popup_name, color, span, yaxis)
                   for span in ("24h", "7d", "30d")]
    return {"type": "custom:button-card", "show_icon": False, "show_name": False,
            "show_state": False, "entity": "this.entity.does.not.exist",
            "styles": {"card": [{"background": "#0a0a0a"}, {"border-radius": "12px"},
                                {"box-shadow": "none"}, {"padding": 0}, {"overflow": "hidden"},
                                {"height": "auto"}],
                       "custom_fields": {"chart": [{"pointer-events": "none"}]}},
            "custom_fields": {"chart": {"card": mini}},
            "tap_action": {"action": "fire-dom-event", "browser_mod": {
                "service": "browser_mod.popup",
                "data": {"title": popup_title,
                         "content": {"type": "vertical-stack", "cards": popup_cards},
                         "style": {"--popup-background-color": "#0a0a0a",
                                   "--popup-border-radius": "12px",
                                   "--popup-min-width": "650px"}}}}}


def sup_temp_charts(t_eid, h_eid):
    return {"type": "horizontal-stack", "cards": [
        sup_chart_tile(t_eid, "Temperatura", "Temperatura", "Temp", "#22d3ee", {"decimals": 1}),
        sup_chart_tile(h_eid, "Wilgotność", "Wilgotność", "Wilgotność", "#4ade80",
                       {"decimals": 0, "min": 0, "max": 100})]}


# ── WIDOKI ──
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
    if ROLE == "supervisor":
        # WIERNIE wg supervisor_lovelace_fixed.yaml (widok Sterowanie): 3 karty top-level.
        leak_name, leak_eid, leak_batt = ALARM_SENSORS[0][0], ALARM_SENSORS[0][1], ALARM_SENSORS[0][2]
        door_name, door_eid, door_batt = ALARM_SENSORS[1][0], ALARM_SENSORS[1][1], ALARM_SENSORS[1][2]
        door_avail = door_eid.replace("_contact", "_available")
        cards = [
            hdr("STEROWANIE"),
            {"type": "horizontal-stack",
             "cards": [sup_switch_card(n, e, a) for n, e, a in SWITCHES]},
            {"type": "horizontal-stack", "cards": [
                sup_leak_card(leak_name, leak_eid, leak_batt),
                sup_door_card(door_name, door_avail, door_batt)]},
        ]
        return {"path": "scada-ctl", "title": "Sterowanie", "icon": "mdi:toggle-switch",
                "cards": cards}
    rows = [hdr("PRZEKAŹNIKI"),
            {"type": "horizontal-stack",
             "cards": [switch_card(n, e, a) for n, e, a in SWITCHES]},
            hdr("VIRTUAL I/O"),
            {"type": "horizontal-stack", "cards": [
                switch_card("VIO Switch", E["vio_sw"]),
                {"type": "custom:button-card", "template": "lora_btn",
                 "name": "VIO BUTTON", "icon": "mdi:gesture-tap-button",
                 "tap_action": tap_of(("btn", E["vio_btn"])),
                 "styles": {"card": [{"height": "120px"}, {"border": "1px solid #164e63"}],
                            "icon": [{"color": "#22d3ee"}], "name": [{"color": "#22d3ee"}]}}]},
            hdr("AKCJE"),
            actions_grid()]
    return {"path": "scada-ctl", "title": "Sterowanie", "icon": "mdi:toggle-switch",
            "cards": [{"type": "vertical-stack", "cards": rows}]}


def view_measure():
    if ROLE == "supervisor":
        # WIERNIE wg supervisor_lovelace_fixed.yaml (widok Pomiary): 1 vertical-stack,
        # tylko Temp 1/2 + wykresy — leak/door NIE występują tu (są w Sterowaniu).
        rows = [hdr("POMIARY")]
        for name, t_eid, h_eid, b_eid, _xl, _xi in TEMPS:
            avail_eid = "binary_sensor." + t_eid.split(".", 1)[1].replace("_temperature", "_available")
            ls_eid = t_eid.replace("_temperature", "_last_seen")
            lq_eid = t_eid.replace("_temperature", "_link_quality")
            rows.append(sup_temp_content_card(name, t_eid, h_eid, b_eid, ls_eid, lq_eid, avail_eid))
            rows.append(sup_temp_charts(t_eid, h_eid))
        return {"path": "scada-meas", "title": "Pomiary", "icon": "mdi:thermometer",
                "cards": [{"type": "vertical-stack", "cards": rows}]}
    rows = [hdr("CZUJNIKI TEMP/WILG")]
    for name, t, h, x, xl, xi in TEMPS:
        rows.append(temp_card(name, t, h, x, xl, xi))
    rows.append(hdr("WYKRESY 24H"))
    rows.append(chart(TEMPS[0][1], "Temp 1 [°C]", "#22d3ee", 15, 35))
    rows.append(chart(TEMPS[1][1], "Temp 2 [°C]", "#c084fc", 15, 35))
    rows.append(hdr("CZUJNIKI ALARMOWE"))
    rows.append({"type": "horizontal-stack",
                 "cards": [alarm_sensor_card(n, e, b, em) for n, e, b, em in ALARM_SENSORS]})
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
    if PARAM_FIELDS:
        rows.append({"type": "entities",
                     "entities": [{"entity": e, "name": l} for e, l in PARAM_FIELDS],
                     "card_mod": CARD_MOD_DARK})
    else:
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


def build_config():
    return {"title": f"LoRa SCADA ({ROLE})", "button_card_templates": TEMPLATES,
            "views": [view_bramka(), view_control(), view_measure(), view_alarms(),
                      view_schedule(), view_params(), view_diag()]}


async def deploy():
    cfg = build_config()
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

        lst = await call({"type": "lovelace/dashboards/list"})
        if not any(d.get("url_path") == URL_PATH for d in lst.get("result", [])):
            c = await call({"type": "lovelace/dashboards/create", "url_path": URL_PATH,
                            "title": "LoRa SCADA", "mode": "storage",
                            "show_in_sidebar": True, "icon": "mdi:antenna"})
            print("create:", c.get("success"), c.get("error", ""))
        s = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
        print("save:", s.get("success"), s.get("error", ""),
              "| role:", ROLE, "| views:", [v["title"] for v in cfg["views"]])


if __name__ == "__main__":
    asyncio.run(deploy())
