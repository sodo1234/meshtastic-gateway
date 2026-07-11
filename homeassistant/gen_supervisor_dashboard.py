#!/usr/bin/env python3
"""Widoki 'Bramka Gx' na dashboardzie supervisora (WS API), 1:1 ze stylem gateway
(button-card dark). MULTI-GATEWAY: JEDEN widok per bramka z listy GATEWAYS (zero hardcode g1 —
iteracja po SUPERVISOR_CONFIG.gateways; dodanie bramki = zmiana listy).

KAŻDA funkcja sterująca jest PER-BRAMKA (adresowana do konkretnej g):
  - STEROWANIE:  Ping / Discovery / Sync czasu / Dump anomalii   → lora/supervisor/cmd/<gw>/<action>
  - CLEAR:       Offline / Bateria / Inne (kubełki anomalii)      → lora/supervisor/cmd/<gw>/clear_*
  - HARMONOGRAM: Push→bramka / Push→GLOBAL / Pull←bramka          → lora/supervisor/cmd/<gw>/calendar | .../calendar_all
  - PARAMETRY:   Config / Timeout / Progi (TYLKO per-bramka)      → lora/params/supervisor/cmd/<gw>/send_*

Kafle STATUSU/statystyk filtrowane przez REALNE get_states (istnienie encji) — automatycznie
pomija brakujące (np. G2 nie ma devices_priority), zero hardcode wyjątków.

BEZPIECZNIE: dodaje/zastępuje widoki o path 'lora-sup-<gw>' na dashboardzie 'lovelace'; reszta
nietknięta. Szablony button-card mergowane przez setdefault.
"""
import asyncio, json, os, sys
import websockets

HA = "ws://100.79.111.24:8123/api/websocket"
VIEW_PREFIX = "lora-sup-"          # +gw.lower() → path per bramka (idempotentne czyszczenie)


def _gateways():
    """env LORA_GATEWAYS → config.gateways (jeśli ≥1) → ['G1','G2']. Dodanie bramki = tylko lista."""
    env = os.environ.get("LORA_GATEWAYS", "").strip()
    if env:
        return [g.strip().upper() for g in env.split(",") if g.strip()]
    try:
        sys.path.insert(0, os.path.expanduser("~/meshtastic"))
        from config import CONFIG
        gws = CONFIG.get("gateways") or []
        if gws:
            return [g.upper() for g in gws]
    except Exception:
        pass
    return ["G1", "G2"]


GATEWAYS = _gateways()


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


TOKEN = _token()

# ── encje per-bramka (dwa schematy nazw HA — potwierdzone live get_states dla G1 i G2) ──
#   prosty:   lora_gw_<gl>_<suf>                (status, last_seen, uptime, devices_*, disc_hash)
#   munged:   lora_gateway_<gl>_gw_<gl>_<suf>   (hash_param_bramka, jakosc_czasu, *_anomalies)


def e_simple(gw, suf, domain="sensor"):
    return f"{domain}.lora_gw_{gw.lower()}_{suf}"


def e_munged(gw, suf, domain="sensor"):
    gl = gw.lower()
    return f"{domain}.lora_gateway_{gl}_gw_{gl}_{suf}"


E_SUP_CLOCK = "sensor.supervisor_czas_zrodlo_sync"
E_SUN = "sun.sun"
E_SUNRISE = "sensor.sun_next_rising"
E_SUNSET = "sensor.sun_next_setting"

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
        "card": [{"padding": "10px 6px"}, {"height": "60px"}, {"--btn-accent": "#22d3ee"}],
        "grid": [{"grid-template-areas": '"i" "n"'}, {"grid-template-rows": "auto auto"},
                 {"justify-items": "center"}, {"row-gap": "4px"}],
        "icon": [{"width": "19px"}, {"color": "var(--btn-accent)"}],
        "name": [{"font-size": "8.5px"}, {"font-weight": 800}, {"color": "#a3a3a3"},
                 {"letter-spacing": "0.5px"}, {"text-transform": "uppercase"}, {"text-align": "center"}]}},
}


# ── kafle statusu / statystyk ───────────────────────────────────────────────
def stat_tile(eid, label, icon):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid,
            "name": label, "icon": icon, "tap_action": {"action": "more-info"}}


def status_card(gw):
    e_status = e_simple(gw, "status", "binary_sensor")
    e_rx = e_simple(gw, "last_seen")
    e_up = e_simple(gw, "uptime")
    content = (
        "[[[ var l=states['" + e_status + "'];var on=l&&l.state==='on';"
        "var col=on?'#4ade80':'#f87171';var lbl=on?'POŁĄCZONA':'BRAK ŁĄCZNOŚCI';"
        "var rx=states['" + e_rx + "'];var rxT=rx?rx.state:'--';"
        "var up=states['" + e_up + "'];var upS=up&&up.state&&!isNaN(up.state)?"
        "(parseInt(up.state).toLocaleString('pl-PL')+' s'):'--';"
        "return `<div style=\"display:flex;flex-direction:column;gap:8px;width:100%;\">"
        "<div style=\"display:flex;align-items:center;justify-content:space-between;\">"
        "<span style=\"font-size:11px;font-weight:700;color:#525252;letter-spacing:2px;\">BRAMKA " + gw + "</span>"
        "<span style=\"font-size:13px;font-weight:800;color:${col};\">● ${lbl}</span></div>"
        "<div style=\"display:flex;justify-content:space-between;font-size:10px;color:#525252;\">"
        "<span>ostatni RX: <span style=\"color:#e5e5e5;\">${rxT}</span></span>"
        "<span>uptime: <span style=\"color:#22d3ee;\">${upS}</span></span></div>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": e_status,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "16px"},
                                {"border": ("[[[ var l=states['" + e_status + "'];return l&&l.state==='on'?"
                                            "'1px solid #1f3a1f':'1px solid #3a1f1f'; ]]]")}]}}


def day_night_card():
    fmt = ("var fmt=function(e){if(!e||!e.state)return '--';var d=new Date(e.state);"
           "if(isNaN(d.getTime()))return '--';return ('0'+d.getHours()).slice(-2)+':'+"
           "('0'+d.getMinutes()).slice(-2);};")
    content = (
        "[[[ var s=states['" + E_SUN + "'];var day=s&&s.state==='above_horizon';" + fmt +
        "var srT=fmt(states['" + E_SUNRISE + "']);var ssT=fmt(states['" + E_SUNSET + "']);"
        "var icon=day?'☀️':'🌙';var col=day?'#fbbf24':'#818cf8';var lbl=day?'DZIEŃ':'NOC';"
        "return `<div style=\"display:flex;align-items:center;gap:16px;width:100%;\">"
        "<div style=\"font-size:50px;line-height:1;filter:drop-shadow(0 0 12px ${col}55);\">${icon}</div>"
        "<div style=\"display:flex;flex-direction:column;gap:4px;min-width:0;\">"
        "<span style=\"font-size:24px;font-weight:800;color:${col};letter-spacing:2px;\">${lbl}</span>"
        "<span style=\"font-size:10px;color:#525252;white-space:nowrap;\">🌅 świt <span style=\"color:#e5e5e5;font-weight:700;\">${srT}</span></span>"
        "<span style=\"font-size:10px;color:#525252;white-space:nowrap;\">🌇 zmierzch <span style=\"color:#e5e5e5;font-weight:700;\">${ssT}</span></span>"
        "</div></div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": E_SUN,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "18px 20px"}, {"height": "108px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def clock_card(gw):
    e_tq = e_munged(gw, "jakosc_czasu")
    content = (
        "[[[ var c=states['" + E_SUP_CLOCK + "'];var ct=c?c.state:'--';"
        "var tq=states['" + e_tq + "'];var tqS=tq?tq.state:'--';"
        "var tqShort=/Zsynchron/i.test(tqS)?'Zsynchronizowany 🛰️':(/Holdover/i.test(tqS)?'Holdover':tqS);"
        "var tqCol=/Zsynchron/i.test(tqS)?'#4ade80':(/Holdover/i.test(tqS)?'#fbbf24':'#f87171');"
        "return `<div style=\"display:flex;flex-direction:column;gap:12px;width:100%;min-width:0;\">"
        "<div style=\"display:flex;flex-direction:column;\">"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;\">CZAS SUPERVISORA · NTP</span>"
        "<span style=\"font-size:30px;font-weight:800;color:#a5b4fc;font-variant-numeric:tabular-nums;\">${ct}</span></div>"
        "<div style=\"font-size:10px;color:#525252;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\">jakość " + gw + ": "
        "<span style=\"color:${tqCol};font-weight:700;\">${tqShort}</span></div>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "entity": E_SUP_CLOCK,
            "show_icon": False, "show_name": False, "show_state": False,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "16px 20px"}, {"height": "108px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def offline_tile(gw):
    e_off = e_simple(gw, "devices_offline")
    content = (
        "[[[ var o=states['" + e_off + "'];var n=o&&!isNaN(o.state)?parseInt(o.state):0;"
        "var col=n>0?'#f87171':'#525252';"
        "return `<div style=\"display:flex;flex-direction:column;justify-content:center;height:100%;\">"
        "<span style=\"font-size:19px;font-weight:800;color:${col};\">${n}</span>"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;margin-top:6px;\">OFFLINE</span>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "show_icon": False,
            "show_name": False, "show_state": False, "entity": e_off,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "14px 16px"}, {"height": "88px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


def uptime_tile(gw):
    e_up = e_simple(gw, "uptime")
    content = (
        "[[[ var u=states['" + e_up + "'];var s=u&&!isNaN(u.state)?"
        "(parseInt(u.state).toLocaleString('pl-PL')+' s'):'--';"
        "return `<div style=\"display:flex;flex-direction:column;justify-content:center;height:100%;\">"
        "<span style=\"font-size:19px;font-weight:800;color:#e5e5e5;font-variant-numeric:tabular-nums;\">${s}</span>"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;margin-top:6px;\">UPTIME</span>"
        "</div>`; ]]]")
    return {"type": "custom:button-card", "template": "lora_base", "show_icon": False,
            "show_name": False, "show_state": False, "entity": e_up,
            "tap_action": {"action": "more-info"}, "custom_fields": {"content": content},
            "styles": {"card": [{"padding": "14px 16px"}, {"height": "88px"}],
                       "custom_fields": {"content": [{"justify-self": "start"}]}}}


# ── przyciski KOMEND per-bramka ─────────────────────────────────────────────
def _btn(topic, label, icon, accent, payload="1"):
    return {"type": "custom:button-card", "template": "lora_btn",
            "name": label, "icon": icon, "show_state": False,
            "tap_action": {"action": "call-service", "service": "mqtt.publish",
                           "service_data": {"topic": topic, "payload": payload},
                           "data": {"topic": topic, "payload": payload}},
            "styles": {"card": [{"--btn-accent": accent}]}}


def sup_cmd_btn(gw, action, label, icon, accent="#22d3ee"):
    """lora/supervisor/cmd/<gw>/<action> — backend 2-seg handler adresuje TĘ bramkę."""
    return _btn(f"lora/supervisor/cmd/{gw.lower()}/{action}", label, icon, accent)


def global_cmd_btn(action, label, icon, accent="#f59e0b"):
    """lora/supervisor/cmd/<action> (1-seg) — akcja GLOBAL na wszystkie known_gateways."""
    return _btn(f"lora/supervisor/cmd/{action}", label, icon, accent)


def param_btn(gw, eid, label, icon, accent="#c084fc"):
    """lora/params/supervisor/cmd/<gw>/<send_*> — parametry TYLKO per-bramka (zero global)."""
    return _btn(f"lora/params/supervisor/cmd/{gw.lower()}/{eid}", label, icon, accent)


def hdr(name):
    return {"type": "custom:button-card", "template": "lora_hdr", "name": name}


def control_rows(gw):
    """Wszystkie funkcje sterujące per-bramka, w blokach nagłówkowych."""
    return [
        hdr(f"STEROWANIE (→ {gw})"),
        {"type": "horizontal-stack", "cards": [
            sup_cmd_btn(gw, "ping", f"Ping {gw}", "mdi:radar", "#22d3ee"),
            sup_cmd_btn(gw, "disc", f"Disc {gw}", "mdi:magnify-scan", "#a78bfa"),
            sup_cmd_btn(gw, "sync", f"Czas {gw}", "mdi:clock-check", "#4ade80"),
            sup_cmd_btn(gw, "dump_anom", f"Dump {gw}", "mdi:reload-alert", "#fb7185"),
        ]},
        hdr(f"CLEAR ANOMALII (→ {gw})"),
        {"type": "horizontal-stack", "cards": [
            sup_cmd_btn(gw, "clear_offline", f"Offline {gw}", "mdi:lan-disconnect", "#f87171"),
            sup_cmd_btn(gw, "clear_battery", f"Bateria {gw}", "mdi:battery-alert", "#fbbf24"),
            sup_cmd_btn(gw, "clear_other", f"Inne {gw}", "mdi:alert-circle", "#fb923c"),
        ]},
        hdr(f"HARMONOGRAM (→ {gw})"),
        {"type": "horizontal-stack", "cards": [
            sup_cmd_btn(gw, "calendar", f"Push → {gw}", "mdi:calendar-arrow-right", "#fbbf24"),
            global_cmd_btn("calendar_all", "Push → GLOBAL", "mdi:calendar-multiple", "#f59e0b"),
            sup_cmd_btn(gw, "pull_calendar", f"Pull ← {gw}", "mdi:calendar-arrow-left", "#38bdf8"),
        ]},
        hdr(f"PARAMETRY (→ {gw})"),
        {"type": "horizontal-stack", "cards": [
            param_btn(gw, "send_config", f"Config {gw}", "mdi:cog-sync", "#c084fc"),
            param_btn(gw, "send_timeout", f"Timeout {gw}", "mdi:timer-cog", "#818cf8"),
            param_btn(gw, "send_threshold", f"Progi {gw}", "mdi:gauge", "#f472b6"),
        ]},
    ]


def _stat_row(specs, have):
    """Zbuduj horizontal-stack tylko z kafli, których encja ISTNIEJE (have=set entity_id)."""
    cards = [c for eid, c in specs if eid is None or eid in have]
    return {"type": "horizontal-stack", "cards": cards} if cards else None


def gw_view(gw, have):
    e_total = e_simple(gw, "devices_total")
    e_mon = e_simple(gw, "devices_monitored")
    e_prio = e_simple(gw, "devices_priority")
    e_off = e_simple(gw, "devices_offline")
    e_rx = e_simple(gw, "last_seen")
    e_hp = e_munged(gw, "hash_param_bramka")
    e_dh = e_simple(gw, "disc_hash")

    stats1 = [(e_total, stat_tile(e_total, "TOTAL", "mdi:devices")),
              (e_mon, stat_tile(e_mon, "MONITORED", "mdi:eye")),
              (e_prio, stat_tile(e_prio, "PRIORITY", "mdi:alert-octagon")),
              (e_off, offline_tile(gw))]
    stats2 = [(None, uptime_tile(gw)),
              (e_rx, stat_tile(e_rx, "OSTATNI HB", "mdi:heart-pulse"))]
    sync = [(e_hp, stat_tile(e_hp, "HASH PARAM", "mdi:tune-variant")),
            (e_dh, stat_tile(e_dh, "HASH DISC", "mdi:fingerprint"))]

    cards = [hdr(f"BRAMKA {gw}"), status_card(gw)]
    cards += control_rows(gw)
    cards += [hdr("CZAS & PORA DNIA"),
              {"type": "horizontal-stack", "cards": [day_night_card(), clock_card(gw)]},
              hdr("STATYSTYKI URZĄDZEŃ")]
    for row in (_stat_row(stats1, have), _stat_row(stats2, have)):
        if row:
            cards.append(row)
    srow = _stat_row(sync, have)
    if srow:
        cards += [hdr("SYNCHRONIZACJA"), srow]

    return {"path": f"{VIEW_PREFIX}{gw.lower()}", "title": f"Bramka {gw}",
            "icon": "mdi:radio-tower", "cards": [{"type": "vertical-stack", "cards": cards}]}


async def run():
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [0]

        async def cmd(payload):
            mid[0] += 1; payload["id"] = mid[0]
            await ws.send(json.dumps(payload))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0] and r.get("type") == "result":
                    return r

        # istnienie encji — filtr kafli (G2 bez devices_priority itp., zero hardcode)
        st = await cmd({"type": "get_states"})
        have = {e["entity_id"] for e in (st.get("result") or [])}

        cfg_r = await cmd({"type": "lovelace/config", "url_path": "lovelace"})
        if cfg_r.get("error"):
            print("GET cfg error:", cfg_r["error"]); return
        cfg = cfg_r.get("result") or {"views": []}
        bct = cfg.setdefault("button_card_templates", {})
        for k, v in TEMPLATES.items():
            bct.setdefault(k, v)
        views = cfg.setdefault("views", [])
        gw_paths = {f"{VIEW_PREFIX}{g.lower()}" for g in GATEWAYS}
        views[:] = [v for v in views if v.get("path") not in gw_paths]
        for gw in GATEWAYS:
            views.append(gw_view(gw, have))
        sr = await cmd({"type": "lovelace/config/save", "url_path": "lovelace", "config": cfg})
        print("gateways:", GATEWAYS, "save:", sr.get("success"), sr.get("error", ""))
        print("views:", [v.get("title") for v in views])


if __name__ == "__main__":
    asyncio.run(run())
