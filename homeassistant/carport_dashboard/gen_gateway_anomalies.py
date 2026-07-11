#!/usr/bin/env python3
"""BRAMKA — widok 🚨 Anomalie 1:1 ze stylem SUPERVISORA, napędzany LOKALNYMI
AUTORYTATYWNYMI sensorami anomalii bramki (bramka = źródło prawdy).

Zawiera:
  - pulsujący baner licznika (total + per-bucket)  [wzór: gen_supervisor_anomalies.BANNER_JS]
  - 3 klikalne kafle OFFLINE / BATERIA / INNE  [wzór: gen_supervisor_dashboard.offline_tile]
  - popup browser_mod z listą wierszy (dev · gw · typ · data+godzina), KLIK w wiersz =
    clear TEJ anomalii, przycisk WYCZYŚĆ WSZYSTKIE  [wzór: popup_clickable_clear._popup_content]

Źródło danych = sensory anomalii publikowane LOKALNIE przez harness bramki (NIE z2m
availability — to była proteza carportu). Encje (munge HA z device 'LoRa Gateway G2' +
nazwa encji), parametryzowane po `GW`:
    sensor.lora_gateway_<gw>_gw_<gw>_{offline,battery,other}_anomalies   (state=count, attr.items)

Clear (per-wiersz + all) publikuje na LOKALNY broker bramki:
    lora/supervisor/cmd/clear_anomaly  {gw, dev, bucket}
Bramka (proces test_step5_anomaly.py na tym samym brokerze) subskrybuje ten topic →
remove_one + reconcile (clear z OBU stron: bramka i supervisor). Bramka = źródło, więc
realnie-trwająca anomalia wróci po oknie wyciszenia (re-arm reconcilera) — poprawne.

Deploy (token bramki bywa 401 po restore → użyj hassConnection z przeglądarki lub świeży LLT):
    HA_GW_TOKEN=... LORA_GW=g2 python gen_gateway_anomalies.py
Wstawia/aktualizuje widok 'lora-anomalies-<gw>' do dashboardu bramki (szuka dashboardu z
widokiem lora-*/carport-*; fallback default). browser_mod MUSI być zainstalowany na bramce
(carport: jest — 2.13.5).
"""
import asyncio, json, os, sys
import websockets

GW = os.environ.get("LORA_GW", "g2").lower()          # 'g2' — MUSI zgadzać się z gw_id harnessu
GWU = GW.upper()
HA = os.environ.get("HA_GW_WS", "ws://100.98.155.78:8123/api/websocket")
VIEW_PATH = f"lora-anomalies-{GW}"

# ── encje anomalii bramki (lokalne, autorytatywne) ──
# REALNE encje (zweryfikowane live na G2): sensor.lora_gateway_<gw>_gw_<gw>_anomalie_<PL>
# gdzie PL ∈ {offline, bateria, inne} (gw_anom_pub publikuje PL nazwy kubełków).
def _an(pl_bucket):
    return f"sensor.lora_gateway_{GW}_gw_{GW}_anomalie_{pl_bucket}"


OFF, BAT, OTH = _an("offline"), _an("bateria"), _an("inne")

# przyciski clear-all bramki (button.press). ⚠️ entity_id do POTWIERDZENIA live po restarcie —
# harness rejestruje clear-all lokalnie; jeśli inne id, podmień tu (i tak są parametryzowane).
CLEAR_ALL = {
    "offline": os.environ.get("CLEAR_OFFLINE_BTN", f"button.lora_gateway_{GW}_clear_offline"),
    "battery": os.environ.get("CLEAR_BATTERY_BTN", f"button.lora_gateway_{GW}_clear_battery"),
    "other":   os.environ.get("CLEAR_OTHER_BTN",   f"button.lora_gateway_{GW}_clear_other"),
}
CLEAR_TOPIC = "lora/supervisor/cmd/clear_anomaly"     # bramka subskrybuje lokalnie
N_ROWS = 12                                           # max wierszy w popupie (pusty → ukryty)

TYPE_LABELS = {"offline": "Offline", "do": "Offline", "lb": "Niska bateria",
               "cb": "Krytyczna bateria", "sg": "Stagnacja", "low_battery": "Niska bateria",
               "critical_battery": "Krytyczna bateria", "stagnation": "Stagnacja",
               "temp_high": "Temp. wysoka", "temp_low": "Temp. niska", "water_leak": "Wyciek",
               "smoke": "Dym", "hum_high": "Wilg. wysoka", "hum_low": "Wilg. niska"}
TYPE_UNITS = {"lb": "%", "cb": "%", "low_battery": "%", "critical_battery": "%",
              "temp_high": "°C", "temp_low": "°C", "hum_high": "%", "hum_low": "%", "stagnation": "h"}

# tile bucket → (encja, tytuł, kolor, ikona)
TILES = [
    ("offline", OFF, "OFFLINE", "#f87171", "mdi:lan-disconnect"),
    ("battery", BAT, "BATERIA", "#fbbf24", "mdi:battery-alert"),
    ("other",   OTH, "INNE",    "#22d3ee", "mdi:alert-octagram"),
]

# ── szablony button-card (kopia 1:1 ze stylu supervisora) ──
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
    "lora_btn": {"template": "lora_base", "show_state": False, "styles": {
        "card": [{"padding": "14px 12px"}, {"height": "58px"}],
        "name": [{"font-size": "11px"}, {"font-weight": 800}, {"letter-spacing": "1.5px"},
                 {"text-transform": "uppercase"}, {"white-space": "nowrap"},
                 {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
        "icon": [{"width": "20px"}]}},
}

PULSE_MOD = {"style": ("@keyframes anompulse{0%,100%{box-shadow:0 0 16px #7f1d1d66;}"
                       "50%{box-shadow:0 0 46px #ef4444cc;}}"
                       "ha-card{animation:anompulse 1.6s ease-in-out infinite;}")}


def _len(eid):
    return "(states['" + eid + "']&&states['" + eid + "'].attributes.items||[]).length"


BANNER_JS = (
    "[[[ "
    "var off=" + _len(OFF) + ";var bat=" + _len(BAT) + ";var oth=" + _len(OTH) + ";"
    "var total=off+bat+oth;var col=total>0?'#ef4444':'#4ade80';"
    "return `<div style=\"text-align:center;width:100%;\">"
    "<div style=\"font-size:13px;letter-spacing:6px;font-weight:800;color:#fca5a5;\">🚨 AKTYWNE ANOMALIE · BRAMKA " + GWU + " 🚨</div>"
    "<div style=\"font-size:92px;line-height:1;font-weight:900;color:${col};text-shadow:0 0 32px ${col};\">${total}</div>"
    "<div style=\"display:flex;justify-content:center;gap:10px;flex-wrap:wrap;margin-top:10px;\">"
    "<span style=\"background:#7f1d1d;color:#fecaca;padding:5px 16px;border-radius:20px;font-weight:800;\">📴 OFFLINE ${off}</span>"
    "<span style=\"background:#78350f;color:#fde68a;padding:5px 16px;border-radius:20px;font-weight:800;\">🪫 BATERIA ${bat}</span>"
    "<span style=\"background:#7f1d1d;color:#fecaca;padding:5px 16px;border-radius:20px;font-weight:800;\">🌡️ INNE ${oth}</span>"
    "</div></div>`; ]]]"
)


def banner_card():
    c = {"type": "custom:button-card", "show_icon": False, "show_name": False,
         "show_state": False, "entity": OFF, "triggers_update": "all",
         "tap_action": {"action": "none"}, "custom_fields": {"content": BANNER_JS},
         "styles": {"card": [{"background": "#0a0a0a"}, {"border": "2px solid #7f1d1d"},
                             {"border-radius": "16px"}, {"padding": "18px 20px"}, {"box-shadow": "none"}],
                    "custom_fields": {"content": [{"width": "100%"}]}},
         "card_mod": PULSE_MOD}
    return c


def _row(eid, i, bucket, color):
    """Button-card wiersz i (PEŁNA SZEROKOŚĆ): items[i] → dev · gw · typ · data | WARTOŚĆ | ✕ clear."""
    L = json.dumps(TYPE_LABELS, ensure_ascii=False)
    U = json.dumps(TYPE_UNITS, ensure_ascii=False)
    content = (
        "[[[ "
        "var items=(states['" + eid + "'].attributes.items)||[];"
        "var it=items[" + str(i) + "];"
        "if(!it) return '';"
        "var L=" + L + ";var U=" + U + ";"
        "var typ=L[it.type]||it.type||'';"
        "var val=(it.value!=null&&it.value!=='')?(it.value+(U[it.type]||'')):'';"
        "var ts=it.detected_at?new Date(it.detected_at):(it.since?new Date(it.since*1000):null);"
        "var tss=ts?(('0'+ts.getDate()).slice(-2)+'.'+('0'+(ts.getMonth()+1)).slice(-2)+' '+"
        "('0'+ts.getHours()).slice(-2)+':'+('0'+ts.getMinutes()).slice(-2)):'—';"
        "return `<div style=\"display:flex;justify-content:space-between;align-items:center;"
        "width:100%;box-sizing:border-box;gap:14px;\">"
        "<div style=\"display:flex;flex-direction:column;gap:3px;flex:1 1 auto;min-width:0;\">"
        "<span style=\"color:#e5e5e5;font-weight:700;font-size:14px;\">${it.dev}</span>"
        "<span style=\"color:#737373;font-size:11px;\">${it.gw} · ${typ} · ${tss}</span></div>"
        "<span style=\"color:#fca5a5;font-weight:900;font-size:18px;flex:0 0 auto;white-space:nowrap;\">${val}</span>"
        "<span style=\"color:" + color + ";font-size:18px;font-weight:800;flex:0 0 auto;\">✕</span>"
        "</div>`; ]]]")
    height = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
              "return it?'auto':'0px'; ]]]")
    pad = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
           "return it?'10px 12px':'0px'; ]]]")
    border = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
              "return it?'1px solid #1a1a1a':'none'; ]]]")
    # payload clear: {gw,dev,bucket} z items[i] (gw = it.gw = '" + GWU + "')
    payload = ("[[[ var it=(states['" + eid + "'].attributes.items||[])[" + str(i) + "];"
               "return it?JSON.stringify({gw:it.gw,dev:it.dev,bucket:'" + bucket + "'}):''; ]]]")
    return {
        "type": "custom:button-card", "entity": eid, "show_icon": False,
        "show_name": False, "show_state": False,
        "custom_fields": {"content": content},
        "tap_action": {"action": "call-service", "service": "mqtt.publish",
                       "service_data": {"topic": CLEAR_TOPIC, "payload": payload},
                       "data": {"topic": CLEAR_TOPIC, "payload": payload}},
        "styles": {"card": [{"background": "#0a0a0a"}, {"box-shadow": "none"},
                            {"border-bottom": border}, {"border-radius": "0"}, {"width": "100%"},
                            {"padding": pad}, {"height": height}, {"overflow": "hidden"}],
                   "grid": [{"grid-template-columns": "1fr"}],
                   "custom_fields": {"content": [{"width": "100%"}, {"justify-self": "stretch"}]}}}


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


def anomaly_tile(bucket, eid, title, color, icon):
    """Klikalny kafel licznika: count colorized + fire-dom-event browser_mod.popup (clear w środku)."""
    content = (
        "[[[ var n=" + _len(eid) + ";var col=n>0?'" + color + "':'#525252';"
        "return `<div style=\"display:flex;flex-direction:column;justify-content:center;height:100%;\">"
        "<span style=\"font-size:26px;font-weight:900;color:${col};\">${n}</span>"
        "<span style=\"font-size:8px;font-weight:700;color:#525252;letter-spacing:2px;margin-top:6px;\">"
        + title + "</span></div>`; ]]]")
    return {
        "type": "custom:button-card", "template": "lora_base", "entity": eid,
        "show_icon": False, "show_name": False, "show_state": False,
        "triggers_update": "all",
        "custom_fields": {"content": content},
        "tap_action": {"action": "fire-dom-event", "browser_mod": {
            "service": "browser_mod.popup",
            "data": {"title": f"Anomalie — {title}",
                     "dismissable": True,
                     "content": _popup_content(eid, bucket, title, color, CLEAR_ALL[bucket]),
                     "style": {"--popup-min-width": "min(720px,92vw)",
                               "--popup-max-width": "min(900px,94vw)",
                               "--popup-background-color": "#0a0a0a",
                               "--popup-border-radius": "12px"}}}},
        "styles": {"card": [{"padding": "14px 16px"}, {"height": "96px"},
                            {"border": f"1px solid {color}33"}],
                   "custom_fields": {"content": [{"justify-self": "start"}]}}}


def anomaly_view():
    tiles = [anomaly_tile(b, e, t, c, ic) for b, e, t, c, ic in TILES]
    return {"path": VIEW_PATH, "title": "🚨 Anomalie", "icon": "mdi:alert-octagram", "badges": [],
            "cards": [{"type": "vertical-stack", "cards": [
                banner_card(),
                {"type": "custom:button-card", "template": "lora_hdr", "name": f"ANOMALIE BRAMKI {GWU} — KLIKNIJ KAFEL BY WYCZYŚCIĆ"},
                {"type": "horizontal-stack", "cards": tiles},
            ]}]}


async def run():
    token = os.environ.get("HA_GW_TOKEN")
    if not token:
        try:
            sys.path.insert(0, os.path.expanduser("~/meshtastic"))
            from config import CONFIG
            token = (CONFIG.get("ha_api") or {}).get("token")
        except Exception:
            token = None
    if not token:
        print("BRAK HA_GW_TOKEN (i brak config.py) — podaj token lub deploy przez hassConnection")
        return
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL (401 — token bramki martwy? użyj hassConnection z przeglądarki)"); return
        mid = [0]

        async def cmd(p):
            mid[0] += 1; p["id"] = mid[0]; await ws.send(json.dumps(p))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0]:
                    return r

        dl = await cmd({"type": "lovelace/dashboards/list"})
        targets = [d.get("url_path") for d in (dl.get("result") or [])] + [None]
        for up in targets:
            g = {"type": "lovelace/config"}
            if up:
                g["url_path"] = up
            cr = await cmd(g)
            if cr.get("error"):
                continue
            cfg = cr.get("result") or {}
            views = cfg.get("views", [])
            paths = [v.get("path") or "" for v in views]
            if not any(p.startswith(("lora", "carport")) for p in paths):
                continue
            bct = cfg.setdefault("button_card_templates", {})
            for k, v in TEMPLATES.items():
                bct.setdefault(k, v)
            views[:] = [v for v in views if v.get("path") != VIEW_PATH]
            insert_at = 1 if len(views) >= 1 else 0
            views.insert(insert_at, anomaly_view())
            cfg["views"] = views
            sp = {"type": "lovelace/config/save", "config": cfg}
            if up:
                sp["url_path"] = up
            sr = await cmd(sp)
            print("dashboard=%s save=%s titles=%s" % (
                up or "default", sr.get("success"), [v.get("title") for v in views]))
            if sr.get("success"):
                return
        print("Nie znaleziono dashboardu bramki (widok path 'lora*'/'carport*')")


if __name__ == "__main__":
    asyncio.run(run())
