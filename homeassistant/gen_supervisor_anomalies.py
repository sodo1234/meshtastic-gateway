#!/usr/bin/env python3
"""Widok 🚨 ANOMALIE na dashboardzie supervisora (WS API), custom:button-card (JS template).
Wielki pulsujący licznik + lista wszystkich anomalii z WARTOŚCIĄ i jednostką.

MULTI-GATEWAY: banner SUMUJE anomalie ze WSZYSTKICH bramek (GATEWAYS), lista pokazuje wpisy
z każdej bramki z etykietą [Gx]. Zero hardcode g1 — iteracja po encjach per-bramka.
Encje per-gw: sensor.lora_gateway_<gl>_gw_<gl>_{offline,battery,other}_anomalies (state=count, attr.items).

BEZPIECZNIE: tylko DODAJE/aktualizuje JEDEN widok (path 'lora-anomalies').
"""
import asyncio, json, os, sys
import websockets

HA = "ws://100.79.111.24:8123/api/websocket"
VIEW_PATH = "lora-anomalies"


def _gateways():
    env = os.environ.get("LORA_GATEWAYS", "").strip()
    if env:
        return [g.strip().upper() for g in env.split(",") if g.strip()]
    try:
        sys.path.insert(0, os.path.expanduser("~/meshtastic"))
        from config import CONFIG
        gws = CONFIG.get("gateways") or []
        if len(gws) >= 2:
            return [g.upper() for g in gws]
    except Exception:
        pass
    return ["G1", "G2"]


GATEWAYS = _gateways()


def _ent(gw, bucket):
    gl = gw.lower()
    return f"sensor.lora_gateway_{gl}_gw_{gl}_{bucket}_anomalies"


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

# Zbiór encji per (gw,bucket) — używany w JS do iteracji
OFF_ENTS = [_ent(g, "offline") for g in GATEWAYS]
BAT_ENTS = [_ent(g, "battery") for g in GATEWAYS]
OTH_ENTS = [_ent(g, "other") for g in GATEWAYS]
FIRST_OFF = OFF_ENTS[0] if OFF_ENTS else _ent("G1", "offline")


def _js_arr(names):
    return "[" + ",".join("'" + n + "'" for n in names) + "]"


def _js_len(names):
    """Suma length(attr.items) po liście encji (odporne na brak encji)."""
    return ("(" + "+".join(
        "((states['" + n + "']&&states['" + n + "'].attributes.items||[]).length)" for n in names
    ) + ")") if names else "0"


# mapa gw→etykieta dla wierszy listy
GW_OF = {}   # entity → gw label
for g in GATEWAYS:
    GW_OF[_ent(g, "offline")] = g
    GW_OF[_ent(g, "battery")] = g
    GW_OF[_ent(g, "other")] = g
GW_MAP_JS = "{" + ",".join("'" + e + "':'" + g + "'" for e, g in GW_OF.items()) + "}"

BANNER_JS = (
    "[[[ "
    "var off=" + _js_len(OFF_ENTS) + ";"
    "var bat=" + _js_len(BAT_ENTS) + ";"
    "var oth=" + _js_len(OTH_ENTS) + ";"
    "var total=off+bat+oth;var col=total>0?'#ef4444':'#4ade80';"
    "return `<div style=\"text-align:center;width:100%;\">"
    "<div style=\"font-size:13px;letter-spacing:6px;font-weight:800;color:#fca5a5;\">🚨 AKTYWNE ANOMALIE 🚨</div>"
    "<div style=\"font-size:92px;line-height:1;font-weight:900;color:${col};text-shadow:0 0 32px ${col};\">${total}</div>"
    "<div style=\"display:flex;justify-content:center;gap:10px;flex-wrap:wrap;margin-top:10px;\">"
    "<span style=\"background:#7f1d1d;color:#fecaca;padding:5px 16px;border-radius:20px;font-weight:800;\">📴 OFFLINE ${off}</span>"
    "<span style=\"background:#78350f;color:#fde68a;padding:5px 16px;border-radius:20px;font-weight:800;\">🪫 BATERIA ${bat}</span>"
    "<span style=\"background:#7f1d1d;color:#fecaca;padding:5px 16px;border-radius:20px;font-weight:800;\">🌡️ TEMP/WILG ${oth}</span>"
    "</div></div>`; ]]]"
)

LIST_JS = (
    "[[[ "
    "var lbl={offline:['📴','OFFLINE',''],critical_battery:['🪫','BATERIA KRYTYCZNA','%'],"
    "low_battery:['🔋','BATERIA NISKA','%'],temp_high:['🔺','TEMPERATURA WYSOKA','°C'],"
    "temp_low:['🔻','TEMPERATURA NISKA','°C'],hum_high:['💧','WILGOTNOŚĆ WYSOKA','%'],"
    "hum_low:['🏜️','WILGOTNOŚĆ NISKA','%'],stagnation:['🕰️','STAGNACJA','h'],"
    "smoke:['🔥','DYM',''],water_leak:['🌊','ZALANIE','']};"
    "var gwof=" + GW_MAP_JS + ";"
    "var rows='';"
    + _js_arr(OFF_ENTS + BAT_ENTS + OTH_ENTS) + ".forEach(function(s){"
    "var st=states[s];if(!st)return;var g=gwof[s]||'';"
    "var items=(st.attributes.items||[]);"
    "items.forEach(function(it){var l=lbl[it.type]||['⚠️',it.type,''];"
    "var val=(it.value!=null&&it.value!=='')?(it.value+l[2]):'●';"
    "rows+=`<div style=\"display:flex;align-items:center;justify-content:space-between;"
    "padding:12px 16px;margin:7px 0;background:#160a0a;border-left:5px solid #ef4444;border-radius:10px;\">`"
    "+`<span style=\"display:flex;align-items:center;\"><span style=\"font-size:22px;margin-right:12px;\">${l[0]}</span>`"
    "+`<span style=\"font-weight:800;color:#e5e5e5;font-size:15px;\">${it.dev}</span>`"
    "+`<span style=\"background:#1e293b;color:#7dd3fc;font-size:9px;font-weight:800;padding:2px 7px;border-radius:8px;margin-left:8px;letter-spacing:1px;\">${g}</span>`"
    "+`<span style=\"color:#9ca3af;font-size:11px;margin-left:10px;letter-spacing:1px;\">${l[1]}</span></span>`"
    "+`<span style=\"font-weight:900;font-size:20px;color:#fca5a5;\">${val}</span></div>`;"
    "});});"
    "return rows||`<div style=\"text-align:center;color:#4ade80;padding:24px;font-weight:800;font-size:18px;\">✅ Brak anomalii</div>`; ]]]"
)

PULSE_MOD = {"style": ("@keyframes anompulse{0%,100%{box-shadow:0 0 16px #7f1d1d66;}"
                       "50%{box-shadow:0 0 46px #ef4444cc;}}"
                       "ha-card{animation:anompulse 1.6s ease-in-out infinite;}")}


def _card(content_js, pulse=False):
    c = {"type": "custom:button-card", "show_icon": False, "show_name": False,
         "show_state": False, "entity": FIRST_OFF, "tap_action": {"action": "none"},
         "custom_fields": {"content": content_js},
         "styles": {"card": [{"background": "#0a0a0a"}, {"border": "2px solid #7f1d1d"},
                             {"border-radius": "16px"}, {"padding": "18px 20px"}, {"box-shadow": "none"}],
                    "custom_fields": {"content": [{"width": "100%"}]}}}
    if pulse:
        c["card_mod"] = PULSE_MOD
    return c


def anomaly_view():
    return {"path": VIEW_PATH, "title": "🚨 Anomalie", "icon": "mdi:alert-octagram", "badges": [],
            "cards": [_card(BANNER_JS, pulse=True), _card(LIST_JS)]}


async def run():
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [0]

        async def cmd(p):
            mid[0] += 1; p["id"] = mid[0]; await ws.send(json.dumps(p))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0]:
                    return r

        for up in ("lovelace", None):
            getp = {"type": "lovelace/config"}
            if up:
                getp["url_path"] = up
            cfg_r = await cmd(getp)
            if cfg_r.get("error"):
                continue
            cfg = cfg_r.get("result") or {"views": []}
            views = cfg.setdefault("views", [])
            views[:] = [v for v in views if v.get("path") != VIEW_PATH]
            views.insert(0, anomaly_view())
            savep = {"type": "lovelace/config/save", "config": cfg}
            if up:
                savep["url_path"] = up
            sr = await cmd(savep)
            print(f"gateways={GATEWAYS} dashboard={up or 'default'} → save={sr.get('success')} views={[v.get('title') for v in views]}")
            if sr.get("success"):
                return
        print("Nie udało się zapisać widoku anomalii")


if __name__ == "__main__":
    asyncio.run(run())
