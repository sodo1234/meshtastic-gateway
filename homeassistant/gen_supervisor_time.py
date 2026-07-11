#!/usr/bin/env python3
"""Dodaje widok 'Czas & Pora dnia' do dashboardu supervisora (WS API).
BEZPIECZNIE: tylko DODAJE/aktualizuje JEDEN widok (path 'lora-sup-time'), nie rusza reszty.

Encje:
  - zegar źródłowy supervisora: sensor.supervisor_czas_zrodlo_sync (state=HH:MM:SS, attr.date)
  - per-bramka (pojawiają się przy HB przez LoRa): sensor.lora_gateway_g1_gw_g1_jakosc_czasu / _pora_dnia
"""
import asyncio, json, os, sys
import websockets

HA = "ws://100.79.111.24:8123/api/websocket"
VIEW_PATH = "lora-sup-time"
SUP_CLOCK = "sensor.supervisor_czas_zrodlo_sync"


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


def _tq(gw):
    gl = gw.lower()
    return f"sensor.lora_gateway_{gl}_gw_{gl}_jakosc_czasu"


def _pora(gw):
    gl = gw.lower()
    # potwierdzone live: encja to '..._pora_dnia_tryb' (nie samo '_pora_dnia')
    return f"sensor.lora_gateway_{gl}_gw_{gl}_pora_dnia_tryb"


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

CLOCK_MD = (
    "<div style='display:flex;align-items:center;gap:18px;'>"
    "<div style='font-size:46px;'>🛰️</div>"
    "<div><div style='font-size:9px;letter-spacing:2px;color:#888;font-weight:700;'>ŹRÓDŁO CZASU (SUPERVISOR · NTP)</div>"
    "<div style='font-size:30px;font-weight:800;color:#a5b4fc;font-variant-numeric:tabular-nums;'>"
    "{{ states('" + SUP_CLOCK + "') }}</div>"
    "<div style='font-size:11px;color:#888;'>{{ state_attr('" + SUP_CLOCK + "','date') }} · "
    "{{ state_attr('" + SUP_CLOCK + "','role') }}</div></div></div>"
)

def _gw_row(gw):
    tq, pora = _tq(gw), _pora(gw)
    return (
        "{% set tq = states('" + tq + "') %}{% set pora = states('" + pora + "') %}"
        "<div style='display:flex;justify-content:space-between;padding:8px 12px;margin-bottom:6px;"
        "background:#141414;border:1px solid #1f1f1f;border-radius:10px;'>"
        "<span style='font-weight:800;color:#e5e5e5;'>" + gw + "</span>"
        "<span style='color:#a5b4fc;'>{{ pora if pora not in ['unknown','unavailable',''] else '— (czeka na LoRa)' }}</span>"
        "<span style='color:#fbbf24;'>{{ tq if tq not in ['unknown','unavailable',''] else '—' }}</span>"
        "</div>")


GW_MD = (
    "<div style='display:flex;flex-direction:column;gap:2px;'>"
    "<div style='font-size:9px;letter-spacing:2px;color:#888;font-weight:700;margin-bottom:6px;'>BRAMKI — JAKOŚĆ CZASU / PORA DNIA</div>"
    + "".join(_gw_row(g) for g in GATEWAYS) +
    "</div>"
)

CARD_MOD = ("ha-card{background:#0a0a0a;border:1px solid #1f1f1f;border-radius:12px;"
            "box-shadow:none;padding:16px 18px;}")


def time_view():
    return {
        "path": VIEW_PATH, "title": "Czas", "icon": "mdi:clock-star-four-points",
        "cards": [
            {"type": "markdown", "content": CLOCK_MD, "card_mod": {"style": CARD_MOD}},
            {"type": "markdown", "content": GW_MD, "card_mod": {"style": CARD_MOD}},
        ],
    }


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
                if r.get("id") == mid[0]:
                    return r

        # Celuj w GŁÓWNY dashboard 'lovelace' (Przegląd: Sterowanie/Alarmy/Pomiary/Bramki).
        # NIE iteruj po wszystkich storage — 'map' to wbudowana Mapa, nadpisanie ją psuje.
        for up in ("lovelace", None):
            getp = {"type": "lovelace/config"}
            if up:
                getp["url_path"] = up
            cfg_r = await cmd(getp)
            if cfg_r.get("error"):
                continue
            cfg = cfg_r.get("result") or {"views": []}
            views = cfg.setdefault("views", [])
            views[:] = [v for v in views if v.get("path") != VIEW_PATH]  # idempotentnie
            views.append(time_view())
            savep = {"type": "lovelace/config/save", "config": cfg}
            if up:
                savep["url_path"] = up
            sr = await cmd(savep)
            ok = sr.get("success")
            print(f"dashboard={up or 'default'} → save={ok} views={[v.get('title') for v in views]}")
            if ok:
                return
        print("Nie udało się zapisać do dashboardu 'lovelace'")


if __name__ == "__main__":
    asyncio.run(run())
