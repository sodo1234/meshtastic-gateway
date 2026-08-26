#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dash_sync — LIVE jest źródłem prawdy dla dashboardów, generatory NIE (decyzja usera 2026-07-30).

Dashboardy były strojone ręcznie na żywym HA (zakładki, kafle, popupy), a generatory
(`gen_unified_dashboard.py`, `gen_g1_dashboard.py`, `gen_gateway_dashboard.py`) zostały w tyle —
odtwarzają STARY układ (7 zakładek vs 5 live) i uruchomione na produkcji skasowałyby ręczne
poprawki. Dlatego pętla jest odwrotna: `pull` zrzuca live do repo (baseline = kanon),
`push` przywraca baseline na maszynę (rollback / klonowanie).

    python dash_sync.py pull            # wszystkie 3 dashboardy live → dashboards_baseline/
    python dash_sync.py pull sup-g2     # pojedynczy
    python dash_sync.py push sup-g1     # baseline → live (ŚWIADOMY zapis; robi backup pull-em)
    python dash_sync.py diff sup-g2     # co się rozjechało od ostatniego pull

Token czytany po ssh z config.py maszyny — nie trzymamy go w repo ani w plikach.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(HERE, "dashboards_baseline")

# nazwa → (host, url_path, plik baseline)
TARGETS = {
    "sup-g2": ("100.79.111.24", "lora-scada", "live_sup_lora-scada_G2.json"),
    "sup-g1": ("100.79.111.24", "lora-scada-g1", "live_sup_lora-scada-g1.json"),
    "gw-g2": ("100.98.155.78", "lora-scada", "live_gw_g2_lora-scada.json"),
    # G1 carport wróciła do sieci 2026-08-24 pod adresem 100.68.79.100 (100.104.38.14 MARTWY).
    # Ma DWA dashboardy: `lora-scada` (SCADA) i `carport-dashboard` (widok obiektowy).
    "gw-g1": ("100.68.79.100", "lora-scada", "live_gw_g1_lora-scada.json"),
    "gw-g1-carport": ("100.68.79.100", "carport-dashboard", "live_gw_g1_carport-dashboard.json"),
}


def token(host):
    out = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", f"td@{host}",
         'cd ~/meshtastic && python3 -c "import config;print(config.CONFIG[\'ha_api\'][\'token\'])"'],
        capture_output=True, text=True, check=True)
    return out.stdout.strip()


def ws(host, tok, msg):
    import asyncio
    import websockets

    async def _go():
        async with websockets.connect(f"ws://{host}:8123/api/websocket", max_size=None) as w:
            await w.recv()
            await w.send(json.dumps({"type": "auth", "access_token": tok}))
            if json.loads(await w.recv()).get("type") != "auth_ok":
                raise SystemExit(f"auth_failed {host}")
            await w.send(json.dumps({**msg, "id": 1}))
            while True:
                r = json.loads(await w.recv())
                if r.get("id") == 1:
                    return r
    return asyncio.run(_go())


def _cfg(name):
    host, url, fname = TARGETS[name]
    r = ws(host, token(host), {"type": "lovelace/config", "url_path": url})
    if not r.get("success"):
        raise SystemExit(f"GET_FAIL {name}: {json.dumps(r)[:200]}")
    return host, url, os.path.join(BASE, fname), r["result"]


def pull(name):
    _, _, path, cfg = _cfg(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=1)
    views = cfg.get("views", [])
    print(f"  {name}: {len(views)} widoków → {os.path.basename(path)}  "
          f"[{', '.join(str(v.get('title')) for v in views)}]")


def diff(name):
    _, _, path, live = _cfg(name)
    if not os.path.exists(path):
        print(f"  {name}: brak baseline — zrób pull")
        return
    old = json.load(open(path, encoding="utf-8"))
    same = json.dumps(old, sort_keys=True) == json.dumps(live, sort_keys=True)
    print(f"  {name}: {'bez zmian' if same else 'ROZJAZD live vs baseline → pull albo push'}")


def push(name):
    host, url, path, _ = _cfg(name)                     # _cfg = jednocześnie test dostępu
    cfg = json.load(open(path, encoding="utf-8"))
    r = ws(host, token(host), {"type": "lovelace/config/save", "config": cfg, "url_path": url})
    print(f"  {name}: SAVE {'OK' if r.get('success') else 'FAIL ' + json.dumps(r)[:200]}")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "pull"
    names = sys.argv[2:] or list(TARGETS)
    fn = {"pull": pull, "push": push, "diff": diff}.get(action)
    if not fn:
        raise SystemExit(__doc__)
    for n in names:
        if n not in TARGETS:
            raise SystemExit(f"nieznany cel {n!r}; dostępne: {', '.join(TARGETS)}")
        fn(n)
