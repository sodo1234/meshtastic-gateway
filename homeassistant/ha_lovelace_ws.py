#!/usr/bin/env python3
"""Minimal HA Lovelace WS helper — list/get/dump/save dashboard config over Tailscale.

HA MCP nie robi Lovelace; to jest droga do edycji dashboardów per-bramka/supervisor.
Token czytaj z config.py maszyny (NIE hardcoduj, NIE zapisuj na dysk):
  T=$(ssh td@<host> 'cd ~/meshtastic && python3 -c "import config;print(config.CONFIG[\"ha_api\"][\"token\"])"')

Usage (uruchamiaj LOKALNIE na Windows, łączy się po Tailscale ws://<host>:8123):
  python ha_lovelace_ws.py list <host> <token>
  python ha_lovelace_ws.py get  <host> <token> [url_path]
  python ha_lovelace_ws.py dump <host> <token> <url_path|-> <outfile.json>
  python ha_lovelace_ws.py save <host> <token> <config_json_file> [url_path]

Hosty: gateway G1 100.98.155.78, supervisor G0 100.79.111.24.
Dashboardy: gateway -> url_path 'lora-gw'; supervisor -> 'lovelace'.
"""
import asyncio, json, sys
import websockets


async def ws_cmd(host, token, msg):
    uri = f"ws://{host}:8123/api/websocket"
    async with websockets.connect(uri, max_size=None) as ws:
        await ws.recv()  # auth_required
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        r = json.loads(await ws.recv())
        if r.get("type") != "auth_ok":
            return {"error": "auth_failed", "detail": r}
        await ws.send(json.dumps({**msg, "id": 1}))
        while True:
            resp = json.loads(await ws.recv())
            if resp.get("id") == 1:
                return resp


def main():
    action, host, token = sys.argv[1], sys.argv[2], sys.argv[3]
    if action == "list":
        res = asyncio.run(ws_cmd(host, token, {"type": "lovelace/dashboards/list"}))
        if not res.get("success"):
            print("LIST_FAIL", json.dumps(res)[:300]); return
        for d in res["result"]:
            print(f"  url_path={d.get('url_path')!r} title={d.get('title')!r} mode={d.get('mode')!r}")
        return
    if action in ("get", "dump"):
        url_path = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] != "-" else None
        msg = {"type": "lovelace/config"}
        if url_path:
            msg["url_path"] = url_path
        res = asyncio.run(ws_cmd(host, token, msg))
        if not res.get("success", False):
            print("GET_FAIL", json.dumps(res)[:400]); return
        cfg = res["result"]
        views = cfg.get("views", [])
        print(f"OK views={len(views)} mode=storage")
        for i, v in enumerate(views):
            print(f"  [{i}] title={v.get('title')!r} path={v.get('path')!r} cards={len(v.get('cards', []))}")
        if action == "dump":
            outfile = sys.argv[5]
            json.dump(cfg, open(outfile, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
            print("DUMPED ->", outfile)
    elif action == "save":
        cfg_file = sys.argv[4]
        url_path = sys.argv[5] if len(sys.argv) > 5 else None
        cfg = json.load(open(cfg_file, encoding="utf-8"))
        msg = {"type": "lovelace/config/save", "config": cfg}
        if url_path:
            msg["url_path"] = url_path
        res = asyncio.run(ws_cmd(host, token, msg))
        print("SAVE", "OK" if res.get("success") else "FAIL", json.dumps(res)[:300])


if __name__ == "__main__":
    main()
