#!/usr/bin/env python3
"""Remove ALL devices belonging to the local 'mqtt' config entry from this host's HA
device_registry, using the same WS call the frontend's device 'Usuń' button uses
(config/device_registry/remove_config_entry) — verified live against a single device
before this bulk version was written. Entities were already wiped in a prior pass;
this clears the now-empty orphan device shells so the MQTT integration page shows 0
devices, ready for a completely fresh discovery cycle."""
import asyncio
import json
import os
import sys

import websockets

sys.path.insert(0, os.path.expanduser("~/meshtastic"))
from config import CONFIG

TOKEN = (CONFIG.get("ha_api") or {}).get("token", "")
HA = "ws://localhost:8123/api/websocket"


async def main():
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL")
            return
        mid = [0]

        async def cmd(payload):
            mid[0] += 1
            payload["id"] = mid[0]
            await ws.send(json.dumps(payload))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0]:
                    return r

        entries = (await cmd({"type": "config_entries/get"})).get("result", [])
        mqtt_entries = [e["entry_id"] for e in entries if e.get("domain") == "mqtt"]
        print(f"mqtt config_entry ids: {mqtt_entries}")
        if not mqtt_entries:
            print("no mqtt config entry found, aborting")
            return

        devs = (await cmd({"type": "config/device_registry/list"})).get("result", [])
        targets = []
        for d in devs:
            ces = d.get("config_entries", [])
            hit = [ce for ce in ces if ce in mqtt_entries]
            if hit:
                targets.append((d["id"], hit[0]))
        print(f"BEFORE: {len(devs)} devices total, {len(targets)} on mqtt entry")

        ok, fail = 0, 0
        log_path = os.path.expanduser("~/meshtastic/device_wipe_log.txt")
        with open(log_path, "w", encoding="utf-8") as log:
            for i, (did, entry_id) in enumerate(targets):
                r = await cmd({"type": "config/device_registry/remove_config_entry",
                               "config_entry_id": entry_id, "device_id": did})
                success = r.get("success", False)
                if success:
                    ok += 1
                else:
                    fail += 1
                line = f"{'OK' if success else 'FAIL'} {i+1}/{len(targets)} {did} {r.get('error','')}"
                if (i + 1) % 50 == 0 or not success:
                    print(line)
                log.write(line + "\n")

        print(f"\nSUMMARY: ok={ok} fail={fail}")
        devs2 = (await cmd({"type": "config/device_registry/list"})).get("result", [])
        remaining = [d for d in devs2 if any(ce in mqtt_entries for ce in d.get("config_entries", []))]
        print(f"AFTER: {len(devs2)} devices total, {len(remaining)} still on mqtt entry")


if __name__ == "__main__":
    asyncio.run(main())
