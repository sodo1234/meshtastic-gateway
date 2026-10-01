#!/usr/bin/env python3
"""FULL wipe of platform=mqtt entities from this host's local HA (localhost:8123).
No keep-list — everything auto-discovered via MQTT gets removed so it can be rebuilt clean
by the next fresh autodiscovery cycle (z2m native + custom lora_* harness). Run only while
the lora python harness (Meshtastic_SCADA.py) is STOPPED on this host, to avoid the
harness racing the wipe with its own retained re-publishes mid-run.

Uses config/entity_registry/remove per entity — the same WS call already proven on this
codebase (fix_stale_anomaly_registry.py, purge_lora_registry.py), no unverified device-level
API. HA prunes now-empty devices on its own once all their entities are gone.

BEZPIECZNIKI (2026-08-05): kasowanie JEST nieodwracalne z poziomu HA, więc:
  • bez `--yes` skrypt tylko POKAZUJE, co by usunął (dry-run to domyślny tryb),
  • odmawia startu, gdy harness żyje — docstring wymagał tego od zawsze, ale nic
    nie sprawdzało; harness publikuje retained configi w trakcie i wyścig kończy
    się mapą pół-skasowaną, pół-odtworzoną (najgorszy możliwy stan do diagnozy).
    `--force` omija tę kontrolę, jeśli naprawdę wiesz, co robisz.

Użycie NA MASZYNIE (bramka albo supervisor):
    venv/bin/python deploy/mqtt/full_wipe_mqtt.py            # podgląd, nic nie kasuje
    venv/bin/python deploy/mqtt/full_wipe_mqtt.py --yes      # realne czyszczenie
"""
import asyncio
import json
import os
import subprocess
import sys

import websockets

sys.path.insert(0, os.path.expanduser("~/meshtastic"))
from config import CONFIG

TOKEN = (CONFIG.get("ha_api") or {}).get("token", "")
HA = "ws://localhost:8123/api/websocket"
APPLY = "--yes" in sys.argv
FORCE = "--force" in sys.argv


def harness_running():
    """Czy lora harness żyje na tym hoście (wyścig o retained configi)."""
    try:
        out = subprocess.run(["pgrep", "-af", "(test_step5|Meshtastic_SCADA).*py"],
                             capture_output=True, text=True, timeout=10).stdout
        return [l for l in out.splitlines() if "pgrep" not in l]
    except Exception:
        return []


async def main():
    alive = harness_running()
    if alive and not FORCE:
        print("❌ HARNESS DZIAŁA — zatrzymaj go najpierw (Launcher :8765 → Stop):")
        for l in alive:
            print(f"   {l}")
        print("   Inaczej harness odtworzy część encji w trakcie czyszczenia.")
        print("   Świadome pominięcie kontroli: --force")
        return
    if not APPLY:
        print("🔎 DRY-RUN (nic nie zostanie skasowane). Realne czyszczenie: --yes\n")
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

        devs = (await cmd({"type": "config/device_registry/list"})).get("result", [])
        ents = (await cmd({"type": "config/entity_registry/list"})).get("result", [])
        mqtt_ents = [e for e in ents if e.get("platform") == "mqtt"]

        print(f"BEFORE: {len(devs)} devices total, {len(ents)} entities total, "
              f"mqtt entities: {len(mqtt_ents)}")

        if not APPLY:
            by_prefix = {}
            for e in mqtt_ents:
                by_prefix.setdefault(e["entity_id"].split(".")[0], []).append(e["entity_id"])
            for dom, lst in sorted(by_prefix.items()):
                print(f"  {dom:16s} {len(lst):>4}   np. {', '.join(lst[:3])}")
            print(f"\nDO SKASOWANIA: {len(mqtt_ents)} encji platform=mqtt. "
                  f"Uruchom z --yes, żeby wykonać.")
            return

        ok, fail = 0, 0
        log_path = os.path.expanduser("~/meshtastic/full_wipe_log.txt")
        with open(log_path, "w", encoding="utf-8") as log:
            for i, e in enumerate(mqtt_ents):
                eid = e["entity_id"]
                r = await cmd({"type": "config/entity_registry/remove", "entity_id": eid})
                success = r.get("success", False)
                if success:
                    ok += 1
                else:
                    fail += 1
                line = f"{'OK' if success else 'FAIL'} {i+1}/{len(mqtt_ents)} {eid} {r.get('error','')}"
                if (i + 1) % 50 == 0 or not success:
                    print(line)
                log.write(line + "\n")

        print(f"\nSUMMARY: ok={ok} fail={fail}")

        devs2 = (await cmd({"type": "config/device_registry/list"})).get("result", [])
        ents2 = (await cmd({"type": "config/entity_registry/list"})).get("result", [])
        mqtt_ents2 = [e for e in ents2 if e.get("platform") == "mqtt"]
        print(f"AFTER: {len(devs2)} devices total, {len(ents2)} entities total, "
              f"mqtt entities remaining: {len(mqtt_ents2)}")


if __name__ == "__main__":
    asyncio.run(main())
