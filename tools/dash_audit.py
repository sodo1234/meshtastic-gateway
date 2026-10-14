#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audyt dashboardów LoRa SCADA: każda encja użyta w Lovelace musi istnieć i nie być unavailable.

  python tools/dash_audit.py 100.79.111.24 100.98.155.78

Token czytany z config.py maszyny przez SSH (nie wypisywany, nie zapisywany). Tylko odczyt.
"""
import asyncio
import json
import re
import subprocess
import sys

import websockets

ENT = re.compile(r'\b(?:sensor|binary_sensor|switch|button|number|select|text|light|calendar|'
                 r'input_\w+|script|automation)\.[a-z0-9_]+')


def token(host):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', f'td@{host}',
                           'cd ~/meshtastic && venv/bin/python -c "import config;'
                           'c=getattr(config,\'CONFIG\',None) or config.SUPERVISOR_CONFIG;'
                           'print(c[\'ha_api\'][\'token\'])"'],
                          capture_output=True, text=True, timeout=30).stdout.strip()


async def audit(host):
    async with websockets.connect(f'ws://{host}:8123/api/websocket', max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({'type': 'auth', 'access_token': token(host)}))
        assert json.loads(await ws.recv())['type'] == 'auth_ok', 'auth'
        n = 0

        async def call(msg):
            nonlocal n
            n += 1
            await ws.send(json.dumps({**msg, 'id': n}))
            while True:
                r = json.loads(await ws.recv())
                if r.get('id') == n:
                    return r.get('result')

        states = {s['entity_id']: s['state'] for s in await call({'type': 'get_states'})}
        dashes = [d['url_path'] for d in await call({'type': 'lovelace/dashboards/list'})
                  if 'lora' in d['url_path'] or 'scada' in d['url_path']]
        bad_total = 0
        for url in dashes:
            cfg = await call({'type': 'lovelace/config', 'url_path': url})
            if not cfg:
                print(f'  {url}: brak configu'); continue
            for v in cfg.get('views', []):
                ents = sorted(set(ENT.findall(json.dumps(v))) - {'button.press'})   # usługa, nie encja
                missing = [e for e in ents if e not in states]
                unav = [e for e in ents if states.get(e) in ('unavailable', 'unknown')]
                bad_total += len(missing) + len(unav)
                flag = '✅' if not missing and not unav else '⚠️'
                print(f"  {flag} {url}/{v.get('path', v.get('title'))}: {len(ents)} encji, "
                      f"brak {len(missing)}, unavailable/unknown {len(unav)}")
                for e in missing[:15]:
                    print(f'       BRAK  {e}')
                for e in unav[:15]:
                    print(f'       {states[e]:11s} {e}')
        return bad_total


def main():
    total = 0
    for h in sys.argv[1:] or ['100.79.111.24', '100.98.155.78']:
        print(f'== {h}')
        total += asyncio.run(audit(h))
    print(f'\nproblemy: {total}')


if __name__ == '__main__':
    main()
