#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wgraj zbudowany dashboard „LoRa SCADA" na HA bramki G1 (po HA WS, generatory zakazane).

Idempotentne: jeśli dashboard `lora-scada` już istnieje, config jest tylko nadpisywany (z backupem
starej wersji do /tmp). `carport-dashboard` NIE jest dotykany.

Najpierw RAPORTUJE zasoby lovelace — widoki używają `custom:button-card` i popupów `browser_mod`;
bez nich karty wyświetlą „Custom element doesn't exist" i dashboard będzie bezużyteczny.

Użycie NA G1:  venv/bin/python /tmp/apply_dashboard_g1.py /tmp/g1_lora_scada.json [--check-only]
"""
import asyncio
import json
import sys
import time

sys.path.insert(0, '/home/td/meshtastic')
import config  # noqa: E402
import websockets  # noqa: E402

URL_PATH = 'lora-scada'
TITLE = 'LoRa SCADA G1'
TOK = config.CONFIG['ha_api']['token']


async def cmd(ws, msg, mid):
    await ws.send(json.dumps({**msg, 'id': mid}))
    while True:
        r = json.loads(await ws.recv())
        if r.get('id') == mid:
            return r


async def go(cfg_path, check_only):
    async with websockets.connect('ws://localhost:8123/api/websocket', max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({'type': 'auth', 'access_token': TOK}))
        if json.loads(await ws.recv()).get('type') != 'auth_ok':
            raise SystemExit('auth_failed')
        mid = 0

        mid += 1
        res = await cmd(ws, {'type': 'lovelace/resources'}, mid)
        urls = [r.get('url', '') for r in (res.get('result') or [])]
        print('ZASOBY lovelace:', len(urls))
        for potrzebne in ('button-card', 'browser_mod', 'card-mod', 'apexcharts'):
            masz = any(potrzebne in u for u in urls)
            print(f'   {"OK  " if masz else "BRAK"} {potrzebne}')

        mid += 1
        dash = (await cmd(ws, {'type': 'lovelace/dashboards/list'}, mid)).get('result') or []
        istnieje = any(d.get('url_path') == URL_PATH for d in dash)
        print('dashboardy:', [d.get('url_path') for d in dash], '| lora-scada istnieje:', istnieje)

        if check_only:
            return

        if not istnieje:
            mid += 1
            r = await cmd(ws, {'type': 'lovelace/dashboards/create', 'url_path': URL_PATH,
                               'title': TITLE, 'icon': 'mdi:antenna',
                               'show_in_sidebar': True, 'require_admin': False}, mid)
            print('create:', 'OK' if r.get('success') else json.dumps(r)[:300])
        else:
            mid += 1
            r = await cmd(ws, {'type': 'lovelace/config', 'url_path': URL_PATH}, mid)
            if r.get('success'):
                bak = f'/tmp/lovelace_g1_scada_{time.strftime("%Y%m%d_%H%M%S")}.json'
                json.dump(r['result'], open(bak, 'w', encoding='utf-8'), ensure_ascii=False)
                print('backup starej wersji ->', bak)

        cfg = json.load(open(cfg_path, encoding='utf-8'))
        mid += 1
        r = await cmd(ws, {'type': 'lovelace/config/save', 'url_path': URL_PATH, 'config': cfg}, mid)
        print('save:', 'OK' if r.get('success') else json.dumps(r)[:400],
              '| widoki:', [v.get('path') for v in cfg.get('views', [])])


if __name__ == '__main__':
    asyncio.run(go(sys.argv[1], '--check-only' in sys.argv))
