#!/usr/bin/env python3
"""Dołóż 5. flagę zgodności (DANE / hash stanu urządzeń, B6) do bloku SYNCHRONIZACJA.

Backend liczy 5 flag od 2026-08-03 (disc/param/cal/anom/data), ale dashboardy zostały na 4 —
przez co kafel „ZGODNOŚĆ OGÓLNA" świecił ROZJAZD, a wszystkie WIDOCZNE hashe były zgodne.

Chirurgicznie po HA WS (generatory zakazane — patrz feedback-meshtastic-dashboard-api-only):
  * nowy kafel HASH DANYCH, sklonowany z kafla HASH ANOM. (ten sam template i skracanie stanu),
  * DANE dopięte do wiersza flag DISC-PARAM-KALENDARZ-ANOMALIE.
Idempotentne: drugi przebieg nic nie zmienia. Backup configu ląduje w /tmp przed zapisem.

Uruchamiaj NA MASZYNIE (token czytany z jej config.py, połączenie na localhost):
  venv/bin/python homeassistant/patch_hash_danych.py <url_path> <hash_entity> <flag_entity>

  supervisor, zakładka G2:  lora_scada     sensor.lora_g2_hash_danych_bramka  sensor.lora_g2_dane_zgodnosc
  supervisor, zakładka G1:  lora_scada_g1  sensor.lora_g1_hash_danych_bramka  sensor.lora_g1_dane_zgodnosc
  bramka G2:                lora-scada     sensor.gw_g2_hash_danych           sensor.gw_g2_dane_zgodnosc
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.expanduser('~/meshtastic'))
import config  # noqa: E402

import websockets  # noqa: E402


async def ws_cmd(token, msg, port=8123):
    async with websockets.connect(f"ws://localhost:{port}/api/websocket", max_size=None) as ws:
        await ws.recv()                                  # auth_required
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            raise SystemExit("auth_failed")
        await ws.send(json.dumps({**msg, "id": 1}))
        while True:
            resp = json.loads(await ws.recv())
            if resp.get("id") == 1:
                return resp


def find_sync_stack(cfg):
    """Zwróć listę kart pionowego stacka, w którym siedzą kafle HASH — plus indeks wiersza ANOM."""
    for view in cfg.get('views', []):
        for card in view.get('cards', []):
            kids = card.get('cards')
            if not isinstance(kids, list):
                continue
            for i, k in enumerate(kids):
                if k.get('type') == 'horizontal-stack' and any(
                        (c.get('name') or '').startswith('HASH ANOM') for c in k.get('cards', [])):
                    return kids, i
    return None, None


def patch(cfg, hash_entity, flag_entity):
    kids, idx = find_sync_stack(cfg)
    if kids is None:
        raise SystemExit('nie znalazłem wiersza z kaflem HASH ANOM. — dashboard nietknięty')
    changed = []

    if not any('HASH DANYCH' in json.dumps(k, ensure_ascii=False) for k in kids):
        tmpl = next(c for c in kids[idx]['cards'] if (c.get('name') or '').startswith('HASH ANOM'))
        tile = dict(tmpl)                                # klon: ten sam template + skracanie stanu
        tile['entity'] = hash_entity
        tile['name'] = 'HASH DANYCH'
        tile['icon'] = 'mdi:database-check'
        kids.insert(idx + 1, {'type': 'horizontal-stack', 'cards': [tile]})
        changed.append('kafel HASH DANYCH')

    for k in kids:
        content = (k.get('custom_fields') or {}).get('content')
        if not isinstance(content, str) or "'ANOMALIE')" not in content:
            continue
        if ",'DANE')" in content:
            break
        k['custom_fields']['content'] = content.replace(
            "+'</div>'", "+f('%s','DANE')+'</div>'" % flag_entity, 1)
        changed.append('flaga DANE w wierszu zgodności')
        break
    return changed


def main():
    url_path, hash_entity, flag_entity = sys.argv[1], sys.argv[2], sys.argv[3]
    token = config.CONFIG['ha_api']['token']

    msg = {"type": "lovelace/config"}
    if url_path != '-':
        msg["url_path"] = url_path
    res = asyncio.run(ws_cmd(token, msg))
    if not res.get('success'):
        raise SystemExit('GET_FAIL ' + json.dumps(res)[:300])
    cfg = res['result']

    backup = f"/tmp/lovelace_{url_path}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    json.dump(cfg, open(backup, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print('backup →', backup)

    changed = patch(cfg, hash_entity, flag_entity)
    if not changed:
        print('bez zmian — 5. flaga już jest')
        return

    save = {"type": "lovelace/config/save", "config": cfg}
    if url_path != '-':
        save["url_path"] = url_path
    res = asyncio.run(ws_cmd(token, save))
    print('SAVE', 'OK' if res.get('success') else json.dumps(res)[:300], '|', ', '.join(changed))


if __name__ == '__main__':
    main()
