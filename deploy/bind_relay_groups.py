#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wpisz 52 przekazniki carport (FAZY 2 i 3) do 6 grup Zigbee NA BRAMCE G1.

⚠️ URUCHAMIAC PO ZMIERZCHU. Dopisanie urzadzenia do grupy Zigbee to ZCL `genGroups.add`
WYSYLANE DO URZADZENIA — urzadzenie musi byc pod napieciem. Sprawdzone 2026-09-17 w dzien:
`Failed to add to group (ZCL command ... genGroups.add ... failed)`, bo obwod przekaznikow
carport jest w dzien beznapieciowy (zmierzone: 0/78 przekaznikow i 0/25 repeaterow online,
przy 82/82 czujnikach bateryjnych online).

Idempotentny: czlonkowie juz wpisani sa pomijani. Mozna odpalac wielokrotnie, az wyjdzie 52/52.

Uzycie NA G1:  cd ~/meshtastic && venv/bin/python /tmp/bind_relay_groups.py [--dry-run]
"""
import json
import sys
import time

import paho.mqtt.client as mqtt

sys.path.insert(0, '/home/td/meshtastic')
from config import CONFIG                                    # noqa: E402

# FAZY 2 i 3 — BEZ FAZY 1 (user 2026-09-17). Faza 1 ma swiecic NA STALE, dopoki przekazniki
# maja zasilanie, wiec nie wolno jej przelaczac. Harmonogram na bramce (`Schedule ON`/`OFF`)
# juz tak dziala — rusza 52 encje `_2`/`_3` i ani jednej `_1`. Grupy MUSZA byc zgodne z tym
# zbiorem, inaczej jeden groupcast zgasilby faze 1, a nic by jej potem nie zapalilo (harmonogram
# jej nie dotyka) — dokladnie objaw „nie wszystkie sie wlaczyly".
FAZY = (2, 3)
GRUPY = {
    'GRP_RA1_6':   [f'RA{s}_{k}' for s in range(1, 7) for k in FAZY],      # 12
    'GRP_RA7_12':  [f'RA{s}_{k}' for s in range(7, 13) for k in FAZY],     # 12
    'GRP_RA13_15': [f'RA{s}_{k}' for s in range(13, 16) for k in FAZY],    # 6
    'GRP_RB1_3':   [f'RB{s}_{k}' for s in range(1, 4) for k in FAZY],      # 6
    'GRP_RB4_7':   [f'RB{s}_{k}' for s in range(4, 8) for k in FAZY],      # 8
    'GRP_RB8_11':  [f'RB{s}_{k}' for s in range(8, 12) for k in FAZY],     # 8
}
DRY = '--dry-run' in sys.argv


def main():
    assert sum(len(v) for v in GRUPY.values()) == 52, 'podzial = 52 przekazniki (fazy 2 i 3; faza 1 stale zalaczona)'
    m = CONFIG['mqtt']
    stan = {'devices': None, 'groups': None}
    odp = []

    def on_c(c, u, f, rc):
        c.subscribe('zigbee2mqtt/bridge/devices')
        c.subscribe('zigbee2mqtt/bridge/groups')
        c.subscribe('zigbee2mqtt/bridge/response/group/members/add')

    def on_m(c, u, msg):
        t = msg.topic
        try:
            p = json.loads(msg.payload.decode())
        except Exception:
            return
        if t.endswith('bridge/devices'):
            stan['devices'] = p
        elif t.endswith('bridge/groups'):
            stan['groups'] = p
        else:
            odp.append(p)

    c = mqtt.Client()
    c.username_pw_set(m['user'], m['pass'])
    c.on_connect, c.on_message = on_c, on_m
    c.connect(m['host'], m['port'], 10)
    c.loop_start()
    t0 = time.time()
    while (stan['devices'] is None or stan['groups'] is None) and time.time() - t0 < 20:
        time.sleep(0.3)
    if stan['devices'] is None or stan['groups'] is None:
        print('BLAD: brak bridge/devices lub bridge/groups z z2m'); return 2

    ieee2n = {d['ieee_address']: d['friendly_name'] for d in stan['devices']
              if d.get('ieee_address') and d.get('friendly_name')}
    zywe = {d['friendly_name'] for d in stan['devices'] if d.get('friendly_name')}
    juz = {}
    for g in stan['groups']:
        juz[g.get('friendly_name')] = {ieee2n.get(x.get('ieee_address'))
                                       for x in (g.get('members') or [])}

    brak_grup = [g for g in GRUPY if g not in juz]
    if brak_grup:
        print('BLAD: brak grup w z2m:', brak_grup, '→ zaloz je najpierw (group/add)'); return 3

    do_zrobienia = []
    for g, czlonkowie in GRUPY.items():
        for dev in czlonkowie:
            if dev not in zywe:
                print(f'  ⚠️  {dev}: nie ma takiego urzadzenia w z2m — POMIJAM')
                continue
            if dev in juz.get(g, set()):
                continue
            do_zrobienia.append((g, dev))

    print(f'== do wpisania: {len(do_zrobienia)} (juz wpisanych: '
          f'{sum(len(v & set(GRUPY[k])) for k, v in juz.items() if k in GRUPY)}/52)')
    if DRY:
        for g, d in do_zrobienia:
            print(f'   [dry] {d} → {g}')
        c.loop_stop(); return 0

    udane, nieudane = 0, []
    for g, dev in do_zrobienia:
        odp.clear()
        c.publish('zigbee2mqtt/bridge/request/group/members/add',
                  json.dumps({'group': g, 'device': dev}))
        t = time.time()
        while not odp and time.time() - t < 25:           # ZCL ma timeout 10 s + zapas na mesh
            time.sleep(0.2)
        if odp and odp[-1].get('status') == 'ok':
            udane += 1
            print(f'   ✅ {dev} → {g}')
        else:
            blad = (odp[-1].get('error', '')[:90] if odp else 'brak odpowiedzi (timeout)')
            nieudane.append((dev, g, blad))
            print(f'   ❌ {dev} → {g}: {blad}')
        time.sleep(1.0)                                   # nie zalewaj koordynatora

    c.loop_stop()
    print(f'\n== WYNIK: {udane} wpisanych, {len(nieudane)} nieudanych')
    for dev, g, b in nieudane:
        print(f'   ❌ {dev} ({g}): {b}')
    if nieudane:
        print('\nPowtorz skrypt — jest idempotentny, dopisze tylko brakujacych.')
    return 0 if not nieudane else 1


if __name__ == '__main__':
    sys.exit(main())
