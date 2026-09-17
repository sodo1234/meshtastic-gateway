#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sprawdzian twardego sufitu ramki HB/pong (modules/protocol/heartbeat.py).

Tlo (G1, 2026-09-11): Meshtastic tnie pakiet >220 B i NIE chunkuje go — cicha awaria.
Zmierzone w logu G1: 106 ostrzezen OVERSIZE, HB 218-240 B, `pong` 219-220 B.
Efekt: G1 odpowiadala na 26% pingow supa (6/23), G2 na 100% (8/8).
To byl NAWROT regresji z 2026-08-26 (bezwarunkowe `sh` -> 228 B); wrocil, bo `mon` 78->82.

Pilnujemy: zadna ramka nie wychodzi nad limit, a porzucane sa pola OPCJONALNE
(nigdy liveness), z rotacja, zeby zadne nie bylo glodzone.
Uruchom: venv/bin/python test_hb_frame_limit.py
"""
import json
from modules.protocol.heartbeat import GatewayHeartbeat

# realny payload z G1 2026-09-11 (218 B) + warianty, ktore go przepelnialy
DIAG = {'hash': 'f795e257116c', 'ph': 'c6a8bf7e', 'cal': 'a4772e43b76c', 'm': 0,
        'tq': 'holdover', 'ah': 'd7517139', 'dh': 'b888a5e3', 'sh': '38290e',
        'gm': 'night', 'ga': 1}
KRYTYCZNE = ('t', 'g', 'up', 'dev', 'mon', 'pri', 'ts', 'gm', 'ga', 'm', 'tq')


class _Lora:
    def send(self, text): pass


def _hb(devs=194):
    d = {f'DEV{i}': {'monitored': True} for i in range(devs)}
    return GatewayHeartbeat(gw_id='G1', devices_fn=lambda: d, lora=_Lora(),
                            diag_fn=lambda: dict(DIAG), start_time=0)


def _size(p): return len(json.dumps(p, separators=(',', ':')).encode('utf-8'))


def main():
    hb = _hb()

    # 1. HB i pong MIESZCZA sie pod limitem Meshtastic
    for typ in ('hb', 'pong'):
        p = hb.build_payload(typ)
        assert _size(p) <= GatewayHeartbeat.FRAME_LIMIT, (typ, _size(p))
        assert _size(p) <= GatewayHeartbeat.FRAME_SAFE, (typ, _size(p))

    # 2. pola LIVENESS nigdy nie znikaja — inaczej supervisor traci podstawowa diagnostyke
    p = hb.build_payload('hb')
    for k in KRYTYCZNE:
        assert k in p, f'porzucono pole krytyczne {k}'

    # 3. ROTACJA: przez kilka beatow kazde opcjonalne pole kiedys przechodzi
    widziane = set()
    for _ in range(12):
        widziane |= set(hb.build_payload('hb'))
    for k in GatewayHeartbeat._DROP_ORDER:
        assert k in widziane, f'pole {k} glodzone — nigdy nie wyszlo w 12 beatach'

    # 4. gdy payload i tak sie miesci, NIC nie jest porzucane (bramka mala jak G2)
    maly = GatewayHeartbeat(gw_id='G2', devices_fn=lambda: {'A': {}}, lora=_Lora(),
                            diag_fn=lambda: {'dh': 'abc123', 'ga': 1}, start_time=0)
    p2 = maly.build_payload('hb')
    assert 'dh' in p2 and 'ga' in p2 and _size(p2) <= GatewayHeartbeat.FRAME_SAFE

    # 5. skrajnosc: bardzo szerokie pola diagnostyczne dalej nie przebijaja limitu
    gruby = GatewayHeartbeat(gw_id='G1', devices_fn=lambda: {f'D{i}': {} for i in range(999)},
                             lora=_Lora(), start_time=0,
                             diag_fn=lambda: {k: 'F' * 24 for k in GatewayHeartbeat._DROP_ORDER})
    p3 = gruby.build_payload('hb')
    assert _size(p3) <= GatewayHeartbeat.FRAME_LIMIT, _size(p3)
    for k in KRYTYCZNE:
        if k in ('gm', 'ga', 'm', 'tq'):
            continue                      # te wnosi diag_fn, tu go nie ma
        assert k in p3, f'porzucono pole krytyczne {k} przy skrajnym payloadzie'

    print(f"OK — 5/5: hb={_size(hb.build_payload('hb'))} B, pong={_size(hb.build_payload('pong'))} B, "
          f"limit {GatewayHeartbeat.FRAME_LIMIT} B, sufit roboczy {GatewayHeartbeat.FRAME_SAFE} B")


if __name__ == "__main__":
    main()
