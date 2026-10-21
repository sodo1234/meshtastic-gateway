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
    def __init__(self): self.sent = []
    def send(self, text): self.sent.append(json.loads(text))


def _hb(devs=194, diag=None, gw='G1'):
    d = {f'DEV{i}': {'monitored': True} for i in range(devs)}
    return GatewayHeartbeat(gw_id=gw, devices_fn=lambda: d, lora=_Lora(),
                            diag_fn=lambda: dict(diag if diag is not None else DIAG), start_time=0)


def _size(p): return len(json.dumps(p, separators=(',', ':')).encode('utf-8'))


def _frames(hb, typ):
    hb.lora.sent.clear()
    hb.send_heartbeat() if typ == 'hb' else hb.handle_ping({'g': hb.gw_id})
    return hb.lora.sent


def main():
    # v12.15: HB/pong = informacje, hashe w osobnej ramce `hs` — KOMPLET w każdym beacie.
    hb = _hb()
    for typ in ('hb', 'pong'):
        fr = _frames(hb, typ)
        assert [f['t'] for f in fr] == [typ, 'hs'], fr
        info, hs = fr
        # 1. obie ramki pod sufitem roboczym (z zapasem)
        assert _size(info) <= GatewayHeartbeat.FRAME_SAFE, (typ, _size(info))
        assert _size(hs) <= GatewayHeartbeat.FRAME_SAFE, ('hs', _size(hs))
        # 2. liveness w info, ZERO hashy w info
        for k in KRYTYCZNE:
            assert k in info, f'porzucono pole krytyczne {k}'
        assert not set(GatewayHeartbeat.HASH_KEYS) & set(info), info
        # 3. komplet hashy w KAŻDEJ ramce hs (koniec rotacji/głodzenia pól)
        assert set(GatewayHeartbeat.HASH_KEYS) <= set(hs), hs
        assert hs['g'] == 'G1' and hs['ts'] == info['ts']

    # 4. bramka bez hashy (np. przed discovery) → sama ramka info, bez pustego `hs`
    maly = _hb(devs=1, diag={'ga': 1}, gw='G2')
    assert [f['t'] for f in _frames(maly, 'hb')] == ['hb']

    # 5. skrajność: bardzo szerokie hashe dalej nie przebijają limitu Meshtastic
    gruby = _hb(devs=999, diag={k: 'F' * 24 for k in GatewayHeartbeat.HASH_KEYS})
    for f in _frames(gruby, 'hb'):
        assert _size(f) <= GatewayHeartbeat.FRAME_LIMIT, _size(f)

    info, hs = _frames(hb, 'hb')
    print(f"OK — 5/5: hb={_size(info)} B + hs={_size(hs)} B (komplet 6 hashy), "
          f"limit {GatewayHeartbeat.FRAME_LIMIT} B, sufit roboczy {GatewayHeartbeat.FRAME_SAFE} B")


if __name__ == "__main__":
    main()
