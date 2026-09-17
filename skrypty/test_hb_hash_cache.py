#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""REGRESJA 2026-09-17: przyciete pole HB robilo FALSZYWY „ROZJAZD" i czyscilo kafel.

Objaw zgloszony przez usera: na dashboardzie supervisora dla G1 „HASH ANOM." pokazuje `--`,
a „DANE" stoi na ROZJAZD ⚠️, mimo ze bramka i supervisor sa zgodne.

Przyczyna: `GatewayHeartbeat._fit` przycina ramke do sufitu 200 B i porzuca rotacyjnie pola
diagnostyczne — na G1 przy niemal kazdym beacie (zmierzone: 357 przycietych ramek na 362).
Kontrakt mowi „brak pola = BEZ ZDANIA", ale odbiorca robil `d.get('ah','--')`, wiec:
  • kafel hasha byl nadpisywany `--`,
  • `_data_ok`/`_disc_ok`/`_param_ok`/`_cal_ok` przy braku pola dawaly False = falszywy rozjazd,
  • flagi `zg` migotaly (11110 → 10111 → 01111), kazda zmiana = push ramki `zg` po LoRa.

Uruchom: venv/bin/python test_hb_hash_cache.py
"""
from modules.protocol import merge_hb_hashes
from modules.protocol.heartbeat import GatewayHeartbeat

KLUCZE = ('hash', 'ph', 'cal', 'ah', 'dh', 'sh')


def main():
    # ── 1. Sekwencja DOKLADNIE jak w logu G1 13:00-13:30 (kolejne beaty gubia inne pola) ──
    cache = {}
    pelny = {'hash': 'f795e257116c', 'ph': 'c6a8bf7e', 'cal': 'a81c8791a42c',
             'ah': 'd7517139', 'dh': '0', 'sh': '26243c'}
    v = merge_hb_hashes(cache, dict(pelny), KLUCZE)
    assert v == pelny, v

    bez_ah = {k: x for k, x in pelny.items() if k not in ('ah', 'dh')}   # ✂️ pominieto ah,dh
    v = merge_hb_hashes(cache, bez_ah, KLUCZE)
    assert v['ah'] == 'd7517139', f'kafel anomalii zgasl mimo znanej wartosci: {v["ah"]}'
    assert v['dh'] == '0', f'flaga DANE poszlaby na falszywy rozjazd: {v["dh"]}'

    bez_sh = {k: x for k, x in pelny.items() if k != 'sh'}               # ✂️ pominieto sh
    v = merge_hb_hashes(cache, bez_sh, KLUCZE)
    assert v['sh'] == '26243c', v['sh']

    # ── 2. Nowa wartosc MUSI przebic cache (inaczej realny rozjazd bylby ukryty) ──
    v = merge_hb_hashes(cache, {**pelny, 'ah': 'ZMIENIONY'}, KLUCZE)
    assert v['ah'] == 'ZMIENIONY', v['ah']
    v = merge_hb_hashes(cache, {k: x for k, x in pelny.items() if k != 'ah'}, KLUCZE)
    assert v['ah'] == 'ZMIENIONY', 'cache musi trzymac NAJNOWSZA wartosc, nie pierwsza'

    # ── 3. Pole, ktore NIGDY nie przyszlo, zostaje '--' (G2 bez anomalii = brak `ah`) ──
    v = merge_hb_hashes({}, {'hash': 'e294f674d184'}, KLUCZE)
    assert v['ah'] == '--' and v['dh'] == '--', v
    assert v['hash'] == 'e294f674d184', v

    # ── 4. `dh` == '0' to PELNOPRAWNA wartosc, nie brak (bramka nic jeszcze nie dowiozla) ──
    c2 = {}
    merge_hb_hashes(c2, {'dh': 'abc123'}, KLUCZE)
    v = merge_hb_hashes(c2, {'dh': '0'}, KLUCZE)
    assert v['dh'] == '0', f'falsy wartosc zostala potraktowana jak brak: {v["dh"]}'

    # ── 5. Spiecie z realnym `_fit`: ramka nad sufitem GUBI pola, a cache to laga ──
    hb = GatewayHeartbeat.__new__(GatewayHeartbeat)      # bez I/O — testujemy sam przyciety payload
    hb.log = None
    duza = {'t': 'hb', 'g': 'G1', 'up': 481626, 'dev': 194, 'mon': 107, 'pri': 0,
            'ts': 1789643762, 'm': 0, 'tq': 'synced', 'gm': 'night', 'ga': 0, **pelny}
    assert GatewayHeartbeat._size(duza) > GatewayHeartbeat.FRAME_SAFE, \
        'ramka z kompletem hashy miala przekraczac sufit — inaczej ten test nic nie pilnuje'
    przyciete = hb._fit(dict(duza))
    zgubione = [k for k in KLUCZE if k not in przyciete]
    assert zgubione, 'fit mial cos porzucic'
    c3 = dict(pelny)
    v = merge_hb_hashes(c3, przyciete, KLUCZE)
    assert all(v[k] != '--' for k in KLUCZE), f'po realnym przycieciu kafle zgasly: {v}'
    assert GatewayHeartbeat._size(przyciete) <= GatewayHeartbeat.FRAME_SAFE

    print(f"OK — 5/5: przyciete pola ({','.join(zgubione)}) nie gasza kafli "
          f"i nie robia falszywego ROZJAZDU; nowa wartosc przebija cache")


if __name__ == "__main__":
    main()
