#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sprawdzian: BURST `b` idzie JEDNYM pakietem-plikiem, maly flush zostaje na ramkach.

Tlo (G1, pomiar 56 h, 2026-09-11): `b` = 443 pojedyncze ramki = 17% calego pasma TX bramki.
Rebase „wszystko-jako-plik" objal kalendarz, devmap, `ansnap` i `avail` — ale NIE wolumen `b`.
Polecenie usera: „burst komend i burst stanow w pakietach jak harmonogram".

Pilnujemy trzech rzeczy:
  1. duzy flush -> 1 transfer, ZERO ramek `b`,
  2. maly flush -> po staremu ramki `b` (transfer ma wlasny narzut, ponizej progu jest drozszy),
  3. gdy transfer PADNIE -> fallback na ramki `b`, czyli ZERO utraty danych.
Uruchom: venv/bin/python test_bdata_blob.py
"""
from modules.data.batcher import Batcher


def _batcher(blob_fn=None, **kw):
    sent = []
    b = Batcher('G1', sent.append, interval=9999, max_payload=120,
                tag='TEST', blob_fn=blob_fn, **kw)
    return b, sent


def main():
    # 1. BURST: 40 urzadzen nie miesci sie w 3 ramkach -> ma isc jednym transferem
    blobs = []
    b, sent = _batcher(blob_fn=blobs.append)
    for sid in range(40):
        b.add(sid, {'s': sid % 2, 'b': 90})
    b.flush()
    assert len(blobs) == 1, f'oczekiwano 1 transferu, jest {len(blobs)}'
    assert sent == [], f'przy transferze nie moze wyjsc ZADNA ramka `b`: {len(sent)}'
    d = blobs[0]['d']
    assert len(d) == 40, len(d)
    assert 'ts' in blobs[0]
    # tresc musi byc 1:1 z formatem ramki `b` — supervisor oddaje to temu samemu handle_b
    assert all(isinstance(e, list) and len(e) == 2 for e in d)

    # 2. MALY flush: 2 urzadzenia = 1 ramka -> bez transferu
    blobs2 = []
    b2, sent2 = _batcher(blob_fn=blobs2.append)
    b2.add(1, {'s': 1}); b2.add(2, {'s': 0})
    pkts = b2.flush()
    assert blobs2 == [], 'maly flush nie moze isc transferem'
    assert len(sent2) == len(pkts) == 1, (len(sent2), len(pkts))
    assert sent2[0]['t'] == 'b'

    # 3. TRANSFER PADA -> fallback na ramki `b`, nic nie ginie
    def _pada(_payload):
        raise RuntimeError('lacze padlo w trakcie rt_begin')
    b3, sent3 = _batcher(blob_fn=_pada)
    for sid in range(40):
        b3.add(sid, {'s': 1, 'b': 77})
    b3.flush()
    assert sent3, 'po padzie transferu MUSZA polecec ramki `b`'
    odzyskane = sum(len(p['d']) for p in sent3)
    assert odzyskane == 40, f'zgubiono dane: {odzyskane}/40'

    # 4. brak blob_fn (zgodnosc wsteczna) -> zachowanie dokladnie jak dotad
    b4, sent4 = _batcher()
    for sid in range(40):
        b4.add(sid, {'s': 1})
    b4.flush()
    assert sent4 and all(p['t'] == 'b' for p in sent4)

    # 5. prog: domyslnie 3 ramki; przy blob_min_packets=99 burst zostaje na ramkach
    blobs5 = []
    b5, sent5 = _batcher(blob_fn=blobs5.append, blob_min_packets=99)
    for sid in range(40):
        b5.add(sid, {'s': 1})
    b5.flush()
    assert blobs5 == [] and sent5, 'prog blob_min_packets nie jest respektowany'

    print(f"OK — 5/5: burst 40 dev -> 1 transfer (zamiast {len(sent3)} ramek `b`), "
          f"maly flush bez zmian, pad transferu = 0 utraty")


if __name__ == "__main__":
    main()
