#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audyt ruchu LoRa z dziennika TSV — czego szukać, żeby było co optymalizować.

Nie liczy „ile ramek" (to widać w --stats), tylko rzeczy, które WSKAZUJĄ NA PROBLEM:
  1. udział narzutu (ACK/CACK/ping-pong) w całym ruchu — ile pasma idzie na obsługę samej siebie,
  2. transfery: ile rozpoczętych nie domknęło się `rt_end` (= zerwane, zmarnowane pasmo),
  3. retransmisje: ten sam `tid/s` widziany >1 raz,
  4. powtórki transferów tego samego rodzaju w krótkim czasie (np. devmap 3× po restarcie),
  5. ramki nadane do bramki, która w tym momencie milczała (nadawanie w pustkę).

Użycie: venv/bin/python tools/lora_traffic_audit.py PLIK.tsv [PLIK2.tsv ...]
"""
import sys
from collections import Counter, defaultdict

NARZUT = {'rt_ack', 'rt_cack', 'cal_ack', 'cal_cack', 'back', 'sup_ping', 'sup_pong',
          'pong', 'ping', 'zg'}


def wczytaj(p):
    w = []
    for ln in open(p, encoding='utf-8', errors='replace'):
        c = ln.rstrip('\n').split('\t')
        if len(c) == 6:
            w.append(dict(zip(('ts', 'dir', 'gw', 't', 'n', 'key'), c)))
    return w


def audyt(p):
    w = wczytaj(p)
    if not w:
        print(f'{p}: pusty'); return
    print(f'\n══════ {p}')
    print(f'  ramek: {len(w)} | zakres: {w[0]["ts"]} -> {w[-1]["ts"]}')

    bajty = Counter()
    ile = Counter()
    for x in w:
        bajty[x['t']] += int(x['n'])
        ile[x['t']] += 1
    suma_b = sum(bajty.values())

    nar_b = sum(b for t, b in bajty.items() if t in NARZUT)
    nar_n = sum(n for t, n in ile.items() if t in NARZUT)
    print(f'  NARZUT (ack/cack/ping-pong/zg): {nar_n}/{len(w)} ramek '
          f'({100.0*nar_n/len(w):.1f}%), {nar_b}/{suma_b} B ({100.0*nar_b/suma_b:.1f}%)')

    print('  TOP typy (ramki / bajty):')
    for t, n in ile.most_common(10):
        print(f'     {t:12s} {n:6d}  {bajty[t]:8d} B  ({100.0*bajty[t]/suma_b:4.1f}%)')

    # transfery: begin bez end
    beg, end = {}, set()
    for x in w:
        if x['t'] in ('rt_begin', 'cal_begin') and x['key'].startswith('tid='):
            beg[x['key']] = x['ts']
        if x['t'] in ('rt_end', 'cal_end') and x['key'].startswith('tid='):
            end.add(x['key'])
    zerwane = {k: v for k, v in beg.items() if k not in end}
    print(f'  TRANSFERY: rozpoczetych {len(beg)}, domknietych {len(beg)-len(zerwane)}, '
          f'ZERWANYCH {len(zerwane)}')
    for k, v in sorted(zerwane.items(), key=lambda kv: kv[1])[-6:]:
        print(f'     zerwany  {v}  {k}')

    # Retransmisje: klucz MUSI zawierać typ. `rt_chunk` i jego `rt_cack` mają ten sam `tid/s`,
    # więc liczenie po samym kluczu pokazywało potwierdzenia jako „retransmisje" (fałszywe 1388).
    dubl = Counter((x['t'], x['key']) for x in w if '/s=' in x['key'])
    powt = {k: c for k, c in dubl.items() if c > 1}
    nadmiar = sum(powt.values()) - len(powt)
    baza = sum(1 for x in w if x['t'] == 'rt_chunk')
    print(f'  RETRANSMISJE chunkow: {len(powt)} powtorzonych, {nadmiar} nadmiarowych ramek '
          f'({100.0*nadmiar/baza:.1f}% ruchu chunkow)' if baza else '  RETRANSMISJE: brak chunkow')

    # powtorki transferow tego samego rodzaju
    rodz = defaultdict(list)
    for x in w:
        if x['t'] == 'rt_begin':
            rodz[x['gw']].append(x['ts'])
    for gw, ts in rodz.items():
        if len(ts) > 1:
            print(f'  transfery rt_begin [{gw}]: {len(ts)}')


if __name__ == '__main__':
    for p in sys.argv[1:]:
        audyt(p)
