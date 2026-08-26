#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test dziennika ruchu: parsowanie linii harnessu, odciski ramek, wykrywanie strat.

Bez frameworków — `python tools/lora_traffic_log_test.py`. Sprawdza to, co realnie może się
zepsuć: czy regex łapie prawdziwe linie, czy ODRZUCA duplikaty RF (inaczej zawyżałby odbiór),
czy odcisk ramki pozwala dopasować ją po drugiej stronie i czy zestawienie widzi zgubiony HB.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lora_traffic_log as m  # noqa: E402

PRAWDZIWE = [
    ('15:11:44 [INFO] [TX] \U0001f4e4 TX: {"t":"hb","g":"G1","up":7,"dev":194,"mon":78}', 'TX', 'hb', 'up=7'),
    ('15:23:14 [INFO] [RX] \U0001f4e5 RX: {"t":"b","g":"G1","ts":1786627080,"d":[[95,{"a":0}]]}', 'RX', 'b', 'ts=1786627080'),
    ('15:11:44 [INFO] [RX] \U0001f4e5 RX: {"t":"rt_begin","tid":"gdx8","g":"G1","kind":"devmap","n":23}', 'RX', 'rt_begin', 'tid=gdx8'),
    ('16:30:00 [INFO] [TX] \U0001f4e4 TX: {"t":"rt_chunk","tid":"abc1","s":4,"g":"G1"}', 'TX', 'rt_chunk', 'tid=abc1/s=4'),
]

ODRZUCANE = [
    # duplikat radiowy — ramka ODRZUCONA, nie wolno jej liczyć jako odbioru
    '15:12:00 [DEBUG] [RX] ⏭️ RF-dup (1.2s) odrzucony: {"t":"hb","g":"G1","up":7}',
    '15:11:36 [INFO] [MAIN] SUPERVISOR running (STEP 5)',
    '15:12:09 [WARN] [ANOM] \U0001f6a8 G1/RB6_1 offline',
]


def test_parsowanie():
    for linia, kier, typ, kl in PRAWDZIWE:
        g = m.LINIA.match(linia.strip())
        assert g, f'nie sparsowano: {linia[:60]}'
        assert g.group(2) == kier, (g.group(2), kier)
        import json
        d = json.loads(g.group(3))
        assert d['t'] == typ, (d['t'], typ)
        assert m.klucz(d) == kl, (m.klucz(d), kl)
    print('OK  parsowanie prawdziwych linii RX/TX + odciski ramek')


def test_odrzucanie():
    for linia in ODRZUCANE:
        assert not m.LINIA.match(linia.strip()), f'NIE POWINNO sie parsowac: {linia[:60]}'
    print('OK  RF-dup i linie nie-ramkowe pomijane (brak zawyzania odbioru)')


def test_zestawienie_widzi_strate(capsys=None):
    """Bramka nadała 3 HB, supervisor odebrał 2 → zestawienie MUSI wskazać ten trzeci."""
    d = tempfile.mkdtemp()
    a, b = os.path.join(d, 'gw.tsv'), os.path.join(d, 'sup.tsv')
    with open(a, 'w', encoding='utf-8') as f:
        for up in (7, 907, 1807):
            f.write(f'2026-08-13 15:00:00\tTX\tG1\thb\t120\tup={up}\n')
    with open(b, 'w', encoding='utf-8') as f:
        for up in (7, 1807):                       # 907 zginął na RF
            f.write(f'2026-08-13 15:00:00\tRX\tG1\thb\t120\tup={up}\n')

    wa, wb = m.wczytaj(a), m.wczytaj(b)
    assert len(wa) == 3 and len(wb) == 2, (len(wa), len(wb))
    tx = {(x['t'], x['key']) for x in wa if x['dir'] == 'TX'}
    rx = {(x['t'], x['key']) for x in wb if x['dir'] == 'RX'}
    zgubione = tx - rx
    assert zgubione == {('hb', 'up=907')}, zgubione
    m.compare(a, b)                                # nie może się wywrócić
    print('OK  zestawienie wskazuje dokladnie zgubiona ramke (hb up=907)')


def test_gaps_z_jednego_pliku():
    """Sedno przy odciętym carporcie: straty MUSZĄ być wykrywalne z samego logu supervisora.
    HB co 900 s → up 7, 907, (brak 1807), 2707 = jeden zgubiony."""
    d = tempfile.mkdtemp()
    p = os.path.join(d, 'sup.tsv')
    with open(p, 'w', encoding='utf-8') as f:
        for up in (7, 907, 2707):                   # 1807 zginął
            f.write(f'2026-08-13 15:00:00\tRX\tG1\thb\t216\tup={up}\n')
        for s in (0, 1, 3):                         # chunk 2 zginął
            f.write(f'2026-08-13 15:05:00\tRX\tG1\trt_chunk\t80\ttid=abc1/s={s}\n')

    w = m.wczytaj(p)
    ups = sorted(int(x['key'][3:]) for x in w if x['t'] == 'hb')
    braki = sum(round((b - a) / 900) - 1 for a, b in zip(ups, ups[1:]))
    assert braki == 1, braki

    s = {int(x['key'].split('/s=')[1]) for x in w if '/s=' in x['key']}
    assert sorted(set(range(max(s) + 1)) - s) == [2], s

    m.gaps(p)                                        # nie może się wywrócić
    print('OK  straty wykryte z JEDNEGO pliku (1 zgubiony HB + chunk 2)')


def test_pelna_ramka_i_zgodnosc_wstecz():
    """Sens zbierania = móc PÓŹNIEJ zapytać o coś, czego dziś nie przewidzieliśmy — więc ostatnia
    kolumna trzyma całą ramkę. Pliki w starym formacie (6 kolumn) muszą dalej się czytać."""
    d = tempfile.mkdtemp()
    p = os.path.join(d, 'mix.tsv')
    ramka = '{"t":"hb","g":"G1","up":907,"dev":194,"mon":78,"dh":"0"}'
    with open(p, 'w', encoding='utf-8') as f:
        f.write('2026-08-14 10:00:00\tRX\tG1\thb\t60\tup=907\t' + ramka + '\n')   # nowy
        f.write('2026-08-14 10:15:00\tRX\tG1\thb\t60\tup=1807\n')                 # stary

    w = m.wczytaj(p)
    assert len(w) == 2, len(w)
    assert w[0]['raw'] == ramka, w[0]['raw']
    assert w[1]['raw'] == '', w[1]['raw']

    import json as _j
    d0 = _j.loads(w[0]['raw'])
    assert d0['mon'] == 78 and d0['dh'] == '0', d0      # da się sięgnąć po DOWOLNE pole
    print('OK  pelna ramka zapisana + stare pliki (6 kolumn) nadal czytane')


if __name__ == '__main__':
    test_parsowanie()
    test_odrzucanie()
    test_zestawienie_widzi_strate()
    test_gaps_z_jednego_pliku()
    test_pelna_ramka_i_zgodnosc_wstecz()
    print('\n5/5 passed')
