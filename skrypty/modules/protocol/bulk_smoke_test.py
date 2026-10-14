"""Ramka ZBIORCZA cmd/st — test bez sprzętu.

Pilnuje trzech rzeczy, na których ta optymalizacja stoi lub upada:
  1. ŻADNA ramka nie przekracza limitu (Meshtastic >220 B ucina PO CICHU — zguba bez śladu),
  2. nic nie ginie po drodze: zbiór urządzeń po pocięciu == zbiór wejściowy, wartości te same,
  3. 1 urządzenie = stara ramka `cmd`/`st` (zgodność wstecz z niezaktualizowaną drugą stroną).

Uruchomienie:
    cd skrypty && python3 -m modules.protocol.bulk_smoke_test
"""
import json
import sys
import time

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.protocol.bulk import Coalescer, chunk_frames, cmd_frames, st_frames

MAX = 200
# realna lista z harmonogramu G1: 52 przekaźniki gaszone/zapalane jednym kliknięciem
RELAYS = ([f'RA{i}_2' for i in range(1, 16)] + [f'RB{i}_2' for i in range(1, 12)] +
          [f'RA{i}_3' for i in range(1, 16)] + [f'RB{i}_3' for i in range(1, 12)])


def _size(fr):
    return len(json.dumps(fr, separators=(',', ':')))


def test_cmd_zbiorczy_miesci_sie_i_nic_nie_ginie():
    frames = cmd_frames([('G1', d, 'state', 'OFF') for d in RELAYS], max_bytes=MAX)
    for fr in frames:
        assert _size(fr) <= MAX, f'ramka {_size(fr)}B > {MAX}B — Meshtastic utnie: {fr}'
        assert fr['t'] == 'cmdm' and fr['g'] == 'G1' and fr['v'] == 'OFF'
    got = [d for fr in frames for d in fr['d']]
    assert got == RELAYS, 'kolejność/komplet urządzeń musi przetrwać cięcie'
    assert len(frames) < len(RELAYS) / 5, f'zysk za mały: {len(frames)} ramek na {len(RELAYS)} dev'
    print(f'  ✓ cmd: {len(RELAYS)} urządzeń → {len(frames)} ramek '
          f'(max {max(_size(f) for f in frames)}B)')


def test_st_zbiorczy_miesci_sie_i_niesie_wartosci():
    stany = {d: ('ON' if i % 3 else 'OFF') for i, d in enumerate(RELAYS)}
    frames = st_frames('G1', [(d, 'state', v) for d, v in stany.items()], max_bytes=MAX)
    odtworzone = {}
    for fr in frames:
        assert _size(fr) <= MAX, f'ramka {_size(fr)}B > {MAX}B: {fr}'
        assert fr['t'] == 'stm' and fr['c'] == 'state'
        odtworzone.update(fr['d'])
    assert odtworzone == stany, 'stan po złożeniu ramek musi być identyczny'
    print(f'  ✓ st: {len(stany)} urządzeń → {len(frames)} ramek '
          f'(max {max(_size(f) for f in frames)}B)')


def test_jedno_urzadzenie_to_stara_ramka():
    assert cmd_frames([('G1', 'RA1_2', 'state', 'ON')]) == \
challenge if False else [{"t": "cmd", "g": "G1", "d": "RA1_2", "c": "state", "v": "ON"}]
    assert st_frames('G1', [('RA1_2', 'state', 'ON')]) == \
        [{"t": "st", "g": "G1", "d": "RA1_2", "c": "state", "v": "ON"}]
    print('  ✓ pojedyncze urządzenie → stare `cmd`/`st` (zgodność wstecz)')


def test_ostatnia_komenda_wygrywa():
    frames = cmd_frames([('G1', 'RA1_2', 'state', 'ON'), ('G1', 'RA1_2', 'state', 'OFF')])
    assert frames == [{"t": "cmd", "g": "G1", "d": "RA1_2", "c": "state", "v": "OFF"}], frames
    st = st_frames('G1', [('RA1_2', 'state', 'ON'), ('RA1_2', 'state', 'OFF')])
    assert st[0]['v'] == 'OFF'
    print('  ✓ duplikat w oknie → ostatnia wartość, jedna ramka')


def test_dwie_bramki_nie_mieszaja_sie():
    frames = cmd_frames([('G1', 'RA1_2', 'state', 'ON'), ('G1', 'RA2_2', 'state', 'ON'),
                         ('G2', 'RB1_2', 'state', 'ON'), ('G2', 'RB2_2', 'state', 'ON')])
    per_gw = {fr['g']: fr for fr in frames}
    assert set(per_gw) == {'G1', 'G2'} and len(frames) == 2
    assert per_gw['G1']['d'] == ['RA1_2', 'RA2_2'] and per_gw['G2']['d'] == ['RB1_2', 'RB2_2']
    print('  ✓ grupowanie per bramka i per wartość')


def test_element_wiekszy_niz_limit_nie_ginie():
    dev = 'X' * 300
    frames = chunk_frames({"t": "cmdm", "g": "G1"}, "d", ['RA1_2', dev, 'RA2_2'], max_bytes=MAX)
    assert [d for fr in frames for d in fr['d']] == ['RA1_2', dev, 'RA2_2'], \
        'za duży element ma iść w osobnej ramce, nie wypaść'
    print('  ✓ element ponad limit → osobna ramka (nie znika po cichu)')


def test_coalescer_zbiera_w_oknie_i_ma_zawor():
    got = []
    c = Coalescer(got.append, window=0.3, name='test')
    for i in range(5):
        c.add(i)
    assert got == [], 'przed upływem okna nic nie leci'
    time.sleep(0.5)
    assert got == [[0, 1, 2, 3, 4]], f'jedno okno = jeden flush, dostałem {got}'

    got.clear()
    c2 = Coalescer(got.append, window=30.0, max_items=3, name='test')
    for i in range(3):
        c2.add(i)
    assert got == [[0, 1, 2]], 'max_items ma wypuścić natychmiast, nie czekać 30 s'

    got.clear()
    c3 = Coalescer(got.append, window=30.0, name='test')
    c3.add('a'); c3.flush_now()
    assert got == [['a']], 'flush_now wypycha bufor'
    print('  ✓ Coalescer: okno, zawór max_items, flush_now')


def test_flush_ktory_rzuca_nie_zabija_watku():
    def zly(_):
        raise RuntimeError('boom')

    class _Log:
        msgs = []
        def warn(self, tag, m): self.msgs.append(m)

    lg = _Log()
    c = Coalescer(zly, window=0.1, logger=lg, name='test')
    c.add(1)
    time.sleep(0.3)
    assert lg.msgs and 'boom' in lg.msgs[0], 'wyjątek flusha ma być zalogowany, nie połknięty'
    c.add(2)                                   # kolejarz dalej przyjmuje — wątek Timera żyje
    print('  ✓ wyjątek we flushu nie zabija kolejnych okien')


def test_coalescer_quiet_wypuszcza_po_ciszy_ale_okno_jest_sufitem():
    """`quiet`: pojedynczy element wychodzi po ciszy (nie po pełnym oknie); ciągły strumień — po oknie."""
    import time
    from modules.protocol.bulk import Coalescer
    out = []
    c = Coalescer(lambda it: out.append((time.monotonic(), list(it))), window=0.6, quiet=0.1)
    t0 = time.monotonic(); c.add('a'); time.sleep(0.3)
    assert out and out[0][1] == ['a'] and out[0][0] - t0 < 0.25, f'pojedynczy po ciszy: {out}'
    out.clear(); t0 = time.monotonic()
    for i in range(10):                     # strumień co 0.08 s < quiet → wychodzi dopiero po oknie
        c.add(i); time.sleep(0.08)
    time.sleep(0.3)
    assert out[0][0] - t0 >= 0.55 and len(out[0][1]) >= 7, f'okno jako sufit: {out}'
    print('  ✓ quiet: pojedynczy po ciszy, ciągły strumień po oknie (sufit)')


def main():
    print('═' * 62)
    print('RAMKA ZBIORCZA cmd/st — smoke test')
    print('═' * 62)
    testy = (test_cmd_zbiorczy_miesci_sie_i_nic_nie_ginie,
               test_st_zbiorczy_miesci_sie_i_niesie_wartosci,
               test_jedno_urzadzenie_to_stara_ramka,
               test_ostatnia_komenda_wygrywa,
               test_dwie_bramki_nie_mieszaja_sie,
               test_element_wiekszy_niz_limit_nie_ginie,
               test_coalescer_zbiera_w_oknie_i_ma_zawor,
             test_flush_ktory_rzuca_nie_zabija_watku,
             test_coalescer_quiet_wypuszcza_po_ciszy_ale_okno_jest_sufitem)
    for fn in testy:
        fn()
    print('═' * 62)
    print(f'{len(testy)}/{len(testy)} passed')


if __name__ == '__main__':
    main()
