"""Sprawdzian GroupCaster: groupcast zamiast N unicastow, ale gwarancja z weryfikacji.

Uruchom: venv/bin/python -m modules.control.smoke_test
"""
import sys

from modules.control import GroupCaster

# FAZY 2 i 3 — jak w realnym podziale na obiekcie. Faza 1 (`_1`) swieci NA STALE, dopoki
# przekazniki maja zasilanie, wiec NIE moze byc czlonkiem grupy: jeden groupcast by ja zgasil,
# a harmonogram (ktory rusza wylacznie `_2`/`_3`) nigdy by jej nie zapalil.
FAZY = (2, 3)
GRUPY = {
    'GRP_RA1_6':   [f'RA{s}_{k}' for s in range(1, 7) for k in FAZY],      # 12
    'GRP_RA7_12':  [f'RA{s}_{k}' for s in range(7, 13) for k in FAZY],     # 12
    'GRP_RA13_15': [f'RA{s}_{k}' for s in range(13, 16) for k in FAZY],    # 6
    'GRP_RB1_3':   [f'RB{s}_{k}' for s in range(1, 4) for k in FAZY],      # 6
    'GRP_RB4_7':   [f'RB{s}_{k}' for s in range(4, 8) for k in FAZY],      # 8
    'GRP_RB8_11':  [f'RB{s}_{k}' for s in range(8, 12) for k in FAZY],     # 8
}
WSZYSTKIE = [d for m in GRUPY.values() for d in m]


class Swiat:
    """Udawany Zigbee: groupcast przelacza czlonkow, unicast jedno urzadzenie.
    `gluche` to przekazniki, ktore ignoruja GROUPCAST (dokladnie objaw z obiektu),
    ale reaguja na unicast — czyli dosylanie MUSI je dobic."""

    def __init__(self, gluche=(), gluche_na_zawsze=()):
        self.stan = {d: 'OFF' for d in WSZYSTKIE}
        self.gluche = set(gluche)
        self.martwe = set(gluche_na_zawsze)
        self.publikacje = []

    def publish(self, topic, payload):
        import json
        self.publikacje.append(topic)
        cel = topic[len('zigbee2mqtt/'):-len('/set')]
        val = json.loads(payload).get('state')
        if cel in GRUPY:
            for d in GRUPY[cel]:
                if d not in self.gluche and d not in self.martwe:
                    self.stan[d] = val
        else:
            if cel not in self.martwe:
                self.stan[cel] = val

    def get_state(self, dev):
        return self.stan.get(dev)


def _caster(sw, **kw):
    kw.setdefault('settle', 0); kw.setdefault('verify_wait', 0); kw.setdefault('unicast_gap', 0)
    gc = GroupCaster(sw.publish, sw.get_state, **kw)
    gc.set_groups(GRUPY)
    return gc


def test_wszystkie_jednym_groupcastem_na_grupe():
    sw = Swiat()
    gc = _caster(sw)
    r = gc.apply(WSZYSTKIE, 'state', 'ON')
    assert r['total'] == 52, r
    assert r['groups'] == 6 and r['unicast'] == 0, r
    assert r['missing'] == [] and r['ok'] == 52, r
    assert len(sw.publikacje) == 6, f'52 urzadzenia maja kosztowac 6 publikacji, jest {len(sw.publikacje)}'
    assert all(v == 'ON' for v in sw.stan.values())
    print('✅ 52 przekazniki (fazy 2+3) = 6 groupcastow (bylo 52 unicasty), wszystkie ON')


def test_gluche_na_groupcast_dobijane_unicastem():
    gluche = ['RA3_2', 'RB6_2', 'RB10_3']
    sw = Swiat(gluche=gluche)
    for d in WSZYSTKIE:                 # wyjscie ze stanu ON, zeby OFF byl REALNA zmiana
        sw.stan[d] = 'ON'
    gc = _caster(sw)
    r = gc.apply(WSZYSTKIE, 'state', 'OFF')
    assert r['missing'] == [], f'dosylanie nie dobilo gluchych: {r["missing"]}'
    assert r['ok'] == 52, r
    # 6 groupcastow + dokladnie 3 dosylki — NIE cala grupa
    assert len(sw.publikacje) == 6 + len(gluche), sw.publikacje[6:]
    assert all(sw.stan[d] == 'OFF' for d in gluche)
    print('✅ gluche na groupcast dobite unicastem — doslano 3, nie 3 cale grupy')


def test_trwale_martwe_raportowane_a_nie_przemilczane():
    """Przekaznik realnie martwy MUSI zostac w `missing` — cisza to najgorszy mozliwy wynik."""
    sw = Swiat(gluche_na_zawsze=['RA5_2'])
    gc = _caster(sw, retries=2)
    r = gc.apply(WSZYSTKIE, 'state', 'ON')
    assert r['missing'] == ['RA5_2'], r
    assert r['ok'] == 51 and r['total'] == 52, r
    print('✅ trwale martwy przekaznik raportowany jako missing, nie przemilczany')


def test_czesciowe_pokrycie_nie_rusza_obcych():
    """Zgaszenie SAMEJ szyny RA1 nie moze zgasic calej grupy RA1-RA6 przez multicast."""
    sw = Swiat()
    gc = _caster(sw)
    gc.apply(WSZYSTKIE, 'state', 'ON')
    sw.publikacje.clear()
    szyna = ['RA1_2', 'RA1_3']
    r = gc.apply(szyna, 'state', 'OFF')
    assert r['groups'] == 0 and r['unicast'] == 2, r
    assert all(sw.stan[d] == 'OFF' for d in szyna)
    obce = [d for d in GRUPY['GRP_RA1_6'] if d not in szyna]
    assert all(sw.stan[d] == 'ON' for d in obce), 'multicast ruszyl urzadzenia spoza zadania!'
    print('✅ czesciowe pokrycie idzie unicastem — groupcast nie rusza obcych przekaznikow')


def test_sekwencyjnie_nie_jednym_strzalem():
    """Grupy maja isc PO KOLEI z odstepem — to byl jawny wymog: nie jeden strzal w 78."""
    sw = Swiat()
    odstepy = []
    gc = _caster(sw, settle=2.0)
    gc.apply(WSZYSTKIE, 'state', 'ON', sleep=odstepy.append)
    przerwy = [x for x in odstepy if x == 2.0]
    assert len(przerwy) == 5, f'miedzy 6 grupami ma byc 5 przerw, jest {len(przerwy)}'
    print('✅ grupy ida sekwencyjnie z odstepem (5 przerw miedzy 6 groupcastami)')


def test_stan_nieznany_liczy_sie_jako_brakujacy():
    sw = Swiat()
    sw.stan['RA2_2'] = None
    gc = _caster(sw)
    r = gc.apply(['RA2_2'], 'state', 'ON')
    assert r['missing'] == [] and sw.stan['RA2_2'] == 'ON', r
    print('✅ stan nieznany traktowany jak brakujacy → doslane')


if __name__ == "__main__":
    tests = [test_wszystkie_jednym_groupcastem_na_grupe,
             test_gluche_na_groupcast_dobijane_unicastem,
             test_trwale_martwe_raportowane_a_nie_przemilczane,
             test_czesciowe_pokrycie_nie_rusza_obcych,
             test_sekwencyjnie_nie_jednym_strzalem,
             test_stan_nieznany_liczy_sie_jako_brakujacy]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─' * 40}\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
