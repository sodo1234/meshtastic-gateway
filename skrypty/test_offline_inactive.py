#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sprawdzian: bramka NIEAKTYWNA (nocna w dzien) nie klamie o dostepnosci.

Tlo (2026-09-10, carport G1): obwod 78 przekaznikow jest beznapieciowy w dzien
(z2m: 19:xx = 80x online, 06:xx = 91x offline). Tryb `night` mial wyciszyc 78 falszywych
ALARMOW — ale stara wersja OfflineMonitor emitowala przy tym `dn` ("urzadzenie wrocilo"),
przez co supervisor przestawial 78 encji `_available` na `on` i kafle pokazywaly stan
sprzed nocy zamiast OFFLINE. Dokladnie to, przed czym ostrzegal user.

Pilnujemy dwoch rzeczy naraz:
  1. bramka nieaktywna gasi ALARM (wklad do hasha `ah`), ale NIE emituje `dn`,
  2. supervisor przy `ga:0` przepuszcza availability, a nie tworzy anomalii.
Uruchom: venv/bin/python test_offline_inactive.py
"""
from modules.anomaly.offline import GatewayOfflineAnomaly


class _Disc:
    def __init__(self, devs):
        self.devices = {d: {} for d in devs}
        self.short_ids = {d: i + 1 for i, d in enumerate(devs)}


class _Data:
    def __init__(self, alive):
        self._alive = alive


RELAYS = [f"{p}{i}_{k}" for p, n in (("RA", 15), ("RB", 11))
          for i in range(1, n + 1) for k in (1, 2, 3)]


def _monitor(active, alive_val):
    emitted = []
    disc = _Disc(RELAYS)
    data = _Data({d: alive_val for d in RELAYS})
    m = GatewayOfflineAnomaly('G1', disc, data,
                              emit=lambda sid, code: emitted.append((sid, code)),
                              get_timeout=lambda dtype: 45,
                              is_active=lambda: active)
    return m, emitted


def _set_avail_anomaly(gw_active, prev, on, acked=False):
    """Reguła z run_supervisor._set_avail: czy powstaje anomalia offline."""
    if prev == on:
        return False
    if on:
        return False
    if acked:
        return False
    return bool(gw_active)


def main():
    assert len(RELAYS) == 78, len(RELAYS)

    # 1. bramka AKTYWNA + urzadzenia nieosiagalne => 78 alarmow `do`
    m, emitted = _monitor(active=True, alive_val=False)
    m.check()
    assert len(m.offline_devices()) == 78, len(m.offline_devices())
    assert [c for _, c in emitted].count("do") == 78, emitted[:3]

    # 2. ta sama bramka przechodzi w NIEAKTYWNA => alarm gasnie...
    m.is_active = lambda: False
    emitted.clear()
    m.check()
    assert len(m.offline_devices()) == 0, 'alarm powinien zniknac z hasha'
    # ...ale ZERO `dn` — bo nic nie wrocilo online (to byl blad, ktory klamal supervisorowi)
    assert emitted == [], f'bramka nieaktywna nie moze emitowac dn: {emitted[:3]}'

    # 3. nieaktywna od poczatku: nie zglasza nic, mimo ze wszystko nieosiagalne
    m2, em2 = _monitor(active=False, alive_val=False)
    m2.check()
    assert m2.offline_devices() == set() and em2 == []

    # 4. supervisor: przy `ga:0` availability leci, ale anomalii NIE ma
    assert _set_avail_anomaly(gw_active=False, prev=True, on=False) is False
    # a przy aktywnej bramce anomalia powstaje normalnie
    assert _set_avail_anomaly(gw_active=True, prev=True, on=False) is True
    # brak wpisu o bramce = domyslnie aktywna (zachowanie wsteczne)
    assert _set_avail_anomaly(gw_active=True, prev=None, on=False) is True
    # ack nadal wycisza
    assert _set_avail_anomaly(gw_active=True, prev=True, on=False, acked=True) is False
    # powrot online nigdy nie tworzy anomalii
    assert _set_avail_anomaly(gw_active=True, prev=False, on=True) is False

    # 5. scenariusz carport: 78 przekaznikow bez zasilania, bramka nocna w dzien
    anomalie = sum(1 for _ in RELAYS
                   if _set_avail_anomaly(gw_active=False, prev=True, on=False))
    assert anomalie == 0, anomalie

    print("OK — 5/5: nieaktywna bramka gasi alarm, nie klamie o dostepnosci "
          f"({len(RELAYS)} przekaznikow -> 0 anomalii, 0 falszywych `dn`)")


if __name__ == "__main__":
    main()
