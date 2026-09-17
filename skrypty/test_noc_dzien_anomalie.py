#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pelny cykl doba: anomalie Z NOCY przezywaja dzien, a dzien NIE robi 78 nowych.

Pytanie usera (2026-09-17): „czy anomalie z nocy moga przetrwac na supervisorze — tzn. w nocy
wygenerowano anomalie, w dzien one tez sa widoczne — ale tak, zeby jednoczesnie nie generowalo
sie 80 anomalii offline w dzien, skoro wiadomo, ze wtedy nie ma zasilania?"

To sa DWA NIEZALEZNE mechanizmy i oba musza dzialac naraz:
  A. brama na TWORZENIE — `GatewayOfflineAnomaly(is_active=...)`: bramka nieaktywna nie zglasza
     nowych anomalii offline (inaczej co rano 78 falszywych alarmow o obwodzie bez napiecia);
  B. wyciszenie na KASOWANIE — `AnomalyReconciler(muted_cat=...)`: skoro silnik z punktu A
     przestal potwierdzac anomalie, reconciler NIE MOZE uznac tego za ich ustanie i skasowac
     wpisow. Bez B urzadzenie martwe od tygodnia bylo w dzien niewidoczne (RB6_2, RA6_2).

Sam punkt A bez B = anomalia znika o swicie. Sam B bez A = 78 anomalii co rano.
Ten test pilnuje OBU naraz przez pelny cykl: noc → swit → caly dzien → zmierzch.

Uruchom: venv/bin/python test_noc_dzien_anomalie.py
"""
from modules.anomaly import AnomalyStore, AnomalyReconciler
from modules.anomaly.offline import GatewayOfflineAnomaly

# 26 szyn x 3 fazy = 78 przekaznikow, jak na obiekcie
RELAYS = [f"{p}{i}_{k}" for p, n in (("RA", 15), ("RB", 11))
          for i in range(1, n + 1) for k in (1, 2, 3)]
MARTWE = ["RB6_2", "RA6_2"]          # nie wstaly ani razu przez 5 dni (zmierzone na G1)
SID = {d: i + 1 for i, d in enumerate(RELAYS)}
REV = {v: k for k, v in SID.items()}


class _Disc:
    devices = {d: {} for d in RELAYS}
    short_ids = SID


class _Data:
    def __init__(self):
        self._alive = {}


def main():
    swiat = {"aktywna": True}            # tryb night: aktywna w nocy, nieaktywna w dzien
    data = _Data()
    store = AnomalyStore(resolve_dev=lambda gw, sid: REV.get(sid))

    silnik = GatewayOfflineAnomaly(
        'G1', _Disc(), data,
        emit=lambda sid, code: store.handle_ab({'g': 'G1', 'd': [[sid, code]]}),
        get_timeout=lambda dtype: 45,
        is_active=lambda: swiat["aktywna"])

    rec = AnomalyReconciler(
        'G1', store, probes=[silnik.snapshot],
        resolve_dev=lambda sid: REV.get(sid),
        dev_to_sid=lambda dev: SID.get(dev),
        readd_fn=lambda sid, code, val=None: store.handle_ab(
            {'g': 'G1', 'd': [[sid, code]]}),
        interval=999, stale_grace=0, mute_window=0,
        muted_cat=lambda cat: cat == 'offline' and not swiat["aktywna"])

    # ── NOC: obwod pod napieciem, 76 przekaznikow wstalo, 2 nie ──────────────────
    swiat["aktywna"] = True
    data._alive = {d: (d not in MARTWE) for d in RELAYS}
    silnik.check()
    rec.run_once()
    noc = store.counts('G1')['offline']
    assert noc == len(MARTWE), f'w nocy maja byc {len(MARTWE)} anomalie, jest {noc}'
    assert set(store.offline_devs('G1')) == set(MARTWE), store.offline_devs('G1')

    # ── SWIT: obwod traci zasilanie, WSZYSTKIE 78 znikaja, bramka nieaktywna ─────
    swiat["aktywna"] = False
    data._alive = {d: False for d in RELAYS}
    for _ in range(5):                    # kilka cykli — tyle, ile trwa dzien
        silnik.check()
        rec.run_once()
    dzien = store.counts('G1')['offline']
    assert dzien == len(MARTWE), (
        f'W DZIEN ma zostac dokladnie {len(MARTWE)} anomalii z nocy, jest {dzien}. '
        f'{"Powstaly nowe (brama na tworzenie nie dziala)" if dzien > len(MARTWE) else "Skasowane (wyciszenie nie dziala)"}')
    assert set(store.offline_devs('G1')) == set(MARTWE), \
        f'w dzien widac zle urzadzenia: {sorted(store.offline_devs("G1"))[:6]}'

    # ── ZMIERZCH: napiecie wraca, 76 wstaje, te 2 nadal nie → nadal 2 ────────────
    swiat["aktywna"] = True
    data._alive = {d: (d not in MARTWE) for d in RELAYS}
    silnik.check()
    rec.run_once()
    assert store.counts('G1')['offline'] == len(MARTWE), store.counts('G1')

    # ── NAPRAWA: przekaznik wymieniony, wraca → anomalia MUSI zniknac ────────────
    data._alive = {d: True for d in RELAYS}
    silnik.check()
    rec.run_once()
    po = store.counts('G1')['offline']
    assert po == 0, f'po naprawie anomalia ma zniknac, zostalo {po}'

    print(f"OK — 5/5: noc {len(MARTWE)} anomalii → dzien {len(MARTWE)} (NIE {len(RELAYS)}, NIE 0) "
          f"→ zmierzch {len(MARTWE)} → po naprawie 0")


if __name__ == "__main__":
    main()
