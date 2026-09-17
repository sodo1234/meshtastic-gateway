#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sprawdzian gate'u ponowien `cmd` (test_step5_anomaly.py, run_supervisor.control_loop).

Pilnuje jednej rzeczy: klik „WSZYSTKIE ON/OFF" na 78 przekaznikach NIE moze zamienic sie
w 78 pojedynczych ramek retry. Zmierzone 2026-09-10 na zywym supie: pierwsza tura poszla
5 ramkami `cmdm`, ponowienia per-urzadzenie zrobily z tego 78 ramek `cmd` x 2 tury.
Uruchom: venv/bin/python test_cmd_retry.py
"""
from modules.protocol.bulk import retry_action, cmd_frames

RELAYS = [f"{p}{i}_{k}" for p, n in (("RA", 15), ("RB", 11))
          for i in range(1, n + 1) for k in (1, 2, 3)]


def main():
    assert len(RELAYS) == 78, len(RELAYS)

    # 1. urzadzenie ZNANE JAKO NIEDOSTEPNE nie dostaje ponowienia — od razu 'fail'
    assert retry_action(2, False) == 'fail'
    assert retry_action(1, False) == 'fail'

    # 2. dostepne albo jeszcze nieokreslone (grace / brak wpisu) — ponawiamy normalnie
    assert retry_action(2, True) == 'retry'
    assert retry_action(1, True) == 'retry'
    assert retry_action(2, None) == 'retry', 'brak wiedzy != niedostepne'

    # 3. wyczerpane proby zawsze koncza sie 'fail', niezaleznie od dostepnosci
    for av in (True, False, None):
        assert retry_action(0, av) == 'fail'

    # 4. ponowienia dla ZYWYCH urzadzen ida Coalescerem => ramki zbiorcze, nie 78 pojedynczych
    items = [("G1", d, "state", "OFF") for d in RELAYS]
    frames = cmd_frames([(g, d, c, v) for g, d, c, v in items], max_bytes=200)
    assert all(f["t"] == "cmdm" for f in frames), [f["t"] for f in frames]
    assert len(frames) < 10, f"za duzo ramek: {len(frames)}"
    assert sum(len(f["d"]) for f in frames) == 78
    assert all(len(str(f)) <= 260 for f in frames), 'ramka przekracza limit Meshtastic'

    # 5. scenariusz z 2026-09-10: 78 przekaznikow bez zasilania => ZERO ponowien
    ponowienia = sum(1 for _ in RELAYS if retry_action(2, False) == 'retry')
    assert ponowienia == 0, ponowienia

    print(f"OK — 5/5: gate ponowien trzyma, {len(frames)} ramek zbiorczych na 78 urzadzen")


if __name__ == "__main__":
    main()
