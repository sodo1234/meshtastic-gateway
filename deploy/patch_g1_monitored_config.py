#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wpisz `monitored` = 78 przekaźników RA1-15/RB1-11 do config.py NA BRAMCE G1.

Chirurgicznie, bo `config.py` per maszyna trzyma sekrety (token HA, hasło MQTT), port anteny
i tuning — deploy kodu celowo go NIE dotyka. Podmieniamy wyłącznie blok `monitored` i kłamiący
po zmianie komentarz nad nim.

Idempotentne: drugi przebieg wykrywa gotową listę i nic nie rusza. Backup obok pliku.

Kontekst decyzji (user 2026-08-13): monitored = WYŁĄCZNIE przekaźniki. Czujniki ruchu (occupancy,
~82 szt.) zostają POZA listą — mają być widoczne na dashboardzie bramki, ale nie generować ruchu
LoRa. Przekaźniki to `switch`, więc wg polityki raportowania jadą ZDARZENIOWO (zmiana stanu),
a nie cyklicznym sweepem — koszt pasma jest proporcjonalny do realnych przełączeń.

Użycie NA G1:  venv/bin/python /tmp/patch_g1_monitored_config.py [--dry-run]
"""
import re
import shutil
import sys
import time

CFG = '/home/td/meshtastic/config.py'
RA_MAX, RB_MAX, KANALY = 15, 11, (1, 2, 3)

NOWY_KOMENTARZ = '''    # Produkcja: same przekaźniki (switch) + ruch (occupancy) — brak temp/hum/leak/door na G1.
    # monitored = 78 STEROWANYCH przekaźników RA1-15_{1,2,3} + RB1-11_{1,2,3} (user 2026-08-13).
    # Czujniki ruchu i pozostałe wyjścia RA*_S*/RA*_R* są POZA listą: widoczne na dashboardzie
    # bramki, ale nie nadają po LoRa (przy ~82 czujnikach sweep zjadłby całe pasmo).
    # Przekaźnik to `switch`, więc raportuje ZDARZENIOWO (zmiana on/off), nie cyklicznie —
    # koszt pasma rośnie z liczbą realnych przełączeń, nie z liczbą urządzeń.'''


def lista():
    return [f'{p}{n}_{k}' for p, mx in (('RA', RA_MAX), ('RB', RB_MAX))
            for n in range(1, mx + 1) for k in KANALY]


def blok(items):
    wiersze = ['        ' + ', '.join(f'"{x}"' for x in items[i:i + 6]) + ','
               for i in range(0, len(items), 6)]
    return '    "monitored": [\n' + '\n'.join(wiersze) + '\n    ],'


def main():
    dry = '--dry-run' in sys.argv
    src = open(CFG, encoding='utf-8').read()
    items = lista()
    assert len(items) == 78, len(items)

    if '"RA1_1"' in src and '"RB11_3"' in src:
        print('bez zmian — lista 78 przekaźników już jest w config.py')
        return 0

    # Komentarz opisujący PUSTĄ listę staje się po zmianie nieprawdziwy → idzie razem z blokiem.
    stary = re.compile(
        r'[ \t]*#[^\n]*Produkcja: same przekaźniki.*?\n(?:[ \t]*#[^\n]*\n)*'
        r'[ \t]*"monitored":\s*\[\s*\],', re.S)
    nowy = NOWY_KOMENTARZ + '\n' + blok(items)

    out, n = stary.subn(nowy, src, count=1)
    if not n:                                    # awaryjnie: sama linia, bez komentarza
        out, n = re.subn(r'[ \t]*"monitored":\s*\[\s*\],', nowy, src, count=1)
    if not n:
        print('❌ nie znalazłem bloku "monitored": [] — NIC nie zmieniam'); return 2

    if dry:
        print(out[out.index('# Produkcja') - 200:out.index('"RB11_3"') + 60][:2000]); return 0

    bak = f'{CFG}.bak_monitored78_{time.strftime("%Y%m%d_%H%M%S")}'
    shutil.copy2(CFG, bak)
    open(CFG, 'w', encoding='utf-8').write(out)
    print('backup →', bak)

    import ast
    ast.parse(out)                               # składnia zanim ktokolwiek to zaimportuje
    print('AST OK')
    return 0


if __name__ == '__main__':
    sys.exit(main())
