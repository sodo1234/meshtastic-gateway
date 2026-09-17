#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Podmien nazwe wydarzenia harmonogramu w automatyzacjach Schedule ON/OFF na bramce G1.

PO CO (user 2026-09-17): automatyzacje wyzwalaja sie na wydarzeniu kalendarza o tytule
"PRODUKCJA", ale robia DOKLADNIE ODWROTNIE: start eventu (22:15) gasi swiatlo, koniec (05:45)
je zapala. Okno 22:15-05:45 to czas POZA produkcja, wiec tytul "PRODUKCJA" byl artefaktem
po testach. Kalendarze zostaly przemianowane na "BRAK PRODUKCJI" — warunek w automatyzacjach
MUSI pojsc za tym, inaczej zaden wyzwalacz nie przejdzie i swiatlo przestanie sie przelaczac.

Idempotentny: drugie uruchomienie nic nie zmienia.
Uzycie NA G1:  cd ~/meshtastic && venv/bin/python /tmp/rename_schedule_mode.py [--dry-run]
"""
import json
import sys
import urllib.request

sys.path.insert(0, '/home/td/meshtastic')
from config import CONFIG                                    # noqa: E402

STARA = 'PRODUKCJA'
NOWA = 'BRAK PRODUKCJI'
AUTOMATYZACJE = {'1774458295533': 'Schedule ON', '1774458640306': 'Schedule OFF'}
DRY = '--dry-run' in sys.argv


def main():
    ha = CONFIG['ha_api']
    H = {'Authorization': 'Bearer ' + ha['token'], 'Content-Type': 'application/json'}

    def get(p):
        return json.load(urllib.request.urlopen(
            urllib.request.Request(ha['url'] + p, headers=H), timeout=25))

    def post(p, body):
        r = urllib.request.Request(ha['url'] + p, data=json.dumps(body).encode(),
                                   headers=H, method='POST')
        return urllib.request.urlopen(r, timeout=25).read().decode()[:120]

    def podmien(o):
        """Rekurencyjnie: tylko PELNE porownanie do starej nazwy w szablonie warunku.
        Celowo NIE robimy globalnego str.replace po calym JSON-ie — 'PRODUKCJA' jest
        podciagiem 'BRAK PRODUKCJI', wiec drugi przebieg zepsulby juz poprawiony warunek."""
        if isinstance(o, dict):
            return {k: podmien(v) for k, v in o.items()}
        if isinstance(o, list):
            return [podmien(v) for v in o]
        if isinstance(o, str) and f'== "{STARA}"' in o and f'== "{NOWA}"' not in o:
            return o.replace(f'== "{STARA}"', f'== "{NOWA}"')
        return o

    rc = 0
    for aid, nazwa in AUTOMATYZACJE.items():
        try:
            cfg = get(f'/api/config/automation/config/{aid}')
        except Exception as e:
            print(f'   ❌ {nazwa} (id={aid}): nie moge odczytac — {e}'); rc = 1; continue
        nowy = podmien(cfg)
        if nowy == cfg:
            print(f'   • {nazwa}: juz na "{NOWA}" (albo brak warunku) — bez zmian')
            continue
        if DRY:
            print(f'   [dry] {nazwa}: warunek -> "{NOWA}"')
            continue
        print(f'   ✅ {nazwa}: warunek -> "{NOWA}" | {post(f"/api/config/automation/config/{aid}", nowy)}')

    # weryfikacja: po zapisie w konfiguracji nie moze zostac gole "PRODUKCJA"
    for aid, nazwa in AUTOMATYZACJE.items():
        try:
            s = json.dumps(get(f'/api/config/automation/config/{aid}'), ensure_ascii=False)
        except Exception:
            continue
        if f'== "{STARA}"' in s:
            print(f'   ❌ {nazwa}: WCIAZ warunek na "{STARA}"'); rc = 1
    return rc


if __name__ == '__main__':
    sys.exit(main())
