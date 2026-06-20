#!/usr/bin/env python3
"""git clean filter dla config.py — zeruje sekrety zanim trafią do gita.

Drzewo robocze trzyma PRAWDZIWE wartości; `git add config.py` przepuszcza plik przez
ten filtr → w repo ląduje placeholder (`MQTT_PASS = ""`, `HA_TOKEN = ""`). Dzięki temu
config.py jest JEDNYM plikiem z całością (loginy/hasła/tokeny/timeouty/parametry/urządzenia),
a mimo to sekrety NIE wyciekają do gita.

Rejestracja (raz na klon):
    git config filter.scrubcfg.clean "python skrypty/_cfg_scrub.py"
    git config filter.scrubcfg.smudge cat
.gitattributes:  skrypty/config.py filter=scrubcfg

Zeruje TYLKO inline-przypisania (nie linie os.environ.get). stdin → stdout.
"""
import re
import sys

src = sys.stdin.read()
# Każda zmienna z PASS lub TOKEN w nazwie przypisana do literału "..." → "".
# Łapie: MQTT_PASS, HA_TOKEN, HA_TOKEN_GW, HA_TOKEN_SUP, ...  (przyszłe sekrety automatycznie).
# Linia `... = os.environ.get(...)` NIE pasuje (brak "..." tuż po =) → zostaje.
src = re.sub(r'(?m)^([A-Z_]*(?:PASS|TOKEN)[A-Z_]*\s*=\s*)"[^"]*"', r'\1""', src)
sys.stdout.write(src)
