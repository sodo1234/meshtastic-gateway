#!/usr/bin/env bash
# deploy_config.sh — JEDEN plik konfiguracyjny (skrypty/config.py) → fizyczna bramka + supervisor.
#
# config.py = całość zarządzania: loginy, hasła, tokeny, timeouty, parametry, listy urządzeń.
# Wersja ROBOCZA na tej maszynie trzyma PRAWDZIWE sekrety inline; ten sam plik (z sekretami)
# jedzie na oba hosty. Do GITa trafia tylko placeholder — pilnuje git clean filter
# (`skrypty/_cfg_scrub.py` + .gitattributes). Edycja = JEDNO miejsce: skrypty/config.py (albo launcher).
#
# Użycie:
#   ./deploy_config.sh            # tylko config.py na oba hosty
#   ./deploy_config.sh --modules  # config.py + modules/ + test_step4_calendar.py (przy nowych stepach)
set -euo pipefail

GW=td@100.98.155.78         # fizyczna bramka G1
SUP=td@100.79.111.24        # supervisor G0
DST='~/meshtastic'
SRC="$(cd "$(dirname "$0")/skrypty" && pwd)"
WITH_MODULES="${1:-}"

# Strażnik: nie wysyłaj configu z pustymi sekretami (inaczej restart zerwie MQTT/HA).
for V in MQTT_PASS HA_TOKEN_GW HA_TOKEN_SUP; do
  if ! grep -qE "^${V}\s*=\s*\"[^\"]+\"" "$SRC/config.py"; then
    echo "⛔ STOP: config.py ma pusty $V — uzupełnij sekrety przed deployem (filtr git je chroni)."; exit 1
  fi
done

for H in "$GW" "$SUP"; do
  echo "===== $H ====="
  scp -q "$SRC/config.py" "$H:$DST/config.py"
  echo "  config.py ✓ (z sekretami inline)"
  if [ "$WITH_MODULES" = "--modules" ]; then
    tar -C "$SRC" --exclude='__pycache__' -cf - modules test_step4_calendar.py | ssh "$H" "tar -C $DST -xf -"
    echo "  modules/ + test_step4_calendar.py ✓"
  fi
  ssh "$H" "cd $DST && python3 -c \"import config as c; print('  sekrety OK:', bool(c.CONFIG['mqtt']['pass']) and bool(c.CONFIG.get('ha_api',{}).get('token')))\""
done
echo "Gotowe. Restart przez launcher (jedna instancja — bez konfliktu anteny)."
