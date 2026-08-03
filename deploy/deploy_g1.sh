#!/usr/bin/env bash
# Deploy KODU na bramkę G1 (carport) jednym strzałem — do odpalenia w oknie dostępności.
#
# PO CO: G1 to ~93% ruchu LoRa całego systemu i od 2026-07-29 nie dostała ŻADNEJ z optymalizacji
# ani fixów (host miga w sieci: Tailscale widzi go co jakiś czas na chwilę). Ręczne kopiowanie
# plik-po-pliku nie mieści się w takim oknie, więc wszystko idzie jedną komendą.
#
# CZEGO NIE RUSZA (świadomie):
#   • config.py — sekrety (token HA, hasło MQTT) i lista `monitored` są PER-MASZYNA. Nadpisanie
#     configu z repo skasowałoby je i wywróciło bramkę. Parametry produkcyjne ustawia się
#     dashboardem (P1/P2/T1-T4/progi), nie plikiem.
#   • /tmp/lora_* — stany, persist anomalii, mapa sid.
#
# Użycie:  bash deploy/deploy_g1.sh [host]        (domyślnie 100.104.38.14)
set -u
HOST="${1:-100.104.38.14}"
USER_SSH="td"
SRC="$(cd "$(dirname "$0")/../skrypty" && pwd)"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8)

FILES=(
  test_step5_anomaly.py
  modules/data/z2m_reader.py modules/data/__init__.py modules/data/gateway_data.py
  modules/data/supervisor_data.py modules/data/batcher.py
  modules/anomaly/alarm.py modules/anomaly/__init__.py modules/anomaly/batcher.py
  modules/anomaly/store.py
  modules/protocol/heartbeat.py
  modules/transport/lora_transport.py modules/transport/tx_queue.py
  modules/calendar/manager.py modules/calendar/ha_source.py
)

echo "== sprawdzam dostępność $HOST"
if ! ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "echo ok" >/dev/null 2>&1; then
  echo "❌ $HOST nieosiągalny — okno zamknięte, spróbuj ponownie"; exit 1
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
echo "== backup na bramce (suffix .bak_${STAMP}_deploy)"
for f in "${FILES[@]}"; do
  ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "[ -f ~/meshtastic/$f ] && cp ~/meshtastic/$f ~/meshtastic/$f.bak_${STAMP}_deploy" 2>/dev/null
done

echo "== kopiuję ${#FILES[@]} plików"
for f in "${FILES[@]}"; do
  scp -q "${SSH_OPTS[@]}" "$SRC/$f" "$USER_SSH@$HOST:~/meshtastic/$f" || { echo "❌ scp $f"; exit 2; }
  printf '.'
done
echo

echo "== walidacja składni NA BRAMCE (przed restartem, nie po)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && python3 - <<'EOF'
import ast, sys
files = '''${FILES[*]}'''.split()
for f in files:
    try:
        ast.parse(open(f, encoding='utf-8').read(), f)
    except Exception as e:
        print('AST FAIL', f, e); sys.exit(1)
print('AST OK', len(files), 'plikow')
EOF" || { echo "❌ walidacja nie przeszła — NIE restartuję"; exit 3; }

echo "== restart procesu bramki (tmux gw)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && tmux kill-session -t gw 2>/dev/null; sleep 3; \
  tmux new -d -s gw 'venv/bin/python -u test_step5_anomaly.py 2>>/tmp/test_transport.stderr.log'; sleep 12; \
  echo '-- instancje:'; ps -eo pid,cmd | grep '[v]env/bin/python -u test_step5' | wc -l; \
  echo '-- start:'; grep -E 'GATEWAY .* running|ALARM' /tmp/test_transport.log | tail -2; \
  echo '-- stderr:'; tail -2 /tmp/test_transport.stderr.log"

echo "✅ deploy G1 zakończony. Sprawdź na supie: HB z polem 'dh' + spadek ruchu 'b'."
