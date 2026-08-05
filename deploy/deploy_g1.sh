#!/usr/bin/env bash
# Deploy KODU na bramkę jednym strzałem — do odpalenia w oknie dostępności hosta.
#
# PO CO: G1 to ~93% ruchu LoRa całego systemu, a host bywa w sieci tylko chwilami
# (Tailscale widzi go co jakiś czas na moment). Ręczne kopiowanie plik-po-pliku nie mieści
# się w takim oknie, więc wszystko idzie jedną komendą i jednym round-tripem.
#
# DLACZEGO CAŁE DRZEWO, A NIE LISTA PLIKÓW (zmiana 2026-08-05): poprzednia wersja miała
# sztywną listę 15 plików i po dwóch dniach pracy była już nieaktualna — nowe zmiany
# siedziały w plikach spoza listy. Deploy połowy zmian jest GORSZY niż brak deployu:
# obie strony gadają wtedy różnymi wersjami protokołu, a objaw wychodzi dopiero na łączu.
# `modules/` + harness to całość kodu; jedyne, czego świadomie NIE dotykamy, to:
#   • config.py — sekrety (token HA, hasło MQTT), lista `monitored`, port anteny,
#     `params_preset` — wszystko PER-MASZYNA. Nadpisanie configu z repo wywraca bramkę.
#   • /tmp/lora_* — stany, persist anomalii, mapa sid, parametry ustawione z dashboardu.
#
# Użycie:  bash deploy/deploy_g1.sh [host]        (domyślnie 100.104.38.14 = G1 carport)
#          bash deploy/deploy_g1.sh 100.98.155.78 gw    (G2 testbed, sesja tmux `gw`)
set -u
HOST="${1:-100.104.38.14}"
SESSION="${2:-gw}"
USER_SSH="td"
SRC="$(cd "$(dirname "$0")/../skrypty" && pwd)"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8)

echo "== sprawdzam dostępność $HOST"
if ! ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "echo ok" >/dev/null 2>&1; then
  echo "❌ $HOST nieosiągalny — okno zamknięte, spróbuj ponownie"; exit 1
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
echo "== backup na maszynie (~/meshtastic_backup_${STAMP}.tgz)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" \
  "cd ~/meshtastic && tar czf ~/meshtastic_backup_${STAMP}.tgz test_step5_anomaly.py modules 2>/dev/null; \
   ls -la ~/meshtastic_backup_${STAMP}.tgz" || { echo "❌ backup nieudany — NIE wgrywam"; exit 2; }

echo "== wgrywam harness + całe modules/ (jeden strumień, bez pliku-po-pliku)"
tar czf - -C "$SRC" --exclude='__pycache__' --exclude='*.pyc' test_step5_anomaly.py modules \
  | ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && tar xzf -" \
  || { echo "❌ transfer nieudany"; exit 3; }

echo "== walidacja składni NA MASZYNIE (przed restartem, nie po)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && python3 - <<'EOF'
import ast, pathlib, sys
files = ['test_step5_anomaly.py'] + [str(p) for p in pathlib.Path('modules').rglob('*.py')]
bad = 0
for f in files:
    try:
        ast.parse(open(f, encoding='utf-8').read(), f)
    except Exception as e:
        print('AST FAIL', f, e); bad += 1
if bad:
    sys.exit(1)
print('AST OK', len(files), 'plikow')
EOF" || { echo "❌ walidacja nie przeszła — NIE restartuję (kod na maszynie do cofnięcia z backupu)"; exit 4; }

echo "== smoke testy NA MASZYNIE (szybkie, bez sprzętu)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && for m in data.smoke_test anomaly.smoke_test params.smoke_test; do \
  venv/bin/python -m modules.\$m 2>&1 | tail -1; done"

echo "== restart procesu (tmux $SESSION)"
ssh "${SSH_OPTS[@]}" "$USER_SSH@$HOST" "cd ~/meshtastic && tmux kill-session -t $SESSION 2>/dev/null; sleep 3; \
  tmux new -d -s $SESSION 'venv/bin/python -u test_step5_anomaly.py 2>>/tmp/test_transport.stderr.log'; sleep 12; \
  echo -n '-- instancji: '; ps -eo args | grep -c '^venv/bin/python -u test_step5_anomaly.py'; \
  echo '-- stderr:'; tail -2 /tmp/test_transport.stderr.log; \
  echo '-- log:'; tail -5 /tmp/test_transport.log"

echo "✅ deploy zakończony ($HOST). Sprawdź na supervisorze: HB z polem 'dh' + spadek ramek 'b'."
echo "   Cofnięcie: ssh $USER_SSH@$HOST 'cd ~/meshtastic && tar xzf ~/meshtastic_backup_${STAMP}.tgz'"
