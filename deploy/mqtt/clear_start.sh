#!/usr/bin/env bash
# CLEAR START: skasuj WSZYSTKIE encje MQTT na maszynie i pozwól odbudować je od zera
# przez zigbee2mqtt (natywne discovery) + harness python (encje lora_*).
#
# PO CO: po długim rozwoju rejestr HA zbiera encje-duchy — po urządzeniach odparowanych
# z z2m, po starych schematach nazw, po zmianach w kodzie discovery. Retained config na
# brokerze potrafi je wskrzeszać, więc „usuń w UI" nie wystarcza. Ten skrypt sprowadza
# maszynę do stanu, w którym JEDYNYM źródłem encji jest żywe discovery.
#
# KOLEJNOŚĆ MA ZNACZENIE (stąd ten runner zamiast trzech osobnych wywołań):
#   1. STOP harnessu — inaczej w trakcie czyszczenia publikuje retained configi i zostawia
#      rejestr w stanie pół-skasowanym. To najczęstszy błąd przy ręcznym robieniu tego.
#   2. wipe ENCJI (platform=mqtt) — HA sam przycina urządzenia, którym zabrakło encji.
#   3. wipe OSIEROCONYCH urządzeń — puste skorupy, które zostały mimo pkt 2.
#   4. restart z2m — wymusza ponowne ogłoszenie WSZYSTKICH urządzeń Zigbee.
#   5. START harnessu — odtwarza encje lora_* (gateway/anomalie/parametry/kalendarz).
#
# Kroki 1 i 5 celowo NIE są tu zautomatyzowane przez tmux: proces ma startować z Launchera
# (:8765), żeby nie powstały dwie instancje walczące o antenę. Skrypt mówi, kiedy kliknąć.
#
# Użycie NA MASZYNIE:
#   bash deploy/mqtt/clear_start.sh            # podgląd (dry-run, nic nie kasuje)
#   bash deploy/mqtt/clear_start.sh --yes      # realne czyszczenie
set -u

DIR="$HOME/meshtastic"
APPLY=""
[ "${1:-}" = "--yes" ] && APPLY="--yes"

cd "$DIR" || { echo "❌ brak $DIR"; exit 1; }
ROLE="$(cat .role 2>/dev/null || echo gateway)"
case "$ROLE" in supervisor) SESSION="sup" ;; *) SESSION="gw" ;; esac

echo "═══ CLEAR START — $(hostname) / rola: $ROLE ═══"
[ -z "$APPLY" ] && echo "🔎 TRYB PODGLĄDU. Realne czyszczenie: --yes" && echo

# ── 1. harness musi milczeć ───────────────────────────────────────────────────
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "❌ Sesja tmux '$SESSION' żyje — zatrzymaj harness z Launchera (:8765 → Stop) i uruchom ponownie."
  echo "   Powód: w trakcie czyszczenia harness odtwarza retained configi (wyścig)."
  exit 2
fi
echo "✅ harness zatrzymany"

# ── 2. encje MQTT ─────────────────────────────────────────────────────────────
echo; echo "── encje platform=mqtt ──"
venv/bin/python deploy/mqtt/full_wipe_mqtt.py $APPLY || { echo "❌ wipe encji nieudany"; exit 3; }

# ── 3. osierocone urządzenia ──────────────────────────────────────────────────
if [ -n "$APPLY" ]; then
  echo; echo "── osierocone urządzenia MQTT ──"
  venv/bin/python deploy/mqtt/bulk_remove_mqtt_devices.py || echo "⚠️ czyszczenie urządzeń zgłosiło błąd (encje już usunięte — sprawdź ręcznie)"
fi

# ── 4. z2m ogłasza wszystko od nowa ───────────────────────────────────────────
if [ -n "$APPLY" ]; then
  echo; echo "── restart zigbee2mqtt (ponowne discovery) ──"
  if sudo -n docker ps --format '{{.Names}}' 2>/dev/null | grep -q zigbee2mqtt; then
    Z="$(sudo -n docker ps --format '{{.Names}}' | grep zigbee2mqtt | head -1)"
    sudo -n docker restart "$Z" >/dev/null && echo "✅ restart $Z" || echo "⚠️ nie udało się zrestartować $Z"
  else
    echo "ℹ️  brak kontenera zigbee2mqtt na tym hoście (supervisor go nie ma) — pomijam"
  fi
fi

echo
if [ -n "$APPLY" ]; then
  echo "═══ GOTOWE ═══"
  echo "TERAZ: wystartuj harness z Launchera (:8765 → Start, skrypt test_step5_anomaly.py)."
  echo "Odbudowa potrwa kilka minut: z2m ogłasza urządzenia, harness rejestruje encje lora_*,"
  echo "supervisor dociąga mapę (devmap) i stan (dsnap). Obserwuj konsolę z logami."
else
  echo "Podgląd zakończony — nic nie zmieniono. Uruchom z --yes, żeby wykonać."
fi
