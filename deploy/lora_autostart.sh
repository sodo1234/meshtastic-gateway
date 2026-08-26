#!/usr/bin/env bash
# Autostart po zalogowaniu do sesji GNOME: WIDOCZNA konsola z logami harnessu + przeglądarka z HA.
#
# PO CO: test zimnego startu (2026-08-05) pokazał, że po zaniku zasilania wstają same tylko
# HA i z2m — harness i antena wymagały ręcznej interwencji. Ten skrypt domyka lukę.
#
# DLACZEGO tmux, a nie samo `python` w oknie: Launcher (:8765) zarządza procesami przez sesje
# tmux o nazwach `gw`/`sup` (ROLE_META). Gdyby autostart uruchomił pythona poza tmuxem, Launcher
# nie miałby czym sterować, a `Start` zrobiłby DRUGĄ instancję walczącą o port anteny.
# Tu sesja nazywa się tak samo, więc okno pokazuje logi, a Launcher dalej robi stop/start.
#
# DLACZEGO czekanie na port: przy zimnym starcie CP2102 bywa jeszcze nieobecny/nieodpowiadający.
# Harness startujący za wcześnie dostaje `Timed out waiting for connection completion`, ale zdąży
# otworzyć port i GO NIE ZWALNIA — każdy kolejny reconnect blokuje się wtedy o własny uchwyt
# (Errno 11) i bramka jest głucha aż do ręcznego restartu. Zaobserwowane live 2026-08-05.
#
# INSTALACJA (jako td, na bramce i na supervisorze):
#   cp deploy/lora_autostart.sh ~/meshtastic/deploy/ && chmod +x ~/meshtastic/deploy/lora_autostart.sh
#   cp deploy/lora-autostart.desktop ~/.config/autostart/
# Autologin do GNOME musi być włączony, inaczej po boocie nie ma sesji graficznej i nic nie ruszy.
set -u

DIR="$HOME/meshtastic"
SCRIPT="test_step5_anomaly.py"
STDERR="/tmp/test_transport.stderr.log"
HA_URL="http://localhost:8123"

ROLE="$(cat "$DIR/.role" 2>/dev/null || echo gateway)"
case "$ROLE" in
  supervisor) SESSION="sup" ;;
  *)          SESSION="gw"  ;;
esac

log() { echo "[autostart $(date '+%H:%M:%S')] $*"; }

# ── 1. Poczekaj aż antena CP2102 się wyliczy (do ~2 min) ─────────────────────
for _ in $(seq 1 60); do
  if ls /dev/serial/by-id/*CP2102* >/dev/null 2>&1; then
    log "antena widoczna: $(ls /dev/serial/by-id/*CP2102*)"
    break
  fi
  sleep 2
done
sleep 5     # radio potrzebuje chwili po enumeracji, zanim przyjmie połączenie

# ── 2. Poczekaj na broker MQTT i HA ──────────────────────────────────────────
# Harness łączy się z MQTT NATYCHMIAST przy starcie i przy odmowie połączenia PADA
# (ConnectionRefusedError, brak retry) — a po boocie kontenery HA/mosquitto wstają
# wolniej niż sesja graficzna. Bez tego czekania autostart kończył się cichym zgonem:
# tmux ginął razem z procesem, więc okno `tmux attach` zamykało się natychmiast i user
# widział tylko przeglądarkę. Zaobserwowane live po reboocie 2026-08-05.
# Adres brokera RÓŻNI SIĘ per maszyna (sup: localhost, bramka: 172.17.0.1) → z config.py.
log "czekam na broker MQTT i HA..."
if "$DIR/venv/bin/python" - "$DIR" <<'PY'
import socket, sys, time, urllib.request
sys.path.insert(0, sys.argv[1])
import config
m = config.CONFIG['mqtt']
host, port = m.get('host', 'localhost'), int(m.get('port', 1883))

def mqtt_up():
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False

def ha_up():
    try:
        urllib.request.urlopen('http://localhost:8123', timeout=3)
        return True
    except urllib.error.HTTPError:
        return True          # odpowiada = żyje (401/403 też się liczy)
    except Exception:
        return False

deadline = time.time() + 300
while time.time() < deadline:
    if mqtt_up() and ha_up():
        print(f'deps OK: mqtt {host}:{port} + HA')
        sys.exit(0)
    time.sleep(5)
print(f'TIMEOUT 300s: mqtt {host}:{port} lub HA nie wstaly', file=sys.stderr)
sys.exit(1)
PY
then
  log "zależności gotowe"
else
  log "UWAGA: MQTT/HA nie wstały w 300 s — startuję mimo to (zobaczysz błąd w oknie)"
fi

# ── 3. Harness w tmux (nazwa zgodna z Launcherem) ────────────────────────────
if tmux has-session -t "$SESSION" 2>/dev/null; then
  log "sesja '$SESSION' już istnieje — nie duplikuję, tylko pokazuję"
else
  log "startuję harness ($ROLE) w sesji '$SESSION'"
  cd "$DIR" && tmux new -d -s "$SESSION" "venv/bin/python -u $SCRIPT 2>>$STDERR"
  sleep 6
fi

# ── 3b. Dziennik ruchu LoRa (TYLKO supervisor) ───────────────────────────────
# Supervisor ma internet non stop i widzi obie strony łącza: własne TX oraz wszystko, co
# odbiera po LoRa od bramek. Bramka carportu bywa odcięta od sieci, więc to JEDYNE miejsce,
# gdzie da się trzymać trwały, porównywalny zapis ruchu. Osobne pliki per bramka
# (`logs/traffic_sup_*.tsv`, `logs/traffic_g1_*.tsv`) → różnica pokazuje realne straty RF.
if [ "$ROLE" = "supervisor" ]; then
  if pgrep -f "[l]ora_traffic_log.py --collect" >/dev/null; then
    log "dziennik ruchu już działa"
  else
    log "startuję dziennik ruchu LoRa"
    cd "$DIR" && setsid nohup "$DIR/venv/bin/python" tools/lora_traffic_log.py --collect \
      >/tmp/traffic_collector.log 2>&1 </dev/null &
  fi
fi

# ── 4. WIDOCZNE okno: logi albo POWÓD śmierci (nigdy pusty ekran) ────────────
if tmux has-session -t "$SESSION" 2>/dev/null; then
  gnome-terminal --title="LoRa SCADA — ${ROLE} (${SESSION})" -- tmux attach -t "$SESSION" &
else
  # Bez tej gałęzi okno zamyka się w mgnieniu oka i awaria jest NIEWIDOCZNA.
  gnome-terminal --title="LoRa SCADA — ${ROLE}: HARNESS PADŁ" -- \
    bash -c "echo '❌ Harness nie wstał. Ostatnie błędy:'; echo; tail -25 '$STDERR'; echo; echo 'Start ręczny: cd ~/meshtastic && venv/bin/python -u $SCRIPT'; exec bash" &
fi

# ── 5. Przeglądarka z HA, dopiero gdy HA realnie odpowiada ───────────────────
# Bez tego firefox otwiera się na "nie można połączyć" i user musi odświeżać ręcznie.
(
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null -m 3 "$HA_URL" && break
    sleep 3
  done
  exec firefox "$HA_URL"
) &

wait
