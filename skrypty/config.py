"""
Centralny config modularnego systemu LoRa SCADA (refaktor).
JEDYNE źródło konfiguracji — moduły i test_stepN_*.py robią `from config import CONFIG`.
Edytowalny zdalnie przez Launcher (/api/config → ~/meshtastic/config.py).

Rola: plik `.role` (gateway|supervisor) albo env LORA_ROLE.

SEKRETY: pola MQTT_PASS / HA_TOKEN poniżej — ustawiane PER-MASZYNA (przez Launcher).
W repo zostają puste; prawdziwych wartości NIE commitujemy. (v38 NIE jest już używane.)
"""
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _detect_role():
    env = os.environ.get('LORA_ROLE', '').strip().lower()
    if env in ('gateway', 'supervisor'):
        return env
    role_file = os.path.join(SCRIPT_DIR, '.role')
    if os.path.exists(role_file):
        try:
            r = open(role_file).read().strip().lower()
            if r in ('gateway', 'supervisor'):
                return r
        except Exception:
            pass
    return 'gateway'                              # domyślnie; ustaw `.role` lub LORA_ROLE


ROLE = _detect_role()

# ═══════════════════════════════════════════════════════════
# SEKRETY — INLINE (JEDEN plik konfiguracyjny: loginy, hasła, tokeny, reszta niżej)
# ═══════════════════════════════════════════════════════════
# Wersja ROBOCZA na tej maszynie i na hostach trzyma PRAWDZIWE wartości tutaj.
# W git trafia tylko PLACEHOLDER — pilnuje tego git clean filter (`_cfg_scrub.py` +
# .gitattributes: `config.py filter=scrubcfg`), który przy `git add` zeruje te 2 linie.
# Deploy (`deploy_config.sh`) pushuje wersję roboczą (z sekretami) na bramkę i supervisora.
# Zmiana wartości = JEDNO miejsce: ten plik (albo launcher → /api/config). env nadpisuje.
MQTT_PASS    = ""   # hasło brokera MQTT (oba hosty)        — PLACEHOLDER w git
HA_TOKEN_GW  = ""   # token HA bramki G1 — PLACEHOLDER w git
HA_TOKEN_SUP = ""   # token HA supervisora G0 — PLACEHOLDER w git
HA_URL       = "http://localhost:8123"

MQTT_PASS    = os.environ.get("MQTT_PASS", MQTT_PASS)
HA_TOKEN_GW  = os.environ.get("HA_TOKEN_GW", HA_TOKEN_GW)
HA_TOKEN_SUP = os.environ.get("HA_TOKEN_SUP", HA_TOKEN_SUP)
HA_URL       = os.environ.get("HA_URL", HA_URL)
# token HA wg roli TEJ maszyny — GATEWAY/SUPERVISOR_CONFIG.ha_api używają HA_TOKEN
HA_TOKEN = HA_TOKEN_GW if ROLE == 'gateway' else HA_TOKEN_SUP

# ═══════════════════════════════════════════════════════════
# STEP 1: Transport
# ═══════════════════════════════════════════════════════════

GATEWAY_CONFIG = {
    "id": "G1",
    "mesh_port": "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0",
    "mesh_reconnect": {"enabled": True, "interval": 10, "max_backoff": 120},
    "mqtt": {"host": "172.17.0.1", "port": 1883, "user": "mqtt", "pass": MQTT_PASS},
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
}

SUPERVISOR_CONFIG = {
    "id": "G0",
    "mesh_ports": [
        # by-id = stabilna ścieżka; /dev/ttyUSBx renumeruje się po reconnect USB
        {"port": "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0",
         "enabled": True, "label": "ANT-1", "gateways": ["G1"]},
    ],
    "mesh_reconnect": {"enabled": True, "interval": 15, "max_backoff": 120},
    "mqtt": {"host": "localhost", "port": 1883, "user": "mqtt", "pass": MQTT_PASS},
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    "gateways": ["G1"],
}

# ═══════════════════════════════════════════════════════════
# STEP 2: Protocol (HB, Discovery, HA Entities)
# ═══════════════════════════════════════════════════════════

GATEWAY_CONFIG.update({
    "heartbeat_interval": 900,                # 15 min — minimalizacja ruchu LoRa
    # MONITORED = batch co 30s (delta). PRIORITY = batch co 10s (security, szybki).
    # Urządzenia SPOZA obu list: nadal mają anomalie (offline/battery/stagnation = ALL devices),
    # ale danych NIE batchują (oszczędność LoRa). Przykład wg v38 (Temp 3/4 = martwe encje, pominięte).
    "monitored": ["Test 1", "Test 2", "Temp 1", "Temp 2"],   # sensory+switche; v38: +Temp 3/4
    "priority_devices": ["Leak 1", "Door 1"],                # security → P1 10s
    # Inne przykłady do rozważenia:
    #   monitored=[] → wszystkie (max ruch LoRa); priority=["Smoke 1","Leak 1"] (czujniki krytyczne)
    #   produkcja: monitored=lista ważnych temp/switch, priority=tylko pożar/wyciek/dostęp
    "virtual_io": [
        {"id": "vs_test", "type": "switch", "name": "Test Switch", "default": 0},
        {"id": "vb_test", "type": "button", "name": "Test Button"},
    ],
    "discovery": {
        "max_payload": 90,                    # mniejsze pakiety = krótszy airtime = mniej strat RF
        "tx_delay": 4.0,                      # sekundy między pakietami disc
    },
    "log_file": "/tmp/test_transport.log",
})

SUPERVISOR_CONFIG.update({
    "heartbeat_timeout": 600,
    "ha_prefix": "homeassistant",
    "state_prefix": "lora",
    "log_file": "/tmp/test_transport.log",
})

# ═══════════════════════════════════════════════════════════
# STEP 3: Data (batch monitored/priority + delta + refresh)
# ═══════════════════════════════════════════════════════════
# UWAGA: offline NIE jest definiowany tu na nowo — wynika ze zdefiniowanych timeoutów
# offline (params T1/T2/T3 per typ) używanych przez OfflineMonitor (supervisor).
# `offline_after` bramki = pochodna (max z T1/T2/T3) liczona w harnessie, nie stała w config.

GATEWAY_CONFIG.update({
    "data": {
        "mon_interval": 30,                   # P3 monitored flush (s)
        "pri_interval": 10,                   # P1 priority flush (s)
        "max_payload": 120,                   # ≤150B operational; batch frame split
        "thresholds": {                       # delta gating (v10): below → no LoRa
            "temperature": 0.5,               # °C
            "humidity": 2.0,                  # %
        },
        "report_interval": 900,               # co tyle s `b` dla KAŻDEGO monitored (heartbeat danych)
    },
})

# ═══════════════════════════════════════════════════════════
# STEP 4: Kalendarz (krok 16) + Slotowanie / anti-collision (krok 7)
# ═══════════════════════════════════════════════════════════

_MODE_NAMES = {0: "BRAK PRODUKCJI", 1: "PRODUKCJA", 2: "PRZERWA", 3: "SERWIS"}

GATEWAY_CONFIG.update({
    "mode_names": _MODE_NAMES,
    "ha_api": {
        "url": HA_URL,
        "token": HA_TOKEN,
        # zapis ICS bezpośrednio do storage HA = bulletproof (reszta przez reload)
        "ics_path": "/var/lib/homeassistant/homeassistant/.storage/local_calendar.g1.ics",
    },
    "calendar": {
        "chunk_size": 60,         # ≤90B = niezawodny próg tego łącza LoRa (lekcja RF); transfer chunkuje b64
        "chunk_delay": 6.0,       # s między chunkami (honor cooldown LoRa)
        "window_days": 14,        # okno harmonogramu kompaktowanego do LoRa
        "gw_calendar_id": "",     # REVERSE: encja kalendarza lokalnego bramki ('' = pchaj effective)
        # ANTY-SPAM (2026-06-16): timeout ACK MUSI pokryć realny round-trip LoRa
        # (sup TX cooldown 3s + airtime + gw TX cooldown 3s + airtime + kolejka), inaczej
        # retransmituje zanim ACK dotrze i zapycha łącze. Round-trip czysty ~6-10s, pod
        # obciążeniem/po reconnect ~30-60s → 30s + mało retry.
        "chunk_ack_timeout": 30.0,  # s czekania na cal_cack przed retransmisją chunku
        "chunk_retries": 5,         # maks. retransmisji chunku (max 3 wysyłki/chunk)
        "end_retries": 3,           # maks. retransmisji cal_end (finalny ACK)
    },
    "slotting": {
        "enabled": True,
        "slot_seconds": 20,       # G1:0-19 / G2:20-39 / G3:40-59 w cyklu 60s
        "gateways": ["G1", "G2", "G3"],   # kolejność = indeks slotu tej bramki
    },
})

SUPERVISOR_CONFIG.update({
    "mode_names": _MODE_NAMES,
    "ha_api": {
        "url": HA_URL,
        "token": HA_TOKEN,
        # REVERSE mirror (gw_push → calendar.lora_<gw>): zapis ICS do storage HA supervisora
        # (jak bramka — REST /api/calendars POST jest read-only=405). {gw}=lower nazwa bramki.
        # Wymaga zapisu dla usera `td` w .storage (chmod 777 jak na bramce).
        "mirror_ics_path": "/var/lib/homeassistant/homeassistant/.storage/local_calendar.lora_{gw}.ics",
    },
    "calendar": {
        "chunk_size": 60,         # ≤90B = niezawodny próg tego łącza LoRa (lekcja RF)
        "chunk_delay": 6.0,
        "window_days": 14,
        "calendar_id": "calendar.lora_global",   # encja HA czytana jako GLOBAL
        "sync_interval": 0,                       # s; 0 = re-read kalendarza tylko na start/przycisk
        "enabled_gateways": ["G1"],               # do których bramek push harmonogramu
        # ANTY-SPAM (2026-06-16) — patrz komentarz w GATEWAY_CONFIG.calendar.
        "chunk_ack_timeout": 30.0,  # s czekania na cal_cack przed retransmisją (round-trip LoRa)
        "chunk_retries": 5,         # maks. retransmisji chunku
        "end_retries": 3,           # maks. retransmisji cal_end
    },
    "slotting": {
        "enabled": True,
        "slot_seconds": 20,
        "safe_window_seconds": 10,                # supervisor TX do bramki tylko ≤10s po RX od niej
    },
})

# ═══════════════════════════════════════════════════════════
# STEP 5: Anomalie (offline+battery+stagnation, ALL devices) + Tryb bramki day/night
# ═══════════════════════════════════════════════════════════

GATEWAY_CONFIG.update({
    "anomaly": {
        "battery_low": 25,        # < % → low_battery (lb)
        "battery_critical": 15,   # < % → critical_battery (cb)
        "battery_check": 300,     # s — częstotliwość sprawdzania baterii
        "offline_check": 30,      # s — gateway-side offline anomaly
        "offline_grace": 120,     # s — grace zanim offline
        "stagnation_check": 300,  # s — stagnacja (P1/P2 godz z param_sync)
        "flush_interval": 30,     # s — AnomalyBatcher flush `ab` P2
        "max_payload": 150,       # B — split ramki ab
    },
    "gateway_mode": {
        # tryb pracy bramki: 'all-time' (zawsze), 'day' (aktywna w dzień), 'night' (aktywna w nocy).
        # Bramka nieaktywna NIE zgłasza offline (urządzenia świadomie bez zasilania).
        "operating_mode": "all-time",
        "day_start": "06:00",     # okno dnia (gdy brak lat/lon)
        "day_end": "20:00",
        "lat": None,              # opcjonalnie: zmierzch/świt solarnie (np. 50.3)
        "lon": None,              # (np. 18.7) — wtedy day_start/end ignorowane
    },
})

SUPERVISOR_CONFIG.update({
    "anomaly": {
        "persist_path": "/tmp/lora_anomaly_ids.json",
        "dump_interval": 1800,    # s — cykliczny dump_anom (reconcyliacja, 30 min)
        "prune_after": 90,        # s po dump — kasuj anomalie niepotwierdzone przez bramkę
        "popup": True,            # browser_mod popup przy nowej anomalii critical
    },
})

# ═══════════════════════════════════════════════════════════
# Aktywny config na tej maszynie
# ═══════════════════════════════════════════════════════════

CONFIG = GATEWAY_CONFIG if ROLE == 'gateway' else SUPERVISOR_CONFIG
