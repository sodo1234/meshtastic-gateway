"""
Centralny config dla modularnego systemu LoRa SCADA.
Rośnie z każdym stepem. Edytuj raz na maszynie — test programy importują stąd.

Auto-detekcja roli:
  - jest gateway_v38.py  → GATEWAY
  - jest supervisor_v38.py → SUPERVISOR

Sekrety (mqtt.pass, ha_api.token): jeśli REPLACE_ME, bootstrap z v38 przy pierwszym imporcie.
"""
import os, re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _detect_role():
    # Priority 1: env var (LORA_ROLE)
    env = os.environ.get('LORA_ROLE', '').strip().lower()
    if env in ('gateway', 'supervisor'):
        return env
    # Priority 2: .role file in SCRIPT_DIR (one-line: gateway|supervisor)
    role_file = os.path.join(SCRIPT_DIR, '.role')
    if os.path.exists(role_file):
        try:
            r = open(role_file).read().strip().lower()
            if r in ('gateway', 'supervisor'):
                return r
        except Exception:
            pass
    # Priority 3: v38 file presence (legacy auto-detect)
    if os.path.exists(os.path.join(SCRIPT_DIR, 'gateway_v38.py')):
        return 'gateway'
    if os.path.exists(os.path.join(SCRIPT_DIR, 'supervisor_v38.py')):
        return 'supervisor'
    return 'gateway'


def _bootstrap_secret(key, pattern, files=('gateway_v38.py', 'supervisor_v38.py')):
    """Jednorazowo wyciąga sekret z v38 jeśli config ma REPLACE_ME."""
    for name in files:
        path = os.path.join(SCRIPT_DIR, name)
        if not os.path.exists(path):
            continue
        src = open(path, encoding='utf-8').read()
        m = re.search(pattern, src)
        if m:
            return m.group(1)
    return key


ROLE = _detect_role()

# ═══════════════════════════════════════════════════════════
# STEP 1: Transport
# ═══════════════════════════════════════════════════════════

GATEWAY_CONFIG = {
    "id": "G1",
    "mesh_port": "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0",
    "mesh_reconnect": {"enabled": True, "interval": 10, "max_backoff": 120},
    "mqtt": {
        "host": "172.17.0.1",
        "port": 1883,
        "user": "mqtt",
        "pass": "REPLACE_ME",
    },
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
    "mqtt": {
        "host": "localhost",
        "port": 1883,
        "user": "mqtt",
        "pass": "REPLACE_ME",
    },
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    "gateways": ["G1"],
}

# ═══════════════════════════════════════════════════════════
# STEP 2: Protocol (HB, Discovery, HA Entities)
# ═══════════════════════════════════════════════════════════

GATEWAY_CONFIG.update({
    "heartbeat_interval": 900,                # 15 min — minimalizacja ruchu LoRa
    "monitored": [],                          # puste = wszystkie
    "priority_devices": [],
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

GATEWAY_CONFIG.update({
    "data": {
        "mon_interval": 30,                   # P3 monitored flush (s)
        "pri_interval": 10,                   # P1 priority flush (s)
        "max_payload": 120,                   # ≤150B operational; batch frame split
        "thresholds": {                       # delta gating (v10): below → no LoRa
            "temperature": 0.5,               # °C
            "humidity": 2.0,                  # %
        },
        # #8 periodic report + #7 gw-side liveness (bramka = autorytet dostępności):
        "report_interval": 900,               # co tyle s wyślij `b` dla KAŻDEGO monitored
                                              #   (heartbeat danych 15min, nie tylko delta).
                                              #   Na live-test obniż np. do 60.
        "offline_after": 1920,                # cisza z2m > tyle s → available=0 (martwy czujnik).
                                              #   Domyślnie 2×report+grace (~32min). Live-test: ~150.
    },
})

# ═══════════════════════════════════════════════════════════
# Aktywny config na tej maszynie
# ═══════════════════════════════════════════════════════════

if ROLE == 'gateway':
    CONFIG = GATEWAY_CONFIG
else:
    CONFIG = SUPERVISOR_CONFIG

# Bootstrap sekretów z v38
if CONFIG['mqtt']['pass'] == 'REPLACE_ME':
    CONFIG['mqtt']['pass'] = _bootstrap_secret(
        'REPLACE_ME',
        r'"mqtt"\s*:\s*\{[^}]*"pass"\s*:\s*"([^"]+)"')
