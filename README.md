# Meshtastic Gateway — LoRa Zigbee SCADA

Rozproszony BMS (Building Management System) dla hal produkcyjnych. Setki czujników Zigbee w wielu lokalizacjach → bramki LoRa (Meshtastic) → centralny supervisor → dashboard Home Assistant.

**Status:** aktywny rozwój, v38 baseline. Pełna specyfikacja w [CLAUDE.md](CLAUDE.md).

## Architektura

```
SUPERVISOR (G0) — Xeon, HA Supervised, 3 anteny LoRa (1/bramka)
         LoRa 868MHz | 150B max, 3.0s cooldown, EU 10% duty cycle
    G1 (100+dev)     G2 (100+dev)     G3 (100+dev)
    Xeon+Z2M+HA      Xeon+Z2M+HA      Xeon+Z2M+HA
```

- **Zigbee**: Aqara/SONOFF/Tuya + Z2M (ConBee II)
- **LoRa**: Heltec V3 (ESP32-S3+SX1262), 868MHz
- **Software**: Python 3, paho-mqtt, meshtastic, HA Supervised na Debian 12

## Layout repo

| Folder | Zawartość |
|---|---|
| `skrypty/` | Skrypty historyczne (`gateway_v10/v23/v38.py`, `supervisor_v10/v23/v38.py`) |
| `homeassistant/` | HA config (`configuration.yaml`, `dashboard.yaml`, `automations.yaml`, `scripts.yaml`) |
| `launcher/` | Web UI (Flask) do remote start/stop/tail skryptów na bramkach przez SSH/Tailscale |
| `CLAUDE.md` | Pełna spec systemu + 17-stepowy plan refactoringu |

## Quick start

### 1. Konfiguracja sekretów

```bash
cp .env.example .env
# edytuj .env — wpisz MQTT password, HA tokeny per gateway
```

Skrypty obecnie czytają sekrety z hardcoded CONFIG na górze pliku — przed pierwszym uruchomieniem wpisz właściwe wartości tam (refactor na env-loading planowany).

### 2. Środowisko Python na bramce/supervisorze

```bash
cd ~/meshtastic
python3 -m venv venv
source venv/bin/activate
pip install meshtastic paho-mqtt
```

### 3. Uruchomienie

**Ręcznie:**
```bash
source venv/bin/activate
python gateway_v38.py    # na bramce
python supervisor_v38.py # na supervisorze
```

**Przez launcher (zalecane, web UI):**
```bash
cd launcher
python app.py
# otwórz http://127.0.0.1:8765
```

Launcher wykrywa hosty z Tailscale i LAN, pozwala wybrać skrypt, wykrywa Heltec po `/dev/serial/by-id/`, startuje w `tmux`, streamuje logi live (SSE), zapisuje do `logs/` na PC.

## Protokół (Short JSON)

| Type | Direction | Format |
|---|---|---|
| `hb` | gw→sup | `{t:hb,g:G1,up:N,dev:N,mon:N,pri:N,air:F,z2m:N,hash:H}` |
| `b` (prio) | gw→sup | `{t:b,g:G1,ts:T,d:[[sid,{a:1,w:0,b:100}],...]}` |
| `b` (mon) | gw→sup | `{t:b,g:G1,ts:T,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}` |
| `ab` | gw→sup | `{t:ab,g:G1,ts:T,d:[[sid,"th",32.6],...]}` |
| `cfg` | sup→gw | `{t:cfg,g:G1,to:{sw:1800,sn:86400}}` |
| `cmd` | sup→gw | `{t:cmd,g:G1,d:"Test 1",c:"state",v:"ON"}` |

Pełna tabela w [CLAUDE.md](CLAUDE.md#protokół--short-json).

## Refactoring roadmap (17 kroków)

Status: v38 = monolit `Gateway`/`Supervisor` (~2500 lin każda). Plan rozbicia na ~10 modułów (Transport, Dispatcher, AnomalyEngine, Batcher, Scheduler, AntennaManager, HAIntegration). Szczegóły w CLAUDE.md.

## License

Private — proprietary domain logic.
