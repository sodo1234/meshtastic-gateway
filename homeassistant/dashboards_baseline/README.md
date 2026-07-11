# Baseline dashboardów bramki G1 (HA na 100.98.155.78)

Zrzut LIVE z HA bramki (2026-06-29) — punkt odniesienia PRZED rozbudową funkcjonalności
dashboardu bramki ("carport gateway dashboard"). Źródło prawdy generujące ten dashboard:
**`../gen_gateway_dashboard.py`** (URL_PATH=`lora-gw`). Edytujemy generator i re-run, NIE ręcznie.

## Pliki
- `lora-gw.json` — pełna konfiguracja custom dashboardu „LoRa Gateway" (5 widoków).
- `lovelace-overview.json` — domyślny „Przegląd" (pusty, 1 widok / 0 kart).

## Dashboardy na bramce (live)
| url_path | title | uwaga |
|---|---|---|
| `lora-gw` | LoRa Gateway | **custom — ten rozbudowujemy** |
| `lovelace` | Przegląd | domyślny, pusty |
| `map` | Mapa | domyślny HA |

> Brak dashboardu literalnie nazwanego „carport gateway" — custom jest tylko `lora-gw`.
> „carport gateway dashboard" = robocza nazwa dla rozbudowy tego dashboardu (do potwierdzenia).

## Struktura `lora-gw` (5 widoków)
| # | path | title | karty |
|---|---|---|---|
| 0 | `lora-gw-stats` | Bramka | vertical-stack (39 encji: status, config/timeout buttons, params P1/P2, availability wszystkich devów, sup_link) |
| 1 | `lora-schedule` | Harmonogram | vertical-stack (sync/push schedule, hash kalendarza, tryb, harmonogram slotów) + custom:button-card |
| 2 | `lora-control` | Sterowanie | custom:button-card + horizontal-stack (test switche, LQI, availability) |
| 3 | `lora-alarms` | Alarmy | custom:button-card + horizontal-stack (door/leak contact+water_leak, battery, LQI) |
| 4 | `lora-sensors` | Pomiary | vertical-stack (temp_1/temp_2: temperature/humidity/battery/LQI/availability) |

Typy kart: `vertical-stack`, `horizontal-stack`, `custom:button-card`.

## Jak odczytać / przywrócić (narzędzie: `../ha_lovelace_ws.py`)
```bash
# token bramki z config.py (NIE hardcoduj):
TOKEN=$(cd ../../skrypty && LORA_ROLE=gateway python -c "from config import HA_TOKEN_GW;print(HA_TOKEN_GW)")
python ../ha_lovelace_ws.py list 100.98.155.78 "$TOKEN"                       # lista
python ../ha_lovelace_ws.py dump 100.98.155.78 "$TOKEN" lora-gw out.json      # zrzut
python ../ha_lovelace_ws.py save 100.98.155.78 "$TOKEN" lora-gw.json lora-gw  # przywrócenie z baseline
```
> Preferuj regenerację przez `gen_gateway_dashboard.py` (źródło prawdy) zamiast `save` z JSON.
