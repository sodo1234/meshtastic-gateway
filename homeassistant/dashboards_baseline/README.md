# Baseline dashboardów — LIVE JEST ŹRÓDŁEM PRAWDY (od 2026-07-30)

**Decyzja usera 2026-07-30: generatory bazują na tym, co jest teraz live — nie odwrotnie.**
Dashboardy były strojone ręcznie na żywym HA, a generatory (`../gen_unified_dashboard.py`,
`../gen_g1_dashboard.py`, `../gen_gateway_dashboard.py`) zostały w tyle: odtwarzają układ
7-zakładkowy, gdy na maszynach żyje 5-zakładkowy z ręcznymi poprawkami. Uruchomienie generatora
na produkcji skasowałoby te poprawki — dlatego są oznaczone LEGACY.

## Pętla robocza (narzędzie: `../dash_sync.py`)
```bash
python ../dash_sync.py pull           # LIVE → repo (po każdej zmianie w HA; to jest commit-owalny kanon)
python ../dash_sync.py diff           # czy live rozjechał się z baseline
python ../dash_sync.py push sup-g1    # repo → LIVE (rollback / klonowanie bramki)
```
Cele: `sup-g2` (100.79.111.24 `lora-scada`), `sup-g1` (100.79.111.24 `lora-scada-g1`),
`gw-g2` (100.98.155.78 `lora-scada`), `gw-g1` (100.68.79.100 `lora-scada`),
`gw-g1-carport` (100.68.79.100 `carport-dashboard`).

**G1 wróciła do sieci 2026-08-24** pod adresem `100.68.79.100` (stary `100.104.38.14` jest MARTWY)
i ma DWA dashboardy, oba objęte pullem. UWAGA: łącze do G1 bywa przerywane — `pull gw-g1` czasem
trzeba powtórzyć (ssh timeout 255), to normalne, nie awaria.

Aktualne zrzuty (2026-08-26, wszystkie 3 maszyny): `live_sup_lora-scada_G2.json` (5 widoków),
`live_sup_lora-scada-g1.json` (4), `live_gw_g2_lora-scada.json` (5),
`live_gw_g1_lora-scada.json` (5), `live_gw_g1_carport-dashboard.json` (2).
Starsze `lora-scada-{sup,gw}.json` = zrzuty z 2026-07-19.

Zmiana 2026-08-26: LQI (📶) przeniesione do OSOBNEJ linii w kafelkach — przy 3 kafelkach obok
siebie wartość była ucinana. Dotyczy szablonów `carport_relay`/`carport_motion` (G1, sup-g1)
oraz kart per-urządzenie na `sup-g2`. Zrobione po WS na żywym HA, potem `pull` do baseline.

---

## (historyczne) Baseline dashboardów bramki G1 (HA na 100.98.155.78)

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
