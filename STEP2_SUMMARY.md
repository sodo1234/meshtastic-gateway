# STEP 2 — Podsumowanie i status (2026-06-03)

Modularny refaktor systemu LoRa Zigbee SCADA. „Step 2" = `skrypty/test_step2_protocol.py`
(integracja: HB + Discovery + Encje HA + Sterowanie + VIO + dashboardy + optymalizacja ruchu).
Maszyny: gateway G1 (tailscale 100.98.155.78) ↔ supervisor (100.79.111.24), Heltec V3 868 MHz.

## Co zrealizowano (✅ zweryfikowane live)

### Protokół
- **Encje HA**: device (sensor/binary_sensor/switch + `_available`/`_last_seen`) + gateway status.
- **Autodiscovery**: `disc` → bramka `disc_meta + disc_vio + db` → supervisor tworzy encje w MQTT/HA.
  Auto-disc przy zmianie `hash`; re-request gdy lista niepełna (cap 2×, event-driven).
- **Ping adresowany**: `g==gw_id` — tylko wskazana bramka odpowiada `pong`. Broadcast `{"t":"ping"}` = Ping All.
- **Diagnostyka w hb/pong**: total/monitored/priority + uptime + `z2m` + `hash`; last_seen sup-side.
- **Sterowanie cmd → st** z **2 retry + timeout → device OFFLINE**; `st` → online + odbicie stanu w HA.
- **Dwustronne VIO**: vswitch/vbutton dashboard → LoRa → bramka → `vsw_st`/`vbtn_ack` → HA.
- **Sync czasu**: przycisk (globalny + per-bramka) → `{"t":"sync","sec":T}`; bramka liczy offset.

### Watchdog / liveness (minimalny ruch LoRa)
- **HB co 15 min** (było 2 min).
- **Watchdog PASYWNY**: offline po 35 min bez HB — ZERO ruchu sam z siebie.
- **Aktywny ping + 2 retry → offline TYLKO na komendę** (przycisk).
- Supervisor TX ≈ 0 w spoczynku (tylko startowe discovery + ewentualne re-requesty).

### Dashboardy (HA, storage-mode, wdrażane przez WS API)
- **Supervisor**: naprawione encje (switch `_available`/`_last_seen`, per-bramka buttony `lora_gw_g1_ping/_discovery`,
  wyczyszczone duchy). `homeassistant/dashboard.yaml` (3271 lin).
- **Gateway** (`lora-gw`, generator `homeassistant/gen_gateway_dashboard.py`): 4 widoki 1:1 ze stylem supervisora —
  **Bramka** (link do supervisora + sync czasu + uptime/Z2M/last HB), **Sterowanie** (Test 1/2), **Alarmy** (Leak/Door),
  **Pomiary** (Temp 1-4 + apexcharts). Encje z2m surowe; entity_id wyczyszczone (purge sierot + rename przez WS).

### Debug
- Każdy pakiet LoRa: `📤 TX: {pełny json}` / `📥 RX: {pełny json}` centralnie w transporcie; handlery logują konsekwencję.

## Czy wszystko zrealizowane?
**TAK dla zakresu step 2** (protokół + identyfikacja + sterowanie + dashboardy). Świadomie poza zakresem (przyszłe stepy):
anomalie (`*_anomaly`, `lora_an_*`), refresh last value (`*_refresh`), batch danych (live temp/bateria), pełny time-sync (korekta zegara/ICS),
slotowanie/anti-collision. Encja `motion_1` — urządzenia fizycznie brak.

## Znane ograniczenia
- **RF**: `db` dzieli się na 2 pakiety, jeden ginie chronicznie w sesji (anteny 1 m = za blisko → saturacja RX).
  Fix sprzętowy: rozsunąć anteny / zmniejszyć TX power. Encje żyją przez retained discovery, więc dashboard działa.
- **HA entity_id mangling**: `device` + `has_entity_name` ignoruje `object_id` → encje statów wymagały purge+rename przez WS.

## Porównanie z planem refaktoru (17 kroków, CLAUDE.md)

| # | Krok | Status |
|---|------|--------|
| 1 | Transport MQTT + LoRa + Dispatcher | ✅ (step1) |
| 2 | Antena reconnect + port recovery | ✅ (step1) |
| 3 | Encje HA: device + gateway status | ✅ step2 |
| 4 | Autodiscovery MQTT + VSwitch/VButton | ✅ step2 |
| 5 | Ping/Pong/Heartbeat | ✅ step2 |
| 6 | Time sync | ⚠️ częściowo (przycisk + offset; korekta zegara/ICS = TODO) |
| 7 | Slotowanie + anti-collision + safe window | ❌ TODO |
| 8 | ON/OFF + status + retry + timeout → offline | ✅ step2 |
| 9 | Batch monitored (30s, delta) | ❌ → **STEP 3** |
| 10 | Batch priority (10s) | ❌ → **STEP 3** |
| 11 | Refresh last known value | ❌ → **STEP 3** |
| 12-15 | Anomalie (offline/battery/temp-hum/stagnation + transport + auto-clear + dump + dashboard + ghost prune) | ❌ TODO |
| 16 | Kalendarz push/pull + hash sync | ❌ TODO |
| 17 | Parametry bidirectional | ❌ TODO |

**Step 2 pokrył kroki 3, 4, 5, 8 w pełni + 6 częściowo.** Dodatkowo (poza planem): dashboardy HA, minimalizacja ruchu,
niezawodność discovery, HA MCP do obu maszyn.

## STEP 3 — plan (DANE: batch + live values)

Cel: bramka strumieniuje realne dane urządzeń z Z2M do supervisora, z podziałem priority/monitored i deltą.
Realizuje kroki refaktoru **9, 10, 11** + domyka 3 odłożone wymagania usera.

1. **Batch monitored** (`b` P3, co 30 s, tylko delta): `{t:b,g,ts,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}`.
   Gateway czyta stany z Z2M (MQTT `zigbee2mqtt/<dev>`), buforuje, wysyła zmiany co 30 s.
2. **Batch priority** (`b` P1, co 10 s): urządzenia z `priority_devices` częściej, mniejszy payload.
3. **Refresh last known value** (`button.lora_g1_<dev>_refresh` + `req`): wymuszenie ostatniej wartości on-demand.
4. **last_seen per device** (z Z2M `last_seen` lub czas odbioru) → encje już istnieją, podpiąć dane.
5. **availability + battery**: bateryjne/sieciowe → `available` ON/OFF + poziom baterii (z Z2M).
6. **„Każdy świeży stan z Z2M → online"**: odbiór dowolnego update z Z2M oznacza urządzenie online (cascade na supervisorze).
7. **Minimalizacja LoRa**: tylko delta, batch, chunk split >150 B, P-priorytety (P0 system, P1 10 s, P3 30 s).

Pliki: nowy `modules/data/` (batcher + z2m_reader) lub rozszerzenie discovery; `test_step3_data.py`.
Uwaga RF: batch dołoży ruchu — najpierw rozwiązać saturację anten (sprzęt), inaczej delta/batch będą ginąć.

## Następne po STEP 3
6 (pełny time-sync) → 7 (slotowanie/anti-collision) → 12-15 (anomalie) → 16 (kalendarz) → 17 (parametry).
