# BUILD REPORT — konwencja nazw encji supervisora

Data: 2026-07-17

Branch: `step-04-calendar`

Zakres: lokalna implementacja `ROUTE_PLAN.md`; bez commita, deployu i kontaktu z maszynami zdalnymi.

## Zmienione pliki

| Plik | Linie po zmianie | Zmiana |
|---|---:|---|
| `skrypty/modules/protocol/ha_entities.py` | 16–38 | Dodano `gw_name_fmt` z domyślnym `LoRa Gateway {gw}`; `_gw_device()` używa formatu; urządzenia per-device mają nazwę `LoRa <gw> <dev>`. |
| `skrypty/modules/protocol/ha_entities.py` | 47–79, 106–122 | Encje bramki i kontrolki supervisora nie powtarzają `GW <gw>` w nazwie encji. `reg_gw_buttons_local()` pozostał bez zmian. |
| `skrypty/modules/protocol/ha_entities.py` | 185–220 | Virtual I/O ma format nazwy urządzenia i prefiks encji zależny od roli: domyślnie zachowanie gateway bez zmian, supervisor używa `LoRa <gw> Virtual I/O` bez prefiksu encji `LoRa`. |
| `skrypty/modules/protocol/ha_entities.py` | 227–315 | Sensory, binary sensory i switche używają krótkich nazw encji; główny switch ma `name=None`. Wszystkie `unique_id`, `object_id` i topiki pozostały bez zmian. |
| `skrypty/modules/params/manager.py` | 130–140 | `ParamSync._device()` zwraca `LoRa G2` dla roli supervisor i zachowuje `LoRa Gateway G2` dla roli gateway. |
| `skrypty/test_step5_anomaly.py` | 1180 | Tylko harness supervisora przekazuje formaty nazw gateway i Virtual I/O; harness gateway używa nadal wartości domyślnych. |
| `skrypty/test_step5_anomaly.py` | 1403–1426, 1652–1748, 1793–1835 | Skrócono wyłącznie nazwy encji w rejestratorach supervisora: anomalie, Refresh, Last Seen, Link Quality, Contact, Stagnation, hashe i czas bramki. Bloki `device` oraz `unique_id` pozostały bez zmian. |
| `skrypty/modules/params/smoke_test.py` | 209–230 | Dodano regresję nazw urządzenia per rola oraz niezmienionych identifierów, UID i topiców F3. |
| `skrypty/modules/protocol/naming_smoke_test.py` | 1–196 | Nowy test `slugify()`, wyliczania entity ID z `device.name` + `entity.name`, braku kolizji multi-gw, domyślnej roli gateway, nazw Virtual I/O per rola i twardych list historycznych `unique_id`. |

## Mapa encji urządzeń G2

Poniższe mapowania dotyczą świeżego utworzenia encji po purge registry. Istniejące encje zachowują stare `entity_id`, dopóki nie zostaną usunięte i odtworzone.

| Urządzenie | Stare `entity_id` | Nowe `entity_id` |
|---|---|---|
| Door 1 | `binary_sensor.lora_door_1_door_1_contact` | `binary_sensor.lora_g2_door_1_contact` |
| Door 1 | `sensor.lora_door_1_door_1_battery` | `sensor.lora_g2_door_1_battery` |
| Door 1 | `binary_sensor.lora_door_1_door_1_available` | `binary_sensor.lora_g2_door_1_available` |
| Door 1 | `sensor.lora_door_1_door_1_last_seen` | `sensor.lora_g2_door_1_last_seen` |
| Door 1 | `button.lora_door_1_door_1_refresh` | `button.lora_g2_door_1_refresh` |
| Door 1 | `sensor.lora_door_1_door_1_link_quality` | `sensor.lora_g2_door_1_link_quality` |
| Door 1 | `binary_sensor.lora_door_1_door_1_stagnation` | `binary_sensor.lora_g2_door_1_stagnation` |
| Leak 1 | `binary_sensor.lora_leak_1_leak_1_water_leak` | `binary_sensor.lora_g2_leak_1_water_leak` |
| Leak 1 | `sensor.lora_leak_1_leak_1_battery` | `sensor.lora_g2_leak_1_battery` |
| Leak 1 | `binary_sensor.lora_leak_1_leak_1_available` | `binary_sensor.lora_g2_leak_1_available` |
| Leak 1 | `sensor.lora_leak_1_leak_1_last_seen` | `sensor.lora_g2_leak_1_last_seen` |
| Leak 1 | `button.lora_leak_1_leak_1_refresh` | `button.lora_g2_leak_1_refresh` |
| Leak 1 | `sensor.lora_leak_1_leak_1_link_quality` | `sensor.lora_g2_leak_1_link_quality` |
| Leak 1 | `binary_sensor.lora_leak_1_leak_1_stagnation` | `binary_sensor.lora_g2_leak_1_stagnation` |
| Temp 1 | `sensor.lora_temp_1_temp_1_temperature` | `sensor.lora_g2_temp_1_temperature` |
| Temp 1 | `sensor.lora_temp_1_temp_1_humidity` | `sensor.lora_g2_temp_1_humidity` |
| Temp 1 | `sensor.lora_temp_1_temp_1_battery` | `sensor.lora_g2_temp_1_battery` |
| Temp 1 | `sensor.lora_temp_1_temp_1_last_seen` | `sensor.lora_g2_temp_1_last_seen` |
| Temp 1 | `binary_sensor.lora_temp_1_temp_1_available` | `binary_sensor.lora_g2_temp_1_available` |
| Temp 1 | `button.lora_temp_1_temp_1_refresh` | `button.lora_g2_temp_1_refresh` |
| Temp 1 | `sensor.lora_temp_1_temp_1_link_quality` | `sensor.lora_g2_temp_1_link_quality` |
| Temp 1 | `binary_sensor.lora_temp_1_temp_1_stagnation` | `binary_sensor.lora_g2_temp_1_stagnation` |
| Temp 2 | `sensor.lora_temp_2_temp_2_temperature` | `sensor.lora_g2_temp_2_temperature` |
| Temp 2 | `sensor.lora_temp_2_temp_2_humidity` | `sensor.lora_g2_temp_2_humidity` |
| Temp 2 | `sensor.lora_temp_2_temp_2_battery` | `sensor.lora_g2_temp_2_battery` |
| Temp 2 | `sensor.lora_temp_2_temp_2_last_seen` | `sensor.lora_g2_temp_2_last_seen` |
| Temp 2 | `binary_sensor.lora_temp_2_temp_2_available` | `binary_sensor.lora_g2_temp_2_available` |
| Temp 2 | `button.lora_temp_2_temp_2_refresh` | `button.lora_g2_temp_2_refresh` |
| Temp 2 | `sensor.lora_temp_2_temp_2_link_quality` | `sensor.lora_g2_temp_2_link_quality` |
| Temp 2 | `binary_sensor.lora_temp_2_temp_2_stagnation` | `binary_sensor.lora_g2_temp_2_stagnation` |
| Test 1 | `switch.lora_test_1_lora_test_1` | `switch.lora_g2_test_1` |
| Test 1 | `binary_sensor.lora_test_1_test_1_available` | `binary_sensor.lora_g2_test_1_available` |
| Test 1 | `sensor.lora_test_1_test_1_last_seen` | `sensor.lora_g2_test_1_last_seen` |
| Test 1 | `button.lora_test_1_test_1_refresh` | `button.lora_g2_test_1_refresh` |
| Test 1 | `sensor.lora_test_1_test_1_link_quality` | `sensor.lora_g2_test_1_link_quality` |
| Test 1 | `binary_sensor.lora_test_1_test_1_stagnation` | `binary_sensor.lora_g2_test_1_stagnation` |
| Test 2 | `switch.lora_test_2_lora_test_2` | `switch.lora_g2_test_2` |
| Test 2 | `binary_sensor.lora_test_2_test_2_available` | `binary_sensor.lora_g2_test_2_available` |
| Test 2 | `sensor.lora_test_2_test_2_last_seen` | `sensor.lora_g2_test_2_last_seen` |
| Test 2 | `button.lora_test_2_test_2_refresh` | `button.lora_g2_test_2_refresh` |
| Test 2 | `sensor.lora_test_2_test_2_link_quality` | `sensor.lora_g2_test_2_link_quality` |
| Test 2 | `binary_sensor.lora_test_2_test_2_stagnation` | `binary_sensor.lora_g2_test_2_stagnation` |

## Parametry i Virtual I/O

| Zakres | Stare `entity_id` | Nowe `entity_id` |
|---|---|---|
| 15 parametrów `number` (`P1…PR`) | `number.lora_gateway_g2_<key>_<nazwa>` | `number.lora_g2_<key>_<nazwa>` |
| Przykład P1 | `number.lora_gateway_g2_p1_stagnation_bateryjne` | `number.lora_g2_p1_stagnation_bateryjne` |
| Send Config | `button.lora_gateway_g2_lora_wyslij_config` | `button.lora_g2_lora_wyslij_config` |
| Send Timeout | `button.lora_gateway_g2_lora_wyslij_timeout` | `button.lora_g2_lora_wyslij_timeout` |
| Send Progi | `button.lora_gateway_g2_lora_wyslij_progi` | `button.lora_g2_lora_wyslij_progi` |
| Virtual switch — rola gateway (bez zmian) | `switch.lora_virtual_i_o_g2_lora_<nazwa>` | `switch.lora_virtual_i_o_g2_lora_<nazwa>` |
| Virtual button — rola gateway (bez zmian) | `button.lora_virtual_i_o_g2_lora_<nazwa>` | `button.lora_virtual_i_o_g2_lora_<nazwa>` |
| Virtual switch — supervisor | `switch.lora_virtual_i_o_g2_lora_<nazwa>` | `switch.lora_g2_virtual_i_o_<nazwa>` |
| Virtual button — supervisor | `button.lora_virtual_i_o_g2_lora_<nazwa>` | `button.lora_g2_virtual_i_o_<nazwa>` |

## Encje bramki G2 na supervisorze

| Grupa | Stare `entity_id` | Nowe `entity_id` |
|---|---|---|
| Status | `binary_sensor.lora_gateway_g2_gw_g2_status` | `binary_sensor.lora_g2_status` |
| Uptime | `sensor.lora_gateway_g2_gw_g2_uptime` | `sensor.lora_g2_uptime` |
| Last Seen | `sensor.lora_gateway_g2_gw_g2_last_seen` | `sensor.lora_g2_last_seen` |
| Total | `sensor.lora_gateway_g2_gw_g2_total` | `sensor.lora_g2_total` |
| Monitored | `sensor.lora_gateway_g2_gw_g2_monitored` | `sensor.lora_g2_monitored` |
| Priority | `sensor.lora_gateway_g2_gw_g2_priority` | `sensor.lora_g2_priority` |
| Offline | `sensor.lora_gateway_g2_gw_g2_offline` | `sensor.lora_g2_offline` |
| Ping | `button.lora_gateway_g2_gw_g2_ping` | `button.lora_g2_ping` |
| Discovery | `button.lora_gateway_g2_gw_g2_discovery` | `button.lora_g2_discovery` |
| Sync Time | `button.lora_gateway_g2_gw_g2_sync_time` | `button.lora_g2_sync_time` |
| Hash Disc (bramka) | `sensor.lora_gateway_g2_gw_g2_hash_disc_bramka` | `sensor.lora_g2_hash_disc_bramka` |
| Hash Param (bramka) | `sensor.lora_gateway_g2_gw_g2_hash_param_bramka` | `sensor.lora_g2_hash_param_bramka` |
| Hash kalendarza (bramka) | `sensor.lora_gateway_g2_gw_g2_hash_kalendarza_bramka` | `sensor.lora_g2_hash_kalendarza_bramka` |
| Kalendarz — zgodność | `sensor.lora_gateway_g2_gw_g2_kalendarz_zgodnosc` | `sensor.lora_g2_kalendarz_zgodnosc` |
| Jakość czasu | `sensor.lora_gateway_g2_gw_g2_jakosc_czasu` | `sensor.lora_g2_jakosc_czasu` |
| Pora dnia / tryb | `sensor.lora_gateway_g2_gw_g2_pora_dnia_tryb` | `sensor.lora_g2_pora_dnia_tryb` |
| Czas bramki | `sensor.lora_gateway_g2_gw_g2_czas_bramki` | `sensor.lora_g2_czas_bramki` |
| Offset czasu [s] | `sensor.lora_gateway_g2_gw_g2_offset_czasu_s` | `sensor.lora_g2_offset_czasu_s` |
| Tryb — stan | `sensor.lora_gateway_g2_gw_g2_tryb_stan` | `sensor.lora_g2_tryb_stan` |
| Anomalie Offline | `sensor.lora_gateway_g2_gw_g2_anomalie_offline` | `sensor.lora_g2_anomalie_offline` |
| Anomalie Bateria | `sensor.lora_gateway_g2_gw_g2_anomalie_bateria` | `sensor.lora_g2_anomalie_bateria` |
| Anomalie Bateria # | `sensor.lora_gateway_g2_gw_g2_anomalie_bateria` (kolizja nazwy po slugowaniu `#`) | `sensor.lora_g2_anomalie_bateria` (kolizja nazwy po slugowaniu `#`) |
| Anomalie Inne | `sensor.lora_gateway_g2_gw_g2_anomalie_inne` | `sensor.lora_g2_anomalie_inne` |
| Anomalie Inne # | `sensor.lora_gateway_g2_gw_g2_anomalie_inne` (kolizja nazwy po slugowaniu `#`) | `sensor.lora_g2_anomalie_inne` (kolizja nazwy po slugowaniu `#`) |
| Sync Calendar (nazwa poza zakresem T4) | `button.lora_gateway_g2_gw_g2_sync_calendar` | `button.lora_g2_gw_g2_sync_calendar` |

Uwaga: wiersze z `#` zachowują nazwę wymaganą przez plan (`f"{nm} #"`), ale końcowy znak jest usuwany przez slugowanie. Odrębność tych encji nadal wynika z niezmienionych `unique_id`, natomiast świeżo generowane propozycje `entity_id` są identyczne z wariantem bez `#`; Home Assistant może nadać sufiks kolizyjny.

## Walidacja składni

Po każdej skutecznej edycji pliku Python uruchomiono `ast.parse`. Końcowa walidacja zbiorcza:

```text
AST OK: skrypty/modules/protocol/ha_entities.py, skrypty/modules/params/manager.py, skrypty/test_step5_anomaly.py, skrypty/modules/params/smoke_test.py, skrypty/modules/protocol/naming_smoke_test.py
```

`git diff --check` dla wszystkich pięciu plików Python zakończył się kodem 0.

## Pełne wyjścia smoke testów

Polecenie: `cd skrypty && python -m modules.protocol.naming_smoke_test`

```text
✅ encje urządzeń G1/G2 bez kolizji; unique_id bez zmian
✅ encje bramki supervisora pod LoRa G2; unique_id bez zmian
✅ domyślna nazwa urządzenia roli gateway bez zmian
✅ Virtual I/O: gateway bez zmian, supervisor wg nowej konwencji

────────────────────────────────────────
4/4 passed
```

Polecenie: `cd skrypty && python -m modules.params.smoke_test`

```text
✅ imports OK (15 params: P/T + progi TH/TL/HH/HL/BL/BC + F2 P4/T4/PR)
✅ defaults + 15 number (device.name OK) + przyciski Send
✅ gateway: local set → apply + on_change, BEZ auto-send
✅ Send: gateway→param_upd grupy (P1-P4 / T1-T4), supervisor→params proposal
✅ clamp to min/max
✅ supervisor: local set optimistic, BEZ auto-send
✅ gateway applies proposal → confirm param_upd + on_change
✅ supervisor mirrors confirmed param_upd
✅ params_req → gateway push_all (full set)
✅ params_hash stable + changes on edit
✅ on_mqtt_set parses topic+payload, ignores junk
✅ F2: P4/T4/PR w PARAM_ORDER+grupach, clamp min/max OK
✅ F3: 2 instancje supervisora (G1,G2) — różne uid/topic/persist, izolowany stan
✅ F3: gateway topics niezmienione (lora/params/gateway/set/P1)
✅ device.name per rola; uid/topics F3 bez zmian

────────────────────────────────────────
15/15 passed
```

## Otwarte pytania

1. `ROUTE_PLAN.md` mówi o „wszystkich 7 urządzeniach G2”, ale wylicza tylko sześć: Door 1, Leak 1, Temp 1, Temp 2, Test 1 i Test 2. Nie zgadywano nazwy siódmego urządzenia; tabela obejmuje wszystkie sześć wskazanych.
2. `reg_calendar_button` nie był wskazany w T4 do zmiany nazwy encji. Zgodnie z HARD RULES pozostawiono `GW G2 Sync Calendar`, więc po zmianie nazwy urządzenia wynik świeżego utworzenia to `button.lora_g2_gw_g2_sync_calendar`. Czy w osobnym planie skrócić nazwę encji do `Sync Calendar`?
3. Plan wymaga nazw anomalii count zakończonych `#`. Ponieważ slugowanie usuwa końcowy `#`, wariant items i count proponują ten sam bazowy `entity_id`; HA może dodać sufiks kolizyjny. Czy count powinien w osobnym planie otrzymać nazwę zawierającą alfanumeryczny wyróżnik, np. `Liczba`?

Brak innych otwartych kwestii implementacyjnych.
