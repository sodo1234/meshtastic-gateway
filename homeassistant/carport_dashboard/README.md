# Carport — deliverable dashboardu + z2m availability

Pliki YAML do RĘCZNEGO przeniesienia na bramkę carport (workflow: nie wgrywamy całej kopii
dashboardu, tylko pojedyncze pliki — żeby nie rozjechać konfiguracji z2m).

## 0. `carport_dashboard.full.yaml` — PEŁNY dashboard (PRIMARY)
Kompletny dashboard „Carport" wzorem G1 `lora-gw`, **4 widoki**, wszystkie entity_id
**ZWERYFIKOWANE na żywym HA carport** (token, 2026-06-29) — nie zgadywane:
- **Bramka** — status bramki G1 (`lora_gw_g1_*`: synced/uptime/tx_count/batcher/tryb/czas/params P1-P3 + virtual I/O)
- **Harmonogram** — `calendar.lora_g1` + tryb biezacy
- **Przekaźniki** — 105× `switch.*` (ZBMINIR2), po rzędach
- **Czujniki ruchu** — 82× `binary_sensor.*_occupancy` (SNZB-03P), po rzędach

Import: dashboard → Edytuj → ⋮ → „Raw configuration editor" → wklej całość.
> Uboższy niż dev-baseline G1 bo carport HA ma tylko te encje bramki (brak total/offline/anomaly/
> push-schedule w tym zestawie). Rozszerzymy gdy dojdą encje.

## 1. `views_relays_and_motion.yaml` — same dwie zakładki (gdy chcesz tylko je doklejać)
Wygenerowane z `zigbee2mqtt/configuration.yaml` + `database.db` (model per urządzenie):
- **Przekaźniki** (`carport-relays`) — wszystkie ZBMINIR2 (`switch.*`), 105 encji, pogrupowane po rzędach hali (RA1…RA15, RB1…RB11), toggle + state_color.
- **Czujniki ruchu** (`carport-motion`) — wszystkie SNZB-03P (`binary_sensor.*_occupancy`), 82 encje, glance per rząd.

Karty RDZENIOWE (`sections`/`grid`/`entities`/`glance`) — BEZ zależności HACS.

**Jak użyć:** otwórz dashboard carport → ⋮ → „Edytuj dashboard" → ⋮ → „Raw configuration editor"
→ wklej oba wpisy do listy `views:`. (Albo dołącz w dashboardzie YAML-mode.)

> ⚠️ Encje wg konwencji z2m HA discovery: `RA8_R1` → `switch.ra8_r1`, `RA7_S1` →
> `binary_sensor.ra7_s1_occupancy`. Jeśli w HA są nadpisane entity_id, popraw nazwy
> (nie dało się zweryfikować na żywo — token HA po restorze nieważny / 401).

## 2. `z2m_availability.patch.yaml` — availability KAŻDEGO urządzenia
**Problem:** blok `availability:` w configu z2m jest, ale BEZ `enabled: true` → w z2m 2.x
(schema `version: 5`) feature bywa wyłączony mimo obecności bloku.
**Fix:** dodaj `enabled: true`. Wtedy z2m publikuje `zigbee2mqtt/<dev>/availability`
(online/offline) dla WSZYSTKICH urządzeń, a HA discovery dorzuca `availability_topic` →
każda encja pokazuje dostępność. (Bateryjne PIR = tryb `passive`, online jeśli raportowały
w `passive.timeout` min; tu 1440=24h — w razie fałszywych „unavailable" skróć/wydłuż.)

Po zmianie: restart z2m (addon) i sprawdź `zigbee2mqtt/bridge/devices` albo encje availability.

## 3. `views_anomaly.yaml` — widok ALARMY (mechanizm anomalii, HACS-free)
Widok „Alarmy" agregujący **anomalie dostępności** po wszystkich 187 urządzeniach hali
(105 przekaźników + 82 czujniki ruchu) — żeby operator nie skrolował 187 kafli szukając co padło.
Generator: `gen_anomaly_view.py` (czyta encje wprost z `carport_dashboard.styled.yaml`).

Trzy sekcje:
- **PODSUMOWANIE** — 3× `custom:button-card` z licznikiem offline liczonym w JS po liście encji
  (STATUS SYSTEMU OK/ALERT, Przekaźniki offline N/105, Czujniki offline N/82). Zielone gdy 0,
  czerwone gdy >0.
- **PRZEKAŹNIKI OFFLINE** / **CZUJNIKI OFFLINE** — core `entity-filter` (BEZ HACS): pokazuje
  TYLKO urządzenia w stanie `unavailable`/`unknown`; `show_empty: false` chowa kartę gdy wszystko OK.

**Anomalia = dostępność.** Carport HA nie ma sensorów anomalii bramki (`lora_*_an_*`), a deliverable
to sam dashboard-YAML (nie ruszamy gatewaya/z2m) → offline liczony po stronie dashboardu ze stanu
encji. **WYMAGA sekcji 2** (`z2m_availability.patch.yaml`, `enabled: true`): bez availability z2m
wszystkie encje są „dostępne" i widok jest ślepy. Reuzywa `button_card_templates` (lora_hdr/lora_base)
z `styled.yaml` — wklejać po nim (albo do tego samego dashboardu).

Import: dashboard → Edytuj → ⋮ → „Raw configuration editor" → wklej wpis do listy `views:`.
> Rozszerzenie (gdy dojdą encje): baterie SNZB-03P (`sensor.*_battery` < próg) i stagnacja
> jako kolejne sekcje entity-filter/numeric — teraz nieobecne w zestawie encji carport.
