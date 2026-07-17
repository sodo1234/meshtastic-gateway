# ROUTE_PLAN: konwencja nazw encji SUPERVISORA `lora_<gw>_<dev>_<param>` (pkt 6)

Repo: `C:\Users\sodoj\Documents\Meshtastic Gateway` (branch step-04-calendar). Kod w `skrypty/`.
Cel usera: na supervisorze jest `binary_sensor.lora_door_1_door_1_contact`, ma być
`binary_sensor.lora_g2_door_1_contact`. Dotyczy WSZYSTKICH encji urządzeń, availability,
oraz parametrów/timeoutów/progów. Rozwiązuje też kolizję multi-gw (G1 „Temp 1" vs G2 „Temp 1").

## MECHANIZM (ZBADANY LIVE — NIE ZGADUJ, NIE ZMIENIAJ TEGO ZAŁOŻENIA)
HA MQTT discovery: gdy config zawiera blok `device`, HA włącza `has_entity_name` i **ignoruje
`object_id`** — `entity_id = slug(device.name) + "_" + slug(entity.name)`.
- Dziś: device `LoRa Door 1` (→`lora_door_1`) + encja `Door 1 Contact` (→`door_1_contact`)
  = `binary_sensor.lora_door_1_door_1_contact` (stąd podwojenie).
- Cel: device `LoRa G2 Door 1` (→`lora_g2_door_1`) + encja `Contact` (→`contact`)
  = `binary_sensor.lora_g2_door_1_contact`. ✅
- Encja główna urządzenia (switch): `"name": None` → entity_id = sam slug device.name
  = `switch.lora_g2_test_1`. ✅
- `unique_id` = tożsamość encji: **NIE ZMIENIAJ unique_id nigdzie** (zmiana = osierocenie).
  Zmieniamy WYŁĄCZNIE `name` encji i `name` w bloku `device`.
- entity_id jest nadawany TYLKO przy pierwszym utworzeniu → istniejące encje zachowają stare
  id. Purge encji + odtworzenie robi Claude LIVE po review. **Ty NIE dotykasz maszyn.**

## HARD RULES
1. Pliki do edycji (TYLKO te): `skrypty/modules/protocol/ha_entities.py`,
   `skrypty/modules/params/manager.py`, `skrypty/test_step5_anomaly.py`,
   `skrypty/modules/params/smoke_test.py`, NOWY `skrypty/modules/protocol/naming_smoke_test.py`.
2. NIE dotykaj `skrypty/modules/transport/` (verified) ani zagnieżdżonego
   `skrypty/modules/protocol/protocol/` (legacy). NIE dotykaj PLAN.md, `homeassistant/gen_*`
   (generatory są WYCOFANE — user przerobił dashboardy ręcznie).
3. **Rola `gateway` MUSI zostać nietknięta** — user ma ręcznie zrobiony dashboard bramki
   oparty o obecne entity_id. Zmiany dotyczą wyłącznie ścieżek używanych przez supervisora.
   Rejestratory wg roli (zweryfikowane):
   - SUPERVISOR: `reg_sensor`, `reg_binary`, `reg_switch_dev`, `reg_gateway`, `reg_gw_controls`,
     `reg_vswitch`, `reg_vbutton`, `ParamSync(role="supervisor")`, oraz inline w harnessie:
     `reg_linkquality`, `reg_stagnant`, `reg_contact_fix`, `reg_binary_last_seen`,
     `reg_refresh_button`, `reg_gw_hashes`, `reg_gw_time_entities`, `reg_anomaly_entities`.
   - GATEWAY (NIE RUSZAĆ): `reg_gw_local_stats`, `reg_gw_buttons_local`,
     `ParamSync(role="gateway")`, encje `calstat`, `reg_gw_lqi`.
4. `test_step5_anomaly.py` ma 131 KB — edytuj chirurgicznie, nie formatuj całości.
   Po KAŻDEJ edycji: `python -c "import ast; ast.parse(open(FILE,encoding='utf-8').read())"`.
5. ZERO `git commit`, ZERO deployu, ZERO ruchu do maszyn zdalnych. Edycje lokalne + testy.
6. Komentarze po polsku, styl otoczenia. JSON wire: `separators=(',',':')`.

## ZADANIA

### T1 — encje per-urządzenie (ha_entities.py)
- `_dev_device(self, gw, dev, model=...)`: `"name": f"LoRa {gw} {dev}"` (było `f"LoRa {dev}"`).
  `identifiers` ZOSTAJĄ `lora_{gw.lower()}_{safe}` (już są per-gw).
- `reg_sensor`: nazwa encji cap → `f"{name.title()}"` (było `f"{dev} {name.title()}"`);
  last_seen → `"Last Seen"`; available → `"Available"`.
- `reg_binary`: cap → `f"{name.replace('_',' ').title()}"`; battery → `"Battery"`;
  available → `"Available"`.
- `reg_switch_dev`: encja główna `"name": None` (było `f"LoRa {dev}"`) → `switch.lora_g2_test_1`;
  available → `"Available"`; last_seen → `"Last Seen"`.
- `reg_vswitch`/`reg_vbutton`: device name → `f"LoRa {gw} Virtual I/O"`, encja → `f"{name}"`.

### T2 — inline rejestratory supervisora (test_step5_anomaly.py, sekcja run_supervisor)
Bloki `device` tych funkcji mają `identifiers` bez `name` (dziedziczą nazwę z `reg_sensor`) —
**nie dodawaj tam `name`**, zmień TYLKO nazwy encji:
- `reg_linkquality`: `f"{dev} Link Quality"` → `"Link Quality"` (uid `lora_{safe}_{safe}_link_quality`
  ZOSTAJE bez zmian — to unique_id).
- `reg_stagnant`: → `"Stagnation"`; `reg_contact_fix`: → `"Contact"`;
  `reg_binary_last_seen`: → `"Last Seen"`; `reg_refresh_button`: → `"Refresh"`.

### T3 — encje poziomu bramki na supervisorze (device `LoRa G2`)
Problem: `_gw_device` (identifiers `lora_gateway_{gl}`) jest współdzielony przez rolę gateway
(`reg_gw_buttons_local`) — nie wolno go zmienić globalnie.
- `HAEntities.__init__`: nowy kwarg `gw_name_fmt="LoRa Gateway {gw}"` (domyślnie = dziś).
  `_gw_device` używa `self.gw_name_fmt.format(gw=gw)`.
- Harness **supervisora**: konstruuj `HAEntities(..., gw_name_fmt="LoRa {gw}")`. Harness bramki
  bez zmian (domyślny format).
- `reg_gateway`: nazwy encji `f"GW {gw} {name}"` → `f"{name}"` (→ `sensor.lora_g2_uptime` itd.).
- `reg_gw_controls`: `f"GW {gw} {name}"` → `f"{name}"` (→ `button.lora_g2_ping`).
- `ParamSync._device()` (manager.py): rola supervisor → `"name": f"LoRa {self.gw_id}"`;
  rola gateway → BEZ ZMIAN `f"LoRa Gateway {self.gw_id}"`. Nazwy encji number zostają
  (`f"{key} · {d['name']}"`) → daje `number.lora_g2_p1_stagnation_bateryjne`.
  Przyciski Send zostają nazwami — dają `button.lora_g2_lora_wyslij_config` (OK).

### T4 — inline encje poziomu bramki (harness supervisora)
- `reg_gw_hashes`: `f"GW {gw} {nm}"` → `f"{nm}"`.
- `reg_gw_time_entities`: `f"GW {gw} {nm}"` → `f"{nm}"`.
- `reg_anomaly_entities`: `f"GW {gw} {nm}"` → `f"{nm}"` (oba warianty: items i count `f"GW {gw} {nm} #"`
  → `f"{nm} #"`).

### T5 — smoke test konwencji (NOWY `skrypty/modules/protocol/naming_smoke_test.py`)
Zaimplementuj `slugify()` odwzorowujący HA (lower, nie-alfanumeryczne→`_`, kolaps powtórzeń,
strip `_`) i `expected_entity_id(device_name, entity_name)` = `slug(dev)+"_"+slug(ent)`
(gdy entity_name None → sam `slug(dev)`). Fake mqtt zbiera publikacje configów.
Asercje (gw="G2"):
- `reg_sensor("G2","Temp 1","thb")` → `sensor.lora_g2_temp_1_temperature`,
  `..._humidity`, `..._battery`, `..._last_seen`, `binary_sensor.lora_g2_temp_1_available`.
- `reg_binary("G2","Door 1","cb")` → `binary_sensor.lora_g2_door_1_contact`,
  `sensor.lora_g2_door_1_battery`, `binary_sensor.lora_g2_door_1_available`.
- `reg_switch_dev("G2","Test 1")` → `switch.lora_g2_test_1` (+ `_available`, `_last_seen`).
- Multi-gw: `reg_sensor("G1","Temp 1","t")` → `sensor.lora_g1_temp_1_temperature`
  (BRAK kolizji z G2 — kluczowy test).
- `HAEntities(gw_name_fmt="LoRa {gw}")` + `reg_gateway("G2")` → `sensor.lora_g2_uptime`,
  `binary_sensor.lora_g2_status`.
- Domyślny format (rola gateway) NIE zmienia się: `HAEntities()` → device name
  `LoRa Gateway G2` (regresja).
- ŻADEN publikowany config nie zmienił `unique_id` względem wartości sprzed zmian
  (lista oczekiwanych uid — asercja twarda).
- params/smoke_test.py: dopisz asercję że supervisor `_device()["name"]=="LoRa G2"` a
  gateway `"LoRa Gateway G2"`, oraz że uid/topics per-gw są nietknięte (regresja F3).
Uruchom: `cd skrypty && python -m modules.protocol.naming_smoke_test` oraz
`python -m modules.params.smoke_test` — wszystkie muszą przejść.

## DELIVERABLE
Zmienione pliki + nowy smoke test, wszystko ast-clean, testy zielone. Zapisz `BUILD_REPORT.md`
w rootcie: co zmienione per plik (z liniami), pełne wyjścia testów, tabela
`stare entity_id → nowe entity_id` dla wszystkich 7 urządzeń G2 (Door 1, Leak 1, Temp 1,
Temp 2, Test 1, Test 2) + params + encje bramki, oraz otwarte pytania. Bez commita.
