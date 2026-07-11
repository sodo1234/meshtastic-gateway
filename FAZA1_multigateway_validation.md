# FAZA 1 — Walidacja multi-gateway (PRZED sprzedażą)

> **Cel:** udowodnić, że architektura wielowęzłowa (slotowanie + N anten + anti-collision)
> DZIAŁA naprawdę, a nie tylko w teorii na G1. To bloker sprzedaży #1.
> **Status kodu (2026-06-21):** logika slotowania ready (`SlotScheduler`), deployment NIE.
> **Przygotowanie bez-sprzętowe (2026-06-29):** część programowo-weryfikowalna ZALICZONA —
> anti-collision (warstwa czasowa) i atrybucja sid udowodnione testami. Config G2 przygotowany
> jako bloki do odkomentowania in-place. Zostaje wyłącznie część sprzętowa (D/E/RF, PUSH/PULL).
> Uruchom: `cd skrypty && python3 test_faza1_multigw.py` (oczekiwane: wszystko zielone).

---

## A. Przygotowanie sprzętu

| Element | Stan teraz | Potrzeba na G2 |
|---|---|---|
| Bramka G2 (Heltec V3 + ConBee/SONOFF + Z2M + HA) | brak | drugi box (jest `.49` debian-new — kandydat) |
| Antena LoRa supervisora dla G2 | 1 antena (ANT-1→G1) | **2. Heltec na supervisorze = ANT-2** |
| Urządzenia Zigbee na G2 | — | min. 3-4 (temp/switch/door) do testu |

**Krytyczne:** architektura = 1 antena LoRa na bramkę po stronie supervisora. Bez drugiego
Helteca na supervisorze G2 nie ma jak słuchać → test niemożliwy. To sprzętowy must-have.

---

## B. Zmiany konfiguracji (po podłączeniu sprzętu)

**Supervisor (`config.py`, sekcja SUPERVISOR_CONFIG):**
```python
"gateways": ["G1", "G2"],                     # było ["G1"]
"mesh_ports": [
   {... ANT-1 ..., "gateways": ["G1"]},
   {... ANT-2 (nowy by-id Helteca) ..., "enabled": True, "label": "ANT-2", "gateways": ["G2"]},
],
"calendar": { "enabled_gateways": ["G1", "G2"] },   # push harmonogramu też do G2
```

**Bramka G2 (na maszynie G2, `~/meshtastic/`):**
- `.role` = `gateway`
- `config.py`: `"id": "G2"`, własny `mesh_port` (by-id Helteca G2), `monitored[]`, `priority[]`,
  `ics_path` = `local_calendar.g2.ics`, `calendar_id` = `calendar.lora_g2`
- sekrety in-place (MQTT_PASS + HA_TOKEN G2) — NIE scp configu
- `slotting.gateways` = `["G1","G2","G3"]` (już jest) → G2 weźmie indeks 1 = okno 20-39s

---

## C. Test slotowania / anti-collision (rdzeń Fazy 1)

**Procedura:**
1. Uruchom G1 + G2 + supervisor (Launcher / test_step5_anomaly.py).
2. Obserwuj logi TX obu bramek przez 3-4 cykle (60s każdy):
   - G1 batch `b` wychodzi TYLKO w `[0,20)s` okna cyklu
   - G2 batch `b` wychodzi TYLKO w `[20,40)s`
3. Sprawdź log slotu: `[SLOT] ⏳ G2 czeka Xs na slot 1`.
4. Supervisor: oba HB/batch docierają, ZERO kolizji (brak zgubionych batchy z nakładania).

**Oczekiwany wynik:**
- ✓ Batche G1 i G2 NIGDY nie nadają w tym samym oknie (rozłączne czasowo)
- ✓ pong/HB/cal idą natychmiast (nie czekają na slot) — reaktywność zachowana
- ✓ Supervisor poprawnie przypisuje dane do G1 vs G2 (brak mieszania sid)

**Jeśli FAIL:** kolizje = batche się gubią gdy obie nadają → slotowanie nie chroni →
architektura 3-bramkowa nie działa. To MUSI przejść przed sprzedażą.

---

## D. Test RF na dystans (zasięg produkcyjny)

1. G2 w docelowej odległości/przez przeszkody (beton/stal hali), nie point-blank.
2. Zmierz przez ~30 min: % dostarczonych batchy, retransmisje cal, RSSI/SNR z HB.
3. Test PUSH kalendarza do G2 (Sync Calendar) + PULL (Push Schedule Up).
4. Zwiększaj dystans aż packet-loss przekroczy ~20% → to max praktyczny zasięg/bramkę.

**Wynik:** tabela zasięg ↔ packet-loss → realna specyfikacja „ile m²/bramkę" do oferty.

---

## E. Fault injection (degradacja + recovery)

| Test | Akcja | Oczekiwane |
|---|---|---|
| Bramka down | wyłącz G2 | supervisor: cascade offline G2 (watchdog HB), G1 nietknięty |
| LoRa jam/zasięg | odłącz antenę G2 | brak HB → offline po timeout, recovery po powrocie |
| Z2M drop | restart z2m na G2 | reconnect, urządzenia wracają, brak fałszywych anomalii |
| Supervisor down | restart supervisora | bramki autonomiczne (lokalne Z2M+HA działają), resync po powrocie |

**Wynik:** udokumentowana macierz degradacji = podstawa SLA dla klienta.

---

## F. Kryteria zaliczenia Fazy 1 (gate do sprzedaży)

- [ ] Slotowanie: G1/G2 rozłączne czasowo, zero kolizji przez 10+ cykli
- [ ] PUSH kalendarza działa do OBU bramek niezależnie
- [ ] PULL działa (po RF fix sprzętowym)
- [ ] Zmierzony max zasięg/bramkę (specyfikacja oferty)
- [ ] Macierz fault-injection udokumentowana (podstawa SLA)
- [ ] Bramki autonomiczne przy supervisor-down

**Dopiero po wszystkich ✓ → Gliwice jako reference case → sprzedaż Tychy.**
Inaczej pierwszy płatny deploy padnie publicznie i zabije łańcuch poleceń.

---

## G. Walidacja bez-sprzętowa (ZROBIONE 2026-06-29)

Część kryteriów Fazy 1 da się udowodnić zanim przyjedzie box G2 — i jest udowodniona:

| Co | Narzędzie | Wynik |
|---|---|---|
| Anti-collision (warstwa czasowa) F#1 | `python3 -m modules.slotting.collision_sim` | ZERO kolizji przez 40 cykli, airtime 1 s |
| Atrybucja sid G1 vs G2 (C#3) | `python3 test_faza1_multigw.py` | ten sam sid w G1/G2 rozłączny, zero mieszania |
| Oba naraz (CI) | `cd skrypty && python3 test_faza1_multigw.py` | zielone = część bez-sprzętowa OK |

**Wniosek z symulacji (do oferty/SLA):**
- Przy realnym airtime ~1 s (≈120 B) slotowanie jest **rozłączne nawet bez guard-band**,
  i to gdy flush danych dryfuje względem cyklu slotu (mon_interval względnie pierwszy z 60 s).
- **Edge-case:** dla wolnych/dużych ramek (airtime ≥ 2 s) ramka startująca tuż przed końcem
  okna może wejść w slot kolejnej bramki (spill-over). Mitygacja: **guard-band = airtime**
  (nie startuj TX, gdy do końca okna < airtime). Symulacja: 10 spill-over → 0 po guard-band.
  Rekomendacja: dodać guard-band do `SlotScheduler`, jeśli kiedykolwiek airtime batcha urośnie
  (większy payload / wolniejszy preset LoRa). Dziś NIE jest blokerem.

**Jak supervisor rozróżnia bramki (potwierdzone w kodzie):**
- RX: każda ramka niesie pole `g:` (bramka stempluje własne id) → `Dispatcher`/`SupervisorData`
  kluczują dane per `g:`. Przeciek RF ramki G1 na ANT-2 NIE skaża G2. To gwarancja braku
  mieszania sid (nie antena, lecz `g:`).
- TX: `mesh_ports[].gateways` buduje `gw_routes` → `send_to(G2)` wychodzi przez ANT-2
  (fallback = broadcast). Komendy trafiają właściwą anteną.

---

## H. Runbook deploy + test live G2 (turnkey — gdy sprzęt gotowy)

**Krok 0 — sanity bez sprzętu (już zielone):**
```bash
cd skrypty && python3 test_faza1_multigw.py        # musi przejść przed deployem
```

**Krok 1 — sprzęt:** drugi Heltec na supervisorze (= ANT-2), box G2 (Z2M+HA), 3-4 urządzenia
Zigbee na G2. Ustal by-id obu Helteców: `ls -l /dev/serial/by-id/`.

**Krok 2 — config SUPERVISORA (in-place, NIE scp/regex — sekrety per-maszyna):**
W `skrypty/config.py` (sekcja `SUPERVISOR_CONFIG`):
1. `_MESH_PORT_ANT2` = realny by-id drugiego Helteca.
2. Odkomentuj wpis `ANT-2 → ["G2"]` w `mesh_ports`.
3. `"gateways": ["G1", "G2"]`.
4. `calendar.enabled_gateways: ["G1", "G2"]`.
Weryfikacja: `LORA_ROLE=supervisor python3 -c "from config import CONFIG; print(CONFIG['gateways'], [p['label'] for p in CONFIG['mesh_ports']])"`
→ oczekiwane `['G1','G2'] ['ANT-1','ANT-2']`.

**Krok 3 — config BRAMKI G2 (na boxie G2, in-place):** patrz blok „FAZA 1 — PRZENIESIENIE
TEGO BLOKU NA MASZYNĘ G2" na górze `GATEWAY_CONFIG`. Zmień `id`=G2, `mesh_port`=by-id G2,
`monitored`/`priority_devices`, `ics_path`=`local_calendar.g2.ics`, ewentualnie
`gw_calendar_id`=`calendar.lora_g2`. Sekrety (HA_TOKEN_GW, MQTT_PASS) ustaw przez Launcher.
`.role` = `gateway`. slotting.gateways już `["G1","G2","G3"]` → G2 = okno 20-39 s automatycznie.

**Krok 4 — uruchomienie (przez Launcher; uwaga na konflikt portów — zabij stare instancje):**
G1, G2, supervisor. W logu supervisora spodziewaj się:
`📡 2 antenna(s) connected, routes: {'G1':'ANT-1','G2':'ANT-2'}`.

**Krok 5 — test slotowania (sekcja C), pass/fail:**
- [ ] log bramek: batch `b` G1 startuje tylko w `[0,20)s` cyklu, G2 tylko w `[20,40)s`.
- [ ] log slotu: `[SLOT] ⏳ G2 czeka Xs na slot 1`.
- [ ] supervisor: oba HB/batch docierają, dane G1 i G2 osobno (sprawdź licznik per bramka).
- [ ] przez 10+ cykli zero zgubionych batchy z nakładania.

**Krok 6 — kalendarz (sekcja D #3, F):** „Sync Calendar" per bramka → ICS na G1 i G2 niezależnie;
„Push Schedule Up" (PULL) po fixie RF. **Krok 7 — RF/zasięg (D)** i **fault injection (E)** wg tabel wyżej.

> Po przejściu sekcji C-F na sprzęcie: odhacz checklistę F i zapisz wyniki RF/fault do oferty/SLA.

---

## H-bis. Szybki test slotowania: DWIE bramki na JEDNEJ maszynie (bez osobnych boxów)

Do walidacji samego slotowania/anti-collision nie trzeba dwóch maszyn — wystarczy jedna z
**2 Heltecami** (po jednym na bramkę). Supervisor na osobnym boksie (jego ANT-2 → patrz sekcja H).
Env nadpisuje `id`+`mesh_port`; pliki stanu `/tmp` dostają suffix `_g2` (G1 bez zmian). Docelowo
każda bramka i tak = osobny box (env nieużywane).

**1. Ustal porty obu Helteców — UŻYJ by-path, NIE by-id:**
```bash
ls -l /dev/serial/by-path/    # np. pci-...-usb-0:3:...-port0 (G1), pci-...-usb-0:1.1:...-port0 (G2)
```
> ⚠️ Tanie Heltec CP2102 mają TEN SAM serial „0001" → by-id daje jeden kolidujący symlink i NIE
> rozróżni dwóch sztuk. by-path (gniazdo USB) jest jednoznaczny i stabilny. Sprawdź którą tty
> trzyma już G1: `ls -l /proc/$(pgrep -f test_step5|tail -1)/fd | grep ttyUSB` → G2 weź drugą.

**2. Terminal 1 — G1 (jak dotąd, domyślnie, swój Heltec):**
```bash
cd ~/meshtastic && python3 test_step5_anomaly.py
```

**3. Terminal 2 — G2 (env-override, DRUGI Heltec po by-path):**
```bash
cd ~/meshtastic
LORA_GW_ID=G2 LORA_MESH_PORT=/dev/serial/by-path/<HELTEC-B-by-path> python3 test_step5_anomaly.py
```

**4. Obserwuj (pass/fail).** Trik: pozycja w cyklu = `epoch%60` = sekundy znacznika logu (HH:MM:**SS**),
więc batch G1 ma SS∈[0,20), G2 SS∈[20,40):
```bash
grep -oE '[0-9:]{8}.*TX: \{"t":"b"' /tmp/test_transport.log    | grep -oE '^[0-9:]+'  # G1 → SS 00-19
grep -oE '[0-9:]{8}.*TX: \{"t":"b"' /tmp/test_transport_g2.log | grep -oE '^[0-9:]+'  # G2 → SS 20-39
```
- [x] log G1: `📤 TX` batcha `b` tylko w `[0,20)s` cyklu.
- [x] log G2: `[SLOT] ⏳ G2 czeka Xs na slot 1`, TX tylko w `[20,40)s`.
- [ ] supervisor (osobny box): oba HB/batch docierają, dane G1 i G2 osobno (atrybucja po `g:`).

> ✅ **WYNIK LIVE (2026-06-29):** wykonane na bramce testowej (2 Heltece, ten sam serial → by-path).
> G1 batche o SS=00/03/06, G2 o SS=20..38 → okna ROZŁĄCZNE, zero nakładania. Anti-collision (F#1)
> potwierdzony na żywym RF, nie tylko w symulacji. Status/`st` szły natychmiast (raw, poza oknem).
> Pozostaje do odhaczenia tylko obserwacja po stronie supervisora (gdy ANT-2 podłączona).

**Czego ten tryb NIE testuje / ograniczenia:**
- RF/zasięg — wszystko point-blank na jednym stole (to NIE sekcja D).
- Jeden Z2M = te same czujniki zgłoszone jako `g1` i `g2` (duplikaty; encje rozłączne po prefiksie).
- Kalendarz/ICS: `ics_path` jest stały (`local_calendar.g1.ics`) — jeśli testujesz też PUSH
  kalendarza w tym trybie, nadpisz `LORA_GW_ID`-aware ścieżkę ICS, inaczej oba procesy piszą
  ten sam plik. Do testu samego slotowania nieistotne.
