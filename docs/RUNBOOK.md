# LoRa Zigbee SCADA — Runbook operacyjny

Dokumentacja techniczna: **uruchamianie skryptów** + **obsługa dashboardu**.
Dotyczy bieżącego kodu step-by-step (`test_step5_anomaly.py` + `skrypty/modules/*`),
NIE legacy monolitów `*_v38.py`.

> Aktualizacja: 2026-08-05. Skróty: **GW** = bramka, **SUP** = supervisor.

---

## 1. Architektura i maszyny

```
        SUPERVISOR (G0) — agreguje wszystkie bramki, dashboard zbiorczy
                 │  LoRa 868 MHz (Meshtastic, Heltec V3) — 1 antena / bramkę
   ┌─────────────┼─────────────┐
  G1            G2            G3
 (bramka)     (bramka)      (bramka)
 Xeon + Zigbee2MQTT + Home Assistant Supervised (każda autonomiczna)
```

- Każda **bramka** jest autonomiczna (lokalny Z2M + HA). Zbiera stany Zigbee,
  wykrywa anomalie, steruje. **Bramka = źródło prawdy.**
- **Supervisor** agreguje bramki i pokazuje zbiorczy dashboard. Nie jest krytyczny
  — jego restart to tylko przerwa w agregacji, bramki działają dalej.
- Ten sam plik `test_step5_anomaly.py` uruchamiany jest w dwóch rolach; rola wynika
  z `ROLE` w `config.py` danej maszyny (`gateway` / `supervisor`).

### Hosty (Tailscale)

| Rola | Nazwa (launcher) | IP Tailscale | Uwaga |
|------|------------------|--------------|-------|
| Bramka produkcyjna | `carport` (G1) | `100.104.38.14` | REALNY site, 194 urządzenia — ostrożnie |
| Bramka testbed | `carport-g2` | `100.98.155.78` | dom, do testów |
| Supervisor | `supervisor` | `100.79.111.24` | G0 |

> **G1 bywa nieosiągalna po sieci** (host miga w Tailscale). Nie znaczy to, że nie żyje —
> potrafi pracować i gadać po LoRa, będąc bez SSH. Stan sprawdzaj po HB na supervisorze,
> nie po pingu. Deploy w oknie dostępności: `bash deploy/deploy_g1.sh` (§3.3).

Login SSH: użytkownik `td` (klucz skonfigurowany — launcher używa `BatchMode`).

---

## 2. Wymagania wstępne

- **Tailscale** aktywny (hosty adresowane po IP Tailscale — omija zawodne MagicDNS).
- **venv** na każdej maszynie: `~/meshtastic/venv/bin/python` (paho-mqtt, meshtastic).
- **Kod** w `~/meshtastic/` (skrypt + `modules/`), zsynchronizowany z repo.
- **Tokeny HA** (long-lived) w `config.py` każdej maszyny:
  `HA_TOKEN_GW` (HA bramki), `HA_TOKEN_SUP` (HA supervisora); aktywny wg roli
  (`HA_TOKEN = HA_TOKEN_GW if ROLE=='gateway' else HA_TOKEN_SUP`).
  MCP (Claude Code) czyta tokeny z `~/.claude.json` (ha-gw / ha-sup) — po zmianie
  tokenu wymagany **restart Claude Code**, żeby MCP je wczytał.
- **MQTT**: broker HA na `localhost:1883` (bramka) / `172.17.0.1:1883` (supervisor),
  user `mqtt`.

---

## 3. Uruchamianie skryptów

### 3.1 Launcher (zalecane)

Webowy launcher (Flask) na **`http://localhost:8765`** steruje startem/stopem na
zdalnych maszynach (SSH → `tmux`). Robi robust-kill + weryfikację (nie kłamie, że
„stopped", jeśli proces żyje).

**Przez UI:**
1. Wybierz **host** (`carport-g2` / `supervisor` / …).
2. Wybierz **skrypt**: `test_step5_anomaly.py`.
3. **Start** (rola `gw` dla bramki, `sup` dla supervisora).
4. Podgląd logu: przycisk **tail** (otwiera nową kartę `tail -F` remote logu).

**Przez API (automatyzacja):**
```bash
# Start bramki na G2
curl -s -X POST http://localhost:8765/api/start \
  -H 'Content-Type: application/json' \
  -d '{"host":"carport-g2","role":"gw","script":"test_step5_anomaly.py"}'

# Start supervisora
curl -s -X POST http://localhost:8765/api/start \
  -H 'Content-Type: application/json' \
  -d '{"host":"supervisor","role":"sup","script":"test_step5_anomaly.py"}'

# Status operacji (start/stop są asynchroniczne — pollowa­nie)
curl -s "http://localhost:8765/api/op?host=carport-g2&role=gw"
#  → {"busy":false,"ok":true,"result":"started on default port"}

# Stop
curl -s -X POST http://localhost:8765/api/stop \
  -H 'Content-Type: application/json' \
  -d '{"host":"carport-g2","role":"gw"}'
```

Inne endpointy: `/api/status`, `/api/scripts?host=`, `/api/usb-devices?host=`,
`/api/config?host=` + `/api/config-save` (podgląd/edycja `config.py`), `/api/pull-log`.

> **Zasada:** skrypty testowe URUCHAMIA SIĘ Z LAUNCHERA. Nie odpalać/ubijać ręcznie
> legacy `*_v38.py`.

### 3.2 Fallback — ręcznie przez SSH + tmux

Gdy launcher niedostępny (odzwierciedla dokładnie to, co robi launcher):

```bash
ssh td@100.98.155.78         # carport-g2
cd ~/meshtastic
# start w tmux (odłączony), log stderr do pliku:
tmux new -d -s gw 'venv/bin/python -u test_step5_anomaly.py 2>>/tmp/test_transport.stderr.log'
tmux has-session -t gw && echo UP     # weryfikacja
tail -f /tmp/test_transport.log       # podgląd logu

# stop:
tmux kill-session -t gw ; pkill -f 'python -u test_step5_anomaly.py'
```

Sesje tmux: **`gw`** (bramka) / **`sup`** (supervisor). Log runtime dla skryptów
`test_*`: `/tmp/test_transport.log` (+ `.stderr.log`).

### 3.3 Wdrożenie / aktualizacja kodu

Kod żyje w `~/meshtastic/` na maszynie. Aktualizacja (przykład, z maszyny dev):
```bash
scp skrypty/modules/protocol/discovery.py td@100.98.155.78:meshtastic/modules/protocol/discovery.py
ssh td@100.98.155.78 'cd ~/meshtastic && venv/bin/python -c "import ast; ast.parse(open(\"modules/protocol/discovery.py\").read()); print(\"ast OK\")"'
# po deployu: restart roli z launchera (wczytuje nowy kod)
```

> **NIE deployować `config.py`** przez scp z repo — remote ma prawdziwe, per-maszynowe
> wartości (tokeny, listy `monitored`/`priority`, porty, `params_preset`). Edytować remote
> in-place (sed/ssh) albo przez `/api/config-save`.

**Deploy całości jednym strzałem** (gdy host bywa osiągalny tylko chwilami — G1):
```bash
bash deploy/deploy_g1.sh              # domyślnie 100.104.38.14
bash deploy/deploy_g1.sh 100.98.155.78   # ten sam mechanizm na G2
```
Kolejność jest celowa: backup na maszynie → kopia plików → **walidacja AST NA MASZYNIE**
→ dopiero restart. Skrypt świadomie nie dotyka `config.py` ani stanów w `/tmp`.
Po deployu sprawdź na supervisorze: HB z polem `dh` oraz spadek liczby ramek `b`.

---

## 4. Wdrożenie dashboardu

Dashboard Lovelace generowany skryptem — uruchom **na danej maszynie** (osobno GW i SUP,
każdy generuje swój wariant wg `ROLE`):

```bash
ssh td@100.98.155.78 'cd ~/meshtastic && venv/bin/python homeassistant/gen_unified_dashboard.py'
```

Tworzy dashboard `url_path = lora-scada` (tytuł „LoRa SCADA"), tryb storage.
Encje pochodzą z MQTT Discovery (retained w brokerze) — dashboard tylko je układa.

---

## 5. Obsługa dashboardu — 7 zakładek

Ten sam układ na obu maszynach (na bramce = wzorzec, supervisor 1:1 z podmianą encji).

| Zakładka | `path` | Do czego |
|----------|--------|----------|
| **Bramka {GW}** | `scada-gw` | widok-wzorzec bramki (stan, urządzenia) |
| **Sterowanie** | `scada-ctl` | switche/światła, przyciski sterujące |
| **Pomiary** | `scada-meas` | kafle temp/wilgotność/bateria + wykresy |
| **Alarmy** | `scada-alarm` | lista anomalii + Clear (offline/bateria/inne) |
| **Harmonogram** | `scada-cal` | kalendarz produkcji + tryb bramki |
| **Parametry** | `scada-param` | P/T/Progi + przyciski Send |
| **Diagnostyka** | `scada-diag` | HB, czas, hashe zgodności, link supervisora |

### 5.1 Pomiary (temp/wilgotność/bateria)

- Kafle: `sensor.lora_<gw>_<dev>_temperature` / `_humidity` / `_battery`,
  `binary_sensor.lora_<gw>_<dev>_water_leak` / `_contact`, `_last_seen`, `_available`.
- **Odświeżanie — polityka raportowania (ważne przy ocenie „czy to nie zamarzło"):**
  - **Pomiarowe** (temperatura/wilgotność/bateria) — CYKLICZNIE co P3 min (domyślnie 15),
    niezależnie od progów, plus zdarzeniowo przy zmianie (≥0,5 °C / ≥2 %). Odczyt analogowy
    się starzeje, więc musi przychodzić sam z siebie.
  - **Binarne** (przekaźniki, drzwi, zalanie, ruch) — **WYŁĄCZNIE zdarzeniowo**. Stan binarny
    się nie starzeje; jego powtarzanie było mierzalnie 93 % pasma G1. To, że przekaźnik
    „milczy", jest poprawnym zachowaniem, nie awarią.
  - Że urządzenie ŻYJE, niesie osobny kanał: blob `avail` (availability + LQI) i hash `dh`.
    Padnięcie całej bramki nadal wykrywają timeouty T1/T2/T3.
  - `last_seen` = ostatni realny raport (uczciwy wskaźnik świeżości).
- **Refresh ręczny**: `button.lora_<gw>_<dev>_refresh` → prosi bramkę o natychmiastową
  wartość (przez LoRa).
- Kafle mają `force_update` → nie „zamarzają" nawet przy stałej wartości.

### 5.2 Harmonogram — tryb bramki (sterowalny z OBU stron, synchronizowany)

Dropdown **„Tryb bramki"** (`select.lora_<gw>_tryb_bramki`) — opcje:

| Etykieta | Tryb | Znaczenie |
|----------|------|-----------|
| **Całodobowa** | `all-time` | zawsze aktywna |
| **Dzienna** | `day` | aktywna w dzień (świt–zmierzch solarnie) |
| **Nocna** | `night` | aktywna w nocy |
| **Kalendarz** | `calendar` | aktywność wg harmonogramu produkcji |

- Zmiana działa **i na bramce, i na supervisorze** — round-trip przez LoRa
  (`set_mode`) + potwierdzenie w HB (`gm`). Oba selecty odbijają realny tryb bramki.
- **Uwaga**: monitorowane + priority (temp/hum/leak/door) raportują ZAWSZE, także gdy
  bramka „nieaktywna" — tłumione są tylko urządzenia oświetleniowe (non-monitored),
  by nie zgłaszać fałszywego offline.

### 5.3 Parametry — edytuj i „Wyślij"

Model: **edytuj pole → naciśnij przycisk Send** (jak v38, nie auto-push). Silniki
czytają wartość lokalnie od razu; do supervisora idzie po Send. Bramka = master.

| Grupa (przycisk) | Parametry |
|------------------|-----------|
| **Wyślij Config** | P1, P2, P3, P4 |
| **Wyślij Timeout** | T1, T2, T3, T4 |
| **Wyślij Progi** | TH, TL, HH, HL, BL, BC, PR |

Referencja (encje `number.lora_<gw>_param_*`, domyślne):

| Klucz | Nazwa | Domyśl. | Jedn. |
|-------|-------|---------|-------|
| P1 | Stagnation bateryjne | 48 | h |
| P2 | Stagnation sieciowe | 24 | h |
| P3 | Raportowanie temp | 15 | min |
| P4 | Ping sup interwał | 15 | min |
| T1 | Offline switch/light | 30 | min |
| T2 | Offline temp/hum | 60 | min |
| T3 | Offline door/leak/motion | 120 | min |
| T4 | Ping sup timeout | 60 | s |
| TH / TL | Próg temp. wysoka / niska | 35 / 10 | °C |
| HH / HL | Próg wilg. wysoka / niska | 70 / 20 | % |
| BL / BC | Próg bateria niska / krytyczna | 25 / 15 | % |
| PR | Ping sup retry | 2 | x |

> Zgodność parametrów widać na zakładce Diagnostyka (mirror ↔ master). Rozjazd
> (np. zgubiony pakiet LoRa) **leczy się sam** — supervisor dociąga wartości od
> bramki (auto-heal), o ile nie masz niewysłanej lokalnej edycji na supervisorze.

> **Wartości startowe nowej bramki** bierze się z `params_preset` w `config.py` tej maszyny
> (§10). Preset działa TYLKO gdy nie ma jeszcze `/tmp/lora_params.json` — Twoje edycje
> z dashboardu zawsze wygrywają i restart ich nie cofa.
> **T4 musi być dłuższy niż realny RTT łącza** (sup→gw bywa 16-72 s przy ruchu, bo jedna
> antena obsługuje wszystkie bramki). Timeout krótszy niż RTT nie wykrywa awarii — tworzy ją.

### 5.4 Alarmy — anomalie i Clear

- Kategorie: offline, bateria (low/critical), temp/hum (progi), stagnacja, wyciek/dym.
- Encje zbiorcze: `sensor.lora_<gw>_an_offline` / `_an_battery` / `_an_other`
  (stan = liczba, atrybut `items` = lista).
- **Clear**: przyciski per-kategoria (`clear_offline` / `clear_battery` / `clear_other`)
  oraz per-wpis (popup). Clear jest ack-owany; realnie-offline urządzenie wróci na
  listę dopiero po recovery (nie „odżywa" przez reconcile).

### 5.5 Diagnostyka

HB (uptime, liczba urządzeń mon/pri), jakość czasu (`tq`), pora dnia, link do
supervisora, oraz **hashe zgodności** — pięć równorzędnych flag („zgodny ✅ / ROZJAZD ⚠️"):

| Flaga | Czego pilnuje | Co robi system przy rozjeździe |
|-------|---------------|--------------------------------|
| DISC | mapa urządzeń (nazwa↔sid) | dociąga `devmap` |
| PARAM | parametry P/T/Progi | auto-heal (pull od mastera) |
| KALENDARZ | harmonogram produkcji | push/pull na przycisk |
| ANOMALIE | lista aktywnych anomalii | `dump_anom` → blob `ansnap` |
| **DANE** | stan urządzeń (`dh`) | dociąga pełny stan blobem `dsnap` |

Flaga DANE ma sens dopiero odkąd binarne raportują tylko zdarzeniowo: skoro nie ma
periodyku, to hash — a nie „coś w końcu przyjdzie" — pilnuje, żeby lustro supervisora
nie rozjechało się po cichu.

**Kafel czasu.** „Offset" to różnica zegarów PO odjęciu połowy czasu obiegu ping→pong
(obok pokazany jest surowy RTT). Bez tej korekty kafel pokazywał całe opóźnienie LoRa
(potrafiło być +21 s) i wyglądało to jak dryf zegara, którym nie było — obie maszyny
mają NTP. Uczciwe zastrzeżenie: łącze jest asymetryczne, więc RTT/2 to przybliżenie;
gdy chcesz mieć pewność co do zegarów, patrz na `tq` (jakość czasu), nie na offset.

---

## 6. Health-check (szybkie komendy)

```bash
# proces żyje?
ssh td@100.98.155.78 'pgrep -af "python -u test_step5_anomaly.py" | grep -v tmux'

# ostatni HB (tryb gm, aktywność ga, hashe):
ssh td@100.98.155.78 'grep -a "\"t\":\"hb\"" /tmp/test_transport.log | tail -1'

# HA API odpowiada? (token bramki):
ssh td@100.98.155.78 'curl -s -H "Authorization: Bearer $HA_TOKEN" http://localhost:8123/api/ ' # w skrypcie
```

---

## 7. Troubleshooting

| Objaw | Prawdopodobna przyczyna | Działanie |
|-------|-------------------------|-----------|
| Kafle temp/leak/door **zamrożone**, dane nie idą | bramka w trybie nieaktywnym (`ga=0`) lub proces stanął | sprawdź HB (`gm`/`ga`); ustaw tryb `Całodobowa` lub sprawdź czy proces żyje |
| Kafel „świeży czas, stara wartość" | pakiet `b` zgubiony na LoRa; kafel podtrzymany przez avail-blob | poczekaj na kolejny cykl (15 min) lub naciśnij **Refresh**; `last_seen` mówi prawdę |
| Encja zamrożona mimo świeżego state topicu | brak `force_update` w configu encji / stale-registry HA | re-generuj dashboard / republish configu encji; ostatecznie restart HA MQTT |
| Urządzenie „offline" mimo że żyje | timeout per-typ (T1/T2/T3) za krótki dla bateryjnego | podnieś T2/T3 (Parametry → Wyślij Timeout) |
| `ROZJAZD ⚠️` parametrów utrzymuje się | zgubiony `param_upd`; masz niewysłaną edycję na supervisorze | naciśnij **Send** (na bramce = master) — auto-heal ruszy po chwili |
| MCP `ha-gw`/`ha-sup` = **401** | token wygasł/zrewokowany | zaktualizuj `HA_TOKEN` w `config.py` (obie maszyny) + `~/.claude.json`; restart roli + **restart Claude Code** |
| Sidy „przesunięte" po restarcie | (naprawione) trwała mapa `/tmp/lora_sid_map_<gw>.json` | jeśli mapa uszkodzona: usuń plik → odbuduje się (lock-in = sorted) |
| Launcher: endpointy timeoutują / puste | za dużo osieroconych sesji SSH (Windows) | `Get-Process ssh | Measure` — jeśli dużo, restart launchera; karty tail dławią sshd — zamykaj je |
| Antena znika (`ttyUSB` / `Timed out`) | CP210x urb stall / fizyka | watchdog anteny auto-reset; ostatecznie replug/power-cycle Helteca |

---

## 8. Persistencja (pliki stanu na maszynie)

| Plik | Zawartość |
|------|-----------|
| `/tmp/lora_params.json` | parametry P/T/Progi (bramka = master) |
| `/tmp/lora_params_sup_<gw>.json` | mirror parametrów per-bramka (supervisor) |
| `/tmp/lora_mode_gw.json` | zapamiętany tryb bramki (ładowany ponad `config.py`) |
| `/tmp/lora_sid_map_<gw>.json` | **trwała mapa nazwa→sid** (stabilne sidy) |
| `/tmp/lora_anomaly_ids.json` | aktywne anomalie |
| `/tmp/lora_schedule_gw.json` | harmonogram (kalendarz) |
| `/tmp/test_transport.log` | log runtime skryptu `test_*` |

---

## 9. Pierwsze uruchomienie nowej bramki (checklista wdrożeniowa)

Zakłada gotowy sprzęt: Debian + Home Assistant Supervised + Zigbee2MQTT + Heltec V3 na USB.
Kolejność jest istotna — każdy krok zdejmuje jedną klasę „dlaczego to nie działa".

1. **Broker MQTT** — user `mqtt` z hasłem; broker widoczny pod `172.17.0.1:1883`
   (adres hosta z kontenera HA). Sprawdź `mosquitto_sub -t 'zigbee2mqtt/#' -C 1`.
2. **Availability w Zigbee2MQTT** — MUSI być włączone, inaczej bramka nie odróżni
   „urządzenie milczy, bo się nie zmieniło" od „urządzenie padło".
   ⚠️ z2m 2.x **ignoruje** `availability: enabled` w `configuration.yaml`. Włącza się to
   przez broker i dopiero wtedy jest trwałe:
   ```
   topic:   zigbee2mqtt/bridge/request/options
   payload: {"options":{"availability":{"enabled":true,
             "active":{"timeout":10},"passive":{"timeout":1440}}}}
   ```
   potem restart z2m.
3. **Token HA** (long-lived) tej maszyny → `HA_TOKEN_GW` w `config.py`.
4. **Kalendarz** — w HA bramki musi istnieć encja `calendar.lora_<gw>` (integracja
   Local Calendar). Nazwa wylicza się z `id` bramki, nie ustawia się jej ręcznie.
   Reload kalendarza idzie przez `config_entries`; przy odmowie dostępu do
   `.storage` potrzebny bywa jednorazowy chmod (znany wyjątek, opisany w historii projektu).
5. **`config.py` tej maszyny** — ustaw TYLKO: `id`, `mesh_port` (ścieżka `by-id`, nie
   `ttyUSB*` — nie renumeruje się po replug), `monitored`, `priority_devices`, sekrety
   oraz **`params_preset`** (§5.3). Reszta pliku jest wspólna.
   `monitored` to nie „lista wszystkiego" — urządzenia spoza list nadal mają anomalie
   (offline/bateria/stagnacja), ale nie batchują danych. Na site'cie z ~200 urządzeń
   wrzucenie wszystkich do `monitored` zapycha LoRa i wyłącza sens trybów bramki.
6. **Kod** — `bash deploy/deploy_g1.sh <ip>` (§3.3). Skrypt sam waliduje składnię na
   maszynie przed restartem.
7. **Start i weryfikacja** — z launchera (§3.1), potem na supervisorze sprawdź:
   HB od nowej bramki, `devmap` (mapa urządzeń), pięć flag zgodności na Diagnostyce.

---

## 10. Zasady bezpieczeństwa operacyjnego

- **Bramka = źródło prawdy.** Dashboard supervisora = odwzorowanie bramki.
- Testuj na **G2 (testbed)**, nie na produkcyjnym **G1**, dopóki nie potwierdzone.
- Restart supervisora jest bezpieczny (bramki autonomiczne); restart bramki = krótka
  utrata monitoringu tej lokalizacji.
- Nie zmieniać stylu/mechanik dashboardu ad-hoc — trzymać się wzorca.
