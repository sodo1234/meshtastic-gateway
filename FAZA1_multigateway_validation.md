# FAZA 1 — Walidacja multi-gateway (PRZED sprzedażą)

> **Cel:** udowodnić, że architektura wielowęzłowa (slotowanie + N anten + anti-collision)
> DZIAŁA naprawdę, a nie tylko w teorii na G1. To bloker sprzedaży #1.
> **Status kodu (2026-06-21):** logika slotowania ready (`SlotScheduler`), deployment NIE.

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
