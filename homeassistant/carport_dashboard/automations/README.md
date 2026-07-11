# Carport — automatyzacje motion (przeprojektowane wg spec)

Pliki do RĘCZNEGO przeniesienia (per automatyzacja). Każda zachowuje oryginalne listy
czujek (triggery) i przekaźników (akcje) 1:1 — zmieniona tylko logika sterująca.

## Logika docelowa (uzgodnione)
- **Dowolna czujka w strefie → cała strefa się zapala** (akcja `choose`: jakakolwiek occupancy=on → włącz wszystkie przekaźniki strefy).
- **Brak ruchu przez timer → strefa gaśnie** (trigger OFF z `for` = `input_number.motion_timeout`).
- **Jeden timer dla wszystkich stref**: `input_number.motion_timeout` (sekundy, domyślnie 150 = 2,5 min). Zmieniasz w JEDNYM miejscu → działa we wszystkich.
- **Tylko w nocy**: warunek `sun.sun = below_horizon` (w dzień strefa bez zasilania → motion nie działa).
- **Spięcie z harmonogramem**: bramka `input_boolean.motion_enabled`.
  - `Schedule ON` (światło on) → ustawia `motion_enabled = on`.
  - `Schedule OFF` (światło off, 22:15) → ustawia `motion_enabled = off` → motion przestaje reagować aż do następnego `Schedule ON`.

Każda motion ma więc warunki: `sun below_horizon` **i** `motion_enabled = on`. Oba muszą być spełnione.

> ⚠️ SEMANTYKA HARMONOGRAMU (zweryfikowane na żywo): event `PRODUKCJA` = 22:15→05:45.
> `Schedule ON` = trigger `calendar.event_started` (22:15) → **GASI światło** (`switch.turn_off`) → `motion_enabled = OFF`.
> `Schedule OFF` = `calendar.event_ended` (05:45) → **ZAPALA światło** → `motion_enabled = ON`.
> Czyli motion działa wieczorem między zmierzchem a 22:15; po 22:15 (światło zgaszone) motion OFF. Zgodnie ze spec.

## Pliki
| Plik | Co |
|---|---|
| `helpers.yaml` | `input_number.motion_timeout` + `input_boolean.motion_enabled` |
| `motion_ra7_ra8_ra9.yaml` | strefa RA7-9 (6 przekaźników) |
| `motion_ra10_ra11_ra12.yaml` | strefa RA10-12 |
| `motion_rb4_rb5_rb6_rb7.yaml` | strefa RB4-7 |
| `motion_rb8_rb9_rb10_rb11.yaml` | strefa RB8-11 |
| `motion_ra13_ra14_ra15_toyota.yaml` | strefa Toyota |
| `schedule_on.yaml` | +akcja `motion_enabled = on` |
| `schedule_off.yaml` | +akcja `motion_enabled = off` |

## Jak wdrożyć
1. **Helpery** — dodaj `helpers.yaml` do `configuration.yaml` (albo utwórz w UI: Ustawienia → Urządzenia i usługi → Helpery → input_number „motion_timeout", input_boolean „motion_enabled"). Restart/Reload.
2. **Automatyzacje** — dla każdej: Ustawienia → Automatyzacje → wybierz → ⋮ → „Edytuj w YAML" → wklej zawartość pliku → Zapisz. (Albo wklej do `automations.yaml` po `id`.)
3. Ustaw `input_number.motion_timeout` (domyślnie 150 s) i sprawdź, że `input_boolean.motion_enabled` przełącza się z harmonogramem.

## Uwagi / do potwierdzenia na żywo
- `for` używa szablonu: `for: {seconds: "{{ states('input_number.motion_timeout')|int(150) }}"}` — zmiana wartości działa od NASTĘPNEGO wyzwolenia triggera (HA liczy `for` w momencie startu odliczania).
- Test pełny wymaga uruchomionego z2m (czujki muszą raportować). Przy z2m off encje occupancy są niedostępne.
- Jeśli strefa ma być aktywna także bez PRODUKCJA w kalendarzu — `motion_enabled` trzeba włączać innym wyzwalaczem (np. o zmierzchu), powiedz to wtedy dorobimy.
