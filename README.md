# Meshtastic Gateway — LoRa Zigbee SCADA

Rozproszony BMS dla hal produkcyjnych i carportu: czujniki/przekaźniki Zigbee (Z2M) →
bramki LoRa (Meshtastic, Heltec V3, 868 MHz) → supervisor → dashboardy Home Assistant.

**Wersja: v12.11 — wersja końcowa (2026-10-14, commit `b2f8673`, branch `step-04-calendar`).**
Supervisor i G2 = v12.11, G1 (carport) = v12.10 (różnica dotyczy tylko kodu supervisora).

| Maszyna | Rola | Adres (Tailscale) |
|---------|------|-------------------|
| G1 | bramka produkcyjna, carport, bez internetu (offline-first) | `100.68.79.100` |
| G2 | bramka testbed | `100.98.155.78` |
| G0 | supervisor | `100.79.111.24` |

## Kod

- `skrypty/Meshtastic_SCADA.py` + `skrypty/modules/` — jeden program w dwóch rolach
  (`gateway` / `supervisor`, wg `config.py` maszyny). Legacy `*_v38.py` jest martwe.
- `deploy/deploy_g1.sh <IP> <gw|sup>` — wgrywanie (backup, walidacja, smoke testy, bez `config.py`).
- `launcher/app.py` — start/stop na maszynach; po restarcie PC uruchomić ręcznie
  (`python launcher/app.py`, http://127.0.0.1:8765).
- `tools/e2e_g2.py`, `tools/dash_audit.py`, `tools/bench_g2.py` — testy na żywo (G2 + supervisor).

## Dokumentacja

- [docs/RUNBOOK.md](docs/RUNBOOK.md) — runbook operacyjny v12.11 (uruchamianie, wgrywanie,
  dashboard, synchronizacja, benchmark, ograniczenia).
- [skrypty/docs](skrypty/docs) — specyfikacja protokołu.
- `docs/gen_wdrozenie_deck.py` — generator instrukcji wdrożenia (PPTX) do paczki odtworzeniowej
  `Desktop\LoRa-SCADA-ODTWORZENIE`.

## License

Private — proprietary domain logic.
