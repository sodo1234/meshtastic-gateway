# Naprawa popupu anomalii (odswiezanie na zywo) — 2026-10-10

PRZYCZYNA: kafel (button-card) ewaluuje WSZYSTKIE szablony `[[[ ]]]` zagniezdzone w `tap_action` w chwili kliknieca,
wiec karty w popupie browser_mod dostaja statyczny HTML — popup nie odswieza sie po skasowaniu/dodaniu anomalii
(backend aktualizuje atrybut `items` w <1 s). Szablony przez `button_card_templates` NIE dzialaja w popupie (Blad konfiguracji).

FIX: kazdy zagniezdzony szablon S w tresci popupu opakowany jako `[[[ return "<S json>"; ]]]` — kafel zwraca S
nieewaluowane, ewaluuje je dopiero karta w popupie (na zywo). Zastosowane: sup `lora-scada`, `lora-scada-g1`, G2 `lora-scada`.
DO ZROBIENIA gdy G1 osiagalny (poniedzialek): ten sam skrypt na HA G1 (`lora-scada`).

Uzycie (na maszynie z HA, w ~/meshtastic, venv): `venv/bin/python fix_popups_live.py <url_path> apply`
(bez `apply` = tylko statystyki). Kopia zapasowa `/tmp/lovelace_backup_<url_path>_<ts>.json`; przywrocenie:
`venv/bin/python restore_dashboard.py <url_path> <plik_backup>`. Od 2026-10-21 skrypt jest idempotentny (pomija szablony juz owiniete);
`fix_popups_live.py all [apply]` = wszystkie dashboardy tego HA. Stan 10-21: G1, G2, sup (lora-scada, lora-scada-g1) — 16 popupow, wszystkie owiniete.
