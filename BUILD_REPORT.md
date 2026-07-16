# BUILD_REPORT — F2/F3/F5/F7 (2026-07-16)

Wykonanie: pętla /route — worker Sonnet (moduły + strona bramki; padł na limicie sesji
przy stronie supervisora) + Fable (dokończenie strony supervisora + review). Codex CLI
2× niedostępny (read-only sandbox / osierocony proces) — udokumentowane w sesji.

## Zmiany per plik
- `skrypty/modules/params/manager.py` — F3: P4/T4/PR w PARAM_DEFS/PARAM_ORDER i grupach
  Config/Timeout/Progi (bez nowych grup); `self.tp` (gateway bez zmian, supervisor →
  `supervisor/<gl>`); de-hardcode uid `lora_sup<gl>_param_*` (G1 = identyczne jak dotąd).
- `skrypty/modules/protocol/supervisor_link.py` (NOWY) — F2: SupervisorLinkProbe
  (sup_ping/sup_pong, interwał P4 min / timeout T4 s / retry PR czytane live, note_rx,
  licznik lost_pongs, on_state callback).
- `skrypty/modules/protocol/discovery.py` — F7: anti-spam disc_meta+disc_vio (180 s ten sam
  hash, `force=True` bypass dla jawnego `disc`/przycisku); SupervisorDiscovery
  `persist_path` (mapa+synced hash przeżywa restart → koniec startowego szturmu devmap).
- `skrypty/modules/protocol/ha_entities.py` — przyciski bramki `dump`+`sync_req`
  (reg_gw_buttons_local) + sensor `sup_lost_pong` (reg_gw_local_stats).
- `skrypty/modules/protocol/__init__.py` — export SupervisorLinkProbe.
- `skrypty/test_step5_anomaly.py`:
  - bramka: probe F2 (start, sup_pong dispatcher, note_rx przy każdym RX od sup, pola
    sup_link/sup_lost_pong w gwstat), przyciski F5 `cmd/dump`→handle_dump_anom i
    `cmd/sync_req`→lora_send sync_req, `force=True` przy jawnym disc.
  - supervisor: F3 lazy `get_sup_params(gw)` (persist `/tmp/lora_params_sup_<gl>.json`,
    encje+subscribe przy 1. kontakcie, params_req TYLKO dla świeżego persistu), routing
    param_upd/MQTT per-gw, ph-drift per instancja; F2 handler `sup_ping`→`sup_pong`;
    F5 handler `sync_req`→targeted `sync`; F7 cichy start (disc tylko dla bramek bez
    mapy w persist).
- `skrypty/modules/params/smoke_test.py` + `skrypty/modules/protocol/link_smoke_test.py`
  (NOWY) — testy F2/F3/F7.

## Testy
- ast.parse: OK dla wszystkich dotkniętych plików.
- `python -m modules.params.smoke_test` → **14/14 passed** (m.in. 2 instancje sup G1/G2:
  różne uid/topic/persist, izolacja stanu; gateway topics niezmienione).
- `python -m modules.protocol.link_smoke_test` → **3/3 passed** (ping→pong online,
  timeout×(retries+1)→offline+licznik, note_rx; anti-spam disc 180 s + force bypass).
- Zero referencji do usuniętego pojedynczego `param_sync` w run_supervisor.

## Wire (nowe pakiety)
`{"t":"sup_ping","g":"G2"}` / `{"t":"sup_pong","g":"G2"}` / `{"t":"sync_req","g":"G2"}`.

## Uwagi / migracja
- Stary persist `/tmp/lora_params_sup.json` osierocony — nowe pliki per-gw; świeży mirror
  robi jednorazowy pull (params_req) od bramki.
- HB już niósł `ah`/`oh` — NIE dublowano hashy anomalii (zakres T3.3 ograniczony zgodnie
  z planem do gate'owania params/disc).
- Deploy na maszyny + test live: PO testach clear anomalii (osobny krok).
