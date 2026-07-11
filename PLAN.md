# PLAN — Audyt + optymalizacja pasma LoRa (bez psucia działających funkcji)

## Kontekst (przeczytane przed planowaniem)
Repo ma JUŻ wdrożone niemal wszystkie standardowe techniki oszczędzania pasma LoRa:
delta encoding z progami (temp 0.5°C/hum 2%), PriorityTxQueue z anti-starvation aging,
ChannelArbiter serializujący transfery, kompresję zlib+base64 dla bulk (devmap/avail/
kalendarz), hash-gated anti-spam (disc_meta/devmap nie re-wysyłane bez zmiany hash),
per-typ timeouty offline (T1/T2/T3), ride-along LQI, throttled heartbeat (15 min),
listen-gap po chunku żeby usłyszeć cack, backpressure-safe TX (fix 2026-07-11).

**Working tree ma dużo NIEZACOMMITOWANYCH zmian** (git status: ~30 plików M, kilkanaście
`??`). To jest ŻYWY, świeży refaktor (step4/step5). NIE dotykamy nic poza dokładnie
wskazanymi liniami niżej — reszta to sprawdzona/wdrażana na żywo logika (arbiter,
backpressure, aging, listen_gap), zmieniana wyłącznie po testach na fizycznym sprzęcie
(zasada projektu: "nie zgadywać, testować LIVE").

## Finding #1 (GŁÓWNY, do naprawy) — niespójny chunk_size na kanale `rt`

Plik: `skrypty/test_step5_anomaly.py`

- Linia ~500/1168: `CalendarTransfer(chunk_size=cal_cfg.get('chunk_size', 140), ...)` —
  realna wartość z `config.py` (`calendar.chunk_size`) to **60**. Fallback 140 to martwy
  kod (nigdy nie trafiony, bo config zawsze podaje 60), ale MYLĄCY.
- Linia ~510-512 (strona GATEWAY): generyczny kanał bulk
  `rt = ReliableTransfer(send_fn=lora_send, logger=log, chunk_ack_timeout=30.0,
  chunk_size=140, chunk_retries=3, arbiter=arbiter, accept_gw=gw_id)` — **chunk_size
  zahardkodowany na 140**, z komentarzem "align z cal" — ale cal realnie ma 60, więc
  komentarz kłamie, a wartość jest 2.3× większa niż empirycznie potwierdzony próg
  niezawodności tego łącza (komentarz w `config.py`: `"chunk_size": 60, # ≤90B =
  niezawodny próg tego łącza LoRa (lekcja RF)"`). Ten kanał `rt` obsługuje **devmap**
  (mapa do 199 urządzeń) i **avail blob** (pełny sweep dostępności) — dwa
  NAJCIĘŻSZE okresowe transfery w systemie. Zbyt duże chunki na stratnym RF = więcej
  retransmisji = WIĘCEJ zużytego pasma, nie mniej.
- Linia ~1206 (strona SUPERVISOR): `rt = ReliableTransfer(send_fn=send_to_all,
  on_received=on_rt_received, logger=log, arbiter=arbiter, accept_gw=None)` — chunk_size
  NIE podany → class default z `reliable_transfer.py` = **90**. Asymetria: supervisor
  (90) i gateway (140) używają RÓŻNYCH rozmiarów chunka na tym samym kanale protokołu
  (rt_begin/rt_chunk/rt_end są symetryczne, nadawca dowolnej strony powinien używać tej
  samej, sprawdzonej wartości).

### Naprawa (bezpieczna, config-level, ZERO zmian logiki protokołu)
1. W `config.py` dodaj do `GATEWAY_CONFIG` i `SUPERVISOR_CONFIG` nową sekcję (obok
   `"calendar"`), np.:
   ```python
   "transfer": {
       "chunk_size": 60,   # ten sam empirycznie sprawdzony próg co calendar.chunk_size
   },
   ```
2. W `test_step5_anomaly.py` w OBU miejscach konstrukcji generycznego `rt`
   (gateway ~linia 510 i supervisor ~linia 1206) czytaj `CONFIG.get('transfer', {}).get(
   'chunk_size', 60)` zamiast hardkodowanego `140` / braku parametru.
3. Popraw fallback w obu `CalendarTransfer(..., chunk_size=cal_cfg.get('chunk_size', 140))`
   → fallback na `60` (kosmetyka, usuwa mylący martwy default — realna wartość z configu
   się nie zmienia, bo config i tak ją nadpisuje).
4. NIE zmieniaj `chunk_delay`, `chunk_ack_timeout`, `chunk_retries`, `arbiter`, `accept_gw`
   — te wartości zostają jak są (już dostrojone).
5. Po zmianie: `python3 -c "import ast; ast.parse(open('skrypty/test_step5_anomaly.py').read())"`
   i `python3 -c "import ast; ast.parse(open('skrypty/config.py').read())"` (reguła
   projektu z CLAUDE.md: `ast.parse()` po każdej edycji).
6. NIE URUCHAMIAJ testów na żywym sprzęcie/porcie LoRa — to sesja offline (kod, nie
   live radio). Nie wołaj `test_step5_anomaly.py` jako procesu ani nic co otwiera
   `/dev/serial/...` lub łączy się po SSH/Tailscale do bramki/supervisora.

## Finding #2 (drugorzędny, config-only, do rozważenia — NIE wdrażaj automatycznie)
`GatewayData(report_full_every=data_cfg.get('report_full_every', 1))` w
`test_step5_anomaly.py` czyta `data.report_full_every` z configu, ale `config.py` w
`GATEWAY_CONFIG["data"]` NIE ma tego klucza → efektywnie zawsze `1` (pełny sweep co
15 min, bez korzyści z już zaimplementowanej redukcji). Kod w `gateway_data.py` sam
dokumentuje, że `report_full_every=2` przy `report_interval=900` daje pełny sweep co
30 min i jest bezpieczne (mieści się w backstopie supervisora T2=60min), redukując
ruch ~45%. To realna, ale WYMAGA decyzji usera (zmiana zachowania na żywym systemie,
nie tylko refaktor) — NIE dodawaj tego klucza do config.py w tym przebiegu. Zostaw
jako rekomendację w podsumowaniu.

## Zakres dla Sola (GPT-5.6) — WYŁĄCZNIE to:
- `config.py`: dodaj sekcję `"transfer": {"chunk_size": 60}` do `GATEWAY_CONFIG` i
  `SUPERVISOR_CONFIG`.
- `skrypty/test_step5_anomaly.py`: podmień 4 miejsca opisane w Finding #1 (2×
  `chunk_size=140`→config-driven `60`, 1× brak param→config-driven, 2× fallback
  `140`→`60` w wywołaniach `CalendarTransfer`).
- Zero innych plików, zero zmian w `lora_transport.py`, `channel.py`, `tx_queue.py`,
  `reliable_transfer.py`, `batcher.py`, `gateway_data.py`.
- Po edycji uruchom WYŁĄCZNIE `ast.parse()` na obu plikach (offline, bez sprzętu).
- Nie commituj (git) — zostaw zmiany w working tree, tak jak reszta obecnego WIP.
