"""
Centralny config modularnego systemu LoRa SCADA (refaktor).
JEDYNE źródło konfiguracji — moduły i test_stepN_*.py robią `from config import CONFIG`.
Edytowalny zdalnie przez Launcher (/api/config → ~/meshtastic/config.py).

Rola: plik `.role` (gateway|supervisor) albo env LORA_ROLE.

SEKRETY: pola MQTT_PASS / HA_TOKEN poniżej — ustawiane PER-MASZYNA (przez Launcher).
W repo zostają puste; prawdziwych wartości NIE commitujemy. (v38 NIE jest już używane.)
"""
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _detect_role():
    env = os.environ.get('LORA_ROLE', '').strip().lower()
    if env in ('gateway', 'supervisor'):
        return env
    role_file = os.path.join(SCRIPT_DIR, '.role')
    if os.path.exists(role_file):
        try:
            r = open(role_file).read().strip().lower()
            if r in ('gateway', 'supervisor'):
                return r
        except Exception:
            pass
    return 'gateway'                              # domyślnie; ustaw `.role` lub LORA_ROLE


ROLE = _detect_role()

# ═══════════════════════════════════════════════════════════
# SEKRETY — INLINE (JEDEN plik konfiguracyjny: loginy, hasła, tokeny, reszta niżej)
# ═══════════════════════════════════════════════════════════
# Wersja ROBOCZA na tej maszynie i na hostach trzyma PRAWDZIWE wartości tutaj.
# W git trafia tylko PLACEHOLDER — pilnuje tego git clean filter (`_cfg_scrub.py` +
# .gitattributes: `config.py filter=scrubcfg`), który przy `git add` zeruje te 2 linie.
# Deploy (`deploy_config.sh`) pushuje wersję roboczą (z sekretami) na bramkę i supervisora.
# Zmiana wartości = JEDNO miejsce: ten plik (albo launcher → /api/config). env nadpisuje.
MQTT_PASS    = ""   # hasło brokera MQTT (oba hosty)        — PLACEHOLDER w git
HA_TOKEN_GW  = ""   # token HA bramki G1 — PLACEHOLDER w git
HA_TOKEN_SUP = ""   # token HA supervisora G0 — PLACEHOLDER w git
HA_URL       = "http://localhost:8123"

MQTT_PASS    = os.environ.get("MQTT_PASS", MQTT_PASS)
HA_TOKEN_GW  = os.environ.get("HA_TOKEN_GW", HA_TOKEN_GW)
HA_TOKEN_SUP = os.environ.get("HA_TOKEN_SUP", HA_TOKEN_SUP)
HA_URL       = os.environ.get("HA_URL", HA_URL)
# token HA wg roli TEJ maszyny — GATEWAY/SUPERVISOR_CONFIG.ha_api używają HA_TOKEN
HA_TOKEN = HA_TOKEN_GW if ROLE == 'gateway' else HA_TOKEN_SUP

# Tryby produkcji (wspólne dla obu ról) — _detect_mode w ScheduleManager sortuje po długości.
_MODE_NAMES = {0: "BRAK PRODUKCJI", 1: "PRODUKCJA", 2: "PRZERWA", 3: "SERWIS"}

# Stabilna ścieżka portu LoRa (Heltec CP2102). by-id = nie renumeruje się po reconnect USB
# (w przeciwieństwie do /dev/ttyUSBx). Bramka i supervisor (ANT-1→G1) dzielą ten sam adapter.
_MESH_PORT = "/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0"

# ── Transport anteny: USB vs TCP (serial-over-IP) — UNIWERSALNIE, wybór per antena ──
#   USB (domyślnie):  "port": "/dev/serial/by-id/..."  (albo /dev/ttyUSBx)
#   TCP (sieć):       "port": "tcp://<host>[:port]"     (domyślny port Meshtastic 4403)
#               albo jawnie:  "transport": "tcp", "host": "<host>", "tcp_port": 4403
#   Rozwiązuje pad 30 m USB-po-skrętce (Unitek) — endpoint: Heltec na WiFi (Meshtastic TCP API).
#   Można MIESZAĆ: ANT-1 po USB, ANT-2 po TCP. "transport":"usb" wymusza USB mimo schematu portu.
_MESH_TCP = "tcp://100.79.111.24"   # przykład endpointu TCP (podmień na IP swojego Helteca/EW11)

# ── FAZA 1 (multi-gateway): port DRUGIEGO Helteca na supervisorze = ANT-2 → G2 ──
# ⚠️ UWAGA (zweryfikowane na żywo 2026-06-29): tanie Heltec CP2102 mają IDENTYCZNY serial
# "...0001..." → przy DWÓCH Heltecach na jednej maszynie by-id jest NIEJEDNOZNACZNE (udev robi
# jeden, kolidujący symlink). Supervisor będzie miał 2 Heltece (ANT-1+ANT-2) → MUSI używać by-path
# (topologia USB, stabilna per gniazdo), NIE by-id. Ustal:
#   ls -l /dev/serial/by-path/      # np. pci-0000:00:12.0-usb-0:1.1:1.0-port0 -> ttyUSBx
# Przykład realny z bramki testowej: "/dev/serial/by-path/pci-0000:00:12.0-usb-0:1.1:1.0-port0"
# (Na OSOBNYM boxie z 1 Heltecem by-id z serialem 0001 jest OK — niejednoznaczność tylko gdy 2 sztuki.)
_MESH_PORT_ANT2 = "/dev/serial/by-path/CHANGEME-drugi-Heltec-ANT2"  # FAZA1: by-path (NIE by-id — dup serial)

# ───────────────────────────────────────────────────────────────────────────
# Konfiguracje pogrupowane PER ROLA (cały config jednej roli w JEDNYM bloku,
# sekcje per STEP w komentarzach). Aktywny wybór = CONFIG na końcu pliku.
# Część kluczy celowo powtarza się między rolami (chunk_*, lora, slot_seconds) —
# wartości równe, ale każda rola trzyma własną kopię = czytelność > DRY tutaj.
# ───────────────────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════
# SUPERVISOR (G0) — agreguje bramki, master kalendarza, anomalie
# ═══════════════════════════════════════════════════════════
SUPERVISOR_CONFIG = {
    # — STEP 1: Transport —
    "id": "G0",
    "mesh_ports": [
        # 1 antena LoRa / bramkę. Multi-gateway: dodać kolejne wpisy (ANT-2→G2 itd.).
        {"port": _MESH_PORT, "enabled": True, "label": "ANT-1", "gateways": ["G1"]},
        # TCP zamiast USB (serial-over-IP) — odkomentuj zamiast wpisu wyżej:
        # {"port": _MESH_TCP, "enabled": True, "label": "ANT-1", "gateways": ["G1"]},
        # ── FAZA 1: ANT-2 → G2 — ODKOMENTUJ po podłączeniu drugiego Helteca + ustawieniu
        #    _MESH_PORT_ANT2 wyżej. `gateways` tu steruje TX-routingiem (send_to(G2) idzie
        #    przez ANT-2; fallback=broadcast). RX odbierają WSZYSTKIE anteny — przypisanie
        #    do bramki robi pole `g:` w ramce (bramka stempluje własne id) → brak mieszania
        #    sid nawet przy przecieku RF między antenami. Szczegóły: runbook sekcja C.
        # {"port": _MESH_PORT_ANT2, "enabled": True, "label": "ANT-2", "gateways": ["G2"]},
    ],
    # usb_reset: programowy „replug" (USBDEVFS_RESET) gdy antena zwisa — auto-recovery bez ręcznego
    # odpinania + restartu. usb_reset_after=N nieudanych reconnectów. rx_timeout=backstop (brak RX>Ns
    # gdy connected ⇒ zwis; >2×heartbeat=900 by nie resetować przy 1 zgubionej HB). 0=off.
    "mesh_reconnect": {"enabled": True, "interval": 15, "max_backoff": 120,
                       "usb_reset": True, "usb_reset_after": 2, "rx_timeout": 1100},
    "mqtt": {"host": "localhost", "port": 1883, "user": "mqtt", "pass": MQTT_PASS},
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    # Generyczny kanał ReliableTransfer (devmap/avail blob/ansnap) — TEN SAM empirycznie
    # sprawdzony próg co calendar.chunk_size (≤90B = niezawodny próg tego łącza LoRa).
    "transfer": {"chunk_size": 60},
    "gateways": ["G1"],            # FAZA 1: zmień na ["G1", "G2"] gdy ANT-2 podłączona
    # "gateways": ["G1", "G2"],    # ← wariant multi-gateway (odkomentuj, usuń wyżej)

    # — STEP 2: Protocol (HB, Discovery, HA Entities) —
    "heartbeat_timeout": 600,
    "ha_prefix": "homeassistant",
    "state_prefix": "lora",
    "log_file": "/tmp/test_transport.log",
    "mode_names": _MODE_NAMES,

    # — STEP 4: Kalendarz (krok 16) + Slotowanie / anti-collision (krok 7) —
    "ha_api": {
        "url": HA_URL,
        "token": HA_TOKEN,
        # REVERSE mirror (gw_push → calendar.lora_<gw>): zapis ICS do storage HA supervisora
        # (jak bramka — REST /api/calendars POST jest read-only=405). {gw}=lower nazwa bramki.
        # Wymaga zapisu dla usera `td` w .storage (chmod 777 jak na bramce).
        "mirror_ics_path": "/var/lib/homeassistant/homeassistant/.storage/local_calendar.lora_{gw}.ics",
    },
    "calendar": {
        "chunk_size": 60,         # ≤90B = niezawodny próg tego łącza LoRa (lekcja RF)
        "chunk_delay": 6.0,
        "window_days": 14,
        "calendar_id": "calendar.lora_global",   # encja HA czytana jako GLOBAL
        "sync_interval": 0,                       # s; 0 = re-read kalendarza tylko na start/przycisk
        "enabled_gateways": ["G1"],               # do których bramek push harmonogramu
        # "enabled_gateways": ["G1", "G2"],       # FAZA 1: push harmonogramu też do G2
        # ANTY-SPAM (2026-06-16): timeout ACK MUSI pokryć realny round-trip LoRa (sup TX cooldown
        # 3s + airtime + gw TX cooldown 3s + airtime + kolejka). Czysty ~6-10s, pod obciążeniem ~30-60s.
        "chunk_ack_timeout": 30.0,  # s czekania na cal_cack przed retransmisją (round-trip LoRa)
        "chunk_retries": 5,         # maks. retransmisji chunku
        "end_retries": 3,           # maks. retransmisji cal_end
    },
    "slotting": {
        "enabled": True,
        "slot_seconds": {"G1": 40, "G2": 10, "G3": 10},
        "safe_window_seconds": 10,                # supervisor TX do bramki tylko ≤10s po RX od niej
    },

    # — STEP 5: Anomalie (reconcyliacja, popup) —
    "anomaly": {
        "persist_path": "/tmp/lora_anomaly_ids.json",
        "dump_interval": 1800,    # s — cykliczny dump_anom (reconcyliacja, 30 min)
        "prune_after": 90,        # s po dump — kasuj anomalie niepotwierdzone przez bramkę
        "popup": True,            # browser_mod popup przy nowej anomalii critical
    },

    # — Sterowanie: retry komendy gdy nie wróci `st` (czyta run_supervisor) —
    # BUG NAPRAWIONY 2026-07-30: te klucze leżały w GATEWAY_CONFIG, więc supervisor ich NIE WIDZIAŁ
    # i leciał na defaulcie 15 s — KRÓTSZYM niż realny RTT łącza, przez co KAŻDA komenda szła dwa
    # razy. Zmierzone: cmd 19:45:29 → bramka wykonała 19:45:41 (12 s transitu) → sup „brak st, retry"
    # 19:45:46 → `st` przyszło 19:45:47, sekundę PO retry. 45 s = zapas ponad RTT (17 s na czystym
    # łączu, 30-70 s pod obciążeniem transferem), a przy REALNEJ stracie komendy retry i tak wejdzie.
    # Ta sama rodzina strojenia co data.pri_ack_wait=35 s i param T4=60 s.
    "control": {
        "timeout": 45,
        "retries": 2,             # łącznie do 3 wysyłek, potem urządzenie → OFFLINE
        # RAMKA ZBIORCZA (2026-08-26): kliknięcia z dashboardu z tego okna [s] lecą JEDNĄ
        # ramką `cmdm` na kilkanaście urządzeń zamiast `cmd` na każde. Podnieść = mniej
        # ramek, wolniejsza reakcja na pojedynczy klik (UI i tak jest optimistic).
        "bulk_window_cmd": 1.5,
        "bulk_max_bytes": 200,    # sufit ramki; Meshtastic >220 B ucina PO CICHU
    },
}

# ═══════════════════════════════════════════════════════════
# GATEWAY (G1) — Zigbee→LoRa, lokalne Z2M+HA, autonomiczna
# ═══════════════════════════════════════════════════════════
# FAZA 1 — DWIE BRAMKI NA JEDNEJ MASZYNIE (test slotowania bez osobnych boxów):
#   LORA_GW_ID=G2 LORA_MESH_PORT=<by-id-Helteca-B> python3 Meshtastic_SCADA.py
# Env nadpisuje id + port; pliki stanu /tmp dostają suffix _g2 (G1 = ścieżki BEZ zmian,
# pełna wsteczna zgodność). Docelowo każda bramka = osobny box (env nieużywane).
_GW_ID = os.environ.get("LORA_GW_ID", "G1").strip() or "G1"


def _inst(path):
    """Suffix pliku stanu /tmp nazwą bramki — TYLKO dla instancji ≠ G1 (G1 bez zmian)."""
    if _GW_ID == "G1":
        return path
    base, ext = os.path.splitext(path)
    return f"{base}_{_GW_ID.lower()}{ext}"


GATEWAY_CONFIG = {
    # ── FAZA 1 — PRZENIESIENIE TEGO BLOKU NA MASZYNĘ G2 (in-place, NIE scp/regex!) ──
    # Ten sam plik na boxie G2; zmień TYLKO te klucze (reszta identyczna):
    #   "id":          "G2"
    #   "mesh_port":   <by-id Helteca G2>   (ls -l /dev/serial/by-id/ na G2)
    #   "monitored":   [...]                 (urządzenia Zigbee G2)
    #   "priority_devices": [...]            (security G2)
    # Kalendarz NIE wymaga już ręcznej zmiany: cal_entity / ics_path / gw_calendar_id wyliczają się
    # z "id" (G2 → calendar.lora_g2). Encja o tej nazwie musi istnieć w HA bramki (local_calendar).
    # Sekrety NA G2: HA_TOKEN_GW = token HA boxa G2, MQTT_PASS = brokera G2 (ustaw przez Launcher,
    # NIE kopiuj configu G1 ze scp — sekrety i tak są per-maszyna). slotting.gateways już ["G1","G2","G3"]
    # → G2 weźmie indeks 1 = okno 20-39s automatycznie.
    # — STEP 1: Transport —
    "id": _GW_ID,                                          # env LORA_GW_ID nadpisuje (test 2 bramek/maszyna)
    "mesh_port": os.environ.get("LORA_MESH_PORT", _MESH_PORT),  # env LORA_MESH_PORT = by-id drugiego Helteca
    "mesh_reconnect": {"enabled": True, "interval": 10, "max_backoff": 120,
                       "usb_reset": True, "usb_reset_after": 2, "rx_timeout": 1100},  # auto-recovery USB (patrz wyżej)
    "mqtt": {"host": "172.17.0.1", "port": 1883, "user": "mqtt", "pass": MQTT_PASS},
    "lora": {"max_size": 220, "tx_cooldown": 3.0},
    # Generyczny kanał ReliableTransfer (devmap/avail blob) — TEN SAM empirycznie sprawdzony
    # próg co calendar.chunk_size (≤90B = niezawodny próg tego łącza LoRa, lekcja RF).
    "transfer": {"chunk_size": 60},

    # — STEP 2: Protocol (HB, Discovery, HA Entities) —
    "heartbeat_interval": 900,                # 15 min — minimalizacja ruchu LoRa
    # MONITORED = batch co 30s (delta). PRIORITY = batch co 10s (security, szybki).
    # Urządzenia SPOZA obu list: nadal mają anomalie (offline/battery/stagnation = ALL devices),
    # ale danych NIE batchują (oszczędność LoRa). Przykład wg v38 (Temp 3/4 = martwe encje, pominięte).
    "monitored": ["Test 1", "Test 2", "Temp 1", "Temp 2"],   # sensory+switche; v38: +Temp 3/4
    "priority_devices": ["Leak 1", "Door 1"],                # security → P1 10s

    # ── E3 (2026-08-03): PRESET PRODUKCYJNY PARAMETRÓW ──────────────────────────────────
    # Wartości startowe pokręteł dashboardu dla TEGO wdrożenia. Stosowane TYLKO gdy nie ma
    # jeszcze persistu (`state.params_path`) — edycje usera z dashboardu zawsze wygrywają
    # i nie są nadpisywane przy restarcie. Deploy NIE dotyka config.py, więc preset to
    # naturalne miejsce na różnice między bramkami (G1 carport ≠ G2 testbed).
    # Dlaczego to w ogóle istnieje: świeża bramka bez presetu wstaje na fabrycznych
    # wartościach i potrafi od razu sypać fałszywymi alarmami (klasyk: timeout pingu
    # KRÓTSZY niż realny RTT łącza = „supervisor offline" przy zdrowym łączu).
    "params_preset": {
    # 2026-08-26 STROJENIE Z POMIARU (G1): bateryjne raportują baterię co 34-48 h
    #   (zdrowy ra12_s9 = 46,2 h) → P1=48 h odpalało stagnację na SPRAWNYCH.
        "P1": 96,    # h  — stagnacja urządzeń bateryjnych
    # 2026-08-26: przekaźniki G1 zmieniają stan max co ~14,8 h (pomiar 48 h) → 24 h
    #   nie miało marginesu na dzień bez przełączeń.
        "P2": 36,    # h  — stagnacja urządzeń sieciowych
        "P3": 15,    # min— kadencja raportowania pomiarowych (binarne = zdarzeniowo, B3/B4)
    # 2026-08-26: z2m sam orzeka niedostępność przekaźnika po 10 min (availability.active),
    #   45 min = werdykt z2m + tranzyt LoRa + zapas.
        "T1": 45,    # min— offline switch/light
    # 2026-08-26: przy kadencji P3=15 min to 6 zgubionych raportów.
        "T2": 90,    # min— offline temp/hum
    # 2026-08-26 NAJWAŻNIEJSZA ZMIANA: 120 min zgłaszało CISZĘ W STREFIE jako awarię.
    #   Pomiar 7 dni na G1: zdrowe czujniki ruchu milczą do 41 h, a z2m sam daje im
    #   24 h (availability.passive.timeout=1440). 1440 min = ten sam horyzont: łapie
    #   realnie martwy czujnik (wszystkie 4 martwe wyszły właśnie po ~24 h), a spokojna
    #   noc już nie generuje alarmu.
        "T3": 1440,   # min— offline door/leak/motion (bateryjne raportują rzadko)
        "P4": 15,    # min— interwał ping→supervisor (siatka NA CISZĘ, nie stały ruch)
        "T4": 60,    # s  — timeout pingu; MUSI być > RTT łącza (sup→gw bywa 16-72 s)
        "PR": 2,     # x  — retry pingu
    },
    # Inne przykłady do rozważenia:
    #   monitored=[] → wszystkie (max ruch LoRa); priority=["Smoke 1","Leak 1"] (czujniki krytyczne)
    #   produkcja: monitored=lista ważnych temp/switch, priority=tylko pożar/wyciek/dostęp
    "virtual_io": [
        {"id": "vs_test", "type": "switch", "name": "Test Switch", "default": 0},
        {"id": "vb_test", "type": "button", "name": "Test Button"},
    ],
    "discovery": {
        "max_payload": 90,                    # mniejsze pakiety = krótszy airtime = mniej strat RF
        "tx_delay": 4.0,                      # sekundy między pakietami disc
    },
    "log_file": _inst("/tmp/test_transport.log"),         # _g2 suffix dla 2. instancji (rozdzielne logi TX)
    "mode_names": _MODE_NAMES,
    # FAZA 1: ścieżki stanu per-instancja (suffix _g2 gdy LORA_GW_ID=G2) — bez kolizji 2 procesów
    "state": {
        "params_path": _inst("/tmp/lora_params.json"),
        "schedule_path": _inst("/tmp/lora_schedule_gw.json"),
    },

    # — STEP 3: Data (batch monitored/priority + delta + refresh) —
    # UWAGA: offline NIE jest definiowany tu — wynika z timeoutów per typ (T1/T2/T3) w harnessie.
    "data": {
        "mon_interval": 30,                   # P3 monitored flush (s)
        "pri_interval": 10,                   # P1 priority flush (s)
        "max_payload": 120,                   # ≤150B operational; batch frame split
        "thresholds": {                       # delta gating (v10): below → no LoRa
            "temperature": 0.5,               # °C
            "humidity": 2.0,                  # %
        },
        # ZMIANA POLITYKI 2026-07-31 (decyzja usera): to NIE JEST już heartbeat wartości. Ramki `b`
        # lecą WYŁĄCZNIE przy zmianie stanu; ten interwał paca teraz tylko blob `avail` (jeden
        # skompresowany transfer, który niesie liveness wszystkich urządzeń) + pierwszy sweep po
        # starcie jako bootstrap dashboardu. Pomiar sprzed zmiany: sweep 194 dev co 15 min = 93%
        # całego pasma LoRa, przy 36% wpisów identycznych z poprzednim raportem.
        "report_interval": 900,
        # PRAWDZIWY ACK dla batcha P1 (2026-07-30, zamiast ślepego pri_repeat=2). Pakiet dostaje id
        # `p`, sup odbija `{t:back,g,p}`, brak ACK w pri_ack_wait s ⇒ retransmisja (max retries).
        # STROJENIE: pri_ack_wait MUSI pokryć realny round-trip tego łącza, liczony od REALNEJ
        # wysyłki (mark_sent), nie od flush. ZMIERZONE live 2026-07-30 na spokojnym łączu G2:
        # TX 16:19:38 → back u bramki 16:20:04 = 26 s (sam transit sup→gw ~16 s). Przy 20 s każdy
        # alarm generował jedną ZBĘDNĄ retransmisję, więc próg = 35 s (margines na jitter).
        # Pod obciążeniem (bulk/kalendarz) round-trip rośnie do 30-60 s — jeśli w logu widać 🔁
        # przy zdrowym łączu, podnieś dalej. 0 = wyłącz ACK i wróć do ślepego pri_repeat.
        "pri_ack_wait": 35.0,
        "pri_ack_retries": 2,                 # łącznie do 3 wysyłek, potem ❌ w logu (utrata P1)
    },

    # — STEP 4: Kalendarz (krok 16) + Slotowanie / anti-collision (krok 7) —
    "ha_api": {
        "url": HA_URL,
        "token": HA_TOKEN,
        # JAWNIE (2026-07-30, polecenie usera): bramka ma kalendarz `calendar.lora_<gw>` — dla G2
        # `calendar.lora_g2`. To encja, do której down-sync pisze przez HA WS (delete +
        # create_event) i z której `push_schedule_up` czyta (gw_calendar_id niżej). Wyliczana z
        # _GW_ID, żeby każdy box był samo-spójny (wcześniej na G2 wisiał wpis z „g1").
        "cal_entity": f"calendar.lora_{_GW_ID.lower()}",
        # MARTWY KLUCZ: bramka NIE pisze już ICS do pliku — `.storage/*.ics` jest root:root i zapis
        # zawodził po cichu (stąd przejście na WS). Zostawiony dla zgodności, ale z nazwą TEJ bramki.
        "ics_path": f"/var/lib/homeassistant/homeassistant/.storage/local_calendar.lora_{_GW_ID.lower()}.ics",
    },
    "calendar": {
        "chunk_size": 60,         # ≤90B = niezawodny próg tego łącza LoRa (lekcja RF); transfer chunkuje b64
        "chunk_delay": 6.0,       # s między chunkami (honor cooldown LoRa)
        "window_days": 14,        # okno harmonogramu kompaktowanego do LoRa
        # REVERSE (2026-07-30): ŹRÓDŁO push-up = lokalny kalendarz bramki, ta SAMA encja, do której
        # down-sync pisze przez WS (`cal_entity` / `calendar.lora_<gw>`) → edycja na HA bramki
        # jedzie w górę i nadpisuje `calendar.lora_<gw>` na supie. '' = pchaj wewnętrzny effective.
        "gw_calendar_id": f"calendar.lora_{_GW_ID.lower()}",
        # ANTY-SPAM (2026-06-16): timeout ACK MUSI pokryć realny round-trip LoRa (patrz supervisor).
        "chunk_ack_timeout": 30.0,  # s czekania na cal_cack przed retransmisją chunku
        "chunk_retries": 5,         # maks. retransmisji chunku (max 3 wysyłki/chunk)
        "end_retries": 3,           # maks. retransmisji cal_end (finalny ACK)
    },
    "slotting": {
        "enabled": True,
        "slot_seconds": {"G1": 40, "G2": 10, "G3": 10},       # G1:0-19 / G2:20-39 / G3:40-59 w cyklu 60s
        "gateways": ["G1", "G2", "G3"],   # kolejność = indeks slotu tej bramki
    },

    # — STEP 5: Anomalie (offline+battery+stagnation, ALL devices) + tryb day/night —
    "anomaly": {
        "persist_path": _inst("/tmp/lora_anomaly_ids.json"),  # FAZA 1: _g2 suffix dla 2. instancji
        "battery_low": 25,        # < % → low_battery (lb)
        "battery_critical": 15,   # < % → critical_battery (cb)
        "battery_check": 15,      # s — częstotliwość sprawdzania baterii (TEST: 15, prod 300)
        "offline_check": 30,      # s — gateway-side offline anomaly
        "offline_grace": 120,     # s — grace zanim offline
        "stagnation_check": 300,  # s — stagnacja (P1/P2 godz z param_sync)
        # STEP 5 krok 14: temp/hum high/low dla WSZYSTKICH urządzeń (None = wymiar wyłączony)
        "temp_high": None,        # > °C → temp_high (th)   [TEST faza2: wyłączony]
        "temp_low": 30,           # < °C → temp_low (tl)    [TEST faza2: real ~28 < 30 → tl]
        "hum_high": 35,           # > % → hum_high (hh)      [TEST faza2: real ~39 > 35 → hh]
        "hum_low": None,          # < % → hum_low (hl)      [TEST faza2: wyłączony]
        "temphum_check": 15,      # s — częstotliwość sprawdzania temp/hum (TEST: 15, prod 60)
        "flush_interval": 30,     # s — AnomalyBatcher flush `ab` P2
        "max_payload": 150,       # B — split ramki ab
    },
    "gateway_mode": {
        # tryb pracy bramki: 'all-time' (zawsze), 'day' (aktywna w dzień), 'night' (aktywna w nocy).
        # Bramka nieaktywna NIE zgłasza offline (urządzenia świadomie bez zasilania).
        "operating_mode": "all-time",
        "day_start": "06:00",     # okno dnia (gdy brak lat/lon)
        "day_end": "20:00",
        "lat": None,              # opcjonalnie: zmierzch/świt solarnie (np. 50.3)
        "lon": None,              # (np. 18.7) — wtedy day_start/end ignorowane
    },

    # — Sterowanie (cmd → urządzenie) —
    "control": {
        # optimistic_status: True = bramka odsyła `st` (status sterowanego urządzenia) OD RAZU
        # po cmd, NIE czekając na realne potwierdzenie z Z2M → supervisor/HA odbija stan natychmiast
        # (szybkie UI, ale jeśli Z2M odrzuci/nie wykona — chwilowo zły stan, korygowany realnym `st`).
        # False = czekaj na potwierdzenie Z2M zanim odeślesz `st` (dokładniej, wolniej). Domyślnie False.
        # Uwaga: dashboard supervisora i tak pokazuje klik optymistycznie; to dotyczy źródła `st` z bramki.
        "optimistic_status": False,
        # RAMKA ZBIORCZA (2026-08-26): statusy z tego okna [s] lecą jedną ramką `stm`.
        # Dłuższe niż po stronie cmd, bo potwierdzenia z z2m sączą się kilka sekund.
        # Zmierzone na harmonogramie 22:15 (G1): 65 potwierdzeń w 44 s, odstępy 1-3 s.
        # Okno 3 s dawało flushe jednoelementowe (= stara ramka `st`) → 63 `st` + 2 `stm`.
        "bulk_window_st": 20.0,
        "bulk_max_items_st": 12,   # zapełniona ramka leci od razu, nie czeka do końca okna
        "bulk_max_bytes": 200,
        # UWAGA: `timeout`/`retries` (retry komendy po braku `st`) czyta ROLA SUPERVISORA
        # (run_supervisor), więc mieszkają w SUPERVISOR_CONFIG["control"] — nie tutaj. Tu zostaje
        # tylko to, co czyta bramka. Trzymanie ich w tym bloku było przyczyną buga z 2026-07-30:
        # supervisor ich nie widział i leciał na defaulcie 15 s.
    },
}

# ═══════════════════════════════════════════════════════════
# Aktywny config na tej maszynie (rola = .role / LORA_ROLE)
# ═══════════════════════════════════════════════════════════

CONFIG = GATEWAY_CONFIG if ROLE == 'gateway' else SUPERVISOR_CONFIG
