#!/usr/bin/env python3
"""
Step 4 Integration Test — Kalendarz (krok 16) + Slotowanie / anti-collision (krok 7)

Nadbudowa nad Step 3 (transport + protokół + dane). DODAJE:
  - SUPERVISOR (master):
      • czyta encję HA `calendar.lora_global` (REST) → ScheduleManager.replace_global
      • push harmonogramu do bramek przez CalendarTransfer (cal_begin/chunk/end/ack,
        zlib+b64, CRC, NACK→retransmit) — `_targeted_calendar_sync`
      • drift: bramka raportuje hash `cal` w HB/pong; ≠ expected → re-sync tej bramki
      • SafeWindow: TX do bramki tylko ≤safe_window_s po RX od niej (half-duplex)
      • przyciski: Sync Calendar All / per-bramka Sync Calendar
  - GATEWAY:
      • odbiera harmonogram → expand_compact → merge → zapis ICS do HA local_calendar
        (atomowo, zachowanie ownera) + reload integracji → recreate kalendarza lokalnie
      • publikuje bieżący tryb: sensor.lora_g1_mode (+ _mode_next, + _cal_hash)
      • hash `cal` + tryb `m` w HB (diag_fn) → supervisor wykrywa drift
      • SlotScheduler: ruch okresowy (HB/batch) tylko w oknie tej bramki (G1:0-19s …);
        reaktywne odpowiedzi (pong/st/ack/cal_*) natychmiast — nie czekają na slot

PROCEDURA TESTOWA (na sprzęcie):
  A) Push — dodaj event w HA calendar.lora_global → supervisor button "Sync Calendar All"
     → GATEWAY log 📥 Begin/✅ Odebrano N slotów → ✅ ICS zapisany → encja sensor.lora_g1_mode
  B) Drift auto-sync — zmień kalendarz na supervisorze → po HB bramki (cal≠expected)
     supervisor sam robi targeted sync (log 📤 {gw}: sending … hash=)
  C) ICS recreate — sprawdź local_calendar w HA bramki: eventy = harmonogram (po reload)
  D) Tryb — w trakcie slotu PRODUKCJA: sensor.lora_g1_mode = PRODUKCJA, _mode_next = next change
  E) Slotowanie — HB/batch wychodzą tylko w oknie [idx*20,(idx+1)*20)s; pong/ack natychmiast
  F) Safe window — supervisor odkłada targeted sync jeśli nie słyszał bramki w oknie
  G) Niezawodność — zgub chunk (RF) → NACK → retransmit → ✅ ACK ok=1; cal_end retry x3

Uruchomienie (launcher lub ręcznie):
  cd skrypty && py test_step4_calendar.py     # rola z config.py (gateway/supervisor)
  (token HA: ustaw env HA_GW_TOKEN / HA_SUP_TOKEN — fetch/reload off bez tokenu, zapis ICS działa)
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import hashlib, json, os, signal, sys, time, threading
from datetime import datetime
from logging.handlers import RotatingFileHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from config import CONFIG, ROLE
from modules.transport import Dispatcher, MqttTransport, LoraTransport
from modules.transport.tx_queue import PriorityTxQueue
from modules.protocol.bulk import (Coalescer, cmd_frames, st_frames,  # ramki ZBIORCZE cmdm/stm
                                   retry_action)                   # gate ponowien cmd
from modules.protocol import (HAEntities, GatewayHeartbeat, SupervisorHeartbeat, merge_hb_hashes,
                               GatewayDiscovery, SupervisorDiscovery, SupervisorLinkProbe)
from modules.data import GatewayData, SupervisorData, encode_short, TYPE_CAPS
from modules.params import ParamSync
from modules.anomaly import (StagnationEngine, OfflineMonitor, GatewayOfflineAnomaly, AlarmMonitor,
                             BatteryMonitor, TempHumMonitor, AnomalyBatcher, AnomalyStore)
from modules.anomaly.ha_v10 import AnomalyHAv10Bridge   # most: store → encje HA per-anomalia (v10)
from modules.anomaly.ha_publish import AnomalyBucketPublisher  # bramka publikuje kubełki anomalii na SWOJE HA
from modules.anomaly.reconciler import AnomalyReconciler  # FAZA 1: anomalie jako plik (ansnap blob)
from modules.calendar import (ScheduleManager, CalendarTransfer, build_ics,
                              write_ics_atomic, fetch_ha_calendar, write_ha_calendar,
                              reload_local_calendar, sync_ha_calendar_ws)
from modules.transport.reliable_transfer import ReliableTransfer  # bulk (mass_offline z2m-down)
from modules.transport.channel import ChannelArbiter  # arbitraż półdupleksu: transfer-plik > wolumen
from modules.slotting import SlotScheduler, SafeWindow
from modules.gateway_mode import GatewayMode, mode_active, fmt_uptime
from modules.control import GroupCaster       # masowe ON/OFF: groupcast Zigbee + weryfikacja

STATE_PREFIX = CONFIG.get('state_prefix', 'lora')
HA_PREFIX = CONFIG.get('ha_prefix', 'homeassistant')


def _safe(s):
    return str(s).replace(' ', '_').lower()


# ── Logger with colors and icons ────────────────────────
class Log:
    LEVEL = {
        'DEBUG': ('\033[36m', '🔍'), 'INFO': ('\033[32m', ''),
        'WARN':  ('\033[33m', '⚠️'), 'ERROR': ('\033[31m', '❌'),
    }
    COMP = {
        'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'ANT': '📡',
        'HA': '🏠', 'HB': '💓', 'DISC': '🔭', 'VIO': '🔘',
        'DISPATCH': '📨', 'TX': '📤', 'RX': '📥', 'STATS': '📊',
        'CMD': '⚡', 'BTN': '🔘', 'CTRL': '🎛️', 'DATA': '📊', 'BATCH': '📦',
    }
    R = '\033[0m'

    def __init__(self):
        self._h = RotatingFileHandler(CONFIG['log_file'], maxBytes=2_000_000, backupCount=2)
        self._h.setFormatter(logging.Formatter('%(message)s'))
        self._f = logging.getLogger('test_step3')
        self._f.addHandler(self._h)
        self._f.setLevel(logging.DEBUG)

    def _w(self, lvl, comp, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        color, _ = self.LEVEL.get(lvl, ('', ''))
        icon = self.COMP.get(comp, '•')
        print(f"{ts} {color}[{lvl}]{self.R} {icon} [{comp}] {msg}", flush=True)
        self._f.info(f"{ts} [{lvl}] [{comp}] {msg}")

    def debug(self, c, m): self._w('DEBUG', c, m)
    def info(self, c, m): self._w('INFO', c, m)
    def warn(self, c, m): self._w('WARN', c, m)
    def error(self, c, m): self._w('ERROR', c, m)
    def close(self):
        try: self._h.close()
        except: pass


def _make_lora(cfg, log):
    if 'mesh_port' in cfg:
        ports = [{"port": cfg['mesh_port'], "enabled": True,
                  "label": "ANT-1", "gateways": [cfg['id']]}]
    else:
        ports = cfg.get('mesh_ports', [])
    return LoraTransport(ports_cfg=ports,
                         reconnect_cfg=cfg.get('mesh_reconnect', {}),
                         logger=log)


# ── Gateway mode ────────────────────────────────────────
def run_gateway(log):
    running = threading.Event(); running.set()
    mqtt_cfg = CONFIG['mqtt']
    tx_delay = CONFIG.get('discovery', {}).get('tx_delay', 4.0)
    gw_id = CONFIG['id']; gw_lower = gw_id.lower()
    data_cfg = CONFIG.get('data', {})

    mqtt = MqttTransport(
        host=mqtt_cfg['host'], port=mqtt_cfg['port'],
        user=mqtt_cfg['user'], password=mqtt_cfg['pass'],
        client_id=f"step3_gw_{gw_id}_{int(time.time())}", logger=log)

    lora = _make_lora(CONFIG, log)
    CONFIG['_lora_tx_queue'] = None     # kolejka TX wyłączona (single-drainer wieszał się na zawieszonym
    #                                     CP2102 send → cały TX stop). Właściwy fix = unified state file
    #                                     (mniej wiadomości = brak potrzeby serializacji). Anomalie: fallback.
    ha = HAEntities(mqtt, logger=log)
    vio_config = CONFIG.get('virtual_io', [])

    discovery = GatewayDiscovery(
        gw_id=gw_id,
        monitored_names=CONFIG.get('monitored', []),
        priority_names=CONFIG.get('priority_devices', []),
        vio_config=vio_config, lora=lora, logger=log,
        max_payload=CONFIG.get('discovery', {}).get('max_payload', 150),
        # STABLE SID: trwała mapa nazwa→sid per-bramka (koniec dryfu sidów przy zmianie składu).
        sid_persist_path=CONFIG.get('state', {}).get(
            'sid_map_path', f'/tmp/lora_sid_map_{gw_id.lower()}.json'))

    # HB: monitored WYŁĄCZNY (bez priority). is_monitored() liczy priority jako monitored
    # (→ mon=6); tu liczymy mon=monitored\priority (=4), pri=priority (=2), dev=total (=7).
    # get_devices_summary zwraca REFERENCJE do wewn. dictów → kopiujemy, nie mutujemy w miejscu.
    def _devices_summary_excl():
        return {n: {**i, 'monitored': bool(i.get('monitored') and not i.get('priority'))}
                for n, i in discovery.get_devices_summary().items()}

    def _hb_hashes():
        """`dh` zawsze, `sh` TYLKO gdy się różni — heartbeat ma twardy sufit 220 B.

        Regresja 2026-08-26: bezwarunkowe `sh` wypchnęło HB na 228 B i heartbeaty G1 przestały
        docierać (bramka żyła, `b`/bloby/sup_ping leciały — zamarzły tylko hashe i liczniki
        na dashboardzie). Gdy hashe są równe, `sh` nie wnosi nic: supervisor czyta wtedy `dh`
        (`_sh = d.get('sh') or _dh`). Zaoszczędzone 16 B wraca do puli ramki."""
        dh = data.state_hash()
        sh = data.status_hash()
        return {'dh': dh} if sh == dh else {'dh': dh, 'sh': sh}

    heartbeat = GatewayHeartbeat(
        gw_id=gw_id, devices_fn=_devices_summary_excl,
        lora=lora, logger=log,
        interval=CONFIG.get('heartbeat_interval', 120),
        diag_fn=lambda: {'hash': discovery.disc_hash, 'ph': param_sync.params_hash(),
                         'cal': scheduler.schedule_hash(gw_id, cal_cfg.get('window_days', 14)),
                         'm': cal_state['mode'],        # STEP 4: hash kalendarza + bieżący tryb
                         # `oh` USUNIĘTE 2026-08-26. Było nadawane w KAŻDYM heartbeacie i
                         # NIGDY nieczytane — zero odbiorców po stronie supervisora (sprawdzone
                         # grepem: brak `d.get('oh')`, brak w szablonach dashboardów). Kosztowało
                         # 17 B w ramce, która ma twardy sufit 220 B (Meshtastic ucina PO CICHU).
                         # Dołożenie `sh` wypchnęło HB na 228 B i heartbeaty G1 PRZESTAŁY
                         # DOCHODZIĆ (ostatni 19:48:10, potem cisza mimo żywej bramki — `b`,
                         # bloby i sup_ping leciały dalej). Martwe pole zwalnia miejsce dla
                         # używanego. Gdyby `oh` kiedyś było potrzebne: dorobić mu ODBIORCĘ
                         # razem z nadajnikiem, nie odwrotnie.
                         'tq': _time_quality(),         # STEP 5+: jakość czasu (synced/holdover/unsynced)
                         # AH-GATE (2026-07-07): hash stanu anomalii nadanego supervisorowi — sup
                         # porównuje ze swoim i żąda dump TYLKO przy rozjeździe (koniec ślepych
                         # pełnych ansnap co dump_interval). an_reconciler zdefiniowany niżej,
                         # wywołanie dopiero po starcie HB (wzorzec jak param_sync wyżej).
                         **({'ah': an_reconciler._last_hash} if an_reconciler._last_hash else {}),
                         # B6 (2026-08-01): hash STANU URZĄDZEŃ. Odkąd binarne raportują wyłącznie
                         # zdarzeniowo, zbieżności lustra nie pilnuje periodyk tylko ten hash — sup
                         # porównuje ze swoim i przy rozjeździe prosi o `dsnap` (jeden transfer).
                         # `dh` + (warunkowo) `sh` — patrz `_hb_hashes` wyżej. `sh` = hash STANU,
                         # KTÓRY BRAMKA ZNA; `dh` = co z tego dowiozła. sh≠dh = dane w drodze.
                         **_hb_hashes(),
                         **gw_mode.state()})            # STEP 5: gm=tryb, ga=aktywna (supresja offline)

    # ── STEP 4: slotowanie (krok 7) — batch `b` nadawany TYLKO w oknie tej bramki ──
    # (HB/pong/st/cal idą surowym lora = natychmiast; gate'ujemy tylko wolumenowy batch,
    #  bo to on koliduje gdy 3 bramki nadają naraz. cal_* mają własny CRC+retry.)
    slot_cfg = CONFIG.get('slotting', {})
    slot = None
    data_lora = lora                                     # domyślnie bez gate
    if slot_cfg.get('enabled'):
        slot = SlotScheduler(gw_id, gateways=slot_cfg.get('gateways', ['G1', 'G2', 'G3']),
                             slot_seconds=slot_cfg.get('slot_seconds', 20), logger=log)

        class _SlottedLora:
            """Proxy nad LoraTransport — send() czeka na okno slotu; reszta delegowana."""
            def __init__(self, inner, sched):
                self._inner, self._sched = inner, sched
            def send(self, text):
                self._sched.wait_my_turn()
                return self._inner.send(text)
            def __getattr__(self, name):
                return getattr(self._inner, name)

        data_lora = _SlottedLora(lora, slot)
        s0, s1 = slot.window()
        log.info('SLOT', f'⏱️ slotowanie ON: {gw_id} okno {s0:.0f}-{s1:.0f}s / cykl {slot.cycle:.0f}s (batch w slocie)')

    # STEP 3: data layer — Z2M → delta → batched `b`
    data = GatewayData(
        gw_id=gw_id, discovery=discovery, lora=data_lora, logger=log,
        # ACK P1 (2026-07-30): pakiety priority dostają id `p`, sup odbija `back`, brak ACK =
        # retransmisja. `pri_lora` = transport BEZ gate'u slotu → alarm nie czeka na okno bramki.
        pri_lora=lora,
        pri_ack_wait=data_cfg.get('pri_ack_wait', 35.0),   # 35s: zmierzony RTT 26s + margines
        pri_ack_retries=data_cfg.get('pri_ack_retries', 2),
        mon_interval=data_cfg.get('mon_interval', 30),
        pri_interval=data_cfg.get('pri_interval', 10),
        max_payload=data_cfg.get('max_payload', 120),
        thresholds=data_cfg.get('thresholds'),
        send_spacing=CONFIG.get('lora', {}).get('tx_cooldown', 3.0),   # honor LoRa cooldown
        report_interval=data_cfg.get('report_interval', 0),            # #8 periodic heartbeat
        report_full_every=data_cfg.get('report_full_every', 1),        # (d) delta availability: pełny sweep co N-ty
        offline_after=data_cfg.get('offline_after'))                   # #7 gw-side liveness

    # STEP 3: lokalna encja Zigbee LQI per urządzenie (Z2M nie tworzy jej sam) —
    # czyta wprost z topicu z2m, więc gateway dashboard ma LQI w pełnej rozdzielczości.
    lqi_regd = set()

    def reg_gw_lqi(dev):
        if dev in lqi_regd:
            return
        safe = _safe(dev)
        uid = f"lora_{gw_lower}_{safe}_lqi"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": f"{dev} LQI", "object_id": uid, "unique_id": uid,
            "state_topic": f"zigbee2mqtt/{dev}",
            "value_template": "{{ value_json.linkquality | default('') }}",
            # force_update: linkquality bywa STAŁE (np. 255) → bez tego MQTT sensor nie zapisuje
            # stanu przy niezmienionej wartości i last_reported zamarza → dashboard fałszywie
            # pokazuje OFFLINE mimo świeżych raportów z2m. force_update bumpuje stan co raport.
            "force_update": True,
            "icon": "mdi:signal", "state_class": "measurement"}, separators=(',', ':')),
            retain=True)
        lqi_regd.add(dev)

    # UNIFIKACJA: encja dostępności per-urządzenie na LOKALNYM HA bramki — publikowana z
    # TEGO SAMEGO źródła (GatewayData.on_avail) co flaga `a:` do supervisora. Dashboard
    # czyta tę encję zamiast heurystyki LQI → bramka == supervisor (jedno źródło prawdy).
    avail_regd = set()

    def publish_gw_avail(dev, available):
        safe = _safe(dev)
        uid = f"lora_{gw_lower}_{safe}_available"
        if dev not in avail_regd:
            mqtt.publish(f"{HA_PREFIX}/binary_sensor/{uid}/config", json.dumps({
                "name": f"{dev} Available", "object_id": uid, "unique_id": uid,
                "state_topic": f"{STATE_PREFIX}/{gw_lower}/{safe}/avail",
                "payload_on": "1", "payload_off": "0", "device_class": "connectivity",
                "device": {"identifiers": [f"lora_{gw_lower}_{safe}"]}},
                separators=(',', ':')), retain=True)
            avail_regd.add(dev)
        mqtt.publish(f"{STATE_PREFIX}/{gw_lower}/{safe}/avail", "1" if available else "0", retain=True)

    data.on_avail = publish_gw_avail   # GatewayData zgłasza zmiany dostępności tutaj

    dev_states = {}
    pending_st = {}                # {dev: (cap, want, ts)} — cmd→Z2M czeka na realne potwierdzenie
    last_fwd_state = {}            # {dev: (cap, val)} — ostatnio przesłany `st` (dedup zmian zewn.)
    st_burst = []                  # znaczniki czasu ostatnich st (burst-guard: restart z2m → flood)
    vio_states = {v['id']: ('ON' if v.get('default') else 'OFF')
                  for v in vio_config if v['type'] == 'switch'}
    gw_stats = {'last_sup_rx_ts': 0.0, 'last_sup_rx': '--',
                'last_sync': '--', 'time_offset': '--', 'sup_rtt': '--', '_last_sync_ts': 0.0,
                'sup_time': '--',                       # #8: czas supervisora (z pakietu sync)
                'sup_link': 'ON', 'sup_lost_pong': 0}   # F2: nadpisywane przez SupervisorLinkProbe.on_state
    SUP_LINK_TIMEOUT = CONFIG.get('sup_link_timeout', 3600)

    # ── STEP 5+: jakość czasu — restart bez RTC/internetu = zegar Debiana niepewny do 1. sync ──
    # synced  : sync od supervisora < 2h temu
    # holdover: ostatni sync 2-48h temu (zegar leci dalej, ale nie świeżo potwierdzony)
    # unsynced: brak sync w tym uruchomieniu (PO RESTARCIE — czas podejrzany, tryb ostrożnie)
    # stale   : > 48h bez sync
    TQ_LABEL = {'synced': 'Zsynchronizowany ✅', 'ntp': 'Zsynchronizowany (NTP) 🛰️',
                'holdover': 'Holdover (stary sync) 🕓',
                'unsynced': 'Niezsynchronizowany ⚠️', 'stale': 'Przeterminowany ❌'}

    _ntp_cache = {'ok': None, 'ts': 0.0}

    def _ntp_synced():
        """Czy zegar systemowy jest zsynchronizowany przez NTP (internet). Cache 5 min —
        bramki z internetem mają wiarygodny czas NIEZALEŻNIE od LoRa-sync z supervisora."""
        if _ntp_cache['ok'] is not None and time.time() - _ntp_cache['ts'] < 300:
            return _ntp_cache['ok']
        ok = False
        try:
            import subprocess
            out = subprocess.run(['timedatectl', 'show', '-p', 'NTPSynchronized', '--value'],
                                 capture_output=True, text=True, timeout=3).stdout.strip()
            ok = (out == 'yes')
        except Exception:
            ok = False
        _ntp_cache['ok'] = ok; _ntp_cache['ts'] = time.time()
        return ok

    def _time_quality():
        lst = gw_stats.get('_last_sync_ts', 0)
        if lst and time.time() - lst < 2 * 3600:
            return 'synced'                          # świeży sync od supervisora (LoRa)
        if _ntp_synced():
            return 'ntp'                             # zegar wiarygodny z NTP mimo braku LoRa-sync
        if lst and time.time() - lst < 48 * 3600:
            return 'holdover'
        return 'unsynced'                            # brak NTP i brak/stary sync = czas podejrzany

    def _hydrate_states_from_ha():
        """Seed `data.states` z LOKALNEGO HA po starcie (2026-08-26).

        z2m publikuje stan urządzeń BEZ retain, więc po restarcie harnessu `states` jest puste aż
        urządzenie samo coś nadeśle — a przekaźnik carportu odezwie się dopiero o zmierzchu.
        Skutek widoczny na dashboardzie: `dh`/`sh` = 0, a supervisor po `dsnap_req` dostaje PUSTY
        zrzut i kasuje sobie lustro 78 przekaźników. HA na bramce te stany ma (encje z2m), więc
        bierzemy je stamtąd: jedno zapytanie HTTP, zero ruchu Zigbee i zero ruchu LoRa.
        `setdefault` — świeży pakiet z z2m ma zawsze pierwszeństwo nad seedem."""
        import urllib.request as _u
        cfg = CONFIG.get('ha_api', {})
        url, tok = cfg.get('url'), cfg.get('token')
        if not url or not tok:
            return
        for _ in range(30):                       # discovery leci asynchronicznie
            if discovery.devices:
                break
            time.sleep(2)
        try:
            req = _u.Request(url.rstrip('/') + '/api/states',
                             headers={'Authorization': 'Bearer ' + tok})
            stany = {e['entity_id']: e['state'] for e in json.load(_u.urlopen(req, timeout=30))}
        except Exception as e:
            log.warn('DATA', f'hydracja stanów z HA nieudana: {e}')
            return
        n = 0
        for dev, info in list(discovery.devices.items()):
            if info.get('type') not in ('switch', 'light'):
                continue                          # bateryjne i tak nie mają stanu sterowanego
            st = stany.get(f"{info.get('type')}.{dev.lower().replace(' ', '_')}")
            if st not in ('on', 'off'):
                continue
            biezace = data.states.setdefault(dev, {})
            if 'state' not in biezace:
                biezace['state'] = st.upper()
                n += 1
        log.info('DATA', f'🌱 hydracja z HA: {n} urządzeń ma stan po starcie '
                         f'(sh={data.status_hash()}) — lustro nie zostanie wyzerowane')
    threading.Thread(target=_hydrate_states_from_ha, daemon=True, name='hydrate').start()

    def lora_send(obj):
        lora.send(json.dumps(obj, separators=(',', ':')))

    # ── RAMKA ZBIORCZA `stm` (2026-08-26) — masowa zmiana stanu szła 1 ramka `st` na
    #    urządzenie (pomiar: 67× st w 7 min, pik 42 ramki/min przy suficie ~20/min).
    #    Teraz statusy z krótkiego okna lecą jedną ramką `stm` na kilkanaście urządzeń.
    #    Potwierdzenia cmd też tędy — SĄ świadomie wyłączone z burst-guarda `b` i to one
    #    dominowały ruch. Okno jest krótkie i supervisor ma optimistic UI, więc klik
    #    nadal wygląda natychmiastowo. ──
    _bulk_cfg = CONFIG.get('control', {})
    def _flush_st(items):
        for fr in st_frames(gw_id, items, max_bytes=_bulk_cfg.get('bulk_max_bytes', 200)):
            lora_send(fr)
        if len(items) > 1:
            log.info('CTRL', f'📦 st zbiorczo: {len(items)} urządzeń → mniej ramek LoRa')
    # OKNO ZMIERZONE NA ŻYWO (harmonogram 22:15 na G1, 2026-08-26): 65 potwierdzeń `st`
    # przyszło w 44 s, ODSTĘPY 1-3 s (mediana 2 s). Przy oknie 3 s większość flushów miała
    # JEDEN element → leciała stara ramka `st` i z 65 zrobiły się 63 `st` + 2 `stm`. Okno musi
    # być WIELOKROTNIE dłuższe od odstępu, żeby cokolwiek zgrupować. 20 s przy odstępie 2 s
    # zbiera ~10 urządzeń na ramkę. `max_items` = zawór w drugą stronę: gdy potwierdzenia lecą
    # gęściej, wypuść od razu po zapełnieniu ramki, nie czekaj do końca okna.
    st_bulk = Coalescer(_flush_st, window=_bulk_cfg.get('bulk_window_st', 20.0),
                        max_items=_bulk_cfg.get('bulk_max_items_st', 12),
                        logger=log, name='st')

    def st_send(dev, cap, val):
        """Status urządzenia → supervisor. Przez okno zbiorcze; 1 sztuka = stara ramka `st`.

        `note_sent` (2026-08-26): `st` TO TEŻ WYSYŁKA STANU, więc musi karmić `dh` — dokładnie
        jak `_enqueue` (ramki `b`) i blob `dsnap`. Supervisor po `st` robi `apply_st` → stan ląduje
        w jego lustrze i NATYCHMIAST zmienia JEGO hash; bez tej linii bramka o tym nie wie i hashe
        się rozjeżdżają, aż dogoni je batch `b` (zmierzone 26-56 s) albo zamaskuje debounce 90 s.
        Są przypadki, w których `b` NIE przyjdzie wcale i rozjazd zostaje na stałe:
          • potwierdzenie cmd bez realnej zmiany (włącz to, co już jest ON) — `compute_delta`
            zwraca pusto, więc `_enqueue` się nie odpala, a `st` (was_pending) leci zawsze,
          • `optimistic_status: True` — `st` idzie PRZED jakimkolwiek pakietem z z2m.
        Klucze są zgodne: `sent_states` i `SupervisorData.apply_st` trzymają PEŁNE nazwy capów
        ('state'), obie strony liczą hash przez `encode_short`."""
        try:
            data.note_sent(dev, {cap: val})
        except Exception as e:                       # wirtualne vio nie mają sid — hash je pomija
            log.debug('DATA', f'note_sent({dev}) pominięte: {e}')
        st_bulk.add((dev, cap, val))

    def for_me(d):
        g = d.get('g'); return (g is None) or (g == gw_id)

    # ── ChannelArbiter (Faza 0 rebuild): półdupleks LoRa — transfer-plik (kalendarz/devmap/
    #    ansnap) ma PIERWSZEŃSTWO; wolumen `b`/`ab` CZEKA aż kanał wolny. Koniec kolizji
    #    chunk↔cack pod ruchem (root cause zawieszania transferów przy 200+ dev). ──
    arbiter = ChannelArbiter(logger=log)
    lora.arbiter = arbiter                       # KLUCZ (2026-07-06): TX bulk (b/disc_vio/hb) odkładany
                                                 # podczas transferu → bramka słyszy cacki (half-duplex)
    data.arbiter = arbiter                       # spaced sender `_out` też respektuje transfer (busy)
    data.mon_batch.arbiter = arbiter
    data.pri_batch.arbiter = arbiter

    # STEP 17: parametry (BRAMKA = master) — P1/P2/P3 stagnation + T1/T2/T3 offline mirror
    # + TH/TL/HH/HL/BL/BC progi anomalii (seed z config.py, dalej strojone z dashboardu na żywo)
    _an_seed = CONFIG.get('anomaly', {})
    param_sync = ParamSync(
        'gateway', gw_id, mqtt, lora_send, logger=log,
        persist_path=CONFIG.get('state', {}).get('params_path', '/tmp/lora_params.json'),
        ha_prefix=HA_PREFIX, state_prefix=STATE_PREFIX,
        on_change=lambda k, v: log.info('PARAM', f'↻ apply {k}={v} (mechanizm)'),
        # E3 (2026-08-03): PRESET PRODUKCYJNY. Kolejność: default modułu → `params_preset`
        # z config.py → progi z sekcji `anomaly` → persist (edycje usera z dashboardu wygrywają).
        # Po co: nowa bramka wstaje z wartościami dla TEGO wdrożenia, a nie z fabrycznymi —
        # bez tego pierwsze uruchomienie potrafi sypać fałszywymi alarmami (np. T4 krótszy niż
        # realny RTT łącza = „supervisor offline" co ping). Preset trzyma się w config.py, bo
        # to jedyny plik per-maszyna (deploy nie nadpisuje go — patrz deploy/deploy_g1.sh).
        seed=dict(CONFIG.get('params_preset', {}),
                  **{k: v for k, v in {
                      'TH': _an_seed.get('temp_high'), 'TL': _an_seed.get('temp_low'),
                      'HH': _an_seed.get('hum_high'), 'HL': _an_seed.get('hum_low'),
                      'BL': _an_seed.get('battery_low', 25),
                      'BC': _an_seed.get('battery_critical', 15)}.items() if v is not None}))

    # FIX offline (2026-06-15): per-typ timeout offline z T1/T2/T3 (minuty→sek) zamiast
    # jednego 1920s. Mapa typu HA → param (CLAUDE.md: T1 switch/light, T2 temp/hum sensor,
    # T3 door/leak/motion binary). Bez tego urządzenia bateryjne (Door/Leak/Button) fałszywie
    # mrugały offline co ~32 min. param_sync gotowy przed data.start() (niżej) → late-bind OK.
    def _offline_after_for(dtype):
        key = {'switch': 'T1', 'light': 'T1', 'sensor': 'T2',
               'binary_sensor': 'T3'}.get(dtype, 'T3')
        v = param_sync.get(key)
        return v * 60 if v else None                 # None → fallback skalar offline_after
    data.offline_after_fn = _offline_after_for
    # 2026-07-21: P3 „Raportowanie temp" [min] → interwał `b`-heartbeatu NA ŻYWO (dotąd martwe
    # pokrętło — report_interval był sztywny 900s). Strojenie kadencji z dashboardu bez restartu.
    data.report_interval_fn = lambda: (param_sync.get('P3') or 15) * 60

    # ── F2: SupervisorLinkProbe — aktywny ping/pong sup_ping/sup_pong (siatka bezpieczeństwa
    # na ciszę modelu pasywnego z CLAUDE.md). P4/T4/PR czytane NA ŻYWO z param_sync (strojenie
    # z dashboardu bez restartu, wzorzec jak progi TH/TL wyżej). Callback aktualizuje TYLKO
    # gw_stats (bez wołania publish_gwstat — ta jest zdefiniowana niżej; late-bind przez
    # gwstat_loop/kolejne wywołania, ten sam wzorzec co time_offset/last_sync).
    def _sup_link_params():
        return (param_sync.get('P4'), param_sync.get('T4'), param_sync.get('PR'))

    def _on_sup_link_state(online, lost_pongs):
        gw_stats['sup_link'] = 'ON' if online else 'OFF'
        gw_stats['sup_lost_pong'] = lost_pongs
        try:                                             # #8: zmiana linku widoczna od razu
            publish_gwstat()
        except NameError:
            pass                                         # jeszcze przed definicją (start) — pętla dociągnie

    sup_link_probe = SupervisorLinkProbe(
        gw_id, lora, get_params=_sup_link_params,
        on_state=_on_sup_link_state, logger=log)

    # ── STEP 5: tryb bramki (day/night/all-time) — supresja offline gdy nieaktywna ──
    gm_cfg = CONFIG.get('gateway_mode', {})
    gw_mode = GatewayMode(operating_mode=gm_cfg.get('operating_mode', 'all-time'),
                          day_start=gm_cfg.get('day_start', '06:00'),
                          day_end=gm_cfg.get('day_end', '20:00'),
                          lat=gm_cfg.get('lat'), lon=gm_cfg.get('lon'), logger=log,
                          dusk_margin=gm_cfg.get('offline_dusk_margin', 0),
                          dawn_margin=gm_cfg.get('offline_dawn_margin', 0))
    # 2026-07-18: gating raportowania trybem — nieaktywna bramka nie wysyla stanow do sup.
    data.active_fn = gw_mode.is_active
    try:
        import os as _os
        if _os.path.exists('/tmp/lora_mode_gw.json'):
            _pm = json.load(open('/tmp/lora_mode_gw.json')).get('mode')
            if _pm and gw_mode.set_mode(_pm):
                log.info('MODE', 'tryb bramki z persist: %s' % _pm)
    except Exception as _e:
        log.warn('MODE', 'load persist mode: %s' % _e)

    # ── STEP 5: anomalie (offline+battery+stagnation, ALL devices) → AnomalyBatcher → `ab` P2 ──
    an_cfg = CONFIG.get('anomaly', {})

    # ── URZĄDZENIA WYŁĄCZONE Z ANOMALII (2026-08-24, polecenie usera) ──────────────────────
    # `anomaly.no_anomaly` = wzorce fnmatch (np. 'RA*_S*') urządzeń zainstalowanych na obiekcie,
    # ale NIEUŻYWANYCH — na G1 to 82 czujniki ruchu. Bez tego każdy z nich zgłaszał offline i
    # stagnację, zalewając listy anomalii alarmami o sprzęcie, którego nikt nie obsługuje.
    # Filtr działa U ŹRÓDŁA (silniki offline/stagnacja), więc urządzenia nie wchodzą do zbiorów
    # anomalii → nie ma ich ani w hashu `oh`, ani w dump_anom, ani u supervisora.
    # Anomalie baterii/temp/wilgoci celowo ZOSTAJĄ — user wykluczył wyłącznie offline i stagnację.
    _no_anom_pat = list(an_cfg.get('no_anomaly', []))

    _skip_logged = []                     # jednorazowy log PO zapełnieniu discovery

    def _skip_anom(dev):
        if not _no_anom_pat:
            return False
        import fnmatch
        # Licznik logujemy dopiero przy pierwszym realnym użyciu: przy starcie discovery.devices
        # jest jeszcze puste (z2m dopiero się ogłasza), więc policzenie tu dałoby „0 urządzeń" —
        # diagnostyka, która myli bardziej, niż pomaga.
        if not _skip_logged and discovery.devices:
            _skip_logged.append(True)
            hit = [d for d in discovery.devices if any(fnmatch.fnmatchcase(d, p) for p in _no_anom_pat)]
            log.info('ANOM', f'🔕 anomalie offline/stagnacja WYŁĄCZONE dla {len(hit)}/'
                             f'{len(discovery.devices)} urządzeń (wzorce: {", ".join(_no_anom_pat)})')
        return any(fnmatch.fnmatchcase(dev, pat) for pat in _no_anom_pat)

    if _no_anom_pat:
        log.info('ANOM', f'🔕 wykluczenie anomalii offline/stagnacja aktywne '
                         f'(wzorce: {", ".join(_no_anom_pat)}) — licznik po discovery')

    anomaly_batcher = AnomalyBatcher(
        gw_id, lora_send, interval=an_cfg.get('flush_interval', 30),
        max_payload=an_cfg.get('max_payload', 150), logger=log, arbiter=arbiter)

    # ── BRAMKA = SOURCE OF TRUTH ANOMALII: lokalny store + publikacja na SWOJE HA ──
    # Store karmiony z TEGO SAMEGO strumienia co LoRa (`tee` na anomaly_batcher.add): każdy emit
    # silnika (offline/battery/temphum/stagnacja) + dump trafia i do supervisora (LoRa) i do
    # lokalnego store → publish sensor.lora_an_g1_{offline,battery,other} (count+items+NAZWY,
    # resolve sid→nazwa z discovery.short_rev) + liczniki devices_*. Recovery (dn/bo/to/ho)
    # auto-czyści (handle_ab). on_change → publikacja kubełków (1:1 sup) + encje v10 (per-wpis clear).
    gw_anom_store = AnomalyStore(
        resolve_dev=lambda g, sid: discovery.short_rev.get(sid),
        persist_path=an_cfg.get('gw_persist_path', '/tmp/lora_anomaly_gw.json'), logger=log)
    gw_anom_pub = AnomalyBucketPublisher(gw_anom_store, mqtt, HA_PREFIX, STATE_PREFIX, logger=log)
    gw_anom_v10 = AnomalyHAv10Bridge(gw_anom_store, mqtt, HA_PREFIX, STATE_PREFIX, logger=log)
    # DEBOUNCE leading+trailing (2026-07-08): pierwsza zmiana publikuje NATYCHMIAST (klik clear w
    # popupie → wiersz i licznik znikają od razu, ~0.3s zamiast 1s), a kolejne w oknie 1s koalescują
    # do 1 trailing (przy starcie ~161 offline naraz nadal 1 publikacja — chroni broker/HA + O(n²) v10).
    _anom_pub = {'cooldown': False, 'pending': False}
    _anom_pub_lock = threading.Lock()
    def _do_pub_gw_anom(g):
        try:
            gw_anom_pub.publish(g); gw_anom_v10.publish(g)
        except Exception as e:
            log.warn('ANOM', f'publish gw anom: {e}')
    def _anom_cooldown_end(g):
        with _anom_pub_lock:
            pend = _anom_pub['pending']; _anom_pub['pending'] = False
            if not pend:
                _anom_pub['cooldown'] = False; return
        _do_pub_gw_anom(g)                                      # trailing: publikuj stan finalny burstu
        t = threading.Timer(1.0, _anom_cooldown_end, args=(g,)); t.daemon = True; t.start()
    def _publish_gw_anom(g):
        fire = False
        with _anom_pub_lock:
            if not _anom_pub['cooldown']:
                _anom_pub['cooldown'] = True; fire = True
            else:
                _anom_pub['pending'] = True
        if fire:                                                # leading: pierwsza zmiana od razu
            _do_pub_gw_anom(g)
            t = threading.Timer(1.0, _anom_cooldown_end, args=(g,)); t.daemon = True; t.start()
    gw_anom_store.on_change = _publish_gw_anom

    def _emit_anom(sid, code, value=None):
        # FAZA 1 (2026-07-03): UPLINK NIE per-anomalia `ab` — AnomalyReconciler wysyła AUTORYTATYWNY
        # blob `ansnap` (cała aktywna lista, 1 skompresowany transfer) przy ZMIANIE hasha. Tu tylko
        # karmimy LOKALNY store (źródło blobu + encje HA bramki); on_change → send_snapshot() (niżej).
        entry = [sid, code] if value is None else [sid, code, value]
        gw_anom_store.handle_ab({'g': gw_id, 'd': [entry]})
    anomaly_batcher.add = _emit_anom                     # silniki (niżej) łapią przez emit=

    def _offline_min_for(dtype):                         # minuty per typ (T1/T2/T3) dla anomalii offline
        key = {'switch': 'T1', 'light': 'T1', 'sensor': 'T2',
               'binary_sensor': 'T3'}.get(dtype, 'T3')
        return param_sync.get(key) or 120

    offline_anom = GatewayOfflineAnomaly(
        gw_id, discovery, data, emit=anomaly_batcher.add,
        get_timeout=_offline_min_for, logger=log,
        check_interval=an_cfg.get('offline_check', 30),
        grace=an_cfg.get('offline_grace', 120),
        skip_dev=_skip_anom,                             # nieużywany sprzęt nie zgłasza offline
        is_active=gw_mode.offline_active)                 # #9: offline_active — calendar off-hours dalej wykrywa offline (BMS)

    def _offline_hash():
        """Hash zbioru offline (md5[:8] z posortowanych nazw, '0'=pusty). MUSI być identyczny
        z AnomalyStore.offline_hash na supervisorze (porównanie driftu w HB → szybki reconcile)."""
        import hashlib
        devs = sorted(offline_anom.offline_devices())
        return hashlib.md5("|".join(devs).encode()).hexdigest()[:8] if devs else "0"

    battery_mon = BatteryMonitor(
        gw_id, discovery,
        get_battery=lambda dev: data.states.get(dev, {}).get('battery'),
        emit=anomaly_batcher.add,
        get_thresholds=lambda: (param_sync.get('BL'), param_sync.get('BC')),  # progi z dashboardu (live)
        logger=log, check_interval=an_cfg.get('battery_check', 300))

    # STEP 5 (krok 14): temp/hum high/low dla WSZYSTKICH urządzeń bramki (override usera 2026-06-25)
    temphum_mon = TempHumMonitor(
        gw_id, discovery,
        get_state=lambda dev: data.states.get(dev, {}),
        emit=anomaly_batcher.add,
        get_thresholds=lambda: {                              # progi z dashboardu (live, bez restartu)
            'temp_low': param_sync.get('TL'), 'temp_high': param_sync.get('TH'),
            'hum_low': param_sync.get('HL'), 'hum_high': param_sync.get('HH')},
        logger=log, check_interval=an_cfg.get('temphum_check', 60))

    # ZALANIE/DYM (2026-07-30): silnika NIE BYŁO — kody `wl`/`sk` istniały tylko w mapach etykiet,
    # więc zalanie jechało wyłącznie jako stan urządzenia i nie trafiało do liczników/list/popupu.
    # Check co 10 s (nie 60 jak temp/hum): przy wycieku i dymie liczy się czas reakcji, a sprawdzenie
    # jest lokalne — LoRa rusza dopiero przy ZMIANIE stanu.
    alarm_mon = AlarmMonitor(
        gw_id, discovery,
        get_state=lambda dev: data.states.get(dev, {}),
        emit=anomaly_batcher.add,
        logger=log, check_interval=an_cfg.get('alarm_check', 10))

    # STEP 5: stagnacja ALL devices, emit przez batcher (zamiast bezpośredniego `ab`)
    stagnation = StagnationEngine(
        gw_id, discovery, data, get_thresholds=lambda: (param_sync.get('P1'), param_sync.get('P2')),
        logger=log, check_interval=an_cfg.get('stagnation_check', 300),
        emit=anomaly_batcher.add, all_devices=True,
        skip_dev=_skip_anom,                             # nieużywany sprzęt nie zgłasza stagnacji
        # store anomalii przeżywa restart, a zbiór stagnujących nie — bez tego odtworzenia
        # wracające urządzenie nie emitowało `sc` i wpis `sg` zostawał w store na zawsze.
        restore=lambda: gw_anom_store.cat_devs(gw_id, 'stagnation'))

    def handle_dump_anom(d):                              # STEP 5: live re-check → pełny zrzut anomalii
        if d.get('g') not in (gw_id, None):
            return
        # FAZA 1 + fix responsywności: dump = ŚWIEŻA detekcja. Silniki latchują stan i sprawdzają
        # na własnym interwale (battery 300s / temphum 60s / offline 30s) — sam reconciler.snapshot()
        # zwróciłby STARY latch. Więc najpierw wymuś check() (świeży odczyt + emit przejść), dopiero
        # potem reconciler re-scan (snapshot już aktualny) + ansnap. „Dump" = prawdziwe wymuś-re-scan.
        for _eng in (offline_anom, battery_mon, temphum_mon, alarm_mon, stagnation):
            try:
                if hasattr(_eng, 'check'):
                    _eng.check()
            except Exception as _e:
                log.warn('ANOM', f'dump check {type(_eng).__name__}: {_e}')
        an_reconciler.request(diagnostic=True, force_send=True)
        log.info('ANOM', '🔄 dump_anom → force check silników + reconciler ansnap')

    # ── STEP 4: kalendarz (krok 16) — bramka odbiera harmonogram → ICS + tryb ──
    cal_cfg = CONFIG.get('calendar', {})
    ha_cfg = CONFIG.get('ha_api', {})
    mode_names = {int(k): v for k, v in CONFIG.get('mode_names', {}).items()}
    win_days = cal_cfg.get('window_days', 14)
    scheduler = ScheduleManager(mode_names=mode_names, gateways=[gw_id], logger=log,
                                persist_path=CONFIG.get('state', {}).get('schedule_path', '/tmp/lora_schedule_gw.json'))
    cal_state = {'mode': 0, 'next': 0, 'src': 'none'}

    def publish_calstat():
        mode, nxt, src = scheduler.compute_now_and_next(gw_id)
        cal_state.update(mode=mode, next=nxt, src=src)
        if gw_mode.mode == 'calendar':                       # #9: aktywnosc raportowania wg biezacego trybu kalendarza
            _cn = mode_names.get(mode, mode) if isinstance(mode, int) else mode
            _inact = [str(x).upper() for x in gm_cfg.get('calendar_inactive_modes', ['BRAK PRODUKCJI', 'PRZERWA'])]
            gw_mode.cal_active = str(_cn).upper() not in _inact
        nxt_str = datetime.fromtimestamp(nxt).strftime('%Y-%m-%d %H:%M') if nxt else '--'
        mqtt.publish(f"{STATE_PREFIX}/{gw_lower}/calstat", json.dumps({
            'mode': mode, 'mode_name': mode_names.get(mode, f'MODE_{mode}'),
            'mode_next': nxt_str, 'mode_src': src,
            'cal_hash': scheduler.schedule_hash(gw_id, win_days),
            'slots': len(scheduler.get_effective_schedule(gw_id))},
            separators=(',', ':')), retain=True)
        return mode, nxt, src

    def on_calendar_received(gw, direction, compact):
        """on_received CalendarTransfer: compact [[start_min,dur,mode]] → merge → ICS + tryb."""
        full = scheduler.expand_compact(compact)
        if direction == 'cal':                           # autorytatywny push global → LUSTRO (nie akumuluj)
            n = scheduler.replace_schedule(gw_id, full)
            log.info('CAL', f'📅 odebrano {len(full)} slotów ({direction}) → replace ({n} po podmianie)')
        else:                                            # inne kierunki (np. lokalne) → dotychczasowy merge
            n = scheduler.merge_schedule(gw_id, full)
            log.info('CAL', f'📅 odebrano {len(full)} slotów ({direction}) → merge ({n} zmian)')
        slots = scheduler.get_effective_schedule(gw_id)
        # F2 fix (2026-07-03): FIZYCZNY zapis kalendarza przez HA WS (delete) + service (create_event),
        # NIE przez plik ICS. Plik `.storage/local_calendar.*.ics` jest root:root — bramka (td) nie ma
        # praw zapisu → ICS zawodził po cichu. Encja: config `cal_entity` lub `calendar.lora_<gw>`.
        cal_entity = ha_cfg.get('cal_entity') or f"calendar.lora_{gw_lower}"
        if ha_cfg.get('token'):
            threading.Thread(target=lambda: sync_ha_calendar_ws(
                ha_cfg.get('url', ''), ha_cfg.get('token', ''), cal_entity, slots, mode_names,
                days=win_days, logger=log), daemon=True, name='cal-ws-sync').start()
        else:
            log.info('CAL', '📅 brak HA tokenu → tylko encja trybu (bez zapisu kalendarza HA)')
        publish_calstat()

    # cal_* idą surowym lora (natychmiast) — transfer ma własny CRC+retransmit
    cal_transfer = CalendarTransfer(
        send_fn=lora_send, chunk_size=cal_cfg.get('chunk_size', 60),
        chunk_delay=cal_cfg.get('chunk_delay', 6.0),
        chunk_ack_timeout=cal_cfg.get('chunk_ack_timeout', 30.0),   # anty-spam: round-trip LoRa
        chunk_retries=cal_cfg.get('chunk_retries', 2),
        end_retries=cal_cfg.get('end_retries', 3),
        on_received=on_calendar_received, logger=log,
        arbiter=arbiter, accept_gw=gw_id)      # multi-gw: przyjmij tylko kalendarz g==gw_id lub globalny (None)

    # BULK: ReliableTransfer — skompresowany niezawodny transfer dla mass-eventów (z2m-down →
    # wszystkie offline jednym transferem zamiast 250 timeoutów). Osobny kanał rt_* (nie rusza cal_*).
    rt = ReliableTransfer(send_fn=lora_send, logger=log,
                          chunk_ack_timeout=30.0,
                          chunk_size=CONFIG.get('transfer', {}).get('chunk_size', 60),  # align z cal: niezawodny próg tego łącza LoRa (lekcja RF)
                          chunk_retries=3,  # RTT pod obciążeniem ~60s, 30s toleruje
                          arbiter=arbiter, accept_gw=gw_id)   # multi-gw: transfer cudzej bramki ignorowany (brak cack)
    # (a)+(d) MAPA i pełny sweep availability jako skompresowane bloby przez ReliableTransfer
    # (kind='devmap'/'avail') — zamiast 94 pkt `db` / ~33 pkt `b`/sweep. Kanał rt_* (osobny od cal_*).
    discovery.map_send_fn = lambda payload: rt.start_send(gw_id, 'devmap', payload)
    # AVAIL HASH-GATE (2026-07-11, census: transfer avail co 45min NAWET bez zmian = największy
    # składnik steady-state ~13-16 pkt/45min z ackami). Wysyłaj blob TYLKO gdy zawartość ('a')
    # się zmieniła; wymuszenie co 6h (heartbeat spójności). Bezpieczne: zmiany live płyną w `b`
    # (flagi a:0/1), rozjazd łata reconcile po `oh` z HB.
    _avail_gate = {'h': None, 'ts': 0.0}

    def _send_avail_blob(blob):
        # B1 (2026-08-03): LQI jedzie tym blobem (wypadło z ramek `b`), więc gate nie może patrzeć
        # wyłącznie na 'a' — inaczej link quality zamarzałby do wymuszenia co 6h. Do hasha wchodzi
        # LQI ZGRUBSZONE do kwartyli (0-3): realne pogorszenie łącza (200→60) przepycha blob,
        # a normalny jitter (255→244) nie — czyli dokładnie to, przed czym gate powstał.
        gate_src = {'a': blob.get('a'),
                    'q': {k: int(v) // 64 for k, v in (blob.get('q') or {}).items()}}
        h = hashlib.md5(json.dumps(gate_src, separators=(',', ':'), sort_keys=True)
                        .encode()).hexdigest()[:8]
        now = time.time()
        if h == _avail_gate['h'] and now - _avail_gate['ts'] < 6 * 3600:
            log.info('XFER', f'⏭️ avail-blob pominięty — hash {h} bez zmian (gate 6h)')
            return
        _avail_gate['h'] = h; _avail_gate['ts'] = now
        rt.start_send(gw_id, 'avail', blob)
    data.avail_blob_fn = _send_avail_blob

    # ── BURST `b` → PLIK (2026-09-11, polecenie usera: „burst stanów w pakietach jak
    #    harmonogram"). Zmierzone na G1 przez 56 h: `b` = 443 pojedyncze ramki = 17% CAŁEGO
    #    pasma TX bramki — jedyny duży strumień, którego rebase „wszystko-jako-plik" nie objął
    #    (kalendarz, devmap, `ansnap`, `avail` już nim jadą). Duży flush płacił nagłówek
    #    `{t,g,ts,p}` razy liczba ramek i tyleż razy wchodził na kanał z cooldownem 3 s.
    #    Próg z Batcher.blob_min_packets (3): 1-2 ramki zostają po staremu, bo transfer ma
    #    własny narzut (`rt_begin`/`rt_end` + cack na chunk) i poniżej progu byłby droższy.
    #    TYLKO batcher MONITORED — PRIORITY (leak/smoke/door) musi być natychmiastowy.
    data.mon_batch.blob_fn = lambda payload: rt.start_send(gw_id, 'bdata', payload)

    # ── FAZA 1 (2026-07-03): ANOMALIE JAKO PLIK (ansnap). Bramka = ŹRÓDŁO PRAWDY. Reconciler buduje
    #    autorytatywny blob aktywnych anomalii z LOKALNEGO store → JEDEN skompresowany transfer
    #    rt kind='ansnap' TYLKO przy zmianie hasha (steady-state = 0 uplinku). Supervisor reconciluje
    #    add+remove. Zastępuje per-anomalia `ab` (koniec spamu przy 200+ dev). run_once (interval/dump):
    #    re-scan truth + re-arm ręcznie wyczyszczonych wciąż-trwających (po mute_window) + self-clear
    #    (store kurczy się na recovery → krótszy blob). send_snapshot (on-change): wyślij bez re-scanu. ──
    an_reconciler = AnomalyReconciler(
        gw_id, gw_anom_store,
        probes=[offline_anom.snapshot, battery_mon.snapshot,
                temphum_mon.snapshot, alarm_mon.snapshot, stagnation.snapshot],
        resolve_dev=lambda sid: discovery.short_rev.get(sid),
        dev_to_sid=lambda dev: discovery.short_ids.get(dev),
        readd_fn=lambda sid, code, val=None: gw_anom_store.handle_ab(
            {'g': gw_id, 'd': [[sid, code] if val is None else [sid, code, val]]}),
        snapshot_send_fn=lambda payload: rt.start_send(gw_id, 'ansnap', payload),
        offline_unack=offline_anom.unack,
        # WYCISZENIE offline PRZY NIEAKTYWNEJ BRAMCE (2026-09-17). O swicie caly obwod
        # przekaznikow carportu traci zasilanie, wiec `GatewayOfflineAnomaly` gasi alarm dla
        # WSZYSTKICH — i reconciler kasowal wtedy takze anomalie urzadzen REALNIE martwych.
        # Zmierzone: `RB6_2`/`RA6_2` nie wstaly ani razu przez 5 dni, a ich anomalia znikala
        # co rano o 06:31 i wracala co wieczor o 18:58 — w dzien byly niewidoczne.
        muted_cat=lambda cat: cat == 'offline' and not gw_mode.offline_active(),
        delta_send_fn=lora_send,                 # DELTA (2026-07-07): 1 zmiana = 1 pakiet an_d, nie cały blob
        on_report=lambda rep: log.info('ANOM', f"🔬 ansnap diag {rep.get('gw')}: "
                                       f"truth={rep.get('truth_n')} store={rep.get('store_n')} "
                                       f"missing={rep.get('missing')} stale={rep.get('stale')}"),
        logger=log, interval=an_cfg.get('reconcile_interval', 180),
        mute_window=an_cfg.get('mute_window', 300))
    # store change (add/remove/ręczny clear) → NATYCHMIAST wyślij blob (bez re-scanu → clear nie cofany).
    gw_anom_store.on_change = lambda g: (_publish_gw_anom(g), an_reconciler.send_snapshot())

    _bridge = {'online': True}                           # debounce z2m bridge/state (LWT z2m-down)

    def on_bridge_state(payload):
        p = (payload or '').strip()
        if p.startswith('{'):
            try:
                p = json.loads(p).get('state', '')
            except Exception:
                pass
        online = (p == 'online')
        if online == _bridge['online']:
            return                                        # bez zmiany (np. retained 'online' na starcie)
        _bridge['online'] = online
        if not online:                                   # z2m padł → CAŁOŚĆ niedostępna → jeden bulk
            devs = list(discovery.devices.keys())
            log.warn('XFER', f'🟥 z2m bridge OFFLINE → bulk mass_offline {len(devs)} dev')
            if devs:
                threading.Thread(target=lambda: rt.start_send(gw_id, 'mass_offline', devs),
                                 daemon=True, name='rt-massoff').start()
        else:
            log.info('XFER', '🟩 z2m bridge ONLINE → availability wróci per-device (retained)')

    def push_schedule_up():
        """REVERSE (gw→sup): pchnij harmonogram bramki w górę przez CalendarTransfer (gw_push).
        Źródło: encja gw_calendar_id na HA bramki (jeśli ustawiona) — inaczej bieżący effective."""
        gw_cal_id = cal_cfg.get('gw_calendar_id', '')
        if gw_cal_id and ha_cfg.get('token'):
            src = fetch_ha_calendar(ha_cfg.get('url', ''), ha_cfg.get('token', ''),
                                    calendar_id=gw_cal_id, days=90, logger=log)
            if src:                                      # guard: pusto/błąd ⇒ NIE czyść harmonogramu
                # REPLACE, nie add_slot: przy push-up kalendarz bramki jest ŹRÓDŁEM, więc
                # SKASOWANIE eventu w HA bramki musi zniknąć też u supa. add_slot tylko dokładał
                # → usunięcia nigdy nie propagowały. Symetria z read_ha_calendar na supie.
                scheduler.replace_schedule(gw_id, src)
                log.info('CAL', f'📅 {len(src)} slotów z {gw_cal_id} → źródło push-up')
        compact, h = scheduler.prepare_compact(gw_id, win_days)
        if not compact:
            log.warn('SYNC', '⤴️ Push Up: brak slotów do wysłania'); return
        log.info('SYNC', f'📤 Push Up {gw_id}: {len(compact)} slotów (hash={h}) → supervisor')
        cal_transfer.start_send(gw_id, 'gw_push', compact)

    # ── Dispatcher (LoRa RX from supervisor) ──
    dispatcher = Dispatcher(logger=log)
    dispatcher.register('ping', lambda d: for_me(d) and heartbeat.handle_ping(d))  # multi-gw: ignoruj ping cudzej bramki

    def handle_disc_request(d):
        if not for_me(d): return
        # ANTI-SPAM: bramka też porównuje hash. `disc` niesie hash supervisora (`h` = co MA
        # zarejestrowane). Jeśli == nasz disc_hash → supervisor aktualny → tylko disc_meta
        # (lekki advertise), pomiń pełne `db`. Inaczej (różny/brak) → pełna discovery.
        sup_hash = d.get('h')
        meta_only = bool(sup_hash) and sup_hash == discovery.disc_hash
        if meta_only:
            log.info('CMD', f'⚡ Discovery req — hash supervisora zgodny ({sup_hash}) → meta_only (anti-spam, bez db)')
        else:
            log.info('CMD', f'⚡ Discovery requested (sup_hash={sup_hash} ≠ {discovery.disc_hash}) → pełna')
        # F7: żądanie JAWNE z supervisora → force=True omija anti-spam 180s (odpowiedz zawsze,
        # nawet gdy identyczny hash wysłany przed chwilą — supervisor mógł zgubić poprzedni pakiet)
        threading.Thread(target=lambda: discovery.send_discovery_with_delay(
                         tx_delay, meta_only=meta_only, force=True),
                         daemon=True).start()
    dispatcher.register('disc', handle_disc_request)

    def _apply_cmd(dev, cap, val):
        if dev in discovery.devices:                       # REALNE urządzenie Zigbee → steruj przez Z2M
            z2m_cap = cap if cap != 'state' else 'state'
            mqtt.publish(f'zigbee2mqtt/{dev}/set',
                         json.dumps({z2m_cap: val}, separators=(',', ':')))
            pending_st[dev] = (cap, val, time.time())
            if CONFIG.get('control', {}).get('optimistic_status', False):
                # optimistic: odeślij `st` OD RAZU (nie czekaj na Z2M); realny `st` z
                # propagate_state_from_z2m i tak skoryguje gdy potwierdzenie dotrze/się różni.
                st_send(dev, cap, val)
                log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → Z2M set + st OPTIMISTIC (bez czekania)')
            else:
                log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → Z2M set (czekam na realne potwierdzenie)')
        else:                                              # urządzenie wirtualne (vio) — bez Z2M
            dev_states[dev] = val
            log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → applied (virtual)')
            st_send(dev, cap, val)

    def handle_cmd(d):
        if not for_me(d): return
        _apply_cmd(d.get('d'), d.get('c', 'state'), str(d.get('v', '')).upper())
    dispatcher.register('cmd', handle_cmd)

    # ── MASOWE ON/OFF PRZEZ GRUPY ZIGBEE (2026-09-17, polecenie usera) ──────────────────
    # Bylo: `cmdm` z 78 urzadzeniami = 78 pojedynczych `zigbee2mqtt/<dev>/set` w ciasnej petli.
    # Ramki LoRa dochodzily, ale Zigbee tego nie unosilo — czesc przekaznikow sie nie przelaczala,
    # a potwierdzenia `st` przy 78 sztukach naraz czesciowo ginely. Teraz: 6 groupcastow
    # sekwencyjnie, po kazdym weryfikacja czlonkow i doslanie TYLKO brakujacych — LOKALNIE
    # na bramce, zero ruchu LoRa na krok posredni. Szczegoly: modules/control/group_cast.py.
    z2m_ieee = {}                                        # ieee_address → friendly_name
    gw_ctrl_cfg = CONFIG.get('control', {})              # `ctrl_cfg` istnieje tylko w run_supervisor

    group_caster = GroupCaster(
        lambda topic, payload: mqtt.publish(topic, payload),
        lambda dev: (data.states.get(dev) or {}).get('state'),
        logger=log,
        settle=gw_ctrl_cfg.get('group_settle', 2.0),
        verify_wait=gw_ctrl_cfg.get('group_verify_wait', 8.0),
        retries=gw_ctrl_cfg.get('group_retries', 2))

    def on_z2m_groups(payload):
        """Czlonkostwo czytamy z z2m (`bridge/groups`), NIE z configu — jedno zrodlo prawdy.
        z2m podaje czlonkow po `ieee_address`, wiec tlumaczymy przez mape z `bridge/devices`."""
        try:
            gl = json.loads(payload)
        except Exception:
            return
        grupy = {}
        for g in gl if isinstance(gl, list) else []:
            nazwa = g.get('friendly_name')
            if not nazwa:
                continue
            czlonkowie = []
            for m in g.get('members') or []:
                dev = z2m_ieee.get(m.get('ieee_address'))
                if dev:
                    czlonkowie.append(dev)
            grupy[nazwa] = czlonkowie
        group_caster.set_groups(grupy)
        log.info('GRP', '👥 grupy z2m: ' + (", ".join(f"{k}={len(v)}" for k, v in sorted(grupy.items()))
                                            or 'brak'))

    def handle_cmdm(d):
        """Ramka ZBIORCZA: jedna komenda, lista urządzeń (masowe ON/OFF z dashboardu)."""
        if not for_me(d): return
        cap = d.get('c', 'state'); val = str(d.get('v', '')).upper()
        devs = d.get('d') or []
        log.info('CTRL', f'📦 cmdm {len(devs)} urządzeń {cap}={val}')
        for dev in devs:                                 # pending_st = zrodlo `st` w gore (bez zmian)
            if dev in discovery.devices:
                pending_st[dev] = (cap, val, time.time())
        prog = gw_ctrl_cfg.get('group_min_devices', 6)
        if len(devs) >= prog and group_caster.plan(devs)[0]:
            # apply() spi miedzy grupami i podczas weryfikacji → osobny watek, zeby nie blokowac
            # dispatchera (kolejne ramki LoRa musza byc obslugiwane w trakcie).
            threading.Thread(target=lambda: group_caster.apply(devs, cap, val),
                             daemon=True, name='groupcast').start()
            return
        for dev in devs:
            _apply_cmd(dev, cap, val)
    dispatcher.register('cmdm', handle_cmdm)

    def propagate_state_from_z2m(topic, payload):
        """STEP 4 fix: KAŻDA zmiana stanu sterowalnego urządzenia (switch/light) z Z2M →
        odeślij `st` z PRAWDZIWĄ wartością. Działa dla:
          • naszego cmd (pending) — potwierdzenie, czyści pending_cmd na supervisorze,
          • zmian ZEWNĘTRZNYCH — przycisk fizyczny, dashboard bramki, bezpośrednio Z2M
            (nie było obsłużone: supervisor zostawał ze starym stanem + złym licznikiem).
        Efekt: urządzenie ONLINE i odbicie stanu po obu stronach + licznik (handle_st)."""
        dev = topic[len('zigbee2mqtt/'):]
        info = discovery.devices.get(dev)
        if not info or info.get('type') not in ('switch', 'light'):
            return                                         # sensory/binary jadą ścieżką danych `b`
        try:
            data = json.loads(payload)
        except Exception:
            return
        if not isinstance(data, dict):
            return
        cap = pending_st[dev][0] if dev in pending_st else 'state'   # cmd→jego cap; zewn.→state
        z2m_cap = cap if cap != 'state' else 'state'
        if z2m_cap not in data:
            return                                         # ten pakiet nie niesie sterowanego cap
        real = str(data[z2m_cap]).upper()
        was_pending = pending_st.pop(dev, None) is not None
        if not was_pending and gw_mode is not None and not gw_mode.is_active():
            return                                         # 2026-07-18: tryb nieaktywny -> nie raportuj zewnetrznej zmiany stanu (`st`) do sup; cmd-confirm zawsze leci
        if not was_pending and last_fwd_state.get(dev) == (cap, real):
            return                                         # zewn. powtórka bez zmiany → nie spamuj
        last_fwd_state[dev] = (cap, real)
        # BURST-GUARD (2026-07-07, audyt: restart z2m/HA → retained stany WSZYSTKICH przekaźników
        # → 105× pojedynczych `st` = flood LoRa). Fala zewnętrznych st (>6 w 10 s) → kolejne stany
        # jadą ZBIORCZO batchem `b` (caps 's', wiele dev/pakiet) zamiast per-dev st. Potwierdzenia
        # cmd (was_pending) NIGDY nie są tłumione — sterowanie zawsze natychmiastowe.
        now_b = time.time()
        st_burst[:] = [t for t in st_burst if now_b - t < 10.0]
        if not was_pending and len(st_burst) >= 6:
            if len(st_burst) == 6:
                log.warn('CTRL', '⚠️ st-burst (restart z2m?) → kolejne stany zbiorczo w batchu `b`')
            st_burst.append(now_b)
            data._enqueue(dev, {'s': 1 if real == 'ON' else 0}, force=True, available=True)
            return
        st_burst.append(now_b)
        log.info('CTRL', f'🎛️ {dev} {cap}={real} (z2m, {"cmd" if was_pending else "zewn."}) '
                 f'→ st (online+licznik)')
        st_send(dev, cap, real)
        # licznik bramki: gwstat_loop (co 300s); supervisor przelicza po `st` (handle_st → _set_avail)

    def handle_vsw(d):
        if not for_me(d): return
        vid = d.get('id'); val = 1 if str(d.get('v')) in ('1', 'ON', 'on', 'True') else 0
        vio_states[vid] = 'ON' if val else 'OFF'
        log.info('VIO', f'🔘 VSwitch {vid} → {vio_states[vid]} (z LoRa)')
        lora_send({'t': 'vsw_st', 'g': gw_id, 'id': vid, 'v': val})
    dispatcher.register('vsw', handle_vsw)

    def handle_vbtn(d):
        if not for_me(d): return
        vid = d.get('id')
        log.info('VIO', f'🔘 VButton {vid} pressed (z LoRa)')
        lora_send({'t': 'vbtn_ack', 'g': gw_id, 'id': vid})
    dispatcher.register('vbtn', handle_vbtn)

    def _apply_time_sync(sec, src='sync', rtt=None):
        # F(2026-07-18): wspólna aktualizacja czasu supervisora — używane przez `sync` ORAZ
        # `sup_pong` (pong niesie sec co P4). Dotąd tylko `sync`, który dla bramek z NTP nigdy
        # nie lecił (time_quality='ntp' poza triggerem resync na sup) → sup_time/offset/last_sync
        # ZAMARZAŁY. Pong odświeża je co ping (bez nowego ruchu LoRa).
        # C3 (2026-08-03, decyzja usera): kafel „offsetu" pokazywał dotąd CAŁE opóźnienie LoRa
        # (potrafiło być +21 s), a nie dryf zegarów — obie maszyny mają NTP i realnie się nie
        # rozjeżdżają. Pakiet `sec` opisuje zegar supervisora z chwili WYSŁANIA pongu, czyli
        # sprzed ~RTT/2. Odejmujemy tę połowę: zostaje to, co jest naprawdę różnicą zegarów.
        # UWAGA (uczciwie): łącze jest ASYMETRYCZNE (gw→sup ~5 s, sup→gw 16-72 s pod ruchem),
        # więc RTT/2 to przybliżenie, nie pomiar. Dlatego RTT pokazujemy obok offsetu — kafel
        # ma mówić, na czym liczba stoi. Ścieżka `sync` (inicjuje sup) nie zna RTT → bez korekty.
        try:
            base = time.time() - (rtt / 2.0 if rtt else 0.0)
            offset = int(round(base - int(sec)))
        except (TypeError, ValueError):
            return
        gw_stats['time_offset'] = f'{offset:+d}s'
        gw_stats['sup_rtt'] = (f'{rtt:.1f}s' if rtt else gw_stats.get('sup_rtt', '--'))
        gw_stats['last_sync'] = datetime.now().strftime('%H:%M:%S')
        gw_stats['_last_sync_ts'] = time.time()      # znacznik świeżości → time_quality='synced'
        try:
            gw_stats['sup_time'] = datetime.fromtimestamp(int(sec)).strftime('%H:%M:%S')
        except (TypeError, ValueError, OSError):
            pass
        log.info('CMD', f'🕐 {src}: offset={offset:+d}s (time_quality→synced)')
        publish_gwstat()                             # natychmiast (nie czekaj na pętlę)

    def handle_sync(d):
        if not for_me(d): return
        sec = d.get('sec')
        if sec:
            _apply_time_sync(sec, 'sync')
    dispatcher.register('sync', handle_sync)
    dispatcher.register('req', lambda d: for_me(d) and data.handle_req(d))   # STEP 3: refresh on-demand (per-gw)
    def handle_set_mode(d):                              # 2026-07-18: supervisor->bramka zmiana trybu (LoRa)
        if not for_me(d): return
        if gw_mode.set_mode(d.get('m')):
            _persist_mode_gw(gw_mode.mode)
            log.info('MODE', '🕹️ tryb bramki (LoRa sup) -> %s, aktywna=%s' % (gw_mode.mode, gw_mode.is_active()))
            publish_gmstat(); heartbeat.send_heartbeat()   # 2026-07-18: od razu rozglos tryb do sup
    dispatcher.register('set_mode', handle_set_mode)
    def handle_sup_pong(d):
        if not for_me(d): return
        sup_link_probe.handle_sup_pong(d)            # F2: link alive
        sec = d.get('sec')
        if sec:                                       # F(2026-07-18): pong niesie czas supervisora → odśwież
            _apply_time_sync(sec, 'pong', rtt=getattr(sup_link_probe, 'last_rtt', None))
        _apply_zg(d.get('zg'))                        # 2026-07-28: zgodność policzona przez supa

    def _apply_zg(z):
        """4 flagi zgodności bramka↔supervisor: z ramki `zg` (push supa przy zmianie) LUB
        z `sup_pong` (darmowy refresher). Pong sam nie wystarcza — probe pinguje tylko przy
        ciszy na łączu, więc na ruchliwym łączu kafel nigdy by się nie zapełnił."""
        if z is not None and gw_stats.get('zg') != z:  # publikuj tylko przy ZMIANIE
            gw_stats['zg'] = z
            publish_gwstat()                          # late-bind (jak time_offset/last_sync)
        return True
    dispatcher.register('sup_pong', handle_sup_pong)  # F2 + czas z pongu
    dispatcher.register('zg', lambda d: for_me(d) and _apply_zg(d.get('z')))  # 2026-07-28

    def handle_ac_b(d):                                  # CLEAR supervisor→bramka: usuń z lokalnego store + ACK
        if d.get('g') not in (gw_id, None):
            return
        from modules.anomaly.store import CODE_LABEL, CODE_CAT
        label2code = {v: k for k, v in CODE_LABEL.items()}
        fine_cats = set(CODE_CAT.values())               # offline,battery,stagnation,temp,hum,smoke,water
        n = 0
        for item in d.get('d', []):
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            dev, label = item[0], item[1]
            # FAZA 1: mute (dev,cat) → reconciler nie re-armnie wciąż-trwającej przed mute_window.
            if label == 'other':                             # bucket zbiorczy → cały kubełek 'other'
                an_reconciler.mute(dev, 'other')
                n += gw_anom_store.remove_one(gw_id, dev, 'other')
            elif label in fine_cats:                         # kategoria (== klucz store; też sup ccat)
                an_reconciler.mute(dev, label)
                n += gw_anom_store.remove_cat(gw_id, dev, label)
                if label == 'offline':
                    offline_anom.ack_clear(dev)
            else:                                            # type_label (np. 'temp_high') → code → cat
                code = label2code.get(label)
                cat = CODE_CAT.get(code) if code else None
                if cat:
                    an_reconciler.mute(dev, cat)
                    n += gw_anom_store.remove_cat(gw_id, dev, cat)
                    if cat == 'offline':
                        offline_anom.ack_clear(dev)
                else:
                    an_reconciler.mute(dev, 'other')
                    n += gw_anom_store.remove_one(gw_id, dev, 'other')
        if n and log:
            log.info('ANOM', f'🧹 ac_b: wyczyszczono {n} anomalii (clear z supervisora) + ACK offline')
    dispatcher.register('ac_b', handle_ac_b)             # CLAUDE.md: {t:ac_b,g,d:[["Dev","temp_high"]]}

    def handle_ac_all(d):                                # F2 fix: CLEAR-ALL kubełka KOMPAKTOWO (flaga, nie lista dev)
        # Zastępuje oversized ac_b z listą 192 urządzeń (~4KB >>220B, cicho ginął). Bramka=źródło:
        # czyści CAŁY kubełek lokalnie + mute (nie re-arm) + ack_clear offline, potem ansnap w górę.
        if not for_me(d):
            return
        bucket = d.get('b', 'offline')
        cnt = 0
        for it in list(gw_anom_store.items(gw_id, bucket)):
            dev = it['dev']
            for (g, dv, cat) in list(gw_anom_store.anomalies):
                if g == gw_id and dv == dev and gw_anom_store._bucket(cat) == bucket:
                    an_reconciler.mute(dev, cat)
            if bucket == 'offline':
                offline_anom.ack_clear(dev)
            cnt += gw_anom_store.remove_one(gw_id, dev, bucket)
        log.info('ANOM', f'🧹 ac_all: clear-all [{bucket}] (z supervisora, kompaktowo) n={cnt}')
        an_reconciler.request(force_send=True)
    dispatcher.register('ac_all', handle_ac_all)         # F2: {t:ac_all,g,b:offline|battery|other}
    dispatcher.register('dump_anom', handle_dump_anom)   # STEP 5: reconcyliacja anomalii
    dispatcher.register('ansnap_req', lambda d: for_me(d) and an_reconciler.request(force_send=True))  # FAZA 1: sup żąda blobu
    dispatcher.register('cal_pull', lambda d: for_me(d) and threading.Thread(  # FAZA 3B: sup Pull ← bramka → push-up
        target=push_schedule_up, daemon=True, name='cal-pull-up').start())
    for _ct in ('cal_begin', 'cal_chunk', 'cal_end', 'cal_ack', 'cal_cack'):  # STEP 4: transfer kalendarza (per-chunk ACK)
        dispatcher.register(_ct, cal_transfer.dispatch)  # handle_end → on_calendar_received
    for _rt in ('rt_begin', 'rt_chunk', 'rt_end', 'rt_ack', 'rt_cack'):       # BULK: mass_offline ACK/cack
        dispatcher.register(_rt, rt.dispatch)
    # TIMEOUTY/PARAMETRY: TYLKO per-bramka (user 2026-07-03) — for_me odrzuca params cudzej bramki.
    dispatcher.register('params', lambda d: for_me(d) and param_sync.handle_remote(d))      # STEP 17: proposal z supervisora
    dispatcher.register('params_req', lambda d: for_me(d) and param_sync.handle_remote(d))  # STEP 17: żądanie pełnego stanu
    dispatcher.register('cfg', lambda d: log.info('RX', 'cfg (passthrough)'))
    # ACK batcha priorytetowego z supervisora → koniec retransmisji tego pakietu.
    dispatcher.register('back', lambda d: for_me(d) and data.ack_pri(d.get('p')))

    def handle_dsnap_req(d):
        """B6: sup wykrył rozjazd hasha `dh` → odeślij PEŁNY stan monitored+priority JEDNYM
        skompresowanym transferem (wzorzec 1:1 z `ansnap`/`devmap`). To zamiennik periodycznego
        heartbeatu wartości: zamiast nadawać w kółko „na wszelki wypadek", nadajemy raz i tylko
        wtedy, gdy lustro faktycznie się rozjechało."""
        if not for_me(d):
            return
        snap = {}
        for dev, info in list(discovery.devices.items()):
            if not discovery.is_monitored(dev):
                continue
            sid = discovery.short_ids.get(dev)
            if sid is None:
                continue
            st = data.states.get(dev, {})
            caps = {k: st[k] for k in TYPE_CAPS.get(info.get('type', 'sensor'), ()) if k in st}
            if caps:
                snap[str(sid)] = encode_short(caps, available=data._alive.get(dev, True))
                data.note_sent(dev, caps)                 # B6: blob to też wysyłka → karmi `dh`
        # PUSTY ZRZUT TEŻ JEST ZRZUTEM (2026-08-24). Wcześniej brak wartości = ciche `return`,
        # co zakleszczało system dokładnie w stanie carportu za dnia: przekaźniki bez zasilania ⇒
        # zero wartości ⇒ bramka `dh="0"`, a supervisor z wartościami sprzed 10 dni prosił o
        # dsnap w kółko i nigdy go nie dostawał. „Nie mam żadnych wartości" to pełnoprawna
        # odpowiedź — dopiero ona pozwala supervisorowi wyczyścić lustro i zejść się hashem.
        if not snap:
            log.info('DATA', f'↻ dsnap_req [{gw_id}]: brak wartości → wysyłam PUSTY zrzut '
                             f'(lustro supervisora ma się wyczyścić)')
        else:
            log.info('DATA', f'↻ dsnap_req [{gw_id}]: {len(snap)} urządzeń → blob (rozjazd hasha)')
        rt.start_send(gw_id, 'dsnap', {'ts': int(time.time()), 's': snap})
    dispatcher.register('dsnap_req', handle_dsnap_req)
    dispatcher.set_fallback(lambda d: log.debug('RX', f'unknown t={d.get("t")}'))

    # ── Local MQTT (gateway broker) ──
    def on_mqtt(topic, payload):
        if topic == 'zigbee2mqtt/bridge/devices':
            discovery.parse_z2m(payload)
            try:                                         # mapa ieee→nazwa dla `bridge/groups`
                for _d in json.loads(payload):
                    if _d.get('ieee_address') and _d.get('friendly_name'):
                        z2m_ieee[_d['ieee_address']] = _d['friendly_name']
            except Exception:
                pass
            for dev in discovery.devices:                # STEP 3: lokalne encje LQI
                reg_gw_lqi(dev)
            # Fix 2026-07-22 (wgrany live 2026-07-30): retained zigbee2mqtt/<dev> może dojść
            # PRZED sparsowaniem bridge/devices → on_z2m() je odrzuca (discovery.devices puste),
            # a door/leak wiszą na availability-only do najbliższego fizycznego wyzwolenia.
            # Re-subskrypcja każe brokerowi dosłać retained teraz. Lokalny broker, 0 ruchu LoRa.
            mqtt.client.unsubscribe('zigbee2mqtt/+')
            mqtt.client.subscribe('zigbee2mqtt/+')
        elif topic == 'zigbee2mqtt/bridge/groups':       # czlonkostwo grup → GroupCaster
            on_z2m_groups(payload)
        elif topic == 'zigbee2mqtt/bridge/state':        # BULK: z2m up/down → mass_offline transfer
            on_bridge_state(payload)
        elif topic.endswith('/availability'):           # FAZA 2: z2m native availability (autorytet)
            data.on_z2m_availability(topic, payload)
        elif topic.startswith('zigbee2mqtt/'):          # STEP 3: device state → delta/batch
            data.on_z2m(topic, payload)
            propagate_state_from_z2m(topic, payload)     # STEP 4: cmd ORAZ zmiana zewn. → st (online+licznik)
        elif topic.startswith(f'{STATE_PREFIX}/params/gateway/set/'):   # STEP 17: edycja z HA bramki
            param_sync.on_mqtt_set(topic, payload)
        elif topic.startswith(f'{STATE_PREFIX}/params/gateway/cmd/'):   # przyciski Send Config/Timeout
            param_sync.on_cmd(topic, payload)
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/ping':
            log.info('BTN', '🔘 Ping button pressed'); heartbeat.send_heartbeat()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/discovery':
            log.info('BTN', '🔘 Discovery button pressed')
            # F7: ręczny przycisk = jawne żądanie → force=True omija anti-spam 180s disc_meta/vio
            threading.Thread(target=lambda: discovery.send_discovery_with_delay(tx_delay, force=True),
                             daemon=True).start()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/dump':
            log.info('BTN', '🔘 Dump anomalii (bramka) pressed')     # F5: reconcyliacja teraz
            handle_dump_anom({'g': gw_id})
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/sync_req':
            log.info('BTN', '🔘 Sync czasu (bramka) pressed')        # F5: poproś supervisora o sync
            lora_send({'t': 'sync_req', 'g': gw_id})
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/push_schedule':
            log.info('BTN', '🔘 Push Schedule Up pressed')          # STEP 4: reverse gw→sup
            threading.Thread(target=push_schedule_up, daemon=True, name='cal-push-up').start()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/sync_schedule':
            log.info('BTN', '🔘 Sync Schedule (pobierz) pressed')   # STEP 4: pull down — poproś supervisora
            lora_send({'t': 'cal_req', 'g': gw_id})
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/set_mode':
            _lbl = payload.decode('utf-8') if isinstance(payload, (bytes, bytearray)) else str(payload or '')
            _lbl = _lbl.strip()
            _m = {'Całodobowa': 'all-time', 'Dzienna': 'day', 'Nocna': 'night', 'Kalendarz': 'calendar'}.get(_lbl)
            if _m and gw_mode.set_mode(_m):
                _persist_mode_gw(_m)
                log.info('MODE', '🕹️ tryb bramki -> %s (%s), aktywna=%s' % (_lbl, _m, gw_mode.is_active()))
                publish_gmstat(); heartbeat.send_heartbeat()   # 2026-07-18: od razu rozglos tryb do sup (HB niesie gm/ga)
            else:
                log.warn('MODE', 'set_mode: zignorowano %r' % _lbl)
        elif topic.startswith(f'{STATE_PREFIX}/anomaly/') and topic.endswith('/clear'):
            # CLEAR per-wpis z HA BRAMKI (button.lora_an_<safe>_clear v10) → usuń z lokalnego store + ACK.
            # Bramka=source: przestaje raportować → supervisor prune'uje na dump_anom (spójność w górę).
            safe_id = AnomalyHAv10Bridge.safe_from_clear_topic(topic)
            info = gw_anom_v10.lookup(safe_id) if safe_id else None
            if info:
                cg, cd, ccat = info
                an_reconciler.mute(cd, ccat)                  # FAZA 1: nie re-armnij przed mute_window
                if ccat == 'offline':
                    offline_anom.ack_clear(cd)
                n = gw_anom_store.remove_cat(cg, cd, ccat)
                log.info('ANOM', f'🗑️ clear v10 (bramka) {cd} kat={ccat} n={n}')
            else:
                log.warn('ANOM', f'clear v10 (bramka): nieznana encja {safe_id}')
        elif topic in (f'{STATE_PREFIX}/gw/{gw_lower}/cmd/clear_offline',
                       f'{STATE_PREFIX}/gw/{gw_lower}/cmd/clear_battery',
                       f'{STATE_PREFIX}/gw/{gw_lower}/cmd/clear_other'):
            bucket = topic.rsplit('_', 1)[1]              # CLEAR-ALL kubełka z HA bramki
            cnt = 0
            for it in list(gw_anom_store.items(gw_id, bucket)):
                dev = it['dev']
                # FAZA 1: mute wszystkie fine-cat tego dev w kubełku (reconciler nie re-armnie)
                for (g, dv, cat) in list(gw_anom_store.anomalies):
                    if g == gw_id and dv == dev and gw_anom_store._bucket(cat) == bucket:
                        an_reconciler.mute(dev, cat)
                if bucket == 'offline':
                    offline_anom.ack_clear(dev)
                cnt += gw_anom_store.remove_one(gw_id, dev, bucket)
            log.info('ANOM', f'🧹 clear-all {bucket} (bramka): {cnt}')
        elif topic.startswith(f'{STATE_PREFIX}/vio/') and topic.endswith('/state'):
            vid = topic.split('/')[2]                     # hydratacja: retained stan vswitcha → vio_states
            if vid in vio_states:
                vio_states[vid] = 'ON' if payload.upper() in ('ON', '1') else 'OFF'
        elif topic == f'{STATE_PREFIX}/gw/cmd/clear_anomaly':
            # UNIFIED DASHBOARD (2026-07-08): clear z popupu na HA BRAMKI (bramka=źródło prawdy).
            # payload {dev,bucket} = single; {bucket,all:1} = cały kubełek. Delta an_d propaguje sama.
            try:
                info = json.loads(payload)
                cb = info.get('bucket', 'offline')
                if info.get('all'):
                    n = 0
                    for (g, dv, cat) in list(gw_anom_store.anomalies):
                        if g == gw_id and gw_anom_store._bucket(cat) == cb:
                            an_reconciler.mute(dv, cat)
                            if cb == 'offline':
                                offline_anom.ack_clear(dv)
                            n += gw_anom_store.remove_one(gw_id, dv, cb)
                    log.info('ANOM', f'🗑️ clear-all [{cb}] z HA bramki n={n}')
                else:
                    cd = info['dev']
                    for (g, dv, cat) in list(gw_anom_store.anomalies):
                        if g == gw_id and dv == cd and gw_anom_store._bucket(cat) == cb:
                            an_reconciler.mute(cd, cat)
                    if cb == 'offline':
                        offline_anom.ack_clear(cd)
                    n = gw_anom_store.remove_one(gw_id, cd, cb)
                    log.info('ANOM', f'🗑️ clear ręczny {cd} [{cb}] z HA bramki n={n}')
            except Exception as e:
                log.warn('ANOM', f'gw clear_anomaly bad payload {payload!r}: {e}')
        elif topic.startswith(f'{STATE_PREFIX}/vio/') and topic.endswith('/set'):
            vid = topic.split('/')[2]
            val = 'ON' if payload.upper() in ('ON', '1') else 'OFF'
            vio_states[vid] = val
            mqtt.publish(f'{STATE_PREFIX}/vio/{vid}/state', val, retain=True)
            log.info('VIO', f'🔘 VSwitch {vid} → {val} (lokalny)')
        elif topic.startswith(f'{STATE_PREFIX}/vio/') and topic.endswith('/press'):
            vid = topic.split('/')[2]
            log.info('VIO', f'🔘 VButton {vid} pressed (lokalny)')

    def on_lora(text):
        try:
            t = json.loads(text).get('t', '?')
            gw_stats['last_sup_rx_ts'] = time.time()
            gw_stats['last_sup_rx'] = f"{datetime.now().strftime('%H:%M:%S')} ({t})"
            sup_link_probe.note_rx()          # F2: DOWOLNA wiadomość od supervisora = dowód życia
        except Exception:
            pass
        dispatcher.dispatch_raw(text)

    mqtt._on_message_cb = on_mqtt
    lora.on_receive = on_lora
    mqtt.subscribe('zigbee2mqtt/bridge/devices')
    mqtt.subscribe('zigbee2mqtt/bridge/groups')          # masowe ON/OFF przez groupcast
    mqtt.subscribe('zigbee2mqtt/bridge/state')           # BULK: z2m bridge up/down (LWT) → mass_offline
    mqtt.subscribe('zigbee2mqtt/+')                      # STEP 3: device states
    mqtt.subscribe('zigbee2mqtt/+/availability')         # FAZA 2: z2m native availability
    mqtt.subscribe(f'{STATE_PREFIX}/gw/{gw_lower}/cmd/#')
    mqtt.subscribe(f'{STATE_PREFIX}/anomaly/+/clear')    # v10: clear per-anomalia z HA bramki (button.lora_an_*_clear)
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/state')   # hydratacja: retained stan vswitchy → vio_states
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/set')
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/press')
    mqtt.subscribe(f'{STATE_PREFIX}/gw/cmd/clear_anomaly')   # UNIFIED: clear z popupu HA bramki
    mqtt.subscribe(f'{STATE_PREFIX}/params/gateway/set/+')   # STEP 17: edycja parametrów
    mqtt.subscribe(f'{STATE_PREFIX}/params/gateway/cmd/+')   # przyciski Send Config/Timeout

    log.info('MQTT', f'Connecting {mqtt_cfg["host"]}:{mqtt_cfg["port"]}...')
    mqtt.start()
    log.info('MQTT', '✅ Connected' if mqtt.wait_connected(timeout=10) else 'Connection timeout')

    log.info('LORA', 'Connecting...')
    lora.start()
    for label, info in lora.get_status().items():
        log.info('LORA', f'{label}: {"✅ OK" if info["connected"] else "❌ FAIL"}')

    ha.reg_gw_buttons_local(gw_id)
    ha.reg_gw_local_stats(gw_id)
    for eid, nm, icon, key in [                           # kafelki hash w Bramce (deviceless)
            ('param_hash', 'Hash Parametrów', 'mdi:tune-variant', 'param_hash'),
            ('disc_hash', 'Hash Disc', 'mdi:fingerprint', 'disc_hash'),
            ('anom_hash', 'Hash Anomalii', 'mdi:alert-decagram', 'anom_hash'),  # 2026-07-29
            # 2026-07-28: zgodność z supervisorem (z sup_pong) — te same napisy co na HA supa,
            # żeby kafel SYNCHRONIZACJA był 1:1 na obu dashboardach.
            ('zg_all', 'Zgodność OGÓLNA', 'mdi:shield-check', 'zg_all'),
            ('zg_disc', 'Disc — zgodność', 'mdi:check-decagram', 'zg_disc'),
            ('zg_param', 'Param — zgodność', 'mdi:check-decagram', 'zg_param'),
            ('zg_cal', 'Kalendarz — zgodność', 'mdi:calendar-check', 'zg_cal'),
            ('zg_anom', 'Anomalie — zgodność', 'mdi:alert-decagram', 'zg_anom'),
            # 2026-08-03: 5. flaga + jej hash. Stan urządzeń pilnuje dziś hash `dh` (B6), nie
            # periodyk — więc rozjazd lustra ma być widoczny tak samo jak disc/param/cal/anom.
            # 2026-08-26: kafel pokazuje `status_hash` (co bramka WIE), nie `data_hash` (co
            # wysłała) — ten drugi po restarcie stoi na 0 i wyglądał jak awaria. Zgodność lustra
            # nadal pilnuje osobna flaga „Dane — zgodność".
            ('data_hash', 'Hash Status', 'mdi:database-check', 'status_hash'),
            ('zg_data', 'Dane — zgodność', 'mdi:database-sync', 'zg_data')]:
        uid = f"lora_{gw_lower}_{eid}"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": f"GW {gw_id} {nm}", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gw_lower}/gwstat",
            "value_template": f"{{{{ value_json.{key} | default('--') }}}}",
            "icon": icon}, separators=(',', ':')), retain=True)
    # liczniki total/priority/offline POD urządzeniem LoRa Gateway G1 (user 2026-06-10);
    # świeże uid 'gwd_' aby HA utworzył je pod device (stare lora_*_gw_total były deviceless+manglowane)
    _gw_di = {"identifiers": [f"lora_gateway_{gw_lower}"]}
    for eid, nm, icon, key in [
            ('gwd_total', 'Total', 'mdi:devices', 'total'),
            ('gwd_priority', 'Priority', 'mdi:alert-octagon', 'priority'),
            ('gwd_offline', 'Offline', 'mdi:lan-disconnect', 'offline')]:
        uid = f"lora_{gw_lower}_{eid}"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": nm, "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gw_lower}/gwstat",
            "value_template": "{{ value_json.%s | default(0) }}" % key,
            "icon": icon, "device": _gw_di}, separators=(',', ':')), retain=True)
    for vio in vio_config:
        if vio['type'] == 'switch':
            ha.reg_vswitch(gw_id, vio['id'], vio['name'], vio.get('default', 0))
        else:
            ha.reg_vbutton(gw_id, vio['id'], vio['name'])

    heartbeat.start()
    data.start()                                         # STEP 3: start batchers
    param_sync.register_entities()                       # STEP 17→F2: 15 encji number (6 param + 6 progi + P4/T4/PR) na HA bramki
    stagnation.start()                                   # STEP 15: pętla stagnacji
    anomaly_batcher.start()                              # STEP 5: flush `ab` co flush_interval
    gw_anom_pub.publish(gw_id)                            # STEP 5: encje anomalii bramki (offline/battery/other + devices_*) — start z 0
    offline_anom.start()                                 # STEP 5: gateway-side offline (do/dn)
    battery_mon.start()                                  # STEP 5: low/critical battery (lb/cb/bo)
    temphum_mon.start()                                  # STEP 5: temp/hum high/low (th/tl/hh/hl) ALL devices
    alarm_mon.start()                                    # 2026-07-30: zalanie/dym (wl/sk + wo/ko) ALL devices
    an_reconciler.start()                                # FAZA 1: pętla re-scan/re-arm + uplink ansnap
    sup_link_probe.start()                               # F2: aktywny ping/pong sup_ping/sup_pong (siatka na ciszę)

    # #2 (2026-07-17): USUNIĘTO auto sync_req_loop bramki — sync czasu inicjuje SUPERVISOR na
    # podstawie `tq` z pong (pierwszy kontakt / tq nieświeży), zgodnie z „supervisor inicjalizuje
    # po wykryciu rozjazdu". Przycisk sync_req (F5) zostaje jako ręczny fallback.
    log.info('MAIN', f'  tryb bramki: {gw_mode.mode} (aktywna teraz: {gw_mode.is_active()})')

    # STEP 4: encje trybu/harmonogramu (pod urządzeniem LoRa Gateway Gx)
    _cal_di = {"identifiers": [f"lora_gateway_{gw_lower}"]}
    for eid, nm, icon, key in [
            ('mode', 'Tryb', 'mdi:calendar-clock', 'mode_name'),
            ('mode_next', 'Tryb — następna zmiana', 'mdi:calendar-arrow-right', 'mode_next'),
            ('cal_slots', 'Harmonogram (slotów)', 'mdi:calendar-multiple', 'slots'),
            ('cal_hash', 'Hash Kalendarza', 'mdi:fingerprint', 'cal_hash')]:
        uid = f"lora_{gw_lower}_{eid}"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": f"GW {gw_id} {nm}", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gw_lower}/calstat",
            "value_template": "{{ value_json.%s | default('--') }}" % key,
            "icon": icon, "device": _cal_di}, separators=(',', ':')), retain=True)
    _pu_uid = f"lora_{gw_lower}_push_schedule"            # STEP 4: przycisk reverse gw→sup (push w górę)
    mqtt.publish(f"{HA_PREFIX}/button/{_pu_uid}/config", json.dumps({
        "name": f"GW {gw_id} Push Schedule Up", "object_id": _pu_uid, "unique_id": _pu_uid,
        "command_topic": f"{STATE_PREFIX}/gw/{gw_lower}/cmd/push_schedule",
        "device": _cal_di, "icon": "mdi:calendar-upload"}, separators=(',', ':')), retain=True)
    _ss_uid = f"lora_{gw_lower}_sync_schedule"            # STEP 4: przycisk pull w dół (poproś supervisora)
    mqtt.publish(f"{HA_PREFIX}/button/{_ss_uid}/config", json.dumps({
        "name": f"GW {gw_id} Sync Schedule", "object_id": _ss_uid, "unique_id": _ss_uid,
        "command_topic": f"{STATE_PREFIX}/gw/{gw_lower}/cmd/sync_schedule",
        "device": _cal_di, "icon": "mdi:calendar-sync"}, separators=(',', ':')), retain=True)
    # STEP 5: encja trybu PRACY bramki (day/night/all-time) — widoczność supresji offline.
    # state = czytelna etykieta, attr.ga = czy aktywna (0 → offline anomalie wstrzymane).
    _GM_LABELS = {"all-time": "Całodobowa", "day": "Dzienna", "night": "Nocna", "calendar": "Kalendarz"}
    _gm_uid = f"lora_{gw_lower}_gateway_mode"
    mqtt.publish(f"{HA_PREFIX}/sensor/{_gm_uid}/config", json.dumps({
        "name": f"GW {gw_id} Tryb pracy bramki", "object_id": _gm_uid, "unique_id": _gm_uid,
        "state_topic": f"{STATE_PREFIX}/{gw_lower}/gmstat",
        "value_template": "{{ value_json.label | default('--') }}",
        "json_attributes_topic": f"{STATE_PREFIX}/{gw_lower}/gmstat",
        "icon": "mdi:theme-light-dark", "device": _cal_di}, separators=(',', ':')), retain=True)

    def publish_gmstat():
        st = gw_mode.state()                             # {'gm': tryb, 'ga': 0/1}
        mqtt.publish(f"{STATE_PREFIX}/{gw_lower}/gmstat", json.dumps({
            "gm": st.get("gm"), "ga": st.get("ga"),
            "label": _GM_LABELS.get(st.get("gm"), st.get("gm")),
            "active": "aktywna" if st.get("ga") else "wstrzymana (supresja offline)"},
            separators=(',', ':')), retain=True)
    publish_gmstat()
    # 2026-07-18: SELECT trybu bramki (dropdown) -> operating_mode w runtime + gating raportu.
    _gm_sel_uid = f"lora_{gw_lower}_tryb_bramki"
    mqtt.publish(f"{HA_PREFIX}/select/{_gm_sel_uid}/config", json.dumps({
        "name": f"GW {gw_id} Tryb bramki", "object_id": _gm_sel_uid, "unique_id": _gm_sel_uid,
        "command_topic": f"{STATE_PREFIX}/gw/{gw_lower}/cmd/set_mode",
        "options": ["Całodobowa", "Dzienna", "Nocna", "Kalendarz"],
        "state_topic": f"{STATE_PREFIX}/{gw_lower}/gmstat",
        "value_template": "{{ value_json.label | default('Całodobowa') }}",
        "icon": "mdi:theme-light-dark", "device": _cal_di}, separators=(',', ':')), retain=True)
    def _persist_mode_gw(m):
        try:
            open('/tmp/lora_mode_gw.json', 'w').write(json.dumps({'mode': m}))
        except Exception as _e:
            log.warn('MODE', 'persist mode: %s' % _e)
    publish_calstat()                                    # bieżący tryb (z persist /tmp/lora_schedule_gw.json)

    # ── STEP 5+: wizualizacja CZASU + PORY DNIA na dashboardzie bramki (MQTT discovery) ──
    for eid, nm, icon, key in [
            ('local_time', 'Czas bramki', 'mdi:clock-digital', 'now'),
            ('time_quality', 'Jakość czasu', 'mdi:clock-check-outline', 'tq_label'),
            ('time_offset', 'Offset vs supervisor', 'mdi:clock-fast', 'offset'),
            ('sun_period', 'Pora dnia', 'mdi:theme-light-dark', 'period_label'),
            ('sunrise', 'Świt', 'mdi:weather-sunset-up', 'sunrise'),
            ('sunset', 'Zmierzch', 'mdi:weather-sunset-down', 'sunset')]:
        uid = f"lora_{gw_lower}_{eid}"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": f"GW {gw_id} {nm}", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gw_lower}/timestat",
            "value_template": "{{ value_json.%s | default('--') }}" % key,
            "icon": icon, "device": _cal_di}, separators=(',', ':')), retain=True)

    def publish_timestat():
        now = time.time()
        di = gw_mode.day_info(now)
        tq = _time_quality()
        sunrise = '%02d:%02d' % (di['sunrise_min'] // 60, di['sunrise_min'] % 60)
        sunset = '%02d:%02d' % (di['sunset_min'] // 60, di['sunset_min'] % 60)
        mqtt.publish(f"{STATE_PREFIX}/{gw_lower}/timestat", json.dumps({
            'now': datetime.fromtimestamp(now).strftime('%H:%M:%S'),
            'tq': tq, 'tq_label': TQ_LABEL.get(tq, tq),
            'offset': gw_stats.get('time_offset', '--'),
            'last_sync': gw_stats.get('last_sync', '--'),
            'period': 'day' if di['is_day'] else 'night',
            'period_label': 'Dzień ☀️' if di['is_day'] else 'Noc 🌙',
            'sunrise': sunrise, 'sunset': sunset},
            separators=(',', ':')), retain=True)
    publish_timestat()

    def calendar_loop():                                 # przeliczaj tryb co minutę (zmiana slotu)
        while running.is_set():
            time.sleep(60)
            publish_calstat()
            publish_gmstat()                             # STEP 5: odśwież tryb pracy (przejścia day/night)
            publish_timestat()                           # STEP 5+: czas + pora dnia (świt/zmierzch, time_quality)
    threading.Thread(target=calendar_loop, daemon=True, name='cal-mode').start()
    # STEP 17: bez pre-instancji — sync parametrów wyzwala hash `ph` w HB/pong (niżej)

    time.sleep(3)
    if discovery.devices:
        log.info('DISC', f'✅ {len(discovery.devices)} devices z Z2M')
        for dev in discovery.devices:                    # STEP 3: lokalne encje LQI
            reg_gw_lqi(dev)
        threading.Thread(target=lambda: discovery.send_discovery_with_delay(tx_delay),
                         daemon=True).start()
    else:
        log.warn('DISC', 'Brak Z2M devices — wyślę discovery gdy Z2M dostarczy listę')

    log.info('MAIN', '═' * 50)
    log.info('MAIN', f'GATEWAY {gw_id} running (STEP 5 — ANOMALIE + TRYB BRAMKI)')
    log.info('MAIN', f'  batch: monitored {data.mon_batch.interval}s / priority {data.pri_batch.interval}s (delta)')
    log.info('MAIN', f'  push (down): cal_* → expand+merge → ICS + sensor.lora_{gw_lower}_mode (hash cal w HB)')
    log.info('MAIN', f'  pull (up):   przycisk Push Schedule Up → gw_push → supervisor')
    log.info('MAIN', f'  slotowanie: {"ON okno "+str(slot.window()) if slot else "OFF"} (batch w slocie; pong/cal natychmiast)')
    log.info('MAIN', '═' * 50)

    def publish_gwstat():
        now = time.time()
        # F2: sup_link jest teraz napędzany przez SupervisorLinkProbe (gw_stats['sup_link'],
        # aktualizowane z _on_sup_link_state — aktywny ping/pong + note_rx na KAŻDYM ruchu od
        # supervisora). `linked`/SUP_LINK_TIMEOUT zostają jako fallback, gdyby klucz kiedyś zniknął.
        linked = gw_stats['last_sup_rx_ts'] > 0 and (now - gw_stats['last_sup_rx_ts']) < SUP_LINK_TIMEOUT
        total = len(discovery.devices)
        prio = sum(1 for d in discovery.devices.values() if d.get('priority'))
        mon = sum(1 for d in discovery.devices.values()           # wyłączny (bez priority)
                  if d.get('monitored') and not d.get('priority'))
        # FIX 2026-07-07 (audyt: kafel OFFLINE=2 vs 174): licz z AUTORYTATYWNEGO data._alive
        # (z2m availability + per-typ timeout — to samo źródło co report_liveness/anomalie),
        # nie z legacy skalarnego timeoutu last_msg_ts. Fallback na starą pętlę przed 1. sweepem.
        alive_map = dict(getattr(data, '_alive', {}) or {})
        if alive_map:
            offline = sum(1 for v in alive_map.values() if v is False)
        else:
            oa = getattr(data, 'offline_after', 0) or 0           # legacy: cisza z2m > offline_after
            offline = 0
            if oa:
                for dev in discovery.devices:
                    ts = data.last_msg_ts.get(dev)
                    if ts is not None and (now - ts) > oa:
                        offline += 1
                    elif ts is None and (now - data._start_ts) > oa:
                        offline += 1
        last_hb = (datetime.fromtimestamp(heartbeat._last_hb).strftime('%H:%M:%S')
                   if heartbeat._last_hb else '--')
        # 5 flag od supa: disc/param/cal/anom/DANE (piąta doszła 2026-08-03 — hash `dh` z B6).
        # Zgodność liczy sup (ma obie strony), bramka tylko odbija. Starszy supervisor pchający
        # 4 znaki nie wywraca kafli: brakująca flaga zostaje '--', a nie fałszywe ✅.
        _ZG_N = 5
        _zg = str(gw_stats.get('zg') or '')
        _zgv = [('zgodny ✅' if c == '1' else 'ROZJAZD ⚠️') for c in _zg[:_ZG_N]]
        _zgv += ['--'] * (_ZG_N - len(_zgv))
        _zg_all = ('WSZYSTKO ZGODNE ✅' if _zg == '1' * _ZG_N
                   else 'ROZJAZD ⚠️' if len(_zg) == _ZG_N else '--')
        ha.pub_gw_stats(gw_id, {
            'uptime': int(now - heartbeat.start_time),
            'total': total, 'monitored': mon, 'priority': prio, 'offline': offline,
            'last_hb': last_hb,
            'sup_link': gw_stats.get('sup_link', 'ON' if linked else 'OFF'),
            'sup_lost_pong': gw_stats.get('sup_lost_pong', 0),   # F2: licznik epizodów braku pong
            'sup_last_rx': gw_stats['last_sup_rx'],
            'time_offset': gw_stats['time_offset'], 'last_sync': gw_stats['last_sync'],
            'sup_rtt': gw_stats.get('sup_rtt', '--'),        # C3: na czym stoi offset (RTT/2)
            'sup_time': gw_stats.get('sup_time', '--'),      # #8: czas supervisora (z sync)
            'anom_hash': (getattr(an_reconciler, '_last_hash', '') or 'brak'),  # 2026-07-29:
                                                             # pusty = zero aktywnych anomalii
            'disc_hash': discovery.disc_hash or '--',        # STEP 17: hash disc
            'param_hash': param_sync.params_hash(),          # STEP 17: hash parametrów
            'data_hash': data.state_hash(),                  # B6: hash stanu urządzeń (5. flaga)
            'status_hash': data.status_hash(),               # 2026-08-26: hash stanu ZNANEGO bramce
            'zg_disc': _zgv[0], 'zg_param': _zgv[1],         # 2026-07-28: zgodność z sup_pong
            'zg_cal': _zgv[2], 'zg_anom': _zgv[3],
            'zg_data': _zgv[4], 'zg_all': _zg_all})          # 2026-08-03: 5. flaga DANE

    publish_gwstat()

    def gwstat_loop():
        # #8: 300s→60s. To publikacja na LOKALNY broker (zero LoRa), a przy 300s dashboard
        # bramki pokazywał link/czas sprzed 5 minut (po restarcie: '--'/offline). Zmiany
        # istotne (sync, zmiana linku) publikują się dodatkowo event-driven.
        while running.is_set():
            time.sleep(60); publish_gwstat()
    threading.Thread(target=gwstat_loop, daemon=True).start()

    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass
    heartbeat.running = False; stagnation.stop(); offline_anom.stop(); battery_mon.stop(); temphum_mon.stop()
    anomaly_batcher.stop(); data.stop(); lora.stop(); mqtt.stop()
    log.info('MAIN', 'Stopped.')


# ── Supervisor mode ─────────────────────────────────────
def run_supervisor(log):
    running = threading.Event(); running.set()
    mqtt_cfg = CONFIG['mqtt']

    mqtt = MqttTransport(
        host=mqtt_cfg['host'], port=mqtt_cfg['port'],
        user=mqtt_cfg['user'], password=mqtt_cfg['pass'],
        client_id=f"step3_sup_{int(time.time())}", logger=log)

    lora = _make_lora(CONFIG, log)
    CONFIG['_lora_tx_queue'] = None     # kolejka TX wyłączona (single-drainer wieszał się na zawieszonym
    #                                     CP2102 send → cały TX stop). Właściwy fix = unified state file
    #                                     (mniej wiadomości = brak potrzeby serializacji). Anomalie: fallback.
    ha = HAEntities(mqtt, logger=log, gw_name_fmt="LoRa {gw}",
                    vio_name_fmt="LoRa {gw} Virtual I/O", vio_entity_prefix="")
    known_gateways = CONFIG.get('gateways', ['G1'])

    def send_to_all(msg):
        lora.send(json.dumps(msg, separators=(',', ':')))

    sup_disc = SupervisorDiscovery(
        ha, logger=log,
        # ANTI-SPAM: dołącz hash KTÓRY SUPERVISOR MA (gw_synced) → bramka porówna i wyśle
        # pełne `db` tylko gdy różny; zgodny → sama meta. Mutualne porównanie hashy.
        # RE-SYNC GUARD (B): hash dołączamy TYLKO gdy mirror kompletny. Przy niepełnym
        # pusty `h` → bramka nie pójdzie w meta_only i odeśle pełną devmapę (koniec
        # samo-blokady, w której fałszywy stempel synced unieważniał własną naprawę).
        request_disc_fn=lambda gw: send_to_all({"t": "disc", "g": gw,
                                                "h": sup_disc.synced_hash_for_request(gw)}),
        # F7 (cichy start): persist gw_devices+gw_synced → po restarcie supervisora hashe
        # zgadzają się od razu (note_hash nie żąda niczego) → koniec sztormu devmap/db po starcie.
        persist_path='/tmp/lora_sup_devices.json')

    # STEP 3: data layer — `b` → merged HA state
    dev_avail = {}                       # {(gw,dev): bool} — REALNY licznik offline (A)

    # ack_offline: urządzenia offline RĘCZNIE wyczyszczone (acknowledged) — anomalia NIE wraca
    # przez reconcile, dopóki urządzenie nie wróci online i nie padnie PONOWNIE (nowe zdarzenie).
    # Rozdziela AVAILABILITY (live truth, encja *_available zawsze prawdziwa) od ANOMALII (alert,
    # ack-owalny). Fix: clear realnie-offline urządzenia działa (wcześniej reconcile re-dodawał).
    ack_offline = set()                  # {(gw,dev)}

    # STEP 5: aktywność bramek (z HB ga) — bramka nieaktywna (np. nocna w dzień) → supresja offline
    gw_active = {}                                        # gw → bool (domyślnie aktywna)

    def _set_avail(gw, dev, on):         # availability = ZAWSZE live truth; anomalia = na transition (chyba że ack)
        prev = dev_avail.get((gw, dev))
        dev_avail[(gw, dev)] = on
        ha.pub_device_avail(gw, dev, on)
        if prev != on:
            if on:
                ack_offline.discard((gw, dev))           # recovery → kasuj ack (re-alert gdy znów padnie)
                anomaly_store.remove_one(gw, dev, 'offline')
            elif (gw, dev) not in ack_offline:           # offline transition → anomalia (jeśli nie ack)
                # BRAMKA NIEAKTYWNA (HB `ga:0`, np. nocna w dzień) → availability leci dalej jako
                # PRAWDA (kafel ma pokazać OFFLINE, nie stan sprzed nocy), ale to nie jest awaria:
                # carport w dzień świadomie odcina zasilanie obwodu przekaźników. Bez tego gate'u
                # blob `avail` odtwarzał 78 anomalii, które bramka właśnie zgasiła trybem —
                # i store'y rozjeżdżały się co sweep (flapping `ah` + dump_anom w kółko).
                # `gw_active` karmi HB (linia ~2455); domyślnie brak wpisu = aktywna.
                if gw_active.get(gw, True):
                    anomaly_store.add_offline(gw, dev)

    # on_avail (batch `a:`) ROUTOWANE przez _set_avail → trafia też do listy offline (kluczowy fix
    # spójności: wcześniej batch pomijał _set_avail i lista nie zgadzała się z licznikiem).
    sup_data = SupervisorData(ha, sup_disc, logger=log,
                              on_avail=lambda gw, dev, av: _set_avail(gw, dev, av))
    # ACK batcha priorytetowego (2026-07-30): `b` z polem `p` → natychmiast `{t:back,g,p}` (P0 w
    # kolejce TX). Bramka dzięki temu wie, że alarm dotarł, i nie dubluje go w ciemno.
    sup_data.ack_fn = lambda gw, pid: send_to_all({"t": "back", "g": gw, "p": pid})
    # hydratacja: retained stany encji HA przychodzą PRZED db (lista z LoRa) → buforuj po
    # (gw,safe) i zahydratuj w on_db gdy znamy nazwy urządzeń (fix klobberu pierwszego `b`).
    pending_hydrate = {}

    # ── STEP 4: kalendarz (krok 16) — SUPERVISOR = master, push do bramek ──
    cal_cfg = CONFIG.get('calendar', {})
    ha_cfg = CONFIG.get('ha_api', {})
    mode_names = {int(k): v for k, v in CONFIG.get('mode_names', {}).items()}
    win_days = cal_cfg.get('window_days', 14)
    cal_id = cal_cfg.get('calendar_id', 'calendar.lora_global')
    scheduler = ScheduleManager(mode_names=mode_names, gateways=known_gateways, logger=log,
                                persist_path='/tmp/lora_schedule_sup.json')
    # ChannelArbiter supervisora: transfer-plik (kalendarz push w dół / odbiór devmap/ansnap/
    # gw_push od bramek) trzyma kanał — wolumen supervisora czeka. accept_gw=None → agreguje
    # transfery od WSZYSTKICH bramek (per-gw filtr jest po stronie bramek).
    arbiter = ChannelArbiter(logger=log)
    lora.arbiter = arbiter                       # jw. — supervisor też odkłada bulk podczas transferu
    cal_transfer = CalendarTransfer(
        send_fn=send_to_all, chunk_size=cal_cfg.get('chunk_size', 60),
        chunk_delay=cal_cfg.get('chunk_delay', 6.0),
        chunk_ack_timeout=cal_cfg.get('chunk_ack_timeout', 30.0),   # anty-spam: round-trip LoRa
        chunk_retries=cal_cfg.get('chunk_retries', 2),
        end_retries=cal_cfg.get('end_retries', 3), logger=log,
        arbiter=arbiter, accept_gw=None)
    expected_cal_hash = {gw: scheduler.schedule_hash(gw, win_days) for gw in known_gateways}
    cal_sync_guard = {}                                  # gw → ts ostatniego auto-sync (debounce drift)
    dsnap_guard = {}                                     # B6: gw → ts ostatniej prośby o dsnap
    gw_push_inflight = {}                                # gw → ts cal_begin(gw_push): transfer w drodze

    # BULK: odbiór ReliableTransfer (mass_offline z bramki = z2m-down → cała bramka niedostępna).
    # _set_avail to JEDNO źródło prawdy availability (idempotentne) → licznik+lista offline spójne.
    def on_rt_received(gw, kind, payload):
        if kind == 'bdata' and isinstance(payload, dict):
            # BURST `b` JAKO PLIK (2026-09-11) — duży flush monitored przyjechał JEDNYM
            # transferem zamiast kilkoma ramkami `b`. Treść jest identyczna z ramką `b`
            # (`{ts, d:[[sid, fields], ...]}`), więc oddajemy ją TEJ SAMEJ ścieżce — zero
            # rozjazdu semantyki między transportem ramkowym a plikowym. Bez `p`:
            # ReliableTransfer ma własny CRC+ACK, drugi ACK (`back`) byłby podwójny.
            n = len(payload.get('d') or [])
            sup_data.handle_b({'t': 'b', 'g': gw, 'ts': payload.get('ts'),
                               'd': payload.get('d') or []})
            log.info('XFER', f'📥 bdata [{gw}] {n} urządzeń → handle_b (1 transfer zamiast ~'
                             f'{max(1, -(-n // 6))} ramek `b`)')
        elif kind == 'mass_offline' and isinstance(payload, list):
            for dev in payload:
                _set_avail(gw, dev, False)
            log.warn('XFER', f'📥 mass_offline [{gw}] {len(payload)} dev → availability OFF (z2m-down)')
        elif kind == 'devmap' and isinstance(payload, dict):
            # (a) skompresowana MAPA urządzeń (zamiast `db`) → rejestracja encji jak handle_db
            sup_disc.handle_devmap(gw, payload)
            # FIX 2026-07-15: pełna ścieżka jak on_db — encje app-level (link_quality/contact/
            # last_seen/refresh/stagnant) + licznik offline + ghost prune (dotąd tylko przy `db`)
            augment_mirror(gw)
            publish_offline_count(gw)
            anomaly_store.ghost_prune(gw, list(sup_disc.devices(gw).keys()))
        elif kind == 'avail' and isinstance(payload, dict):
            # (d) pełny sweep availability jednym blobem {ts, a:{sid:0/1}} → update wszystkich naraz
            bits = payload.get('a', {})
            lqs = payload.get('q', {})       # B1 (2026-08-03): LQI przyjechało blobem, nie w `b`
            rev = {info.get('sid'): name for name, info in sup_disc.devices(gw).items()}
            n = 0
            for sid, bit in bits.items():
                try:
                    sid_i = int(sid)
                except (TypeError, ValueError):
                    sid_i = sid
                dev = rev.get(sid_i)
                if dev:
                    _set_avail(gw, dev, bool(bit)); n += 1
                    if bit:
                        # B3+B4 (2026-07-31): od kiedy bramka NIE wysyła heartbeatów wartości,
                        # zegar offline supervisora nie może wisieć na ramkach `b` — inaczej każde
                        # urządzenie, które po prostu nie zmieniło stanu, poszłoby w fałszywy
                        # offline po T1/T2/T3. Blob availability jest teraz nośnikiem liveness:
                        # bramka mówi wprost, kto żyje. Gdy padnie CAŁA bramka, blob przestaje
                        # przychodzić i timeouty odpalają jak dotąd (właściwa semantyka).
                        sup_data.last_msg_ts[(gw, dev)] = time.time()
                        lq = lqs.get(sid)
                        if lq is not None:
                            # merge w stan urządzenia → value_json.linkquality (encja LQI mirrora)
                            sup_data.apply_st(gw, dev, {'linkquality': lq})
            log.info('XFER', f'📥 avail blob [{gw}] {n}/{len(bits)} sid → availability + zegar offline'
                             + (f' + LQI {len(lqs)}' if lqs else ''))
        elif kind == 'dsnap' and isinstance(payload, dict):
            # B6: pełny stan urządzeń bramki po rozjeździe hasha — jeden transfer zamiast
            # periodycznych powtórek. Wchodzi tą samą ścieżką co `b` (handle_b), żeby merge,
            # publikacja encji, zegar offline i licznik dostępności działały identycznie.
            snap = payload.get('s', {})
            entries = []
            for sid, short in snap.items():
                try:
                    entries.append([int(sid), short])
                except (TypeError, ValueError):
                    entries.append([sid, short])
            # ZRZUT JEST AUTORYTATYWNY: najpierw kasujemy stare wartości, potem nanosimy nowe.
            # `handle_b` merge'uje (dict.update), więc bez tego czyszczenia wartości, których
            # bramka już nie ma, zostawałyby w lustrze i `dh` nigdy by się nie zszedł.
            _cleared = sup_data.clear_values(gw)
            if entries:
                sup_data.handle_b({'t': 'b', 'g': gw, 'ts': payload.get('ts'), 'd': entries})
            log.info('XFER', f'📥 dsnap [{gw}] {len(entries)} urządzeń (wyczyszczono {_cleared}) '
                             f'→ lustro zsynchronizowane (hash {sup_data.state_hash(gw)})')
        elif kind == 'ansnap' and isinstance(payload, dict):
            # FAZA 1: autorytatywny blob aktywnych anomalii z bramki → reconcile store sup (add+remove)
            _reconcile_ansnap(gw, payload)
        else:
            log.warn('XFER', f'rt kind={kind} nieobsłużony (payload {type(payload).__name__})')
    rt = ReliableTransfer(send_fn=send_to_all, on_received=on_rt_received, logger=log,
                          chunk_size=CONFIG.get('transfer', {}).get('chunk_size', 60),  # align z cal: niezawodny próg tego łącza LoRa (lekcja RF)
                          arbiter=arbiter, accept_gw=None)   # agreguj transfery wszystkich bramek

    # ANTY-FLOOD (2026-08-05): wszystkie trzy ścieżki ponawiania `disc` (note_hash,
    # check_complete, fallback po pingu) pytają transport, czy od tej bramki COKOLWIEK leci.
    #
    # DLACZEGO DOWOLNY KIND, A NIE TYLKO 'devmap': ChannelArbiter serializuje transfery, więc
    # dopóki trwa mass_offline/anomalie/kalendarz, devmap NIE MOŻE przyjść równolegle — żądanie
    # `disc` w tym oknie jest z definicji bezcelowe, a kosztuje ramkę i 3 s kanału. Zmierzone na
    # starcie 2026-08-05: transfer mass_offline 108 s, a w jego trakcie 7 żądań `disc` w 2 min;
    # mapa i tak poszła dopiero PO nim. Gate zawężony do 'devmap' nie ruszyłby tam palcem.
    sup_disc.xfer_busy_fn = lambda gw: rt.receiving(gw)

    def on_calendar_received_sup(gw, direction, compact):
        """REVERSE (gw_push): bramka pushuje swój harmonogram → NADPISZ per-bramka źródło supa.

        Decyzja usera 2026-07-30 (symetria per-bramka, oba kierunki tylko na przycisk):
        push z supa nadpisuje kalendarz bramki, push z bramki nadpisuje calendar.lora_<gw>.
        Dlatego oprócz zapisu ICS MUSI iść replace_schedule + expected_cal_hash — inaczej
        najbliższy HB widzi 'cal drift' i sup odpycha swoją starą wersję w dół (efekt znika).
        Pętli nie ma: żaden z kierunków nie wywołuje drugiego, a po podmianie hashe są zgodne."""
        full = scheduler.expand_compact(compact)
        gw_push_inflight.pop(gw, None)                       # transfer zakończony → drift znów aktywny
        scheduler.replace_schedule(gw, full)                 # sup: per-gw źródło = stan bramki
        expected_cal_hash[gw] = scheduler.schedule_hash(gw, win_days)   # ↑ bez tego: drift → re-push w dół
        mirror_tmpl = ha_cfg.get('mirror_ics_path', '')
        log.info('CAL', f'📥 gw_push {gw}: {len(full)} slotów → nadpisuję calendar.lora_{gw.lower()} '
                        f'(hash={expected_cal_hash[gw]})')
        if mirror_tmpl:
            ics_path = mirror_tmpl.format(gw=gw.lower())
            content = build_ics(full, mode_names, cal_name=f"LoRa {gw}")

            def _write():
                if write_ics_atomic(ics_path, content, logger=log):
                    reload_local_calendar(ha_cfg.get('url', ''), ha_cfg.get('token', ''), logger=log)
            threading.Thread(target=_write, daemon=True, name=f'ha-cal-{gw}').start()
        else:
            threading.Thread(target=lambda: write_ha_calendar(
                ha_cfg.get('url', ''), ha_cfg.get('token', ''),
                f"calendar.lora_{gw.lower()}", full, mode_names, logger=log),
                daemon=True, name=f'ha-cal-{gw}').start()
    cal_transfer.on_received = on_calendar_received_sup  # STEP 4: odbiór reverse (gw→sup)

    # ── STEP 4: safe window (krok 7) — TX do bramki tylko gdy słyszeliśmy ją w oknie ──
    slot_cfg = CONFIG.get('slotting', {})
    safe = (SafeWindow(window_seconds=slot_cfg.get('safe_window_seconds', 10), logger=log)
            if slot_cfg.get('enabled') else None)

    cal_btn_regd = {'global': False}; cal_btn_gw = set()

    def reg_cal_button_global():
        if cal_btn_regd['global']:
            return
        uid = 'lora_sup_calendar_all'
        mqtt.publish(f"{HA_PREFIX}/button/{uid}/config", json.dumps({
            "name": "Sync Calendar All", "object_id": uid, "unique_id": uid,
            "command_topic": f"{STATE_PREFIX}/supervisor/cmd/calendar_all",
            "icon": "mdi:calendar-sync"}, separators=(',', ':')), retain=True)
        cal_btn_regd['global'] = True

    def reg_cal_button_gw(gw):
        if gw in cal_btn_gw:
            return
        gl = gw.lower()
        uid = f"lora_gw_{gl}_calendar"
        mqtt.publish(f"{HA_PREFIX}/button/{uid}/config", json.dumps({
            "name": f"GW {gw} Sync Calendar", "object_id": uid, "unique_id": uid,
            "command_topic": f"{STATE_PREFIX}/supervisor/cmd/{gl}/calendar",
            "device": {"identifiers": [f"lora_gateway_{gl}"]},
            "icon": "mdi:calendar-sync"}, separators=(',', ':')), retain=True)
        cal_btn_gw.add(gw)

    def read_ha_calendar():
        """Per-bramka + global baza (2026-07-25): calendar.lora_global → replace_global (WSPÓLNA
        baza, pusta=czysto per-gw) ORAZ per bramkę calendar.lora_<gl> → replace_schedule(gw).
        effective(gw)=GLOBAL+gw (prepare_compact). Guard `if slots` = zero clobber na pustym/błędzie."""
        _url, _tok = ha_cfg.get('url', ''), ha_cfg.get('token', '')
        total = 0
        _base = fetch_ha_calendar(_url, _tok, calendar_id=cal_id, days=90, logger=log)
        if _base is not None:
            scheduler.replace_global(_base); total += len(_base)
        for _cgw in known_gateways:
            _ent = f"calendar.lora_{_cgw.lower()}"
            _gs = fetch_ha_calendar(_url, _tok, calendar_id=_ent, days=90, logger=log)
            if _gs is not None:
                scheduler.replace_schedule(_cgw, _gs); total += len(_gs)
                log.info('CAL', f'📅 {_cgw}: {len(_gs)} slotów z {_ent} (per-bramka źródło)')
        return total

    def targeted_calendar_sync(gateways=None):
        """Push kompaktu do bramek (online + w safe window) przez CalendarTransfer."""
        targets = gateways or cal_cfg.get('enabled_gateways', known_gateways)

        def _run():
            for gw in targets:
                if not sup_hb.gateways.get(gw, {}).get('online'):
                    log.warn('SYNC', f'⏭️ {gw} offline — pomijam sync kalendarza'); continue
                # UWAGA: kalendarz NIE blokuje się na safe-window — CalendarTransfer ma własny
                # CRC + ACK + retransmit (kolizjo-tolerancyjny). Safe-window blokował ręczny push.
                compact, h = scheduler.prepare_compact(gw, win_days)
                if not compact:
                    log.info('SYNC', f'📅 {gw} — brak eventów'); continue
                log.info('SYNC', f'📤 {gw}: wysyłam {len(compact)} slotów (hash={h})')
                cal_transfer.start_send(gw, 'cal', compact)
                expected_cal_hash[gw] = h
                time.sleep(15)                            # rozsuń transfery między bramkami
            log.info('SYNC', f'🏁 sync kalendarza zakończony {targets}')
        threading.Thread(target=_run, daemon=True, name='cal-sync').start()

    # STEP 17→F3: parametry — MIRROR per-BRAMKA (bramka = master). Jedna instancja ParamSync
    # na każdą bramkę (lazy — 1. kontakt HB/param_upd/MQTT tworzy), własny persist + encje pod
    # device LoRa Gateway Gx + topics lora/params/supervisor/<gl>/... (routing niżej).
    sup_params = {}                                       # gw → ParamSync (mirror per-bramka)

    def get_sup_params(gw):
        if not gw or gw == '?':
            return None
        ps = sup_params.get(gw)
        if ps is None:
            persist = f'/tmp/lora_params_sup_{gw.lower()}.json'
            fresh = not os.path.exists(persist)
            ps = ParamSync(
                'supervisor', gw, mqtt, send_to_all, logger=log,
                persist_path=persist,
                ha_prefix=HA_PREFIX, state_prefix=STATE_PREFIX,
                on_change=lambda k, v, _g=gw: log.info('PARAM', f'↻ mirror[{_g}] {k}={v}'))
            sup_params[gw] = ps
            ps.register_entities(); ps.subscribe()
            # #9 (2026-07-17): pull ZAWSZE przy tworzeniu mirrora (start sesji = brak edycji do
            # zclobberowania; bramka=master). Dotąd tylko „świeży persist" → po restarcie sup
            # mirror miał stary/domyślny stan ≠ bramka → param_ok=ROZJAZD na zawsze. 1 pakiet/sesja.
            log.info('PARAM', f'⚙️ {gw}: mirror utworzony → params_req (pull od bramki, master)')
            ps.request()
        return ps


    # UNIFIKACJA 2026-06-20: USUNIĘTO supervisorowy OfflineMonitor (liczył offline z CISZY LoRa
    # = false-positive, bo bramka minimalizuje LoRa → ciche-online urządzenie wyglądało offline;
    # źródło rozjazdu "bramka vs supervisor"). Availability = JEDNO źródło: offline-set bramki
    # (z2m, przez ab/dump→anomaly_store) OR cmd-failure (sticky). Patrz reconcile_availability niżej.
    # Supresja trybu bramki zachowana U ŹRÓDŁA (GatewayOfflineAnomaly.is_active → brak `do`).

    # ── STEP 5: store anomalii (lista/dashboard/popup) + dump_anom reconcyliacja ──
    an_cfg = CONFIG.get('anomaly', {})
    ha_cfg5 = CONFIG.get('ha_api', {})
    an_regd = set()

    def reg_anomaly_entities(gw):
        if gw in an_regd:
            return
        gl = gw.lower(); di = {"identifiers": [f"lora_gateway_{gl}"]}
        # Encje w formacie KONSUMOWANYM przez dashboard.yaml (v38-compat) → istniejące karty
        # anomalii ożywają bez edycji dashboardu. items: sensor.lora_an_<gl>_<bucket>
        # (attr items=[{dev,type,value,detected_at,gw}]); count: sensor.lora_gw_<gl>_devices_*.
        for bucket, nm, icon, cnt_eid in [
                ('offline', 'Anomalie Offline', 'mdi:lan-disconnect', None),       # count = devices_offline (status)
                ('battery', 'Anomalie Bateria', 'mdi:battery-alert', 'devices_low_battery'),
                ('other',   'Anomalie Inne',    'mdi:alert-circle',  'devices_anomaly')]:
            topic = f"{STATE_PREFIX}/gw/{gl}/an_{bucket}"
            iuid = f"lora_an_{gl}_{bucket}"               # items (źródło popupu)
            mqtt.publish(f"{HA_PREFIX}/sensor/{iuid}/config", json.dumps({
                "name": f"{nm}", "object_id": iuid, "unique_id": iuid,
                "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                "json_attributes_topic": topic, "icon": icon, "device": di},
                separators=(',', ':')), retain=True)
            if cnt_eid:                                   # count (przycisk) — battery/other
                cuid = f"lora_gw_{gl}_{cnt_eid}"
                # FIX 2026-07-17: „{nm} #" slugowało się do tego samego entity_id co encja
                # items („Anomalie Bateria" — `#` wypada przy slugifikacji) → HA odrzucał
                # rename (Entity with this ID is already registered). „licznik" = odrębny slug.
                mqtt.publish(f"{HA_PREFIX}/sensor/{cuid}/config", json.dumps({
                    "name": f"{nm} licznik", "object_id": cuid, "unique_id": cuid,
                    "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                    "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        an_regd.add(gw)

    # STEP 5: format anomalii do powiadomienia/popup — (emoji, etykieta PL, jednostka wartości)
    _ANOM_FMT = {
        'offline': ('📴', 'Offline', ''),
        'critical_battery': ('🪫', 'Bateria krytyczna', '%'),
        'low_battery': ('🔋', 'Bateria niska', '%'),
        'temp_high': ('🔺', 'Temperatura wysoka', '°C'),
        'temp_low': ('🔻', 'Temperatura niska', '°C'),
        'hum_high': ('💧', 'Wilgotność wysoka', '%'),
        'hum_low': ('🏜️', 'Wilgotność niska', '%'),
        'stagnation': ('🕰️', 'Stagnacja', 'h'),
        'smoke': ('🔥', 'Dym', ''),
        'water_leak': ('🌊', 'Zalanie', ''),
    }

    def _wl_items(gw, bucket):
        """MIRROR WHITELIST (2026-07-27): listy i liczniki anomalii pokazują TYLKO
        urządzenia, które mają encje na supie. Store zostaje PEŁNY — hash/reconcile
        z bramką bez zmian (bramka = źródło prawdy). Filtrujemy samą prezentację:
        kafle an_<bucket>, popup listy, powiadomienie-dzwonek. Clear-all NIE filtruje
        (musi czyścić dokładnie to, co czyści bramka)."""
        return [it for it in anomaly_store.items(gw, bucket)
                if sup_disc.allowed(gw, it.get('dev'))]

    def publish_anomalies(gw):
        reg_anomaly_entities(gw)
        gl = gw.lower()
        for bucket in ('offline', 'battery', 'other'):
            items = _wl_items(gw, bucket)
            mqtt.publish(f"{STATE_PREFIX}/gw/{gl}/an_{bucket}",
                         json.dumps({"count": len(items), "items": items},
                                    separators=(',', ':')), retain=True)
        _anom_notification(gw)        # POWIADOMIENIE HA (dzwonek) z listą + wartościami

    def _ha_service(domain, service, data):              # best-effort HA service call (thread)
        tok = ha_cfg5.get('token', '')
        if not tok:
            return
        url = ha_cfg5.get('url', '').rstrip('/') + f'/api/services/{domain}/{service}'

        def _post():
            try:
                import urllib.request as u
                req = u.Request(url, data=json.dumps(data).encode(),
                                headers={'Authorization': 'Bearer ' + tok,
                                         'Content-Type': 'application/json'})
                u.urlopen(req, timeout=8)
            except Exception as e:
                log.debug('ANOM', f'service {domain}.{service}: {e}')
        threading.Thread(target=_post, daemon=True, name='ha-svc').start()

    def _anom_notification(gw):
        """POWIADOMIENIE HA (persistent_notification = dzwonek) z listą WSZYSTKICH aktywnych
        anomalii bramki + wartością i jednostką. Aktualizuje (stałe id), dismiss gdy 0."""
        lines = []
        for bucket in ('offline', 'battery', 'other'):
            for it in _wl_items(gw, bucket):
                emoji, label, unit = _ANOM_FMT.get(it['type'], ('⚠️', it['type'], ''))
                v = it.get('value')
                vtxt = f": **{v}{unit}**" if v is not None and v != '' else ''
                lines.append(f"{emoji} **{it['dev']}** — {label}{vtxt}")
        nid = f"lora_anomalie_{gw.lower()}"
        if not lines:
            _ha_service('persistent_notification', 'dismiss', {'notification_id': nid})
            return
        msg = f"### 🚨 {len(lines)} aktywnych anomalii\n\n" + "\n".join(lines)
        _ha_service('persistent_notification', 'create',
                    {'notification_id': nid, 'title': f'🚨 Anomalie {gw} ({len(lines)})',
                     'message': msg})

    def anomaly_popup(gw, dev, code, value, critical):   # STEP 5: browser_mod popup (krytyczne) z wartością
        if not an_cfg.get('popup', True) or not critical:
            return
        from modules.anomaly.store import CODE_LABEL
        typ = CODE_LABEL.get(code, code)
        emoji, label, unit = _ANOM_FMT.get(typ, ('🚨', typ, ''))
        vtxt = f": **{value}{unit}**" if value is not None and value != '' else ''
        content = f"## {emoji} {gw} / {dev}\n\n**{label}**{vtxt}"
        _ha_service('browser_mod', 'popup',
                    {'title': '🚨 Anomalia LoRa', 'content': content,
                     'timeout': 15000, 'dismissable': True})

    anomaly_store = AnomalyStore(
        resolve_dev=lambda gw, sid: sup_data._dev_from_sid(gw, sid),
        on_change=publish_anomalies, on_alert=anomaly_popup,
        persist_path=an_cfg.get('persist_path', '/tmp/lora_anomaly_ids.json'), logger=log)
    # GHOST-PRUNE (2026-07-07, audyt): persistence trzyma anomalie bramek spoza konfiguracji
    # (np. stare test_1/test_2 [G1] po przemianowaniu maszyny G1→G2) → fałszywie zawyżony licznik
    # alarmów (175 vs 173). Przy starcie wywal wszystko dla nieznanych bramek.
    _ghosts = [(g, dv, cat) for (g, dv, cat) in list(anomaly_store.anomalies)
               if g not in known_gateways]
    for (g, dv, cat) in _ghosts:
        anomaly_store.remove_cat(g, dv, cat)
    if _ghosts:
        log.info('ANOM', f'👻 ghost-prune: usunięto {len(_ghosts)} anomalii nieznanych bramek '
                 f'({sorted(set(g for g, _, _ in _ghosts))} ∉ {known_gateways})')

    # v10 DISPLAY: most store → encje HA per-anomalia (sensor.lora_an_* + button.lora_an_*_clear).
    # Reconciluje na każdej zmianie store (ab/auto-clear/reconcile/ghost-prune) → reaktywny dashboard
    # v10 (auto-entities). Backend (detekcja/batching/redundancja) nietknięty. Liczniki/clear-all już są.
    an_v10 = AnomalyHAv10Bridge(anomaly_store, mqtt, HA_PREFIX, STATE_PREFIX, logger=log)
    an_v10.allow_fn = sup_disc.allowed          # MIRROR WHITELIST: encje per-anomalia tylko dla lustra
    anomaly_store.on_change = lambda gw: (publish_anomalies(gw), an_v10.publish(gw))

    # Znacznik czasu (ZEGAR BRAMKI) ostatnio zastosowanej zmiany anomalii z danej bramki —
    # oś porządkująca delty `an_d` i snapshoty `ansnap`. Patrz straż w _reconcile_ansnap.
    gw_an_ts = {}

    def _seed_avail_from_store(gw):
        """Odtwórz availability z AUTORYTATYWNEJ listy offline (persist po restarcie / ansnap
        od bramki). dev_avail żyje tylko w pamięci procesu — po restarcie puste, więc licznik
        OFFLINE zaniżał, a reconcile_availability (default True) kasował ze store'u offline'y
        przysłane przez bramkę (→ ah-drift → dump → refill). Store NIETKNIĘTY: piszemy wprost
        do dev_avail, bez add_offline, więc detected_at i hash anomalii się nie zmieniają."""
        n = 0
        for it in anomaly_store.items(gw, 'offline'):
            dev = it.get('dev')
            if dev and dev_avail.get((gw, dev)) is not False:
                dev_avail[(gw, dev)] = False
                n += 1
        if n:
            log.info('ANOM', f'♻️ {gw}: availability odtworzona z listy offline ({n} urz.)')
        return n

    def _reconcile_ansnap(gw, payload):
        """FAZA 1: blob aktywnych anomalii z bramki (rt kind='ansnap') = AUTORYTATYWNA lista. Reconcile
        store supervisora: dodaj/aktualizuj wszystkie z blobu + USUŃ te dla tej bramki, których w blobie
        NIE MA (recovery lub ręczny clear na bramce = źródle prawdy). Pusty blob → skasuj wszystkie gw.

        STRAŻ AKTUALNOŚCI (2026-08-03, złapane testem `offline`): transfer blobu trwa na tym łączu
        ~1-2 min, a delta `an_d` to JEDEN pakiet — więc delta potrafi wyprzedzić snapshot zbudowany
        PRZED nią. Zaobserwowane: ansnap zbudowany 13:46:19 (0 anomalii) dojechał 13:47:37 i skasował
        anomalię offline, którą delta dodała 13:47:19 → alarm znikał z dashboardu do najbliższego
        AH-GATE (do 15 min). Porządkujemy je ZEGAREM BRAMKI (`ts` w obu ramkach — jedno źródło
        czasu, więc kolejność jest ścisła, bez zgadywania po latencji łącza): snapshot starszy niż
        ostatnio zastosowana zmiana z tej bramki jest odrzucany w całości. Hash by tu nie wystarczył
        — po DUŻEJ zmianie bramka wysyła blob, więc jego hash z definicji różni się od ostatnio
        znanego i świeży snapshot zostałby wyrzucony."""
        from modules.anomaly.batcher import CODE_CAT
        blob = payload.get('a', []) or []
        snap_ts = payload.get('ts') or 0
        if snap_ts and snap_ts < gw_an_ts.get(gw, 0):
            log.warn('ANOM', f'⏭️ ansnap [{gw}] NIEAKTUALNY (ts {snap_ts} < {gw_an_ts[gw]}) — '
                             f'delta wyprzedziła transfer, reconcile pominięty')
            return
        gw_an_ts[gw] = max(gw_an_ts.get(gw, 0), snap_ts)
        anomaly_store.handle_ab({'g': gw, 'd': blob})          # add/update (brak recovery w blobie)
        present = set()
        for row in blob:
            if not isinstance(row, list) or len(row) < 2:
                continue
            dev = sup_data._dev_from_sid(gw, row[0])
            if dev:
                present.add((dev, CODE_CAT.get(row[1], 'other')))
        removed = 0
        for (g, dev, cat) in list(anomaly_store.anomalies):
            if g == gw and (dev, cat) not in present:
                anomaly_store.remove_cat(gw, dev, cat)         # brak w blobie → recovery/clear na bramce
                removed += 1
                if cat == 'offline':                           # bramka nie widzi go offline →
                    _set_avail(gw, dev, True)                  # live truth ON (kasuje ack, publikuje HA)
        _seed_avail_from_store(gw)                             # offline w liście ⇒ dev_avail=False
        log.info('ANOM', f'📥 ansnap [{gw}] {len(blob)} aktywnych → reconcile (usunięto {removed})')

    def _sup_anom_rows(gw):
        """Odbuduj listę [[sid,code,val?],...] z lokalnego store — TA SAMA formuła co bramka
        (_build_blob) → hash porównywalny 1:1 (weryfikacja delty an_d)."""
        from modules.anomaly.batcher import CODE_CAT  # noqa: F401 (spójność importów)
        sidmap = {name: info.get('sid') for name, info in sup_disc.devices(gw).items()}
        rows = []
        for (g, dev, cat), a in anomaly_store.anomalies.items():
            if g != gw:
                continue
            sid = sidmap.get(dev)
            if sid is None:
                continue
            code, val = a.get('code'), a.get('value')
            rows.append([sid, code] if val is None else [sid, code, val])
        rows.sort(key=lambda r: (r[0], r[1]))
        return rows

    for _sgw in known_gateways:      # START: odtwórz availability z persistu (patrz _seed_avail_from_store)
        _seed_avail_from_store(_sgw)

    an_d_dump_guard = {}                                  # gw → ts (debounce dump po rozjeździe delty)

    def handle_an_d(d):
        """DELTA anomalii (2026-07-07): {t:an_d,g,a:[dodane],r:[[sid,code] usunięte],h:hash-po}.
        1 pakiet zamiast pełnego blobu przy małej zmianie. Aplikuj → porównaj hash lokalnej listy
        z h bramki → rozjazd = zgubiona wcześniejsza delta → zażądaj pełnego snapshotu (dump_anom).
        Samonaprawialny hash-chain: maksimum oszczędności LoRa bez utraty spójności.
        DEBOUNCE 90s (2026-07-11): burst delt z bramki (start) = seria rozjazdów → BEZ debounce
        sup słał dump per delta (8×/19s), zapychał kolejkę radia i ubijał własną antenę."""
        from modules.anomaly.batcher import CODE_CAT
        gw = d.get('g')
        if not gw:
            return
        adds = d.get('a', []) or []
        rems = d.get('r', []) or []
        if d.get('ts'):                      # zegar bramki → straż aktualności ansnapa
            gw_an_ts[gw] = max(gw_an_ts.get(gw, 0), d['ts'])
        if adds:
            anomaly_store.handle_ab({'g': gw, 'd': adds})
        for row in rems:
            if not isinstance(row, list) or len(row) < 2:
                continue
            dev = sup_data._dev_from_sid(gw, row[0])
            if dev:
                anomaly_store.remove_cat(gw, dev, CODE_CAT.get(row[1], 'other'))
        h = AnomalyReconciler.blob_hash(_sup_anom_rows(gw))
        if h != d.get('h'):
            if time.time() - an_d_dump_guard.get(gw, 0) >= 90:
                an_d_dump_guard[gw] = time.time()
                log.warn('ANOM', f'⚠️ an_d [{gw}] hash rozjazd {h}≠{d.get("h")} → żądam pełnego ansnap')
                send_to_all({"t": "dump_anom", "g": gw})
            else:
                log.debug('ANOM', f'an_d [{gw}] rozjazd {h}≠{d.get("h")} — dump w debounce (90s)')
        else:
            log.info('ANOM', f'📥 an_d [{gw}] +{len(adds)}/-{len(rems)} hash OK ({h})')

    # ── UNIFIKACJA availability: offline-set bramki OR cmd-failure (jedno źródło prawdy) ──
    cmd_failed = set()                                    # {(gw,dev)} — cmd bez st (sticky offline w OR-merge)
    off_sync_guard = {}                                   # gw → ts (debounce reconcile po drift offline-hash)
    ah_guard = {}                                         # AH-GATE: gw → ts dump po rozjeździe ah (debounce)
    # POTWIERDZANIE STEROWANIA WIRTUALNEGO (2026-09-17). `vsw`/`vbtn` szly JEDNA ramka
    # bez sledzenia: `vbtn_ack` trafial wylacznie do logu, `vsw_st` tylko odbijal stan w HA.
    # ZMIERZONE NA ZYWO (15 nacisniec vbtn G1): do bramki doszlo 13/15, ack wrocil 12/15 —
    # czyli ~13% komend przepadalo BEZ SLADU. Dla wyzwalacza „zgas wszystko" to dyskwalifikacja.
    # Bramka odeslala 13 ackow z 13 odebranych, wiec potwierdzenie dziala — brakowalo tylko
    # strony supervisora. Uzywamy TEGO SAMEGO wzorca co `pending_cmd`: rejestr + timeout + retry
    # w `control_loop`. Klucz to (gw, rodzaj, id), bo `vsw` i `vbtn` maja osobne potwierdzenia.
    pending_vio = {}                                      # (gw,'vsw'|'vbtn',vid) → {msg,since,retries_left}
    vio_lock = threading.Lock()

    def _vio_send(gw, rodzaj, vid, msg):
        with vio_lock:
            pending_vio[(gw, rodzaj, vid)] = {'msg': msg, 'since': time.time(),
                                              'retries_left': ctrl_retries}
        send_to_all(msg)

    def _vio_ack(gw, rodzaj, vid):
        with vio_lock:
            return pending_vio.pop((gw, rodzaj, vid), None) is not None

    gw_ah_sync = {}                                       # AH-GATE: gw → True gdy HB.ah == lokalny hash anomalii
    # OSTATNI ZNANY HASH per bramka (2026-09-17). `GatewayHeartbeat._fit` przycina ramke do
    # sufitu 200 B, porzucajac rotacyjnie pola diagnostyczne — na G1 zdarza sie to przy
    # niemal KAZDYM beacie (zmierzone: 357 przycietych ramek). Kontrakt z heartbeat.py brzmi
    # „brak pola znaczy BEZ ZDANIA, nie rozjazd", ale odbiorca go nie honorowal: `d.get('ah','--')`
    # nadpisywalo kafel wartoscia `--`, a `_data_ok`/`_disc_ok`/`_param_ok`/`_cal_ok` przy braku
    # pola dawaly False, czyli FALSZYWY „ROZJAZD ⚠️". Stad tez migotanie flag `zg` (11110 →
    # 10111 → 01111) i push `zg` przy kazdej zmianie = niepotrzebny ruch LoRa.
    # Uwaga: rozrozniamy BRAK pola od wartosci falsy — `dh` rowne '0' (bramka nic nie wyslala)
    # jest PELNOPRAWNA wartoscia i musi trafic do porownania.
    gw_hb_hash = {}                                       # gw → {pole: ostatnia znana wartosc}

    _HB_HASH_KEYS = ('hash', 'ph', 'cal', 'ah', 'dh', 'sh')

    def _hb_val(gw, d, key):
        """Wartosc pola z HB, a przy BRAKU — ostatnia znana (patrz `merge_hb_hashes`)."""
        return merge_hb_hashes(gw_hb_hash.setdefault(gw, {}), d, _HB_HASH_KEYS)[key]

    def reconcile_availability(gw):
        """Reconcile listy offline z dev_avail (łapie zgubione `do`/`dn`). Recovery → usuń+kasuj ack.
        Offline → dodaj anomalię TYLKO jeśli NIE acknowledged (ack_offline). Encja *_available i tak
        zawsze odzwierciedla dev_avail (live truth, niezależnie od ack)."""
        for dev in list(sup_disc.devices(gw).keys()):
            on = dev_avail.get((gw, dev), True) and ((gw, dev) not in cmd_failed)
            if on:
                ack_offline.discard((gw, dev))
                anomaly_store.remove_one(gw, dev, 'offline')
            elif (gw, dev) not in ack_offline:
                anomaly_store.add_offline(gw, dev)
        publish_offline_count(gw)

    def dump_and_reconcile(gw, wait):
        """Poproś bramkę o pełny zrzut anomalii (battery/stagnation) → reconcile listy offline z
        dev_avail (odświeża `seen` aktualnie-offline) → prune tylko prawdziwie stale (nie-offline)."""
        send_to_all({"t": "dump_anom", "g": gw})
        time.sleep(wait)
        reconcile_availability(gw)                         # offline: lista := dev_avail (odśwież seen)
        anomaly_store.prune_stale(gw, wait + 30)           # battery/stagnation niepotwierdzone → usuń

    def request_offline_reconcile(gw):
        """Drift offline-hash `oh` w HB → szybki reconcile w tle (debounce 30s/gw)."""
        now = time.time()
        if now - off_sync_guard.get(gw, 0) < 30:
            return
        off_sync_guard[gw] = now
        threading.Thread(target=lambda: dump_and_reconcile(gw, an_cfg.get('reconcile_wait', 15)),
                         daemon=True, name='off-reconcile').start()

    def dump_anom_cycle():
        """Backup reconcyliacja: cyklicznie dump_anom → prune niepotwierdzonych → reconcile.
        Skrócone 1800→300 (hash-drift w HB i tak łapie zmiany szybciej; to siatka bezpieczeństwa)."""
        interval = an_cfg.get('dump_interval', 300)
        prune_after = an_cfg.get('prune_after', 90)
        while running.is_set() and interval > 0:
            time.sleep(interval)
            for gw in known_gateways:
                # OFFLINE-GATE (2026-08-13): bramka bez HB nigdy nie przyśle `ah`, więc AH-GATE jej
                # nie zatrzyma i cykl waliłby dump w martwy eter bez końca. Zmierzone na G1 (cisza
                # od 08-02): 337 `dump_anom` w 7 dni vs 4 na żywym G2. Ten sam warunek online co
                # w `targeted_calendar_sync`. Bramka wraca → HB → dump poleci normalnie.
                if not sup_hb.gateways.get(gw, {}).get('online'):
                    log.debug('ANOM', f'⏭️ dump_anom [{gw}] pominięty — bramka offline')
                    continue
                # AH-GATE (2026-07-07): HB niesie hash stanu anomalii bramki — zgodny z lokalnym →
                # pełny dump ZBĘDNY (oszczędność ~10-14 pkt LoRa/cykl). Dump tylko przy rozjeździe
                # lub braku informacji (bramka bez ah = stary soft / przed pierwszym blobem).
                if gw_ah_sync.get(gw):
                    log.debug('ANOM', f'⏭️ dump_anom [{gw}] pominięty — ah zgodny (AH-GATE)')
                    continue
                dump_and_reconcile(gw, prune_after)

    ctrl_cfg = CONFIG.get('control', {})
    ctrl_timeout = ctrl_cfg.get('timeout', 15); ctrl_retries = ctrl_cfg.get('retries', 2)
    pending_cmd = {}; cmd_lock = threading.Lock()

    # ── RAMKA ZBIORCZA `cmdm` (2026-08-26) — „wszystkie ON" z dashboardu szło 1 ramka `cmd`
    #    na urządzenie (pomiar: 104× cmd w 7 min). Krótkie okno zbiorcze grupuje kliknięcia
    #    per (bramka, cap, wartość) i tnie na ramki ≤220 B.
    #    2026-09-10: RETRY TEŻ tędy (patrz control_loop). Założenie „powtórki są rzadkie, nie ma
    #    czego grupować" upadło przy kaflu „WSZYSTKIE ON/OFF": 78 nieodpowiadających urządzeń
    #    = 78 pojedynczych ramek retry × 2 tury. ──
    def _flush_cmd(items):
        for fr in cmd_frames(items, max_bytes=ctrl_cfg.get('bulk_max_bytes', 200)):
            send_to_all(fr)
        if len(items) > 1:
            log.info('CTRL', f'📦 cmd zbiorczo: {len(items)} urządzeń → mniej ramek LoRa')
    cmd_bulk = Coalescer(_flush_cmd, window=ctrl_cfg.get('bulk_window_cmd', 1.5),
                         logger=log, name='cmd')
    refresh_regd = set()                                 # (gw,dev) refresh buttons created
    ls_regd = set()                                      # (gw,dev) binary last_seen sensors

    def reg_refresh_button(gw, dev):
        """App-level Refresh button (does NOT touch verified ha_entities).
        Attaches to the same device card; press → lora/supervisor/req/<gl>/<safe>."""
        key = (gw, dev)
        if key in refresh_regd:
            return
        gl, safe = gw.lower(), _safe(dev)
        uid = f"lora_{gl}_{safe}_refresh"
        mqtt.publish(f"{HA_PREFIX}/button/{uid}/config", json.dumps({
            "name": "Refresh", "object_id": uid, "unique_id": uid,
            "command_topic": f"{STATE_PREFIX}/supervisor/req/{gl}/{safe}",
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"},
            "icon": "mdi:refresh"}, separators=(',', ':')), retain=True)
        refresh_regd.add(key)

    def reg_binary_last_seen(gw, dev):
        """STEP 3 fix: verified reg_binary nie tworzy _last_seen (tylko reg_sensor/
        reg_switch_dev). Dashboard odwołuje się do sensor.lora_g1_<dev>_last_seen →
        dla binary (Door/Leak) brakowało encji. Dorejestruj app-level (jak refresh)."""
        key = (gw, dev)
        if key in ls_regd:
            return
        gl, safe = gw.lower(), _safe(dev)
        uid = f"lora_{gl}_{safe}_last_seen"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": "Last Seen", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ value_json.last_seen | default('--') }}",
            "icon": "mdi:clock-outline",
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"}}, separators=(',', ':')),
            retain=True)
        ls_regd.add(key)

    lq_regd = set()                                      # (gw,dev) linkquality sensors

    def reg_linkquality(gw, dev):
        """STEP 3: encja Zigbee LQI (0-255) — dane jadą jako ride-along w `b` (short `q`),
        supervisor merge'uje do value_json.linkquality. Dla wszystkich urzadzen."""
        key = (gw, dev)
        if key in lq_regd:
            return
        gl, safe = gw.lower(), _safe(dev)
        # FIX 2026-07-15: uid = slug_slug (realne entity_id z manglingu HA device-name —
        # to co czyta dashboard: sensor.lora_<slug>_<slug>_link_quality) + force_update
        # (LQI bywa stałe np. 255 → bez tego last_reported zamarza jak w reg_gw_lqi).
        uid = f"lora_{safe}_{safe}_link_quality"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": "Link Quality", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ value_json.linkquality | default('') }}",
            "icon": "mdi:signal", "state_class": "measurement", "force_update": True,
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"}}, separators=(',', ':')),
            retain=True)
        lq_regd.add(key)

    contact_regd = set()                                 # (gw,dev) naprawione kontaktrony

    def reg_contact_fix(gw, dev):
        """STEP 3 fix: verified reg_binary nadaje contact device_class='contact' —
        NIEPRAWIDŁOWA klasa w HA → encja odrzucana (brak wskazania open/close).
        Re-publikuj z device_class='opening' + INWERSJA (Z2M contact:true=zamknięte,
        HA opening ON=otwarte). Ten sam uid → ostatnia konfiguracja wygrywa."""
        key = (gw, dev)
        if key in contact_regd:
            return
        gl, safe = gw.lower(), _safe(dev)
        uid = f"lora_{gl}_{safe}_contact"
        mqtt.publish(f"{HA_PREFIX}/binary_sensor/{uid}/config", json.dumps({
            "name": "Contact", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ 'ON' if not value_json.contact else 'OFF' }}",
            "device_class": "opening",
            # force_update: contact STAŁY (drzwi zamknięte) → bez tego kafel door zamarza mimo
            # świeżego `b` (last_reported stoi). reg_contact_fix NADPISUJE reg_binary (ostatni
            # wygrywa) więc force_update z ha_entities tu nie dociera — dodane wprost. 2026-07-21.
            "force_update": True,
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"}}, separators=(',', ':')),
            retain=True)
        contact_regd.add(key)

    stag_regd = set()                                    # (gw,dev) encje stagnacji

    def reg_stagnant(gw, dev):
        """STEP 15: binary_sensor stagnacji (problem) — ON gdy bramka zgłosi `sg`."""
        key = (gw, dev)
        if key in stag_regd:
            return
        gl, safe = gw.lower(), _safe(dev)
        uid = f"lora_{gl}_{safe}_stagnant"
        mqtt.publish(f"{HA_PREFIX}/binary_sensor/{uid}/config", json.dumps({
            "name": "Stagnation", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ 'ON' if value_json.stagnant == 'ON' else 'OFF' }}",
            "device_class": "problem", "icon": "mdi:timer-sand-paused",
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"}}, separators=(',', ':')),
            retain=True)
        stag_regd.add(key)

    # MIRROR WHITELIST (2026-07-26): lustro/discovery tylko dla urządzeń z listy per-bramka
    # (G1 = 78 przekaźników RA1-15/RB1-11 x _1/_2/_3, G2 = monitored+priority). Brak = wszystko.
    sup_disc.whitelist = CONFIG.get('mirror_whitelist') or {}
    log.info('DISC', f'🧹 mirror whitelist: ' + ', '.join(f'{k}={len(v)}' for k, v in (sup_disc.whitelist or {}).items()) or 'brak (wszystko)')

    def augment_mirror(gw):
        """Wspólna dorejestracja encji app-level (refresh/link_quality/stagnation/binary
        last_seen/contact) po odbudowie mapy urządzeń. FIX 2026-07-15: dotąd wołane TYLKO
        z on_db (stara ścieżka `db`) — mirror odbudowany przez devmap (ReliableTransfer)
        nie dostawał tych encji (objaw po cleanup MQTT: brak link_quality/contact na sup)."""
        for dev, info in sup_disc.mirror_devices(gw).items():   # MIRROR WHITELIST
            buf = pending_hydrate.pop((gw, _safe(dev)), None)   # hydratacja z retained (jeśli był)
            if buf is not None:
                sup_data.hydrate(gw, dev, buf)
            reg_refresh_button(gw, dev)
            reg_linkquality(gw, dev)                      # Zigbee LQI dla wszystkich
            reg_stagnant(gw, dev)                         # STEP 15: encja stagnacji
            if info.get('type') == 'binary_sensor':      # fix: binary brak _last_seen
                reg_binary_last_seen(gw, dev)
                if 'c' in (info.get('caps') or ''):       # fix: contact device_class + inwersja
                    reg_contact_fix(gw, dev)

    def handle_ab(d):
        """STEP 5: anomalia z bramki (offline/battery/stagnation, ALL devices).
        → AnomalyStore (lista/dashboard an_*/popup) + odbicie na poziomie urządzenia."""
        gw = d.get('g', '?')
        anomaly_store.handle_ab(d)                        # lista + an_* + popup + persist
        for entry in d.get('d', []):
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            sid, code = entry[0], entry[1]
            dev = sup_data._dev_from_sid(gw, sid)
            if not dev:
                continue
            if not sup_disc.allowed(gw, dev):
                continue        # MIRROR WHITELIST (2026-07-26): store + agregaty licza
                                # WSZYSTKIE urzadzenia (bramka = zrodlo prawdy), ale encji
                                # urzadzenia w HA supa nie tworzymy/nie ruszamy. Wykryte
                                # testem CLI: `sg` dla RA15_S2 tworzylo _stagnant mimo filtra.
            if code == 'do':                              # offline → availability OFF
                _set_avail(gw, dev, False)
            elif code == 'dn':                            # powrót → ON (+ czyść cmd-failure sticky)
                cmd_failed.discard((gw, dev))
                _set_avail(gw, dev, True)
            elif code in ('sg', 'sc'):                    # stagnacja → per-device binary_sensor
                reg_stagnant(gw, dev)
                cur = sup_data.state.setdefault(gw, {}).setdefault(dev, {})
                cur['stagnant'] = 'ON' if code == 'sg' else 'OFF'
                ha.pub_device_state(gw, dev, dict(cur))

    gw_zg = {}                                           # gw → '1111' (disc/param/cal/anom):
                                                         # zgodność liczona przy HB, odsyłana w sup_pong
    gwhash_regd = set()                                  # bramki z zarejestrowanymi encjami hash

    def reg_gw_hashes(gw):
        """STEP 17: encje hash ZAPISANEGO NA BRAMCE (disc + param) na HA supervisora —
        z HB/pong. Podpięte pod urządzenie LoRa Gateway Gx (info bramki)."""
        if gw in gwhash_regd:
            return
        gl = gw.lower()
        di = {"identifiers": [f"lora_gateway_{gl}"]}
        for eid, nm, icon, key in [
                # #9: KOMPLET hashy zgodności bramka↔supervisor w JEDNYM miejscu (topic hashes).
                ('disc_hash', 'Hash Disc (bramka)', 'mdi:fingerprint', 'disc'),
                ('disc_hash_ok', 'Disc — zgodność', 'mdi:check-decagram', 'disc_ok'),      # #9
                ('disc_map_n', 'Mapa urządzeń', 'mdi:map-marker-multiple', 'disc_n'),     # RE-SYNC GUARD
                ('param_hash', 'Hash Param (bramka)', 'mdi:tune-variant', 'param'),
                ('param_hash_ok', 'Param — zgodność', 'mdi:check-decagram', 'param_ok'),   # #9
                ('cal_hash', 'Hash kalendarza (bramka)', 'mdi:calendar-sync', 'cal'),      # F9
                ('cal_hash_ok', 'Kalendarz — zgodność', 'mdi:calendar-check', 'cal_ok'),   # F9
                ('anom_hash', 'Hash anomalii (bramka)', 'mdi:alert-decagram', 'anomaly'),  # #9
                ('anom_hash_ok', 'Anomalie — zgodność', 'mdi:check-decagram', 'anomaly_ok'),  # #9
                ('data_hash', 'Hash status (bramka)', 'mdi:database-check', 'data'),        # B6/5. flaga
                ('data_hash_ok', 'Dane — zgodność', 'mdi:database-sync', 'data_ok'),        # B6/5. flaga
                ('all_hashes_ok', 'Zgodność OGÓLNA', 'mdi:shield-check', 'all_ok')]:       # #9 zbiorczy
            uid = f"lora_gw_{gl}_{eid}"
            mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
                "name": f"{nm}", "object_id": uid, "unique_id": uid,
                "state_topic": f"{STATE_PREFIX}/gw/{gl}/hashes",
                "value_template": "{{ value_json.%s | default('--') }}" % key,
                "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        gwhash_regd.add(gw)

    # ── STEP 5+: encje JAKOŚĆ CZASU + PORA DNIA/TRYB bramki na HA supervisora (z HB tq/gm/ga) ──
    gwtime_regd = set()
    _TQ_L = {'synced': 'Zsynchronizowany ✅', 'ntp': 'Zsynchronizowany (NTP) 🛰️',
             'holdover': 'Holdover 🕓',
             'unsynced': 'Niezsynchronizowany ⚠️', 'stale': 'Przeterminowany ❌'}
    _GM_L = {'all-time': 'Całodobowa', 'day': 'Dzienna', 'night': 'Nocna', 'calendar': 'Kalendarz'}

    # F(2026-07-26): okno dnia po stronie supa — do kafli DZIEN/SWIT/ZMIERZCH na obu
    # dashboardach. Protokol LoRa NIE wozi godzin switu/zmierzchu, wiec licza sie tu
    # (solarnie z lat/lon albo ze stalych godzin). Reszta bloku idzie z HB bramki.
    _sdw = CONFIG.get("sup_day_window", {}) or {}
    sup_day = GatewayMode(operating_mode="day",
                          day_start=_sdw.get("day_start", "06:00"),
                          day_end=_sdw.get("day_end", "20:00"),
                          lat=_sdw.get("lat"), lon=_sdw.get("lon"), logger=log)

    def reg_gw_time_entities(gw):
        if gw in gwtime_regd:
            return
        gl = gw.lower(); di = {"identifiers": [f"lora_gateway_{gl}"]}
        for eid, nm, icon, key in [
                ('time_quality', 'Jakość czasu', 'mdi:clock-check-outline', 'tq_label'),
                ('sun_mode', 'Pora dnia / tryb', 'mdi:theme-light-dark', 'mode'),
                ('gw_clock', 'Czas bramki', 'mdi:clock-outline', 'czas'),                  # F9
                ('gw_offset', 'Offset czasu [s]', 'mdi:clock-alert-outline', 'offset'),    # F9
                ('mode_active', 'Tryb — stan', 'mdi:power', 'active'),
                ('day_night', 'Pora dnia', 'mdi:weather-sunny', 'pora'),
                ('sun_dawn', 'Świt', 'mdi:weather-sunset-up', 'swit'),
                ('sun_dusk', 'Zmierzch', 'mdi:weather-sunset-down', 'zmierzch'),
                ('last_rx', 'Ostatni RX', 'mdi:clock-in', 'ost_rx'),
                # 2026-09-11: uptime BRAMKI w d/h/m. Surowy `sensor.lora_gw_<gl>_uptime`
                # (sekundy, z pub_gw_status) ZOSTAJE — trzyma historię i wykresy; ten jest
                # do czytania przez człowieka i biegnie co 30 s z refresh_gw_time.
                ('uptime_hm', 'Uptime bramki', 'mdi:timer-outline', 'uptime')]:
            uid = f"lora_gw_{gl}_{eid}"
            mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
                "name": f"{nm}", "object_id": uid, "unique_id": uid,
                "state_topic": f"{STATE_PREFIX}/gw/{gl}/timestat",
                "value_template": "{{ value_json.%s | default('--') }}" % key,
                "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        # LACZE: binary_sensor z tego samego topicu (parytet G1=G2 — dotad status/last_seen
        # istnialy tylko dla G2).
        _luid = f"lora_gw_{gl}_link"
        mqtt.publish(f"{HA_PREFIX}/binary_sensor/{_luid}/config", json.dumps({
            "name": "Łącze", "object_id": _luid, "unique_id": _luid,
            "state_topic": f"{STATE_PREFIX}/gw/{gl}/timestat",
            "value_template": "{{ value_json.lacze | default('OFF') }}",
            "payload_on": "ON", "payload_off": "OFF",
            "device_class": "connectivity", "device": di}, separators=(',', ':')), retain=True)
        gwtime_regd.add(gw)

    _GM_R = {v: k for k, v in _GM_L.items()}              # etykieta → surowy tryb (seed z retained)
    gw_time_cache = {}                                    # gw → (payload timestat, surowy tryb) z ost. HB
    def publish_gw_time(gw, d):
        tq = d.get('tq'); gm = d.get('gm'); ga_v = d.get('ga')
        # F9: czas bramki + offset vs zegar supervisora — z HB `ts` (epoch bramki) w chwili
        # odbioru; dokładność ± latencja LoRa (sekundy) — wystarcza na wskaźnik "czy zsync".
        gts = d.get('ts'); czas = '--'; off = '--'
        if gts:
            try:
                czas = datetime.fromtimestamp(int(gts)).strftime('%H:%M:%S')
                off = round(time.time() - int(gts), 1)
            except (TypeError, ValueError, OSError):
                pass
        _di = sup_day.day_info()
        _hhmm = lambda m: '%02d:%02d' % (int(m) // 60, int(m) % 60)
        _gd = sup_hb.gateways.get(gw, {})
        payload = {
            'tq': tq or '--', 'tq_label': _TQ_L.get(tq, tq or '--'),
            'mode': _GM_L.get(gm, gm or '--'),
            'czas': czas, 'offset': off,
            'active': 'aktywna' if ga_v else 'wstrzymana',
            'pora': 'Dzień' if _di.get('is_day') else 'Noc',
            'swit': _hhmm(_di.get('sunrise_min', 0)),
            'zmierzch': _hhmm(_di.get('sunset_min', 0)),
            'ost_rx': _gd.get('last_seen', '--'),
            'lacze': 'ON' if _gd.get('online') else 'OFF',
            # UPTIME BRAMKI (2026-09-11): `up` z HB to surowe sekundy — supervisor pokazywał
            # goły licznik („99571 s"). Format d/h/m robimy w JEDNYM miejscu (fmt_uptime).
            # `up_base`/`up_ts` to kotwica dla refresh_gw_time: uptime rośnie liniowo, więc
            # między HB doliczamy upływ czasu — to ekstrapolacja ŚCISŁA, nie zgadywanie
            # (inaczej niż `czas`/`offset`, które celowo zostają z HB).
            'uptime': fmt_uptime(d.get('up')),
            'up_base': int(d.get('up') or 0),
            'up_ts': int(time.time())}
        gw_time_cache[gw] = (payload, gm)                 # baza dla refresh_gw_time (HB bywa raz na dobę)
        mqtt.publish(f"{STATE_PREFIX}/gw/{gw.lower()}/timestat",
                     json.dumps(payload, separators=(',', ':')), retain=True)

    def refresh_gw_time():
        """Odśwież kafle czasu/trybu BEZ czekania na HB.

        Bramka nocna potrafi nie nadać HB przez całą noc (głodzenie kanału przez transfery), a
        `publish_gw_time` chodzi wyłącznie z HB — kafle „Pora dnia"/„Tryb — stan" zamarzały wtedy na
        wartościach sprzed zmierzchu (G1: „Dzień"/„wstrzymana" od 2026-08-30). Odświeżamy to, co
        supervisor wie SAM: porę dnia, aktywność trybu i stan łącza. Dla day/night aktywność to czysta
        funkcja wschodu/zachodu — ta sama, którą liczy `GatewayMode._is_daytime` na bramce, więc to nie
        zgadywanie tylko ten sam wzór. `czas`/`offset` zostają z HB — przeliczone „teraz" kłamałyby
        o kilka godzin na wskaźniku synchronizacji."""
        _di = sup_day.day_info(); is_day = bool(_di.get('is_day'))
        for gw, (payload, gm) in list(gw_time_cache.items()):
            act = mode_active(gm, is_day, last=payload.get('active') == 'aktywna')
            _gd = sup_hb.gateways.get(gw, {})
            upd = {'active': 'aktywna' if act else 'wstrzymana',
                   'pora': 'Dzień' if is_day else 'Noc',
                   'ost_rx': _gd.get('last_seen', '--'),
                   'lacze': 'ON' if _gd.get('online') else 'OFF'}
            # uptime BRAMKI biegnie dalej mimo braku HB: liczymy od kotwicy z ostatniego HB.
            # Bez tego kafel zamarzał — zmierzone 2026-09-11: HB sprzed 29 min, bo ramka zginęła
            # w zapchanym kanale, a kafel pokazywał uptime sprzed tych 29 min.
            if payload.get('up_ts'):
                upd['uptime'] = fmt_uptime(payload['up_base'] + int(time.time() - payload['up_ts']))
            payload.update(upd)
            mqtt.publish(f"{STATE_PREFIX}/gw/{gw.lower()}/timestat",
                         json.dumps(payload, separators=(',', ':')), retain=True)

    wd = CONFIG.get('watchdog', {})
    sup_hb = SupervisorHeartbeat(
        ha, logger=log, send_ping_fn=lambda gw: send_to_all({"t": "ping", "g": gw}),
        devices_provider=sup_disc.devices, gateways=known_gateways,
        passive_timeout=wd.get('passive_timeout', 2100),
        ping_timeout=wd.get('ping_timeout', 25), max_retries=wd.get('max_retries', 2))

    # ── Dispatcher (LoRa RX from gateways) ──
    dispatcher = Dispatcher(logger=log)

    psync_guard = {}                                     # gw → {'ph','ts'} — debounce auto-heal parametrów (per-bramka)
    synced_gw = set()                                    # #2: bramki którym wysłano sync w tej sesji
    time_sync_guard = {}                                 # gw → ts ostatniego wysłanego sync (debounce)
    gw_last_up = {}                                       # #2: gw → ostatni uptime (wykrycie restartu bramki)

    def on_hb(d):
        gw = d.get('g'); sup_hb.handle_hb(d)
        # SPOZNIONY HB (2026-10-01): kanal LoRa z transferami potrafi dostarczyc HB z OPOZNIENIEM
        # rzedu 10 min (zmierzone G1: nadany 13:15, odebrany 13:25). Taki HB niesie hashe SPRZED
        # ostatniej zmiany, wiec wszystkie „rozjazdy" z niego sa falszywe i wywoluja zbedne re-synce
        # (kalendarz 3x, dsnap, dump_anom). Decyzje o rozjezdzie podejmujemy tylko z SWIEZEGO HB.
        _hb_ts = d.get('ts')
        hb_stale = bool(_hb_ts) and (time.time() - float(_hb_ts)) > 180
        if hb_stale:
            log.info('HB', f'⏳ {gw}: HB sprzed {int(time.time() - float(_hb_ts))}s — pomijam decyzje o rozjezdzie hashy')
        if gw:
            ha.reg_gw_controls(gw)
            reg_gw_hashes(gw)                             # STEP 17: encje hash bramki
            reg_cal_button_gw(gw)                         # STEP 4: per-bramka Sync Calendar
            reg_anomaly_entities(gw)                      # STEP 5: encje an_offline/_battery/_other
            reg_gw_time_entities(gw)                      # STEP 5+: encje jakość czasu + pora dnia
            publish_gw_time(gw, d)                        # STEP 5+: tq/gm/ga → dashboard supervisora
            ah = d.get('ah')                              # AH-GATE: hash stanu anomalii z bramki
            if ah:
                local_ah = AnomalyReconciler.blob_hash(_sup_anom_rows(gw))
                gw_ah_sync[gw] = (local_ah == ah)
                if local_ah != ah and not hb_stale and time.time() - ah_guard.get(gw, 0) > 120:
                    ah_guard[gw] = time.time()
                    log.warn('ANOM', f'⚠️ HB [{gw}]: ah rozjazd {local_ah}≠{ah} → dump_anom (reconcile)')
                    send_to_all({"t": "dump_anom", "g": gw})
            ga = d.get('ga')                             # STEP 5: aktywność trybu bramki (supresja)
            if ga is not None:
                gw_active[gw] = bool(ga)
        # RE-SYNC GUARD (A): HB/pong niesie prawdziwe `dev` — karm nim licznik kompletności.
        # Bez tego gw_devn wypełniało tylko disc_meta (praktycznie: restart bramki), więc
        # check_complete spał, a niepełny mirror (78/194) nie miał jak się naprawić.
        sup_disc.note_devn(gw, d.get('dev'))
        sup_disc.note_hash(gw, d.get('hash', ''))
        cal = d.get('cal')                               # STEP 4: hash kalendarza bramki → drift
        if cal is not None and gw and not hb_stale and cal != expected_cal_hash.get(gw):
            now = time.time()
            _inf = gw_push_inflight.get(gw, 0)
            if _inf and now - _inf < 600:                 # push-up tej bramki w drodze (~2-3 min po
                # LoRa): jej hash JUŻ jest nowy, nasz jeszcze stary — odepchnięcie w dół skasowałoby
                # edycję zrobioną na bramce (zmierzone live 2026-07-30). Czekamy na koniec transferu.
                log.info('CAL', f'⏸️ {gw} drift ({cal}) — gw_push w drodze, nie odpycham w dół')
            elif now - cal_sync_guard.get(gw, 0) > 120:   # debounce: max raz / 120s
                cal_sync_guard[gw] = now
                log.info('CAL', f'🔄 {gw} cal drift ({cal}≠{expected_cal_hash.get(gw)}) → re-sync')
                targeted_calendar_sync([gw])
        # B6 (2026-08-01): hash STANU URZĄDZEŃ. Odkąd binarne raportują wyłącznie zdarzeniowo,
        # to jedyny mechanizm pilnujący, że lustro supa nie rozjechało się po cichu (zgubiona
        # ramka, restart supa, dziura w mapie). Rozjazd ⇒ jeden `dsnap` zamiast periodycznego
        # zalewu. Debounce 300 s: transfer trwa, a hash bramki zmienia się przy KAŻDEJ zmianie
        # stanu — bez tego prośby leciałyby seriami w trakcie normalnego ruchu.
        dh = d.get('dh')
        if dh and gw and not hb_stale and dh != sup_data.state_hash(gw):
            _now = time.time()
            # C (2026-08-05): bez MAPY urządzeń dsnap jest bezużyteczny — sid nie mają nazw,
            # więc `handle_b` odrzuci wpisy jako „nieznane sid" (zaobserwowane 15:40:40, sekundę
            # po prośbie wysłanej przy pustym mirrorze). Rozjazd na pustym lustrze jest ponadto
            # OCZYWISTY i zniknie sam, gdy devmap dojedzie. Czekamy więc na mapę, zamiast
            # dokładać transfer do zatoru startowego.
            if not sup_disc.is_complete(gw) or sup_disc._xfer_busy(gw):
                log.debug('DATA', f'⏸️ {gw} rozjazd danych, ale brak kompletnej mapy — '
                                  f'dsnap_req odłożony do czasu devmapy')
            elif _now - dsnap_guard.get(gw, 0) > 300:
                dsnap_guard[gw] = _now
                log.info('DATA', f'🔄 {gw} rozjazd hasha danych ({dh}≠{sup_data.state_hash(gw)}) '
                                 f'→ proszę o dsnap')
                send_to_all({"t": "dsnap_req", "g": gw})
        # #2: SYNC CZASU sterowany hashem — TYLKO gdy rozjazd, nie na każdym starcie.
        # Rozjazd = pierwszy kontakt w sesji (offset nieznany) LUB tq bramki nieświeży
        # (unsynced/stale/holdover). synced/ntp po 1. sync = cisza. „supervisor inicjalizuje".
        if gw:
            tq = d.get('tq')
            up = int(d.get('up', 0) or 0)
            if up < gw_last_up.get(gw, 0):               # #2: uptime spadł = bramka zrestartowała
                synced_gw.discard(gw)                    # → utraciła sync (offset '--'), wyślij ponownie
                time_sync_guard.pop(gw, None)            # restart = zdarzenie pilne → omiń debounce 120s
                gw_zg.pop(gw, None)                      # 2026-07-29: bramka wstala z pustymi kaflami
                                                         # zgodnosci → wymus push przy najblizszym HB
            gw_last_up[gw] = up
            first = gw not in synced_gw
            if first or tq in ('unsynced', 'stale', 'holdover'):
                now = time.time()
                if now - time_sync_guard.get(gw, 0) > 120:
                    time_sync_guard[gw] = now; synced_gw.add(gw)
                    log.info('SYNC', f'🕐 {gw} sync czasu ({"pierwszy kontakt" if first else tq}) → send sync')
                    send_to_all({"t": "sync", "sec": int(time.time()), "g": gw})
        # offline-hash `oh` w HB: zostawiony jako sygnał diagnostyczny, ale NIE triggeruje reconcile —
        # availability jest napędzana CIĄGŁYM batch `a:` (przez _set_avail), więc lista zawsze == licznik.
        # (Wcześniej `oh` z GatewayOfflineAnomaly ≠ hash z batch-a: → ciągłe niepotrzebne dumpy.)
        psync = get_sup_params(gw)                       # F3: lazy mirror per-bramka (1. kontakt tworzy)
        ph = d.get('ph')                                 # STEP 17: hash parametrów (wskaźnik driftu)
        if ph and psync is not None and ph != psync.params_hash():
            now = time.time()
            g = psync_guard.setdefault(gw, {'ph': None, 'ts': 0.0})
            if g['ph'] != ph or now - g['ts'] > 300:      # debounce per-gw (5 min)
                g['ph'] = ph; g['ts'] = now
                # AUTO-HEAL (2026-07-21): dryf mirror≠master zwykle = zgubiony `param_upd` na
                # stratnym LoRa (obserwacja 19.07: 4.5h rozjazdu timeoutów). Mirror ciągnie
                # autorytatywny zestaw od mastera przez params_req→push_all (idempotentne, 1 pakiet).
                # NIE robimy tego gdy user ma NIEWYSŁANĄ lokalną edycję na supervisorze — inaczej
                # pull zclobbersowałby ją przed Send (pierwotny powód wyłączenia auto-pull). Ten
                # przypadek dalej wymaga ręcznego Send (flaga has_pending_local_edit).
                if psync.has_pending_local_edit():
                    log.debug('PARAM', f'⚙️ [{gw}] drift {ph}≠mirror, ale lokalna edycja '
                              f'niewysłana → czekam na Send (bez auto-heal)')
                else:
                    log.info('PARAM', f'⚙️ [{gw}] drift {ph}≠mirror → auto-heal params_req '
                             f'(pull od mastera)')
                    psync.request()
        # #9: KOMPLET hashy + zgodności w JEDNYM topicu (na końcu — wszystkie flagi policzone).
        # disc: gw_synced (mapa zarejestrowana), param: mirror, cal: expected, anomaly: gw_ah_sync.
        if gw:
            def _mark(ok):
                return 'zgodny ✅' if ok else 'ROZJAZD ⚠️'
            _disc = _hb_val(gw, d, 'hash'); _param = _hb_val(gw, d, 'ph')
            _cal = _hb_val(gw, d, 'cal'); _ah = _hb_val(gw, d, 'ah')
            # RE-SYNC GUARD (C): sam hash kłamał — kafel pokazywał „zgodny ✅" przy mirrorze
            # 78/194. Zgodność = hash ORAZ komplet urządzeń.
            # NIEZNANE POLE = BEZ ZDANIA, NIE ROZJAZD (2026-09-17). `_fit` przycina ramke, a cache
            # `merge_hb_hashes` startuje pusty po restarcie SUPERVISORA — przy HB co 900 s
            # skompletowanie wszystkich szesciu hashy trwa kilkadziesiat minut. Wczesniej kazde
            # jeszcze-nieznane pole dawalo False, czyli kafel swiecil sie na czerwono bez powodu.
            # `_ah_ok` mial te obrone od dawna; tu wyrownujemy reszte do tej samej zasady.
            # WYJATEK — RE-SYNC GUARD przy disc: sam hash potrafil klamac (kafel „zgodny ✅"
            # przy mirrorze 78/194), wiec kompletnosc mapy sprawdzamy ZAWSZE, takze gdy hasha
            # jeszcze nie znamy. Inaczej ta poprawka cofnelaby tamten fix.
            _disc_ok = (bool(sup_disc.is_complete(gw)) if _disc == '--' else
                        bool(_disc and _disc == sup_disc.gw_synced.get(gw)
                             and sup_disc.is_complete(gw)))
            _param_ok = True if _param == '--' else bool(psync is not None
                                                         and _param == psync.params_hash())
            _cal_ok = True if _cal == '--' else bool(_cal and _cal == expected_cal_hash.get(gw))
            _ah_ok = bool(gw_ah_sync.get(gw)) if _ah != '--' else True
            # 5. FLAGA — DANE (2026-08-03, decyzja usera). Hash stanu urządzeń (`dh`, B6) jest
            # równorzędny z disc/param/cal/anom: odkąd binarne raportują tylko zdarzeniowo, to on
            # pilnuje, że lustro nie rozjechało się po cichu. Bez flagi klient nie widzi rozjazdu,
            # który system i tak sam naprawia blobem `dsnap` — a widzieć powinien.
            def _sup_status_hash(_gw):
                """Lustrzany odpowiednik `GatewayData.status_hash()` — ten sam wzór po stronie
                supervisora: capy z lustra + REALNA dostępność z `dev_avail`. Pozwala pokazać,
                czy status (w tym masowa zmiana o zmierzchu/świcie) jest zsynchronizowany,
                a nie tylko czy zgadzają się wartości, które bramka zdążyła dowieźć (`dh`)."""
                from modules.data import encode_short as _es, status_hash as _sh_fn
                _items = []
                for _dev, _f in (sup_data.state.get(_gw) or {}).items():
                    _info = (sup_disc.devices(_gw) or {}).get(_dev) or {}
                    _sid = _info.get('sid')
                    if _sid is None or not sup_disc.allowed(_gw, _dev):
                        continue
                    _caps = {k: v for k, v in _f.items()
                             if k in TYPE_CAPS.get(_info.get('type', 'sensor'), ())}
                    _av = dev_avail.get((_gw, _dev), True)
                    _items.append((_sid, _es(_caps, available=bool(_av), include_avail=True)))
                return _sh_fn(_items)

            _dh = _hb_val(gw, d, 'dh')
            # 2026-08-26: na kafel idzie `sh` (hash stanu, KTÓRY BRAMKA ZNA) — `dh` po restarcie
            # bramki stoi na 0 i wyglądał jak awaria. Zgodność lustra dalej liczymy z `dh`,
            # bo to ono opisuje, co bramka faktycznie dowiozła.
            _sh_raw = _hb_val(gw, d, 'sh')
            _sh = _sh_raw if _sh_raw != '--' else _dh
            _status_ok = bool(_sh and _sh != '--' and _sh == _sup_status_hash(gw))
            # NAPRAWA ROZJAZDU STATUSU (2026-10-01). HB ma sufit 200 B i rotacyjnie gubi pola, wiec
            # `dh` bywa nieobecny albo '0', a `sh` jedzie. Wtedy rozjazd lustra (np. dostepnosc po
            # anulowanym transferze `avail`) NIE wywolywal zadnej naprawy — flaga Dane stala na
            # ROZJAZD, bo jedyny wyzwalacz dsnap siedzial na `dh`. Teraz `sh` ≠ lustro tez prosi
            # o dsnap (te same bramki: swiezy HB, kompletna mapa, brak transferu, debounce 300 s).
            if (gw and not hb_stale and not _status_ok and _sh and _sh != '--'
                    and _dh in ('0', '--', None, '')
                    and sup_disc.is_complete(gw) and not sup_disc._xfer_busy(gw)
                    and time.time() - dsnap_guard.get(gw, 0) > 300):
                dsnap_guard[gw] = time.time()
                log.info('DATA', f'🔄 {gw} rozjazd hasha statusu (sh {_sh}≠{_sup_status_hash(gw)}) → proszę o dsnap')
                send_to_all({"t": "dsnap_req", "g": gw})
            # `dh` = hash tego, co bramka DOWIOZLA. Wartosc '0' znaczy „nie dowiozlem jeszcze
            # NICZEGO" — po restarcie bramki, albo gdy bramka nocna stoi w dzien i przekazniki
            # sa bez napiecia (G1 carport: 0/78 online w dzien). To NIE jest rozjazd lustra,
            # tylko brak dostaw, a mimo to flaga szla na ROZJAZD i zostawala tam CALY DZIEN
            # (zgloszone 2026-09-17). Tak samo '--' = pole nigdy nie przyszlo.
            # Wtedy o zgodnosci decyduje `sh` (co bramka WIE) vs lustro supervisora, czyli
            # policzone juz obok `_status_ok`. To nie jest zluzowanie kryterium: `_status_ok`
            # porownuje pelny status z dostepnoscia, wiec realny rozjazd nadal wychodzi.
            if _dh == '--' and _sh == '--':
                _data_ok = True                # nic jeszcze nie wiemy — bez zdania, nie rozjazd
            elif _dh in ('0', '--'):
                _data_ok = _status_ok
            else:
                _data_ok = bool(_dh == sup_data.state_hash(gw))
            _all_ok = _disc_ok and _param_ok and _cal_ok and _ah_ok and _data_ok
            _zgv = ''.join('1' if f else '0'               # → sup_pong ORAZ push przy zmianie
                           for f in (_disc_ok, _param_ok, _cal_ok, _ah_ok, _data_ok))
            if gw_zg.get(gw) != _zgv:                      # PUSH tylko przy ZMIANIE — steady
                gw_zg[gw] = _zgv                           # state = 0 ramek. Sam pong nie wystarcza:
                send_to_all({"t": "zg", "g": gw, "z": _zgv})   # probe bramki pinguje TYLKO przy ciszy.
                log.info('HB', f'🛡️ {gw}: zgodność {_zgv} → push do bramki')
            mqtt.publish(f"{STATE_PREFIX}/gw/{gw.lower()}/hashes", json.dumps({
                'disc': _disc, 'disc_ok': _mark(_disc_ok),
                # RE-SYNC GUARD: ile urządzeń mam w mapie / ile bramka zgłasza w HB.
                # „78/194" mówi od razu ile brakuje — bez zaglądania w log.
                'disc_n': f"{len(sup_disc.devices(gw))}/{sup_disc.gw_devn.get(gw, '?')}",
                'param': _param, 'param_ok': _mark(_param_ok),
                'cal': _cal, 'cal_expected': expected_cal_hash.get(gw, '--'), 'cal_ok': _mark(_cal_ok),
                'anomaly': _ah, 'anomaly_ok': _mark(_ah_ok),
                'data': _sh, 'data_ok': _mark(_data_ok),   # 5. flaga: hash STANU bramki (sh) + zgodność (dh)
                'data_sent': _dh,                          # co bramka dowiozła (diagnostyka rozjazdu)
                'status_ok': _mark(_status_ok),            # 2026-08-26: czy STATUS (z dostępnością) zsynchronizowany
                'all_ok': 'WSZYSTKO ZGODNE ✅' if _all_ok else 'ROZJAZD ⚠️'},
                separators=(',', ':')), retain=True)
    dispatcher.register('hb', on_hb)
    dispatcher.register('pong', on_hb)
    dispatcher.register('disc_meta', sup_disc.handle_disc_meta)
    dispatcher.register('disc_vio', sup_disc.handle_disc_vio)

    def on_db(d):
        sup_disc.handle_db(d)
        gw = d.get('g', '?')                             # STEP 3: per-device app-level entities
        augment_mirror(gw)
        publish_offline_count(gw)                         # A: realny licznik po hydratacji/db
        anomaly_store.ghost_prune(gw, list(sup_disc.devices(gw).keys()))  # STEP 5: usuń anomalie znikłych
    dispatcher.register('db', on_db)
    dispatcher.register('disc_ack', lambda d: log.info('RX', f'disc_ack: {d}'))
    dispatcher.register('b', sup_data.handle_b)          # STEP 3: batch data → HA
    def _cal_dispatch_sup(d):
        """Znacznik: bramka ZACZĘŁA pchać harmonogram w górę → wstrzymaj auto-push w dół
        (patrz drift-branch). Bez tego HB w trakcie transferu kasował edycję na bramce."""
        if d.get('t') == 'cal_begin' and d.get('dir') == 'gw_push' and d.get('g'):
            gw_push_inflight[str(d['g']).upper()] = time.time()
        return cal_transfer.dispatch(d)

    for _ct in ('cal_begin', 'cal_chunk', 'cal_end', 'cal_ack', 'cal_cack'):  # STEP 4: transfer kalendarza (per-chunk ACK)
        dispatcher.register(_ct, _cal_dispatch_sup)      # ACK od bramki / pull gw_push (+ in-flight)
    for _rt in ('rt_begin', 'rt_chunk', 'rt_end', 'rt_ack', 'rt_cack'):       # BULK: odbiór mass_offline
        dispatcher.register(_rt, rt.dispatch)            # handle_end → on_rt_received

    def handle_cal_req(d):                                # STEP 4: bramka prosi o harmonogram (pull down)
        gw = d.get('g', '?')
        log.info('CAL', f'⤵️ {gw} prosi o harmonogram → odczyt HA + push')
        read_ha_calendar(); targeted_calendar_sync([gw])
    dispatcher.register('cal_req', handle_cal_req)
    def on_param_upd(d):                                  # STEP 17→F3: confirmed z bramki → JEJ mirror
        ps = get_sup_params(d.get('g'))
        if ps is not None:
            ps.handle_remote(d)
    dispatcher.register('param_upd', on_param_upd)
    dispatcher.register('ab', handle_ab)                 # STEP 15: anomalie (stagnacja sg/sc)
    dispatcher.register('an_d', handle_an_d)             # DELTA anomalii: 1 pakiet, hash-chain verify
    # F2: bramka pinguje supervisora → natychmiastowy pong (reactive, z id bramki)
    dispatcher.register('sup_ping', lambda d: send_to_all({"t": "sup_pong", "g": d.get('g'), "sec": int(time.time()),
                                                           "zg": gw_zg.get(d.get('g'), '')}))  # F(2026-07-18): pong niesie czas -> bramka odswieza sup_time co P4; zg=zgodnosc (2026-07-28)
    # F5: bramka prosi o sync czasu → targeted sync (to samo co przycisk per-gw)
    dispatcher.register('sync_req', lambda d: send_to_all({"t": "sync", "sec": int(time.time()), "g": d.get('g')}))

    def _apply_st(gw, dev, val, quiet=False):
        if not dev: return
        with cmd_lock: pending_cmd.pop((gw, dev), None)
        cmd_failed.discard((gw, dev))                # st potwierdza działanie → czyść cmd-failure
        _set_avail(gw, dev, True)                    # STEP 4 fix: online w dev_avail → licznik spada
        sup_data.apply_st(gw, dev, {"state": val})   # MERGE + last_seen (był full-replace bez ts → gubił last_seen/capy)
        if not quiet:
            log.info('CTRL', f'🎛️ st {gw}/{dev} = {val} → online + odbite w HA (cmd/zmiana zewn.)')

    def handle_st(d):
        _apply_st(d.get('g', '?'), d.get('d'), str(d.get('v', '')).upper())
    dispatcher.register('st', handle_st)

    def handle_stm(d):
        """Ramka ZBIORCZA statusów: jedna ramka, mapa {urządzenie: wartość}."""
        gw = d.get('g', '?'); m = d.get('d') or {}
        for dev, val in m.items():
            _apply_st(gw, dev, str(val).upper(), quiet=True)   # linia na urządzenie = ten sam flood, tylko w logu
        log.info('CTRL', f'📦 stm {gw}: {len(m)} urządzeń → online + odbite w HA')
    dispatcher.register('stm', handle_stm)

    def handle_vsw_st(d):
        gw = d.get('g', '?'); vid = d.get('id')
        val = 1 if str(d.get('v')) in ('1', 'ON', 'on', 'True') else 0
        gl, safe = gw.lower(), _safe(vid)
        mqtt.publish(f'{STATE_PREFIX}/{gl}/vio/{safe}/state', 'ON' if val else 'OFF', retain=True)
        _vio_ack(gw, 'vsw', vid)                     # potwierdzenie → koniec ponowien
        log.info('VIO', f'🔘 VSwitch {gw}/{vid} = {"ON" if val else "OFF"} → odbite w HA')
    dispatcher.register('vsw_st', handle_vsw_st)

    def handle_vbtn_ack(d):
        gw, vid = d.get('g', '?'), d.get('id')
        potwierdzony = _vio_ack(gw, 'vbtn', vid)
        log.info('VIO', f'🔘 vbtn_ack {gw}/{vid}' + ('' if potwierdzony else ' (spozniony/nieznany)'))
    dispatcher.register('vbtn_ack', handle_vbtn_ack)
    dispatcher.set_fallback(lambda d: log.debug('RX', f'passthrough t={d.get("t")}'))

    # ── HA → LoRa bridge ──
    def on_mqtt(topic, payload):
        segs = topic.split('/')
        # SEED kafli czasu: po restarcie cache jest pusty, a HB bramki nocnej potrafi nie przyjść do
        # rana — retained timestat z brokera odtwarza go natychmiast (potem nadpisuje go HB).
        if len(segs) == 4 and segs[1] == 'gw' and segs[3] == 'timestat':
            if segs[2].upper() not in gw_time_cache:
                try:
                    _p = json.loads(payload)
                    gw_time_cache[segs[2].upper()] = (_p, _GM_R.get(_p.get('mode')))
                except Exception:
                    pass
            return
        # hydratacja: retained stan urządzenia lora/<gl>/<safe>/state → bufor (seed po db)
        if len(segs) == 4 and segs[3] == 'state' and segs[2] != 'vio':
            try:
                pending_hydrate[(segs[1].upper(), segs[2])] = json.loads(payload)
            except Exception:
                pass
            return
        if topic.startswith(f'{STATE_PREFIX}/params/supervisor/'):       # STEP 17→F3: per-bramka
            segs = topic.split('/')                       # lora/params/supervisor/<gl>/set|cmd/...
            if len(segs) >= 6:
                ps = get_sup_params(segs[3].upper())
                if ps is not None:
                    if segs[4] == 'set':
                        ps.on_mqtt_set(topic, payload)
                    elif segs[4] == 'cmd':
                        ps.on_cmd(topic, payload)
            return
        if topic.startswith(f'{STATE_PREFIX}/supervisor/req/'):   # STEP 3: refresh button
            if len(segs) == 5:
                gw, safe = segs[3].upper(), segs[4]
                dev = sup_disc.find_device(gw, safe) or safe
                log.info('CTRL', f'🎛️ Refresh {gw}/{dev} → req')
                send_to_all({"t": "req", "g": gw, "d": dev})
            return
        if len(segs) == 4 and segs[1] == 'anomaly' and segs[3] == 'clear':  # v10 clear per-anomalia
            safe_id = segs[2]
            info = an_v10.lookup(safe_id)
            if info:
                cg, cd, ccat = info
                # PER-ANOMALY: kasuj DOKŁADNIE tę kategorię (temp≠hum≠stagnacja), nie cały kubełek —
                # jedno urządzenie może mieć kilka anomalii, każdą kasujemy osobno (wymóg usera).
                if ccat == 'offline':                     # ACK: nie wracaj przez reconcile aż do recovery
                    cmd_failed.discard((cg, cd)); ack_offline.add((cg, cd))
                n = anomaly_store.remove_cat(cg, cd, ccat)
                send_to_all({"t": "ac_b", "g": cg, "d": [[cd, ccat]]})  # clear→bramka: usuń+ACK (nie re-doda przy dump)
                publish_offline_count(cg)
                log.info('ANOM', f'🗑️ clear v10 {cg}/{cd} kat={ccat} n={n} ({safe_id}) + ac_b→{cg}')
            else:
                log.warn('ANOM', f'clear v10: nieznana encja {safe_id}')
            return
        if topic.startswith(f'{STATE_PREFIX}/supervisor/cmd/'):
            rest = segs[3:]
            if len(rest) == 1:
                a = rest[0]
                if a == 'ping_all':
                    log.info('BTN', '🔘 Ping All (broadcast)')
                    sup_hb.manual_ping_all(known_gateways,
                                           send_broadcast_fn=lambda: send_to_all({"t": "ping"}))
                elif a == 'discovery_all':
                    log.info('BTN', '🔘 Discovery All'); send_to_all({"t": "disc"})
                elif a == 'sync_all':
                    log.info('BTN', '🔘 Sync All'); send_to_all({"t": "sync", "sec": int(time.time())})
                elif a.startswith('set_mode_'):            # 2026-07-18/24: dropdown trybu bramki (sup->LoRa) per-gw
                    _sgw = a[len('set_mode_'):].upper()
                    _lbl = payload.decode('utf-8') if isinstance(payload, (bytes, bytearray)) else str(payload or '')
                    _lbl = _lbl.strip()
                    _m = {'Całodobowa': 'all-time', 'Dzienna': 'day', 'Nocna': 'night', 'Kalendarz': 'calendar'}.get(_lbl)
                    if _m and _sgw in known_gateways:
                        log.info('MODE', 'set_mode %s -> %s (%s) [sup->LoRa]' % (_sgw, _lbl, _m))
                        send_to_all({'t': 'set_mode', 'g': _sgw, 'm': _m})
                    else:
                        log.warn('MODE', 'set_mode %s: zignorowano %r' % (a, _lbl))
                elif a == 'calendar_all':                 # STEP 4: re-read HA + push do wszystkich
                    log.info('BTN', '🔘 Sync Calendar All')
                    log.info('CAL', f'📅 HA → {read_ha_calendar()} slotów GLOBAL')
                    targeted_calendar_sync()
                elif a == 'dump_anom_all':                 # STEP 5: reconcyliacja anomalii teraz
                    log.info('BTN', '🔘 Dump Anomalies All')
                    for g in known_gateways:
                        send_to_all({"t": "dump_anom", "g": g})
                    threading.Thread(target=lambda: (time.sleep(an_cfg.get('prune_after', 90)),
                                     [anomaly_store.prune_stale(g, an_cfg.get('prune_after', 90) + 30)
                                      for g in known_gateways]), daemon=True).start()
                elif a == 'clear_anomaly':                 # PER-ANOMALY clear (klik wiersza w popupie)
                    try:                                   # payload JSON {gw,dev,bucket}
                        info = json.loads(payload)
                        cg, cd = info['gw'], info['dev']; cb = info.get('bucket', 'offline')
                        if cb == 'offline':                # ACK: nie wracaj przez reconcile aż do recovery
                            cmd_failed.discard((cg, cd)); ack_offline.add((cg, cd))
                        n = anomaly_store.remove_one(cg, cd, cb)
                        send_to_all({"t": "ac_b", "g": cg, "d": [[cd, cb]]})  # clear→bramka: usuń+ACK
                        publish_offline_count(cg)
                        log.info('ANOM', f'🗑️ clear ręczny {cg}/{cd} [{cb}] n={n} (ack) + ac_b→{cg}')
                    except Exception as e:
                        log.warn('ANOM', f'clear_anomaly bad payload {payload!r}: {e}')
                elif a in ('clear_offline', 'clear_battery', 'clear_other'):  # CLEAR-ALL kubełka
                    bucket = a.split('_', 1)[1]
                    for g in known_gateways:
                        for it in list(anomaly_store.items(g, bucket)):   # lokalny store sup (UI, bez LoRa)
                            if bucket == 'offline':
                                cmd_failed.discard((g, it['dev'])); ack_offline.add((g, it['dev']))
                            anomaly_store.remove_one(g, it['dev'], bucket)
                        # F2 fix: KOMPAKTOWA flaga (nie lista 192 dev = ~4KB >>220B, cicho ginęła)
                        send_to_all({"t": "ac_all", "g": g, "b": bucket})
                        publish_offline_count(g)
                    log.info('BTN', f'🔘 Clear all [{bucket}] (ack) + ac_all→bramki (kompaktowo)')
            elif len(rest) == 2:
                gw, a = rest[0].upper(), rest[1]
                if a == 'ping':
                    log.info('BTN', f'🔘 Ping {gw}'); sup_hb.manual_ping(gw)
                elif a == 'disc':
                    log.info('BTN', f'🔘 Discovery {gw}'); send_to_all({"t": "disc", "g": gw})
                elif a == 'sync':
                    log.info('BTN', f'🔘 Sync {gw}')
                    send_to_all({"t": "sync", "sec": int(time.time()), "g": gw})
                elif a == 'dump':                         # #5: dump anomalii TYLKO tej bramki
                    log.info('BTN', f'🔘 Dump anomalii {gw}')
                    send_to_all({"t": "dump_anom", "g": gw})
                    threading.Thread(target=lambda g=gw: (time.sleep(an_cfg.get('prune_after', 90)),
                                     anomaly_store.prune_stale(g, an_cfg.get('prune_after', 90) + 30)),
                                     daemon=True).start()
                elif a in ('clear_offline', 'clear_battery', 'clear_other'):  # #5: clear kubełka TYLKO tej bramki
                    bucket = a.split('_', 1)[1]
                    for it in list(anomaly_store.items(gw, bucket)):
                        if bucket == 'offline':
                            cmd_failed.discard((gw, it['dev'])); ack_offline.add((gw, it['dev']))
                        anomaly_store.remove_one(gw, it['dev'], bucket)
                    send_to_all({"t": "ac_all", "g": gw, "b": bucket})   # kompaktowa flaga → bramka
                    publish_offline_count(gw)
                    log.info('BTN', f'🔘 Clear [{bucket}] {gw} (ack) + ac_all→{gw}')
                elif a == 'calendar':                     # STEP 4: re-read HA + push do tej bramki
                    log.info('BTN', f'🔘 Sync Calendar {gw}')
                    read_ha_calendar(); targeted_calendar_sync([gw])
                elif a == 'pull_calendar':                # FAZA 3B: Pull ← Gx (sup prosi bramkę o push-up)
                    log.info('BTN', f'🔘 Pull Calendar ← {gw}')
                    send_to_all({"t": "cal_pull", "g": gw})
                elif a == 'dump_anom':                     # FAZA 3B: Dump anomalii per-bramka
                    log.info('BTN', f'🔘 Dump Anomalies {gw}')
                    send_to_all({"t": "dump_anom", "g": gw})
                    threading.Thread(target=lambda: (time.sleep(an_cfg.get('prune_after', 90)),
                                     anomaly_store.prune_stale(gw, an_cfg.get('prune_after', 90) + 30)),
                                     daemon=True).start()
                elif a in ('clear_offline', 'clear_battery', 'clear_other'):  # FAZA 3B: Clear-all kubełka per-bramka
                    bucket = a.split('_', 1)[1]
                    for it in list(anomaly_store.items(gw, bucket)):   # lokalny store sup (UI, bez LoRa)
                        if bucket == 'offline':
                            cmd_failed.discard((gw, it['dev'])); ack_offline.add((gw, it['dev']))
                        anomaly_store.remove_one(gw, it['dev'], bucket)
                    # F2 fix: KOMPAKTOWA flaga zamiast listy 192 dev (~4KB >>220B, cicho ginęła)
                    send_to_all({"t": "ac_all", "g": gw, "b": bucket})
                    publish_offline_count(gw)
                    log.info('BTN', f'🔘 Clear all [{bucket}] {gw} (ack) + ac_all→{gw} (kompaktowo)')
            return
        if len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'set':
            gw, vid = segs[1].upper(), segs[3]
            val = 1 if payload.upper() in ('ON', '1') else 0
            # #5 optimistic: odbij stan w HA OD RAZU (klik nie gubi się przez latencję
            # LoRa / anty-spam) — realny vsw_st potem to potwierdzi/skoryguje.
            mqtt.publish(f'{STATE_PREFIX}/{gw.lower()}/vio/{_safe(vid)}/state',
                         'ON' if val else 'OFF', retain=True)
            log.info('CTRL', f'🎛️ dashboard VSwitch {gw}/{vid} → {"ON" if val else "OFF"} (optimistic)')
            _vio_send(gw, 'vsw', vid, {"t": "vsw", "g": gw, "id": vid, "v": val})
        elif len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'press':
            gw, vid = segs[1].upper(), segs[3]
            log.info('CTRL', f'🎛️ dashboard VButton {gw}/{vid} pressed')
            _vio_send(gw, 'vbtn', vid, {"t": "vbtn", "g": gw, "id": vid})
        elif len(segs) == 4 and segs[3] == 'set':
            gw, safe = segs[1].upper(), segs[2]
            dev = sup_disc.find_device(gw, safe) or safe
            val = payload.upper()
            msg = {"t": "cmd", "g": gw, "d": dev, "c": "state", "v": val}
            with cmd_lock:
                pending_cmd[(gw, dev)] = {'msg': msg, 'since': time.time(),
                                         'retries_left': ctrl_retries}
            # #5 optimistic: pokaż żądany stan w HA natychmiast; realny `st` (handle_st)
            # potem nadpisze potwierdzoną wartością. Bez tego klik wygląda jak "nie złapał".
            ha.pub_device_state(gw, dev, {"state": val, "available": "ON"})
            log.info('CTRL', f'🎛️ dashboard {gw}/{dev} → {val} (optimistic, timeout {ctrl_timeout}s)')
            cmd_bulk.add((gw, dev, "state", val))

    def note_gw_activity(gw):
        """Bramka ONLINE jeśli przyszła JAKAKOLWIEK wiadomość LoRa z `g` (nie tylko HB/pong).
        Aktualizuje status NATYCHMIAST. Liczniki z HB zachowane (tylko _ts/last_seen/online)."""
        g = sup_hb.gateways.get(gw)
        now = time.time()
        if g is None:
            sup_hb.gateways[gw] = {'_ts': now, 'online': True,
                                   'last_seen': datetime.now().strftime('%H:%M:%S'),
                                   'uptime': 0, 'devices_total': 0, 'devices_monitored': 0,
                                   'devices_priority': 0, 'hash': ''}
            sup_hb.ha.reg_gateway(gw)
        else:
            sup_hb.note_seen(gw)          # _ts + last_seen + ANULUJ sondę ping (fałszywy offline)
            if not g.get('online'):
                g['online'] = True
                sup_hb._cascade_devices(gw, online=True)
                log.info('HB', f'💚 {gw} ONLINE (wiadomość LoRa)')
        sup_hb._publish_status(gw)

    def publish_offline_count(gw):
        """A: REALNY licznik offline = liczba urządzeń bramki z availability=off.
        Nadpisuje watchdogowe devices_offline (0/total) prawdziwą liczbą per-device."""
        # MIRROR WHITELIST (2026-07-26): licz TYLKO urzadzenia, ktore maja encje na supie
        # (inaczej kafel OFFLINE pokazywal 124 z 194, choc widoczne bylo 78 przekaznikow).
        # devices_total/monitored zostaja z hb bramki — bramka jest zrodlem prawdy.
        off = sum(1 for (g, _d), av in dev_avail.items()
                  if g == gw and not av and sup_disc.allowed(gw, _d))
        gd = sup_hb.gateways.get(gw, {})
        ha.pub_gw_status(gw, {
            'state': 'online' if gd.get('online') else 'offline',
            'uptime': gd.get('uptime', 0), 'last_seen': gd.get('last_seen', '--'),
            'devices_total': gd.get('devices_total', 0),
            'devices_monitored': gd.get('devices_monitored', 0),
            'devices_priority': gd.get('devices_priority', 0),
            'devices_offline': off, 'hash': gd.get('hash', '')})

    def on_lora(text):
        g = None
        try:
            g = json.loads(text).get('g')
        except Exception:
            pass
        if g:
            note_gw_activity(g)                           # dowolna wiadomość = bramka żyje
            if safe:
                safe.mark_rx(g)                           # STEP 4: safe window — słyszeliśmy bramkę
        dispatcher.dispatch_raw(text)                     # handle_b aktualizuje dev_avail
        if g:
            publish_offline_count(g)                      # realny licznik offline po aktualizacji

    mqtt._on_message_cb = on_mqtt
    lora.on_receive = on_lora
    mqtt.subscribe(f'{STATE_PREFIX}/supervisor/cmd/#')
    mqtt.subscribe(f'{STATE_PREFIX}/gw/+/timestat')        # seed kafli czasu po restarcie
    mqtt.subscribe(f'{STATE_PREFIX}/anomaly/+/clear')      # v10: clear per-anomalia (button.lora_an_*_clear)
    mqtt.subscribe(f'{STATE_PREFIX}/supervisor/req/+/+')   # STEP 3: refresh buttons
    mqtt.subscribe(f'{STATE_PREFIX}/params/supervisor/#')   # STEP 17→F3: per-bramka <gl>/set|cmd (+instancje same subskrybują)
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/state')   # hydratacja: retained stany urządzeń (seed merge)
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/set')
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/+/set')
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/+/press')

    log.info('MQTT', f'Connecting {mqtt_cfg["host"]}:{mqtt_cfg["port"]}...')
    mqtt.start()
    log.info('MQTT', '✅ Connected' if mqtt.wait_connected(timeout=10) else 'Connection timeout')

    log.info('LORA', 'Connecting...')
    lora.start()
    for label, info in lora.get_status().items():
        log.info('LORA', f'{label}: {"✅ OK" if info["connected"] else "❌ FAIL"}')

    ha.reg_supervisor(); sup_hb.start()
    # STARTUP: republikuj encje anomalii wczytanych z persist (po restarcie supervisora) → dashboard
    # v10 od razu ma wiersze, nie czeka na pierwszy ab/reconcile. Idempotentne (reconcile mostu).
    for _gw in known_gateways:
        try:
            publish_anomalies(_gw); an_v10.publish(_gw)
        except Exception as _e:
            log.warn('ANOM', f'startup republish {_gw}: {_e}')
    for _gw in known_gateways:                           # STEP 17→F3: mirror parametrów per-bramka (pre-create)
        get_sup_params(_gw)
    # UNIFIKACJA: offline_mon usunięty — availability liczona z offline-set bramki (reconcile)
    reg_cal_button_global()                              # STEP 4: przycisk Sync Calendar All
    # 2026-07-18: SELECT trybu bramki na supervisorze — sup steruje bramka przez LoRa set_mode;
    # stan odbijany z gw timestat (HB gm). Round-trip: wybor -> LoRa -> bramka -> HB gm -> select.
    # 2026-07-24: de-hardcode G2 -> per-bramka (parytet G1). uid lora_<gl>_tryb_bramki, cmd set_mode_<gl>.
    for _sgw in known_gateways:
        _sgl = _sgw.lower()
        _gm_sel_uid = f'lora_{_sgl}_tryb_bramki'
        mqtt.publish(f"{HA_PREFIX}/select/{_gm_sel_uid}/config", json.dumps({
            "name": f"GW {_sgw} Tryb bramki", "object_id": _gm_sel_uid, "unique_id": _gm_sel_uid,
            "command_topic": f"{STATE_PREFIX}/supervisor/cmd/set_mode_{_sgl}",
            "options": ["Całodobowa", "Dzienna", "Nocna", "Kalendarz"],
            "state_topic": f"{STATE_PREFIX}/gw/{_sgl}/timestat",
            "value_template": "{{ value_json.mode | default('Całodobowa') }}",
            "icon": "mdi:theme-light-dark",
            "device": {"identifiers": [f"lora_gateway_{_sgl}"]}}, separators=(',', ':')), retain=True)
    _da_uid = 'lora_sup_dump_anom_all'                   # STEP 5: przycisk Dump Anomalies All
    mqtt.publish(f"{HA_PREFIX}/button/{_da_uid}/config", json.dumps({
        "name": "Dump Anomalies All", "object_id": _da_uid, "unique_id": _da_uid,
        "command_topic": f"{STATE_PREFIX}/supervisor/cmd/dump_anom_all",
        "icon": "mdi:reload-alert"}, separators=(',', ':')), retain=True)
    for _cb, _cn, _ci in [('clear_offline', 'Clear Offline', 'mdi:lan-disconnect'),
                          ('clear_battery', 'Clear Battery', 'mdi:battery-alert'),
                          ('clear_other', 'Clear Other', 'mdi:alert-circle')]:   # clear-all kubełków
        _u = f'lora_supervisor_{_cb}'
        mqtt.publish(f"{HA_PREFIX}/button/{_u}/config", json.dumps({
            "name": _cn, "object_id": _u, "unique_id": _u,
            "command_topic": f"{STATE_PREFIX}/supervisor/cmd/{_cb}",
            "icon": _ci}, separators=(',', ':')), retain=True)
    threading.Thread(target=dump_anom_cycle, daemon=True, name='dump-anom').start()  # STEP 5: 30min

    # ── STEP 5+: zegar SUPERVISORA (źródło czasu referencyjnego) na dashboardzie ──
    mqtt.publish(f"{HA_PREFIX}/sensor/lora_sup_local_time/config", json.dumps({
        "name": "Supervisor — czas (źródło sync)", "object_id": "lora_sup_local_time",
        "unique_id": "lora_sup_local_time",
        "state_topic": f"{STATE_PREFIX}/supervisor/timestat",
        "value_template": "{{ value_json.now | default('--') }}",
        "json_attributes_topic": f"{STATE_PREFIX}/supervisor/timestat",
        "icon": "mdi:clock-star-four-points"}, separators=(',', ':')), retain=True)

    def sup_clock_loop():
        while running.is_set():
            mqtt.publish(f"{STATE_PREFIX}/supervisor/timestat", json.dumps({
                'now': datetime.now().strftime('%H:%M:%S'),
                'date': datetime.now().strftime('%Y-%m-%d'),
                'role': 'źródło czasu (UTC sec w sync)'}, separators=(',', ':')), retain=True)
            try:
                refresh_gw_time()                         # kafle pory dnia/trybu nie czekają na HB
            except Exception as e:
                log.warn('TIME', 'refresh_gw_time: %s' % e)
            time.sleep(30)
    threading.Thread(target=sup_clock_loop, daemon=True, name='sup-clock').start()
    # STEP 17: bez pre-instancji — sync wyzwala porównanie `ph` w on_hb (HB/pong)

    def initial_handshake():
        # #2 CICHY START: zamiast eager disc/calendar/sync → PING do wszystkich bramek. Pong niesie
        # hashe (disc/cal/ph/ah) + tq → on_hb inicjuje resync TYLKO per-rozjazd. Zgodne hashe = 0
        # transferu (tylko ping+pong). Fallback: bramki wciąż bez mapy po 22s (pong zgubiony) → disc.
        time.sleep(8)
        log.info('CMD', '⚡ start: ping→pong (resync kalendarza/discovery/anomalii/czasu TYLKO przy rozjeździe)')
        sup_hb.manual_ping_all(known_gateways, send_broadcast_fn=lambda: send_to_all({"t": "ping"}))
        time.sleep(22)
        # Pomijamy bramki, których mapa WŁAŚNIE LECI — „brak mapy" jest wtedy stanem przejściowym,
        # a nie zgubionym pongiem. Bez tego fallback dokładał trzecie żądanie do transferu w toku.
        missing = [g for g in known_gateways
                   if not sup_disc.gw_synced.get(g) and not sup_disc._xfer_busy(g)]
        if missing:
            log.info('CMD', f'⚡ fallback disc (brak mapy po pingu: {missing})')
            for g in missing:
                send_to_all({"t": "disc", "g": g}); time.sleep(2)
        # F3/F7: pull parametrów robi get_sup_params przy tworzeniu mirrora (tylko świeży persist)
    threading.Thread(target=initial_handshake, daemon=True).start()

    def initial_calendar():                              # STEP 4: odczyt HA (bez eager push — #2)
        time.sleep(12)
        # #2: TYLKO odczyt HA → ustala expected_cal_hash. Push robi on_hb per-bramka WYŁĄCZNIE
        # gdy cal-hash z pong ≠ expected (koniec bezwarunkowego push do wszystkich na starcie).
        log.info('CAL', f'📅 startowy odczyt HA: {read_ha_calendar()} slotów (push tylko przy rozjeździe)')
        si = cal_cfg.get('sync_interval', 0)             # 0 = tylko start/przycisk; >0 = okresowo
        while running.is_set() and si > 0:
            time.sleep(si)
            # 2026-08-13: cykl tylko CZYTA HA i odświeża OCZEKIWANY hash. Wysyłkę robi drift-branch
            # w on_hb — czyli wyłącznie do bramki, która faktycznie ma inny harmonogram. Wcześniejsze
            # `targeted_calendar_sync()` pchało pełny transfer do KAŻDEJ bramki co interwał, także gdy
            # nikt nic nie zmienił. Bez tego odświeżenia edycja `calendar.lora_global` na HA supa była
            # niewidoczna aż do restartu/przycisku: hash bramki zgadzał się ze STARYM expected.
            if read_ha_calendar():
                for _cg in known_gateways:
                    expected_cal_hash[_cg] = scheduler.schedule_hash(_cg, win_days)
                log.info('CAL', f'📅 cykliczny odczyt HA ({si}s) → expected_cal_hash odświeżony '
                                f'(push zrobi drift-branch tylko przy realnej różnicy)')
    threading.Thread(target=initial_calendar, daemon=True, name='cal-init').start()

    log.info('MAIN', '═' * 50)
    log.info('MAIN', f'SUPERVISOR running (STEP 5 — ANOMALIE + TRYB BRAMKI) gateways={known_gateways}')
    log.info('MAIN', f'  push (down): HA {cal_id} → replace_global → CalendarTransfer; drift cal w HB → re-sync')
    log.info('MAIN', f'  pull (up):   gw_push → mirror calendar.lora_<gw> na HA supervisora (master=global)')
    log.info('MAIN', f'  safe window: {"ON ≤"+str(slot_cfg.get("safe_window_seconds",10))+"s po RX" if safe else "OFF"}  |  przyciski: Sync Calendar All / per-gw')
    log.info('MAIN', '═' * 50)

    def control_loop():
        while running.is_set():
            time.sleep(3)
            sup_disc.check_complete(max_try=4)
            now = time.time()
            with cmd_lock: items = list(pending_cmd.items())
            for (gw, dev), p in items:
                if now - p['since'] < ctrl_timeout: continue
                # RETRY BEZ FLOODU (2026-09-10) — reguła w modules.protocol.bulk.retry_action
                # (jedno źródło + test `test_cmd_retry.py`). Ponowienia idą tym samym Coalescerem
                # co pierwsza tura, więc 78 nieodpowiadających = znów `cmdm`, nie 78 ramek `cmd`.
                if retry_action(p['retries_left'], dev_avail.get((gw, dev))) == 'retry':
                    p['retries_left'] -= 1; p['since'] = now
                    log.warn('CTRL', f'🔁 brak st {gw}/{dev} — retry (zostało {p["retries_left"]})')
                    m = p['msg']
                    cmd_bulk.add((gw, dev, m.get('c', 'state'), m.get('v')))
                else:
                    with cmd_lock: pending_cmd.pop((gw, dev), None)
                    cmd_failed.add((gw, dev))            # sticky: round-trip nie potwierdził → offline
                    _set_avail(gw, dev, False)           # spójnie w dev_avail (OR-merge, nie tylko HA)
                    log.warn('CTRL', f'💀 {gw}/{dev} brak st → OFFLINE')
            # STEROWANIE WIRTUALNE: ta sama zasada co wyżej — bez ACK w oknie `ctrl_timeout`
            # powtarzamy, po wyczerpaniu prób GŁOŚNO zgłaszamy. Wyzwalacz, który przepadł
            # po cichu, jest gorszy niż brak wyzwalacza: nikt nie wie, że światło nie zgasło.
            with vio_lock: vio_items = list(pending_vio.items())
            for (gw, rodzaj, vid), p in vio_items:
                if now - p['since'] < ctrl_timeout:
                    continue
                if p['retries_left'] > 0:
                    p['retries_left'] -= 1; p['since'] = now
                    log.warn('CTRL', f'🔁 brak potwierdzenia {rodzaj} {gw}/{vid} — retry '
                                     f'(zostało {p["retries_left"]})')
                    send_to_all(p['msg'])
                else:
                    with vio_lock: pending_vio.pop((gw, rodzaj, vid), None)
                    log.error('CTRL', f'❌ {rodzaj} {gw}/{vid} BEZ POTWIERDZENIA po '
                                      f'{ctrl_retries + 1} próbach — wyzwalacz NIE dotarł')
    threading.Thread(target=control_loop, daemon=True, name='control').start()

    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass
    sup_hb.running = False; lora.stop(); mqtt.stop()
    log.info('MAIN', 'Stopped.')


# ── Main ────────────────────────────────────────────────
def main():
    log = Log()
    log.info('MAIN', '═' * 50)
    log.info('MAIN', 'STEP 5 — ANOMALIE + TRYB BRAMKI')
    log.info('MAIN', f'Role: {ROLE.upper()} ({CONFIG["id"]})')
    log.info('MAIN', '═' * 50)
    if ROLE == 'gateway':
        run_gateway(log)
    else:
        run_supervisor(log)
    log.close()


if __name__ == '__main__':
    main()
