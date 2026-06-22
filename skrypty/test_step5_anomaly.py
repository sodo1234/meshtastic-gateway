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

import json, os, signal, sys, time, threading
from datetime import datetime
from logging.handlers import RotatingFileHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from config import CONFIG, ROLE
from modules.transport import Dispatcher, MqttTransport, LoraTransport
from modules.protocol import (HAEntities, GatewayHeartbeat, SupervisorHeartbeat,
                               GatewayDiscovery, SupervisorDiscovery)
from modules.data import GatewayData, SupervisorData
from modules.params import ParamSync
from modules.anomaly import (StagnationEngine, OfflineMonitor, GatewayOfflineAnomaly,
                             BatteryMonitor, AnomalyBatcher, AnomalyStore)
from modules.calendar import (ScheduleManager, CalendarTransfer, build_ics,
                              write_ics_atomic, fetch_ha_calendar, write_ha_calendar,
                              reload_local_calendar)
from modules.slotting import SlotScheduler, SafeWindow
from modules.gateway_mode import GatewayMode

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
    ha = HAEntities(mqtt, logger=log)
    vio_config = CONFIG.get('virtual_io', [])

    discovery = GatewayDiscovery(
        gw_id=gw_id,
        monitored_names=CONFIG.get('monitored', []),
        priority_names=CONFIG.get('priority_devices', []),
        vio_config=vio_config, lora=lora, logger=log,
        max_payload=CONFIG.get('discovery', {}).get('max_payload', 150))

    # HB: monitored WYŁĄCZNY (bez priority). is_monitored() liczy priority jako monitored
    # (→ mon=6); tu liczymy mon=monitored\priority (=4), pri=priority (=2), dev=total (=7).
    # get_devices_summary zwraca REFERENCJE do wewn. dictów → kopiujemy, nie mutujemy w miejscu.
    def _devices_summary_excl():
        return {n: {**i, 'monitored': bool(i.get('monitored') and not i.get('priority'))}
                for n, i in discovery.get_devices_summary().items()}

    heartbeat = GatewayHeartbeat(
        gw_id=gw_id, devices_fn=_devices_summary_excl,
        lora=lora, logger=log,
        interval=CONFIG.get('heartbeat_interval', 120),
        diag_fn=lambda: {'hash': discovery.disc_hash, 'ph': param_sync.params_hash(),
                         'cal': scheduler.schedule_hash(gw_id, cal_cfg.get('window_days', 14)),
                         'm': cal_state['mode'],        # STEP 4: hash kalendarza + bieżący tryb
                         'oh': _offline_hash(),         # UNIFIKACJA: hash zbioru offline → drift→reconcile
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
        mon_interval=data_cfg.get('mon_interval', 30),
        pri_interval=data_cfg.get('pri_interval', 10),
        max_payload=data_cfg.get('max_payload', 120),
        thresholds=data_cfg.get('thresholds'),
        send_spacing=CONFIG.get('lora', {}).get('tx_cooldown', 3.0),   # honor LoRa cooldown
        report_interval=data_cfg.get('report_interval', 0),            # #8 periodic heartbeat
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
    vio_states = {v['id']: ('ON' if v.get('default') else 'OFF')
                  for v in vio_config if v['type'] == 'switch'}
    gw_stats = {'last_sup_rx_ts': 0.0, 'last_sup_rx': '--',
                'last_sync': '--', 'time_offset': '--'}
    SUP_LINK_TIMEOUT = CONFIG.get('sup_link_timeout', 3600)

    def lora_send(obj):
        lora.send(json.dumps(obj, separators=(',', ':')))

    def for_me(d):
        g = d.get('g'); return (g is None) or (g == gw_id)

    # STEP 17: parametry (BRAMKA = master) — P1/P2/P3 stagnation + T1/T2/T3 offline mirror
    param_sync = ParamSync(
        'gateway', gw_id, mqtt, lora_send, logger=log,
        persist_path='/tmp/lora_params.json',
        ha_prefix=HA_PREFIX, state_prefix=STATE_PREFIX,
        on_change=lambda k, v: log.info('PARAM', f'↻ apply {k}={v} (mechanizm)'))

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

    # ── STEP 5: tryb bramki (day/night/all-time) — supresja offline gdy nieaktywna ──
    gm_cfg = CONFIG.get('gateway_mode', {})
    gw_mode = GatewayMode(operating_mode=gm_cfg.get('operating_mode', 'all-time'),
                          day_start=gm_cfg.get('day_start', '06:00'),
                          day_end=gm_cfg.get('day_end', '20:00'),
                          lat=gm_cfg.get('lat'), lon=gm_cfg.get('lon'), logger=log)

    # ── STEP 5: anomalie (offline+battery+stagnation, ALL devices) → AnomalyBatcher → `ab` P2 ──
    an_cfg = CONFIG.get('anomaly', {})
    anomaly_batcher = AnomalyBatcher(
        gw_id, lora_send, interval=an_cfg.get('flush_interval', 30),
        max_payload=an_cfg.get('max_payload', 150), logger=log)

    def _offline_min_for(dtype):                         # minuty per typ (T1/T2/T3) dla anomalii offline
        key = {'switch': 'T1', 'light': 'T1', 'sensor': 'T2',
               'binary_sensor': 'T3'}.get(dtype, 'T3')
        return param_sync.get(key) or 120

    offline_anom = GatewayOfflineAnomaly(
        gw_id, discovery, data, emit=anomaly_batcher.add,
        get_timeout=_offline_min_for, logger=log,
        check_interval=an_cfg.get('offline_check', 30),
        grace=an_cfg.get('offline_grace', 120),
        is_active=gw_mode.is_active)                      # supresja: nieaktywna bramka → brak offline

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
        get_thresholds=lambda: (an_cfg.get('battery_low', 25), an_cfg.get('battery_critical', 15)),
        logger=log, check_interval=an_cfg.get('battery_check', 300))

    # STEP 5: stagnacja ALL devices, emit przez batcher (zamiast bezpośredniego `ab`)
    stagnation = StagnationEngine(
        gw_id, discovery, data, get_thresholds=lambda: (param_sync.get('P1'), param_sync.get('P2')),
        logger=log, check_interval=an_cfg.get('stagnation_check', 300),
        emit=anomaly_batcher.add, all_devices=True)

    def handle_dump_anom(d):                              # STEP 5: live re-check → pełny zrzut anomalii
        if d.get('g') not in (gw_id, None):
            return
        snap = offline_anom.snapshot() + battery_mon.snapshot() + stagnation.snapshot()
        log.info('ANOM', f'🔄 dump_anom: {len(snap)} aktualnych anomalii → ab')
        for sid, code, val in snap:
            anomaly_batcher.add(sid, code, val)
        anomaly_batcher.flush()

    # ── STEP 4: kalendarz (krok 16) — bramka odbiera harmonogram → ICS + tryb ──
    cal_cfg = CONFIG.get('calendar', {})
    ha_cfg = CONFIG.get('ha_api', {})
    mode_names = {int(k): v for k, v in CONFIG.get('mode_names', {}).items()}
    win_days = cal_cfg.get('window_days', 14)
    scheduler = ScheduleManager(mode_names=mode_names, gateways=[gw_id], logger=log,
                                persist_path='/tmp/lora_schedule_gw.json')
    cal_state = {'mode': 0, 'next': 0, 'src': 'none'}

    def publish_calstat():
        mode, nxt, src = scheduler.compute_now_and_next(gw_id)
        cal_state.update(mode=mode, next=nxt, src=src)
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
        n = scheduler.merge_schedule(gw_id, full)
        log.info('CAL', f'📅 odebrano {len(full)} slotów ({direction}) → merge ({n} zmian)')
        slots = scheduler.get_effective_schedule(gw_id)
        ics_path = ha_cfg.get('ics_path', '')
        if ics_path:
            content = build_ics(slots, mode_names, cal_name=f"LoRa {gw_id}")
            if write_ics_atomic(ics_path, content, logger=log):
                threading.Thread(target=lambda: reload_local_calendar(
                    ha_cfg.get('url', ''), ha_cfg.get('token', ''), logger=log),
                    daemon=True, name='ics-reload').start()
        else:
            log.info('CAL', '📅 brak ics_path → tylko encja trybu (bez zapisu kalendarza HA)')
        publish_calstat()

    # cal_* idą surowym lora (natychmiast) — transfer ma własny CRC+retransmit
    cal_transfer = CalendarTransfer(
        send_fn=lora_send, chunk_size=cal_cfg.get('chunk_size', 140),
        chunk_delay=cal_cfg.get('chunk_delay', 6.0),
        chunk_ack_timeout=cal_cfg.get('chunk_ack_timeout', 30.0),   # anty-spam: round-trip LoRa
        chunk_retries=cal_cfg.get('chunk_retries', 2),
        end_retries=cal_cfg.get('end_retries', 3),
        on_received=on_calendar_received, logger=log)

    def push_schedule_up():
        """REVERSE (gw→sup): pchnij harmonogram bramki w górę przez CalendarTransfer (gw_push).
        Źródło: encja gw_calendar_id na HA bramki (jeśli ustawiona) — inaczej bieżący effective."""
        gw_cal_id = cal_cfg.get('gw_calendar_id', '')
        if gw_cal_id and ha_cfg.get('token'):
            src = fetch_ha_calendar(ha_cfg.get('url', ''), ha_cfg.get('token', ''),
                                    calendar_id=gw_cal_id, days=90, logger=log)
            for s in src:                                # edycje lokalne → override bramki
                scheduler.add_slot(gw_id, s['start'], s['end'], note=s.get('note', ''))
        compact, h = scheduler.prepare_compact(gw_id, win_days)
        if not compact:
            log.warn('SYNC', '⤴️ Push Up: brak slotów do wysłania'); return
        log.info('SYNC', f'📤 Push Up {gw_id}: {len(compact)} slotów (hash={h}) → supervisor')
        cal_transfer.start_send(gw_id, 'gw_push', compact)

    # ── Dispatcher (LoRa RX from supervisor) ──
    dispatcher = Dispatcher(logger=log)
    dispatcher.register('ping', lambda d: heartbeat.handle_ping(d))

    def handle_disc_request(d):
        if not for_me(d): return
        log.info('CMD', '⚡ Discovery requested')
        threading.Thread(target=lambda: discovery.send_discovery_with_delay(tx_delay),
                         daemon=True).start()
    dispatcher.register('disc', handle_disc_request)

    def handle_cmd(d):
        if not for_me(d): return
        dev = d.get('d'); cap = d.get('c', 'state'); val = str(d.get('v', '')).upper()
        if dev in discovery.devices:                       # REALNE urządzenie Zigbee → steruj przez Z2M
            z2m_cap = cap if cap != 'state' else 'state'
            mqtt.publish(f'zigbee2mqtt/{dev}/set',
                         json.dumps({z2m_cap: val}, separators=(',', ':')))
            pending_st[dev] = (cap, val, time.time())
            log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → Z2M set (czekam na realne potwierdzenie)')
        else:                                              # urządzenie wirtualne (vio) — bez Z2M
            dev_states[dev] = val
            log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → applied (virtual)')
            lora_send({'t': 'st', 'g': gw_id, 'd': dev, 'c': cap, 'v': val})
    dispatcher.register('cmd', handle_cmd)

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
        if not was_pending and last_fwd_state.get(dev) == (cap, real):
            return                                         # zewn. powtórka bez zmiany → nie spamuj
        last_fwd_state[dev] = (cap, real)
        log.info('CTRL', f'🎛️ {dev} {cap}={real} (z2m, {"cmd" if was_pending else "zewn."}) '
                 f'→ st (online+licznik)')
        lora_send({'t': 'st', 'g': gw_id, 'd': dev, 'c': cap, 'v': real})
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

    def handle_sync(d):
        if not for_me(d): return
        sec = d.get('sec')
        if sec:
            offset = int(time.time()) - int(sec)
            gw_stats['time_offset'] = f'{offset:+d}s'
            gw_stats['last_sync'] = datetime.now().strftime('%H:%M:%S')
            log.info('CMD', f'🕐 sync: offset={offset:+d}s')
    dispatcher.register('sync', handle_sync)
    dispatcher.register('req', data.handle_req)          # STEP 3: refresh on-demand
    dispatcher.register('dump_anom', handle_dump_anom)   # STEP 5: reconcyliacja anomalii
    for _ct in ('cal_begin', 'cal_chunk', 'cal_end', 'cal_ack', 'cal_cack'):  # STEP 4: transfer kalendarza (per-chunk ACK)
        dispatcher.register(_ct, cal_transfer.dispatch)  # handle_end → on_calendar_received
    dispatcher.register('params', param_sync.handle_remote)      # STEP 17: proposal z supervisora
    dispatcher.register('params_req', param_sync.handle_remote)  # STEP 17: żądanie pełnego stanu
    dispatcher.register('cfg', lambda d: log.info('RX', 'cfg (passthrough)'))
    dispatcher.set_fallback(lambda d: log.debug('RX', f'unknown t={d.get("t")}'))

    # ── Local MQTT (gateway broker) ──
    def on_mqtt(topic, payload):
        if topic == 'zigbee2mqtt/bridge/devices':
            discovery.parse_z2m(payload)
            for dev in discovery.devices:                # STEP 3: lokalne encje LQI
                reg_gw_lqi(dev)
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
            threading.Thread(target=lambda: discovery.send_discovery_with_delay(tx_delay),
                             daemon=True).start()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/push_schedule':
            log.info('BTN', '🔘 Push Schedule Up pressed')          # STEP 4: reverse gw→sup
            threading.Thread(target=push_schedule_up, daemon=True, name='cal-push-up').start()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/sync_schedule':
            log.info('BTN', '🔘 Sync Schedule (pobierz) pressed')   # STEP 4: pull down — poproś supervisora
            lora_send({'t': 'cal_req', 'g': gw_id})
        elif topic.startswith(f'{STATE_PREFIX}/vio/') and topic.endswith('/state'):
            vid = topic.split('/')[2]                     # hydratacja: retained stan vswitcha → vio_states
            if vid in vio_states:
                vio_states[vid] = 'ON' if payload.upper() in ('ON', '1') else 'OFF'
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
        except Exception:
            pass
        dispatcher.dispatch_raw(text)

    mqtt._on_message_cb = on_mqtt
    lora.on_receive = on_lora
    mqtt.subscribe('zigbee2mqtt/bridge/devices')
    mqtt.subscribe('zigbee2mqtt/+')                      # STEP 3: device states
    mqtt.subscribe('zigbee2mqtt/+/availability')         # FAZA 2: z2m native availability
    mqtt.subscribe(f'{STATE_PREFIX}/gw/{gw_lower}/cmd/#')
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/state')   # hydratacja: retained stan vswitchy → vio_states
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/set')
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/press')
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
            ('disc_hash', 'Hash Disc', 'mdi:fingerprint', 'disc_hash')]:
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
    param_sync.register_entities()                       # STEP 17: 6 encji number na HA bramki
    stagnation.start()                                   # STEP 15: pętla stagnacji
    anomaly_batcher.start()                              # STEP 5: flush `ab` co flush_interval
    offline_anom.start()                                 # STEP 5: gateway-side offline (do/dn)
    battery_mon.start()                                  # STEP 5: low/critical battery (lb/cb/bo)
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
    _GM_LABELS = {"all-time": "Całodobowa", "day": "Dzienna", "night": "Nocna"}
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
    publish_calstat()                                    # bieżący tryb (z persist /tmp/lora_schedule_gw.json)

    def calendar_loop():                                 # przeliczaj tryb co minutę (zmiana slotu)
        while running.is_set():
            time.sleep(60)
            publish_calstat()
            publish_gmstat()                             # STEP 5: odśwież tryb pracy (przejścia day/night)
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
        linked = gw_stats['last_sup_rx_ts'] > 0 and (now - gw_stats['last_sup_rx_ts']) < SUP_LINK_TIMEOUT
        total = len(discovery.devices)
        prio = sum(1 for d in discovery.devices.values() if d.get('priority'))
        mon = sum(1 for d in discovery.devices.values()           # wyłączny (bez priority)
                  if d.get('monitored') and not d.get('priority'))
        oa = getattr(data, 'offline_after', 0) or 0               # offline = cisza z2m > offline_after
        offline = 0
        if oa:
            for dev in discovery.devices:
                ts = data.last_msg_ts.get(dev)
                if ts is not None and (now - ts) > oa:
                    offline += 1
                elif ts is None and (now - data._start_ts) > oa:  # nigdy nie raportował po grace
                    offline += 1
        last_hb = (datetime.fromtimestamp(heartbeat._last_hb).strftime('%H:%M:%S')
                   if heartbeat._last_hb else '--')
        ha.pub_gw_stats(gw_id, {
            'uptime': int(now - heartbeat.start_time),
            'total': total, 'monitored': mon, 'priority': prio, 'offline': offline,
            'last_hb': last_hb,
            'sup_link': 'ON' if linked else 'OFF', 'sup_last_rx': gw_stats['last_sup_rx'],
            'time_offset': gw_stats['time_offset'], 'last_sync': gw_stats['last_sync'],
            'disc_hash': discovery.disc_hash or '--',        # STEP 17: hash disc
            'param_hash': param_sync.params_hash()})         # STEP 17: hash parametrów

    publish_gwstat()

    def gwstat_loop():
        while running.is_set():
            time.sleep(300); publish_gwstat()
    threading.Thread(target=gwstat_loop, daemon=True).start()

    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass
    heartbeat.running = False; stagnation.stop(); offline_anom.stop(); battery_mon.stop()
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
    ha = HAEntities(mqtt, logger=log)
    known_gateways = CONFIG.get('gateways', ['G1'])

    def send_to_all(msg):
        lora.send(json.dumps(msg, separators=(',', ':')))

    sup_disc = SupervisorDiscovery(
        ha, logger=log, request_disc_fn=lambda gw: send_to_all({"t": "disc", "g": gw}))

    # STEP 3: data layer — `b` → merged HA state
    dev_avail = {}                       # {(gw,dev): bool} — REALNY licznik offline (A)

    # ack_offline: urządzenia offline RĘCZNIE wyczyszczone (acknowledged) — anomalia NIE wraca
    # przez reconcile, dopóki urządzenie nie wróci online i nie padnie PONOWNIE (nowe zdarzenie).
    # Rozdziela AVAILABILITY (live truth, encja *_available zawsze prawdziwa) od ANOMALII (alert,
    # ack-owalny). Fix: clear realnie-offline urządzenia działa (wcześniej reconcile re-dodawał).
    ack_offline = set()                  # {(gw,dev)}

    def _set_avail(gw, dev, on):         # availability = ZAWSZE live truth; anomalia = na transition (chyba że ack)
        prev = dev_avail.get((gw, dev))
        dev_avail[(gw, dev)] = on
        ha.pub_device_avail(gw, dev, on)
        if prev != on:
            if on:
                ack_offline.discard((gw, dev))           # recovery → kasuj ack (re-alert gdy znów padnie)
                anomaly_store.remove_one(gw, dev, 'offline')
            elif (gw, dev) not in ack_offline:           # offline transition → anomalia (jeśli nie ack)
                anomaly_store.add_offline(gw, dev)

    # on_avail (batch `a:`) ROUTOWANE przez _set_avail → trafia też do listy offline (kluczowy fix
    # spójności: wcześniej batch pomijał _set_avail i lista nie zgadzała się z licznikiem).
    sup_data = SupervisorData(ha, sup_disc, logger=log,
                              on_avail=lambda gw, dev, av: _set_avail(gw, dev, av))
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
    cal_transfer = CalendarTransfer(
        send_fn=send_to_all, chunk_size=cal_cfg.get('chunk_size', 140),
        chunk_delay=cal_cfg.get('chunk_delay', 6.0),
        chunk_ack_timeout=cal_cfg.get('chunk_ack_timeout', 30.0),   # anty-spam: round-trip LoRa
        chunk_retries=cal_cfg.get('chunk_retries', 2),
        end_retries=cal_cfg.get('end_retries', 3), logger=log)
    expected_cal_hash = {gw: scheduler.schedule_hash(gw, win_days) for gw in known_gateways}
    cal_sync_guard = {}                                  # gw → ts ostatniego auto-sync (debounce drift)

    def on_calendar_received_sup(gw, direction, compact):
        """REVERSE (gw_push): bramka raportuje swój harmonogram → MIRROR na calendar.lora_<gw>
        (tylko podgląd na HA supervisora; master = calendar.lora_global). Brak sprzężenia w
        scheduler/expected_cal_hash → brak pętli push↔pull.

        Zapis: ICS do storage HA supervisora (jak bramka) — REST /api/calendars POST=405
        (read-only). Fallback do write_ha_calendar gdy brak mirror_ics_path."""
        full = scheduler.expand_compact(compact)
        mirror_tmpl = ha_cfg.get('mirror_ics_path', '')
        log.info('CAL', f'📥 gw_push {gw}: {len(full)} slotów → mirror calendar.lora_{gw.lower()}')
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
        """Pobierz calendar.lora_global z HA → replace_global. Zwraca liczbę slotów (0 bez tokenu)."""
        slots = fetch_ha_calendar(ha_cfg.get('url', ''), ha_cfg.get('token', ''),
                                  calendar_id=cal_id, days=90, logger=log)
        if slots:
            scheduler.replace_global(slots)
        return len(slots)

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

    # STEP 17: parametry — MIRROR (bramka = master). gw_id = pierwsza znana bramka.
    param_sync = ParamSync(
        'supervisor', known_gateways[0], mqtt, send_to_all, logger=log,
        persist_path='/tmp/lora_params_sup.json',
        ha_prefix=HA_PREFIX, state_prefix=STATE_PREFIX,
        on_change=lambda k, v: log.info('PARAM', f'↻ mirror {k}={v} (offline T1/T2/T3)'))

    # STEP 5: aktywność bramek (z HB ga) — bramka nieaktywna (np. nocna w dzień) → supresja offline
    gw_active = {}                                        # gw → bool (domyślnie aktywna)

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
                "name": f"GW {gw} {nm}", "object_id": iuid, "unique_id": iuid,
                "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                "json_attributes_topic": topic, "icon": icon, "device": di},
                separators=(',', ':')), retain=True)
            if cnt_eid:                                   # count (przycisk) — battery/other
                cuid = f"lora_gw_{gl}_{cnt_eid}"
                mqtt.publish(f"{HA_PREFIX}/sensor/{cuid}/config", json.dumps({
                    "name": f"GW {gw} {nm} #", "object_id": cuid, "unique_id": cuid,
                    "state_topic": topic, "value_template": "{{ value_json.count | default(0) }}",
                    "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        an_regd.add(gw)

    def publish_anomalies(gw):
        reg_anomaly_entities(gw)
        gl = gw.lower()
        for bucket in ('offline', 'battery', 'other'):
            items = anomaly_store.items(gw, bucket)
            mqtt.publish(f"{STATE_PREFIX}/gw/{gl}/an_{bucket}",
                         json.dumps({"count": len(items), "items": items},
                                    separators=(',', ':')), retain=True)

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

    def anomaly_popup(gw, dev, code, value, critical):   # STEP 5: browser_mod popup (krytyczne)
        if not an_cfg.get('popup', True) or not critical:
            return
        from modules.anomaly.store import CODE_LABEL
        content = f"**{gw} / {dev}**: {CODE_LABEL.get(code, code)}" + \
                  (f" = {value}" if value is not None else "")
        _ha_service('browser_mod', 'popup',
                    {'title': '🚨 Anomalia LoRa', 'content': content,
                     'timeout': 15000, 'dismissable': True})

    anomaly_store = AnomalyStore(
        resolve_dev=lambda gw, sid: sup_data._dev_from_sid(gw, sid),
        on_change=publish_anomalies, on_alert=anomaly_popup,
        persist_path=an_cfg.get('persist_path', '/tmp/lora_anomaly_ids.json'), logger=log)

    # ── UNIFIKACJA availability: offline-set bramki OR cmd-failure (jedno źródło prawdy) ──
    cmd_failed = set()                                    # {(gw,dev)} — cmd bez st (sticky offline w OR-merge)
    off_sync_guard = {}                                   # gw → ts (debounce reconcile po drift offline-hash)

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
                dump_and_reconcile(gw, prune_after)

    ctrl_cfg = CONFIG.get('control', {})
    ctrl_timeout = ctrl_cfg.get('timeout', 15); ctrl_retries = ctrl_cfg.get('retries', 2)
    pending_cmd = {}; cmd_lock = threading.Lock()
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
            "name": f"{dev} Refresh", "object_id": uid, "unique_id": uid,
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
            "name": f"{dev} Last Seen", "object_id": uid, "unique_id": uid,
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
        uid = f"lora_{gl}_{safe}_linkquality"
        mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
            "name": f"{dev} Link Quality", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ value_json.linkquality | default('') }}",
            "icon": "mdi:signal", "state_class": "measurement",
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
            "name": f"{dev} Contact", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ 'ON' if not value_json.contact else 'OFF' }}",
            "device_class": "opening",
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
            "name": f"{dev} Stagnation", "object_id": uid, "unique_id": uid,
            "state_topic": f"{STATE_PREFIX}/{gl}/{safe}/state",
            "value_template": "{{ 'ON' if value_json.stagnant == 'ON' else 'OFF' }}",
            "device_class": "problem", "icon": "mdi:timer-sand-paused",
            "device": {"identifiers": [f"lora_{gl}_{safe}"],
                       "via_device": f"lora_gateway_{gl}"}}, separators=(',', ':')),
            retain=True)
        stag_regd.add(key)

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

    gwhash_regd = set()                                  # bramki z zarejestrowanymi encjami hash

    def reg_gw_hashes(gw):
        """STEP 17: encje hash ZAPISANEGO NA BRAMCE (disc + param) na HA supervisora —
        z HB/pong. Podpięte pod urządzenie LoRa Gateway Gx (info bramki)."""
        if gw in gwhash_regd:
            return
        gl = gw.lower()
        di = {"identifiers": [f"lora_gateway_{gl}"]}
        for eid, nm, icon, key in [
                ('disc_hash', 'Hash Disc (bramka)', 'mdi:fingerprint', 'disc'),
                ('param_hash', 'Hash Param (bramka)', 'mdi:tune-variant', 'param')]:
            uid = f"lora_gw_{gl}_{eid}"
            mqtt.publish(f"{HA_PREFIX}/sensor/{uid}/config", json.dumps({
                "name": f"GW {gw} {nm}", "object_id": uid, "unique_id": uid,
                "state_topic": f"{STATE_PREFIX}/gw/{gl}/hashes",
                "value_template": "{{ value_json.%s | default('--') }}" % key,
                "icon": icon, "device": di}, separators=(',', ':')), retain=True)
        gwhash_regd.add(gw)

    wd = CONFIG.get('watchdog', {})
    sup_hb = SupervisorHeartbeat(
        ha, logger=log, send_ping_fn=lambda gw: send_to_all({"t": "ping", "g": gw}),
        devices_provider=sup_disc.devices, gateways=known_gateways,
        passive_timeout=wd.get('passive_timeout', 2100),
        ping_timeout=wd.get('ping_timeout', 25), max_retries=wd.get('max_retries', 2))

    # ── Dispatcher (LoRa RX from gateways) ──
    dispatcher = Dispatcher(logger=log)

    psync_guard = {'ph': None, 'ts': 0}                  # debounce żądań sync parametrów

    def on_hb(d):
        gw = d.get('g'); sup_hb.handle_hb(d)
        if gw:
            ha.reg_gw_controls(gw)
            reg_gw_hashes(gw)                             # STEP 17: encje hash bramki
            reg_cal_button_gw(gw)                         # STEP 4: per-bramka Sync Calendar
            reg_anomaly_entities(gw)                      # STEP 5: encje an_offline/_battery/_other
            mqtt.publish(f"{STATE_PREFIX}/gw/{gw.lower()}/hashes",
                         {'disc': d.get('hash', '--'), 'param': d.get('ph', '--')}, retain=True)
            ga = d.get('ga')                             # STEP 5: aktywność trybu bramki (supresja)
            if ga is not None:
                gw_active[gw] = bool(ga)
        sup_disc.note_hash(gw, d.get('hash', ''))
        cal = d.get('cal')                               # STEP 4: hash kalendarza bramki → drift
        if cal is not None and gw and cal != expected_cal_hash.get(gw):
            now = time.time()
            if now - cal_sync_guard.get(gw, 0) > 120:     # debounce: max raz / 120s
                cal_sync_guard[gw] = now
                log.info('CAL', f'🔄 {gw} cal drift ({cal}≠{expected_cal_hash.get(gw)}) → re-sync')
                targeted_calendar_sync([gw])
        # offline-hash `oh` w HB: zostawiony jako sygnał diagnostyczny, ale NIE triggeruje reconcile —
        # availability jest napędzana CIĄGŁYM batch `a:` (przez _set_avail), więc lista zawsze == licznik.
        # (Wcześniej `oh` z GatewayOfflineAnomaly ≠ hash z batch-a: → ciągłe niepotrzebne dumpy.)
        ph = d.get('ph')                                 # STEP 17: hash parametrów (wskaźnik driftu)
        if ph and ph != param_sync.params_hash():
            now = time.time()
            if psync_guard['ph'] != ph or now - psync_guard['ts'] > 120:
                psync_guard['ph'] = ph; psync_guard['ts'] = now
                # model v38: pull dzieje się na starcie i przyciskiem Send — NIE auto,
                # bo auto-pull klobbersował lokalne edycje przed naciśnięciem Send.
                log.debug('PARAM', f'⚙️ drift hash bramki={ph} ≠ mirror (Send aby zsync.)')
    dispatcher.register('hb', on_hb)
    dispatcher.register('pong', on_hb)
    dispatcher.register('disc_meta', sup_disc.handle_disc_meta)
    dispatcher.register('disc_vio', sup_disc.handle_disc_vio)

    def on_db(d):
        sup_disc.handle_db(d)
        gw = d.get('g', '?')                             # STEP 3: per-device app-level entities
        for dev, info in sup_disc.devices(gw).items():
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
        publish_offline_count(gw)                         # A: realny licznik po hydratacji/db
        anomaly_store.ghost_prune(gw, list(sup_disc.devices(gw).keys()))  # STEP 5: usuń anomalie znikłych
    dispatcher.register('db', on_db)
    dispatcher.register('disc_ack', lambda d: log.info('RX', f'disc_ack: {d}'))
    dispatcher.register('b', sup_data.handle_b)          # STEP 3: batch data → HA
    for _ct in ('cal_begin', 'cal_chunk', 'cal_end', 'cal_ack', 'cal_cack'):  # STEP 4: transfer kalendarza (per-chunk ACK)
        dispatcher.register(_ct, cal_transfer.dispatch)  # ACK od bramki / pull gw_push

    def handle_cal_req(d):                                # STEP 4: bramka prosi o harmonogram (pull down)
        gw = d.get('g', '?')
        log.info('CAL', f'⤵️ {gw} prosi o harmonogram → odczyt HA + push')
        read_ha_calendar(); targeted_calendar_sync([gw])
    dispatcher.register('cal_req', handle_cal_req)
    dispatcher.register('param_upd', param_sync.handle_remote)   # STEP 17: confirmed z bramki
    dispatcher.register('ab', handle_ab)                 # STEP 15: anomalie (stagnacja sg/sc)

    def handle_st(d):
        gw = d.get('g', '?'); dev = d.get('d'); val = str(d.get('v', '')).upper()
        if dev:
            with cmd_lock: pending_cmd.pop((gw, dev), None)
            cmd_failed.discard((gw, dev))                # st potwierdza działanie → czyść cmd-failure
            _set_avail(gw, dev, True)                    # STEP 4 fix: online w dev_avail → licznik spada
            ha.pub_device_state(gw, dev, {"state": val, "available": "ON"})
            log.info('CTRL', f'🎛️ st {gw}/{dev} = {val} → online + odbite w HA (cmd/zmiana zewn.)')
    dispatcher.register('st', handle_st)

    def handle_vsw_st(d):
        gw = d.get('g', '?'); vid = d.get('id')
        val = 1 if str(d.get('v')) in ('1', 'ON', 'on', 'True') else 0
        gl, safe = gw.lower(), _safe(vid)
        mqtt.publish(f'{STATE_PREFIX}/{gl}/vio/{safe}/state', 'ON' if val else 'OFF', retain=True)
        log.info('VIO', f'🔘 VSwitch {gw}/{vid} = {"ON" if val else "OFF"} → odbite w HA')
    dispatcher.register('vsw_st', handle_vsw_st)
    dispatcher.register('vbtn_ack', lambda d: log.info('RX', f'vbtn_ack {d.get("id")}'))
    dispatcher.set_fallback(lambda d: log.debug('RX', f'passthrough t={d.get("t")}'))

    # ── HA → LoRa bridge ──
    def on_mqtt(topic, payload):
        segs = topic.split('/')
        # hydratacja: retained stan urządzenia lora/<gl>/<safe>/state → bufor (seed po db)
        if len(segs) == 4 and segs[3] == 'state' and segs[2] != 'vio':
            try:
                pending_hydrate[(segs[1].upper(), segs[2])] = json.loads(payload)
            except Exception:
                pass
            return
        if topic.startswith(f'{STATE_PREFIX}/params/supervisor/set/'):   # STEP 17: edycja z HA sup
            param_sync.on_mqtt_set(topic, payload); return
        if topic.startswith(f'{STATE_PREFIX}/params/supervisor/cmd/'):   # przyciski Send Config/Timeout
            param_sync.on_cmd(topic, payload); return
        if topic.startswith(f'{STATE_PREFIX}/supervisor/req/'):   # STEP 3: refresh button
            if len(segs) == 5:
                gw, safe = segs[3].upper(), segs[4]
                dev = sup_disc.find_device(gw, safe) or safe
                log.info('CTRL', f'🎛️ Refresh {gw}/{dev} → req')
                send_to_all({"t": "req", "g": gw, "d": dev})
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
                        publish_offline_count(cg)
                        log.info('ANOM', f'🗑️ clear ręczny {cg}/{cd} [{cb}] n={n} (ack)')
                    except Exception as e:
                        log.warn('ANOM', f'clear_anomaly bad payload {payload!r}: {e}')
                elif a in ('clear_offline', 'clear_battery', 'clear_other'):  # CLEAR-ALL kubełka
                    bucket = a.split('_', 1)[1]
                    for g in known_gateways:
                        for it in list(anomaly_store.items(g, bucket)):
                            if bucket == 'offline':
                                cmd_failed.discard((g, it['dev'])); ack_offline.add((g, it['dev']))
                            anomaly_store.remove_one(g, it['dev'], bucket)
                        publish_offline_count(g)
                    log.info('BTN', f'🔘 Clear all [{bucket}] (ack)')
            elif len(rest) == 2:
                gw, a = rest[0].upper(), rest[1]
                if a == 'ping':
                    log.info('BTN', f'🔘 Ping {gw}'); sup_hb.manual_ping(gw)
                elif a == 'disc':
                    log.info('BTN', f'🔘 Discovery {gw}'); send_to_all({"t": "disc", "g": gw})
                elif a == 'sync':
                    log.info('BTN', f'🔘 Sync {gw}')
                    send_to_all({"t": "sync", "sec": int(time.time()), "g": gw})
                elif a == 'calendar':                     # STEP 4: re-read HA + push do tej bramki
                    log.info('BTN', f'🔘 Sync Calendar {gw}')
                    read_ha_calendar(); targeted_calendar_sync([gw])
            return
        if len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'set':
            gw, vid = segs[1].upper(), segs[3]
            val = 1 if payload.upper() in ('ON', '1') else 0
            # #5 optimistic: odbij stan w HA OD RAZU (klik nie gubi się przez latencję
            # LoRa / anty-spam) — realny vsw_st potem to potwierdzi/skoryguje.
            mqtt.publish(f'{STATE_PREFIX}/{gw.lower()}/vio/{_safe(vid)}/state',
                         'ON' if val else 'OFF', retain=True)
            log.info('CTRL', f'🎛️ dashboard VSwitch {gw}/{vid} → {"ON" if val else "OFF"} (optimistic)')
            send_to_all({"t": "vsw", "g": gw, "id": vid, "v": val})
        elif len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'press':
            gw, vid = segs[1].upper(), segs[3]
            log.info('CTRL', f'🎛️ dashboard VButton {gw}/{vid} pressed')
            send_to_all({"t": "vbtn", "g": gw, "id": vid})
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
            send_to_all(msg)

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
            g['_ts'] = now
            g['last_seen'] = datetime.now().strftime('%H:%M:%S')
            if not g.get('online'):
                g['online'] = True
                sup_hb._cascade_devices(gw, online=True)
                log.info('HB', f'💚 {gw} ONLINE (wiadomość LoRa)')
        sup_hb._publish_status(gw)

    def publish_offline_count(gw):
        """A: REALNY licznik offline = liczba urządzeń bramki z availability=off.
        Nadpisuje watchdogowe devices_offline (0/total) prawdziwą liczbą per-device."""
        off = sum(1 for (g, _d), av in dev_avail.items() if g == gw and not av)
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
    mqtt.subscribe(f'{STATE_PREFIX}/supervisor/req/+/+')   # STEP 3: refresh buttons
    mqtt.subscribe(f'{STATE_PREFIX}/params/supervisor/set/+')   # STEP 17: edycja parametrów
    mqtt.subscribe(f'{STATE_PREFIX}/params/supervisor/cmd/+')   # przyciski Send Config/Timeout
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
    param_sync.register_entities()                       # STEP 17: 6 encji number na HA supervisora
    # UNIFIKACJA: offline_mon usunięty — availability liczona z offline-set bramki (reconcile)
    reg_cal_button_global()                              # STEP 4: przycisk Sync Calendar All
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
    # STEP 17: bez pre-instancji — sync wyzwala porównanie `ph` w on_hb (HB/pong)

    def initial_discovery():
        time.sleep(8)
        log.info('CMD', '⚡ startowe discovery')
        send_to_all({"t": "disc"})
        time.sleep(2)
        log.info('PARAM', '⚙️ startowy pull parametrów (params_req → bramka push_all)')
        param_sync.request()                             # model v38: pull na starcie (nie auto-HB)
    threading.Thread(target=initial_discovery, daemon=True).start()

    def initial_calendar():                              # STEP 4: odczyt HA + push (po starcie bramek)
        time.sleep(12)
        log.info('CAL', f'📅 startowy odczyt HA: {read_ha_calendar()} slotów GLOBAL')
        targeted_calendar_sync()
        si = cal_cfg.get('sync_interval', 0)             # 0 = tylko start/przycisk; >0 = okresowo
        while running.is_set() and si > 0:
            time.sleep(si)
            if read_ha_calendar():
                targeted_calendar_sync()
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
                if p['retries_left'] > 0:
                    p['retries_left'] -= 1; p['since'] = now
                    log.warn('CTRL', f'🔁 brak st {gw}/{dev} — retry (zostało {p["retries_left"]})')
                    send_to_all(p['msg'])
                else:
                    with cmd_lock: pending_cmd.pop((gw, dev), None)
                    cmd_failed.add((gw, dev))            # sticky: round-trip nie potwierdził → offline
                    _set_avail(gw, dev, False)           # spójnie w dev_avail (OR-merge, nie tylko HA)
                    log.warn('CTRL', f'💀 {gw}/{dev} brak st → OFFLINE')
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
