#!/usr/bin/env python3
"""
Step 2 Integration Test — Protocol (HB + Discovery + Control + VIO)

Auto-detekcja roli z config.py:
  - GATEWAY    → HB+pong (z diagnostyką), discovery, sterowanie urządzeń, VIO
  - SUPERVISOR → aktywny ping-watchdog, rejestracja encji HA, most dashboard→LoRa

Testuje (pełna procedura w docstringu PROCEDURA niżej):
  1. Autodiscovery: disc → bramka wysyła listę → supervisor tworzy encje w MQTT/HA
  2. Ping adresowany (g=Gx): tylko wskazana bramka odpowiada pong
  3. Diagnostyka w hb/pong: total/monitored/priority + uptime + z2m + hash, last_seen sup-side
  4. Ping-watchdog: ping + 2 retry; po 3 brakach bramka I urządzenia → offline
  5. Sterowanie: switch z dashboardu → cmd przez LoRa → bramka → status (st) → HA odbija stan
  6. Dwustronne VIO: vswitch/vbutton z dashboardu → LoRa → bramka → vsw_st → HA
  7. Debug: każdy pakiet LoRa logowany jako TX / RX

PROCEDURA TESTOWA (co kliknąć / sprawdzić):
  A) Autodiscovery   — HA(sup): button "Discovery All" → pojawiają się sensory/switch lora_g1_*
  B) Ping adresowany — HA(sup): "Ping All" → log GATEWAY "🏓 PONG"; G2 (gdyby był) milczy
  C) Diagnostyka     — encje GW Total/Monitored/Priority/Uptime/Z2M/Disc Hash mają wartości
  D) Offline cascade — ubij proces gateway → po ~ping_interval+3×ping_timeout: GW status=offline,
                       devices_offline=total, wszystkie urządzenia available=OFF
  E) Sterowanie      — HA(sup): przełącz switch lora_g1_test_1 → log "CTRL cmd ... applied" na GW,
                       "st reflected" na SUP, stan switcha wraca z bramki
  F) VIO dwustronne  — HA(sup): przełącz lora_g1_vsw_vs_test → log VIO na GW + vsw_st → HA odbija;
                       przycisk lora_g1_vbtn_vb_test → log "VButton pressed" na GW
  G) TX/RX           — logi pokazują 📤 TX i 📥 RX dla każdego pakietu
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

STATE_PREFIX = CONFIG.get('state_prefix', 'lora')


def _safe(s):
    return str(s).replace(' ', '_').lower()


# ── Logger with colors and icons ────────────────────────
class Log:
    LEVEL = {
        'DEBUG': ('\033[36m', '🔍'),
        'INFO':  ('\033[32m', ''),
        'WARN':  ('\033[33m', '⚠️'),
        'ERROR': ('\033[31m', '❌'),
    }
    COMP = {
        'MAIN': '🚀', 'MQTT': '📡', 'LORA': '📻', 'ANT': '📡',
        'HA': '🏠', 'HB': '💓', 'DISC': '🔭', 'VIO': '🔘',
        'DISPATCH': '📨', 'TX': '📤', 'RX': '📥', 'STATS': '📊',
        'CMD': '⚡', 'BTN': '🔘', 'CTRL': '🎛️',
    }
    R = '\033[0m'

    def __init__(self):
        self._h = RotatingFileHandler(CONFIG['log_file'], maxBytes=2_000_000, backupCount=2)
        self._h.setFormatter(logging.Formatter('%(message)s'))
        self._f = logging.getLogger('test_step2')
        self._f.addHandler(self._h)
        self._f.setLevel(logging.DEBUG)

    def _w(self, lvl, comp, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        color, _ = self.LEVEL.get(lvl, ('', ''))
        icon = self.COMP.get(comp, '•')
        line = f"{ts} {color}[{lvl}]{self.R} {icon} [{comp}] {msg}"
        print(line, flush=True)
        self._f.info(f"{ts} [{lvl}] [{comp}] {msg}")

    def debug(self, c, m): self._w('DEBUG', c, m)
    def info(self, c, m): self._w('INFO', c, m)
    def warn(self, c, m): self._w('WARN', c, m)
    def error(self, c, m): self._w('ERROR', c, m)
    def close(self):
        try: self._h.close()
        except: pass


def _make_lora(cfg, log):
    """Buduje LoraTransport z CONFIG (gateway: mesh_port, supervisor: mesh_ports[])."""
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
    running = threading.Event()
    running.set()
    mqtt_cfg = CONFIG['mqtt']
    tx_delay = CONFIG.get('discovery', {}).get('tx_delay', 4.0)
    gw_id = CONFIG['id']
    gw_lower = gw_id.lower()

    mqtt = MqttTransport(
        host=mqtt_cfg['host'], port=mqtt_cfg['port'],
        user=mqtt_cfg['user'], password=mqtt_cfg['pass'],
        client_id=f"step2_gw_{gw_id}_{int(time.time())}",
        logger=log)

    lora = _make_lora(CONFIG, log)

    ha = HAEntities(mqtt, logger=log)
    vio_config = CONFIG.get('virtual_io', [])
    discovery = GatewayDiscovery(
        gw_id=gw_id,
        monitored_names=CONFIG.get('monitored', []),
        priority_names=CONFIG.get('priority_devices', []),
        vio_config=vio_config,
        lora=lora, logger=log,
        max_payload=CONFIG.get('discovery', {}).get('max_payload', 150))

    heartbeat = GatewayHeartbeat(
        gw_id=gw_id,
        devices_fn=discovery.get_devices_summary,
        lora=lora, logger=log,
        interval=CONFIG.get('heartbeat_interval', 120),
        diag_fn=lambda: {'hash': discovery.disc_hash})   # air/z2m usunięte (redundantne)

    # Virtual state stores (no real hardware in step2 — gateway echoes)
    dev_states = {}                                   # device name → 'ON'/'OFF'
    vio_states = {v['id']: ('ON' if v.get('default') else 'OFF')
                  for v in vio_config if v['type'] == 'switch'}

    # statystyki bramki (link do supervisora + sync czasu) → lora/<gl>/gwstat
    gw_stats = {'last_sup_rx_ts': 0.0, 'last_sup_rx': '--',
                'last_sync': '--', 'time_offset': '--'}
    SUP_LINK_TIMEOUT = CONFIG.get('sup_link_timeout', 3600)   # 1h bez RX → link OFF

    def lora_send(obj):
        lora.send(json.dumps(obj, separators=(',', ':')))   # transport logs 📤 TX: {json}

    def for_me(d):
        g = d.get('g')
        return (g is None) or (g == gw_id)

    # ── Dispatcher (LoRa RX from supervisor) ──
    dispatcher = Dispatcher(logger=log)
    dispatcher.register('ping', lambda d: heartbeat.handle_ping(d))   # filters g internally

    def handle_disc_request(d):
        if not for_me(d):
            log.debug('RX', f"disc dla {d.get('g')} — nie moja, ignoruję")
            return
        log.info('CMD', '⚡ Discovery requested')
        threading.Thread(
            target=lambda: discovery.send_discovery_with_delay(tx_delay),
            daemon=True).start()
    dispatcher.register('disc', handle_disc_request)

    def handle_cmd(d):
        if not for_me(d):
            return
        dev = d.get('d'); cap = d.get('c', 'state')
        val = str(d.get('v', '')).upper()
        dev_states[dev] = val
        log.info('CTRL', f'🎛️ cmd {dev} {cap}={val} → applied (virtual)')
        lora_send({'t': 'st', 'g': gw_id, 'd': dev, 'c': cap, 'v': val})
    dispatcher.register('cmd', handle_cmd)

    def handle_vsw(d):
        if not for_me(d):
            return
        vid = d.get('id')
        val = 1 if str(d.get('v')) in ('1', 'ON', 'on', 'True') else 0
        vio_states[vid] = 'ON' if val else 'OFF'
        log.info('VIO', f'🔘 VSwitch {vid} → {vio_states[vid]} (z LoRa)')
        lora_send({'t': 'vsw_st', 'g': gw_id, 'id': vid, 'v': val})
    dispatcher.register('vsw', handle_vsw)

    def handle_vbtn(d):
        if not for_me(d):
            return
        vid = d.get('id')
        log.info('VIO', f'🔘 VButton {vid} pressed (z LoRa)')
        lora_send({'t': 'vbtn_ack', 'g': gw_id, 'id': vid})
    dispatcher.register('vbtn', handle_vbtn)

    def handle_sync(d):
        if not for_me(d):
            return
        sec = d.get('sec')
        if sec:
            offset = int(time.time()) - int(sec)
            gw_stats['time_offset'] = f'{offset:+d}s'
            gw_stats['last_sync'] = datetime.now().strftime('%H:%M:%S')
            log.info('CMD', f'🕐 sync: supervisor sec={sec}, offset={offset:+d}s (czas zsynchronizowany)')
        else:
            log.info('CMD', '🕐 sync (bez sec)')
    dispatcher.register('sync', handle_sync)
    dispatcher.register('cfg', lambda d: log.info('RX', 'cfg (passthrough)'))
    dispatcher.set_fallback(lambda d: log.debug('RX', f'unknown t={d.get("t")}'))

    # ── Local MQTT (gateway broker) ──
    def on_mqtt(topic, payload):
        if topic == 'zigbee2mqtt/bridge/devices':
            discovery.parse_z2m(payload)
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/ping':
            log.info('BTN', '🔘 Ping button pressed')
            heartbeat.send_heartbeat()
        elif topic == f'{STATE_PREFIX}/gw/{gw_lower}/cmd/discovery':
            log.info('BTN', '🔘 Discovery button pressed')
            threading.Thread(
                target=lambda: discovery.send_discovery_with_delay(tx_delay),
                daemon=True).start()
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
        try:                                                 # link do supervisora = ostatni RX
            t = json.loads(text).get('t', '?')
            gw_stats['last_sup_rx_ts'] = time.time()
            gw_stats['last_sup_rx'] = f"{datetime.now().strftime('%H:%M:%S')} ({t})"
        except Exception:
            pass
        dispatcher.dispatch_raw(text)                       # transport already logged 📥 RX: {json}

    mqtt._on_message_cb = on_mqtt
    lora.on_receive = on_lora
    mqtt.subscribe('zigbee2mqtt/bridge/devices')
    mqtt.subscribe(f'{STATE_PREFIX}/gw/{gw_lower}/cmd/#')
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/set')
    mqtt.subscribe(f'{STATE_PREFIX}/vio/+/press')

    # ── Start transport ──
    log.info('MQTT', f'Connecting {mqtt_cfg["host"]}:{mqtt_cfg["port"]}...')
    mqtt.start()
    log.info('MQTT', '✅ Connected' if mqtt.wait_connected(timeout=10)
             else 'Connection timeout')

    log.info('LORA', 'Connecting...')
    lora.start()
    for label, info in lora.get_status().items():
        log.info('LORA', f'{label}: {"✅ OK" if info["connected"] else "❌ FAIL"}')

    # HA entities (local broker) — bramka pokazuje SWOJE staty; status w stylu
    # supervisora jest po stronie supervisora (reg_gateway tworzyłby tu puste sieroty)
    ha.reg_gw_buttons_local(gw_id)          # gateway-local ping/discovery buttons
    ha.reg_gw_local_stats(gw_id)            # panel statystyk: link + sync czasu
    for vio in vio_config:
        if vio['type'] == 'switch':
            ha.reg_vswitch(gw_id, vio['id'], vio['name'], vio.get('default', 0))
        else:
            ha.reg_vbutton(gw_id, vio['id'], vio['name'])

    heartbeat.start()

    # Z2M discovery
    time.sleep(3)
    if discovery.devices:
        log.info('DISC', f'✅ {len(discovery.devices)} devices z Z2M')
        threading.Thread(
            target=lambda: discovery.send_discovery_with_delay(tx_delay),
            daemon=True).start()
    else:
        log.warn('DISC', 'Brak Z2M devices — wyślę discovery gdy Z2M dostarczy listę')

    log.info('MAIN', '')
    log.info('MAIN', '═' * 50)
    log.info('MAIN', f'GATEWAY {gw_id} running')
    log.info('MAIN', f'  HB co {heartbeat.interval}s (hash w payload)')
    log.info('MAIN', f'  ping(g={gw_id}) → pong   |   inne g → ignoruję')
    log.info('MAIN', f'  disc → disc_meta + disc_vio + db (delay {tx_delay}s)')
    log.info('MAIN', f'  cmd → st   |   vsw → vsw_st   |   vbtn → vbtn_ack')
    log.info('MAIN', f'  VIO: {len(vio_config)} items')
    log.info('MAIN', '═' * 50)

    def publish_gwstat():
        now = time.time()
        linked = gw_stats['last_sup_rx_ts'] > 0 and (now - gw_stats['last_sup_rx_ts']) < SUP_LINK_TIMEOUT
        mon = sum(1 for d in discovery.devices.values() if d.get('monitored', True))
        last_hb = (datetime.fromtimestamp(heartbeat._last_hb).strftime('%H:%M:%S')
                   if heartbeat._last_hb else '--')
        ha.pub_gw_stats(gw_id, {
            'uptime': int(now - heartbeat.start_time),
            'monitored': mon, 'last_hb': last_hb,
            'sup_link': 'ON' if linked else 'OFF', 'sup_last_rx': gw_stats['last_sup_rx'],
            'time_offset': gw_stats['time_offset'], 'last_sync': gw_stats['last_sync']})

    publish_gwstat()                         # od razu (panel nie świeci 'unknown')

    def gwstat_loop():                       # tylko odświeża panel bramki (lokalny MQTT), zero LoRa
        while running.is_set():
            time.sleep(300)                  # co 5 min (uptime/link) — bez logu STATS
            publish_gwstat()
    threading.Thread(target=gwstat_loop, daemon=True).start()

    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass
    heartbeat.running = False
    lora.stop(); mqtt.stop()
    log.info('MAIN', 'Stopped.')


# ── Supervisor mode ─────────────────────────────────────
def run_supervisor(log):
    running = threading.Event()
    running.set()
    mqtt_cfg = CONFIG['mqtt']

    mqtt = MqttTransport(
        host=mqtt_cfg['host'], port=mqtt_cfg['port'],
        user=mqtt_cfg['user'], password=mqtt_cfg['pass'],
        client_id=f"step2_sup_{int(time.time())}",
        logger=log)

    lora = _make_lora(CONFIG, log)

    ha = HAEntities(mqtt, logger=log)

    known_gateways = CONFIG.get('gateways', ['G1'])

    def send_to_all(msg):
        lora.send(json.dumps(msg, separators=(',', ':')))   # transport logs 📤 TX: {json}

    # auto-discovery: gdy hash z hb/pong ≠ zsynchronizowany → żądaj disc
    sup_disc = SupervisorDiscovery(
        ha, logger=log,
        request_disc_fn=lambda gw: send_to_all({"t": "disc", "g": gw}))

    # sterowanie: cmd → st, z retry+timeout; brak st → urządzenie offline
    ctrl_cfg = CONFIG.get('control', {})
    ctrl_timeout = ctrl_cfg.get('timeout', 15)
    ctrl_retries = ctrl_cfg.get('retries', 2)
    pending_cmd = {}                  # (gw, dev) → {msg, since, retries_left}
    cmd_lock = threading.Lock()

    wd = CONFIG.get('watchdog', {})
    sup_hb = SupervisorHeartbeat(
        ha, logger=log,
        send_ping_fn=lambda gw: send_to_all({"t": "ping", "g": gw}),
        devices_provider=sup_disc.devices,
        gateways=known_gateways,
        passive_timeout=wd.get('passive_timeout', 2100),   # 35 min (~2× 15-min HB)
        ping_timeout=wd.get('ping_timeout', 25),
        max_retries=wd.get('max_retries', 2))

    # ── Dispatcher (LoRa RX from gateways) ──
    dispatcher = Dispatcher(logger=log)

    def on_hb(d):
        gw = d.get('g')
        sup_hb.handle_hb(d)                       # status + diag + watchdog reset
        if gw:
            ha.reg_gw_controls(gw)                # per-gateway ping/disc/sync buttons (once)
        sup_disc.note_hash(gw, d.get('hash', ''))  # auto-disc on hash change
    dispatcher.register('hb', on_hb)
    dispatcher.register('pong', on_hb)
    dispatcher.register('disc_meta', sup_disc.handle_disc_meta)
    dispatcher.register('disc_vio', sup_disc.handle_disc_vio)
    dispatcher.register('db', sup_disc.handle_db)
    dispatcher.register('disc_ack', lambda d: log.info('RX', f'disc_ack: {d}'))

    def handle_st(d):
        gw = d.get('g', '?'); dev = d.get('d'); val = str(d.get('v', '')).upper()
        if dev:
            with cmd_lock:
                pending_cmd.pop((gw, dev), None)          # potwierdzenie sterowania
            ha.pub_device_state(gw, dev, {"state": val, "available": "ON"})
            log.info('CTRL', f'🎛️ st {gw}/{dev} = {val} → odbite w HA (online, cmd OK)')
    dispatcher.register('st', handle_st)

    def handle_vsw_st(d):
        gw = d.get('g', '?'); vid = d.get('id')
        val = 1 if str(d.get('v')) in ('1', 'ON', 'on', 'True') else 0
        gl, safe = gw.lower(), _safe(vid)
        mqtt.publish(f'{STATE_PREFIX}/{gl}/vio/{safe}/state',
                     'ON' if val else 'OFF', retain=True)
        log.info('VIO', f'🔘 VSwitch {gw}/{vid} = {"ON" if val else "OFF"} → odbite w HA')
    dispatcher.register('vsw_st', handle_vsw_st)
    dispatcher.register('vbtn_ack', lambda d: log.info('RX', f'vbtn_ack {d.get("id")}'))
    dispatcher.set_fallback(lambda d: log.debug('RX', f'passthrough t={d.get("t")}'))

    # ── HA → LoRa bridge ──
    def on_mqtt(topic, payload):
        segs = topic.split('/')
        # ── supervisor command buttons (global broadcast + per-gateway) ──
        if topic.startswith(f'{STATE_PREFIX}/supervisor/cmd/'):
            rest = segs[3:]                          # po lora/supervisor/cmd/
            if len(rest) == 1:                       # GLOBAL → broadcast (bez g)
                a = rest[0]
                if a == 'ping_all':
                    log.info('BTN', '🔘 Ping All (na komendę, broadcast)')
                    sup_hb.manual_ping_all(
                        known_gateways,
                        send_broadcast_fn=lambda: send_to_all({"t": "ping"}))
                elif a == 'discovery_all':
                    log.info('BTN', '🔘 Discovery All → broadcast {"t":"disc"}')
                    send_to_all({"t": "disc"})
                elif a == 'sync_all':
                    log.info('BTN', '🔘 Sync All → broadcast czasu')
                    send_to_all({"t": "sync", "sec": int(time.time())})
            elif len(rest) == 2:                     # PER-GATEWAY → targeted (z g)
                gw, a = rest[0].upper(), rest[1]
                if a == 'ping':
                    log.info('BTN', f'🔘 Ping {gw} (na komendę)')
                    sup_hb.manual_ping(gw)
                elif a == 'disc':
                    log.info('BTN', f'🔘 Discovery {gw}')
                    send_to_all({"t": "disc", "g": gw})
                elif a == 'sync':
                    log.info('BTN', f'🔘 Sync {gw}')
                    send_to_all({"t": "sync", "sec": int(time.time()), "g": gw})
            return
        # ── VIO set/press (dashboard → LoRa) ──
        if len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'set':
            gw, vid = segs[1].upper(), segs[3]
            val = 1 if payload.upper() in ('ON', '1') else 0
            log.info('CTRL', f'🎛️ dashboard VSwitch {gw}/{vid} → {"ON" if val else "OFF"}')
            send_to_all({"t": "vsw", "g": gw, "id": vid, "v": val})
        elif len(segs) == 5 and segs[2] == 'vio' and segs[4] == 'press':
            gw, vid = segs[1].upper(), segs[3]
            log.info('CTRL', f'🎛️ dashboard VButton {gw}/{vid} pressed')
            send_to_all({"t": "vbtn", "g": gw, "id": vid})
        # ── device control (dashboard switch → cmd, z retry/timeout) ──
        elif len(segs) == 4 and segs[3] == 'set':
            gw, safe = segs[1].upper(), segs[2]
            dev = sup_disc.find_device(gw, safe) or safe
            val = payload.upper()
            msg = {"t": "cmd", "g": gw, "d": dev, "c": "state", "v": val}
            with cmd_lock:
                pending_cmd[(gw, dev)] = {'msg': msg, 'since': time.time(),
                                         'retries_left': ctrl_retries}
            log.info('CTRL', f'🎛️ dashboard {gw}/{dev} → {val} '
                     f'(czekam na st, timeout {ctrl_timeout}s, retry {ctrl_retries})')
            send_to_all(msg)

    def on_lora(text):
        dispatcher.dispatch_raw(text)                       # transport already logged 📥 RX: {json}

    mqtt._on_message_cb = on_mqtt
    lora.on_receive = on_lora
    mqtt.subscribe(f'{STATE_PREFIX}/supervisor/cmd/#')
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/set')         # device set: lora/g1/test_1/set
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/+/set')       # vio set:    lora/g1/vio/vs_test/set
    mqtt.subscribe(f'{STATE_PREFIX}/+/+/+/press')     # vio press:  lora/g1/vio/vs_test/press

    log.info('MQTT', f'Connecting {mqtt_cfg["host"]}:{mqtt_cfg["port"]}...')
    mqtt.start()
    log.info('MQTT', '✅ Connected' if mqtt.wait_connected(timeout=10)
             else 'Connection timeout')

    log.info('LORA', 'Connecting...')
    lora.start()
    for label, info in lora.get_status().items():
        log.info('LORA', f'{label}: {"✅ OK" if info["connected"] else "❌ FAIL"}')

    ha.reg_supervisor()
    sup_hb.start()           # PASYWNY watchdog (offline po timeout; ping tylko na komendę)

    def initial_discovery():
        time.sleep(8)        # poczekaj aż LoRa wstanie
        log.info('CMD', '⚡ startowe discovery (jednorazowo — poznanie urządzeń)')
        send_to_all({"t": "disc"})
    threading.Thread(target=initial_discovery, daemon=True).start()

    log.info('MAIN', '')
    log.info('MAIN', '═' * 50)
    log.info('MAIN', f'SUPERVISOR running (gateways: {known_gateways})')
    log.info('MAIN', f'  watchdog PASYWNY: offline po {sup_hb.passive_timeout}s bez HB (0 ruchu)')
    log.info('MAIN', f'  ping na komendę: timeout {sup_hb.ping_timeout}s, retry {sup_hb.max_retries}')
    log.info('MAIN', f'  hb/pong → status+diag w HA   |   db → encje urządzeń')
    log.info('MAIN', f'  switch → cmd → st (retry {ctrl_retries}, timeout {ctrl_timeout}s → offline)')
    log.info('MAIN', f'  globalne: Ping All / Discovery All / Sync All (broadcast)')
    log.info('MAIN', f'  per-bramka: Ping / Discovery / Sync (po pierwszym hb)')
    log.info('MAIN', f'  auto-discovery przy zmianie hash')
    log.info('MAIN', '═' * 50)

    def control_loop():
        """cmd → st watchdog: brak potwierdzenia → retry, po wyczerpaniu → device OFFLINE."""
        while running.is_set():
            time.sleep(3)
            sup_disc.check_complete(max_try=4)   # re-request disc aż lista pełna (cap 4)
            now = time.time()
            with cmd_lock:
                items = list(pending_cmd.items())
            for (gw, dev), p in items:
                if now - p['since'] < ctrl_timeout:
                    continue
                if p['retries_left'] > 0:
                    p['retries_left'] -= 1
                    p['since'] = now
                    log.warn('CTRL', f'🔁 brak st {gw}/{dev} — retry cmd '
                             f'(zostało {p["retries_left"]})')
                    send_to_all(p['msg'])
                else:
                    with cmd_lock:
                        pending_cmd.pop((gw, dev), None)
                    ha.pub_device_avail(gw, dev, False)
                    log.warn('CTRL', f'💀 {gw}/{dev} brak st po {ctrl_retries + 1} '
                             f'próbach → urządzenie OFFLINE')
    threading.Thread(target=control_loop, daemon=True, name='control').start()

    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass
    sup_hb.running = False
    lora.stop(); mqtt.stop()
    log.info('MAIN', 'Stopped.')


# ── Main ────────────────────────────────────────────────
def main():
    log = Log()
    log.info('MAIN', '═' * 50)
    log.info('MAIN', f'STEP 2 PROTOCOL TEST')
    log.info('MAIN', f'Role: {ROLE.upper()} ({CONFIG["id"]})')
    log.info('MAIN', '═' * 50)

    if ROLE == 'gateway':
        run_gateway(log)
    else:
        run_supervisor(log)
    log.close()


if __name__ == '__main__':
    main()
