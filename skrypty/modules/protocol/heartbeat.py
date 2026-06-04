"""Heartbeat / Ping / Pong — gateway ↔ supervisor keep-alive.

Extracted from gateway_v38.py:1489-1551 + 2362-2366.
Gateway sends periodic HB, responds to ping with pong.
Supervisor processes incoming HB and updates gateway status.
"""
import json
import time
import threading
from datetime import datetime


class GatewayHeartbeat:
    """Gateway side: sends HB, responds to ping with pong."""

    def __init__(self, gw_id, devices_fn, lora, logger=None,
                 interval=300, start_time=None, diag_fn=None):
        """
        gw_id:       gateway ID (e.g. "G1")
        devices_fn:  callable() → dict of devices {name: {type, caps}}
        lora:        LoraTransport instance (for send)
        interval:    seconds between heartbeats
        diag_fn:     optional callable() → dict of extra diagnostics merged
                     into HB/pong payload (e.g. {'z2m': N, 'hash': H})
        """
        self.gw_id = gw_id
        self.devices_fn = devices_fn
        self.lora = lora
        self.log = logger
        self.interval = interval
        self.start_time = start_time or time.time()
        self.diag_fn = diag_fn
        self._last_hb = 0
        self.running = True

    def build_payload(self, pkt_type='hb'):
        devs = self.devices_fn()
        monitored = [d for d, info in devs.items()
                     if info.get('monitored', True)]
        priority = [d for d, info in devs.items()
                    if info.get('priority', False)]
        payload = {
            't': pkt_type,
            'g': self.gw_id,
            'up': int(time.time() - self.start_time),
            'dev': len(devs),
            'mon': len(monitored),
            'pri': len(priority),
            'ts': int(time.time()),
        }
        if self.diag_fn:
            try:
                payload.update(self.diag_fn() or {})
            except Exception:
                pass
        return payload

    def send_heartbeat(self):
        msg = self.build_payload('hb')
        text = json.dumps(msg, separators=(',', ':'))
        self.lora.send(text)
        self._last_hb = time.time()
        if self.log:
            self.log.info('HB', f'💓 HB: dev={msg["dev"]} mon={msg["mon"]} pri={msg["pri"]}')

    def handle_ping(self, data=None):
        # Only answer pings addressed to this gateway (or broadcast w/o 'g').
        if data and data.get('g') and data.get('g') != self.gw_id:
            if self.log:
                self.log.debug('HB', f'↪️ ping dla {data.get("g")} — nie moja, ignoruję')
            return
        msg = self.build_payload('pong')
        text = json.dumps(msg, separators=(',', ':'))
        self.lora.send(text)
        if self.log:
            self.log.info('HB', f'🏓 PONG: dev={msg["dev"]} mon={msg["mon"]} pri={msg["pri"]}')

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name='heartbeat').start()

    def _loop(self):
        time.sleep(5)
        self.send_heartbeat()
        while self.running:
            time.sleep(10)
            if time.time() - self._last_hb >= self.interval:
                self.send_heartbeat()


class SupervisorHeartbeat:
    """Supervisor side: PASSIVE liveness + on-demand ping. Minimal LoRa traffic.

    Liveness:
      1. Passive (default) — gateway HB (every ~15 min) refreshes online. If NO
         hb/pong for `passive_timeout`, the gateway + its devices go offline.
         Sends ZERO LoRa traffic by itself.
      2. On-demand ping — only when the dashboard Ping button fires manual_ping().
         Sends a ping, and if no pong within `ping_timeout`, retries up to
         `max_retries`; after the final attempt the gateway + devices go offline.

    No periodic active probing → drastically fewer packets → fewer collisions.
    Marking offline ⇒ gateway status → offline, every device → available=OFF.
    """

    def __init__(self, ha_entities, logger=None, passive_timeout=2100,
                 send_ping_fn=None, devices_provider=None, gateways=None,
                 ping_timeout=25, max_retries=2, tick=5):
        """
        passive_timeout:  seconds without hb/pong → offline (default 2100 = 35 min,
                          ~2× the 15-min HB interval + grace). No LoRa sent.
        send_ping_fn:     callable(gw) → sends a ping for gw over LoRa (manual only)
        devices_provider: callable(gw) → {dev_name: info} for offline cascade
        gateways:         iterable of known gateway ids
        ping_timeout:     manual-ping: wait for pong before retry / declaring dead
        max_retries:      manual-ping: extra attempts after the first (2 ⇒ 3 total)
        tick:             watchdog loop granularity (s)
        """
        self.ha = ha_entities
        self.log = logger
        self.passive_timeout = passive_timeout
        self.send_ping_fn = send_ping_fn
        self.devices_provider = devices_provider
        self.ping_timeout = ping_timeout
        self.max_retries = max_retries
        self.tick = tick
        self.gateways = {}              # {gw: {status fields..., online}}
        self._known = list(gateways or [])
        self._probe = {}               # {gw: {since, retries_left}} — ONLY while a manual ping is active
        self.running = True

    # ── inbound hb/pong ─────────────────────────────────
    def handle_hb(self, data):
        gw = data.get('g', '?')
        was_offline = self.gateways.get(gw, {}).get('online') is False
        self.gateways[gw] = {
            'last_seen': datetime.now().strftime('%H:%M:%S'),
            '_ts': time.time(),
            'uptime': data.get('up', 0),
            'devices_total': data.get('dev', 0),
            'devices_monitored': data.get('mon', 0),
            'devices_priority': data.get('pri', 0),
            'hash': data.get('hash', ''),
            'online': True,
        }
        self._probe.pop(gw, None)        # any in-flight manual ping is answered
        if gw not in self._known:
            self._known.append(gw)
        self.ha.reg_gateway(gw)
        self._publish_status(gw)
        if was_offline:
            self._cascade_devices(gw, online=True)
            if self.log:
                self.log.info('HB', f'💚 {gw} ONLINE ponownie')
        pkt = data.get('t', 'hb')
        icon = '💓' if pkt == 'hb' else '🏓'
        if self.log:
            self.log.info('HB', f'{icon} {pkt.upper()} from {gw}: '
                          f'dev={data.get("dev")} mon={data.get("mon")} '
                          f'pri={data.get("pri")} hash={data.get("hash","")} '
                          f'up={data.get("up")}s')

    def handle_pong(self, data):
        self.handle_hb(data)

    # ── status publishing ───────────────────────────────
    def _publish_status(self, gw):
        g = self.gateways.get(gw, {})
        online = g.get('online', False)
        total = g.get('devices_total', 0)
        self.ha.pub_gw_status(gw, {
            'state': 'online' if online else 'offline',
            'uptime': g.get('uptime', 0),
            'last_seen': g.get('last_seen', '--'),
            'devices_total': total,
            'devices_monitored': g.get('devices_monitored', 0),
            'devices_priority': g.get('devices_priority', 0),
            'devices_offline': 0 if online else total,
            'hash': g.get('hash', ''),
        })

    def _cascade_devices(self, gw, online):
        if not self.devices_provider:
            return
        devs = self.devices_provider(gw) or {}
        for name in devs:
            try:
                self.ha.pub_device_avail(gw, name, online)
            except Exception:
                pass
        if devs and self.log:
            state = 'ONLINE' if online else 'OFFLINE'
            self.log.warn('HB', f'   ↳ {len(devs)} urządzeń {gw} → {state}')

    # ── on-demand ping (dashboard command only) ─────────
    def _send_ping(self, gw, attempt):
        if self.send_ping_fn:
            self.send_ping_fn(gw)
        if self.log:
            self.log.info('HB', f'📡 ping → {gw} (próba {attempt}/{self.max_retries + 1})')

    def manual_ping(self, gw):
        """Arm an on-demand ping with retry→offline for one gateway (Ping button)."""
        if gw not in self._known:
            self._known.append(gw)
        self._probe[gw] = {'since': time.time(), 'retries_left': self.max_retries}
        self._send_ping(gw, 1)

    def manual_ping_all(self, gateways, send_broadcast_fn=None):
        """Ping All: one broadcast packet + arm probes for every known gateway."""
        if send_broadcast_fn:
            send_broadcast_fn()                       # single {"t":"ping"} for all
        for gw in gateways:
            if gw not in self._known:
                self._known.append(gw)
            self._probe[gw] = {'since': time.time(), 'retries_left': self.max_retries,
                               'no_send': True}        # broadcast already sent
            if self.log:
                self.log.info('HB', f'📡 ping(all)→ {gw} (próba 1/{self.max_retries + 1})')

    def _mark_offline(self, gw, reason):
        g = self.gateways.setdefault(gw, {})
        if g.get('online') is False:
            return
        g['online'] = False
        self._publish_status(gw)
        if self.log:
            self.log.warn('HB', f'💀 {gw} OFFLINE — {reason}')
        self._cascade_devices(gw, online=False)

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name='hb-watchdog').start()

    def _loop(self):
        while self.running:
            time.sleep(self.tick)
            now = time.time()
            # 1) PASSIVE: no hb/pong for passive_timeout → offline (no LoRa sent)
            for gw, g in list(self.gateways.items()):
                if g.get('online') and now - g.get('_ts', 0) > self.passive_timeout:
                    self._mark_offline(gw, f'brak HB > {self.passive_timeout}s (pasywny)')
            # 2) ON-DEMAND: only gateways with an active manual ping
            for gw in list(self._probe):
                p = self._probe[gw]
                if now - p['since'] < self.ping_timeout:
                    continue
                if p['retries_left'] > 0:
                    p['retries_left'] -= 1
                    p['since'] = now
                    self._send_ping(gw, self.max_retries + 1 - p['retries_left'])
                else:
                    self._probe.pop(gw, None)
                    self._mark_offline(gw, f'brak pong po {self.max_retries + 1} próbach (ping)')

    # legacy alias (older callers)
    def check_timeouts(self):
        pass
