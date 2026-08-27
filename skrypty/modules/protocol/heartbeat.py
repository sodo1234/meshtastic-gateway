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
                 interval=300, start_time=None, diag_fn=None, jitter=None):
        """
        gw_id:       gateway ID (e.g. "G1")
        devices_fn:  callable() → dict of devices {name: {type, caps}}
        lora:        LoraTransport instance (for send)
        interval:    seconds between heartbeats
        jitter:      +/- s losowego rozsuniecia (None = 5% interval); 0 = wylaczone
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
        # JITTER HB (2026-08-24, zmierzona kolizja): przy stalym interwale dwie bramki, ktore raz
        # trafia w te sama sekunde, koliduja JUZ ZAWSZE - HB swiadomie omija slotowanie (to
        # liveness P0), wiec nic ich nie rozsuwa. Live 17:16:08 i 17:31:08: HB z G1 i G2 w tej
        # samej sekundzie, dokladnie co 900 s, oba razy HB G1 przepadl.
        # Losowe rozsuniecie +/-5% lamie zbieznosc faz bez ruszania architektury (HB zostaje poza
        # slotem) i bez wplywu na wykrywanie offline (timeout to ~2x interwal).
        self.jitter = max(0.0, float(jitter if jitter is not None else 0.05 * interval))
        self._gap = self._next_gap()
        self.running = True

    def _next_gap(self):
        import random
        return self.interval + random.uniform(-self.jitter, self.jitter)

    def build_payload(self, pkt_type='hb'):
        # devices_fn iteruje po slowniku, ktory discovery aktualizuje w INNYM watku —
        # `dictionary changed size during iteration` jest tu realne. Bez tej bariery wyjatek
        # leci wyzej i (patrz `_loop`) kasuje CALY heartbeat, nie jedna ramke.
        try:
            devs = self.devices_fn()
            monitored = [d for d, info in devs.items() if info.get('monitored', True)]
            priority = [d for d, info in devs.items() if info.get('priority', False)]
            self._last_counts = (len(devs), len(monitored), len(priority))
        except Exception as e:
            if self.log:
                self.log.warn('HB', f'devices_fn padlo ({e}) — licznik z poprzedniego cyklu')
            devs, monitored, priority = (), (), ()
        payload = {
            't': pkt_type,
            'g': self.gw_id,
            'up': int(time.time() - self.start_time),
            'dev': len(devs) or getattr(self, '_last_counts', (0, 0, 0))[0],
            'mon': len(monitored) or getattr(self, '_last_counts', (0, 0, 0))[1],
            'pri': len(priority) or getattr(self, '_last_counts', (0, 0, 0))[2],
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
        self._safe_hb()
        while self.running:
            time.sleep(10)
            if time.time() - self._last_hb >= self._gap:
                self._safe_hb()
                self._gap = self._next_gap()      # kazdy cykl inny -> brak zbieznosci faz

    def _safe_hb(self):
        """Jeden nieudany heartbeat NIE MOZE skasowac wszystkich nastepnych.

        REGRESJA 2026-08-26 (G1): ostatni HB o 19:48:10, potem **9 h ciszy** przy calkowicie
        zywej bramce — `b`, bloby `avail`, `an_d` i `sup_ping` szly caly czas. Petla nie miala
        zadnego `try`, wiec pojedynczy wyjatek (najpewniej z `devices_fn`, ktore iteruje po
        slowniku aktualizowanym przez discovery w innym watku) zabil watek NA STALE.
        Objaw byl podstepny: supervisor dalej widzial bramke ONLINE, bo `note_gw_activity`
        reaguje na DOWOLNA ramke — zamarzly tylko hashe, liczniki i jakosc czasu.
        To ta sama rodzina co bugi z 2026-08-24 (petle, ktore po cichu przestaja isc).
        Znaczymy `_last_hb` TAKZE przy bledzie: inaczej po awarii lecielibysmy w kolko
        co 10 s, zamieniajac cisze w zalew."""
        try:
            self.send_heartbeat()
        except Exception as e:
            self._last_hb = time.time()
            if self.log:
                self.log.error('HB', f'heartbeat nieudany ({e}) — nastepny za ~{int(self._gap)}s')


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
            # KASKADA TYLKO W DÓŁ (2026-08-26). Wcześniej powrót bramki hurtem ustawiał
            # WSZYSTKIM jej urządzeniom `available=ON` — i to była główna przyczyna tego, że
            # supervisor w dzień pokazywał 78/78 przekaźników G1 jako dostępne, choć z2m na
            # bramce mówił `offline` (carport ma zasilanie tylko nocą). Kaskada w dół jest
            # uprawniona (bramka milczy ⇒ NIE WIEMY nic o urządzeniach), w górę NIE JEST:
            # „bramka żyje" nie znaczy „urządzenia żyją". Prawdę o dostępności dowozi blob
            # `avail` z pełnego sweepu — bramka jest źródłem prawdy, nie domysł supervisora.
            if self.log:
                self.log.info('HB', f'💚 {gw} ONLINE ponownie (dostępność urządzeń — z blobu bramki)')
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

    def note_seen(self, gw):
        """DOWOLNA ramka od bramki (b / rt_* / an_d / st / …) = dowód życia: odśwież `_ts` i ANULUJ
        trwającą sondę ping.

        BUG NAPRAWIONY 2026-07-30: sonda ping żyła własnym życiem — nie patrzyła na to, czy bramkę
        słychać. Efekt zmierzony live: `RX b [G1]` o 20:05:29 → `💀 G1 OFFLINE — brak pong po 3
        próbach` o 20:05:38 → **194 urządzenia kaskadą na offline**, mimo że bramka właśnie nadawała.
        Powód: sup→gw to słabszy kierunek tego łącza (RTT 16-72 s pod obciążeniem), więc to PONG
        ginie, a nie bramka pada. Anulowanie sondy przy dowolnym ruchu kasuje fałszywy alarm
        i oszczędza airtime na zbędnych retry-pingach."""
        if not gw:
            return
        g = self.gateways.setdefault(gw, {})
        g['_ts'] = time.time()
        g['last_seen'] = datetime.now().strftime('%H:%M:%S')
        self._probe.pop(gw, None)

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
                    # DRUGA LINIA OBRONY (2026-07-30): brak pong ≠ martwa bramka. Jeśli słyszeliśmy
                    # od niej COKOLWIEK w oknie pasywnym, to zginął pong (kierunek sup→gw), a nie
                    # bramka. Ogłoszenie offline kaskadowałoby setki urządzeń na oczach klienta.
                    heard = time.time() - self.gateways.get(gw, {}).get('_ts', 0)
                    if heard < self.passive_timeout:
                        if self.log:
                            self.log.warn('HB', f'⚠️ {gw}: brak pong (ping), ale słyszana '
                                                f'{heard:.0f}s temu → NIE oznaczam offline')
                    else:
                        self._mark_offline(gw, f'brak pong po {self.max_retries + 1} próbach (ping)')

    # legacy alias (older callers)
    def check_timeouts(self):
        pass
