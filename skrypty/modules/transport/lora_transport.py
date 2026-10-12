"""LoraTransport — Meshtastic SerialInterface manager with hot-reconnect.

Extracted from supervisor_v38.py:777-996 (AntennaManager). Changes:
  - CONFIG injected via constructor (no global CONFIG access)
  - on_receive(text) callback injected (wired to Dispatcher.dispatch_raw in production)
  - Logger injected (no module-level `log`)

Features (preserved from v38):
  - Dedicated TX thread with per-send timeout — main loop never blocks on iface.sendText()
  - Multiple antenna support with per-gateway routing (gw_routes), fallback broadcast
  - Force-release stale pyserial lockfiles and fds (handles USB-Ethernet disconnect cycles)
  - Reconnect loop with exponential backoff (capped)
  - RX deduplication (md5 over last 200 messages)
"""
import glob
import hashlib
import json
import os
import threading
import time
from collections import deque

from .codec import decode, encode, is_short_frame, long_type
from .fastlane import (DEFAULT_IMMEDIATE_TYPES, immediate_types,
                       is_immediate_frame, lbt_wait_or_requeue)

# meshtastic + pubsub imported lazily in start() — module loadable without hardware deps


class LoraTransport:
    def __init__(self, ports_cfg, reconnect_cfg=None, on_receive=None, logger=None,
                 tx_timeout=8, dedup_seconds=6, tx_cooldown=2.0, tx_hard_timeout=90,
                 immediate=None, lbt_quiet=2.0, lbt_max_wait=6.0,
                 codec_role='gateway', codec_short_tx=False, lbt_hard_cap=20.0):
        """
        ports_cfg:     list of dicts: [{"port": "/dev/...", "enabled": True,
                                        "label": "ANT-1", "gateways": ["G1"]}, ...]
        reconnect_cfg: {"enabled": True, "interval": 15, "max_backoff": 120}
        on_receive:    callable(text:str) — typically dispatcher.dispatch_raw
        dedup_seconds: drop a byte-identical RX only if seen within this window.
                       TIME-based (not count-based): catches Meshtastic's RF-layer
                       duplicate deliveries (arrive within ~1-3s) WITHOUT swallowing
                       legitimate repeats like an identical periodic ping/disc_meta.
        """
        self.ports_cfg = ports_cfg or []
        self.reconnect_cfg = reconnect_cfg or {"enabled": True, "interval": 15, "max_backoff": 120}
        self.on_receive = on_receive
        self.log = logger
        self.tx_timeout = tx_timeout
        self.tx_hard_timeout = tx_hard_timeout
        self.dedup_seconds = dedup_seconds
        # F3 fix: minimalny odstęp między pakietami LoRa (duty cycle / cooldown radia). Bez tego
        # wątek TX słał back-to-back → burst komend (klik-klik) przepełniał kolejkę Meshtastic i
        # pakiety ginęły. Dławi TYLKO bursty; ruch już rozstawiony (transfery czekają na cack) nie
        # dostaje dodatkowego opóźnienia bo odstęp od ostatniego TX i tak > cooldown.
        self.slot_gate = None            # callable → sekundy do okna slotu (0 = wolno nadawac); ustawia SCADA
        self.tx_cooldown = tx_cooldown
        self._last_tx = 0.0
        self.immediate_types = immediate_types(
            DEFAULT_IMMEDIATE_TYPES if immediate is None else immediate)
        self.lbt_quiet = max(0.0, float(lbt_quiet))
        self.lbt_max_wait = max(0.0, float(lbt_max_wait))
        self.lbt_hard_cap = max(0.0, float(lbt_hard_cap))
        self._last_rx_time = 0.0          # monotonic; LBT, aktualizowany dla KAZDEJ ramki RX
        self.codec_role = str(codec_role or 'gateway').lower()
        self.codec_short_tx = bool(codec_short_tx)
        self._short_peers = {}            # {frame['g']: True}; pamiec procesu, bez persistencji
        self._cap_lock = threading.Lock()
        # ARBITER (2026-07-06): podczas transferu-pliku (arbiter.busy) ODŁÓŻ ruch nie-transferowy
        # (b/ab/disc_vio/disc_meta/hb) — inaczej bramka nadaje i przez half-duplex NIE SŁYSZY cacka
        # → transfer pada → mapa niekompletna → sterowanie. Transfery+odpowiedzi przechodzą od razu.
        self.arbiter = None

        self.interfaces = {}              # {label: SerialInterface}
        self.lock = threading.Lock()
        self._seen_msgs = {}              # {md5: last_seen_ts} — time-windowed dedup
        self._reconnect_backoff = {}
        self.gw_routes = {}               # {"G1": "ANT-1", ...}
        self._tx_queue = deque()
        self._tx_thread = None
        self.running = True
        self._last_rx_ts = time.time()
        # AUTO-RECOVERY USB: zwis CP2102 / extendera (Unitek) → programowy „replug" (USBDEVFS_RESET,
        # uprawnienia z udev/plugdev) zamiast ręcznego odpięcia-wpięcia + restartu skryptu.
        rc = self.reconnect_cfg
        self.usb_reset_enabled = rc.get('usb_reset', True)
        self.usb_reset_after = rc.get('usb_reset_after', 2)   # po N nieudanych reconnectach → reset USB
        self.rx_timeout = rc.get('rx_timeout', 0)             # 0=off; >0 = brak RX > Ns gdy connected ⇒ wedge → reset
        # ANTY-ZAMROŻENIE PĘTLI (2026-08-24, root-cause 10 dni ciszy G1): SerialInterface() oraz
        # iface.close() na zwisniętym porcie potrafią NIE WRÓCIĆ NIGDY. Wołane wprost z pętli TX
        # i pętli reconnectu zamrażały je na stałe: bramka żyła jako proces, ale nie nadała ani
        # nie odebrała nic, a reconnect wykonał JEDNĄ próbę przez 10 dni. Odtąd oba wywołania idą
        # przez wątek-strażnik z limitem czasu — pętle wracają do pracy nawet gdy port wisi.
        self.connect_timeout = rc.get('connect_timeout', 45)  # s na SerialInterface()/TCPInterface()
        self._connect_inflight = {}                           # {label: bool} — maks. 1 próba w locie
        self._tx_blocked = {}                                 # {label: bool} — sendText() wciąż wisi

    # ── public API ──
    @staticmethod
    def _is_net(acfg):
        """Czy antena idzie po SIECI (TCP) zamiast USB — wybór UNIWERSALNY, per-antena:
          - jawnie:  "transport": "tcp"   (albo "usb")
          - albo ze schematu portu: "port": "tcp://host[:port]"
          - USB (domyślnie): "port": "/dev/serial/by-id/..."
        Rozwiązuje pad 30 m USB-po-skrętce (Unitek) → [[reference-meshtastic-launcher-antenna]].
        Endpoint TCP: Heltec na WiFi (Meshtastic TCP API, domyślnie 4403)."""
        tr = str(acfg.get('transport', '')).lower()
        if tr == 'tcp':
            return True
        if tr == 'usb':
            return False
        p = acfg.get('port', '')
        return isinstance(p, str) and p.startswith('tcp://')

    @staticmethod
    def _tcp_host_port(acfg):
        """(host, port) dla anteny TCP. Źródło: host/tcp_port jawnie, albo z 'tcp://host:port'."""
        p = acfg.get('port', '') or ''
        rest = p[len('tcp://'):] if p.startswith('tcp://') else p
        host = (acfg.get('host') or rest.split(':')[0]).strip()
        if acfg.get('tcp_port'):
            tcp_port = int(acfg['tcp_port'])
        elif ':' in rest:
            tcp_port = int(rest.split(':', 1)[1])
        else:
            tcp_port = 4403
        return host, tcp_port

    def start(self):
        """Subscribe to meshtastic RX, connect all enabled antennas, spawn TX + reconnect loops."""
        import meshtastic.serial_interface  # noqa: F401  — needed by _connect_one via module-level ref below
        from pubsub import pub
        self._meshtastic = meshtastic.serial_interface
        try:
            import meshtastic.tcp_interface
            self._meshtastic_tcp = meshtastic.tcp_interface   # antena po IP (tcp://)
        except Exception:
            self._meshtastic_tcp = None
        pub.subscribe(self._mesh_rx, "meshtastic.receive.text")
        for acfg in self.ports_cfg:
            if acfg.get('enabled', False):
                self._connect_one(acfg)
                for gw in acfg.get('gateways', []):
                    self.gw_routes[gw] = acfg['label']
        n = len(self.interfaces)
        if self.log:
            self.log.info('ANT', f"📡 {n} antenna(s) connected, routes: {self.gw_routes}")
            if n == 0:
                self.log.warn('ANT', "⚠️ No antennas connected! Will retry...")
        self._tx_thread = threading.Thread(target=self._tx_loop, daemon=True, name="lora-tx")
        self._tx_thread.start()
        if self.reconnect_cfg.get('enabled', True):
            threading.Thread(target=self._reconnect_loop, daemon=True, name="lora-reconnect").start()

    def _wire_text(self, frame):
        """Serialize one frame in negotiated long or abbreviated wire form.

        String input remains supported for existing callers, but is parsed back
        to a dict here so every LoRa path crosses the same codec boundary.
        """
        if isinstance(frame, str):
            data = json.loads(frame)
        else:
            data = frame
        if not isinstance(data, dict):
            raise TypeError("LoRa frame must be a JSON object")
        data = decode(data)               # short input cannot bypass negotiation
        peer = data.get('g')
        use_short = False
        if peer:
            with self._cap_lock:
                peer_short = bool(self._short_peers.get(peer))
            if self.codec_role == 'supervisor':
                use_short = peer_short
            else:
                use_short = self.codec_short_tx or peer_short
        wire = encode(data) if use_short else data
        return json.dumps(wire, separators=(',', ':'))

    def peer_supports_short(self, peer):
        """Expose the in-memory negotiation result for diagnostics/tests."""
        with self._cap_lock:
            return bool(peer and self._short_peers.get(peer))

    def _enqueue(self, job):
        """Fast-lane jobs lead the queue; an in-flight send remains untouched."""
        if is_immediate_frame(job.get('text'), self.immediate_types):
            self._tx_queue.appendleft(job)
        else:
            self._tx_queue.append(job)

    def send(self, frame):
        """Broadcast via ALL antennas. Non-blocking (enqueues to TX thread)."""
        text = self._wire_text(frame)
        # F2 guard: LoRa/Meshtastic max ~220B/pakiet. Większy = odrzucony/ucięty (cicha awaria).
        # Ostrzegamy GŁOŚNO — łapie bugi typu clear-all enumerujący setki urządzeń w 1 pakiecie.
        n = len(text.encode('utf-8')) if isinstance(text, str) else len(text)
        if n > 220 and self.log:
            self.log.warn('TX', f"⚠️ OVERSIZE {n}B > 220B — Meshtastic ODRZUCI/UTNIE (pakiet nie chunkowany!): {text[:70]}…")
        if self.log: self.log.info('TX', f"📤 TX: {text}")
        evt = threading.Event()          # ustawiany przez _tx_loop, gdy ramka FAKTYCZNIE poszla do radia
        self._enqueue({'text': text, 'label': None, 'evt': evt})
        return evt

    def send_to(self, gw, frame):
        """Unicast via antenna routed to `gw`. Falls back to broadcast if no route."""
        text = self._wire_text(frame)
        if self.log: self.log.info('TX', f"📤 TX→{gw}: {text}")
        label = self.gw_routes.get(gw)
        if label:
            with self.lock:
                has_iface = label in self.interfaces
            if has_iface:
                self._enqueue({'text': text, 'label': label})
                if self.log: self.log.debug('ANT', f"📡 Unicast TX queued via {label} → {gw}")
                return
        if self.log: self.log.warn('ANT', f"⚠️ No route for {gw}, fallback broadcast")
        self._enqueue({'text': text, 'label': None})

    def get_status(self):
        out = {}
        with self.lock:
            for acfg in self.ports_cfg:
                label = acfg['label']
                _p = acfg.get('port') or ("tcp://%s:%d" % self._tcp_host_port(acfg) if self._is_net(acfg) else '')
                out[label] = {
                    "port": _p,
                    "enabled": acfg.get('enabled', False),
                    "connected": label in self.interfaces,
                }
        return out

    def stop(self):
        self.running = False
        with self.lock:
            for label, iface in list(self.interfaces.items()):
                try: iface.close()
                except Exception: pass
            self.interfaces.clear()

    # ── connection management ──
    def _connect_one(self, acfg):
        """Podłącz antenę BEZ ryzyka zamrożenia wołającego (patrz connect_timeout w __init__).
        Rejestracja interfejsu dzieje się WEWNĄTRZ wątku-strażnika, więc spóźniony sukces (po
        przekroczeniu limitu) i tak trafia do self.interfaces zamiast osierocić port."""
        label = acfg['label']; port = acfg.get('port', '')
        if self._connect_inflight.get(label):
            return False                                    # próba już trwa — nie mnóż wątków
        self._connect_inflight[label] = True
        box = {}

        def _mk():
            shown = port
            try:
                if self._is_net(acfg):                      # antena po sieci (serial-over-IP)
                    if not self._meshtastic_tcp:
                        raise RuntimeError("meshtastic.tcp_interface niedostępny")
                    host, tcp_port = self._tcp_host_port(acfg)
                    iface = self._meshtastic_tcp.TCPInterface(hostname=host, portNumber=tcp_port)
                    shown = f"tcp://{host}:{tcp_port}"       # do logów/statusu
                else:
                    iface = self._meshtastic.SerialInterface(devPath=port)
                with self.lock: self.interfaces[label] = iface
                self._reconnect_backoff[label] = 0
                self._last_rx_ts = time.time()              # świeże okno RX-watchdog po (re)connect
                box['ok'] = True
                if self.log: self.log.info('ANT', f"✅ {label} ({shown}) connected")
            except Exception as e:
                if self.log: self.log.error('ANT', f"❌ {label} ({shown}): {e}")
            finally:
                self._connect_inflight[label] = False

        t = threading.Thread(target=_mk, daemon=True, name=f"ant-connect-{label}")
        t.start(); t.join(self.connect_timeout)
        if t.is_alive():
            if self.log:
                self.log.error('ANT', f"⏱️ {label}: otwarcie portu nie wróciło w {self.connect_timeout}s "
                                      f"— pętla reconnectu pracuje dalej (próba dokończy się w tle)")
            return False
        return bool(box.get('ok'))

    def _close_bg(self, label, iface):
        """close() zwisniętego interfejsu w tle. NIGDY nie wołaj close() wprost z pętli TX —
        na zapchanym porcie potrafi nie wrócić i zamraża nadawanie na stałe (live G1 2026-08-13)."""
        def _do():
            try: iface.close()
            except Exception: pass
        threading.Thread(target=_do, daemon=True, name=f"ant-close-{label}").start()

    def _force_release_port(self, port):
        """Close stale pyserial fds + lockfile after USB-Ethernet disconnect cycles."""
        try:
            pid = os.getpid()
            for fd_link in glob.glob(f'/proc/{pid}/fd/*'):
                try:
                    target = os.readlink(fd_link)
                    if port in target:
                        fd_num = int(os.path.basename(fd_link))
                        os.close(fd_num)
                        if self.log: self.log.info('ANT', f"🔓 Closed stale fd {fd_num} → {port}")
                except Exception: pass
        except Exception: pass
        try:
            lockfile = f'/tmp/pyserial.{port.replace("/", "_")}.lock'
            if os.path.exists(lockfile):
                os.unlink(lockfile)
                if self.log: self.log.info('ANT', f"🔓 Removed lockfile {lockfile}")
        except Exception: pass
        time.sleep(1)

    def _resolve_usb_devnode(self, port):
        """Serial port (/dev/ttyUSBx lub by-id) → ścieżka usbfs /dev/bus/usb/BBB/DDD (do USBDEVFS_RESET).
        Idzie po sysfs w górę aż znajdzie busnum/devnum urządzenia USB."""
        try:
            real = os.path.realpath(port)                  # by-id → /dev/ttyUSBx
            name = os.path.basename(real)
            dev = os.path.realpath(f'/sys/class/tty/{name}/device')
            for _ in range(5):                             # ttyUSB→iface→urządzenie USB (parent z busnum)
                bn, dn = os.path.join(dev, 'busnum'), os.path.join(dev, 'devnum')
                if os.path.exists(bn) and os.path.exists(dn):
                    return '/dev/bus/usb/%03d/%03d' % (int(open(bn).read()), int(open(dn).read()))
                parent = os.path.dirname(dev)
                if parent == dev:
                    break
                dev = parent
        except Exception:
            pass
        return None

    def _usb_reset(self, port):
        """Programowy „replug": USBDEVFS_RESET na urządzeniu USB portu. Czyści zwis CP2102/extendera
        bez fizycznego odpinania. Uprawnienia: udev daje grupie plugdev zapis do usbfs (99-meshtastic-cp2102)."""
        node = self._resolve_usb_devnode(port)
        if not node or not os.path.exists(node):
            if self.log: self.log.warn('ANT', f"⚠️ USB reset: brak usbfs dla {port} (urządzenie znikło z magistrali?)")
            return False
        try:
            import fcntl
            USBDEVFS_RESET = ord('U') << 8 | 20            # _IO('U', 20)
            fd = os.open(node, os.O_WRONLY)
            try:
                fcntl.ioctl(fd, USBDEVFS_RESET, 0)
            finally:
                os.close(fd)
            if self.log: self.log.info('ANT', f"🔌 USB reset (programowy replug) {node} — czyszczę zwis anteny")
            time.sleep(2)                                  # re-enumeracja
            return True
        except PermissionError:
            if self.log: self.log.error('ANT', f"❌ USB reset {node}: brak uprawnień (udev rule + grupa plugdev?)")
            return False
        except Exception as e:
            if self.log: self.log.error('ANT', f"❌ USB reset {node}: {e}")
            return False

    def _reconnect_loop(self):
        while self.running:
            time.sleep(5)
            now = time.time()
            for acfg in self.ports_cfg:
                if not acfg.get('enabled', False): continue
                label, port = acfg['label'], acfg.get('port', '')
                is_net = self._is_net(acfg)
                with self.lock: connected = label in self.interfaces
                if connected:
                    # ZWIS: health-check rzuca wyjątek LUB brak RX > rx_timeout (gdy connected).
                    wedged = (not is_net and self.rx_timeout > 0
                              and (now - self._last_rx_ts) > self.rx_timeout)
                    try:
                        iface = self.interfaces.get(label)
                        if iface and hasattr(iface, 'localNode'):
                            _ = iface.localNode
                    except Exception:
                        wedged = True
                    if wedged:
                        if self.log: self.log.warn('ANT', f"⚠️ {label} ZWIS (unhealthy / brak RX>{self.rx_timeout}s) — force-close + reset USB")
                        with self.lock: old = self.interfaces.pop(label, None)
                        if old:
                            self._close_bg(label, old)      # close() potrafi zawisnąć — w tle
                        if not is_net:
                            self._force_release_port(port)
                            if self.usb_reset_enabled:
                                self._usb_reset(port)        # programowy „replug" od razu przy zwisie
                else:
                    backoff = self._reconnect_backoff.get(label, 0)
                    interval = min(self.reconnect_cfg['interval'] * (2 ** backoff),
                                   self.reconnect_cfg['max_backoff'])
                    time.sleep(interval)
                    # AUTO-RECOVERY: po N nieudanych reconnectach (port wisi / Errno 11/5) → programowy replug USB
                    if not is_net and self.usb_reset_enabled and backoff >= self.usb_reset_after:
                        self._usb_reset(port)
                    if not is_net and not os.path.exists(port):
                        if self.log: self.log.debug('ANT', f"⚠️ {label} port {port} not found (USB disconnected)")
                        self._reconnect_backoff[label] = min(backoff + 1, 6)
                        continue
                    if self.log: self.log.info('ANT', f"🔄 Reconnecting {label} ({port or self._tcp_host_port(acfg)})...")
                    if not is_net:                              # force-release dotyczy tylko USB/serial
                        self._force_release_port(port)
                    if self._connect_one(acfg):
                        self._reconnect_backoff[label] = 0
                    else:
                        self._reconnect_backoff[label] = min(backoff + 1, 6)

    # ── RX path ──
    def _mesh_rx(self, packet, interface):
        now_mono = time.monotonic()
        self._last_rx_time = now_mono                    # LBT: kazda ramka radiowa, takze zla/nie-text
        try:
            text = packet.get('decoded', {}).get('text') if isinstance(packet, dict) else None
            if not text: return
            # Decode immediately after JSON parsing.  Dispatch, address filters
            # and RF dedup therefore keep their long-name application contract.
            wire_frame = json.loads(text)
            if isinstance(wire_frame, dict) and wire_frame.get('g') and is_short_frame(wire_frame):
                peer = wire_frame['g']
                with self._cap_lock:
                    first = not self._short_peers.get(peer)
                    self._short_peers[peer] = True
                if first and self.log:
                    self.log.info('LORA', f'codec: {peer} potwierdzil odbior skroconych ramek')
            frame = decode(wire_frame)
            if not isinstance(frame, dict):
                return
            text = json.dumps(frame, separators=(',', ':'))
            now = time.time()
            self._last_rx_ts = now                          # watchdog zwisu: ostatni żywy RX
            h = hashlib.md5(text.encode()).hexdigest()[:8]
            last = self._seen_msgs.get(h)
            self._seen_msgs[h] = now
            if last is not None and (now - last) < self.dedup_seconds:
                if self.log:
                    self.log.debug('RX', f"⏭️ RF-dup ({now - last:.1f}s) odrzucony: {text}")
                return
            if len(self._seen_msgs) > 256:                       # prune stale hashes
                cutoff = now - max(self.dedup_seconds, 10)
                self._seen_msgs = {k: v for k, v in self._seen_msgs.items() if v >= cutoff}
            if self.log: self.log.info('RX', f"📥 RX: {text}")
            if self.on_receive: self.on_receive(text)
        except Exception:
            pass

    # ── TX path ──
    # ruch odkładany podczas transferu-pliku (bulk/periodic; NIE odpowiedzi/komendy/transfery)
    _DEFER_DURING_XFER = {'b', 'ab', 'disc_vio', 'disc_meta', 'hb'}

    def _job_type(self, text):
        try:
            return long_type(json.loads(text).get('t', ''))
        except Exception:
            return ''

    def _sendable_during_xfer(self, text):
        """Czy ten job może iść W TRAKCIE transferu-pliku."""
        if is_immediate_frame(text, self.immediate_types):
            return True
        t = self._job_type(text)
        if t not in self._DEFER_DURING_XFER:
            return True
        # WYJĄTEK P1 (2026-07-30, decyzja usera): `b` z polem `p` to ALARM (leak/smoke/door,
        # batcher priority), nie wolumen — nie może czekać na koniec transferu. Zmierzone live:
        # devmap trzymał kanał ~2 min, a przez ten czas zdarzenie alarmowe stało w kolejce.
        # KOSZT (świadomy): ~120B w środku transferu może zderzyć się z chunkiem → jedna
        # retransmisja chunka (transfer ma własny CRC+ACK+retry, więc się nie wywróci).
        # Alarmy są rzadkie i event-driven, wolumen `b`/`ab`/`hb` dalej czeka jak dotąd.
        return t == 'b' and '"p":' in text

    def _pop_first_sendable(self):
        """Podczas transferu: wybierz pierwszy job NIE-odkładany (transfer/odpowiedź/komenda/alarm),
        zostaw ruch bulk w kolejce (bramka milczy → słyszy cack). None = tylko bulk → milcz."""
        with self.lock:
            for i, job in enumerate(self._tx_queue):
                if self._sendable_during_xfer(job['text']):
                    self._tx_queue.rotate(-i)
                    j = self._tx_queue.popleft()
                    self._tx_queue.rotate(i)
                    return j
        return None

    def _tx_loop(self):
        """Dedicated TX thread. Per-send timeout — main loop never blocks on sendText()."""
        while self.running:
            if not self._tx_queue:
                time.sleep(0.05); continue
            # ARBITER: podczas transferu przepuść tylko transfer/odpowiedzi, odłóż bulk (half-duplex).
            job = None
            if self.arbiter is not None and self.arbiter.busy():
                job = self._pop_first_sendable()
                if job is None:
                    time.sleep(0.15); continue          # tylko bulk w kolejce → milcz, słuchaj cacka
            else:
                job = self._pop_first_sendable()         # 2026-07-18: priorytet komend/odpowiedzi nad bulk (b/ab/hb/disc) TAKZE poza transferem -> niska latencja sterowania/trybu/kalendarza (fallback FIFO gdy sama kolejka bulk)
            wait_started = time.monotonic()
            # F3: wymuś min. odstęp od ostatniego TX (LoRa cooldown) — dławi bursty, chroni przed
            # przepełnieniem kolejki Meshtastic i gubieniem pakietów.
            if self.tx_cooldown > 0:
                gap = time.monotonic() - self._last_tx
                if gap < self.tx_cooldown:
                    time.sleep(self.tx_cooldown - gap)
            if job is None:
                job = self._tx_queue.popleft()
            immediate = is_immediate_frame(job['text'], self.immediate_types)
            # JEDNA bramka slotu, PO odstepie (cooldown potrafi przesunac nadanie za koniec okna):
            # ramka wychodzi w powietrze tylko w oknie bramki, z zapasem min_left na jej czas antenowy.
            if not immediate and self.slot_gate is not None:
                try:
                    try:
                        _w = self.slot_gate(job['text'])         # bramka moze zalezec od typu ramki (odpowiedzi vs inicjowane)
                    except TypeError:
                        _w = self.slot_gate()
                except Exception:
                    _w = 0.0
                if _w > 0:
                    # Nie trzymaj bulkowego joba przez caly czas do slotu. Odlozenie go
                    # pozwala nowej ramce fast-lane wejsc na poczatek kolejki.
                    self._tx_queue.appendleft(job)
                    time.sleep(min(_w, 0.2))
                    continue
            if immediate:
                hard_started = job.setdefault('_lbt_started', wait_started)
                while self.running:
                    now_mono = time.monotonic()
                    _w, requeue = lbt_wait_or_requeue(
                        self._last_rx_time, now_mono, wait_started, hard_started,
                        self.lbt_quiet, self.lbt_max_wait, self.lbt_hard_cap)
                    if requeue:
                        # Miękki limit nie jest zgodą na TX na zajętym kanale. Zachowaj
                        # początek twardego limitu w jobie i spróbuj ponownie po ciszy.
                        self._tx_queue.appendleft(job)
                        job = None
                        time.sleep(0.05)
                        break
                    if _w <= 0:
                        break
                    time.sleep(min(_w, 0.1))
                if not self.running:
                    break
                if job is None:
                    continue
            text, target_label = job['text'], job.get('label')
            if target_label:
                with self.lock: iface = self.interfaces.get(target_label)
                if iface:
                    self._tx_with_timeout(target_label, iface, text)
                else:
                    if self.log: self.log.warn('ANT', f"⚠️ {target_label} gone, fallback broadcast")
                    self._tx_broadcast(text)
            else:
                self._tx_broadcast(text)
            self._last_tx = time.monotonic()      # F3: znacznik do min-spacingu następnego TX
            self._air_log(job['text'])             # moment FAKTYCZNEGO nadania (log TX jest przy wstawieniu do kolejki)
            _e = job.get('evt')
            if _e is not None:
                _e.set()                          # nadawca (ReliableTransfer) liczy timeout ACK od TEJ chwili

    def _air_log(self, text):
        """Osobny plik /tmp/lora_air.log: czas faktycznego nadania + typ + rozmiar. Sluzy do weryfikacji slotow i
        odstepu 3 s (log glowny pokazuje czas wstawienia do kolejki, nie czas anteny). Bez spamu w logu glownym."""
        try:
            now = time.time()
            tt = long_type(json.loads(text).get('t', '?'))
            with open('/tmp/lora_air.log', 'a', encoding='utf-8') as f:
                f.write(time.strftime('%H:%M:%S', time.localtime(now)) + ('.%03d %s %d' % (int((now % 1) * 1000), tt, len(text.encode('utf-8')))) + chr(10))
        except Exception:
            pass

    def _tx_broadcast(self, text):
        with self.lock: ifaces = list(self.interfaces.items())
        sent = False
        for label, iface in ifaces:
            if self._tx_with_timeout(label, iface, text):
                sent = True
        if not sent and self.log:
            self.log.error('ANT', "❌ No antenna available for TX!")

    def _tx_with_timeout(self, label, iface, text):
        """Run iface.sendText() in sub-thread with timeout. Kill antenna if blocked.
        BACKPRESSURE (2026-07-11, root-cause 3-dniowej pętli): sendText BLOKUJE legalnie, gdy
        kolejka TX radia pełna (duty cycle EU 10% dławi opróżnianie przy burstach) — to flow
        control, NIE zwis. Ubicie interfejsu po tx_timeout=8s tworzyło pętlę: burst → block →
        force-close → reconnect → burst → block… (sup głuchy dla bramki przez 3 dni). Teraz:
        po tx_timeout WARN + czekamy dalej do tx_hard_timeout (łącznie) — dopiero wtedy zwis."""
        # STRAŻNIK WYCIEKU WĄTKÓW (2026-08-24): gdy poprzedni sendText() dla tej anteny NADAL wisi,
        # nie startuj kolejnego. Python nie potrafi ubić wątku — porzucony wątek żyje wiecznie, a
        # przy jednym wątku na każdy TX bramka G1 uzbierała 3072 wątki i load 1731 w 10 dni
        # (maszyna przestała się nadawać do czegokolwiek). Flaga sama się kasuje, gdy sendText
        # w końcu wróci, więc antena odzyskuje sprawność bez restartu procesu.
        if self._tx_blocked.get(label):
            if self.log:
                self.log.warn('ANT', f"⛔ {label}: poprzedni sendText() wciąż wisi — pomijam ramkę "
                                     f"(bez tego rośnie góra martwych wątków)")
            return False
        result, error = [False], [None]
        def _do():
            try:
                iface.sendText(text); result[0] = True
            except Exception as e:
                error[0] = e
            finally:
                self._tx_blocked.pop(label, None)   # wrócił — antena znów zdatna do nadawania
        t = threading.Thread(target=_do, daemon=True, name=f"ant-tx-{label}")
        t.start(); t.join(timeout=self.tx_timeout)
        if t.is_alive():
            hard = max(getattr(self, 'tx_hard_timeout', 90) - self.tx_timeout, 1)
            if self.log:
                self.log.warn('ANT', f"⏳ TX {label}: sendText() >{self.tx_timeout}s — backpressure "
                                     f"(kolejka radia/duty-cycle), czekam do {self.tx_timeout + hard}s")
            t.join(timeout=hard)
        if t.is_alive():
            if self.log:
                self.log.error('ANT', f"❌ TX {label}: sendText() blocked "
                                      f">{getattr(self, 'tx_hard_timeout', 90)}s — force-closing")
            self._tx_blocked[label] = True          # wątek porzucony — blokuj kolejne, aż wróci
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                self._close_bg(label, dead)         # close() na zwisie potrafi nie wrócić
            return False
        if error[0]:
            if self.log: self.log.error('ANT', f"❌ TX {label}: {error[0]}")
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                self._close_bg(label, dead)
            return False
        return result[0]
