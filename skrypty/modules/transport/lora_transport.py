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

# meshtastic + pubsub imported lazily in start() — module loadable without hardware deps


class LoraTransport:
    def __init__(self, ports_cfg, reconnect_cfg=None, on_receive=None, logger=None,
                 tx_timeout=8, dedup_seconds=6, tx_cooldown=2.0, tx_hard_timeout=90):
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
        self.tx_cooldown = tx_cooldown
        self._last_tx = 0.0
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

    def send(self, text):
        """Broadcast via ALL antennas. Non-blocking (enqueues to TX thread)."""
        # F2 guard: LoRa/Meshtastic max ~220B/pakiet. Większy = odrzucony/ucięty (cicha awaria).
        # Ostrzegamy GŁOŚNO — łapie bugi typu clear-all enumerujący setki urządzeń w 1 pakiecie.
        n = len(text.encode('utf-8')) if isinstance(text, str) else len(text)
        if n > 220 and self.log:
            self.log.warn('TX', f"⚠️ OVERSIZE {n}B > 220B — Meshtastic ODRZUCI/UTNIE (pakiet nie chunkowany!): {text[:70]}…")
        if self.log: self.log.info('TX', f"📤 TX: {text}")
        self._tx_queue.append({'text': text, 'label': None})

    def send_to(self, gw, text):
        """Unicast via antenna routed to `gw`. Falls back to broadcast if no route."""
        if self.log: self.log.info('TX', f"📤 TX→{gw}: {text}")
        label = self.gw_routes.get(gw)
        if label:
            with self.lock:
                has_iface = label in self.interfaces
            if has_iface:
                self._tx_queue.append({'text': text, 'label': label})
                if self.log: self.log.debug('ANT', f"📡 Unicast TX queued via {label} → {gw}")
                return
        if self.log: self.log.warn('ANT', f"⚠️ No route for {gw}, fallback broadcast")
        self._tx_queue.append({'text': text, 'label': None})

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
        label = acfg['label']; port = acfg.get('port', '')
        try:
            if self._is_net(acfg):                          # antena po sieci (serial-over-IP)
                if not self._meshtastic_tcp:
                    raise RuntimeError("meshtastic.tcp_interface niedostępny")
                host, tcp_port = self._tcp_host_port(acfg)
                iface = self._meshtastic_tcp.TCPInterface(hostname=host, portNumber=tcp_port)
                port = f"tcp://{host}:{tcp_port}"            # do logów/statusu
            else:
                iface = self._meshtastic.SerialInterface(devPath=port)
            with self.lock: self.interfaces[label] = iface
            self._reconnect_backoff[label] = 0
            self._last_rx_ts = time.time()                  # świeże okno RX-watchdog po (re)connect
            if self.log: self.log.info('ANT', f"✅ {label} ({port}) connected")
            return True
        except Exception as e:
            if self.log: self.log.error('ANT', f"❌ {label} ({port}): {e}")
            return False

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
                            try: old.close()
                            except Exception: pass
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
        try:
            text = packet.get('decoded', {}).get('text') if isinstance(packet, dict) else None
            if not text: return
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
            return json.loads(text).get('t', '')
        except Exception:
            return ''

    def _pop_first_sendable(self):
        """Podczas transferu: wybierz pierwszy job NIE-odkładany (transfer/odpowiedź/komenda),
        zostaw ruch bulk w kolejce (bramka milczy → słyszy cack). None = tylko bulk → milcz."""
        with self.lock:
            for i, job in enumerate(self._tx_queue):
                if self._job_type(job['text']) not in self._DEFER_DURING_XFER:
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
            # F3: wymuś min. odstęp od ostatniego TX (LoRa cooldown) — dławi bursty, chroni przed
            # przepełnieniem kolejki Meshtastic i gubieniem pakietów.
            if self.tx_cooldown > 0:
                gap = time.monotonic() - self._last_tx
                if gap < self.tx_cooldown:
                    time.sleep(self.tx_cooldown - gap)
            if job is None:
                job = self._tx_queue.popleft()
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
        result, error = [False], [None]
        def _do():
            try:
                iface.sendText(text); result[0] = True
            except Exception as e:
                error[0] = e
        t = threading.Thread(target=_do, daemon=True)
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
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                try: dead.close()
                except Exception: pass
            return False
        if error[0]:
            if self.log: self.log.error('ANT', f"❌ TX {label}: {error[0]}")
            with self.lock: dead = self.interfaces.pop(label, None)
            if dead:
                try: dead.close()
                except Exception: pass
            return False
        return result[0]
