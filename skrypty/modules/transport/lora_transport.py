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
import os
import threading
import time
from collections import deque

# meshtastic + pubsub imported lazily in start() — module loadable without hardware deps


class LoraTransport:
    def __init__(self, ports_cfg, reconnect_cfg=None, on_receive=None, logger=None,
                 tx_timeout=8, dedup_seconds=6):
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
        self.dedup_seconds = dedup_seconds

        self.interfaces = {}              # {label: SerialInterface}
        self.lock = threading.Lock()
        self._seen_msgs = {}              # {md5: last_seen_ts} — time-windowed dedup
        self._reconnect_backoff = {}
        self.gw_routes = {}               # {"G1": "ANT-1", ...}
        self._tx_queue = deque()
        self._tx_thread = None
        self.running = True

    # ── public API ──
    def start(self):
        """Subscribe to meshtastic RX, connect all enabled antennas, spawn TX + reconnect loops."""
        import meshtastic.serial_interface  # noqa: F401  — needed by _connect_one via module-level ref below
        from pubsub import pub
        self._meshtastic = meshtastic.serial_interface
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
                out[label] = {
                    "port": acfg['port'],
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
        label, port = acfg['label'], acfg['port']
        try:
            iface = self._meshtastic.SerialInterface(devPath=port)
            with self.lock: self.interfaces[label] = iface
            self._reconnect_backoff[label] = 0
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

    def _reconnect_loop(self):
        while self.running:
            time.sleep(5)
            for acfg in self.ports_cfg:
                if not acfg.get('enabled', False): continue
                label, port = acfg['label'], acfg['port']
                with self.lock: connected = label in self.interfaces
                if connected:
                    try:
                        iface = self.interfaces.get(label)
                        if iface and hasattr(iface, 'localNode'):
                            _ = iface.localNode
                    except Exception:
                        if self.log: self.log.warn('ANT', f"⚠️ {label} unhealthy, force-closing")
                        with self.lock: old = self.interfaces.pop(label, None)
                        if old:
                            try: old.close()
                            except Exception: pass
                        self._force_release_port(port)
                else:
                    backoff = self._reconnect_backoff.get(label, 0)
                    interval = min(self.reconnect_cfg['interval'] * (2 ** backoff),
                                   self.reconnect_cfg['max_backoff'])
                    time.sleep(interval)
                    if not os.path.exists(port):
                        if self.log: self.log.debug('ANT', f"⚠️ {label} port {port} not found (USB disconnected)")
                        self._reconnect_backoff[label] = min(backoff + 1, 6)
                        continue
                    if self.log: self.log.info('ANT', f"🔄 Reconnecting {label} ({port})...")
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
    def _tx_loop(self):
        """Dedicated TX thread. Per-send timeout — main loop never blocks on sendText()."""
        while self.running:
            if not self._tx_queue:
                time.sleep(0.05); continue
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

    def _tx_broadcast(self, text):
        with self.lock: ifaces = list(self.interfaces.items())
        sent = False
        for label, iface in ifaces:
            if self._tx_with_timeout(label, iface, text):
                sent = True
        if not sent and self.log:
            self.log.error('ANT', "❌ No antenna available for TX!")

    def _tx_with_timeout(self, label, iface, text):
        """Run iface.sendText() in sub-thread with timeout. Kill antenna if blocked."""
        result, error = [False], [None]
        def _do():
            try:
                iface.sendText(text); result[0] = True
            except Exception as e:
                error[0] = e
        t = threading.Thread(target=_do, daemon=True)
        t.start(); t.join(timeout=self.tx_timeout)
        if t.is_alive():
            if self.log:
                self.log.error('ANT', f"❌ TX {label}: sendText() blocked >{self.tx_timeout}s — force-closing")
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
