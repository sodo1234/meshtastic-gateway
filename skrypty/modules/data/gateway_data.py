"""GatewayData — Step 3 gateway side: Z2M state → delta → batched `b` over LoRa.

Owns two streams (CLAUDE.md priorities):
  - monitored : P3, flush every 30s  → {t:b,...}
  - priority  : P1, flush every 10s  (priority_devices, ships sooner)

Pulls device identity (type, sid, monitored/priority) from GatewayDiscovery so
short ids stay consistent with the `db` the supervisor already registered.

Wiring (app):  mqtt.subscribe("zigbee2mqtt/+")  and route those topics to
on_z2m(topic, payload); register handle_req on the dispatcher for `req`.
"""
import json
import threading
import time
from collections import deque

from .batcher import Batcher
from .z2m_reader import compute_delta, encode_short, TYPE_CAPS


def _now_str():
    return time.strftime('%d.%m %H:%M:%S')      # data + godzina


class GatewayData:
    def __init__(self, gw_id, discovery, lora, logger=None,
                 mon_interval=30, pri_interval=10, max_payload=150,
                 thresholds=None, send_spacing=0,
                 report_interval=0, offline_after=None, offline_after_fn=None,
                 on_avail=None):
        self.gw_id = gw_id
        self.disc = discovery          # GatewayDiscovery — short_ids, is_monitored, devices
        self.lora = lora
        self.log = logger
        # UNIFIKACJA: callback(dev, available_bool) przy ZMIANIE dostępności — to TO SAMO
        # źródło co flaga `a:` wysyłana do supervisora. Bramka publikuje z tego encję
        # binary_sensor ...available do lokalnego HA → dashboard czyta JEDNO źródło
        # (koniec rozjazdu LQI-heurystyka vs supervisor).
        self.on_avail = on_avail
        self._avail_pub = {}           # {dev: bool} ostatnio opublikowana dostępność (dedup)
        self.thresholds = thresholds
        self.send_spacing = send_spacing   # s between outbound `b` frames (LoRa cooldown).
                                           # 0 = send synchronously (tests); >0 = paced TX thread.
        # #8 periodic report + #7 gw-side liveness:
        #   report_interval > 0 ⇒ co tyle sekund wysyłamy `b` dla KAŻDEGO monitored
        #   urządzenia (heartbeat danych), available=1 gdy z2m świeży, available=0 gdy
        #   cisza > offline_after. Bramka = autorytet dostępności (CLAUDE.md: bramka
        #   autonomiczna). Supervisor OfflineMonitor zostaje backstopem (martwa bramka).
        self.report_interval = report_interval
        self.offline_after = offline_after if offline_after is not None \
            else (report_interval * 2 + 120 if report_interval else 0)
        # opcjonalny per-typ timeout offline (CLAUDE.md: T1 switch/light, T2 temp/hum,
        # T3 door/leak/motion). callable(dtype)->sekundy. None → skalar self.offline_after
        # (ścieżka step 3 bez zmian). Naprawia fałszywe offline urządzeń bateryjnych,
        # które meldują rzadziej niż domyślne ~32 min.
        self.offline_after_fn = offline_after_fn
        self.states = {}               # {dev: last full Z2M state dict}
        self.last_seen = {}            # {dev: "date HH:MM:SS"}
        self.last_msg_ts = {}          # {dev: epoch} — dowolna wiadomość z2m (dla stagnation)
        self.linkquality = {}          # {dev: Zigbee LQI 0-255} — ride-along, no own trigger
        self._alive = {}               # {dev: bool} — ostatnio zaraportowana dostępność (transition log)
        self._start_ts = 0.0           # znacznik startu (grace zanim ogłosimy offline)
        self._out = deque()            # paced outbound queue (both batchers share it)
        self._sender_running = False
        self._report_running = False
        self.mon_batch = Batcher(gw_id, self._send, interval=mon_interval,
                                 max_payload=max_payload, tag="MON", logger=logger)
        self.pri_batch = Batcher(gw_id, self._send, interval=pri_interval,
                                 max_payload=max_payload, tag="PRI", logger=logger)

    # ── lifecycle ───────────────────────────────────────
    def start(self):
        self._start_ts = time.time()
        self.mon_batch.start()
        self.pri_batch.start()
        if self.send_spacing > 0:
            self._sender_running = True
            threading.Thread(target=self._sender_loop, daemon=True,
                             name="data-tx").start()
        if self.report_interval > 0:
            self._report_running = True
            threading.Thread(target=self._report_loop, daemon=True,
                             name="data-report").start()
        if self.log:
            extra = (f', report {self.report_interval}s/offline>{self.offline_after}s'
                     if self.report_interval > 0 else '')
            self.log.info('DATA', f'📊 GatewayData started (mon {self.mon_batch.interval}s / '
                          f'pri {self.pri_batch.interval}s, spacing {self.send_spacing}s{extra})')

    def stop(self):
        self.mon_batch.stop()
        self.pri_batch.stop()
        self._sender_running = False
        self._report_running = False

    def subscribe(self, mqtt):
        """Subscribe to all Z2M device topics on the gateway's local broker."""
        mqtt.subscribe("zigbee2mqtt/+")

    # ── ingest ──────────────────────────────────────────
    def on_z2m(self, topic, payload):
        """Handle a `zigbee2mqtt/<device>` retained/live state message."""
        dev = self._device_from_topic(topic)
        if not dev:
            return
        info = self.disc.devices.get(dev)
        if not info:
            return                     # unknown device (not yet discovered)
        try:
            data = json.loads(payload)
        except Exception:
            return
        if not isinstance(data, dict):
            return

        dtype = info.get('type', 'sensor')
        old = self.states.get(dev, {})
        delta = compute_delta(old, data, dtype, self.thresholds)

        # any fresh Z2M state ⇒ device online + last_seen refresh
        self.states[dev] = {**old, **{k: data[k] for k in TYPE_CAPS.get(dtype, ()) if k in data}}
        self.last_seen[dev] = _now_str()
        self.last_msg_ts[dev] = time.time()    # dowolna wiadomość → reset zegara stagnation
        if 'linkquality' in data:          # Zigbee LQI — track always, ship as ride-along
            self.linkquality[dev] = data['linkquality']

        if not delta or not self.disc.is_monitored(dev):
            return
        self._enqueue(dev, delta)

    def _enqueue(self, dev, caps_dict, force=False, available=True):
        sid = self.disc.short_ids.get(dev)
        if sid is None:
            return
        # UNIFIKACJA: zgłoś zmianę dostępności (to samo źródło co `a:`) → encja na lokalnym HA
        if self.on_avail is not None and self._avail_pub.get(dev) != available:
            self._avail_pub[dev] = available
            try:
                self.on_avail(dev, available)
            except Exception:
                pass
        cd = dict(caps_dict)
        if available:
            lq = self.linkquality.get(dev)  # ride-along: LQI nie wyzwala batcha, ale jedzie z delta
            if lq is not None:
                cd['linkquality'] = lq
        short = encode_short(cd, available=available)
        if dev in self.disc.priority_names:
            self.pri_batch.add(sid, short, force_flush=force)
        else:
            self.mon_batch.add(sid, short, force_flush=force)

    # ── periodic report + gw-side liveness (#7/#8) ──────
    def _report_loop(self):
        """Co report_interval wyślij `b` dla każdego monitored urządzenia:
        available=1 + ostatnie wartości (heartbeat danych, #8) gdy z2m świeży,
        available=0 (#7 martwy/zacichły) gdy cisza > offline_after po grace."""
        # Pierwszy tick PO grace (60s) — szybko ustala availability po starcie/restarcie
        # (inaczej 15-min okno bez heartbeatu dostępności). Potem co report_interval.
        first_grace = min(60.0, self.report_interval)
        waited = self.report_interval - first_grace
        while self._report_running:
            time.sleep(1.0)
            waited += 1.0
            if waited < self.report_interval:
                continue
            waited = 0.0
            try:
                self.report_liveness()
            except Exception as e:                       # nigdy nie ubijaj wątku
                if self.log:
                    self.log.warn('DATA', f'report_liveness błąd: {e}')

    def _oa_for(self, dtype):
        """Timeout offline [s] dla typu urządzenia. Per-typ (T1/T2/T3) jeśli podano
        offline_after_fn; inaczej skalar self.offline_after (ścieżka step 3)."""
        if self.offline_after_fn:
            try:
                v = self.offline_after_fn(dtype)
                if v:
                    return v
            except Exception:
                pass
        return self.offline_after

    def report_liveness(self):
        """Okresowy heartbeat dostępności = JEDYNE źródło offline (bramka autorytetem, CLAUDE.md).
        UNIFIKACJA 2026-06-20: pokrywa WSZYSTKIE urządzenia (nie tylko monitored), bo urządzenia
        event-driven (door/switch) inaczej nigdy nie dostają update'u availability → supervisor
        utyka. Dla non-monitored wysyłamy TYLKO bit `a:` (puste caps, ~minimalny payload — szanuje
        LoRa), pełne wartości tylko dla monitored (heartbeat danych #8)."""
        now = time.time()
        for dev, info in list(self.disc.devices.items()):
            monitored = self.disc.is_monitored(dev)
            oa = self._oa_for(info.get('type', 'sensor'))   # per-typ T1/T2/T3 (lub skalar)
            ts = self.last_msg_ts.get(dev)
            seen = ts is not None
            if not seen and (now - self._start_ts) < oa:
                continue                                 # grace startowy — z2m nie retainuje stanu
            alive = seen and (now - ts) < oa
            if alive:
                full = {}
                if monitored:                            # pełne wartości tylko dla monitored
                    dtype = info.get('type', 'sensor')
                    full = {k: self.states[dev][k] for k in TYPE_CAPS.get(dtype, ())
                            if k in self.states.get(dev, {})}
                self._enqueue(dev, full, force=True, available=True)
                if self._alive.get(dev) is False and self.log:
                    self.log.info('DATA', f'🟢 {dev} ONLINE (z2m wrócił)')
                self._alive[dev] = True
            else:
                if self._alive.get(dev) is not False and self.log:
                    silence = int(now - ts) if seen else -1
                    self.log.warn('DATA', f'💀 {dev} OFFLINE (cisza z2m '
                                  f'{silence}s > {oa}s) → available=0')
                self._enqueue(dev, {}, force=True, available=False)
                self._alive[dev] = False

    # ── refresh on-demand (supervisor `req`) ────────────
    def handle_req(self, data):
        """{t:req,g:G1,d:"Temp 2"} → ship last known full value immediately."""
        if data.get('g') not in (self.gw_id, None):
            return
        dev = data.get('d')
        if not dev or dev not in self.states:
            if self.log:
                self.log.warn('DATA', f'↻ req: brak stanu dla {dev!r}')
            return
        info = self.disc.devices.get(dev, {})
        dtype = info.get('type', 'sensor')
        full = {k: self.states[dev][k] for k in TYPE_CAPS.get(dtype, ())
                if k in self.states.get(dev, {})}
        if self.log:
            self.log.info('DATA', f'↻ req {dev}: wysyłam ostatnią wartość {full}')
        self._enqueue(dev, full, force=True)

    # ── helpers ─────────────────────────────────────────
    def _device_from_topic(self, topic):
        if not topic.startswith("zigbee2mqtt/"):
            return None
        rest = topic[len("zigbee2mqtt/"):]
        if not rest or rest.startswith("bridge") or rest.endswith("/set") \
                or rest.endswith("/get") or rest.endswith("/availability"):
            return None
        return rest

    def _send(self, packet):
        """Batcher flush callback. Paced through a shared TX queue when
        send_spacing>0 so the two batchers never burst into each other and the
        LoRa cooldown is honored; synchronous (direct) otherwise (tests)."""
        if self.send_spacing > 0:
            self._out.append(packet)
        else:
            self.lora.send(json.dumps(packet, separators=(',', ':')))

    def _sender_loop(self):
        while self._sender_running:
            if not self._out:
                time.sleep(0.05)
                continue
            pkt = self._out.popleft()
            self.lora.send(json.dumps(pkt, separators=(',', ':')))
            time.sleep(self.send_spacing)

    @property
    def stats(self):
        return {"mon": self.mon_batch.stats, "pri": self.pri_batch.stats,
                "devices": len(self.states)}
