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
                 on_avail=None, report_full_every=1, avail_blob_fn=None):
        self.gw_id = gw_id
        self.disc = discovery          # GatewayDiscovery — short_ids, is_monitored, devices
        self.lora = lora
        self.log = logger
        # UNIFIKACJA: callback(dev, available_bool) przy ZMIANIE dostępności — to TO SAMO
        # źródło co flaga `a:` wysyłana do supervisora. Bramka publikuje z tego encję
        # binary_sensor ...available do lokalnego HA → dashboard czyta JEDNO źródło
        # (koniec rozjazdu LQI-heurystyka vs supervisor).
        self.on_avail = on_avail
        # 2026-07-18: gating raportowania trybem bramki. active_fn()==False (np. Nocna w dzien)
        # -> NIE wysylaj stanow urzadzen (`b`) do supervisora. HB leci dalej (ga=0 -> sup wstrzymuje
        # offline). Ustawiane z harnessu po utworzeniu GatewayMode (data.active_fn = gw_mode.is_active).
        self.active_fn = None
        self._avail_pub = {}           # {dev: bool} ostatnio opublikowana dostępność (dedup)
        self._z2m_avail = {}           # {dev: bool} z2m native availability (autorytet gdy włączone)
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
        # (d) REDUKCJA RUCHU LoRa 24/7: report_liveness domyślnie re-wysyła availability
        # WSZYSTKICH urządzeń co sweep (199 dev = ~36 pakietów/15min, w kółko, choć nic się
        # nie zmienia — 688/728 pakietów `b` to sam bit `a:`). report_full_every=N → pełny
        # sweep tylko co N-ty raz; pomiędzy wysyłamy TYLKO urządzenia ze ZMIENIONĄ dostępnością
        # (delta). =1 → zachowanie jak dotąd (bezpieczny default). Supervisor OfflineMonitor to
        # backstop na last_msg_ts — pełny resync musi mieścić się w jego timeoucie (T2=60min):
        # report_full_every=2 przy report_interval=900 → pełny co 30min (bezpieczne), ruch −~45%.
        self.report_full_every = max(1, int(report_full_every or 1))
        self._sweep_n = 0
        # (d) PEŁNY sweep availability → JEDEN skompresowany blob (ReliableTransfer kind='avail')
        # zamiast ~33 pakietów `b` (6 dev/pakiet, ts powtórzony). Payload {ts, a:{sid:0/1}} dla
        # WSZYSTKICH 199 → 1 transfer. DELTA (zmiany availability) nadal małymi `b` natychmiast
        # (event-driven z2m) — responsywność bez zmian. None = stare zachowanie (per-device `b`).
        self.avail_blob_fn = avail_blob_fn
        self.states = {}               # {dev: last full Z2M state dict}
        self.last_seen = {}            # {dev: "date HH:MM:SS"}
        self.last_msg_ts = {}          # {dev: epoch} — dowolna wiadomość z2m (dla stagnation)
        self.linkquality = {}          # {dev: Zigbee LQI 0-255} — ride-along, no own trigger
        self._alive = {}               # {dev: bool} — ostatnio zaraportowana dostępność (transition log)
        self._start_ts = 0.0           # znacznik startu (grace zanim ogłosimy offline)
        self._out = deque()            # paced outbound queue (both batchers share it)
        # ChannelArbiter (opcjonalny, ustawiany z harnessu): gdy trwa transfer-plik, spaced sender
        # WSTRZYMUJE drenaż `_out` (poza gate w Batcher.flush — domyka backlog już zakolejkowany).
        self.arbiter = None
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

    # ── z2m native availability (FAZA 2) ────────────────
    def on_z2m_availability(self, topic, payload):
        """`zigbee2mqtt/<device>/availability` = online/offline (active-ping routerów / passive).
        AUTORYTET dostępności gdy z2m availability włączone — naprawia switche (mains nie
        raportują stanu bez zmiany, ale z2m pinguje → wie czy żyją). Payload: 'online'/'offline'
        albo JSON {"state":"online"}. Brak tych topiców = fallback na timeout (report_liveness)."""
        if not topic.endswith("/availability"):
            return
        dev = topic[len("zigbee2mqtt/"):-len("/availability")]
        if dev not in self.disc.devices:
            return
        p = (payload or "").strip()
        if p.startswith("{"):
            try:
                p = json.loads(p).get("state", "")
            except Exception:
                pass
        online = (p == "online")
        self._z2m_avail[dev] = online              # autorytet dla report_liveness
        if online:                                 # z2m widzi urządzenie żywe → reset zegara
            self.last_msg_ts[dev] = time.time()
        full = {}
        if online and self.disc.is_monitored(dev):
            dtype = self.disc.devices[dev].get('type', 'sensor')
            full = {k: self.states[dev][k] for k in TYPE_CAPS.get(dtype, ())
                    if k in self.states.get(dev, {})}
        # ANTY-SPAM: enqueue TYLKO na realną ZMIANĘ dostępności + force=False (batcher pakuje wiele
        # urządzeń w 1 pakiet). Poprzednio force=True + brak strażnika zmiany → z2m publikujący
        # availability 199 urządzeń (retained/re-eval) = 199 osobnych pakietów `b` [[sid,{a:0}]].
        if self._alive.get(dev) != online:
            self._enqueue(dev, full, force=False, available=online)
            if self.log:
                self.log.info('DATA', f"{'🟢' if online else '💀'} {dev} z2m availability={p}")
        self._alive[dev] = online

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
        if self.active_fn is not None and not self.active_fn():
            return                     # tryb bramki nieaktywny -> brak heartbeatu stanow do supervisora
        now = time.time()
        # (d) pełny sweep co report_full_every-ty raz; pomiędzy — tylko zmiany (delta).
        # KOLEJNOŚĆ: sprawdź PRZED inkrementem → PIERWSZY sweep po starcie jest FULL (blob dla
        # wszystkich zamiast 199×`b`, bo przy starcie _alive puste = changed dla wszystkich).
        full_sweep = (self.report_full_every <= 1) or (self._sweep_n % self.report_full_every == 0)
        self._sweep_n += 1
        # (d) blob: pełny sweep availability jednym skompresowanym transferem (kind='avail')
        # zamiast ~33 pakietów `b`. Zbieramy {sid: bit} dla WSZYSTKICH; monitored dalej dostają
        # wartości `b` (heartbeat #8), non-monitored trafiają TYLKO do blobu.
        use_blob = bool(full_sweep and self.avail_blob_fn)
        blob = {}
        for dev, info in list(self.disc.devices.items()):
            monitored = self.disc.is_monitored(dev)
            oa = self._oa_for(info.get('type', 'sensor'))   # per-typ T1/T2/T3 (lub skalar)
            ts = self.last_msg_ts.get(dev)
            seen = ts is not None
            # FAZA 2: z2m native availability = AUTORYTET (gdy włączone). Naprawia switche/mains,
            # które nie raportują bez zmiany stanu, ale z2m pinguje → wie czy żyją. Brak = timeout.
            if dev in self._z2m_avail:
                alive = self._z2m_avail[dev]
            else:
                if not seen and (now - self._start_ts) < oa:
                    continue                             # grace startowy — z2m nie retainuje stanu
                alive = seen and (now - ts) < oa
            sid = self.disc.short_ids.get(dev)
            if use_blob and sid is not None:
                blob[str(sid)] = 1 if alive else 0        # availability wszystkich → blob
            changed = (self._alive.get(dev) != alive)    # (d) zmiana dostępności od ostatniego sweepu
            # EFEKTYWNOŚĆ (2026-07-05): non-monitored na PEŁNYM sweepie z blobem → TYLKO blob, NIE
            # także `b`. Bug: przy starcie (pusty _alive → changed=True dla WSZYSTKICH) słał 199×
            # non-monitored I przez `b` I do blobu (redundancja = zalew ~33 pakietów `b`). Monitored:
            # `b` (wartości) na zmianę/sweep. Non-monitored bez blobu (między sweepami): `b` na deltę.
            if monitored:
                send_b = changed or full_sweep
            elif use_blob:
                send_b = False                            # pełny sweep → availability przez blob (1 transfer)
            else:
                send_b = changed                          # między sweepami: tylko realna delta
            if alive:
                if send_b:
                    full = {}
                    if monitored:                        # pełne wartości tylko dla monitored
                        dtype = info.get('type', 'sensor')
                        full = {k: self.states[dev][k] for k in TYPE_CAPS.get(dtype, ())
                                if k in self.states.get(dev, {})}
                    self._enqueue(dev, full, force=False, available=True)   # akumuluj, NIE flush per-dev
                if self._alive.get(dev) is False and self.log:
                    self.log.info('DATA', f'🟢 {dev} ONLINE (z2m wrócił)')
                self._alive[dev] = True
            else:
                if send_b:
                    self._enqueue(dev, {}, force=False, available=False)   # akumuluj, NIE flush per-dev
                if self._alive.get(dev) is not False and self.log:
                    silence = int(now - ts) if seen else -1
                    self.log.warn('DATA', f'💀 {dev} OFFLINE (cisza z2m '
                                  f'{silence}s > {oa}s) → available=0')
                self._alive[dev] = False
        # (d) pełny sweep availability → JEDEN skompresowany transfer (ts w blobie RAZ, nie per-pakiet)
        if use_blob and blob:
            try:
                self.avail_blob_fn({'ts': int(now), 'a': blob})
                if self.log:
                    self.log.info('DATA', f'📤 avail blob: {len(blob)} sid → ReliableTransfer (1 transfer, zamiast ~{-(-len(blob)//6)} pkt `b`)')
            except Exception as e:
                if self.log:
                    self.log.warn('DATA', f'avail blob błąd: {e}')
        # JEDEN flush po sweepie → Batcher pakuje wiele urządzeń w pakiet (split do max_payload).
        # Po wprowadzeniu blobu tu lecą tylko: wartości monitored (heartbeat #8) + delty zmian.
        self.pri_batch.flush()
        self.mon_batch.flush()

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
        if self.active_fn is not None and not self.active_fn():
            return                     # tryb bramki nieaktywny -> nie wysylaj stanow do supervisora
        if self.send_spacing > 0:
            self._out.append(packet)
        else:
            self.lora.send(json.dumps(packet, separators=(',', ':')))

    def _sender_loop(self):
        while self._sender_running:
            if self.arbiter is not None and self.arbiter.busy():
                time.sleep(0.2)                  # transfer-plik trwa → wstrzymaj drenaż `b` (kanał zajęty)
                continue
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
