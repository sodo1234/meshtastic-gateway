"""OfflineMonitor — supervisor side (step 12): brak wiadomości od urządzenia przez
timeout (wg typu) → device OFFLINE; powrót wiadomości → ONLINE.

Timeout [min] wg typu: T1 switch/light, T2 sensor(temp/hum), T3 binary(door/leak/motion).
+ grace [s]. Czyta znaczniki czasu z SupervisorData.last_msg_ts, typy z
SupervisorDiscovery. Progi z callbacka (param_sync T1/T2/T3) — czytane na bieżąco.
"""
import threading
import time


class OfflineMonitor:
    def __init__(self, gateways, discovery, data, get_timeouts, set_avail_fn,
                 logger=None, check_interval=30, grace=120, is_suppressed=None):
        self.gateways = gateways
        self.disc = discovery            # SupervisorDiscovery (devices(gw))
        self.data = data                 # SupervisorData (last_msg_ts)
        self.get_timeouts = get_timeouts  # callable() → dict {T1,T2,T3} minut
        self.set_avail = set_avail_fn    # callable(gw, dev, online_bool)
        self.log = logger
        self.check_interval = check_interval
        self.grace = grace
        # STEP 5: is_suppressed(gw) → True gdy bramka nieaktywna (np. nocna w dzień) →
        # NIE oznaczaj offline jej urządzeń (świadomie bez zasilania). Wyczyść istniejące offline.
        self.is_suppressed = is_suppressed
        self.offline = set()             # {(gw, dev)}
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="offline").start()
        if self.log:
            self.log.info('OFFLINE', f'💀 Offline monitor start (check co {self.check_interval}s, grace {self.grace}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('OFFLINE', f'loop: {e}')

    def check(self):
        t = self.get_timeouts()
        now = time.time()
        for gw in self.gateways:
            suppressed = bool(self.is_suppressed and self.is_suppressed(gw))
            for dev, info in self.disc.devices(gw).items():
                key = (gw, dev)
                if suppressed:           # bramka nieaktywna → traktuj jak online, wyczyść offline
                    if key in self.offline:
                        self.offline.discard(key)
                        self.set_avail(gw, dev, True)
                    continue
                ts = self.data.last_msg_ts.get((gw, dev))
                if ts is None:
                    continue             # jeszcze nic nie przyszło → brak odniesienia
                timeout_s = self._timeout_for(info.get('type'), t) * 60 + self.grace
                stale = (now - ts) > timeout_s
                if stale and key not in self.offline:
                    self.offline.add(key)
                    self.set_avail(gw, dev, False)
                    if self.log:
                        self.log.warn('OFFLINE', f'💀 {gw}/{dev} OFFLINE (brak {int((now - ts) / 60)}min)')
                elif not stale and key in self.offline:
                    self.offline.discard(key)
                    self.set_avail(gw, dev, True)
                    if self.log:
                        self.log.info('OFFLINE', f'🟢 {gw}/{dev} ONLINE (wiadomość wróciła)')

    def _timeout_for(self, dtype, t):
        if dtype in ('switch', 'light'):
            return t.get('T1', 30)
        if dtype == 'sensor':
            return t.get('T2', 60)
        return t.get('T3', 120)          # binary_sensor (door/leak/motion)

    def offline_devices(self):
        return set(self.offline)


class GatewayOfflineAnomaly:
    """Gateway side (step 5): cisza z2m > timeout(typ) → anomalia `do` (offline), powrót → `dn`.
    Dla WSZYSTKICH urządzeń (także spoza monitored/priority). Niezależne od Z2M availability —
    liczone z GatewayData.last_msg_ts. Emituje przez AnomalyBatcher (`emit(sid,code)`).

    Supresja trybu bramki: `is_active()` → False (np. nocna bramka w dzień) → NIE zgłaszaj
    offline, wyczyść istniejące (urządzenia świadomie bez zasilania)."""

    def __init__(self, gw_id, discovery, data, emit, get_timeout, logger=None,
                 check_interval=30, grace=120, is_active=None):
        self.gw_id = gw_id
        self.disc = discovery            # GatewayDiscovery (devices, short_ids) — ALL
        self.data = data                 # GatewayData (last_msg_ts keyed by dev)
        self.emit = emit                 # callable(sid, code)
        self.get_timeout = get_timeout   # callable(dtype) → minuty
        self.log = logger
        self.check_interval = check_interval
        self.grace = grace
        self.is_active = is_active       # callable() → bool (None = zawsze aktywna)
        self.offline = set()             # dev names (= availability offline, do hasha/dryfu)
        # ACK: urządzenia offline RĘCZNIE wyczyszczone (clear z bramki/supervisora przez ac_b).
        # Rozdziela AVAILABILITY (prawda — urządzenie nadal offline, zostaje w self.offline dla
        # hasha) od ANOMALII (ACK-owalny alert). Acked → NIE re-emituj `do` i snapshot() go pomija,
        # więc dump_anom NIE re-dodaje. Kasowane przy recovery (dev wraca online) → re-alert gdy
        # znów padnie. Odpowiednik ack_offline na supervisorze.
        self.ack = set()
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="gw-offline").start()
        if self.log:
            self.log.info('OFFLINE', f'💀 GW offline anomaly start (check co {self.check_interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('OFFLINE', f'gw-loop: {e}')

    def check(self):
        active = (self.is_active() if self.is_active else True)
        # UNIFIKACJA 2026-07-02 (fix dump_anom=0): offline-ANOMALIA == AVAILABILITY bramki.
        # Zamiast osobnego, rozjeżdżającego się timeoutu — czytamy autorytatywny GatewayData._alive
        # (wynik report_liveness + eventów z2m: True/False/None). To TO SAMO źródło co flaga `a:`
        # wysyłana do supervisora → 161 realnie-offline (żyjące dotąd tylko w availability) wpadają
        # do anomaly-store → snapshot()/dump_anom/batch/sensory lora_an_g1_* działają. Bramka =
        # source of truth. None = jeszcze nieokreślone (grace startowy) → nie zgłaszaj.
        # (get_timeout/grace zostają w sygnaturze dla zgodności; availability już liczy timeout.)
        alive_map = getattr(self.data, '_alive', {})
        for dev in list(self.disc.devices.keys()):
            sid = self.disc.short_ids.get(dev)
            if sid is None:
                continue
            if not active:               # bramka nieaktywna → wyczyść offline, nie zgłaszaj
                if dev in self.offline:
                    self.offline.discard(dev)
                    self.emit(sid, "dn")
                continue
            state = alive_map.get(dev)
            if state is None:            # dostępność jeszcze nieokreślona (grace) → pomiń
                continue
            stale = (state is False)
            if stale and dev not in self.offline:
                self.offline.add(dev)                    # availability offline (do hasha)
                if dev not in self.ack:                  # acked → alert wyciszony (dump nie re-doda)
                    self.emit(sid, "do")
                    if self.log:
                        self.log.warn('OFFLINE', f'💀 {dev} OFFLINE (availability)')
            elif not stale and dev in self.offline:
                self.offline.discard(dev)
                self.ack.discard(dev)                    # recovery → kasuj ack (re-alert gdy znów padnie)
                self.emit(sid, "dn")
                if self.log:
                    self.log.info('OFFLINE', f'🟢 {dev} ONLINE (wróciło)')

    def offline_devices(self):
        return set(self.offline)

    def ack_clear(self, dev):
        """Ręczny clear anomalii offline (z ac_b / lokalnego HA bramki): wycisz alert do recovery.
        Urządzenie zostaje w self.offline (availability=offline nadal prawdą, hash spójny)."""
        self.ack.add(dev)

    def unack(self, dev):
        """Cofnij ręczne wyciszenie offline (AnomalyReconciler re-arm: urządzenie NADAL offline po
        upływie okna wyciszenia → anomalia wraca; snapshot()/dump znów ją uwzględnia)."""
        self.ack.discard(dev)

    def snapshot(self):
        """[(sid,'do',None)] dla dump_anom — aktualnie offline, POMIJAJĄC acked (nie re-dodawaj)."""
        return [(self.disc.short_ids.get(d), "do", None) for d in self.offline
                if d not in self.ack and self.disc.short_ids.get(d) is not None]
