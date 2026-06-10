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
                 logger=None, check_interval=30, grace=120):
        self.gateways = gateways
        self.disc = discovery            # SupervisorDiscovery (devices(gw))
        self.data = data                 # SupervisorData (last_msg_ts)
        self.get_timeouts = get_timeouts  # callable() → dict {T1,T2,T3} minut
        self.set_avail = set_avail_fn    # callable(gw, dev, online_bool)
        self.log = logger
        self.check_interval = check_interval
        self.grace = grace
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
            for dev, info in self.disc.devices(gw).items():
                ts = self.data.last_msg_ts.get((gw, dev))
                if ts is None:
                    continue             # jeszcze nic nie przyszło → brak odniesienia
                timeout_s = self._timeout_for(info.get('type'), t) * 60 + self.grace
                stale = (now - ts) > timeout_s
                key = (gw, dev)
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
