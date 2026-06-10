"""StagnationEngine — gateway side (step 15): brak JAKIEJKOLWIEK wiadomości z2m
przez P1[h] (bateryjne) / P2[h] (sieciowe) → anomalia stagnacji.

One-shot: zgłasza `ab`(sg) raz; gdy urządzenie znów raportuje → `sc` (clear).
Czyta znaczniki czasu z GatewayData.last_msg_ts i typ zasilania z
GatewayDiscovery.devices[dev]['mains_powered']. Progi (godziny) z callbacka
(zwykle param_sync P1/P2) — czytane na bieżąco, więc zmiana parametru działa live.
"""
import threading
import time


class StagnationEngine:
    def __init__(self, gw_id, discovery, data, lora_send, get_thresholds,
                 logger=None, check_interval=300):
        self.gw_id = gw_id
        self.disc = discovery            # GatewayDiscovery (devices, short_ids, is_monitored)
        self.data = data                 # GatewayData (last_msg_ts)
        self.send = lora_send            # callable(dict) → LoRa
        self.get_thresholds = get_thresholds   # callable() → (p1_hours, p2_hours)
        self.log = logger
        self.check_interval = check_interval
        self.stagnant = set()            # dev names currently reported stagnant
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="stagnation").start()
        if self.log:
            self.log.info('STAG', f'🕰️ Stagnation start (check co {self.check_interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('STAG', f'loop: {e}')

    def check(self):
        p1, p2 = self.get_thresholds()
        now = time.time()
        for dev, info in list(self.disc.devices.items()):
            if not self.disc.is_monitored(dev):
                continue
            ts = self.data.last_msg_ts.get(dev)
            if ts is None:
                continue                 # nigdy nie widziane → brak punktu odniesienia
            thr_h = p2 if info.get('mains_powered') else p1
            stagnant = (now - ts) > thr_h * 3600
            if stagnant and dev not in self.stagnant:
                self.stagnant.add(dev)
                self._report(dev, thr_h, True)
            elif not stagnant and dev in self.stagnant:
                self.stagnant.discard(dev)
                self._report(dev, thr_h, False)

    def _report(self, dev, hours, on):
        sid = self.disc.short_ids.get(dev)
        if sid is None:
            return
        code = "sg" if on else "sc"
        self.send({"t": "ab", "g": self.gw_id, "ts": int(time.time()),
                   "d": [[sid, code, hours]]})
        if self.log:
            self.log.info('STAG', f'{"⚠️" if on else "✅"} {dev} '
                          f'{"STAGNACJA" if on else "recovery"} (próg {hours}h)')

    def stagnant_devices(self):
        return set(self.stagnant)
