"""BatteryMonitor — gateway side (step 5 / krok 13): low/critical battery dla WSZYSTKICH
urządzeń (także spoza monitored/priority).

Progi (CONFIG): low < `low_pct` (domyślnie 25%), critical < `crit_pct` (15%). Czyta poziom
baterii ze stanu Z2M (callback `get_battery(dev)` → int|None, zwykle GatewayData.states[dev]).
One-shot per urządzenie: przejście ok→low = `lb`, ok/low→critical = `cb`, powrót >low = `bo`
(auto-clear). Emituje przez `emit(sid, code, value)` (zwykle AnomalyBatcher.add).

Wzór: gateway_v38 _check_battery_anomaly. Brak gate `is_monitored` — sprawdza ALL devices.
"""
import threading
import time


class BatteryMonitor:
    def __init__(self, gw_id, discovery, get_battery, emit, get_thresholds,
                 logger=None, check_interval=300):
        self.gw_id = gw_id
        self.disc = discovery            # GatewayDiscovery (devices, short_ids) — ALL devices
        self.get_battery = get_battery   # callable(dev) → int|None
        self.emit = emit                 # callable(sid, code, value)
        self.get_thresholds = get_thresholds   # callable() → (low_pct, crit_pct)
        self.log = logger
        self.check_interval = check_interval
        self.level = {}                  # dev → 'ok' | 'low' | 'critical'
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="battery").start()
        if self.log:
            self.log.info('BATT', f'🔋 Battery monitor start (check co {self.check_interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('BATT', f'loop: {e}')

    def _classify(self, b, low, crit):
        if b < crit:
            return 'critical'
        if b < low:
            return 'low'
        return 'ok'

    def check(self):
        low, crit = self.get_thresholds()
        for dev in list(self.disc.devices.keys()):
            b = self.get_battery(dev)
            if b is None:
                continue                 # urządzenie bez baterii (sieciowe) / brak odczytu
            try:
                b = int(b)
            except (TypeError, ValueError):
                continue
            sid = self.disc.short_ids.get(dev)
            if sid is None:
                continue
            level = self._classify(b, low, crit)
            prev = self.level.get(dev, 'ok')
            if level == prev:
                continue
            self.level[dev] = level
            if level == 'critical':
                self.emit(sid, 'cb', b)
            elif level == 'low':
                self.emit(sid, 'lb', b)
            else:
                self.emit(sid, 'bo', b)  # recovery (auto-clear)
            if self.log:
                icon = {'critical': '🪫', 'low': '🔋', 'ok': '✅'}[level]
                self.log.warn('BATT', f'{icon} {dev} bateria {b}% → {level}')

    def battery_levels(self):
        return dict(self.level)

    def snapshot(self):
        """[(sid,'lb'/'cb',None)] dla dump_anom — aktualne low/critical."""
        out = []
        for dev, lvl in self.level.items():
            if lvl not in ("low", "critical"):
                continue
            sid = self.disc.short_ids.get(dev)
            if sid is not None:
                out.append((sid, "cb" if lvl == "critical" else "lb", None))
        return out
