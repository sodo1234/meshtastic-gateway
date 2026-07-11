"""TempHumMonitor — gateway side (step 5 / krok 14): high/low TEMPERATURA i WILGOTNOŚĆ
dla WSZYSTKICH urządzeń bramki (override usera 2026-06-25: anomalie dla wszystkich, nie tylko
monitored). Progi z CONFIG.

Progi (CONFIG anomaly): temp_low/temp_high [°C], hum_low/hum_high [%]. Czyta z
GatewayData.states (klucze 'temperature'/'humidity'). One-shot per (dev, wymiar):
  ok→high = 'th'/'hh', ok→low = 'tl'/'hl', powrót w normę = 'to'/'ho' (auto-clear).
Emituje przez emit(sid, code, value) → AnomalyBatcher.add. value = bieżący odczyt
(wyświetlany jako poziom na dashboardzie). Próg None = wymiar wyłączony.

Wzór: gateway_v38 _check_temp_hum_anomaly. Kategorie: th/tl/to→temp, hh/hl/ho→hum (→ kubełek 'other').
"""
import threading
import time


class TempHumMonitor:
    # (wymiar, klucz_stanu, próg_low, próg_high, kod_high, kod_low, kod_ok)
    DIMS = [
        ("temp", "temperature", "temp_low", "temp_high", "th", "tl", "to"),
        ("hum",  "humidity",    "hum_low",  "hum_high",  "hh", "hl", "ho"),
    ]
    _ICON = {"high": "🔺", "low": "🔻", "ok": "✅"}

    def __init__(self, gw_id, discovery, get_state, emit, get_thresholds,
                 logger=None, check_interval=60):
        self.gw_id = gw_id
        self.disc = discovery            # GatewayDiscovery (is_monitored, short_ids, devices)
        self.get_state = get_state       # callable(dev) → dict (GatewayData.states[dev])
        self.emit = emit                 # callable(sid, code, value)
        self.get_thresholds = get_thresholds   # callable() → {temp_low,temp_high,hum_low,hum_high}
        self.log = logger
        self.check_interval = check_interval
        self.level = {}                  # (dev, dim) → 'ok' | 'high' | 'low'
        self.value = {}                  # (dev, dim) → ostatni odczyt (snapshot/value)
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="temphum").start()
        if self.log:
            self.log.info('TEMP', f'🌡️ Temp/Hum monitor start (monitored, check co {self.check_interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('TEMP', f'loop: {e}')

    @staticmethod
    def _classify(v, low, high):
        if high is not None and v > high:
            return 'high'
        if low is not None and v < low:
            return 'low'
        return 'ok'

    def check(self):
        thr = self.get_thresholds() or {}
        for dev in list(self.disc.devices.keys()):   # WSZYSTKIE urządzenia bramki (nie tylko monitored)
            st = self.get_state(dev) or {}
            sid = self.disc.short_ids.get(dev)
            if sid is None:
                continue
            for dim, skey, lowk, highk, ch, cl, cok in self.DIMS:
                raw = st.get(skey)
                if raw is None:
                    continue
                try:
                    v = float(raw)
                except (TypeError, ValueError):
                    continue
                self.value[(dev, dim)] = v
                level = self._classify(v, thr.get(lowk), thr.get(highk))
                prev = self.level.get((dev, dim), 'ok')
                if level == prev:
                    continue
                self.level[(dev, dim)] = level
                code = ch if level == 'high' else cl if level == 'low' else cok
                self.emit(sid, code, v)
                if self.log:
                    self.log.warn('TEMP', f'{self._ICON[level]} {dev} {dim}={v} → {level}')

    def levels(self):
        return dict(self.level)

    def snapshot(self):
        """[(sid, code, value)] dla dump_anom — aktualne high/low z wartością (poziom)."""
        _code = {"temp": {"high": "th", "low": "tl"}, "hum": {"high": "hh", "low": "hl"}}
        out = []
        for (dev, dim), lvl in self.level.items():
            if lvl == 'ok':
                continue
            sid = self.disc.short_ids.get(dev)
            if sid is not None:
                out.append((sid, _code[dim][lvl], self.value.get((dev, dim))))
        return out
