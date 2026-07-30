"""AlarmMonitor — WYCIEK (`wl`) i DYM (`sk`) jako anomalie. Strona bramki.

LUKA ZNALEZIONA 2026-07-30: kody `wl`/`sk` były w mapach etykiet (`store.CODE_LABEL`), w
kategoriach (`batcher.CODE_CAT`) i na liście krytycznych (`store.CRITICAL_CODES` → popup), ale
**nikt ich nie generował** — nie istniał silnik. Efekt: zalanie jechało do supervisora wyłącznie
jako stan urządzenia (`w:1` w batchu priority), więc pojawiało się jako binary_sensor, ale NIE
trafiało do liczników, list, popupu ani do skasowania. Przy „wykrywanie wycieków/pożarów" w
celach systemu to była luka blokująca wdrożenie u klienta.

Zakres: WSZYSTKIE urządzenia bramki (jak TempHumMonitor po override usera 2026-06-25) — czujnik
wycieku nie musi być `monitored`, żeby zalanie było alarmem.

Kody: alarm `wl`/`sk`, powrót do normy `wo`/`ko` (spójne z `to`/`ho`/`bo`; `k` = smoke w tabeli
capów protokołu). Kategorie `water`/`smoke` → kubełek `other`, ale flaga CRITICAL → popup.

Wzór interfejsu 1:1 z TempHumMonitor (get_state/emit/snapshot), żeby wpiąć się w istniejący
AnomalyBatcher + reconciler bez wyjątków w kodzie wołającym.
"""
import threading
import time


class AlarmMonitor:
    # (klucz stanu z2m, kod alarmu, kod powrotu, ikona logu, etykieta)
    CAPS = [
        ("water_leak", "wl", "wo", "💧", "ZALANIE"),
        ("smoke",      "sk", "ko", "🔥", "DYM"),
    ]

    def __init__(self, gw_id, discovery, get_state, emit, logger=None, check_interval=10):
        self.gw_id = gw_id
        self.disc = discovery            # GatewayDiscovery (devices, short_ids)
        self.get_state = get_state       # callable(dev) → dict (GatewayData.states[dev])
        self.emit = emit                 # callable(sid, code, value)
        self.log = logger
        # 10 s (nie 60 jak temp/hum): przy wycieku i dymie liczy się czas reakcji, a sprawdzenie
        # jest lokalne — zero ruchu LoRa dopóki nie ma ZMIANY stanu.
        self.check_interval = check_interval
        self.active = {}                 # (dev, cap) → bool
        self.running = True

    def start(self):
        threading.Thread(target=self._loop, daemon=True, name="alarm").start()
        if self.log:
            self.log.info('ALARM', f'🚨 Monitor zalania/dymu start (wszystkie urządzenia, '
                                   f'check co {self.check_interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.check_interval)
            try:
                self.check()
            except Exception as e:
                if self.log:
                    self.log.error('ALARM', f'loop: {e}')

    @staticmethod
    def _truthy(raw):
        """z2m bywa różny: bool, 'ON'/'OFF', 1/0, 'true'/'false'."""
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            return raw.strip().lower() in ('on', 'true', 'yes', '1')
        return False

    def check(self):
        for dev in list(self.disc.devices.keys()):
            st = self.get_state(dev) or {}
            sid = self.disc.short_ids.get(dev)
            if sid is None:
                continue
            for skey, code, code_ok, icon, label in self.CAPS:
                raw = st.get(skey)
                if raw is None:
                    continue                     # urządzenie nie ma tej capy
                now_on = self._truthy(raw)
                prev = self.active.get((dev, skey))
                if prev is None:
                    self.active[(dev, skey)] = now_on
                    if not now_on:
                        continue                 # pierwszy odczyt „czysto" = nic nie zgłaszamy
                    prev = False                 # start z alarmem → zgłoś od razu
                if now_on == prev:
                    continue
                self.active[(dev, skey)] = now_on
                self.emit(sid, code if now_on else code_ok, 1 if now_on else 0)
                if self.log:
                    self.log.warn('ALARM', f'{icon} {dev}: {label} '
                                           f'{"WYKRYTE" if now_on else "ustąpiło"}')

    def snapshot(self):
        """[(sid, code, value)] dla dump_anom / ansnap — aktywne alarmy (reconcyliacja z supem)."""
        out = []
        for (dev, skey), on in self.active.items():
            if not on:
                continue
            sid = self.disc.short_ids.get(dev)
            if sid is None:
                continue
            code = next((c for k, c, _o, _i, _l in self.CAPS if k == skey), None)
            if code:
                out.append((sid, code, 1))
        return out
