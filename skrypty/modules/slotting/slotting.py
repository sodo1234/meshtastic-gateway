"""Anti-collision: SlotScheduler (bramka TX-okno) + SafeWindow (supervisor po-RX).

Zegar wstrzykiwany (clock=time.time) → testowalne bez realnego czasu.
"""
import threading
import time


class SlotScheduler:
    """Bramka nadaje tylko w swoim oknie [idx*slot, (idx+1)*slot) wewnątrz cyklu
    cycle = num_slots*slot_seconds. Domyślnie 3 sloty × 20 s = 60 s (G1:0-19, G2:20-39, G3:40-59).

    gateways = lista ID w kolejności → indeks slotu. Nieznana bramka → slot 0 (defensywnie).
    """

    def __init__(self, gw_id, gateways=None, slot_seconds=20, num_slots=None,
                 clock=time.time, logger=None):
        gws = list(gateways or [gw_id])
        self._gws = gws
        self.gw_id = gw_id
        # slot_seconds: liczba (wszystkie sloty rowne) ALBO dict {bramka: sekundy} — np. G1 40 s (93%
        # ruchu), G2/G3 po 10 s. Cykl = suma dlugosci; kolejnosc `gateways` = kolejnosc okien.
        if isinstance(slot_seconds, dict):
            lens = [float(slot_seconds.get(g, 10)) for g in gws]
        else:
            lens = [float(slot_seconds)] * len(gws)
        self.num_slots = int(num_slots or max(len(gws), 1))
        self.lens = lens
        self.cycle = sum(lens)
        try:
            self.index = gws.index(gw_id) % self.num_slots
        except ValueError:
            self.index = 0
        self._start = sum(lens[:self.index])
        self.slot_seconds = lens[self.index] if lens else 20.0
        self.clock = clock
        self.log = logger

    def window(self):
        """(start_sec, end_sec) okna tej bramki w obrębie cyklu."""
        return self._start, self._start + self.slot_seconds

    def position(self, now=None):
        """Pozycja [0, cycle) w bieżącym cyklu."""
        now = self.clock() if now is None else now
        return now % self.cycle

    def in_slot(self, now=None):
        s, e = self.window()
        return s <= self.position(now) < e

    def seconds_until_slot(self, now=None, min_left=0.0):
        """0 jeśli teraz w oknie (i zostało ≥ min_left s); inaczej sekundy do najbliższego startu okna."""
        if self.in_slot(now):
            left = self.window()[1] - self.position(now)
            if min_left <= 0 or left >= min_left:
                return 0.0
            return left + self.cycle - self.slot_seconds       # za późno w oknie → następny cykl
        pos = self.position(now)
        start = self.window()[0]
        delta = start - pos
        if delta < 0:
            delta += self.cycle          # okno już minęło w tym cyklu → następny cykl
        return delta

    def fast_lane_wait(self, active, now=None, guard=2.0):
        """Szybki pas BRAMKI (2026-10-14): od razu w swoim i w wolnym slocie, ale NIE w slocie innej
        aktywnej bramki. Powód: ukryty nadajnik — G2 nie słyszy G1 (słyszą się tylko przez supa), więc
        LBT nie chroni; zmierzone v12.8: `param_upd` G2 w slocie G1 zderzył się z HB G1 i zginął.
        `guard` = czas antenowy ramki: nie zaczynaj, jeśli cudzy slot zaczyna się za < guard s.
        Zwraca sekundy oczekiwania (0 = nadawaj)."""
        pos = self.position(now)
        start = 0.0
        windows = []
        for i, ln in enumerate(self.lens):
            windows.append((i, start, start + ln))
            start += ln
        foreign = {i for i, (g) in enumerate(self._gws) if g != self.gw_id and g in set(active)}
        for i, a0, a1 in windows:
            if a0 <= pos < a1:
                if i in foreign:
                    return a1 - pos                      # trwa cudzy slot → do jego końca
                nxt = (i + 1) % len(windows)
                if nxt in foreign and a1 - pos < guard:  # cudzy slot tuż-tuż → przeczekaj go
                    return (a1 - pos) + self.lens[nxt]
                return 0.0
        return 0.0

    def frame_capacity(self, gap, now=None):
        """Ile ramek (co `gap` s) zmiesci sie w biezacym oknie; poza oknem — w calym nastepnym."""
        left = (self.window()[1] - self.position(now)) if self.in_slot(now) else self.slot_seconds
        return max(1, int(left // max(gap, 0.1)))

    def wait_my_turn(self, now=None, min_left=0.0):
        """Blokująco poczekaj do swojego okna (realny sleep). Zwraca ile czekano."""
        wait = self.seconds_until_slot(now, min_left)
        if wait > 0:
            if self.log:
                self.log.debug('SLOT', f"⏳ {self.gw_id} czeka {wait:.1f}s na slot {self.index}")
            time.sleep(wait)
        return wait


class SafeWindow:
    """Supervisor: wysyłaj do bramki tylko gdy niedawno coś od niej przyszło
    (half-duplex — nie nadawaj gdy bramka prawdopodobnie nadaje/odbiera coś innego).
    mark_rx() przy każdej ramce LoRa z bramki; can_send() przed TX do niej.
    """

    def __init__(self, window_seconds=10.0, clock=time.time, logger=None):
        self.window = float(window_seconds)
        self.clock = clock
        self.log = logger
        self._last_rx = {}
        self._lock = threading.Lock()

    def mark_rx(self, gw, now=None):
        with self._lock:
            self._last_rx[gw] = self.clock() if now is None else now

    def seconds_since_rx(self, gw, now=None):
        now = self.clock() if now is None else now
        with self._lock:
            last = self._last_rx.get(gw)
        return None if last is None else (now - last)

    def can_send(self, gw, now=None):
        """True jeśli usłyszeliśmy bramkę w oknie `window`. Nigdy-słyszana → False."""
        dt = self.seconds_since_rx(gw, now)
        return dt is not None and dt <= self.window
