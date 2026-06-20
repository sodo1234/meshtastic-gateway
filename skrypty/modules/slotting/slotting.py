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
        self.gw_id = gw_id
        self.num_slots = int(num_slots or max(len(gws), 1))
        self.slot_seconds = float(slot_seconds)
        self.cycle = self.slot_seconds * self.num_slots
        try:
            self.index = gws.index(gw_id) % self.num_slots
        except ValueError:
            self.index = 0
        self.clock = clock
        self.log = logger

    def window(self):
        """(start_sec, end_sec) okna tej bramki w obrębie cyklu."""
        s = self.index * self.slot_seconds
        return s, s + self.slot_seconds

    def position(self, now=None):
        """Pozycja [0, cycle) w bieżącym cyklu."""
        now = self.clock() if now is None else now
        return now % self.cycle

    def in_slot(self, now=None):
        s, e = self.window()
        return s <= self.position(now) < e

    def seconds_until_slot(self, now=None):
        """0 jeśli teraz w oknie; inaczej sekundy do najbliższego startu okna."""
        if self.in_slot(now):
            return 0.0
        pos = self.position(now)
        start = self.window()[0]
        delta = start - pos
        if delta < 0:
            delta += self.cycle          # okno już minęło w tym cyklu → następny cykl
        return delta

    def wait_my_turn(self, now=None):
        """Blokująco poczekaj do swojego okna (realny sleep). Zwraca ile czekano."""
        wait = self.seconds_until_slot(now)
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
