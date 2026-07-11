"""ChannelArbiter — arbitraż półdupleksowego kanału LoRa dla NIEZAWODNYCH transferów-plików.

PROBLEM 1 (wolumen vs transfer): transfer bulk (kalendarz/mapa/anomalie) = stop-and-wait per chunk.
Gdy w trakcie inne wątki (b/ab/hb/ping/disc) nadają, psują naprzemienność chunk↔cack na half-duplex
→ cack ginie → transfer stoi. ROZWIĄZANIE: gdy transfer aktywny, `busy()`=True → batchery `b`/`ab`
CZEKAJą (guard w flush). Nadawca odnawia `hold()` przy każdym chunku; odbiorca trzyma po *_begin.

PROBLEM 2 (transfer vs transfer): DWA transfery naraz (mapa w górę + kalendarz w dół, albo devmap+ansnap
przy starcie) INTERLEAVUJą chunki na jednej antenie → oba retransmitują, oba degradują. ROZWIĄZANIE:
SERIALIZACJA — `acquire(owner)` blokuje aż poprzedni transfer zwolni kanał (`release`), dopiero wtedy
drugi transfer wysyła swój *_begin. Jedna antena = i tak muszą iść po kolei; tu wymuszamy to jawnie.

Bezpieczeństwo: hold ma deadline w przyszłości; gdy owner-transfer padnie bez release, `_until` wygasa
i kanał sam się zwalnia (acquire budzi się na timeout). Cacki/pong NIGDY nie wstrzymywane (odblokowują
transfer) — arbiter dotyczy wolumenu (busy) i serializacji transferów (acquire), nie ACK-ów.
"""
import threading
import time


class ChannelArbiter:
    def __init__(self, logger=None):
        self._until = 0.0
        self._owner = None
        self._cv = threading.Condition()     # lock + wait/notify dla serializacji transferów
        self.log = logger

    def hold(self, seconds, owner=None):
        """Zajmij/odnów kanał na `seconds` (odnawialne). Wołane przez transfer-plik przy każdym chunku.
        Nie zmienia ownera gdy owner=None (renew w trakcie transferu)."""
        with self._cv:
            self._until = max(self._until, time.monotonic() + seconds)
            if owner:
                self._owner = owner

    def acquire(self, owner, hold_seconds, max_wait=300.0):
        """SERIALIZACJA transfer-vs-transfer: czekaj aż kanał wolny (żaden inny transfer aktywny),
        potem przejmij jako `owner` + hold. Zwraca True gdy przejęto, False gdy timeout.
        Wolny = deadline minął (poprzedni padł) LUB brak ownera LUB owner to my."""
        deadline = time.monotonic() + max_wait
        with self._cv:
            while True:
                now = time.monotonic()
                free = (now >= self._until) or (self._owner is None) or (self._owner == owner)
                if free:
                    self._owner = owner
                    self._until = max(self._until, now + hold_seconds)
                    return True
                remaining = deadline - now
                if remaining <= 0:
                    return False
                # obudź się najpóźniej gdy deadline mija albo gdy bieżący hold wygasa
                wait_for = min(remaining, max(0.1, self._until - now))
                self._cv.wait(wait_for)

    def release(self, owner=None):
        """Zwolnij kanał (koniec transferu). Budzi czekające transfery (acquire)."""
        with self._cv:
            if owner is None or self._owner == owner:
                self._until = 0.0
                self._owner = None
                self._cv.notify_all()

    def busy(self):
        """Czy trwa transfer-plik (kanał zajęty dla wolumenu b/ab)."""
        with self._cv:
            return time.monotonic() < self._until

    def owner(self):
        with self._cv:
            return self._owner if time.monotonic() < self._until else None
