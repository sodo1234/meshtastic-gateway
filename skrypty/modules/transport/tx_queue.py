"""PriorityTxQueue — jedna serializowana kolejka nadawania LoRa z priorytetami + anti-starvation.

PROBLEM (zdiagnozowany na żywo): różne podsystemy (data `b`, anomalie `ab`, heartbeat `hb`,
chunki rt/cal, potwierdzenia rt_cack/cal_cack) wołały `lora.send` NIEZALEŻNIE → wyścig na
half-duplex → potwierdzenia (cack) bulk-transferu ginęły → devmap/kalendarz nigdy się nie
kończyły → pętla = spam.

ROZWIĄZANIE: jeden wątek-drainer, JEDNO inner.send() naraz + cooldown. Priorytet wg pola `t`.
KLUCZ 1 — potwierdzenia (P0) wyprzedzają wszystko → odblokowują transfery bulk.
KLUCZ 2 — ANTI-STARVATION: im dłużej pakiet czeka, tym pilniejszy (efektywny priorytet spada
co `aging_secs`), CLAMP do min 1 → bulk (P4) czekający 30s+ przebija świeże `b`/`ab`, ale NIGDY
nie wyprzedza cacków P0. Bez tego kolejka głodziła devmap (P4) pod ciągłym `b`/`ab` i bulk nie
dochodził. Z agingiem chunki bulk sączą się miarowo między wolumen, transfer się kończy.

Drop-in nad LoraTransport: ma .send(text) jak `lora`; reszta metod delegowana (__getattr__).
Slot (SlotScheduler) zostaje ORTOGONALNY: gate'uje KIEDY `b` trafia do kolejki (okno bramki),
kolejka serializuje nadawanie W bramce.
"""
import json
import threading
import time

# Priorytet wg typu pakietu (niżej = pilniejsze). Dobór z audytu spamu:
_PRIO = {
    # P0 — reaktywne + POTWIERDZENIA: zawsze pierwsze (odblokowują transfery bulk, sterowanie)
    'rt_cack': 0, 'cal_cack': 0, 'rt_ack': 0, 'cal_ack': 0,
    'pong': 0, 'st': 0, 'ack': 0, 'sync': 0, 'hb': 0, 'vsw_st': 0,
    'disc_meta': 0, 'disc_vio': 0, 'param_upd': 0,
    # P1 — sterowanie / żądania + BULK chunki. Bulk to stop-and-wait (1 chunk naraz w kolejce),
    # więc P1 = „transfer raz rozpoczęty kończy się szybko" (bije wolumen ab/b), ale NIGDY nie
    # wyprzedza cacków P0. Jeden transfer bulk naraz pilnuje bulk-mutex (patrz start_send).
    'cmd': 1, 'vsw': 1, 'params': 1, 'cfg': 1, 'req': 1, 'cal_req': 1, 'disc': 1,
    'rt_begin': 1, 'rt_chunk': 1, 'rt_end': 1,
    'cal_begin': 1, 'cal_chunk': 1, 'cal_end': 1,
    # P2 — anomalie
    'ab': 2, 'ac_b': 2,
    # P3 — monitored data (wolumen)
    'b': 3,
}
_DEFAULT_PRIO = 3


def _type_of(text):
    """Tani wyciąg 't' z ramki JSON {"t":"xxx",...} (separators=(',',':'))."""
    try:
        i = text.index('"t":"') + 5
        return text[i:text.index('"', i)]
    except Exception:
        return ''


class PriorityTxQueue:
    def __init__(self, inner, cooldown=3.0, logger=None, prio_map=None, aging_secs=6.0,
                 listen_gap=5.0):
        self._inner = inner                 # LoraTransport — ma .send(text) + connect/receive/...
        self.cooldown = cooldown
        self.log = logger
        self.aging_secs = aging_secs        # co tyle s oczekiwania: -1 do efektywnego priorytetu
        # OKNO NASŁUCHU: po wysłaniu chunka bulk (P1) wstrzymaj nadawanie nie-P0 na listen_gap s,
        # żeby half-duplex mógł ODEBRAĆ cack (inaczej bramka nadaje ab/b zaraz po chunku → głucha →
        # cack ginie → transfer stoi). Cacki (P0) nadal wychodzą natychmiast (np. gdy sami odbieramy).
        self.listen_gap = listen_gap
        self._listen_until = 0.0
        self._prio = dict(_PRIO)
        if prio_map:
            self._prio.update(prio_map)
        self._items = []                    # [base_prio, seq, enqueue_ts, text]
        self._seq = 0
        self._cv = threading.Condition()
        self._stop = False
        self._t = threading.Thread(target=self._drain, daemon=True, name='tx-queue')
        self._t.start()

    def priority_of(self, text):
        return self._prio.get(_type_of(text), _DEFAULT_PRIO)

    # drop-in za lora.send(text)
    def send(self, text):
        with self._cv:
            self._items.append([self.priority_of(text), self._seq, time.monotonic(), text])
            self._seq += 1
            self._cv.notify()

    # jawne API dla podsystemów (np. anomalie): enqueue(P2, ramka_dict|text)
    def enqueue(self, priority, frame):
        text = frame if isinstance(frame, str) else json.dumps(frame, separators=(',', ':'))
        with self._cv:
            self._items.append([priority, self._seq, time.monotonic(), text])
            self._seq += 1
            self._cv.notify()

    def _pick_locked(self):
        """Wybierz najpilniejszy z agingiem. eff = clamp(base - age/aging, min=1) — bulk floatuje
        w górę z czasem ale NIGDY nie przebija cacków P0 (=0). Remis → FIFO (seq).
        W oknie nasłuchu (po chunku bulk) przepuszczaj TYLKO P0 — reszta czeka aż okno minie."""
        now = time.monotonic()
        listening = now < self._listen_until
        best, best_key = None, None
        for it in self._items:
            base, seq, ts, _ = it
            if listening and base > 0:
                continue                          # okno nasłuchu: tylko P0 (cacki)
            eff = base - int((now - ts) / self.aging_secs)
            if eff < 1:
                eff = 1 if base > 0 else 0        # P0 zostaje 0; reszta clamp do 1
            key = (eff, seq)
            if best_key is None or key < best_key:
                best, best_key = it, key
        return best

    def _drain(self):
        while True:
            with self._cv:
                it = None
                while not self._stop:
                    it = self._pick_locked()
                    if it is not None:
                        break
                    # nic do wysłania teraz: albo pusto, albo okno nasłuchu blokuje nie-P0.
                    now = time.monotonic()
                    wait_t = None
                    if self._items and now < self._listen_until:
                        wait_t = self._listen_until - now     # obudź gdy okno minie
                    self._cv.wait(timeout=wait_t)
                if self._stop:
                    return
                self._items.remove(it)
                text = it[3]
            try:
                self._inner.send(text)
            except Exception as e:
                if self.log:
                    self.log.error('TXQ', f'send: {e}')
            # po chunku bulk → otwórz okno nasłuchu (cack może wrócić bez zagłuszania)
            if _type_of(text) in ('rt_chunk', 'rt_begin', 'cal_chunk', 'cal_begin'):
                self._listen_until = time.monotonic() + self.listen_gap
            time.sleep(self.cooldown)

    def pending(self):
        with self._cv:
            return len(self._items)

    def stop(self):
        with self._cv:
            self._stop = True
            self._cv.notify_all()

    # deleguj connect/receive/close/... do wewnętrznego transportu
    def __getattr__(self, name):
        return getattr(self._inner, name)
