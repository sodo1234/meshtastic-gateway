"""SupervisorLinkProbe — F2: aktywny ping/pong bramka↔supervisor (wskaźnik jakości łącza LoRa).

Model CLAUDE.md jest PASYWNY (supervisor milczy, minimalizacja ruchu) — ten probe NIE zastępuje
tego podejścia, tylko dokłada siatkę bezpieczeństwa NA CISZĘ: bramka co P4 minut wysyła `sup_ping`
i czeka T4 sekund na `sup_pong`; brak odpowiedzi po PR próbach → link OFFLINE (lost_pongs++).
KAŻDA wiadomość odebrana OD supervisora (nie tylko pong) liczy się jako dowód życia — `note_rx()`
wołane z harnessu na każdym odbiorze LoRa resetuje probe do ONLINE, więc aktywny ping realnie
strzela tylko wtedy, gdy supervisor faktycznie milczy dłużej niż interwał P4 (rzadkość w praktyce).

Parametry (P4/T4/PR) czytane NA ŻYWO przez `get_params()` z ParamSync — strojenie z dashboardu
bez restartu (ten sam wzorzec co progi anomalii TH/TL/HH/HL w test_step5_anomaly.py).
"""
import json
import threading
import time


class SupervisorLinkProbe:
    def __init__(self, gw_id, lora, get_params, on_state=None, logger=None, tick=1.0, clock=None):
        """
        gw_id:      identyfikator tej bramki (do pakietu sup_ping/sup_pong)
        lora:       transport LoRa (musi mieć .send(str))
        get_params: callable() → (interval_min, timeout_s, retries) — LIVE z ParamSync (P4/T4/PR)
        on_state:   callable(online: bool, lost_pongs: int) — wołane PRZY ZMIANIE stanu online
        tick:       granulacja pętli tła (s)
        clock:      callable() → float (domyślnie time.time) — wstrzykiwalny zegar, testowalność
        """
        self.gw_id = gw_id
        self.lora = lora
        self.get_params = get_params
        self.on_state = on_state
        self.log = logger
        self.tick = tick
        self._clock = clock or time.time
        self.online = True                 # optymistyczny start (cichy start — brak dowodu offline)
        self.lost_pongs = 0
        self._awaiting = False
        self._sent_ts = 0.0
        self._retries_left = 0
        self._next_ping_ts = 0.0
        self._lock = threading.Lock()
        self.running = False

    # ── parametry live (P4/T4/PR z ParamSync, z bezpiecznym fallbackiem) ──
    def _params(self):
        try:
            interval_min, timeout_s, retries = self.get_params()
        except Exception:
            interval_min, timeout_s, retries = 15, 25, 2
        interval_min = interval_min if interval_min else 15
        timeout_s = timeout_s if timeout_s else 25
        retries = retries if retries is not None else 2
        return interval_min, timeout_s, retries

    # ── lifecycle ───────────────────────────────────────
    def start(self):
        if self.running:
            return
        self.running = True
        interval_min, _, _ = self._params()
        with self._lock:
            self._next_ping_ts = self._clock() + interval_min * 60
        threading.Thread(target=self._loop, daemon=True, name='sup-link-probe').start()
        if self.log:
            self.log.info('LINK', f'📡 SupervisorLinkProbe start (interwał {interval_min}min)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.tick)
            self.check()

    # ── jeden krok automatu — wołany z pętli LUB bezpośrednio (testy z fake clock) ──
    def check(self, now=None):
        now = now if now is not None else self._clock()
        with self._lock:
            awaiting = self._awaiting
            sent_ts = self._sent_ts
            retries_left = self._retries_left
            next_ping = self._next_ping_ts
        _, timeout_s, _ = self._params()
        if awaiting:
            if now - sent_ts < timeout_s:
                return
            if retries_left > 0:
                with self._lock:
                    self._retries_left -= 1
                    self._sent_ts = now
                self._send_ping()
            else:
                self._mark_offline()
                interval_min, _, _ = self._params()
                with self._lock:
                    self._awaiting = False
                    self._next_ping_ts = now + interval_min * 60
        else:
            if now >= next_ping:
                self._start_probe(now)

    def _start_probe(self, now):
        _, _, retries = self._params()
        with self._lock:
            self._awaiting = True
            self._sent_ts = now
            self._retries_left = retries
        self._send_ping()

    def _send_ping(self):
        try:
            self.lora.send(json.dumps({"t": "sup_ping", "g": self.gw_id}, separators=(',', ':')))
            if self.log:
                self.log.info('LINK', f'📡 sup_ping → supervisor ({self.gw_id})')
        except Exception as e:
            if self.log:
                self.log.warn('LINK', f'sup_ping send: {e}')

    def _mark_offline(self):
        with self._lock:
            self.lost_pongs += 1
            lost = self.lost_pongs
            self.online = False
        if self.log:
            self.log.warn('LINK', f'💀 sup_link OFFLINE (brak sup_pong, lost_pongs={lost})')
        if self.on_state:
            self.on_state(False, lost)

    # ── wejście: pong / dowolny ruch od supervisora ──────
    def handle_sup_pong(self, data=None):
        """dispatcher 'sup_pong' — supervisor potwierdził żywotność."""
        self._go_online()

    def note_rx(self):
        """KAŻDA wiadomość od supervisora (harness w on_lora) = dowód życia → reset probe."""
        self._go_online()

    def _go_online(self):
        now = self._clock()
        with self._lock:
            was_online = self.online
            self.online = True
            self._awaiting = False
            interval_min, _, _ = self._params()
            self._next_ping_ts = now + interval_min * 60
            lost = self.lost_pongs
        if not was_online and self.on_state:
            self.on_state(True, lost)

    def status(self):
        with self._lock:
            return {'online': self.online, 'lost_pongs': self.lost_pongs,
                    'awaiting': self._awaiting}
