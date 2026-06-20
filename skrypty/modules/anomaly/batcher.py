"""AnomalyBatcher — gateway side (step 5): zbiera anomalie i wysyła zbiorczo `ab` P2.

Wzorzec v38: one-shot per (sid, kategoria) z deduplikacją w oknie flush → bufor → flush co
`interval` s → `ab` P2 `{t:ab,g,ts,d:[[sid,code,val],...]}`, split gdy ramka > max_payload.

Dedup po (sid, kategoria) — gdy w jednym oknie urządzenie pójdzie offline (`do`) i wróci
(`dn`), wygrywa OSTATNI kod → zero spamu. Wpis `[sid,code]` (bez wartości) lub `[sid,code,val]`.

Kody → kategorie (CLAUDE.md Short JSON):
  offline: do/dn | battery: lb/cb/bo | stagnation: sg/sc
  temp: th/tl/to | hum: hh/hl/ho | smoke: sk | water: wl
"""
import json
import threading
import time

CODE_CAT = {
    "do": "offline", "dn": "offline",
    "lb": "battery", "cb": "battery", "bo": "battery",
    "sg": "stagnation", "sc": "stagnation",
    "th": "temp", "tl": "temp", "to": "temp",
    "hh": "hum", "hl": "hum", "ho": "hum",
    "sk": "smoke", "wl": "water",
}


class AnomalyBatcher:
    def __init__(self, gw_id, send_fn, interval=30, max_payload=150, logger=None):
        self.gw_id = gw_id
        self.send = send_fn              # callable(dict) → LoRa
        self.interval = interval
        self.max_payload = max_payload
        self.log = logger
        self.buffer = {}                 # (sid, kategoria) → [sid, code, val?]
        self.lock = threading.Lock()
        self.running = False

    # ── enqueue (z silników anomalii) ───────────────────
    def add(self, sid, code, value=None):
        if sid is None:
            return
        cat = CODE_CAT.get(code, "other")
        entry = [sid, code] if value is None else [sid, code, value]
        with self.lock:
            self.buffer[(sid, cat)] = entry

    # ── lifecycle ───────────────────────────────────────
    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._loop, daemon=True, name="anomaly-flush").start()
        if self.log:
            self.log.info('ANOM', f'🚨 AnomalyBatcher start (flush co {self.interval}s)')

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            time.sleep(self.interval)
            try:
                self.flush()
            except Exception as e:
                if self.log:
                    self.log.error('ANOM', f'flush: {e}')

    # ── flush + split ───────────────────────────────────
    def _frame_len(self, entries):
        return len(json.dumps({"t": "ab", "g": self.gw_id, "ts": 9999999999, "d": entries},
                              separators=(',', ':')))

    def _split(self, entries):
        """Greedy: pakuj wpisy aż ramka < max_payload."""
        chunks, cur = [], []
        for e in entries:
            if cur and self._frame_len(cur + [e]) > self.max_payload:
                chunks.append(cur)
                cur = []
            cur.append(e)
        if cur:
            chunks.append(cur)
        return chunks

    def flush(self):
        with self.lock:
            entries = list(self.buffer.values())
            self.buffer.clear()
        if not entries:
            return 0
        n = 0
        for chunk in self._split(entries):
            self.send({"t": "ab", "g": self.gw_id, "ts": int(time.time()), "d": chunk})
            n += 1
        if self.log:
            self.log.info('ANOM', f'📤 [AB] {len(entries)} anomalii → {n} pkt(s)')
        return n

    def pending(self):
        with self.lock:
            return list(self.buffer.values())
