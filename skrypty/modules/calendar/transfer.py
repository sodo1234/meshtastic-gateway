"""CalendarTransfer — chunkowany, NIEZAWODNY transfer harmonogramu przez LoRa (STEP 4 / krok 16).

Per-chunk ACK (stop-and-wait) — odporny na stratne łącze. ACK stosujemy TYLKO tu (transfer
zserializowanego/skompresowanego harmonogramu): każdy chunk potwierdzany osobnym `cal_cack`;
nadawca retransmituje dany chunk aż do potwierdzenia (lub limitu), dopiero potem następny.
Na końcu `cal_end` + finalny `cal_ack` (weryfikacja CRC całości).

Protokół:  cal_begin(tid,n,crc) → [ cal_chunk(s,d) ↔ cal_cack(s) ]×n → cal_end(tid) → cal_ack(ok)
  - serializacja: json(slots) → zlib(9) → base64 → chunki po chunk_size
  - CRC = md5(b64)[:4]; finalny cal_ack(ok=1) potwierdza poprawne złożenie
Slots = format compact [[start_min,dur_min,mode],...] z ScheduleManager.
"""
import base64
import hashlib
import json
import random
import string
import threading
import time
import zlib


class CalendarTransfer:
    def __init__(self, send_fn, chunk_size=90, chunk_delay=3.0,
                 on_received=None, logger=None,
                 chunk_ack_timeout=12.0, chunk_retries=6, end_retries=4):
        self.send = send_fn              # callable(dict) → wyślij ramkę LoRa
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay   # odstęp startowy / cooldown LoRa
        self.on_received = on_received   # callback(gw, direction, slots) po udanym odbiorze
        self.log = logger
        self.chunk_ack_timeout = chunk_ack_timeout   # ile czekać na cal_cack zanim retransmit
        self.chunk_retries = chunk_retries           # ile retransmisji chunku
        self.end_retries = end_retries               # ile retransmisji cal_end (finalny ACK)
        self.incoming = {}
        self.outgoing = {}
        self.completed = {}              # tid → ts ukończonych odbiorów (idempotentny re-ACK)
        self.lock = threading.Lock()

    # ── helpers ─────────────────────────────────────────
    @staticmethod
    def gen_tid():
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))

    @staticmethod
    def serialize(slots):
        raw = json.dumps(slots, separators=(',', ':')).encode()
        return base64.b64encode(zlib.compress(raw, 9)).decode()

    @staticmethod
    def deserialize(b64):
        return json.loads(zlib.decompress(base64.b64decode(b64)))

    @staticmethod
    def crc16(data):
        return hashlib.md5(data.encode()).hexdigest()[:4]

    def _log(self, lvl, tag, msg):
        if self.log:
            getattr(self.log, lvl, self.log.info)(tag, msg)

    # ── wysyłka (master → odbiorca), stop-and-wait per chunk ────
    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots)
        crc = self.crc16(b64)
        chunks = [b64[i:i + self.chunk_size] for i in range(0, len(b64), self.chunk_size)] or ['']
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'dir': direction,
                                  'crc': crc, 'ts': time.time(), 'acked': set(), 'done': False}
        self.send({"t": "cal_begin", "tid": tid, "g": gw, "dir": direction,
                   "n": len(chunks), "crc": crc})
        self._log('info', 'SYNC', f"📤 Begin {direction} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")

        def _wait_flag(check):
            waited = 0.0
            while waited < self.chunk_ack_timeout:
                time.sleep(0.4)
                waited += 0.4
                with self.lock:
                    info = self.outgoing.get(tid)
                    if info is None:
                        return None              # anulowane
                    if check(info):
                        return True
            return False

        def _run():
            time.sleep(self.chunk_delay)
            for i, chunk in enumerate(chunks):
                ok = False
                for attempt in range(self.chunk_retries + 1):
                    self.send({"t": "cal_chunk", "tid": tid, "s": i, "d": chunk})
                    res = _wait_flag(lambda inf: i in inf['acked'])
                    if res is None:
                        return
                    if res:
                        ok = True
                        break
                    if attempt < self.chunk_retries:
                        self._log('warn', 'SYNC', f"🔁 chunk {i}/{len(chunks)} brak cack tid={tid} — "
                                  f"retry {attempt + 1}/{self.chunk_retries}")
                if not ok:
                    self._log('error', 'SYNC', f"❌ chunk {i} tid={tid} bez potwierdzenia → przerwane")
                    with self.lock:
                        self.outgoing.pop(tid, None)
                    return
            # wszystkie chunki potwierdzone → cal_end + finalny ACK (CRC).
            # cal_end niesie REDUNDANTNIE n/crc/g/dir — gdy cal_begin zginął na RF,
            # odbiorca odtwarza wpis z chunków-sierot i finalizuje bez nowego round-tripu.
            for attempt in range(self.end_retries):
                self.send({"t": "cal_end", "tid": tid, "n": len(chunks),
                           "crc": crc, "g": gw, "dir": direction})
                res = _wait_flag(lambda inf: inf.get('done'))
                if res is None or res:
                    return
                self._log('warn', 'SYNC', f"⚠️ cal_end retry tid={tid} {attempt + 1}/{self.end_retries}")
            self._log('error', 'SYNC', f"❌ tid={tid} brak finalnego ACK po {self.end_retries} cal_end — rezygnacja")
            with self.lock:
                self.outgoing.pop(tid, None)

        threading.Thread(target=_run, daemon=True, name=f"cal-tx-{tid}").start()
        return tid

    def handle_cack(self, data):
        """Per-chunk ACK od odbiorcy → odblokuj wysyłkę kolejnego chunku."""
        tid = data.get('tid')
        s = data.get('s')
        with self.lock:
            info = self.outgoing.get(tid)
            if info is not None and s is not None:
                info['acked'].add(s)

    # ── odbiór ──────────────────────────────────────────
    def handle_begin(self, data):
        tid = data.get('tid')
        with self.lock:
            self.incoming[tid] = {'chunks': {}, 'total': data.get('n', 0),
                                  'crc': data.get('crc', ''), 'gw': data.get('g', '?'),
                                  'dir': data.get('dir', 'push'), 'ts': time.time()}
        self._log('info', 'SYNC', f"📥 Begin {data.get('dir')} [{data.get('g')}] tid={tid} expect={data.get('n')}")

    def handle_chunk(self, data):
        tid = data.get('tid')
        s = data.get('s', 0)
        with self.lock:
            if tid not in self.incoming:
                # cal_begin zgubiony na RF → odtwórz wpis z chunku-sieroty.
                # total/crc/gw/dir uzupełni cal_end (niesie je redundantnie). Bez tego
                # chunk byłby porzucony, a cal_cack i tak wysłany = nadawca myśli że dotarł.
                self.incoming[tid] = {'chunks': {}, 'total': None, 'crc': None,
                                      'gw': '?', 'dir': 'push', 'ts': time.time(),
                                      'no_begin': True}
                self._log('warn', 'SYNC', f"⚠️ chunk {s} tid={tid} bez cal_begin (zgubiony na RF) → odtwarzam z cal_end")
            self.incoming[tid]['chunks'][s] = data.get('d', '')
            self.incoming[tid]['ts'] = time.time()
        # PER-CHUNK ACK — potwierdź odbiór tego chunku (idempotentnie, też dla retransmisji)
        self.send({"t": "cal_cack", "tid": tid, "s": s})

    def handle_end(self, data):
        tid = data.get('tid')
        with self.lock:
            info = self.incoming.get(tid)
            done_before = tid in self.completed
            if not info and not done_before and data.get('n') is not None:
                # cal_begin ORAZ wszystkie chunki zgubione, ale cal_end niesie n/crc → odtwórz
                # pusty wpis, by handle_end policzył braki i poprawnie NACK-nął cały transfer.
                info = {'chunks': {}, 'total': None, 'crc': None,
                        'gw': '?', 'dir': 'push', 'ts': time.time(), 'no_begin': True}
                self.incoming[tid] = info
        if not info:
            # Już złożone wcześniej: finalny cal_ack mógł zginąć na RF, nadawca retransmituje
            # cal_end → re-ACK idempotentnie (bez tego master "rezygnuje" mimo poprawnego odbioru).
            if done_before:
                self.send({"t": "cal_ack", "tid": tid, "ok": 1, "miss": []})
            return None
        # Begin zgubiony → uzupełnij total/crc/gw/dir z redundantnych pól cal_end.
        if info.get('total') is None:
            info['total'] = data.get('n', 0)
            info['crc'] = data.get('crc', '')
        if info.get('gw', '?') == '?' and data.get('g'):
            info['gw'] = data['g']
            info['dir'] = data.get('dir', info['dir'])
        missing = [i for i in range(info['total']) if i not in info['chunks']]
        if missing:
            self._log('warn', 'SYNC', f"⚠️ tid={tid} brakujące chunki: {missing[:5]}")
            with self.lock:
                self.incoming[tid]['ts'] = time.time()
            self.send({"t": "cal_ack", "tid": tid, "ok": 0, "miss": missing[:5]})
            return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            self._log('warn', 'SYNC', f"⚠️ tid={tid} CRC mismatch")
            with self.lock:
                self.incoming[tid]['ts'] = time.time()
            self.send({"t": "cal_ack", "tid": tid, "ok": 0, "miss": []})
            return None
        with self.lock:
            self.incoming.pop(tid, None)
            self.completed[tid] = time.time()
            if len(self.completed) > 32:                  # przytnij najstarsze (bounded)
                for old in sorted(self.completed, key=self.completed.get)[:-16]:
                    self.completed.pop(old, None)
        self.send({"t": "cal_ack", "tid": tid, "ok": 1, "miss": []})
        try:
            slots = self.deserialize(b64)
        except Exception as e:
            self._log('error', 'SYNC', f"deserialize: {e}")
            return None
        self._log('info', 'SYNC', f"✅ Odebrano {len(slots)} slotów [{info['gw']}] dir={info['dir']}")
        if self.on_received:
            self.on_received(info['gw'], info['dir'], slots)
        return info['gw'], info['dir'], slots

    def handle_ack(self, data):
        """Finalny ACK (po cal_end). ok=1 → koniec. ok=0 → fallback retransmit miss + cal_end."""
        tid = data.get('tid')
        ok = data.get('ok', 0)
        miss = data.get('miss', [])
        with self.lock:
            info = self.outgoing.get(tid)
            if not info:
                return
            if ok == 1:
                info['done'] = True
                self.outgoing.pop(tid, None)
                self._log('info', 'SYNC', f"✅ ACK ok tid={tid}")
                return
            chunks = info['chunks']
            crc = info.get('crc', '')
            gw = info.get('gw', '?')
            direction = info.get('dir', 'push')
        self._log('warn', 'SYNC', f"🔁 NACK tid={tid} retransmit {miss}")
        for i in miss:
            if 0 <= i < len(chunks):
                self.send({"t": "cal_chunk", "tid": tid, "s": i, "d": chunks[i]})
        self.send({"t": "cal_end", "tid": tid, "n": len(chunks),
                   "crc": crc, "g": gw, "dir": direction})

    # ── router (z dispatchera app) ──────────────────────
    def dispatch(self, data):
        t = data.get('t')
        if t == 'cal_begin':
            self.handle_begin(data)
        elif t == 'cal_chunk':
            self.handle_chunk(data)
        elif t == 'cal_end':
            self.handle_end(data)
        elif t == 'cal_ack':
            self.handle_ack(data)
        elif t == 'cal_cack':
            self.handle_cack(data)
