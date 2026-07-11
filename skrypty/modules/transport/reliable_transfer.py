"""ReliableTransfer — generyczny, niezawodny, SKOMPRESOWANY transfer dowolnego payloadu przez LoRa.

Uogólnienie sprawdzonego CalendarTransfer (krok 16) na DOWOLNĄ treść: harmonogram, snapshot
stanu 250 urządzeń, masowa anomalia (z2m-down → wszystkie offline). Powód: przy 250+ dev i
slocie 20s / cooldown 3s (~6 tx/slot) wysyłanie per-urządzenie NIE mieści się — kompresja
zlib (stany repetytywne → 4-7×) + chunking + per-chunk ACK to jedyna droga dla bulku.

Serializacja: json(payload) → zlib(9) → base64 → chunki po chunk_size.
Protokół (prefix domyślny `rt`):
  rt_begin(tid,kind,n,crc) → [ rt_chunk(s,d) ↔ rt_cack(s) ]×n → rt_end(tid,n,crc,kind) → rt_ack(ok,miss)
Odporność (z CalendarTransfer): stop-and-wait per chunk + retransmit; odtworzenie zgubionego
begina z chunków-sierot (rt_end niesie redundantnie n/crc/kind); idempotentny re-ACK; CRC całości.

`kind` rozróżnia treść u odbiorcy: on_received(gw, kind, payload). Wiele równoległych transferów
(różne tid) bez kolizji. NIE rusza CalendarTransfer (żywy/przetestowany) — to osobny kanał `rt_*`.
"""
import base64
import hashlib
import json
import random
import string
import threading
import time
import zlib


class ReliableTransfer:
    def __init__(self, send_fn, on_received=None, logger=None, prefix="rt",
                 chunk_size=90, chunk_delay=3.0,
                 chunk_ack_timeout=12.0, chunk_retries=6, end_retries=4,
                 arbiter=None, accept_gw=None):
        self.send = send_fn                  # callable(dict) → wyślij ramkę LoRa
        self.on_received = on_received       # callback(gw, kind, payload) po udanym odbiorze
        self.log = logger
        self.p = prefix                      # prefiks typów ('rt' → rt_begin/rt_chunk/...)
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay
        self.chunk_ack_timeout = chunk_ack_timeout
        self.chunk_retries = chunk_retries
        self.end_retries = end_retries
        # ChannelArbiter: gdy transfer aktywny, kanał należy do niego (wolumen b/ab czeka).
        # Nadawca odnawia hold przy każdym chunku; odbiorca trzyma po *_begin/*_chunk.
        self.arbiter = arbiter
        # accept_gw: adresowanie multi-gw na WSPÓŁDZIELONYM kanale (wszyscy słyszą wszystko).
        #   None  → przyjmij transfery od/do dowolnej bramki (supervisor agreguje wszystkie).
        #   'Gx'  → tylko transfery z g==Gx lub g==None (globalny). Transfer cudzej bramki =
        #           IGNORUJEMY całkowicie: brak cack (żeby nie kłamać ACK nadawcy) i brak apply.
        self.accept_gw = accept_gw
        self._foreign = set()                # tid transferów NIE dla nas (z *_begin/*_end)
        self.incoming = {}
        self.outgoing = {}
        self.completed = {}                  # tid → ts (idempotentny re-ACK)
        self.lock = threading.Lock()

    def _hold_channel(self):
        if self.arbiter is not None:
            self.arbiter.hold(self.chunk_ack_timeout + 4.0)

    def _note_foreign(self, tid):
        self._foreign.add(tid)
        if len(self._foreign) > 64:
            self._foreign = set(list(self._foreign)[-32:])

    # ── helpers ─────────────────────────────────────────
    @staticmethod
    def gen_tid():
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))

    @staticmethod
    def serialize(payload):
        raw = json.dumps(payload, separators=(',', ':')).encode()
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

    # ── wysyłka (stop-and-wait per chunk) ───────────────
    def start_send(self, gw, kind, payload):
        b64 = self.serialize(payload)
        crc = self.crc16(b64)
        chunks = [b64[i:i + self.chunk_size] for i in range(0, len(b64), self.chunk_size)] or ['']
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'kind': kind,
                                  'crc': crc, 'ts': time.time(), 'acked': set(), 'done': False}

        def _wait_flag(check, timeout=None):
            budget = timeout if timeout is not None else self.chunk_ack_timeout
            waited = 0.0
            while waited < budget:
                time.sleep(0.4)
                waited += 0.4
                with self.lock:
                    info = self.outgoing.get(tid)
                    if info is None:
                        return None
                    if check(info):
                        return True
            return False

        def _tx_body():
            self.send({"t": f"{self.p}_begin", "tid": tid, "g": gw, "kind": kind,
                       "n": len(chunks), "crc": crc})
            self._log('info', 'XFER', f"📤 Begin kind={kind} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")
            time.sleep(self.chunk_delay)
            # STOP-AND-WAIT per-chunk cack (2026-07-06 PRZYWRÓCONE): niezawodność > liczba wiadomości.
            # Fire-and-NACK zawieszał transfer na stratnym łączu (zgubiony NACK = brak retransmisji chunka).
            # Per-chunk cack: zgubiony chunk = natychmiastowy retry. Koszt (N cacków) płacony RZADKO
            # (mapa/ansnap tylko przy zmianie hasha). „Minimum danych" = plik+hash-gate, nie brak cacków.
            for i, chunk in enumerate(chunks):
                ok = False
                for attempt in range(self.chunk_retries + 1):
                    self._hold_channel()             # kanał należy do transferu (wolumen b/ab czeka)
                    self.send({"t": f"{self.p}_chunk", "tid": tid, "s": i, "d": chunk})
                    res = _wait_flag(lambda inf: i in inf['acked'])
                    if res is None:
                        return
                    if res:
                        ok = True
                        break
                    if attempt < self.chunk_retries:
                        self._log('warn', 'XFER', f"🔁 chunk {i}/{len(chunks)} brak cack tid={tid} — "
                                  f"retry {attempt + 1}/{self.chunk_retries}")
                if not ok:
                    self._log('error', 'XFER', f"❌ chunk {i} tid={tid} bez potwierdzenia → przerwane")
                    with self.lock:
                        self.outgoing.pop(tid, None)
                    return
            for attempt in range(self.end_retries):
                self._hold_channel()
                self.send({"t": f"{self.p}_end", "tid": tid, "n": len(chunks),
                           "crc": crc, "g": gw, "kind": kind})
                res = _wait_flag(lambda inf: inf.get('done'))
                if res is None or res:
                    return
                self._log('warn', 'XFER', f"⚠️ rt_end retry tid={tid} {attempt + 1}/{self.end_retries}")
            self._log('error', 'XFER', f"❌ tid={tid} brak finalnego ACK po {self.end_retries} rt_end — rezygnacja")
            with self.lock:
                self.outgoing.pop(tid, None)

        def _run():
            # SERIALIZACJA transfer-vs-transfer: czekaj aż poprzedni transfer zwolni kanał.
            # Begin wysyłamy DOPIERO po przejęciu (nie zapowiadamy transferu w środek cudzego).
            if self.arbiter is not None:
                if not self.arbiter.acquire(tid, self.chunk_ack_timeout + 4.0):
                    self._log('warn', 'XFER', f"⏳ tid={tid} kind={kind} nie dostał kanału → anuluję (retry później)")
                    with self.lock:
                        self.outgoing.pop(tid, None)
                    return
            try:
                _tx_body()
            finally:
                if self.arbiter is not None:
                    self.arbiter.release(tid)

        threading.Thread(target=_run, daemon=True, name=f"rt-tx-{tid}").start()
        return tid

    def handle_cack(self, data):
        tid = data.get('tid')
        s = data.get('s')
        with self.lock:
            info = self.outgoing.get(tid)
            if info is not None and s is not None:
                info['acked'].add(s)

    # ── odbiór ──────────────────────────────────────────
    def handle_begin(self, data):
        tid = data.get('tid')
        self._hold_channel()                 # kanał zajęty (nawet cudzy transfer → backoff wolumenu)
        if self.accept_gw is not None and data.get('g') not in (None, self.accept_gw):
            self._note_foreign(tid)          # transfer NIE do nas → nie uczestniczymy (brak cack/apply)
            self._log('debug', 'XFER', f"⏭️ begin tid={tid} g={data.get('g')} ≠ {self.accept_gw} → ignoruję")
            return
        with self.lock:
            self.incoming[tid] = {'chunks': {}, 'total': data.get('n', 0),
                                  'crc': data.get('crc', ''), 'gw': data.get('g', '?'),
                                  'kind': data.get('kind', '?'), 'ts': time.time()}
        self._log('info', 'XFER', f"📥 Begin kind={data.get('kind')} [{data.get('g')}] tid={tid} expect={data.get('n')}")

    def handle_chunk(self, data):
        tid = data.get('tid')
        s = data.get('s', 0)
        self._hold_channel()
        if tid in self._foreign:
            return                           # cudzy transfer → NIE cack (unikamy fałszywego ACK nadawcy)
        with self.lock:
            if tid not in self.incoming:
                # begin zgubiony na RF → odtwórz z chunku-sieroty (kind/total/crc z rt_end)
                self.incoming[tid] = {'chunks': {}, 'total': None, 'crc': None,
                                      'gw': '?', 'kind': '?', 'ts': time.time(), 'no_begin': True}
                self._log('warn', 'XFER', f"⚠️ chunk {s} tid={tid} bez rt_begin (zgubiony) → odtwarzam z rt_end")
            self.incoming[tid]['chunks'][s] = data.get('d', '')
            self.incoming[tid]['ts'] = time.time()
        self.send({"t": f"{self.p}_cack", "tid": tid, "s": s})    # per-chunk ACK (idempotentny, niezawodność)

    def handle_end(self, data):
        tid = data.get('tid')
        if tid in self._foreign:
            return                           # cudzy transfer → nie finalizuj, nie ACK-uj
        if self.accept_gw is not None and data.get('g') and data['g'] != self.accept_gw:
            self._note_foreign(tid)          # begin zgubiony, ale end zdradza obcą bramkę → odrzuć
            with self.lock:
                self.incoming.pop(tid, None)
            return
        with self.lock:
            info = self.incoming.get(tid)
            done_before = tid in self.completed
            if not info and not done_before and data.get('n') is not None:
                info = {'chunks': {}, 'total': None, 'crc': None,
                        'gw': '?', 'kind': '?', 'ts': time.time(), 'no_begin': True}
                self.incoming[tid] = info
        if not info:
            if done_before:
                self.send({"t": f"{self.p}_ack", "tid": tid, "ok": 1, "miss": []})
            return None
        if info.get('total') is None:
            info['total'] = data.get('n', 0)
            info['crc'] = data.get('crc', '')
        if info.get('kind', '?') == '?' and data.get('kind'):
            info['kind'] = data['kind']
        if info.get('gw', '?') == '?' and data.get('g'):
            info['gw'] = data['g']
        missing = [i for i in range(info['total']) if i not in info['chunks']]
        if missing:
            self._log('warn', 'XFER', f"⚠️ tid={tid} brakujące chunki: {missing[:5]}")
            with self.lock:
                self.incoming[tid]['ts'] = time.time()
            self.send({"t": f"{self.p}_ack", "tid": tid, "ok": 0, "miss": missing[:5]})
            return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            self._log('warn', 'XFER', f"⚠️ tid={tid} CRC mismatch")
            with self.lock:
                self.incoming[tid]['ts'] = time.time()
            self.send({"t": f"{self.p}_ack", "tid": tid, "ok": 0, "miss": []})
            return None
        with self.lock:
            self.incoming.pop(tid, None)
            self.completed[tid] = time.time()
            if len(self.completed) > 32:
                for old in sorted(self.completed, key=self.completed.get)[:-16]:
                    self.completed.pop(old, None)
        self.send({"t": f"{self.p}_ack", "tid": tid, "ok": 1, "miss": []})
        try:
            payload = self.deserialize(b64)
        except Exception as e:
            self._log('error', 'XFER', f"deserialize: {e}")
            return None
        n = len(payload) if hasattr(payload, '__len__') else '?'
        self._log('info', 'XFER', f"✅ Odebrano kind={info['kind']} [{info['gw']}] ({n} elem.)")
        if self.on_received:
            try:
                self.on_received(info['gw'], info['kind'], payload)
            except Exception as e:
                self._log('error', 'XFER', f"on_received: {e}")
        return info['gw'], info['kind'], payload

    def handle_ack(self, data):
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
                self._log('info', 'XFER', f"✅ ACK ok tid={tid}")
                return
            chunks = info['chunks']
            crc = info.get('crc', '')
            gw = info.get('gw', '?')
            kind = info.get('kind', '?')
        self._log('warn', 'XFER', f"🔁 NACK tid={tid} retransmit {miss}")
        for i in miss:
            if 0 <= i < len(chunks):
                self.send({"t": f"{self.p}_chunk", "tid": tid, "s": i, "d": chunks[i]})
        self.send({"t": f"{self.p}_end", "tid": tid, "n": len(chunks),
                   "crc": crc, "g": gw, "kind": kind})

    # ── router (rejestruj w dispatcherze: rt_begin/chunk/end/ack/cack) ──
    def dispatch(self, data):
        t = data.get('t', '')
        if t == f"{self.p}_begin":
            self.handle_begin(data)
        elif t == f"{self.p}_chunk":
            self.handle_chunk(data)
        elif t == f"{self.p}_end":
            self.handle_end(data)
        elif t == f"{self.p}_ack":
            self.handle_ack(data)
        elif t == f"{self.p}_cack":
            self.handle_cack(data)
