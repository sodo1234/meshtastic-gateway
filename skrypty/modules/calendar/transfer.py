"""CalendarTransfer — chunkowany transfer harmonogramu przez LoRa (STEP 4 / krok 16).

Port CalendarTransfer z supervisor_v38.py z wstrzykiwaniem (send_fn, on_received).
Protokół:  cal_begin(tid,n,crc) → cal_chunk(s,d)×n → cal_end(tid) → cal_ack(ok,miss)
  - serializacja: json(slots) → zlib(9) → base64 → chunki po chunk_size
  - CRC = md5(b64)[:4]; brakujące chunki → NACK(ok=0,miss) → retransmit
  - cal_end z retry (gubienie sch_e w eterze → deadlock bez tego)
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
    def __init__(self, send_fn, chunk_size=140, chunk_delay=6.0,
                 on_received=None, logger=None):
        self.send = send_fn              # callable(dict) → wyślij ramkę LoRa
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay
        self.on_received = on_received   # callback(gw, direction, slots) po udanym odbiorze
        self.log = logger
        self.incoming = {}
        self.outgoing = {}
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

    # ── wysyłka (master → odbiorca) ─────────────────────
    def start_send(self, gw, direction, slots):
        b64 = self.serialize(slots)
        crc = self.crc16(b64)
        chunks = [b64[i:i + self.chunk_size] for i in range(0, len(b64), self.chunk_size)] or ['']
        tid = self.gen_tid()
        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'gw': gw, 'dir': direction,
                                  'ts': time.time(), 'ack_received': False}
        self.send({"t": "cal_begin", "tid": tid, "g": gw, "dir": direction,
                   "n": len(chunks), "crc": crc})
        self._log('info', 'SYNC', f"📤 Begin {direction} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")

        def _run():
            for i, chunk in enumerate(chunks):
                time.sleep(self.chunk_delay)
                self.send({"t": "cal_chunk", "tid": tid, "s": i, "d": chunk})
            for attempt in range(3):                       # cal_end z retry (gubienie w eterze)
                time.sleep(self.chunk_delay)
                with self.lock:
                    if tid not in self.outgoing:
                        return
                self.send({"t": "cal_end", "tid": tid})
                if attempt > 0:
                    self._log('warn', 'SYNC', f"⚠️ cal_end retry tid={tid} {attempt + 1}/3")
                time.sleep(15)
                with self.lock:
                    info = self.outgoing.get(tid)
                    if info is None or info.get('ack_received'):
                        return
            self._log('error', 'SYNC', f"❌ tid={tid} brak odpowiedzi po 3 cal_end — rezygnacja")
            with self.lock:
                self.outgoing.pop(tid, None)

        threading.Thread(target=_run, daemon=True, name=f"cal-tx-{tid}").start()
        return tid

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
        with self.lock:
            if tid in self.incoming:
                self.incoming[tid]['chunks'][data.get('s', 0)] = data.get('d', '')
                self.incoming[tid]['ts'] = time.time()

    def handle_end(self, data):
        tid = data.get('tid')
        with self.lock:
            info = self.incoming.get(tid)
        if not info:
            return None
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
        tid = data.get('tid')
        ok = data.get('ok', 0)
        miss = data.get('miss', [])
        with self.lock:
            info = self.outgoing.get(tid)
            if not info:
                return
            if ok == 1:
                self.outgoing.pop(tid, None)
                self._log('info', 'SYNC', f"✅ ACK ok tid={tid}")
                return
            info['ack_received'] = True
            chunks = info['chunks']
        # NACK → retransmit brakujących + ponowny cal_end
        self._log('warn', 'SYNC', f"🔁 NACK tid={tid} retransmit {miss}")
        for i in miss:
            if 0 <= i < len(chunks):
                self.send({"t": "cal_chunk", "tid": tid, "s": i, "d": chunks[i]})
        self.send({"t": "cal_end", "tid": tid})

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
