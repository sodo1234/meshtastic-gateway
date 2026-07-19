"""Batcher — buffers device deltas, flushes compact `b` packets over LoRa.

Ported from gateway_v38.py:193 `Batcher`, generalized for Step 3:
  - one instance per stream: monitored (P3, 30s) and priority (P1, 10s)
  - buffer keyed by short id (sid); fields MERGE within a window so partial
    deltas accumulate (temp arrives, then humidity arrives → both ship together)
  - packet split honors max_payload (LoRa 150B operational)

Wire format (CLAUDE.md `b`): {t:b,g:G1,ts:T,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}
"""
import json
import threading
import time


class Batcher:
    def __init__(self, gw_id, flush_callback, interval=30, max_payload=150,
                 max_items=6, tag="BATCH", logger=None, arbiter=None, repeat=1):
        self.gw_id = gw_id
        self.flush_callback = flush_callback      # callable(packet_dict) → send over LoRa
        self.interval = interval
        self.max_payload = max_payload
        self.max_items = max_items
        self.tag = tag
        self.log = logger
        # ChannelArbiter (opcjonalny): gdy trwa transfer-plik (kalendarz/devmap/ansnap) kanał
        # należy do niego — wolumen `b` CZEKA (bufor zostaje na następny tick, zero utraty).
        self.arbiter = arbiter
        # 2026-07-19: retry P1 — priority batcher wysyla kazdy pakiet `repeat`x
        # (2 rozstrzelone proby przez kolejke TX) zeby door/leak nie ginely w krotkich stratach LoRa.
        self.repeat = max(1, int(repeat))
        self.buffer = {}            # {sid: fields_dict} — merged per device
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.running = True
        self._stats = {"batched": 0, "flushed": 0, "merged": 0}

    def add(self, sid, fields, force_flush=False):
        """Buffer a delta for `sid`. Fields merge into any pending entry.
        force_flush=True ships immediately (alarm / on-demand refresh)."""
        do_flush = force_flush
        with self.lock:
            if sid in self.buffer:
                self.buffer[sid].update(fields)
                self._stats["merged"] += 1
            else:
                self.buffer[sid] = dict(fields)
            self._stats["batched"] += 1
            if len(self.buffer) >= self.max_items:
                do_flush = True
        if do_flush:
            self.flush()

    def start(self):
        threading.Thread(target=self._flush_loop, daemon=True,
                         name=f"batcher-{self.tag}").start()

    def stop(self):
        self.running = False

    def _flush_loop(self):
        while self.running:
            time.sleep(1)
            if time.time() - self.last_flush >= self.interval:
                self.flush()

    def flush(self):
        """Ship buffered deltas. Returns list of packet dicts sent (for tests)."""
        if self.arbiter is not None and self.arbiter.busy():
            return []                    # transfer-plik trwa → nie zapychaj kanału (bufor zostaje)
        with self.lock:
            items = list(self.buffer.items())
            self.buffer.clear()
            self.last_flush = time.time()
        if not items:
            return []
        packets = self._split_into_packets(items)
        for pkt in packets:
            for _ in range(self.repeat):
                self.flush_callback(pkt)
            self._stats["flushed"] += 1
        if self.log:
            self.log.info('BATCH', f"📦 [{self.tag}] {len(items)} dev → {len(packets)} pkt(s)")
        return packets

    def _split_into_packets(self, items):
        packets, current = [], []
        for item in items:
            test = current + [item]
            if len(self._serialize(test)) > self.max_payload and current:
                packets.append(self._build_dict(current))
                current = [item]
            else:
                current = test
        if current:
            packets.append(self._build_dict(current))
        return packets

    def _build_dict(self, items):
        return {"t": "b", "g": self.gw_id, "ts": int(time.time()),
                "d": [[sid, fields] for sid, fields in items]}

    def _serialize(self, items):
        return json.dumps({"t": "b", "g": self.gw_id, "ts": 0,
                           "d": [[s, f] for s, f in items]}, separators=(',', ':'))

    def pending_count(self):
        with self.lock:
            return len(self.buffer)

    @property
    def stats(self):
        with self.lock:
            return {**self._stats, "pending": len(self.buffer)}
