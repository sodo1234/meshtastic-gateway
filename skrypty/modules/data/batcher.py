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


# Sufit bufora, gdy flush idzie transferem-plikiem: tyle urzadzen ma carport na jednej
# bramce (78 przekaznikow + czujniki). Powyzej i tak ciete na chunki, wiec to tylko
# zabezpieczenie pamieci, nie parametr do strojenia.
BLOB_MAX_ITEMS = 120


class Batcher:
    def __init__(self, gw_id, flush_callback, interval=30, max_payload=150,
                 max_items=6, tag="BATCH", logger=None, arbiter=None, repeat=1,
                 ack_wait=0.0, ack_retries=2, bypass_arbiter=False,
                 blob_fn=None, blob_min_packets=3):
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
        # bypass_arbiter (2026-07-30, decyzja usera): batcher ALARMOWY (P1) nie czeka na koniec
        # transferu. Zmierzone live: devmap trzymał kanał ~2 min — tyle NIE może czekać wyciek
        # ani dym. Wolumen monitored zostaje pod arbitrem bez zmian.
        self.bypass_arbiter = bypass_arbiter
        # 2026-07-19: retry P1 — priority batcher wysyla kazdy pakiet `repeat`x
        # (2 rozstrzelone proby przez kolejke TX) zeby door/leak nie ginely w krotkich stratach LoRa.
        self.repeat = max(1, int(repeat))
        # 2026-07-30: PRAWDZIWY ACK zamiast slepego dublowania. ack_wait>0 ⇒ kazdy pakiet dostaje
        # krotkie id `p`, supervisor odbija `{t:back,g,p}`, a brak `back` w ack_wait s = retransmisja
        # (do ack_retries prob). Zysk podwojny: (1) strata dluzsza niz jedna ramka jest NAPRAWIANA,
        # czego `repeat=2` nie umialo; (2) w normalnym przypadku leci JEDNA ramka + ~30B ack zamiast
        # dwoch pelnych batchy, czyli MNIEJ airtime niz dotad. Ustaw repeat=1 gdy ACK wlaczony.
        self.ack_wait = float(ack_wait)
        self.ack_retries = max(0, int(ack_retries))
        self._pid = 0                    # licznik id pakietow (1..999, cyklicznie)
        self._pending = {}               # p → [pkt, tries, last_ts]
        # BURST → PLIK (2026-09-11): gdy flush rozpada sie na >= blob_min_packets ramek `b`,
        # calosc idzie JEDNYM ReliableTransfer (jak kalendarz/ansnap/avail). Ustawiane
        # z harnessu: `batch.blob_fn = lambda payload: rt.start_send(gw_id, 'bdata', payload)`.
        # None = stare zachowanie (same ramki `b`) — zgodnosc wsteczna i tryb testowy.
        self.blob_fn = blob_fn
        self.blob_min_packets = max(2, int(blob_min_packets))
        self.buffer = {}            # {sid: fields_dict} — merged per device
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.running = True
        self._stats = {"batched": 0, "flushed": 0, "merged": 0,
                       "acked": 0, "resent": 0, "lost": 0, "blobbed": 0}

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
            # `max_items` to wyzwalacz auto-flushu, NIE limit rozmiaru ramki (tym zajmuje sie
            # `max_payload` w `_split_into_packets`). Przy blob_fn chcemy ODWROTNIE niz dotad:
            # burst ma sie SKLEIC, bo dopiero od ~12 urzadzen transfer bije ramki `b` (pomiar
            # 2026-09-17: 78 dev = 20 ramek/3052 B vs 8 ramek/718 B). Ciecie co 6 urzadzen
            # gwarantowalo 1 ramke na flush i czynilo sciezke blob MARTWA — to zrodlo
            # 443 pojedynczych ramek `b` = 17% pasma TX G1.
            if len(self.buffer) >= (BLOB_MAX_ITEMS if self.blob_fn else self.max_items):
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
            if self.ack_wait > 0:
                self.retry_unacked()             # ten sam tick 1s — bez osobnego wątku
            if time.time() - self.last_flush >= self.interval:
                self.flush()

    # ── ACK / retransmisja (ack_wait > 0) ───────────────
    def ack(self, pid):
        """`back` z supervisora: pakiet dotarł → koniec retransmisji. Zwraca True gdy nasz."""
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return False
        entry = self._pending.pop(pid, None)
        if entry is None:
            return False                         # nieznane/już potwierdzone (np. spóźniony dubel ACK)
        self._stats["acked"] += 1
        if self.log:
            self.log.info('BATCH', f"✅ [{self.tag}] ack p={pid} (próby: {entry[1]})")
        return True

    def retry_unacked(self):
        """Retransmituj pakiety bez `back` po ack_wait s; po ack_retries próbach porzuć z alarmem.
        Porzucenie MA być widoczne w logu — to utrata danych priorytetowych, nie drobiazg."""
        now = time.time()
        for pid, entry in list(self._pending.items()):
            pkt, tries, last = entry
            if last is None or now - last < self.ack_wait:
                continue                     # None = czeka na kanał (arbiter/kolejka) — nie liczy się
            if tries > self.ack_retries:
                self._pending.pop(pid, None)
                self._stats["lost"] += 1
                if self.log:
                    self.log.error('BATCH', f"❌ [{self.tag}] p={pid} BEZ ACK po {tries} próbach "
                                            f"({len(pkt.get('d', []))} dev) — utrata danych P1")
                continue
            entry[1] = tries + 1
            entry[2] = None                  # znów „czeka na kanał" — zegar ruszy przy realnym TX
            self._stats["resent"] += 1
            if self.log:
                self.log.warn('BATCH', f"🔁 [{self.tag}] retransmisja p={pid} (próba {tries + 1}"
                                       f"/{self.ack_retries + 1})")
            self.flush_callback(pkt)

    def mark_sent(self, pid):
        """Ramka REALNIE poszła w eter → od teraz liczymy ack_wait. Woła transport (GatewayData._tx),
        bo tylko on wie, kiedy arbiter/kolejka faktycznie puściły pakiet."""
        entry = self._pending.get(pid)
        if entry is not None:
            entry[2] = time.time()

    def pending_acks(self):
        return len(self._pending)

    def flush(self):
        """Ship buffered deltas. Returns list of packet dicts sent (for tests)."""
        if self.arbiter is not None and self.arbiter.busy() and not self.bypass_arbiter:
            return []                    # transfer-plik trwa → nie zapychaj kanału (bufor zostaje)
        with self.lock:
            items = list(self.buffer.items())
            self.buffer.clear()
            self.last_flush = time.time()
        if not items:
            return []
        packets = self._split_into_packets(items)
        # ── BURST `b` → JEDEN PAKIET-PLIK (2026-09-11, polecenie usera: „burst stanow
        #    w pakietach jak harmonogram") ──────────────────────────────────────────────
        # Zmierzone na G1 (56 h): `b` = 443 pojedynczych ramek, 17% calego pasma TX bramki.
        # Rebase „wszystko-jako-plik" objal kalendarz, devmap, anomalie (`ansnap`) i
        # availability (`avail`) — ale NIE zwykly wolumen `b`. Przy duzym flushu placimy
        # naglowek `{t,g,ts,p}` (~30 B) RAZY liczba ramek, a kazda ramka to osobne wejscie
        # na kanal z cooldownem 3 s. Jeden ReliableTransfer ma naglowek raz i wlasny CRC+ACK.
        # Maly flush (1-2 ramki) zostaje po staremu — transfer ma wlasny narzut
        # (`rt_begin`/`rt_end` + cack per chunk), wiec oplaca sie dopiero od kilku ramek.
        if self.blob_fn is not None and len(packets) >= self.blob_min_packets:
            payload = {'ts': int(time.time()), 'd': [[sid, f] for sid, f in items]}
            try:
                self.blob_fn(payload)
                self._stats["blobbed"] += 1
                if self.log:
                    self.log.info('BATCH', f"📦 [{self.tag}] {len(items)} dev → 1 transfer "
                                           f"(zamiast {len(packets)} ramek `b`)")
                return []
            except Exception as e:                  # transfer padl → normalne ramki, zero utraty
                if self.log:
                    self.log.warn('BATCH', f"[{self.tag}] blob padl ({e}) → ramki `b`")
        for pkt in packets:
            if self.ack_wait > 0:
                self._pid = self._pid % 999 + 1
                pkt["p"] = self._pid                     # id do ACK + znacznik P1 dla kolejki TX
                # last_ts=None ⇒ „jeszcze NIE nadany". Zegar ACK startuje dopiero w mark_sent(),
                # bo między flush() a eterem stoi arbiter kanału (transfer-plik) i kolejka `_out`
                # z odstępem cooldownu. Licząc od flush() wypalaliśmy wszystkie próby i ogłaszali
                # utratę danych zanim ramka w ogóle poszła (zmierzone live 2026-07-30).
                self._pending[self._pid] = [pkt, 1, None]
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
        probe = {"t": "b", "g": self.gw_id, "ts": 0, "d": [[s, f] for s, f in items]}
        if self.ack_wait > 0:
            probe["p"] = 999          # licz `p` w rozmiarze, inaczej split zaniża o ~8B (limit 150B)
        return json.dumps(probe, separators=(',', ':'))

    def pending_count(self):
        with self.lock:
            return len(self.buffer)

    @property
    def stats(self):
        with self.lock:
            return {**self._stats, "pending": len(self.buffer)}
