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
import os
import random
import string
import threading
import time
import zlib


class ReliableTransfer:
    def __init__(self, send_fn, on_received=None, logger=None, prefix="rt",
                 chunk_size=90, chunk_delay=3.0,
                 chunk_ack_timeout=12.0, chunk_retries=6, end_retries=4,
                 arbiter=None, accept_gw=None, window=1, window_gap=3.5, hold_extra=0.0,
                 frames_fit=None, window_max=0, start_delay=0.0):
        self.send = send_fn                  # callable(dict) → wyślij ramkę LoRa
        self.on_received = on_received       # callback(gw, kind, payload) po udanym odbiorze
        self.log = logger
        self.p = prefix                      # prefiks typów ('rt' → rt_begin/rt_chunk/...)
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay
        self.chunk_ack_timeout = chunk_ack_timeout
        self.chunk_retries = chunk_retries
        self.end_retries = end_retries
        # OKNO (2026-10-02): window=1 = dotychczasowy stop-and-wait (cack po KAZDYM chunku).
        # window=W>1: nadawca wysyla W chunkow pod rzad (spacing robi kolejka TX / cooldown), a odbiorca
        # odpowiada JEDNYM cack z lista brakujacych (`m`) dopiero po chunku oznaczonym `w`. Zgubione
        # chunki leca w kolejnej rundzie. Odbiorca bez obslugi okna (stary kod) potwierdza kazdy chunk -
        # dziala dalej, tylko bez przyspieszenia. window_gap [s] = zapas czasu na jeden chunk w kolejce TX.
        self.window = max(1, int(window or 1))
        # hold_extra [s]: dodatek do rezerwacji kanalu — przy slotowaniu wysylka potrafi czekac na okno do
        # dlugosci cyklu; bez dodatku hold wygasal, arbiter wpuszczal kolejne transfery RÓWNOLEGLE.
        self.hold_extra = float(hold_extra or 0.0)
        # frames_fit: callable → ile ramek miesci sie teraz w oknie slotu (None = bez limitu). Runda okna
        # nie wiekszej niz to, zeby nie przerzucac polowy okna na kolejny cykl 60 s.
        self.frames_fit = frames_fit
        # window_max: gdy window>1, okno rosnie do tej wartosci, a runda i tak nie przekracza frames_fit()
        # (czyli tego, co miesci sie w slocie: G1 ~13 ramek, G2 ~3). 0 = bez wzrostu (jak dotad).
        self.window_max = int(window_max or 0)
        # start_delay [s]: bulk-transfery (devmap/avail/ansnap/dsnap/bdata) nie startuja przed uplywem
        # tego czasu od uruchomienia — bramki po wspolnym restarcie nie zalewaja kanalu naraz.
        self._not_before = time.monotonic() + float(start_delay or 0.0)
        self.window_gap = window_gap
        # ChannelArbiter: gdy transfer aktywny, kanał należy do niego (wolumen b/ab czeka).
        # Nadawca odnawia hold przy każdym chunku; odbiorca trzyma po *_begin/*_chunk.
        self.arbiter = arbiter
        # accept_gw: adresowanie multi-gw na WSPÓŁDZIELONYM kanale (wszyscy słyszą wszystko).
        #   None  → przyjmij transfery od/do dowolnej bramki (supervisor agreguje wszystkie).
        #   'Gx'  → tylko transfery z g==Gx lub g==None (globalny). Transfer cudzej bramki =
        #           IGNORUJEMY całkowicie: brak cack (żeby nie kłamać ACK nadawcy) i brak apply.
        self.accept_gw = accept_gw
        self._foreign = set()                # tid transferów NIE dla nas (z *_begin/*_end)
        # HASHE NA KONCU TRANSFERU (2026-10-09): nadawca dokleja do rt_end swoje biezace hashe stanu
        # (`hash_fn()` → {'dh':..,'sh':..,'ah':..}), a odbiorca odklada je w `last_hs[gw]` tuz przed
        # on_received. Dzieki temu zgodnosc mozna policzyc w chwili zakonczenia transferu, a nie dopiero
        # po kolejnym HB/pingu (HB zostaje kontrola dryfu). Brak hash_fn = zachowanie jak dotad.
        self.hash_fn = None
        self.last_hs = {}
        self.incoming = {}
        self.outgoing = {}
        self.completed = {}                  # tid → ts (idempotentny re-ACK)
        self.lock = threading.Lock()

    def _hold_channel(self, tid=None, sender=False, wait_extra=0.0):
        """Nadawca (sender=True) odnawia hold z dodatkiem na czekanie w slocie; odbiorca trzyma
        krotko i z ownerem=tid, zeby handle_end mogl go zwolnic (inaczej b/ab/hb stoja do ~94 s)."""
        if self.arbiter is not None:
            if sender:
                # Okno czeka dluzej niz zwykly stop-and-wait: timeout bazowy + zapas na
                # kazda ramke okna. Hold musi pokrywac TEN SAM budzet, inaczej wygasa jeszcze
                # podczas oczekiwania na zbiorczy CACK i drugi lokalny transfer wchodzi w srodek
                # pierwszego (v7: avail wystartowal 4 s przed retry devmapy G1).
                self.arbiter.hold(self.chunk_ack_timeout + 4.0 + self.hold_extra
                                  + float(wait_extra or 0.0))
            else:
                self.arbiter.hold(self.chunk_ack_timeout + 4.0, owner=tid)

    def receiving(self, gw=None, kind=None, stale_after=300.0):
        """Czy TRWA ODBIÓR transferu (opcjonalnie: tej bramki / tego rodzaju)?

        PO CO: logika ponawiania `disc` na supervisorze pytała o mapę urządzeń, nie wiedząc,
        że mapa właśnie leci — a transfer trwa dłużej niż okno retry (zmierzone 2026-08-05:
        devmap 108 s, 7 żądań `disc` w 2 min, w efekcie DRUGI zbędny transfer). Na bramce
        z ~200 urządzeniami transfer jest wielokrotnie dłuższy, więc to skaluje się w lawinę.

        stale_after: wpis starszy niż to NIE liczy się jako aktywny — zawieszony transfer
        nie może zablokować discovery na zawsze (gate ma tłumić flood, nie naprawę).

        gw/kind '?' (begin zgubiony na RF, odtwarzany z chunku-sieroty) traktujemy jako
        potencjalnie nasze: lepiej chwilę zamilczeć niż dołożyć do zatoru."""
        now = time.time()
        with self.lock:
            for info in self.incoming.values():
                if now - info.get('ts', 0) > stale_after:
                    continue
                if gw is not None and info.get('gw') not in (gw, '?', None):
                    continue
                if kind is not None and info.get('kind') not in (kind, '?', None):
                    continue
                return True
        return False

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

    @staticmethod
    def _with_g(frame, gw):
        """Attach a concrete transfer gateway; leave legacy/global unknowns unaddressed."""
        if gw not in (None, '', '?'):
            frame['g'] = gw
        return frame

    def _log(self, lvl, tag, msg):
        if self.log:
            getattr(self.log, lvl, self.log.info)(tag, msg)

    # ── wysyłka (stop-and-wait per chunk) ───────────────
    def _window_now(self):
        """Okno na TEN transfer. Plik /tmp/rt_window (liczba) = jawny limit, nadpisuje config w locie (A/B,
        natychmiastowe wylaczenie wpisem 1). Bez pliku: window=1 → stop-and-wait; window>1 → rosnie do
        window_max (runda i tak <= frames_fit(), czyli pojemnosc slotu)."""
        try:
            with open('/tmp/rt_window', encoding='utf-8') as f:
                return max(1, int(f.read().strip()))
        except Exception:
            return max(self.window, self.window_max) if self.window > 1 else self.window

    def _tx(self, msg):
        """Nadaj ramke i (jesli transport zwraca Event) poczekaj az faktycznie poszla do radia —
        timeout ACK ma liczyc sie od NADANIA, nie od wstawienia do kolejki. Tylko watek nadawczy."""
        r = self.send(msg)
        if hasattr(r, 'wait'):
            r.wait(120.0)

    def _end_frame(self, tid, n, crc, gw, kind):
        f = {"t": f"{self.p}_end", "tid": tid, "n": n, "crc": crc, "g": gw, "kind": kind}
        # hs = [dh, sh, ah] (kompakt: ~-27 B wzgledem dict; rt_end ma isc po stratnym LoRa).
        # /tmp/rt_nohs (plik) wylacza hashe w locie — do testow A/B bez restartu.
        if self.hash_fn is not None and kind in ('dsnap', 'avail', 'bdata', 'ansnap') and not os.path.exists('/tmp/rt_nohs'):
            try:
                hs = self.hash_fn()
                if hs:
                    # tylko to, co dany rodzaj zmienia: stan/status (dsnap, avail, bdata) albo
                    # anomalie (ansnap); devmap ma hash juz we wlasnym payloadzie.
                    f["hs"] = ["", "", hs.get('ah', '')] if kind == 'ansnap' else [hs.get('dh', ''), hs.get('sh', '')]
            except Exception as e:
                self._log('debug', 'XFER', f"hash_fn: {e}")
        return f

    def start_send(self, gw, kind, payload):
        win = self._window_now()
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
            _b = {"t": f"{self.p}_begin", "tid": tid, "g": gw, "kind": kind,
                  "n": len(chunks), "crc": crc}
            if win > 1:
                _b["win"] = win                          # odbiorca: nie potwierdzaj kazdego chunka
            self._tx(_b)
            self._log('info', 'XFER', f"📤 Begin kind={kind} [{gw}] tid={tid} chunks={len(chunks)} ({len(b64)}B)")
            time.sleep(self.chunk_delay)
            # STOP-AND-WAIT per-chunk cack (2026-07-06 PRZYWRÓCONE): niezawodność > liczba wiadomości.
            # Fire-and-NACK zawieszał transfer na stratnym łączu (zgubiony NACK = brak retransmisji chunka).
            # Per-chunk cack: zgubiony chunk = natychmiastowy retry. Koszt (N cacków) płacony RZADKO
            # (mapa/ansnap tylko przy zmianie hasha). „Minimum danych" = plik+hash-gate, nie brak cacków.
            if win <= 1:
                for i, chunk in enumerate(chunks):
                    ok = False
                    for attempt in range(self.chunk_retries + 1):
                        self._hold_channel(sender=True)             # kanał należy do transferu (wolumen b/ab czeka)
                        self._tx({"t": f"{self.p}_chunk", "tid": tid, "g": gw,
                                  "s": i, "d": chunk})
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
            else:
                # OKNO: rundy po `window` chunkow; kazda runda konczy sie JEDNYM cack zbiorczym.
                rounds_bez_postepu = 0
                while True:
                    with self.lock:
                        info = self.outgoing.get(tid)
                        if info is None:
                            return
                        brak = [i for i in range(len(chunks)) if i not in info['acked']]
                    if not brak:
                        break
                    paczka = brak[:min(win, max(1, self.frames_fit()))] if self.frames_fit else brak[:win]
                    with self.lock:
                        info = self.outgoing.get(tid)
                        cack_rev = info.get('cack_rev', 0) if info else 0
                    wait_extra = self.window_gap * len(paczka)
                    for k, i in enumerate(paczka):
                        self._hold_channel(sender=True, wait_extra=wait_extra)
                        m = {"t": f"{self.p}_chunk", "tid": tid, "g": gw,
                             "s": i, "d": chunks[i]}
                        if k == len(paczka) - 1:
                            m["w"] = 1                   # koniec okna → odbiorca odpowiada cack zbiorczym
                        self._tx(m)
                    budzet = self.chunk_ack_timeout + self.window_gap * len(paczka)
                    # Zbiorczy CACK z lista brakow jest ODPOWIEDZIA, nie timeoutem. Dawniej
                    # predicate czekal tylko na komplet, wiec po CACK m=[...] spal do konca
                    # calego budzetu (v7 G1: +64 s) zanim wyslal brakujace chunki.
                    res = _wait_flag(
                        lambda inf: (all(j in inf['acked'] for j in paczka)
                                     or inf.get('cack_rev', 0) > cack_rev),
                        timeout=budzet)
                    if res is None:
                        return
                    with self.lock:
                        info = self.outgoing.get(tid)
                        zrobione = sum(1 for j in paczka if info and j in info['acked'])
                        komplet = bool(info) and all(j in info['acked'] for j in paczka)
                    if komplet:
                        rounds_bez_postepu = 0
                        continue
                    rounds_bez_postepu = 0 if zrobione else rounds_bez_postepu + 1
                    self._log('warn', 'XFER', f"🔁 okno {paczka[0]}..{paczka[-1]}/{len(chunks)} tid={tid}: "
                              f"{zrobione}/{len(paczka)} potwierdzone — kolejna runda")
                    if rounds_bez_postepu > self.chunk_retries:
                        self._log('error', 'XFER', f"❌ okno {paczka[0]}..{paczka[-1]} tid={tid} bez postepu → przerwane")
                        with self.lock:
                            self.outgoing.pop(tid, None)
                        return
            attempt = 0
            nacks = 0
            while attempt < self.end_retries:
                self._hold_channel(sender=True)
                self._tx(self._end_frame(tid, len(chunks), crc, gw, kind))
                res = _wait_flag(lambda inf: inf.get('done') or inf.get('resend_end'))
                if res is None:
                    return
                if res:
                    with self.lock:
                        inf = self.outgoing.get(tid)
                        if inf is None or inf.get('done'):
                            return
                        inf.pop('resend_end', None)
                    # NACK: brakujace chunki sa juz w kolejce (handle_ack) — jeden kolejny rt_end,
                    # bez zuzywania proby (ale nie w nieskonczonosc przy stalym bledzie CRC).
                    nacks += 1
                    if nacks > 6:
                        attempt += 1
                    continue
                attempt += 1
                self._log('warn', 'XFER', f"⚠️ rt_end retry tid={tid} {attempt}/{self.end_retries}")
            self._log('error', 'XFER', f"❌ tid={tid} brak finalnego ACK po {self.end_retries} rt_end — rezygnacja")
            with self.lock:
                self.outgoing.pop(tid, None)

        def _run():
            _w = self._not_before - time.monotonic()
            if _w > 0:
                time.sleep(_w)                                  # rozproszenie startu bramek (start_delay)
            # SERIALIZACJA transfer-vs-transfer: czekaj aż poprzedni transfer zwolni kanał.
            # Begin wysyłamy DOPIERO po przejęciu (nie zapowiadamy transferu w środek cudzego).
            if self.arbiter is not None:
                if not self.arbiter.acquire(tid, self.chunk_ack_timeout + 4.0 + self.hold_extra):
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
        m = data.get('m')
        with self.lock:
            info = self.outgoing.get(tid)
            if (info is not None and data.get('g') is not None
                    and data.get('g') != info.get('gw')):
                return
            if info is not None and s is not None:
                if m is None:
                    info['acked'].add(s)                 # cack pojedynczy (stary tryb / stary odbiorca)
                else:                                    # cack zbiorczy: wszystko <= s oprocz brakujacych
                    info['acked'].update(i for i in range(s + 1) if i not in m)
                    info['cack_rev'] = info.get('cack_rev', 0) + 1

    # ── odbiór ──────────────────────────────────────────
    def handle_begin(self, data):
        tid = data.get('tid')
        self._hold_channel(tid)              # kanał zajęty (nawet cudzy transfer → backoff wolumenu)
        if self.accept_gw is not None and data.get('g') not in (None, self.accept_gw):
            self._note_foreign(tid)          # transfer NIE do nas → nie uczestniczymy (brak cack/apply)
            self._log('debug', 'XFER', f"⏭️ begin tid={tid} g={data.get('g')} ≠ {self.accept_gw} → ignoruję")
            return
        with self.lock:
            self.incoming[tid] = {'chunks': {}, 'total': data.get('n', 0),
                                  'crc': data.get('crc', ''), 'gw': data.get('g', '?'),
                                  'kind': data.get('kind', '?'), 'ts': time.time(),
                                  'win': data.get('win', 1)}
        self._log('info', 'XFER', f"📥 Begin kind={data.get('kind')} [{data.get('g')}] tid={tid} expect={data.get('n')}")

    def handle_chunk(self, data):
        tid = data.get('tid')
        s = data.get('s', 0)
        self._hold_channel(tid)
        if (self.accept_gw is not None and data.get('g') is not None
                and data.get('g') != self.accept_gw):
            self._note_foreign(tid)
            return
        if tid in self._foreign:
            return                           # cudzy transfer → NIE cack (unikamy fałszywego ACK nadawcy)
        with self.lock:
            if tid not in self.incoming:
                # begin zgubiony na RF → odtwórz z chunku-sieroty (kind/total/crc z rt_end)
                self.incoming[tid] = {'chunks': {}, 'total': None, 'crc': None,
                                      'gw': data.get('g', '?'), 'kind': '?',
                                      'ts': time.time(), 'no_begin': True}
                self._log('warn', 'XFER', f"⚠️ chunk {s} tid={tid} bez rt_begin (zgubiony) → odtwarzam z rt_end")
            elif self.incoming[tid].get('gw', '?') == '?' and data.get('g'):
                self.incoming[tid]['gw'] = data['g']
            self.incoming[tid]['chunks'][s] = data.get('d', '')
            self.incoming[tid]['ts'] = time.time()
            win = self.incoming[tid].get('win', 1)
            have = self.incoming[tid]['chunks']
            brak = [i for i in range(s + 1) if i not in have]
            reply_gw = self.incoming[tid].get('gw')
        if win > 1:
            if data.get('w'):                            # koniec okna → jeden cack zbiorczy z lista brakujacych
                self.send(self._with_g(
                    {"t": f"{self.p}_cack", "tid": tid, "s": s, "m": brak}, reply_gw))
            return                                       # chunki w srodku okna: cisza (oszczednosc lacza)
        self.send(self._with_g(
            {"t": f"{self.p}_cack", "tid": tid, "s": s}, reply_gw))  # per-chunk ACK

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
                self.send(self._with_g(
                    {"t": f"{self.p}_ack", "tid": tid, "ok": 1, "miss": []}, data.get('g')))
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
            self.send(self._with_g(
                {"t": f"{self.p}_ack", "tid": tid, "ok": 0, "miss": missing[:8]},
                info.get('gw') or data.get('g')))
            return None
        b64 = ''.join(info['chunks'][i] for i in range(info['total']))
        if self.crc16(b64) != info['crc']:
            self._log('warn', 'XFER', f"⚠️ tid={tid} CRC mismatch")
            with self.lock:
                self.incoming[tid]['ts'] = time.time()
            self.send(self._with_g(
                {"t": f"{self.p}_ack", "tid": tid, "ok": 0, "miss": []},
                info.get('gw') or data.get('g')))
            return None
        with self.lock:
            self.incoming.pop(tid, None)
            self.completed[tid] = time.time()
            if len(self.completed) > 32:
                for old in sorted(self.completed, key=self.completed.get)[:-16]:
                    self.completed.pop(old, None)
        self.send(self._with_g(
            {"t": f"{self.p}_ack", "tid": tid, "ok": 1, "miss": []},
            info.get('gw') or data.get('g')))
        if self.arbiter is not None:
            self.arbiter.release(tid)                     # koniec odbioru → b/ab/hb moga isc (hold nie wisi ~94 s)
        try:
            payload = self.deserialize(b64)
        except Exception as e:
            self._log('error', 'XFER', f"deserialize: {e}")
            return None
        n = len(payload) if hasattr(payload, '__len__') else '?'
        self._log('info', 'XFER', f"✅ Odebrano kind={info['kind']} [{info['gw']}] ({n} elem.)")
        hs = data.get('hs')
        if isinstance(hs, list) and 1 <= len(hs) <= 3:         # [dh, sh, ah] → dict (puste = brak pola)
            self.last_hs[info['gw']] = {k: v for k, v in zip(('dh', 'sh', 'ah'), hs) if v}
        elif isinstance(hs, dict):
            self.last_hs[info['gw']] = hs
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
            if data.get('g') is not None and data.get('g') != info.get('gw'):
                return
            if ok == 1:
                info['done'] = True
                self.outgoing.pop(tid, None)
                self._log('info', 'XFER', f"✅ ACK ok tid={tid}")
                return
            chunks = info['chunks']
        self._log('warn', 'XFER', f"🔁 NACK tid={tid} retransmit {miss}")
        for i in miss:
            if 0 <= i < len(chunks):
                self.send({"t": f"{self.p}_chunk", "tid": tid, "g": info.get('gw'),
                           "s": i, "d": chunks[i]})
        with self.lock:                                   # rt_end wysyla WYLACZNIE petla _tx_body (po chunkach)
            inf = self.outgoing.get(tid)
            if inf is not None:
                inf['resend_end'] = True

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
