#!/usr/bin/env python3
"""test_step0_file_lora.py — UNIWERSALNY transfer plikow przez LoRa (Meshtastic).

Nie jest czescia harnessu step5 — zero importow z gateway/supervisor/modules.
Przejmuje sprawdzony KSZTALT protokolu z `tge_lora.py` (stop-and-wait per chunk),
ale przesyla DOWOLNY plik bajt w bajt, zamiast 96 cen TGE.

Uruchamiany z Meshtastic Launchera BEZ argumentow — rola wynika z katalogow:

    ~/lora_transfer/wyslij/     <- wrzuc plik TU  => ta maszyna NADAJE
    ~/lora_transfer/odebrane/   <- tu ladują pliki odebrane
    ~/lora_transfer/wyslane/    <- tu ląduje plik po udanym nadaniu

Pusty `wyslij/` = maszyna NASLUCHUJE. Dzieki temu ten sam skrypt siedzi w
dropdownie na obu maszynach i nie trzeba podawac zadnych flag (Launcher UI
nie ma pola na argumenty).

Protokol:
    f_begin(tid,nm,n,sz,h,z) <-> f_back(tid)          # begin JEST ACK-owany
      -> [ f_chunk(s,d) <-> f_cack(s) ] x n
      -> f_end(tid,n,h) -> f_ack(ok)

Dlaczego begin jest ACK-owany (roznica vs tge_lora.py): tam begin szedl
fire-and-forget i gubil sie na RF, a metadane trzeba bylo doklejac do ostatniego
chunku. Przy dowolnym pliku metadanych jest wiecej (nazwa!) i nie mieszcza sie w
ramce razem z danymi — wiec zamiast redundancji ACK-ujemy sam begin.

Kodek: zlib(9) -> base64, ale TYLKO gdy zlib realnie zmniejsza (flaga `z`).
Dla CSV zysk jest duzy (powtarzalne daty/separatory); dla juz spakowanych
wejsc (zip/png) zlib by spuchl, wiec wtedy leci samo base64.

ROZMIAR RAMKI — zmierzone live gw<->sup 2026-07-19 (patrz tge_lora.py):
ramka ~130 B NIE dochodzi, ~90 B dochodzi. Stad chunk_size=50 znakow base64
(=37 B danych) -> ramka ~85 B. NIE podnosic bez pomiaru na antenie.

PRZEPUSTOWOSC: ~37 B pliku na ramke, cooldown 3 s -> ~12 B/s netto.
Plik 1 kB ~ 1 min, 10 kB ~ 10 min, 100 kB ~ 1.5 h. Skrypt liczy ETA przed
startem i wypisuje ja w logu — dla duzych plikow to jest ta liczba, na ktora
trzeba spojrzec ZANIM sie odejdzie od kompa.

Log leci na stdout ORAZ do /tmp/test_transport.log — Launcher tail-uje ten plik
(nazwa skryptu zaczyna sie od `test_`), wiec postep widac na zywo w UI.

Uzycie recznie (poza Launcherem):
    python3 test_step0_file_lora.py                 # auto: wyslij/ decyduje o roli
    python3 test_step0_file_lora.py --send plik.csv --port /dev/ttyUSB0
    python3 test_step0_file_lora.py --recv ./odebrane --port /dev/ttyUSB0
    python3 test_step0_file_lora.py --send plik.csv --loopback --loss 0.3
"""
import argparse
import base64
import glob
import hashlib
import io
import json
import os
import random
import string
import sys
import threading
import time
import zlib

# ── Parametry transportu (NIE ruszac bez pomiaru na antenie) ──
CHUNK_SIZE = 50            # znakow base64 w jednej ramce -> ramka ~85 B
CHUNK_DELAY = 3.0          # cooldown LoRa miedzy ramkami
# 12 s bylo za malo: przy bramce 3 s po OBU stronach + airtime + kontencja z G1
# realne RTT dochodzilo do 31 s (pomiar live 2026-07-26) -> nadawca retransmitowal
# chunki, ktore odbiornik JUZ mial, i sam sobie zapychal kanal duplikatami.
CHUNK_ACK_TIMEOUT = 25.0
TX_MIN_GAP = 3.0           # TWARDY cooldown miedzy KAZDYMI dwiema ramkami tego wezla
CHUNK_RETRIES = 6
BEGIN_RETRIES = 8
END_RETRIES = 4
FRAME_WARN = 150           # "220B max, 150B operational"

BASE_DIR = os.path.expanduser('~/lora_transfer')
DIR_OUT = os.path.join(BASE_DIR, 'wyslij')
DIR_SENT = os.path.join(BASE_DIR, 'wyslane')
DIR_IN = os.path.join(BASE_DIR, 'odebrane')
LOG_PATH = '/tmp/test_transport.log'

_log_lock = threading.Lock()


def log(msg):
    """Jedna linia -> stdout (pane tmux) + /tmp/test_transport.log (stream Launchera)."""
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    with _log_lock:
        print(line, flush=True)
        try:
            with io.open(LOG_PATH, 'a', encoding='utf-8') as fh:
                fh.write(line + '\n')
        except OSError:
            pass                      # brak /tmp (np. test na Windows) nie moze zabic transferu


def banner(title, rows):
    log('=' * 64)
    log(f"  {title}")
    for k, v in rows:
        log(f"    {k:.<22} {v}")
    log('=' * 64)


def human_eta(seconds):
    if seconds < 90:
        return f"{seconds:.0f} s"
    if seconds < 5400:
        return f"{seconds / 60:.1f} min"
    return f"{seconds / 3600:.1f} h"


# ─────────────────────────────────────────────────────────────────────
# Kodek — czysty, testowalny bez anteny
# ─────────────────────────────────────────────────────────────────────

def pack(raw):
    """bytes -> (base64_str, z_flag). zlib tylko gdy REALNIE zmniejsza."""
    z = zlib.compress(raw, 9)
    if len(z) < len(raw):
        return base64.b64encode(z).decode(), 1
    return base64.b64encode(raw).decode(), 0


def unpack(b64, z_flag):
    raw = base64.b64decode(b64)
    return zlib.decompress(raw) if z_flag else raw


def md5hex(raw):
    return hashlib.md5(raw).hexdigest()


def safe_name(name):
    """Odbiorca NIGDY nie ufa nazwie z RF — tylko basename, bez separatorow sciezki."""
    name = os.path.basename(str(name or '').replace('\\', '/'))
    name = ''.join(c for c in name if c not in '\0')
    return name or 'plik.bin'


# ─────────────────────────────────────────────────────────────────────
# Transfer — stop-and-wait per chunk
# ─────────────────────────────────────────────────────────────────────

class FileTransfer:
    def __init__(self, send_fn, chunk_size=CHUNK_SIZE, chunk_delay=CHUNK_DELAY,
                 chunk_ack_timeout=CHUNK_ACK_TIMEOUT, chunk_retries=CHUNK_RETRIES,
                 on_received=None):
        self.send = send_fn
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay
        self.chunk_ack_timeout = chunk_ack_timeout
        self.chunk_retries = chunk_retries
        self.on_received = on_received
        self.incoming = {}
        self.outgoing = {}
        self.completed = {}
        self.last_verdict = None
        self.lock = threading.Lock()

    @staticmethod
    def gen_tid():
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))

    def _wait(self, tid, check):
        """Czeka na warunek nad wpisem outgoing. None = wpis zniknal (= sukces calosci)."""
        waited = 0.0
        while waited < self.chunk_ack_timeout:
            time.sleep(0.4)
            waited += 0.4
            with self.lock:
                info = self.outgoing.get(tid)
                if info is None:
                    return None
                if check(info):
                    return True
        return False

    # ── nadawanie ────────────────────────────────────────
    def send_file(self, path, blocking=True):
        raw = io.open(path, 'rb').read()
        name = os.path.basename(path)
        full_md5 = md5hex(raw)
        b64, z_flag = pack(raw)
        chunks = [b64[i:i + self.chunk_size] for i in range(0, len(b64), self.chunk_size)] or ['']
        tid = self.gen_tid()
        eta = len(chunks) * (self.chunk_delay + 1.2)
        banner(f"NADAJE: {name}", [
            ('tid', tid),
            ('rozmiar zrodla', f"{len(raw)} B"),
            ('na antenie', f"{len(b64)} B (zlib={'tak' if z_flag else 'nie'}, "
                           f"redukcja {len(raw) / max(len(b64), 1):.2f}x)"),
            ('chunki', f"{len(chunks)} x {self.chunk_size} znakow"),
            ('md5 zrodla', full_md5),
            ('ETA', f"~{human_eta(eta)} (przy zerowych retry)"),
        ])

        with self.lock:
            self.outgoing[tid] = {'chunks': chunks, 'h': full_md5[:8], 'acked': set(),
                                  'begin_ok': False, 'done': False}
        self.last_verdict = None
        t0 = time.time()
        stats = {'retries': 0}

        def _tx():
            # 1) BEGIN — ACK-owany, bo niesie nazwe pliku (nie da sie jej dokleic do chunku)
            begin = {"t": "f_begin", "tid": tid, "nm": name[:48], "n": len(chunks),
                     "sz": len(raw), "h": full_md5[:8], "z": z_flag}
            for attempt in range(BEGIN_RETRIES):
                self.send(begin)
                res = self._wait(tid, lambda inf: inf['begin_ok'])
                if res is None:
                    self.last_verdict = 'ok'
                    return
                if res:
                    log(f"[TX] begin potwierdzony (proba {attempt + 1})")
                    break
                stats['retries'] += 1
                log(f"[TX] begin bez f_back — retry {attempt + 1}/{BEGIN_RETRIES}")
            else:
                log("[TX] BLAD: odbiorca nie potwierdzil begin -> przerwane "
                    "(druga maszyna nie slucha? zla antena/port?)")
                self.last_verdict = 'failed'
                with self.lock:
                    self.outgoing.pop(tid, None)
                return

            time.sleep(self.chunk_delay)

            # 2) CHUNKI — stop-and-wait, kazdy retransmitowany do skutku
            for i, chunk in enumerate(chunks):
                ok = False
                c0 = time.time()
                for attempt in range(self.chunk_retries + 1):
                    self.send({"t": "f_chunk", "tid": tid, "s": i, "d": chunk})
                    res = self._wait(tid, lambda inf: i in inf['acked'])
                    if res is None:
                        # Odbiorca zlozyl komplet juz na ostatnim chunku i odeslal f_ack ok=1
                        # ZANIM domknelismy petle. Sciezka NORMALNA przy dobrym lauczu.
                        log(f"[TX] ACK ok (odbiorca zlozyl na ostatnim chunku)")
                        self.last_verdict = 'ok'
                        return
                    if res:
                        ok = True
                        break
                    stats['retries'] += 1
                    if attempt < self.chunk_retries:
                        log(f"[TX] chunk {i + 1}/{len(chunks)} brak cack — "
                            f"retry {attempt + 1}/{self.chunk_retries}")
                if not ok:
                    log(f"[TX] BLAD: chunk {i + 1}/{len(chunks)} bez potwierdzenia -> przerwane")
                    self.last_verdict = 'failed'
                    with self.lock:
                        self.outgoing.pop(tid, None)
                    return
                done_n = i + 1
                el = time.time() - t0
                rate = done_n / max(el, 0.001)
                log(f"[TX] chunk {done_n}/{len(chunks)} OK "
                    f"({time.time() - c0:.1f}s) — {100 * done_n / len(chunks):.0f}%, "
                    f"pozostalo ~{human_eta((len(chunks) - done_n) / max(rate, 1e-6))}")

            # 3) END — guard: wpis mogl juz zniknac (finalizacja na ostatnim chunku)
            with self.lock:
                if self.outgoing.get(tid) is None:
                    log("[TX] ACK ok (odbiorca zlozyl na ostatnim chunku)")
                    self.last_verdict = 'ok'
                    return
            for attempt in range(END_RETRIES):
                self.send({"t": "f_end", "tid": tid, "n": len(chunks), "h": full_md5[:8]})
                res = self._wait(tid, lambda inf: inf.get('done'))
                if res is None or res:
                    self.last_verdict = 'ok'
                    return
                log(f"[TX] f_end retry {attempt + 1}/{END_RETRIES}")
            # Wszystkie chunki potwierdzone, zginelo samo POTWIERDZENIE calosci.
            # To NIE utrata danych — nazwanie tego bledem prowokuje retransmisje calego pliku.
            log("[TX] UWAGA: wszystkie chunki potwierdzone, brak finalnego ACK. "
                "Dane najpewniej dostarczone — sprawdz odbiornik.")
            self.last_verdict = 'delivered_unconfirmed'
            with self.lock:
                self.outgoing.pop(tid, None)

        def _tx_guarded():
            try:
                _tx()
            except Exception as e:
                import traceback
                log(f"[TX] WYJATEK w watku nadawcy: {e!r}\n{traceback.format_exc()}")
                self.last_verdict = 'failed'
            finally:
                el = time.time() - t0
                verdict = {'ok': 'ZAKONCZONY SUKCESEM',
                           'delivered_unconfirmed': 'DOSTARCZONY (bez potwierdzenia CRC)',
                           'failed': 'NIEUDANY'}.get(self.last_verdict, 'NIEUDANY')
                banner(f"TRANSFER {verdict}", [
                    ('plik', name),
                    ('tid', tid),
                    ('czas', human_eta(el)),
                    ('chunki', len(chunks)),
                    ('retransmisje', stats['retries']),
                    ('srednio', f"{len(raw) / max(el, 0.001):.1f} B/s"),
                    ('md5 zrodla', full_md5),
                    ('werdykt', self.last_verdict),
                ])

        th = threading.Thread(target=_tx_guarded, daemon=True, name=f"file-tx-{tid}")
        th.start()
        if blocking:
            th.join()
        return self.last_verdict

    def handle_back(self, data):
        with self.lock:
            info = self.outgoing.get(data.get('tid'))
            if info is not None:
                info['begin_ok'] = True

    def handle_cack(self, data):
        with self.lock:
            info = self.outgoing.get(data.get('tid'))
            s = data.get('s')
            if info is not None and s is not None:
                info['acked'].add(s)

    def handle_ack(self, data):
        tid = data.get('tid')
        with self.lock:
            info = self.outgoing.get(tid)
            if not info:
                return
            if data.get('ok') == 1:
                info['done'] = True
                self.outgoing.pop(tid, None)
                log(f"[TX] ACK ok tid={tid}")
                return
            chunks = info['chunks']
        miss = data.get('miss', [])
        log(f"[TX] NACK tid={tid} retransmisja {miss}")
        for i in miss:
            if 0 <= i < len(chunks):
                self.send({"t": "f_chunk", "tid": tid, "s": i, "d": chunks[i]})

    # ── odbior ───────────────────────────────────────────
    def handle_begin(self, data):
        tid = data.get('tid')
        with self.lock:
            self.incoming[tid] = {'chunks': {}, 'total': data.get('n', 0),
                                  'h': data.get('h', ''), 'nm': safe_name(data.get('nm')),
                                  'sz': data.get('sz', 0), 'z': data.get('z', 0),
                                  't0': time.time()}
        info = self.incoming[tid]
        self.send({"t": "f_back", "tid": tid})
        banner(f"ODBIERAM: {info['nm']}", [
            ('tid', tid),
            ('rozmiar zrodla', f"{info['sz']} B"),
            ('chunki', info['total']),
            ('zlib', 'tak' if info['z'] else 'nie'),
            ('md5 (skrot)', info['h']),
        ])

    def handle_chunk(self, data):
        tid = data.get('tid')
        s = data.get('s', 0)
        with self.lock:
            if tid in self.completed:
                # Spozniona retransmisja JUZ ZLOZONEGO pliku. Bez tego guardu tworzylismy
                # pusty wpis-sierote i zaczynali NACK-owac "braki" pliku, ktory mamy w
                # calosci — nadawca dostawal falszywy negatyw (zaobserwowane live 2026-07-26,
                # transfer pgp8: plik zapisany 08:05:02, a mimo to NACK [0,1,2,3,4]).
                self.send({"t": "f_ack", "tid": tid, "ok": 1, "miss": []})
                log(f"[RX] spozniony chunk {s} dla ZLOZONEGO tid={tid} — re-ACK ok")
                return
            info = self.incoming.get(tid)
            if info is None:
                # Chunk bez begin = begin zginal na RF. Bez nazwy/n nie zlozymy pliku,
                # ale zbieramy dane i prosimy nadawce o powtorzenie begin.
                self.incoming[tid] = {'chunks': {}, 'total': None, 'h': None, 'nm': None,
                                      'sz': 0, 'z': 0, 't0': time.time()}
                info = self.incoming[tid]
                log(f"[RX] chunk {s} tid={tid} BEZ begin — czekam na powtorke begin")
            info['chunks'][s] = data.get('d', '')
            have, total = len(info['chunks']), info['total']
        self.send({"t": "f_cack", "tid": tid, "s": s})
        if total:
            log(f"[RX] chunk {s + 1}/{total} (mam {have}/{total}, {100 * have / total:.0f}%)")
        self._try_finalize(tid, nack=False)

    def _try_finalize(self, tid, nack=False):
        with self.lock:
            info = self.incoming.get(tid)
            if not info or not info.get('total'):
                return None
            total = info['total']
            missing = [i for i in range(total) if i not in info['chunks']]
            if missing:
                if nack:
                    log(f"[RX] brakujace chunki: {missing[:5]}")
                    self.send({"t": "f_ack", "tid": tid, "ok": 0, "miss": missing[:5]})
                return None
            b64 = ''.join(info['chunks'][i] for i in range(total))
            meta = dict(info)
        try:
            raw = unpack(b64, meta['z'])
        except Exception as e:
            log(f"[RX] BLAD dekodowania tid={tid}: {e}")
            if nack:
                self.send({"t": "f_ack", "tid": tid, "ok": 0, "miss": []})
            return None
        got = md5hex(raw)
        if meta.get('h') and got[:8] != meta['h']:
            log(f"[RX] md5 MISMATCH tid={tid}: {got[:8]} != {meta['h']}")
            if nack:
                self.send({"t": "f_ack", "tid": tid, "ok": 0, "miss": []})
            return None
        with self.lock:
            self.incoming.pop(tid, None)
            self.completed[tid] = time.time()
        self.send({"t": "f_ack", "tid": tid, "ok": 1, "miss": []})
        el = time.time() - meta['t0']
        banner("PLIK ODEBRANY — KOMPLET, md5 ZGODNY", [
            ('plik', meta['nm']),
            ('tid', tid),
            ('bajty', f"{len(raw)} B (zapowiadane {meta['sz']} B)"),
            ('chunki', total),
            ('czas', human_eta(el)),
            ('md5', got),
        ])
        if self.on_received:
            self.on_received(meta['nm'], raw)
        return meta['nm'], raw

    def handle_end(self, data):
        tid = data.get('tid')
        with self.lock:
            info = self.incoming.get(tid)
            if info is None:
                if tid in self.completed:
                    # Zlozone wczesniej, finalny ACK zginal -> re-ACK idempotentnie,
                    # inaczej nadawca raportuje falszywy negatyw.
                    self.send({"t": "f_ack", "tid": tid, "ok": 1, "miss": []})
                return None
            if not info.get('total'):
                info['total'] = data.get('n', 0)
                info['h'] = info.get('h') or data.get('h', '')
        return self._try_finalize(tid, nack=True)

    def dispatch(self, data):
        t = data.get('t')
        if t == 'f_begin':
            self.handle_begin(data)
        elif t == 'f_chunk':
            self.handle_chunk(data)
        elif t == 'f_end':
            self.handle_end(data)
        elif t == 'f_back':
            self.handle_back(data)
        elif t == 'f_cack':
            self.handle_cack(data)
        elif t == 'f_ack':
            self.handle_ack(data)


# ─────────────────────────────────────────────────────────────────────
# Transporty
# ─────────────────────────────────────────────────────────────────────

class LoopbackTransport:
    """Obie strony w jednym procesie — weryfikacja kodeka i skladania BEZ anteny."""

    def __init__(self, loss=0.0, seed=None):
        self.loss = loss
        self.rnd = random.Random(seed)
        self.tx = None
        self.rx = None
        self.dropped = 0
        self._lock = threading.Lock()

    def _deliver(self, target, data):
        if data.get('t') not in ('f_cack', 'f_back') and self.rnd.random() < self.loss:
            with self._lock:
                self.dropped += 1
            log(f"    (utracono {data.get('t')} s={data.get('s')})")
            return
        threading.Thread(target=target.dispatch, args=(data,), daemon=True).start()

    def send_from_tx(self, data):
        self._deliver(self.rx, data)

    def send_from_rx(self, data):
        self._deliver(self.tx, data)


class MeshtasticTransport:
    """SerialInterface + subskrypcja tekstu. sendText() z timeoutem.

    sendText() potrafi LEGALNIE blokowac pod backpressure — kazde wyslanie idzie
    w watku z limitem czasu, zeby nie zablokowac petli protokolu.
    """

    def __init__(self, port=None, tx_timeout=30.0):
        self.tx_timeout = tx_timeout
        self.transfer = None
        self._last_tx = 0.0
        self._tx_lock = threading.Lock()
        self.foreign = 0                 # ramki CUDZE na kanale (G1 itd.) = kontencja
        self.rf = []                     # (rssi, snr) ostatnich naszych ramek
        import meshtastic.serial_interface as msi
        from pubsub import pub
        self.iface = msi.SerialInterface(devPath=port) if port else msi.SerialInterface()
        pub.subscribe(self._on_rx, "meshtastic.receive.text")
        log(f"[ANT] polaczono {port or '(auto)'}")

    def _on_rx(self, packet, interface=None):
        rssi = packet.get('rxRssi') if isinstance(packet, dict) else None
        snr = packet.get('rxSnr') if isinstance(packet, dict) else None
        try:
            text = packet.get('decoded', {}).get('text') if isinstance(packet, dict) else None
            if not text:
                return
            data = json.loads(text)
        except Exception:
            self.foreign += 1
            return                       # nie nasz ruch — ignoruj (ale policz: to kontencja)
        if not str(data.get('t', '')).startswith('f_'):
            self.foreign += 1            # cudzy protokol na tym kanale (harness G1/G2)
            return
        if rssi is not None:
            self.rf.append((rssi, snr))
            self.rf = self.rf[-200:]
        if self.transfer:
            self.transfer.dispatch(data)

    def rf_summary(self):
        if not self.rf:
            return "brak pomiarow RF"
        rssis = [r for r, _ in self.rf]
        snrs = [s for _, s in self.rf if s is not None]
        out = f"RSSI min/sr/max {min(rssis)}/{sum(rssis) / len(rssis):.0f}/{max(rssis)} dBm"
        if snrs:
            out += f", SNR min/sr/max {min(snrs):.1f}/{sum(snrs) / len(snrs):.1f}/{max(snrs):.1f} dB"
        return out + f", ramek obcych na kanale: {self.foreign}"

    def send(self, data):
        text = json.dumps(data, separators=(',', ':'))
        if len(text) > FRAME_WARN:
            log(f"[ANT] UWAGA ramka {len(text)}B > {FRAME_WARN}B — ryzyko gubienia")
        # TWARDY cooldown: bez tego petla chunkow strzelala kolejna ramka natychmiast po
        # ACK-u (0.4 s po poprzedniej), lamiac zasade projektu "3.0 s" i zwiekszajac kolizje.
        with self._tx_lock:
            gap = TX_MIN_GAP - (time.time() - self._last_tx)
            if gap > 0:
                time.sleep(gap)
            self._last_tx = time.time()
        def _tx():
            try:
                self.iface.sendText(text)
            except Exception as e:
                log(f"[ANT] BLAD sendText: {e}")
        th = threading.Thread(target=_tx, daemon=True)
        th.start()
        th.join(self.tx_timeout)
        if th.is_alive():
            log(f"[ANT] sendText() zablokowany >{self.tx_timeout}s (backpressure)")


def autodetect_port():
    """Antena = CP2102 po /dev/serial/by-id (stabilne). Goly ttyUSB0 bywa donglem Zigbee!"""
    cands = sorted(glob.glob('/dev/serial/by-id/*CP210*'))
    if len(cands) == 1:
        return cands[0]
    if not cands:
        log("[ANT] BLAD: nie znaleziono CP2102 w /dev/serial/by-id/ — antena odpieta?")
        return None
    log(f"[ANT] BLAD: kilka kandydatow CP210x: {cands} — podaj --port recznie")
    return None


# ─────────────────────────────────────────────────────────────────────
# Tryby
# ─────────────────────────────────────────────────────────────────────

def make_receiver_sink(out_dir):
    os.makedirs(out_dir, exist_ok=True)

    def _sink(name, raw):
        dest = os.path.join(out_dir, name)
        if os.path.exists(dest):
            stamp = time.strftime('%H%M%S')
            root, ext = os.path.splitext(name)
            dest = os.path.join(out_dir, f"{root}.{stamp}{ext}")   # nie nadpisuj poprzedniego
        with io.open(dest, 'wb') as fh:
            fh.write(raw)
        log(f"[RX] zapisano: {dest}")
    return _sink


def run_receiver(port, out_dir, once=False, timeout=0):
    ant = MeshtasticTransport(port=port)
    tr = FileTransfer(ant.send, on_received=make_receiver_sink(out_dir))
    ant.transfer = tr
    log(f"NASLUCHUJE — pliki beda ladowac w: {out_dir}")
    log("(Stop w Launcherze konczy nasluch)")
    deadline = time.time() + timeout if timeout else 0
    last_beat = time.time()
    try:
        while True:
            time.sleep(1.0)
            if once and tr.completed:
                log("[RX] plik odebrany, koncze (--once)")
                return 0
            if deadline and time.time() > deadline:
                log("[RX] timeout — nic nie odebrano")
                return 1
            if time.time() - last_beat > 60:
                last_beat = time.time()
                log(f"[RX] nasluchuje... (odebrane: {len(tr.completed)}) | {ant.rf_summary()}")
    except KeyboardInterrupt:
        log("przerwano")
        return 130


def run_sender(port, files):
    ant = MeshtasticTransport(port=port)
    tr = FileTransfer(ant.send)
    ant.transfer = tr
    rc = 0
    for path in files:
        verdict = tr.send_file(path)
        log(f"[ANT] diagnostyka lacza: {ant.rf_summary()}")
        if verdict in ('ok', 'delivered_unconfirmed'):
            os.makedirs(DIR_SENT, exist_ok=True)
            try:
                os.replace(path, os.path.join(DIR_SENT, os.path.basename(path)))
                log(f"[TX] przeniesiono do {DIR_SENT}/ (nie wysle sie drugi raz)")
            except OSError as e:
                log(f"[TX] nie udalo sie przeniesc pliku: {e}")
            rc = max(rc, 0 if verdict == 'ok' else 3)
        else:
            rc = 1
            log(f"[TX] plik ZOSTAJE w {DIR_OUT}/ — mozna ponowic Start")
    return rc


def run_loopback(path, loss, seed):
    got = {}
    link = LoopbackTransport(loss=loss, seed=seed)
    tx = FileTransfer(link.send_from_tx, chunk_delay=0.05, chunk_ack_timeout=2.0)
    rx = FileTransfer(link.send_from_rx, chunk_delay=0.05,
                      on_received=lambda n, r: got.update({'name': n, 'raw': r}))
    link.tx, link.rx = tx, rx
    tx.send_file(path)
    time.sleep(0.5)
    src = io.open(path, 'rb').read()
    log(f"utracone pakiety: {link.dropped}")
    if not got:
        log("WYNIK: NIEPOWODZENIE — nic nie odebrano")
        return 1
    if got['raw'] != src or got['name'] != os.path.basename(path):
        log(f"WYNIK: NIEZGODNOSC (nazwa {got['name']}, {len(got['raw'])} vs {len(src)} B)")
        return 1
    log("WYNIK: OK — odebrane bajt w bajt zgodne ze zrodlem")
    return 0 if tx.last_verdict == 'ok' else 3


def auto_mode(port):
    """Bez argumentow (Launcher): katalog `wyslij/` decyduje kto nadaje, kto sluc­ha."""
    for d in (DIR_OUT, DIR_IN, DIR_SENT):
        os.makedirs(d, exist_ok=True)
    files = sorted(p for p in glob.glob(os.path.join(DIR_OUT, '*'))
                   if os.path.isfile(p))
    if files:
        log(f"TRYB: NADAJNIK — {len(files)} plik(ow) w {DIR_OUT}/")
        return run_sender(port, files)
    log(f"TRYB: ODBIORNIK — {DIR_OUT}/ jest pusty")
    return run_receiver(port, DIR_IN)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--send', metavar='PLIK')
    ap.add_argument('--recv', metavar='KATALOG')
    ap.add_argument('--port')
    ap.add_argument('--loopback', action='store_true', help='test bez anteny, w pamieci')
    ap.add_argument('--loss', type=float, default=0.0)
    ap.add_argument('--seed', type=int, default=None)
    ap.add_argument('--once', action='store_true', help='--recv: zakoncz po pierwszym pliku')
    ap.add_argument('--timeout', type=float, default=0)
    args = ap.parse_args()

    if args.loopback:
        if not args.send:
            print("--loopback wymaga --send PLIK", file=sys.stderr)
            return 2
        return run_loopback(args.send, args.loss, args.seed)

    port = args.port or autodetect_port()
    if not port:
        return 2
    if args.send:
        return run_sender(port, [args.send])
    if args.recv:
        return run_receiver(port, args.recv, once=args.once, timeout=args.timeout)
    return auto_mode(port)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except FileNotFoundError as e:
        log(f"BLAD: nie ma pliku: {e.filename}")
        sys.exit(2)
