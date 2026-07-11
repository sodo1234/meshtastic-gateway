"""AnomalyReconciler — gateway-side, autorytatywna reconcyliacja AKTYWNYCH anomalii (step 5+).

Bramka = źródło prawdy. Ten moduł spina detekcję (silniki) ze stanem (store) i realizuje
wymogi użytkownika, których nie pokrywały pojedyncze silniki one-shot:

  4. Bramka ZAWSZE wysyła aktualną LISTĘ AKTYWNYCH anomalii jako JEDEN skompresowany blob
     (ReliableTransfer kind='ansnap' — dokładnie jak harmonogram) → supervisor reconciluje
     dodane ORAZ usunięte. Blob nadawany TYLKO gdy zbiór aktywnych się ZMIENIŁ (hash) —
     steady-state = zero uplinku (oszczędność LoRa).
  6. Cykliczny re-scan silników (surowa prawda ze stanu z2m); anomalia RĘCZNIE wyczyszczona
     ale WCIĄŻ występująca → PONOWNIE wystawiona po oknie wyciszenia `mute_window`
     (ręczny clear = tymczasowe wyciszenie, nie trwałe skasowanie prawdy).
  7. Diagnostyka: wymuszony pełny re-scan + zrzut aktywnych + porównanie truth↔store →
     raport rozbieżności (missing/stale/muted/rearmed) → callback (HA sensor) + log.

ZASADY (oszczędność + jedna ścieżka nadawania):
  - Moduł NIE woła LoRa wprost. Uplink (blob) idzie przez wstrzykiwany `snapshot_send_fn`,
    który orkiestrator wiąże z JEDNĄ zunifikowaną kolejką nadawczą (priorytet P2, half-duplex).
  - Re-emisja lokalna (`readd_fn`) NIE generuje osobnych pakietów `ab` — anomalia wraca do
    lokalnego store i pojawia się w kolejnym blobie (jedna ścieżka nadawania, zero spamu).
  - `probes` zwracają SUROWĄ prawdę silników (nie filtrowaną przez ACK) — to referencja, do
    której dostraja się store.

Kontrakt wstrzyknięć:
  probes            : list[callable() -> list[[sid, code, val?], ...]]  (surowa prawda silników)
  resolve_dev(sid)  -> nazwa | None
  dev_to_sid(dev)   -> sid | None
  readd_fn(sid,code,val)     -> lokalne przywrócenie do store (bez LoRa)
  snapshot_send_fn(payload)  -> ENKOLEJKUJ blob (P2). payload = {g, ts, a:[[sid,code,val?],...]}
  offline_unack(dev)         -> cofnij ręczne wyciszenie offline w silniku (re-arm offline)
  on_report(report_dict)     -> publikacja raportu diagnostyki (np. na HA bramki)
"""
import hashlib
import json
import threading
import time

from .batcher import CODE_CAT

RECOVERY = {"dn", "bo", "to", "ho", "sc"}   # kody recovery — nie są anomaliami (ignoruj w truth)


class AnomalyReconciler:
    def __init__(self, gw_id, store, probes, resolve_dev, dev_to_sid, readd_fn,
                 snapshot_send_fn=None, offline_unack=None, on_report=None,
                 logger=None, interval=180, mute_window=300,
                 delta_send_fn=None, delta_max=6, stale_grace=360):
        self.gw_id = gw_id
        self.store = store
        self.probes = probes
        self.resolve_dev = resolve_dev
        self.dev_to_sid = dev_to_sid
        self.readd = readd_fn
        self.snapshot_send_fn = snapshot_send_fn
        self.offline_unack = offline_unack
        self.on_report = on_report
        self.log = logger
        self.interval = interval
        self.mute_window = mute_window
        self._mute = {}                  # (dev,cat) → ts ręcznego clear (okno wyciszenia)
        self._last_hash = None           # hash ostatnio WYSŁANEGO blobu (dedup uplinku)
        # DELTA (2026-07-07, wymóg usera „po co wysyłać całą listę gdy zmieniła się 1 anomalia"):
        # mała zmiana (≤delta_max pozycji) → JEDEN pakiet an_d {a:dodane, r:usunięte, h:hash-po}
        # zamiast pełnego blobu (transfer wielochunkowy). Supervisor aplikuje deltę i porównuje
        # hash swojej listy — rozjazd → żąda pełnego snapshotu (dump_anom). Bootstrap/duża zmiana
        # (clear-all) → pełny blob jak dotąd. delta_send_fn = zwykły lora_send (1 pakiet).
        self.delta_send_fn = delta_send_fn
        self.delta_max = delta_max
        self._last_sent = None           # {(sid,code): val} stan ostatnio przekazany supervisorowi
        # SELF-CLEAR STALE (2026-07-07, audyt: zombie „Test 2/offline"): wpis w store, którego
        # silniki już NIE potwierdzają (zgubione recovery) → usuń po karencji stale_grace
        # (≥2 kolejne skany), zamiast wysyłać zombie w każdym blobie (churn: sup pokazuje →
        # clear → wraca). Karencja chroni przed skasowaniem świeżej anomalii w oknie wyścigu.
        self.stale_grace = stale_grace
        self._stale_since = {}           # (dev,cat) → ts pierwszego skanu bez potwierdzenia w truth
        # COALESCE UPLINKU (2026-07-11, root-cause 3-dniowej głuchoty): on_change store woła
        # send_snapshot per KAŻDĄ zmianę → przy starcie ~100 re-armów = ~100 pakietów an_d, sup
        # odpowiada dumpem na każdy rozjazd → kolejka radia pełna → sendText blokuje → watchdog
        # ubija antenę (pętla). Leading+trailing: 1. zmiana leci OD RAZU (ręczny clear = instant),
        # kolejne w oknie coalesce_s zwijają się do JEDNEJ transmisji (duża paczka → i tak snapshot).
        self.coalesce_s = 20
        self._co = {'cooldown': False, 'pending': False}
        self._lock = threading.Lock()
        self._req = threading.Event()
        self._req_diag = False
        self._req_force = False
        self.running = False

    # ── ręczne wyciszenie (wołane z handlerów clear w harnessie) ──
    def mute(self, dev, cat):
        """Zarejestruj RĘCZNY clear (dev,cat) → start okna wyciszenia. Po jego wygaśnięciu, jeśli
        warunek nadal trwa (probes), reconciler ponownie wystawi anomalię (wymóg 6)."""
        with self._lock:
            self._mute[(dev, cat)] = time.time()

    # ── lifecycle ───────────────────────────────────────
    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self._loop, daemon=True, name="anom-reconcile").start()
        if self.log:
            self.log.info('ANOM', f'🔁 AnomalyReconciler start (re-scan co {self.interval}s, '
                          f'okno wyciszenia {self.mute_window}s)')

    def stop(self):
        self.running = False
        self._req.set()

    def request(self, diagnostic=False, force_send=False):
        """Wyzwól natychmiastowy cykl (async). diagnostic=True → raport; force_send=True → wyślij
        blob nawet gdy niezmieniony (np. na żądanie supervisora `ansnap_req` / przycisk diag)."""
        with self._lock:
            self._req_diag = self._req_diag or diagnostic
            self._req_force = self._req_force or force_send
        self._req.set()

    def _loop(self):
        while self.running:
            fired = self._req.wait(self.interval)
            self._req.clear()
            with self._lock:
                diag, force = self._req_diag, self._req_force
                self._req_diag = self._req_force = False
            if not self.running:
                break
            try:
                self.run_once(diagnostic=diag, force_send=force)
            except Exception as e:
                if self.log:
                    self.log.error('ANOM', f'reconcile: {e}')

    # ── skan surowej prawdy silników ────────────────────
    def _scan_truth(self):
        truth = {}                       # (dev,cat) → (sid,code,val)
        for probe in self.probes:
            try:
                rows = probe() or []
            except Exception as e:
                if self.log:
                    self.log.warn('ANOM', f'probe: {e}')
                rows = []
            for row in rows:
                sid, code = row[0], row[1]
                val = row[2] if len(row) > 2 else None
                if code in RECOVERY:
                    continue
                dev = self.resolve_dev(sid)
                if not dev:
                    continue
                truth[(dev, CODE_CAT.get(code, "other"))] = (sid, code, val)
        return truth

    def _store_active(self):
        return {(dev, cat): (a.get("code"), a.get("value"))
                for (g, dev, cat), a in self.store.anomalies.items() if g == self.gw_id}

    def _build_blob(self):
        """Autorytatywna lista aktywnych z LOKALNEGO store (to co bramka realnie pokazuje)."""
        rows = []
        for (g, dev, cat), a in self.store.anomalies.items():
            if g != self.gw_id:
                continue
            sid = self.dev_to_sid(dev)
            if sid is None:
                continue
            code, val = a.get("code"), a.get("value")
            rows.append([sid, code] if val is None else [sid, code, val])
        rows.sort(key=lambda r: (r[0], r[1]))
        return rows

    @staticmethod
    def blob_hash(rows):
        """Hash listy aktywnych — TA SAMA formuła po obu stronach (supervisor weryfikuje deltę)."""
        return hashlib.md5(json.dumps(rows, separators=(',', ':')).encode()).hexdigest()[:8]

    def _transmit(self, blob, force_send, ts):
        """Wyślij zmianę stanu anomalii: DELTA (1 pakiet an_d) gdy mała, pełny blob gdy bootstrap/
        duża/force. Zwraca True gdy coś poszło. Aktualizuje _last_hash/_last_sent."""
        h = self.blob_hash(blob)
        if not force_send and h == self._last_hash:
            return False
        cur = {(r[0], r[1]): (r[2] if len(r) > 2 else None) for r in blob}
        added = [r for r in blob if self._last_sent is None
                 or self._last_sent.get((r[0], r[1]), '∅') != cur[(r[0], r[1])]]
        removed = [[sid, code] for (sid, code) in (self._last_sent or {})
                   if (sid, code) not in cur]
        small = 0 < (len(added) + len(removed)) <= self.delta_max
        if (self.delta_send_fn and small and not force_send
                and self._last_sent is not None):
            self.delta_send_fn({"t": "an_d", "g": self.gw_id,
                                "a": added, "r": removed, "h": h})
            if self.log:
                self.log.info('ANOM', f'📤 [AN_D] delta +{len(added)}/-{len(removed)} '
                              f'(1 pakiet, hash={h}) zamiast blobu {len(blob)}')
        elif self.snapshot_send_fn:
            self.snapshot_send_fn({"g": self.gw_id, "ts": int(ts), "a": blob})
            if self.log:
                self.log.info('ANOM', f'📤 [ANSNAP] {len(blob)} aktywnych → blob '
                              f'(hash={h}{" force" if force_send else ""})')
        else:
            return False
        self._last_hash = h
        self._last_sent = cur
        return True

    def send_snapshot(self, force_send=False):
        """Zbuduj blob z LOKALNEGO store i wyślij (dedup po hashu; mała zmiana → delta an_d).
        NIE re-skanuje truth, NIE re-arm — do wołania ON-CHANGE (add/remove/ręczny clear).
        Re-scan+re-arm robi run_once (interval/dump). Rozprzężenie KLUCZOWE: ręczny clear NIE jest
        natychmiast cofany przez re-arm w tym samym cyklu (delta/blob leci od razu skrócona →
        supervisor usuwa; re-arm dopiero na interval, po mute_window).
        COALESCE: pierwsza zmiana transmituje natychmiast; kolejne w oknie coalesce_s → JEDNA
        zbiorcza transmisja (trailing). force_send omija okno (ansnap_req/dump)."""
        if force_send:
            with self._lock:
                self._co['pending'] = False
            return self._transmit(self._build_blob(), True, time.time())
        fire = False
        with self._lock:
            if not self._co['cooldown']:
                self._co['cooldown'] = True
                fire = True
            else:
                self._co['pending'] = True
        if not fire:
            return False
        sent = self._transmit(self._build_blob(), False, time.time())
        t = threading.Timer(self.coalesce_s, self._co_window_end)
        t.daemon = True
        t.start()
        return sent

    def _co_window_end(self):
        """Koniec okna coalesce: jeśli w oknie były zmiany → wyślij stan zbiorczo i otwórz kolejne
        okno; brak zmian → zamknij cooldown (następna zmiana znów poleci natychmiast)."""
        with self._lock:
            pend = self._co['pending']
            self._co['pending'] = False
            if not pend:
                self._co['cooldown'] = False
                return
        self._transmit(self._build_blob(), False, time.time())
        t = threading.Timer(self.coalesce_s, self._co_window_end)
        t.daemon = True
        t.start()

    # ── jeden cykl reconcyliacji ────────────────────────
    def run_once(self, diagnostic=False, force_send=False):
        now = time.time()
        truth = self._scan_truth()
        store_active = self._store_active()

        # 6. RE-ARM: warunek nadal prawdziwy, ale zniknął ze store (ręczny clear / zgubione zdarzenie)
        #    → przywróć, o ile minęło okno wyciszenia. Offline: cofnij też ACK w silniku.
        rearmed = []
        for (dev, cat), (sid, code, val) in truth.items():
            if (dev, cat) in store_active:
                continue
            with self._lock:
                mt = self._mute.get((dev, cat))
            if mt is not None and now - mt < self.mute_window:
                continue                 # świeży ręczny clear — uszanuj wyciszenie
            if cat == "offline" and self.offline_unack:
                self.offline_unack(dev)
            self.readd(sid, code, val)
            with self._lock:
                self._mute.pop((dev, cat), None)
            rearmed.append((dev, cat))
        if rearmed and self.log:
            self.log.info('ANOM', f'♻️ re-arm {len(rearmed)} anomalii wciąż trwających po clear: '
                          + ", ".join(f"{d}/{c}" for d, c in rearmed))

        # 6b. SELF-CLEAR STALE: store ma wpis, truth go nie potwierdza → po karencji usuń
        #     (store kurczy się → _transmit wyśle deltę `r` → sup usuwa; koniec zombie-churn).
        stale_cleared = []
        for (dev, cat) in list(store_active):
            if (dev, cat) in truth:
                with self._lock:
                    self._stale_since.pop((dev, cat), None)
                continue
            with self._lock:
                first = self._stale_since.setdefault((dev, cat), now)
            if now - first >= self.stale_grace:
                self.store.remove_cat(self.gw_id, dev, cat)
                with self._lock:
                    self._stale_since.pop((dev, cat), None)
                stale_cleared.append((dev, cat))
        if stale_cleared and self.log:
            self.log.info('ANOM', '🧹 self-clear stale (silniki nie potwierdzają): '
                          + ", ".join(f"{d}/{c}" for d, c in stale_cleared))

        # 4. AUTORYTATYWNY stan → supervisor: mała zmiana = delta an_d (1 pakiet), inaczej pełny
        #    blob; tylko przy zmianie zbioru, chyba że force_send (dump → zawsze pełny snapshot).
        blob = self._build_blob()
        h = self.blob_hash(blob)
        sent = self._transmit(blob, force_send, now)

        report = self._report(truth, store_active, rearmed, blob, h, sent)
        if diagnostic:
            if self.log:
                self.log.info('ANOM', f'🔬 DIAG {self.gw_id}: truth={report["truth_n"]} '
                              f'store={report["store_n"]} missing={len(report["missing"])} '
                              f'stale={len(report["stale"])} muted={len(report["muted"])}')
            if self.on_report:
                try:
                    self.on_report(report)
                except Exception as e:
                    if self.log:
                        self.log.warn('ANOM', f'on_report: {e}')
        return report

    def _report(self, truth, store_active, rearmed, blob, h, sent):
        tk, sk = set(truth), set(store_active)
        with self._lock:
            muted = [f"{d}/{c}" for (d, c), ts in self._mute.items()
                     if time.time() - ts < self.mute_window]
        # missing: warunek prawdziwy, ale NIE pokazany (świeżo wyciszony lub zgubione zdarzenie)
        # stale:   pokazany w store, ale warunek już NIE trwa (zgubione recovery → kandydat do audytu)
        return {"gw": self.gw_id, "ts": int(time.time()),
                "truth_n": len(tk), "store_n": len(sk),
                "blob_n": len(blob), "blob_hash": h, "snapshot_sent": sent,
                "missing": sorted(f"{d}/{c}" for d, c in (tk - sk)),
                "stale": sorted(f"{d}/{c}" for d, c in (sk - tk)),
                "muted": sorted(muted),
                "rearmed": sorted(f"{d}/{c}" for d, c in rearmed)}
