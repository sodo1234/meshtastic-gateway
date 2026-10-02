"""SupervisorData — Step 3 supervisor side: `b` packets → HA entity states.

Receives batched deltas, reverse-maps short id → device name (via
SupervisorDiscovery), MERGES into the device's retained state JSON and
republishes. The merge is the key Step 3 fix: ha.pub_device_state full-replaces
the retained topic, so a partial delta (just temp) must not wipe humidity/battery.

Any incoming `b` ⇒ device available=ON + last_seen refresh (online cascade).
"""
import time

from .z2m_reader import decode_short, encode_short, state_hash


def _now_str(epoch=None):
    return time.strftime('%d.%m %H:%M:%S', time.localtime(epoch))      # data + godzina dla last_seen


class SupervisorData:
    def __init__(self, ha_entities, discovery, logger=None, on_avail=None,
                 unknown_window=300, time_fn=None):
        self.ha = ha_entities          # HAEntities — pub_device_state
        self.disc = discovery          # SupervisorDiscovery — gw_devices (sid map)
        self.log = logger
        self.on_avail = on_avail       # callback(gw, dev, available:bool) — do licznika offline
        # ACK batcha priorytetowego (2026-07-30): late-bind z harnessu, callable(gw, pid) → wyślij
        # `{t:back,g,p}`. Bez tego bramka retransmituje P1 aż do wyczerpania prób.
        self.ack_fn = None
        self.state = {}                # {gw: {dev: {ha_json fields}}} — merged
        self.last_msg_ts = {}          # {(gw,dev): epoch} — ostatnia wiadomość (dla offline)
        # RATE-LIMIT meldunku o nieznanych sidach (2026-07-25): przy niepełnej mapie KAŻDY
        # wpis `b` logował osobny WARN (carport: 5456 linii) i topił resztę logu. Teraz:
        # 1. wystąpienie melduje się od razu, potem jedno zbiorcze podsumowanie na okno.
        self.unknown_window = unknown_window
        self._now = time_fn or time.time
        self._unknown = {}             # {gw: set(sid)} — unikaty w bieżącym oknie
        self._unknown_ts = {}          # {gw: epoch ostatniego meldunku}

    def handle_b(self, data):
        """{t:b,g:G1,ts:T,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}"""
        gw = data.get('g', '?')
        items = data.get('d', [])
        # ACK NAJPIERW: `back` potwierdza ODBIÓR ramki, nie rozpoznanie sidów. Gdyby czekał na
        # `applied>0`, niepełna mapa (unknown sid) kazałaby bramce retransmitować w kółko coś,
        # czego retransmisja nie naprawi — dziury w mapie łata devmap/disc, nie powtórka batcha.
        pid = data.get('p')
        if pid is not None and self.ack_fn:
            try:
                self.ack_fn(gw, pid)
            except Exception as e:                       # ACK nigdy nie wywraca ścieżki danych
                if self.log:
                    self.log.warn('DATA', f'back {gw} p={pid} nieudany: {e}')
        applied = 0
        for entry in items:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            sid, short = entry[0], entry[1]
            dev = self._dev_from_sid(gw, sid)
            if not dev:
                self._note_unknown(gw, sid)
                continue
            short = dict(short) if isinstance(short, dict) else {}
            o = short.pop('o', 0)                     # wiek ostatniej wiadomosci [s] wzgledem `ts` ramki
            fields = decode_short(short)
            fields.setdefault('available', 'ON')      # online cascade
            # LAST_SEEN = moment realnej wiadomosci urzadzenia NA BRAMCE (ts - o), nie chwila odbioru
            # ramki po LoRa (2026-10-02). o=-1: bramka nie ma danych → zostaw poprzednia wartosc.
            # Brak `o` (stara bramka) = swieze: czas nadania ramki.
            if o != -1:
                _ts = data.get('ts')
                fields['last_seen'] = _now_str((_ts - o) if _ts else None)
            merged = self._merge(gw, dev, fields)
            self.ha.pub_device_state(gw, dev, merged)
            self.last_msg_ts[(gw, dev)] = time.time()    # reset zegara offline
            if self.on_avail:                            # licznik offline: available z `b`
                self.on_avail(gw, dev, str(merged.get('available', 'ON')).upper() == 'ON')
            applied += 1
        if self.log and applied:
            self.log.info('DATA', f'📊 b {gw}: {applied}/{len(items)} urządzeń zaktualizowanych')

    def state_hash(self, gw):
        """To samo co GatewayData.state_hash, ale z LUSTRA — liczone na formie krótkiej, żeby
        obie strony dawały identyczny wynik (`encode_short(decode_short(x)) == x`)."""
        items = []
        devs = self.disc.devices(gw) or {}
        for dev, fields in (self.state.get(gw) or {}).items():
            sid = (devs.get(dev) or {}).get('sid')
            if sid is None:
                continue
            items.append((sid, encode_short({k: v for k, v in fields.items()
                                             if k not in ('available', 'last_seen')})))
        return state_hash(items)

    def _note_unknown(self, gw, sid):
        """Zbierz nierozpoznany sid; melduj zbiorczo raz na `unknown_window`.
        Nieprawidłowość MA być widoczna (mapa niekompletna → dane lecą do kosza), ale jedna
        linia na okno zamiast jednej na każdy wpis batcha."""
        pend = self._unknown.setdefault(gw, set())
        pend.add(sid)
        now = self._now()
        if now - self._unknown_ts.get(gw, 0) < self.unknown_window:
            return
        self._unknown_ts[gw] = now
        sample = ','.join(str(s) for s in sorted(pend, key=str)[:8])
        if self.log:
            self.log.warn('DATA', f'b {gw}: {len(pend)} nieznanych sid (np. {sample}) — '
                          f'mapa urządzeń niekompletna, dane pomijane; czekam na devmapę')
        pend.clear()

    def unknown_sids(self, gw=None):
        """Bieżące nierozpoznane sidy — do encji/diagnostyki (bez zaglądania w log)."""
        if gw is not None:
            return set(self._unknown.get(gw, set()))
        return {g: set(s) for g, s in self._unknown.items()}

    def apply_st(self, gw, dev, fields):
        """`st` (sterowanie cmd / zmiana zewn.) → MERGE w stan urządzenia + last_seen + publish
        pełnego dict. `st` jest event-driven (NIE batch `b`), więc omijał logikę last_seen z
        handle_b; dodatkowo pub_device_state robi full-replace → goły {state,available} kasował
        last_seen i capy (temp/humi/batt). Tu mergujemy w istniejący stan i znaczymy czas."""
        cur = self.state.setdefault(gw, {}).setdefault(dev, {})
        cur.update(fields)
        cur['last_seen'] = _now_str()
        cur.setdefault('available', 'ON')
        self.ha.pub_device_state(gw, dev, dict(cur))
        self.last_msg_ts[(gw, dev)] = time.time()
        return dict(cur)

    def hydrate(self, gw, dev, fields):
        """Startowy seed stanu z RETAINED encji HA (lora/<gl>/<dev>/state) — żeby pierwszy
        częściowy `b` po restarcie nie skasował reszty capów (merge ma od czego startować).
        Seeduje TYLKO gdy brak w pamięci → świeży `b` (handle_b) ma pierwszeństwo."""
        cur = self.state.setdefault(gw, {})
        if dev not in cur and isinstance(fields, dict) and fields:
            cur[dev] = dict(fields)
            # zasiej też cache ha_entities → pub_device_avail nie skasuje capów przed 1. `b`
            if hasattr(self.ha, 'seed_device_cache'):
                self.ha.seed_device_cache(gw, dev, fields)
            if self.on_avail and 'available' in fields:   # zasiej licznik offline z retained
                self.on_avail(gw, dev, str(fields['available']).upper() == 'ON')
            if self.log:
                self.log.debug('DATA', f'💧 hydrate {gw}/{dev}: {list(fields)}')
            return True
        return False

    # ── state merge (retained JSON is full-replace) ─────
    def _merge(self, gw, dev, fields):
        cur = self.state.setdefault(gw, {}).setdefault(dev, {})
        cur.update(fields)
        return dict(cur)

    def clear_values(self, gw):
        """Wyczyść WARTOŚCI lustra bramki, zostawiając dostępność i last_seen.

        `dsnap` to ZRZUT, nie delta: czego w nim nie ma, tego bramka już nie ma. Bez tego
        `_merge` (dict.update) potrafi wyłącznie DOKŁADAĆ — wartości raz wpisane zostawały w
        lustrze na zawsze, więc `dh` nie miał jak się zejść z bramką.

        Objaw live (2026-08-24, G1 carport): bramka po restarcie miała pusty `sent_states`
        (przekaźniki bez zasilania w dzień ⇒ zero wartości) i słała `dh="0"`. Supervisor liczył
        swój hash z wartości sprzed 10 dni, widział rozjazd i prosił o `dsnap` co 5 minut —
        16 razy pod rząd, bez szansy na zbieżność. Zwrot: ile urządzeń wyczyszczono.
        """
        n = 0
        for _dev, fields in (self.state.get(gw) or {}).items():
            stale = [k for k in fields if k not in ('available', 'last_seen')]
            if stale:
                n += 1
                for k in stale:
                    fields.pop(k, None)
        return n

    # ── sid → device name (from registered db) ──────────
    def _dev_from_sid(self, gw, sid):
        for name, info in self.disc.devices(gw).items():
            if info.get('sid') == sid:
                return name
        return None

    def snapshot(self, gw=None):
        """Current merged states — for smoke tests / diagnostics."""
        return self.state if gw is None else self.state.get(gw, {})
