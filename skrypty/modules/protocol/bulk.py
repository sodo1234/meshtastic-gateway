"""Ramki ZBIORCZE `cmdm`/`stm` — jedna ramka na WIELE urządzeń zamiast jednej na urządzenie.

PO CO: masowa zmiana stanu (harmonogram gasi `_2`/`_3` o 22:15, klik „wszystkie ON" na
dashboardzie) szła POJEDYNCZO — 1 ramka `cmd` + 1 ramka `st` na urządzenie. Pomiar live
22:14-22:21: **104× `cmd` + 67× `st`, pik 42 ramki/min** przy suficie kanału ~20/min
(cooldown 3 s) — kanał stał kilkanaście minut, a HB i anomalie czekały w kolejce.
Anomalie (`ansnap`), devmap i kalendarz miały już blob/transfer; sterowanie i status NIE.

JAK: krótkie okno zbiorcze (`Coalescer`) zbiera zdarzenia i wypuszcza je pogrupowane,
pocięte na ramki ≤ `max_bytes` (Meshtastic po cichu ucina/odrzuca >220 B — patrz guard
w `lora_transport.send`). Reguła zgodności wstecz: **1 element = stara ramka `cmd`/`st`**
(ruch jednostkowy bez zmian, klik pojedynczego przekaźnika wygląda jak dotąd), 2+ = `cmdm`/`stm`.

KOLEJNOŚĆ WDROŻENIA: najpierw supervisor (rozumie `stm`), potem bramki (rozumieją `cmdm`).
Odwrotnie = ramka zbiorcza trafia w stronę, która jej nie zna → dispatcher fallback = zgubione
sterowanie/status.
"""
import json
import threading


def chunk_frames(base, key, items, max_bytes=200):
    """Potnij `items` na ramki `base` + {key: kawałek}, każda ≤ max_bytes po serializacji.

    `items` może być listą (→ lista w ramce) albo dict-em (→ mapa dev:val).
    Element, który sam nie mieści się w limicie, idzie w ramce jednoelementowej — lepiej
    ramka za duża z ostrzeżeniem `OVERSIZE` z transportu niż po cichu zgubione urządzenie.
    """
    is_map = isinstance(items, dict)
    seq = list(items.items()) if is_map else list(items)
    if not seq:
        return []

    def _frame(payload):
        f = dict(base); f[key] = payload
        return f

    def _size(payload):
        # ponytail: O(n²) dumps przy budowie ramek — n to dziesiątki urządzeń, nie warto liczyć
        # przyrostowo; gdyby kiedyś szło o tysiące, liczyć długość elementu i sumować.
        return len(json.dumps(_frame(payload), separators=(',', ':')))

    out = []
    cur = {} if is_map else []
    for it in seq:
        cand = dict(cur) if is_map else list(cur)
        if is_map:
            cand[it[0]] = it[1]
        else:
            cand.append(it)
        if cur and _size(cand) > max_bytes:
            out.append(_frame(cur))
            cur = {it[0]: it[1]} if is_map else [it]
        else:
            cur = cand
    if cur:
        out.append(_frame(cur))
    return out


def cmd_frames(items, max_bytes=200):
    """[(gw, dev, cap, val)] → ramki sterowania. Duplikaty: ostatnia komenda wygrywa."""
    last = {}
    for gw, dev, cap, val in items:
        last[(gw, dev)] = (cap, val)
    groups = {}
    for (gw, dev), (cap, val) in last.items():
        groups.setdefault((gw, cap, val), []).append(dev)
    out = []
    for (gw, cap, val), devs in groups.items():
        if len(devs) == 1:
            out.append({"t": "cmd", "g": gw, "d": devs[0], "c": cap, "v": val})
        else:
            out += chunk_frames({"t": "cmdm", "g": gw, "c": cap, "v": val}, "d", devs, max_bytes)
    return out


def st_frames(gw, items, max_bytes=200):
    """[(dev, cap, val)] → ramki statusu bramki. Duplikaty: ostatni stan wygrywa."""
    last = {}
    for dev, cap, val in items:
        last[(cap, dev)] = val
    groups = {}
    for (cap, dev), val in last.items():
        groups.setdefault(cap, {})[dev] = val
    out = []
    for cap, m in groups.items():
        if len(m) == 1:
            dev, val = next(iter(m.items()))
            out.append({"t": "st", "g": gw, "d": dev, "c": cap, "v": val})
        else:
            out += chunk_frames({"t": "stm", "g": gw, "c": cap}, "d", m, max_bytes)
    return out


class Coalescer:
    """Zbiera zdarzenia przez `window` s od PIERWSZEGO, potem woła `flush(items)`.

    Okno STAŁE, nie debounce — pod ciągłym strumieniem i tak wypuszcza co `window`
    (debounce przy ciągłym ruchu potrafiłby nie wypuścić nic). `max_items` = zawór
    bezpieczeństwa: nie trzymaj w pamięci więcej niż jedna masowa zmiana.
    """

    def __init__(self, flush, window=2.0, max_items=250, logger=None, name='bulk'):
        self._flush = flush
        self.window = float(window)
        self.max_items = int(max_items)
        self.log = logger
        self.name = name
        self._buf = []
        self._lock = threading.Lock()
        self._timer = None

    def add(self, item):
        due = None
        with self._lock:
            self._buf.append(item)
            if len(self._buf) >= self.max_items:
                due = self._take_locked()
            elif self._timer is None:
                self._timer = threading.Timer(self.window, self._fire)
                self._timer.daemon = True
                self._timer.start()
        if due:
            self._emit(due)

    def flush_now(self):
        with self._lock:
            items = self._take_locked()
        if items:
            self._emit(items)

    def _take_locked(self):
        items, self._buf = self._buf, []
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        return items

    def _fire(self):
        with self._lock:
            self._timer = None
            items, self._buf = self._buf, []
        if items:
            self._emit(items)

    def _emit(self, items):
        try:
            self._flush(items)
        except Exception as e:                     # flush w wątku Timera — wyjątek nie może go zabić
            if self.log:
                self.log.warn('BULK', f'flush {self.name} padł: {e}')
