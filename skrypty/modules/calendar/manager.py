"""ScheduleManager — kalendarz/harmonogram trybów produkcji (STEP 16).

Port rdzenia z supervisor_v38.py (ScheduleManager) z wstrzykiwaniem zależności.
Tryby (mode_names): 0=BRAK PRODUKCJI, 1=PRODUKCJA, 2=PRZERWA, 3=SERWIS.

Model (uzgodnione 2026-06-10): supervisor = master, push do bramek; typ eventu =
TYTUŁ eventu (detekcja po tekście, sort po długości słowa). Kompakt do LoRa:
  [[start_min, dur_min, mode], ...]  (start_min = minuty od BASE_EPOCH)

Sloty trzymane per target: 'GLOBAL' (wszystkie bramki) + per-gw override (np. 'G1').
Effective(gw) = GLOBAL + override gw. Persist JSON.
"""
import hashlib
import json
import os
import threading
import time
from datetime import datetime, timedelta


DEFAULT_MODE_NAMES = {0: "BRAK PRODUKCJI", 1: "PRODUKCJA", 2: "PRZERWA", 3: "SERWIS"}


class ScheduleManager:
    BASE_EPOCH = 1767225600          # 2026-01-01 00:00 UTC — baza kodowania kompakt

    def __init__(self, mode_names=None, persist_path=None, gateways=None, logger=None):
        self.mode_names = {int(k): v for k, v in (mode_names or DEFAULT_MODE_NAMES).items()}
        self.persist_path = persist_path
        self.gateways = list(gateways or [])
        self.log = logger
        self.data = {}               # {target: [slot,...]}  target = 'GLOBAL' | gw_id
        self.lock = threading.RLock()
        self._seq = 0
        self._load()

    # ── persistence ─────────────────────────────────────
    def _load(self):
        if not self.persist_path or not os.path.exists(self.persist_path):
            if self.log:
                self.log.info('CAL', '📂 Brak pliku harmonogramu — czysty start')
            return
        try:
            self.data = json.load(open(self.persist_path, encoding='utf-8'))
        except Exception as e:
            if self.log:
                self.log.warn('CAL', f'load failed: {e}')

    def _save(self):
        if not self.persist_path:
            return
        try:
            json.dump(self.data, open(self.persist_path, 'w', encoding='utf-8'),
                      separators=(',', ':'))
        except Exception as e:
            if self.log:
                self.log.warn('CAL', f'save failed: {e}')

    def _gen_id(self):
        self._seq += 1
        return f"s{int(time.time())}{self._seq}"

    @staticmethod
    def _parse_ts(val):
        if isinstance(val, str):
            return datetime.fromisoformat(val).timestamp()
        return float(val)

    # ── tryb z tekstu eventu ────────────────────────────
    def _detect_mode(self, text):
        """Numer trybu z tytułu eventu. Sort po długości słowa kluczowego DESC
        ('BRAK PRODUKCJI' dopasowane przed 'PRODUKCJA')."""
        if not text:
            return 1                                     # domyślnie PRODUKCJA
        tu = text.upper()
        for ch in ['🟢', '⚪', '🔧', '⏸']:
            tu = tu.replace(ch, '').strip()
        modes = {name.upper(): num for num, name in self.mode_names.items()}
        for keyword, mode_num in sorted(modes.items(), key=lambda x: -len(x[0])):
            if keyword in tu:
                return mode_num
        return 1

    # ── sloty ───────────────────────────────────────────
    def add_slot(self, target, start, end, mode=None, note=""):
        mode = self._detect_mode(note) if mode is None else int(mode)
        mode_name = self.mode_names.get(mode, (note[:30] if note else ""))
        try:
            new_st, new_et = self._parse_ts(start), self._parse_ts(end)
        except Exception:
            return None
        with self.lock:
            self.data.setdefault(target, [])
            for ex in self.data[target]:                 # dedup po start+end (±60s)
                try:
                    if abs(self._parse_ts(ex['start']) - new_st) < 60 and \
                       abs(self._parse_ts(ex['end']) - new_et) < 60:
                        if ex.get('mode') != mode:
                            ex.update(mode=mode, note=mode_name, updated_ts=int(time.time()))
                            self._save()
                        return ex['id']
                except Exception:
                    continue
            slot = {"id": self._gen_id(), "start": start, "end": end, "mode": mode,
                    "note": mode_name, "updated_ts": int(time.time()), "origin": "supervisor"}
            self.data[target].append(slot)
            self._save()
        if self.log:
            self.log.info('CAL', f"➕ [{target}] {start}→{end} mode={mode} ({mode_name})")
        return slot['id']

    def remove_slot(self, target, slot_id):
        with self.lock:
            before = len(self.data.get(target, []))
            self.data[target] = [s for s in self.data.get(target, []) if s['id'] != slot_id]
            self._save()
            return before > len(self.data.get(target, []))

    def clear_slots(self, target):
        with self.lock:
            self.data[target] = []
            self._save()

    def list_slots(self, target):
        with self.lock:
            return list(self.data.get(target, []))

    def get_effective_schedule(self, gw):
        with self.lock:
            return list(self.data.get(gw, [])) + list(self.data.get("GLOBAL", []))

    def compute_now_and_next(self, gw):
        """(mode_teraz, epoch_następnej_zmiany, źródło) dla bramki gw."""
        now = time.time()
        slots = self.get_effective_schedule(gw)
        active, next_change = None, 0
        gw_ids = [x['id'] for x in self.data.get(gw, [])]
        for s in sorted(slots, key=lambda x: x.get('updated_ts', 0), reverse=True):
            try:
                st, et = self._parse_ts(s['start']), self._parse_ts(s['end'])
            except Exception:
                continue
            if st <= now < et and active is None:
                active = (s['mode'], "override" if s['id'] in gw_ids else "global")
            for tv in (st, et):
                if tv > now and (next_change == 0 or tv < next_change):
                    next_change = tv
        if active:
            return active[0], int(next_change), active[1]
        return 0, int(next_change), "none"

    def merge_schedule(self, gw, incoming):
        with self.lock:
            local = {s['id']: s for s in self.data.get(gw, [])}
            changes = 0
            for slot in incoming:
                sid = slot.get('id')
                if sid not in local or slot.get('updated_ts', 0) > local[sid].get('updated_ts', 0):
                    local[sid] = slot
                    changes += 1
            self.data[gw] = sorted(local.values(), key=lambda s: s.get('start', ''))
            self._save()
        if self.log:
            self.log.info('CAL', f"🔄 Merge [{gw}]: {changes} zmian, {len(self.data[gw])} łącznie")
        return changes

    def replace_global(self, slots):
        """Master: nadpisz GLOBAL (po odczycie z kalendarza HA). slots = [{start,end,mode,note}]."""
        with self.lock:
            self.data["GLOBAL"] = []
            self._save()
        for s in slots:
            self.add_slot("GLOBAL", s['start'], s['end'],
                          mode=s.get('mode'), note=s.get('note', ''))

    # ── kompakt do LoRa ─────────────────────────────────
    def prepare_compact(self, gw, window_days=14):
        """Kompakt [[start_min,dur_min,mode]] dla okna window_days + hash (12 hex)."""
        now = datetime.now()
        ws_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        we_dt = ws_dt + timedelta(days=window_days)
        ws, we = ws_dt.timestamp(), we_dt.timestamp()
        seen = {}
        for slot in self.list_slots('GLOBAL') + self.list_slots(gw):
            try:
                st, et = self._parse_ts(slot['start']), self._parse_ts(slot['end'])
            except Exception:
                continue
            if et < ws or st > we:
                continue
            seen[(int((st - self.BASE_EPOCH) / 60), int((et - st) / 60))] = slot.get('mode', 1)
        compact = sorted([[k[0], k[1], v] for k, v in seen.items()])
        h = hashlib.sha256(json.dumps(compact, separators=(',', ':')).encode()).hexdigest()[:12]
        return compact, h

    def expand_compact(self, compact_list):
        """Dekoduj kompakt → pełne sloty (mode→note z mode_names)."""
        out = []
        for item in compact_list:
            if not isinstance(item, list) or len(item) < 3:
                continue
            start_min, dur_min, mode = item[0], item[1], item[2]
            st = self.BASE_EPOCH + start_min * 60
            et = st + dur_min * 60
            out.append({
                # id musi rozróżniać sloty o tym samym starcie (np. PRZERWA wewnątrz
                # PRODUKCJI) — granularność (start,dur) = klucz kompaktu po stronie mastera.
                # Sam start_min kolidował → merge nadpisywał slot → utrata danych + wieczny
                # drift hasha → re-sync w kółko. Mode poza id (zmiana trybu = update, nie sierota).
                "id": f"s{start_min}_{dur_min}",
                "start": datetime.fromtimestamp(st).strftime('%Y-%m-%d %H:%M:%S'),
                "end": datetime.fromtimestamp(et).strftime('%Y-%m-%d %H:%M:%S'),
                "mode": mode, "note": self.mode_names.get(mode, f"MODE_{mode}"),
                "updated_ts": int(time.time()), "origin": "supervisor"})
        return out

    def schedule_hash(self, gw, window_days=14):
        return self.prepare_compact(gw, window_days)[1]
