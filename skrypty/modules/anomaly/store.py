"""AnomalyStore — supervisor side (step 5): odbiera `ab` z bramek, trzyma listę anomalii,
dedup/auto-clear per (gw,dev,kategoria), wystawia liczniki+listy do dashboardu, alert do popupu,
persist + ghost-prune.

Kody (CLAUDE.md): do/dn offline, lb/cb/bo battery, sg/sc stagnation, th/tl/to temp, hh/hl/ho hum,
sk smoke, wl water. Kody recovery (CLEAR_CODES) kasują kategorię. Kubełki dashboardu:
  offline → an_offline | battery → an_battery | reszta → an_other.
"""
import json
import os
import time

from .batcher import CODE_CAT

CLEAR_CODES = {"dn", "bo", "sc", "to", "ho"}     # recovery → kasuje anomalię kategorii
CRITICAL_CODES = {"cb", "sk", "wl"}              # → popup (krytyczne)
BUCKET = {"offline": "offline", "battery": "battery"}   # reszta kategorii → 'other'
CODE_LABEL = {
    "do": "offline", "lb": "low_battery", "cb": "critical_battery",
    "sg": "stagnation", "th": "temp_high", "tl": "temp_low",
    "hh": "hum_high", "hl": "hum_low", "sk": "smoke", "wl": "water_leak",
}


class AnomalyStore:
    def __init__(self, resolve_dev, on_change=None, on_alert=None,
                 persist_path=None, logger=None):
        self.resolve_dev = resolve_dev   # callable(gw, sid) → dev name | None
        self.on_change = on_change       # callable(gw) — odśwież dashboard danej bramki
        self.on_alert = on_alert         # callable(gw, dev, code, value, critical_bool) — popup
        self.persist_path = persist_path
        self.log = logger
        self.anomalies = {}              # (gw,dev,kategoria) → {code,value,ts}
        self._load()

    # ── persistence ─────────────────────────────────────
    def _load(self):
        if not self.persist_path or not os.path.exists(self.persist_path):
            return
        try:
            raw = json.load(open(self.persist_path, encoding="utf-8"))
            self.anomalies = {tuple(k.split("\x1f")): v for k, v in raw.items()}
        except Exception as e:
            if self.log:
                self.log.warn("ANOM", f"load: {e}")

    def _save(self):
        if not self.persist_path:
            return
        try:
            raw = {"\x1f".join(k): v for k, v in self.anomalies.items()}
            json.dump(raw, open(self.persist_path, "w", encoding="utf-8"),
                      separators=(",", ":"))
        except Exception as e:
            if self.log:
                self.log.warn("ANOM", f"save: {e}")

    # ── odbiór ab ───────────────────────────────────────
    def handle_ab(self, d):
        gw = d.get("g", "?")
        changed = False
        for entry in d.get("d", []):
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            sid, code = entry[0], entry[1]
            value = entry[2] if len(entry) > 2 else None
            dev = self.resolve_dev(gw, sid)
            if not dev:
                continue
            cat = CODE_CAT.get(code, "other")
            key = (gw, dev, cat)
            if code in CLEAR_CODES:
                if key in self.anomalies:
                    del self.anomalies[key]
                    changed = True
                    if self.log:
                        self.log.info("ANOM", f"✅ clear {gw}/{dev} {cat}")
            else:
                prev = self.anomalies.get(key)
                now = int(time.time())
                # ts = czas pierwszego/zmiany kodu; seen = każda wzmianka (do prune_stale/dump_anom)
                self.anomalies[key] = {"code": code, "value": value,
                                       "ts": prev.get("ts", now) if prev and prev.get("code") == code else now,
                                       "seen": now}
                changed = True
                if prev is None or prev.get("code") != code:
                    crit = code in CRITICAL_CODES
                    if self.log:
                        self.log.warn("ANOM", f"🚨 {gw}/{dev} {CODE_LABEL.get(code, code)}"
                                      f"{'=' + str(value) if value is not None else ''}")
                    if self.on_alert:
                        self.on_alert(gw, dev, code, value, crit)
        if changed:
            self._save()
            if self.on_change:
                self.on_change(gw)
        return changed

    # ── zapytania dla dashboardu ────────────────────────
    def _bucket(self, cat):
        return BUCKET.get(cat, "other")

    def items(self, gw, bucket):
        """Lista items dla dashboardu. Pola zgodne z kartami HA: dev, type, value,
        detected_at (ISO string — szablony robią a.detected_at[5:16]), gw, since."""
        out = []
        for (g, dev, cat), a in self.anomalies.items():
            if g == gw and self._bucket(cat) == bucket:
                ts = a.get("ts", 0)
                out.append({"dev": dev, "gw": gw,
                            "type": CODE_LABEL.get(a["code"], a["code"]),
                            "value": a.get("value"), "since": ts,
                            "detected_at": time.strftime("%Y-%m-%dT%H:%M:%S",
                                                         time.localtime(ts)) if ts else ""})
        return sorted(out, key=lambda x: x["dev"])

    def counts(self, gw):
        c = {"offline": 0, "battery": 0, "other": 0}
        for (g, _dev, cat) in self.anomalies:
            if g == gw:
                c[self._bucket(cat)] += 1
        return c

    # ── offline-set: jedno źródło prawdy availability (unifikacja 2026-06-20) ──
    def offline_devs(self, gw):
        """Zbiór nazw urządzeń offline danej bramki (kubełek 'offline')."""
        return {dev for (g, dev, cat) in self.anomalies
                if g == gw and self._bucket(cat) == "offline"}

    def offline_hash(self, gw):
        """Hash zbioru offline (md5[:8] z posortowanych nazw, '0' = pusty). MUSI być
        identyczny z _offline_hash bramki (porównanie driftu w HB)."""
        import hashlib
        devs = sorted(self.offline_devs(gw))
        return hashlib.md5("|".join(devs).encode()).hexdigest()[:8] if devs else "0"

    def add_offline(self, gw, dev):
        """Dodaj anomalię offline (jeśli brak) — wołane z _set_avail, by lista offline == availability."""
        key = (gw, dev, "offline")
        if key not in self.anomalies:
            now = int(time.time())
            self.anomalies[key] = {"code": "do", "value": None, "ts": now, "seen": now}
            self._save()
            if self.on_change:
                self.on_change(gw)
            return True
        self.anomalies[key]["seen"] = int(time.time())   # odśwież seen (nie prune'uj aktywnego)
        return False

    def remove_one(self, gw, dev, bucket):
        """Ręczny clear jednej anomalii (klik wiersza w popupie) — usuń (gw,dev,*) z kubełka."""
        keys = [k for k in self.anomalies
                if k[0] == gw and k[1] == dev and self._bucket(k[2]) == bucket]
        for k in keys:
            del self.anomalies[k]
        if keys:
            self._save()
            if self.on_change:
                self.on_change(gw)
            if self.log:
                self.log.info("ANOM", f"🗑️ clear ręczny {gw}/{dev} [{bucket}]")
        return len(keys)

    # ── reconcyliacja (dump_anom) ───────────────────────
    def prune_stale(self, gw, max_age_s):
        """Po dump_anom: skasuj anomalie danej bramki, których bramka NIE potwierdziła
        (seen starsze niż max_age_s) — wyłapuje anomalie skasowane bez powiadomienia (strata RF)."""
        cutoff = int(time.time()) - max_age_s
        stale = [k for k, a in self.anomalies.items()
                 if k[0] == gw and a.get("seen", 0) < cutoff]
        for k in stale:
            del self.anomalies[k]
        if stale:
            self._save()
            if self.on_change:
                self.on_change(gw)
            if self.log:
                self.log.info("ANOM", f"🧹 prune_stale {gw}: {len(stale)} (po dump_anom)")
        return len(stale)

    # ── ghost-prune ─────────────────────────────────────
    def ghost_prune(self, gw, current_devices):
        """Usuń anomalie urządzeń, których już nie ma w discovery danej bramki."""
        cur = set(current_devices)
        removed = [k for k in self.anomalies if k[0] == gw and k[1] not in cur]
        for k in removed:
            del self.anomalies[k]
        if removed:
            self._save()
            if self.on_change:
                self.on_change(gw)
            if self.log:
                self.log.info("ANOM", f"👻 ghost-prune {gw}: usunięto {len(removed)}")
        return len(removed)
