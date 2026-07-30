"""Smoke test: RE-SYNC GUARD mapy urządzeń (2026-07-25). No hardware.

Regresja z carportu: mirror supervisora miał 78/194 urządzeń G1, ale `gw_synced` niósł
PEŁNY hash bramki (f795e257116c) → 116 sidów nie do rozwiązania w nieskończoność
(„nieznany sid=X (czekam na db)" ×5456). Wszystkie ścieżki naprawcze były zablokowane
tym samym stemplem:
  1. note_hash: hash == synced → return (nie żąda disc),
  2. check_complete: porównuje do gw_devn, które wypełnia TYLKO disc_meta (przychodzi
     dopiero po restarcie bramki) → pusty licznik = pętla no-op,
  3. żądanie `disc` niesie h=gw_synced → bramka widzi zgodność → meta_only BEZ devmapy.

Fix: HB niesie prawdziwe `dev` co heartbeat — karmimy nim gw_devn (note_devn), a hash
w żądaniu `disc` dołączamy TYLKO gdy mirror jest kompletny (synced_hash_for_request).

Usage:
    cd skrypty
    python3 -m modules.protocol.discovery_resync_smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.protocol.discovery import SupervisorDiscovery

FULL_HASH = "f795e257116c"          # hash bramki dla pełnych 194 urządzeń


class _Log:
    def __init__(self): self.warns = []; self.infos = []
    def warn(self, tag, msg): self.warns.append(msg)
    def info(self, tag, msg): self.infos.append(msg)
    def debug(self, tag, msg): pass


def _sup(mirror_n, request_log=None, logger=None):
    """Supervisor z mirrorem `mirror_n` urządzeń G1 ostemplowanym PEŁNYM hashem."""
    d = SupervisorDiscovery(ha_entities=None, persist_path=None, logger=logger,
                            request_disc_fn=(request_log.append if request_log is not None else None))
    d.gw_devices['G1'] = {f"RA{i}": {'sid': i, 'type': 'switch', 'caps': 's'}
                          for i in range(mirror_n)}
    d.gw_synced['G1'] = FULL_HASH
    d.gw_hashes['G1'] = FULL_HASH
    return d


def test_hb_devn_marks_mirror_incomplete():
    """HB niesie dev=194 — mirror 78/194 musi być rozpoznany jako NIEPEŁNY."""
    d = _sup(78)
    d.note_devn('G1', 194)
    assert d.gw_devn.get('G1') == 194, d.gw_devn
    assert d.is_complete('G1') is False, "78/194 to nie komplet"
    print("✅ dev z HB karmi licznik → mirror 78/194 = niepełny")


def test_incomplete_mirror_requests_disc():
    """Niepełny mirror → check_complete żąda discovery (mimo zgodnego hasha)."""
    reqs = []
    d = _sup(78, reqs)
    d.note_devn('G1', 194)
    d.check_complete()
    assert reqs == ['G1'], f"oczekiwano żądania disc dla G1, mam: {reqs}"
    print("✅ niepełny mirror → check_complete żąda disc")


def test_disc_request_drops_hash_when_incomplete():
    """Żądanie `disc` przy niepełnym mirrorze NIE niesie hasha — inaczej bramka
    odpowie meta_only i devmapa nigdy nie dojdzie (samo-blokada)."""
    d = _sup(78)
    d.note_devn('G1', 194)
    assert d.synced_hash_for_request('G1') == '', "niepełny mirror nie ma prawa wysyłać hasha"
    print("✅ niepełny mirror → `disc` bez hasha (bramka wyśle pełną devmapę)")


def test_complete_mirror_stays_quiet():
    """Komplet → cisza: żadnego disc, hash dołączony (anti-spam nienaruszony)."""
    reqs = []
    d = _sup(194, reqs)
    d.note_devn('G1', 194)
    d.check_complete()
    d.note_hash('G1', FULL_HASH)
    assert reqs == [], f"komplet nie może generować ruchu, mam: {reqs}"
    assert d.synced_hash_for_request('G1') == FULL_HASH, d.synced_hash_for_request('G1')
    assert d.is_complete('G1') is True
    print("✅ komplet → zero ruchu LoRa + hash dołączony (anti-spam działa)")


def test_unknown_devn_assumes_complete():
    """Brak `dev` (stara bramka / przed 1. HB) → nie histeryzuj: traktuj jak komplet."""
    reqs = []
    d = _sup(78, reqs)
    d.check_complete()
    assert d.is_complete('G1') is True, "bez licznika nie ma podstaw do re-requestu"
    assert reqs == [], f"brak licznika = brak ruchu, mam: {reqs}"
    print("✅ brak dev_n → zachowanie bez zmian (żadnego sztormu)")


def test_devn_ignores_garbage():
    """dev=0/None/śmieć nie kasuje licznika (HB bywa okrojony)."""
    d = _sup(78)
    d.note_devn('G1', 194)
    for junk in (0, None, '', 'x'):
        d.note_devn('G1', junk)
        assert d.gw_devn.get('G1') == 194, (junk, d.gw_devn)
    print("✅ śmieciowe dev nie kasuje licznika")


def test_burst_then_backoff_never_gives_up():
    """Krótka seria prób, potem rzadki retry — system NIGDY nie milknie na stałe.
    Twardy `max_try` bez backoffu = po 4 próbach cisza do restartu (mapa zostaje dziurawa)."""
    reqs = []
    d = _sup(78, reqs)
    d.note_devn('G1', 194)
    d.check_complete(retry_after=18, max_try=2, idle_retry=900)      # próba 1
    d.check_complete(retry_after=18, max_try=2, idle_retry=900)      # za wcześnie → nic
    assert len(reqs) == 1, reqs
    d.gw_disc_ts['G1'] -= 20
    d.check_complete(retry_after=18, max_try=2, idle_retry=900)      # próba 2 (koniec serii)
    assert len(reqs) == 2, reqs
    d.gw_disc_ts['G1'] -= 20
    d.check_complete(retry_after=18, max_try=2, idle_retry=900)      # seria wyczerpana → cisza
    assert len(reqs) == 2, f"po serii ma być backoff, nie spam: {reqs}"
    d.gw_disc_ts['G1'] -= 900
    d.check_complete(retry_after=18, max_try=2, idle_retry=900)      # okno backoff minęło
    assert len(reqs) == 3, f"po backoffie system musi spróbować ponownie: {reqs}"
    print("✅ seria → backoff → ponowna próba (bez spamu, bez poddania się)")


def test_incomplete_is_reported_with_numbers():
    """Meldunek niesie konkret (78/194), nie samo „coś nie tak" — inaczej nie da się
    ocenić skali z dashboardu ani z logu."""
    log = _Log()
    d = _sup(78, [], logger=log)
    d.note_devn('G1', 194)
    d.check_complete()
    assert log.warns and '78/194' in log.warns[0], log.warns
    print(f"✅ niepełna mapa meldowana z liczbami: {log.warns[0]!r}")


def test_complete_mirror_reports_nothing():
    """Komplet → cisza w logu (brak meldunku = informacja, że jest dobrze)."""
    log = _Log()
    d = _sup(194, [], logger=log)
    d.note_devn('G1', 194)
    d.check_complete()
    assert log.warns == [], log.warns
    print("✅ komplet → zero meldunków")


if __name__ == "__main__":
    tests = [test_hb_devn_marks_mirror_incomplete, test_incomplete_mirror_requests_disc,
             test_disc_request_drops_hash_when_incomplete, test_complete_mirror_stays_quiet,
             test_unknown_devn_assumes_complete, test_devn_ignores_garbage,
             test_burst_then_backoff_never_gives_up, test_incomplete_is_reported_with_numbers,
             test_complete_mirror_reports_nothing]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"❌ {t.__name__}: {type(e).__name__}: {e}")
    print("\n" + "─" * 40)
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
