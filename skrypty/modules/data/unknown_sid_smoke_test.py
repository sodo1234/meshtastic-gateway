"""Smoke test: RATE-LIMIT logu nieznanych sidów (2026-07-25). No hardware.

Regresja z carportu: mirror 78/194 → KAŻDY wpis `b` z nierozpoznanym sidem logował
osobny WARN („nieznany sid=X (czekam na db)") = 5456 linii, w których tonie wszystko
inne. Zasada: system ma INFORMOWAĆ o nieprawidłowości, nie zasypywać nią logu.

Kontrakt: pierwszy nieznany sid melduje się natychmiast, potem przez `unknown_window`
sekund cisza — a po oknie jedno zbiorcze podsumowanie z liczbą unikatów.

Usage:
    cd skrypty
    python3 -m modules.data.unknown_sid_smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.data.supervisor_data import SupervisorData


class _HA:
    def __init__(self): self.pubs = []
    def pub_device_state(self, gw, dev, fields): self.pubs.append((gw, dev, dict(fields)))


class _Disc:
    """Mapa sid→nazwa jak w SupervisorDiscovery (tu: tylko sid 5 znany)."""
    def __init__(self, mapping): self.m = mapping
    def devices(self, gw): return self.m.get(gw, {})


class _Log:
    def __init__(self): self.warns = []; self.infos = []
    def warn(self, tag, msg): self.warns.append(msg)
    def info(self, tag, msg): self.infos.append(msg)
    def debug(self, tag, msg): pass


class _Clock:
    def __init__(self): self.t = 1000.0
    def __call__(self): return self.t


def _mk():
    ha, log, clk = _HA(), _Log(), _Clock()
    disc = _Disc({'G1': {'Temp 1': {'sid': 5, 'type': 'sensor', 'caps': 'bth'}}})
    sd = SupervisorData(ha, disc, logger=log, unknown_window=300, time_fn=clk)
    return sd, ha, log, clk


def _batch(sids):
    return {'t': 'b', 'g': 'G1', 'ts': 1, 'd': [[s, {'a': 1}] for s in sids]}


def test_unknown_sids_summarized_not_per_packet():
    """Trzy paczki z nieznanymi sidami → JEDEN meldunek, nie sześć."""
    sd, ha, log, clk = _mk()
    for _ in range(3):
        sd.handle_b(_batch([100, 101]))
        clk.t += 3
    assert len(log.warns) == 1, f"oczekiwano 1 meldunku, mam {len(log.warns)}: {log.warns}"
    print(f"✅ 3 paczki × 2 nieznane sidy → 1 meldunek: {log.warns[0]!r}")


def test_summary_reports_unique_count():
    """Meldunek niesie liczbę unikatowych nieznanych sidów (diagnostyka: ile brakuje)."""
    sd, ha, log, clk = _mk()
    sd.handle_b(_batch([100]))                 # 1. wystąpienie → melduje natychmiast
    for s in (101, 102, 103):                  # w oknie: cisza, ale zliczaj
        clk.t += 3; sd.handle_b(_batch([s]))
    clk.t += 300                               # okno minęło
    sd.handle_b(_batch([104]))
    assert len(log.warns) == 2, log.warns
    assert '4' in log.warns[1], f"podsumowanie ma nieść liczbę unikatów (4): {log.warns[1]!r}"
    print(f"✅ podsumowanie po oknie z liczbą unikatów: {log.warns[1]!r}")


def test_known_sid_still_applied():
    """Rate-limit dotyczy TYLKO logu — znane urządzenia lecą do HA jak dotąd."""
    sd, ha, log, clk = _mk()
    sd.handle_b(_batch([5, 100]))
    assert [p[1] for p in ha.pubs] == ['Temp 1'], ha.pubs
    print("✅ znany sid nadal publikowany do HA (rate-limit nie gubi danych)")


def test_no_warn_when_all_known():
    """Zdrowy stan = cisza absolutna."""
    sd, ha, log, clk = _mk()
    sd.handle_b(_batch([5]))
    assert log.warns == [], log.warns
    print("✅ komplet mapy → zero meldunków")


if __name__ == "__main__":
    tests = [test_unknown_sids_summarized_not_per_packet, test_summary_reports_unique_count,
             test_known_sid_still_applied, test_no_warn_when_all_known]
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
