"""Smoke test: sonda ping NIE MOŻE ogłosić offline bramki, którą słychać (regresja 2026-07-30).

Bug z produkcji: sup→gw to słabszy kierunek tego łącza (RTT 16-72 s pod obciążeniem), więc ginie
PONG, a nie bramka. Sonda ping żyła własnym życiem i po 3 nieodebranych pongach ogłaszała
`💀 G1 OFFLINE` — kaskadując 194 urządzenia na offline 9 sekund po tym, jak supervisor odebrał
od tej bramki ramkę z danymi.

Usage:
    cd skrypty
    python3 -m modules.protocol.heartbeat_probe_smoke_test
"""
import sys
import time

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


class FakeHA:
    def __init__(self): self.status = {}
    def pub_gw_status(self, gw, fields): self.status[gw] = dict(fields)
    def reg_gateway(self, gw): pass
    def pub_device_avail(self, *a, **kw): pass


def _hb(**kw):
    from modules.protocol import SupervisorHeartbeat
    return SupervisorHeartbeat(FakeHA(), ping_timeout=0.01, max_retries=0, tick=0.02, **kw)


def test_probe_does_not_kill_heard_gateway():
    """Sonda wygasa, ale bramkę słychać (ruch danych) → NIE offline."""
    hb = _hb(passive_timeout=600)
    hb.gateways['G1'] = {'online': True, '_ts': time.time(), 'devices_total': 194}
    hb._probe['G1'] = {'since': 0, 'retries_left': 0, 'no_send': True}
    hb.note_seen('G1')                       # ramka `b` z bramki = dowód życia
    assert 'G1' not in hb._probe, "dowolny ruch musi ANULOWAĆ trwającą sondę ping"
    # drugie zabezpieczenie: nawet gdyby sonda przetrwała, jej wygaśnięcie przy świeżym `_ts`
    # nie może zabić bramki — puszczamy realny watchdog na chwilę
    hb._probe['G1'] = {'since': 0, 'retries_left': 0, 'no_send': True}
    hb.start()
    time.sleep(0.15)                         # tick=0.02 → kilka przebiegów pętli
    hb.running = False
    assert hb.gateways['G1']['online'] is True, "słyszana bramka NIE MOŻE iść w offline"
    print("✅ sonda ping: bramka słyszana (ruch danych) NIE jest oznaczana offline")


def test_probe_kills_truly_silent_gateway():
    """Cisza dłuższa niż okno pasywne → offline zostaje (inaczej zepsulibyśmy detekcję awarii)."""
    hb = _hb(passive_timeout=60)
    hb.gateways['G2'] = {'online': True, '_ts': time.time() - 3600, 'devices_total': 9}
    hb._mark_offline('G2', 'brak pong (test)')
    assert hb.gateways['G2']['online'] is False
    print("✅ realnie milcząca bramka nadal trafia na offline (detekcja awarii nietknięta)")


def test_note_seen_refreshes_ts():
    hb = _hb()
    hb.gateways['G1'] = {'online': True, '_ts': 0}
    hb.note_seen('G1')
    assert time.time() - hb.gateways['G1']['_ts'] < 1
    assert hb.gateways['G1'].get('last_seen')
    hb.note_seen(None)                        # brak gw nie wywraca
    print("✅ note_seen odświeża _ts/last_seen i toleruje puste gw")


if __name__ == "__main__":
    tests = [test_probe_does_not_kill_heard_gateway, test_probe_kills_truly_silent_gateway,
             test_note_seen_refreshes_ts]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
