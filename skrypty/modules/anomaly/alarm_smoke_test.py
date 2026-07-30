"""Smoke test AlarmMonitor — zalanie (`wl`) i dym (`sk`) jako anomalie.

Luka 2026-07-30: kody istniały w mapach etykiet/kategorii/krytycznych, ale NIKT ich nie
generował — zalanie nie trafiało do liczników, list ani popupu.

Usage:
    cd skrypty
    python3 -m modules.anomaly.alarm_smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


class FakeDisc:
    def __init__(self):
        self.devices = {"Leak 1": {}, "Smoke 1": {}, "Temp 1": {}}
        self.short_ids = {"Leak 1": 2, "Smoke 1": 5, "Temp 1": 0}


def _mon(states):
    from modules.anomaly import AlarmMonitor
    emitted = []
    m = AlarmMonitor("G2", FakeDisc(), get_state=lambda d: states.get(d, {}),
                     emit=lambda sid, code, val: emitted.append((sid, code, val)))
    return m, emitted


def test_leak_detect_and_recover():
    states = {"Leak 1": {"water_leak": False, "battery": 100}}
    m, out = _mon(states)
    m.check()
    assert out == [], 'pierwszy odczyt bez alarmu nie moze nic zglaszac'
    states["Leak 1"]["water_leak"] = True
    m.check()
    assert out == [(2, "wl", 1)], out
    m.check()
    assert len(out) == 1, "brak zmiany = brak powtórki (one-shot)"
    states["Leak 1"]["water_leak"] = False
    m.check()
    assert out[-1] == (2, "wo", 0), out
    print("✅ zalanie: wykrycie `wl` → one-shot → powrót `wo`")


def test_smoke_and_first_read_already_alarming():
    states = {"Smoke 1": {"smoke": "ON"}}          # start Z alarmem (retained z2m)
    m, out = _mon(states)
    m.check()
    assert out == [(5, "sk", 1)], "alarm obecny przy pierwszym odczycie MUSI być zgłoszony"
    print("✅ dym: `sk` zgłoszony też gdy alarm trwa już przy starcie")


def test_ignores_devices_without_cap():
    states = {"Temp 1": {"temperature": 22.0, "humidity": 40}}
    m, out = _mon(states)
    m.check(); m.check()
    assert out == [], "urządzenie bez capy wycieku/dymu nie generuje nic"
    print("✅ urządzenia bez capy pomijane")


def test_truthy_variants_and_snapshot():
    states = {"Leak 1": {"water_leak": "true"}, "Smoke 1": {"smoke": 1}}
    m, out = _mon(states)
    m.check()
    assert sorted(c for _s, c, _v in out) == ["sk", "wl"], out
    snap = sorted(m.snapshot())
    assert snap == [(2, "wl", 1), (5, "sk", 1)], snap
    states["Leak 1"]["water_leak"] = "OFF"
    m.check()
    assert m.snapshot() == [(5, "sk", 1)], "po ustąpieniu wycieku snapshot ma tylko dym"
    print("✅ warianty bool/str/int + snapshot dla dump_anom/ansnap")


def test_codes_wired_into_store_and_batcher():
    """Kody muszą być spięte z resztą: kategoria, clear i flaga krytyczna."""
    from modules.anomaly.batcher import CODE_CAT
    from modules.anomaly.store import CLEAR_CODES, CRITICAL_CODES, CODE_LABEL
    assert CODE_CAT["wl"] == "water" and CODE_CAT["wo"] == "water"
    assert CODE_CAT["sk"] == "smoke" and CODE_CAT["ko"] == "smoke"
    assert {"wo", "ko"} <= CLEAR_CODES, "bez tego alarmu NIE DA SIĘ skasować powrotem do normy"
    assert {"wl", "sk"} <= CRITICAL_CODES, "zalanie/dym muszą iść na popup"
    assert CODE_LABEL["wl"] == "water_leak" and CODE_LABEL["sk"] == "smoke"
    print("✅ wl/sk/wo/ko spięte: kategorie, CLEAR_CODES, CRITICAL_CODES, etykiety")


if __name__ == "__main__":
    tests = [test_leak_detect_and_recover, test_smoke_and_first_read_already_alarming,
             test_ignores_devices_without_cap, test_truthy_variants_and_snapshot,
             test_codes_wired_into_store_and_batcher]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
