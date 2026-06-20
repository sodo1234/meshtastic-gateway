"""Smoke test gateway_mode (step 5). No hardware.

    cd skrypty && python3 -m modules.gateway_mode.smoke_test
"""
import sys
import time

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


def _at(hour, minute=0):
    """epoch dla dzisiejszej daty o godz hour:minute (czas lokalny)."""
    lt = time.localtime()
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hour, minute, 0, 0, 0, -1))


def test_imports():
    from modules.gateway_mode import GatewayMode
    assert GatewayMode
    print("✅ imports OK")


def test_all_time_always_active():
    from modules.gateway_mode import GatewayMode
    gm = GatewayMode("all-time")
    assert gm.is_active(_at(3)) and gm.is_active(_at(14))
    assert gm.state(_at(3))["ga"] == 1
    print("✅ all-time: zawsze aktywna")


def test_day_mode_hours():
    from modules.gateway_mode import GatewayMode
    gm = GatewayMode("day", day_start="06:00", day_end="20:00")
    assert gm.is_active(_at(12)) is True        # południe → dzień → aktywna
    assert gm.is_active(_at(23)) is False       # noc → nieaktywna
    assert gm.is_active(_at(5)) is False        # przed świtem
    print("✅ day: aktywna 06-20, nieaktywna w nocy")


def test_night_mode_hours():
    from modules.gateway_mode import GatewayMode
    gm = GatewayMode("night", day_start="06:00", day_end="20:00")
    assert gm.is_active(_at(2)) is True          # noc → bramka nocna aktywna
    assert gm.is_active(_at(12)) is False        # dzień → nieaktywna (urządzenia bez zasilania)
    assert gm.state(_at(12)) == {"gm": "night", "ga": 0}
    print("✅ night: aktywna w nocy, nieaktywna w dzień (sedno supresji offline)")


def test_solar_window_sane():
    """lat/lon (Gliwice ~50.3N) → świt/zmierzch w sensownym zakresie i day<night."""
    from modules.gateway_mode import GatewayMode
    gm = GatewayMode("day", lat=50.3, lon=18.7)
    lt = time.localtime(_at(12))
    sr, ss = gm._solar(lt)
    if sr is None:
        print("✅ solar: polar/None (akceptowalne)"); return
    assert 0 <= sr < ss <= 24 * 60, (sr, ss)
    assert 2 * 60 < sr < 11 * 60 and 14 * 60 < ss < 23 * 60, (sr, ss)
    print(f"✅ solar: świt {sr // 60}:{sr % 60:02d}, zmierzch {ss // 60}:{ss % 60:02d} (Gliwice)")


if __name__ == "__main__":
    tests = [test_imports, test_all_time_always_active, test_day_mode_hours,
             test_night_mode_hours, test_solar_window_sane]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
