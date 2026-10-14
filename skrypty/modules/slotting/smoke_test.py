"""Smoke test for slotting module (STEP 4 / krok 7). No hardware.

Usage:
    cd skrypty
    python3 -m modules.slotting.smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


def test_imports():
    from modules.slotting import SlotScheduler, SafeWindow
    assert SlotScheduler and SafeWindow
    print("✅ imports OK")


def test_slot_windows():
    from modules.slotting import SlotScheduler
    gws = ["G1", "G2", "G3"]
    assert SlotScheduler("G1", gws).window() == (0.0, 20.0)
    assert SlotScheduler("G2", gws).window() == (20.0, 40.0)
    assert SlotScheduler("G3", gws).window() == (40.0, 60.0)
    # nieznana bramka → slot 0 (defensywnie)
    assert SlotScheduler("GX", gws).window() == (0.0, 20.0)
    print("✅ okna G1:0-19 / G2:20-39 / G3:40-59")


def test_min_left_nie_zaczyna_na_koncu_okna():
    from modules.slotting import SlotScheduler
    s = SlotScheduler("G1", ["G1", "G2", "G3"], 20)
    assert s.seconds_until_slot(now=10, min_left=4) == 0.0
    assert s.seconds_until_slot(now=17, min_left=4) == 43.0        # 3 s do konca okna → nastepny cykl
    assert s.seconds_until_slot(now=17) == 0.0                     # bez min_left jak dotad
    assert s.seconds_until_slot(now=25, min_left=4) == 35.0
    print("✅ min_left: ramka nie startuje w ostatnich sekundach okna")


def test_in_slot_and_wait():
    from modules.slotting import SlotScheduler
    gws = ["G1", "G2", "G3"]
    g2 = SlotScheduler("G2", gws, clock=lambda: 0)
    # cykl 60s; G2 = [20,40)
    assert g2.in_slot(now=25) and not g2.in_slot(now=5)
    assert g2.in_slot(now=60 + 30)                     # zawijanie cyklu
    assert g2.seconds_until_slot(now=25) == 0.0        # w oknie
    assert g2.seconds_until_slot(now=5) == 15.0        # przed oknem
    assert g2.seconds_until_slot(now=45) == 35.0       # po oknie → następny cykl (60-45+20)
    print("✅ in_slot + seconds_until_slot (wraparound)")


def test_safe_window():
    from modules.slotting import SafeWindow
    sw = SafeWindow(window_seconds=10)
    assert sw.can_send("G1", now=100) is False         # nigdy nie słyszana
    sw.mark_rx("G1", now=100)
    assert sw.can_send("G1", now=105) is True           # w oknie
    assert sw.can_send("G1", now=115) is False          # poza oknem (>10s)
    assert round(sw.seconds_since_rx("G1", now=108), 1) == 8.0
    print("✅ SafeWindow: send tylko po RX w oknie")


def test_fast_lane_avoids_foreign_slot():
    """Szybki pas bramki: swój slot i wolny G3 = od razu; slot G1 = czekaj do końca; tuż przed G1 = przeczekaj."""
    from modules.slotting import SlotScheduler
    s = SlotScheduler("G2", gateways=["G1", "G2", "G3"], slot_seconds=20)
    act = ["G1", "G2"]
    assert s.fast_lane_wait(act, now=25) == 0          # swój slot
    assert s.fast_lane_wait(act, now=45) == 0          # wolny slot G3
    assert s.fast_lane_wait(act, now=5) == 15          # slot G1 → do jego końca
    assert s.fast_lane_wait(act, now=59) == 21         # G1 za 1 s < guard 2 s → przeczekaj cały G1
    assert s.fast_lane_wait(act, now=57) == 0          # G1 za 3 s → zdąży
    g1 = SlotScheduler("G1", gateways=["G1", "G2", "G3"], slot_seconds=20)
    assert g1.fast_lane_wait(act, now=30) == 10 and g1.fast_lane_wait(act, now=50) == 0
    print("✅ szybki pas omija cudzy slot (ukryty nadajnik), swój i wolny slot bez czekania")


if __name__ == "__main__":
    tests = [test_imports, test_slot_windows, test_min_left_nie_zaczyna_na_koncu_okna, test_in_slot_and_wait, test_safe_window, test_fast_lane_avoids_foreign_slot]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
