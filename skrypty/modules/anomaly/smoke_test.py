"""Smoke test for anomaly module (steps 12/15). No hardware.

    cd skrypty && python3 -m modules.anomaly.smoke_test
"""
import sys, time

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

H = 3600


class FakeGwDisc:
    def __init__(self):
        self.devices = {
            "Temp 1": {"type": "sensor", "mains_powered": False},   # bateryjne → P1
            "Test 1": {"type": "switch", "mains_powered": True},    # sieciowe → P2
        }
        self.short_ids = {"Temp 1": 0, "Test 1": 5}
    def is_monitored(self, dev): return dev in self.devices


class FakeGwData:
    def __init__(self): self.last_msg_ts = {}


class FakeSupDisc:
    def __init__(self):
        self._d = {"G1": {
            "Temp 1": {"type": "sensor"}, "Test 1": {"type": "switch"},
            "Door 1": {"type": "binary_sensor"}}}
    def devices(self, gw): return self._d.get(gw, {})


class FakeSupData:
    def __init__(self): self.last_msg_ts = {}


def test_imports():
    from modules.anomaly import StagnationEngine, OfflineMonitor
    assert StagnationEngine and OfflineMonitor
    print("✅ imports OK")


def test_stagnation_battery_vs_mains():
    from modules.anomaly import StagnationEngine
    disc, data, sent = FakeGwDisc(), FakeGwData(), []
    eng = StagnationEngine("G1", disc, data, lambda d: sent.append(d),
                           get_thresholds=lambda: (48, 24))
    now = time.time()
    data.last_msg_ts["Temp 1"] = now - 49 * H      # battery, >48h → stagnant
    data.last_msg_ts["Test 1"] = now - 10 * H      # mains, <24h → ok
    eng.check()
    assert eng.stagnant_devices() == {"Temp 1"}, eng.stagnant_devices()
    assert sent[-1] == {"t": "ab", "g": "G1", "ts": int(sent[-1]["ts"]), "d": [[0, "sg", 48]]}
    # mains crosses P2=24h
    data.last_msg_ts["Test 1"] = now - 25 * H
    eng.check()
    assert "Test 1" in eng.stagnant_devices()
    assert sent[-1]["d"] == [[5, "sg", 24]]
    print("✅ stagnation: battery→P1, mains→P2, sends ab(sg)")


def test_stagnation_recovery():
    from modules.anomaly import StagnationEngine
    disc, data, sent = FakeGwDisc(), FakeGwData(), []
    eng = StagnationEngine("G1", disc, data, lambda d: sent.append(d),
                           get_thresholds=lambda: (48, 24))
    data.last_msg_ts["Temp 1"] = time.time() - 49 * H
    eng.check()
    assert "Temp 1" in eng.stagnant_devices()
    data.last_msg_ts["Temp 1"] = time.time()       # świeża wiadomość
    eng.check()
    assert "Temp 1" not in eng.stagnant_devices()
    assert sent[-1]["d"][0][1] == "sc"             # recovery
    print("✅ stagnation recovery → sc")


def test_stagnation_skips_unseen():
    from modules.anomaly import StagnationEngine
    disc, data, sent = FakeGwDisc(), FakeGwData(), []
    eng = StagnationEngine("G1", disc, data, lambda d: sent.append(d),
                           get_thresholds=lambda: (1, 1))
    eng.check()                                    # nikt nie raportował → brak ts → pomiń
    assert not sent and not eng.stagnant_devices()
    print("✅ stagnation skips never-seen devices")


def test_offline_by_type():
    from modules.anomaly import OfflineMonitor
    disc, data, avail = FakeSupDisc(), FakeSupData(), []
    mon = OfflineMonitor(["G1"], disc, data,
                         get_timeouts=lambda: {"T1": 30, "T2": 60, "T3": 120},
                         set_avail_fn=lambda gw, dev, on: avail.append((gw, dev, on)),
                         grace=0)
    now = time.time()
    data.last_msg_ts[("G1", "Temp 1")] = now - 61 * 60   # sensor T2=60min → offline
    data.last_msg_ts[("G1", "Test 1")] = now - 20 * 60   # switch T1=30min → ok
    data.last_msg_ts[("G1", "Door 1")] = now - 10 * 60   # binary T3=120min → ok
    mon.check()
    assert mon.offline_devices() == {("G1", "Temp 1")}, mon.offline_devices()
    assert ("G1", "Temp 1", False) in avail
    # switch crosses T1=30
    data.last_msg_ts[("G1", "Test 1")] = now - 31 * 60
    mon.check()
    assert ("G1", "Test 1") in mon.offline_devices()
    print("✅ offline: per-type timeout T1/T2/T3 → available OFF")


def test_offline_recovery_and_grace():
    from modules.anomaly import OfflineMonitor
    disc, data, avail = FakeSupDisc(), FakeSupData(), []
    mon = OfflineMonitor(["G1"], disc, data,
                         get_timeouts=lambda: {"T1": 30, "T2": 60, "T3": 120},
                         set_avail_fn=lambda gw, dev, on: avail.append((gw, dev, on)),
                         grace=120)
    now = time.time()
    # grace: 60min timeout + 120s grace → 59min nie offline, 63min offline
    data.last_msg_ts[("G1", "Temp 1")] = now - 59 * 60
    mon.check(); assert ("G1", "Temp 1") not in mon.offline_devices()
    data.last_msg_ts[("G1", "Temp 1")] = now - 63 * 60
    mon.check(); assert ("G1", "Temp 1") in mon.offline_devices()
    # recovery
    data.last_msg_ts[("G1", "Temp 1")] = now
    mon.check()
    assert ("G1", "Temp 1") not in mon.offline_devices()
    assert avail[-1] == ("G1", "Temp 1", True)
    print("✅ offline: grace window + recovery → ONLINE")


if __name__ == "__main__":
    tests = [test_imports, test_stagnation_battery_vs_mains, test_stagnation_recovery,
             test_stagnation_skips_unseen, test_offline_by_type, test_offline_recovery_and_grace]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
