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
    from modules.anomaly import (StagnationEngine, OfflineMonitor, BatteryMonitor,
                                 AnomalyBatcher, CODE_CAT)
    assert StagnationEngine and OfflineMonitor and BatteryMonitor and AnomalyBatcher
    assert CODE_CAT["do"] == "offline" and CODE_CAT["cb"] == "battery"
    print("✅ imports OK")


def test_battery_low_critical_recovery():
    from modules.anomaly import BatteryMonitor
    disc, emitted = FakeGwDisc(), []
    batt = {"Temp 1": 100, "Test 1": None}      # Test 1 = sieciowe, brak baterii
    mon = BatteryMonitor("G1", disc, get_battery=lambda d: batt.get(d),
                         emit=lambda sid, code, val: emitted.append((sid, code, val)),
                         get_thresholds=lambda: (25, 15))
    mon.check()
    assert not emitted                           # 100% ok, sieciowe pominięte
    batt["Temp 1"] = 20; mon.check()             # <25 → low
    assert emitted[-1] == (0, "lb", 20)
    batt["Temp 1"] = 10; mon.check()             # <15 → critical
    assert emitted[-1] == (0, "cb", 10)
    batt["Temp 1"] = 90; mon.check()             # recovery → bo
    assert emitted[-1] == (0, "bo", 90)
    # brak zmiany poziomu → brak ponownej emisji
    n = len(emitted); mon.check()
    assert len(emitted) == n
    print("✅ battery: ok→low(lb)→critical(cb)→recovery(bo), sieciowe pominięte, dedup")


def test_anomaly_batcher_dedup_and_flush():
    from modules.anomaly import AnomalyBatcher
    sent = []
    b = AnomalyBatcher("G1", lambda m: sent.append(m), max_payload=150)
    b.add(0, "do")                               # offline
    b.add(0, "dn")                               # ...wrócił w tym samym oknie → wygrywa dn
    b.add(5, "lb", 20)
    b.flush()
    assert len(sent) == 1
    d = sent[0]["d"]
    assert [0, "dn"] in d and [5, "lb", 20] in d  # dedup: tylko dn dla sid 0
    assert not any(e[:2] == [0, "do"] for e in d)
    assert b.pending() == []                       # bufor wyczyszczony
    print("✅ batcher: dedup per (sid,kategoria) + flush ab")


def test_anomaly_batcher_split():
    from modules.anomaly import AnomalyBatcher
    sent = []
    b = AnomalyBatcher("G1", lambda m: sent.append(m), max_payload=60)
    for sid in range(12):                          # dużo wpisów → split na kilka ramek
        b.add(sid, "do")
    b.flush()
    assert len(sent) > 1, "powinno się rozbić na >1 ramkę"
    for frame in sent:
        import json as _j
        assert len(_j.dumps(frame, separators=(',', ':'))) <= 60 + 20  # ~max_payload
    total = sum(len(f["d"]) for f in sent)
    assert total == 12                             # nic nie zgubione
    print("✅ batcher: split ramek >max_payload, brak utraty")


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


def test_stagnation_all_devices_and_emit():
    """STEP 5: all_devices=True → także NIE-monitored; emit przez callback (batcher)."""
    from modules.anomaly import StagnationEngine
    disc, data, emitted = FakeGwDisc(), FakeGwData(), []
    disc.devices["Ghost"] = {"type": "sensor", "mains_powered": False}  # poza is_monitored? fake liczy wszystko jako monitored
    disc.short_ids["Ghost"] = 9
    disc._monitored = {"Temp 1"}                                        # tylko Temp 1 "monitored"
    disc.is_monitored = lambda d: d in disc._monitored
    eng = StagnationEngine("G1", disc, data, get_thresholds=lambda: (48, 24),
                           emit=lambda sid, code, val: emitted.append((sid, code, val)),
                           all_devices=True)
    now = time.time()
    data.last_msg_ts["Ghost"] = now - 49 * H       # nie-monitored, >P1 → przy all_devices wykryte
    eng.check()
    assert (9, "sg", 48) in emitted, emitted
    print("✅ stagnation: all_devices=True wykrywa nie-monitored + emit callback")


def test_gateway_offline_anomaly_and_suppression():
    from modules.anomaly import GatewayOfflineAnomaly
    disc, data, emitted = FakeGwDisc(), FakeGwData(), []
    active = {"v": True}
    eng = GatewayOfflineAnomaly("G1", disc, data,
                                emit=lambda sid, code: emitted.append((sid, code)),
                                get_timeout=lambda dt: 60, grace=0,
                                is_active=lambda: active["v"])
    now = time.time()
    data.last_msg_ts["Temp 1"] = now - 61 * 60     # >60min → offline `do`
    eng.check()
    assert emitted[-1] == (0, "do")
    # supresja: bramka nieaktywna → wyczyść offline (`dn`), brak dalszych `do`
    active["v"] = False; emitted.clear()
    eng.check()
    assert emitted == [(0, "dn")] and eng.offline_devices() == set()
    eng.check()                                    # dalej nieaktywna → cisza
    assert emitted == [(0, "dn")]
    print("✅ gateway offline anomaly: do/dn + supresja trybu (night w dzień)")


def test_anomaly_store_handle_dedup_clear_alert():
    from modules.anomaly import AnomalyStore
    sid2dev = {0: "Temp 1", 2: "Door 1", 5: "Test 1"}
    changes, alerts = [], []
    st = AnomalyStore(resolve_dev=lambda gw, sid: sid2dev.get(sid),
                      on_change=lambda gw: changes.append(gw),
                      on_alert=lambda gw, dev, code, val, crit: alerts.append((dev, code, crit)))
    # offline + critical battery + stagnation
    st.handle_ab({"t": "ab", "g": "G1", "d": [[0, "do"], [2, "cb", 10], [5, "sg", 24]]})
    c = st.counts("G1")
    assert c == {"offline": 1, "battery": 1, "other": 1}, c
    assert ("Door 1", "cb", True) in alerts          # critical battery → popup flag True
    assert ("Temp 1", "do", False) in alerts          # offline → nie-critical
    # auto-clear: dn kasuje offline, bo kasuje battery
    st.handle_ab({"t": "ab", "g": "G1", "d": [[0, "dn"], [2, "bo", 80]]})
    assert st.counts("G1") == {"offline": 0, "battery": 0, "other": 1}
    # dedup: ponowne sg bez zmiany kodu → brak nowego alertu
    n = len(alerts); st.handle_ab({"t": "ab", "g": "G1", "d": [[5, "sg", 24]]})
    assert len(alerts) == n
    # listy dashboardu
    other = st.items("G1", "other")
    assert len(other) == 1 and other[0]["dev"] == "Test 1" and other[0]["type"] == "stagnation"
    assert other[0]["value"] == 24 and other[0]["gw"] == "G1" and other[0]["detected_at"]  # pola dla dashboardu
    print("✅ store: handle_ab dedup + auto-clear + counts + items + alert(critical)")


def test_anomaly_store_prune_stale_and_snapshot():
    from modules.anomaly import AnomalyStore, GatewayOfflineAnomaly, BatteryMonitor, StagnationEngine
    st = AnomalyStore(resolve_dev=lambda gw, sid: {0: "Temp 1", 5: "Test 1"}.get(sid))
    st.handle_ab({"t": "ab", "g": "G1", "d": [[0, "do"], [5, "sg", 24]]})
    # postarz wpis sid5, odśwież sid0 świeżym dump
    st.anomalies[("G1", "Test 1", "stagnation")]["seen"] = int(time.time()) - 9999
    st.handle_ab({"t": "ab", "g": "G1", "d": [[0, "do"]]})        # tylko Temp 1 potwierdzone
    removed = st.prune_stale("G1", max_age_s=300)
    assert removed == 1 and st.counts("G1") == {"offline": 1, "battery": 0, "other": 0}
    # snapshot z silników (dump_anom)
    disc, data = FakeGwDisc(), FakeGwData()
    off = GatewayOfflineAnomaly("G1", disc, data, emit=lambda *a: None, get_timeout=lambda dt: 1)
    off.offline.add("Temp 1")
    assert off.snapshot() == [(0, "do", None)]
    print("✅ store: prune_stale po dump + snapshot silników (dump_anom)")


def test_anomaly_store_ghost_prune():
    from modules.anomaly import AnomalyStore
    st = AnomalyStore(resolve_dev=lambda gw, sid: {0: "Temp 1", 9: "Ghost"}.get(sid))
    st.handle_ab({"t": "ab", "g": "G1", "d": [[0, "do"], [9, "do"]]})
    assert st.counts("G1")["offline"] == 2
    removed = st.ghost_prune("G1", ["Temp 1"])        # Ghost zniknął z discovery
    assert removed == 1 and st.counts("G1")["offline"] == 1
    print("✅ store: ghost-prune usuwa anomalie urządzeń spoza discovery")


if __name__ == "__main__":
    tests = [test_imports, test_stagnation_battery_vs_mains, test_stagnation_recovery,
             test_stagnation_skips_unseen, test_offline_by_type, test_offline_recovery_and_grace,
             test_battery_low_critical_recovery, test_anomaly_batcher_dedup_and_flush,
             test_anomaly_batcher_split, test_stagnation_all_devices_and_emit,
             test_gateway_offline_anomaly_and_suppression,
             test_anomaly_store_handle_dedup_clear_alert,
             test_anomaly_store_prune_stale_and_snapshot, test_anomaly_store_ghost_prune]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
