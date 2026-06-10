"""Smoke test for data module (Step 3). Run locally, no hardware/broker.

Tests:
  1. imports
  2. compute_delta thresholds (v10: temp 0.5, hum 2, battery/binary any, first-obs)
  3. encode_short / decode_short roundtrip + value normalization
  4. Batcher: merge within window, flush, packet split by max_payload
  5. GatewayData.on_z2m: delta gating, monitored vs priority routing, online cascade
  6. GatewayData.handle_req: on-demand refresh force-flushes last value
  7. SupervisorData.handle_b: sid→dev, MERGE keeps untouched caps, online cascade
  8. end-to-end: gateway delta → `b` frame → supervisor → merged HA state

Usage:
    cd skrypty
    python3 -m modules.data.smoke_test
"""
import sys, json, time, threading

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


# ── lightweight fakes (constructor injection makes this trivial) ──
class FakeLora:
    def __init__(self): self.sent = []
    def send(self, text): self.sent.append(text)


class FakeGwDiscovery:
    """Mimics GatewayDiscovery surface used by GatewayData."""
    def __init__(self):
        self.devices = {
            "Temp 1": {"type": "sensor", "caps": "thb"},
            "Temp 2": {"type": "sensor", "caps": "thb"},
            "Leak 1": {"type": "binary_sensor", "caps": "wb"},
            "Test 1": {"type": "switch", "caps": "s"},
        }
        self.short_ids = {"Temp 1": 0, "Temp 2": 1, "Leak 1": 2, "Test 1": 3}
        self.priority_names = ["Leak 1"]
    def is_monitored(self, dev): return dev in self.devices


class FakeSupDiscovery:
    """Mimics SupervisorDiscovery.devices(gw)."""
    def __init__(self):
        self._d = {"G1": {
            "Temp 1": {"sid": 0, "type": "sensor", "caps": "thb"},
            "Leak 1": {"sid": 2, "type": "binary_sensor", "caps": "wb"},
        }}
    def devices(self, gw): return self._d.get(gw, {})


class FakeHA:
    def __init__(self): self.published = {}   # {(gw,dev): fields}
    def pub_device_state(self, gw, dev, fields): self.published[(gw, dev)] = dict(fields)


def test_imports():
    from modules.data import (Batcher, GatewayData, SupervisorData,
                              compute_delta, encode_short, decode_short)
    assert all([Batcher, GatewayData, SupervisorData, compute_delta,
                encode_short, decode_short])
    print("✅ imports OK")


def test_compute_delta():
    from modules.data import compute_delta
    # first observation always counts
    d = compute_delta({}, {"temperature": 22.0, "battery": 95}, "sensor")
    assert d == {"temperature": 22.0, "battery": 95}
    # temp below threshold (0.3 < 0.5) → no change; humidity 2.0 >= 2 → change
    d = compute_delta({"temperature": 22.0, "humidity": 40},
                      {"temperature": 22.3, "humidity": 42}, "sensor")
    assert "temperature" not in d and d["humidity"] == 42
    # battery any change
    d = compute_delta({"battery": 95}, {"battery": 94}, "sensor")
    assert d == {"battery": 94}
    # binary contact change
    d = compute_delta({"water_leak": False}, {"water_leak": True}, "binary_sensor")
    assert d == {"water_leak": True}
    # no change → empty
    assert compute_delta({"temperature": 22.0}, {"temperature": 22.1}, "sensor") == {}
    print("✅ compute_delta thresholds (temp 0.5 / hum 2 / any)")


def test_encode_decode_short():
    from modules.data import encode_short, decode_short
    s = encode_short({"temperature": 22.54, "humidity": 45.8, "battery": 95}, available=True)
    assert s == {"a": 1, "t": 22.5, "h": 46, "b": 95}, s
    back = decode_short(s)
    assert back == {"available": "ON", "temperature": 22.5, "humidity": 46, "battery": 95}, back
    # state + binary normalization
    assert encode_short({"state": "ON"})["s"] == 1
    assert decode_short({"a": 0, "s": 0, "w": 1}) == {
        "available": "OFF", "state": "OFF", "water_leak": True}
    # linkquality (Zigbee LQI) → short 'q', int
    assert encode_short({"temperature": 22.0, "linkquality": 120.0}) == {"a": 1, "t": 22.0, "q": 120}
    assert decode_short({"a": 1, "q": 120}) == {"available": "ON", "linkquality": 120}
    print("✅ encode/decode short roundtrip + normalization + linkquality")


def test_batcher_merge_split():
    from modules.data import Batcher
    sent = []
    b = Batcher("G1", lambda pkt: sent.append(pkt), interval=999,
                max_payload=150, max_items=99, tag="T")
    b.add(0, {"a": 1, "t": 22.5})
    b.add(0, {"h": 45})              # merge into sid 0
    assert b.pending_count() == 1
    pkts = b.flush()
    assert pkts[0]["d"] == [[0, {"a": 1, "t": 22.5, "h": 45}]], pkts
    # split: tiny payload forces multiple packets
    b2 = Batcher("G1", lambda pkt: None, max_payload=40, max_items=99, tag="T2")
    for sid in range(4):
        b2.add(sid, {"a": 1, "t": 22.5, "h": 45, "b": 95})
    pkts2 = b2.flush()
    assert len(pkts2) >= 2, f"expected split, got {len(pkts2)}"
    print("✅ Batcher merge + flush + split")


def test_gateway_on_z2m_routing():
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999)
    # monitored sensor → monitored batcher
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 22.0, "battery": 95}))
    assert gd.mon_batch.pending_count() == 1
    assert gd.last_seen.get("Temp 1")          # online cascade: last_seen set
    # priority device → priority batcher
    gd.on_z2m("zigbee2mqtt/Leak 1", json.dumps({"water_leak": True, "battery": 80}))
    assert gd.pri_batch.pending_count() == 1
    # sub-threshold update produces no new delta
    before = gd.mon_batch.stats["batched"]
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 22.1, "battery": 95}))
    assert gd.mon_batch.stats["batched"] == before
    # bridge/set topics ignored
    gd.on_z2m("zigbee2mqtt/bridge/devices", "[]")
    gd.on_z2m("zigbee2mqtt/Temp 1/set", "{}")
    # linkquality rides along on a real delta
    gd.mon_batch.flush()
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 23.0, "battery": 95, "linkquality": 84}))
    pkts = gd.mon_batch.flush()
    fields = pkts[0]["d"][0][1]
    assert fields.get("q") == 84, f"linkquality ride-along missing: {fields}"
    print("✅ GatewayData.on_z2m: delta gating + mon/pri routing + online + LQI ride-along")


def test_gateway_handle_req():
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999)
    gd.on_z2m("zigbee2mqtt/Temp 2", json.dumps({"temperature": 20.0, "humidity": 50, "battery": 88}))
    gd.mon_batch.flush(); lora.sent.clear()
    gd.handle_req({"t": "req", "g": "G1", "d": "Temp 2"})   # force flush last value
    assert lora.sent, "refresh should ship immediately"
    pkt = json.loads(lora.sent[-1])
    assert pkt["t"] == "b" and pkt["d"][0][0] == 1            # sid of Temp 2
    print("✅ GatewayData.handle_req force-flushes last value")


def test_gateway_send_pacing():
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999, send_spacing=1.0)
    # sender thread NOT started → paced _send must enqueue, never send synchronously
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 22.0, "battery": 95}))
    gd.mon_batch.flush()
    assert lora.sent == [], "paced mode must not send synchronously"
    assert len(gd._out) == 1, "frame queued for paced TX"
    # drain via the real sender loop briefly
    gd._sender_running = True
    th = threading.Thread(target=gd._sender_loop, daemon=True); th.start()
    time.sleep(0.2); gd._sender_running = False
    assert lora.sent, "sender drains queued frame"
    print("✅ GatewayData paced TX: shared queue + cooldown (no back-to-back burst)")


def test_gateway_periodic_liveness():
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999,
                     report_interval=1, offline_after=5)
    gd._start_ts = time.time() - 100             # poza grace startowym
    def sid0_entries():                          # force=True → idzie wprost na LoRa
        out = []
        for fr in lora.sent:
            for e in json.loads(fr).get("d", []):
                if e[0] == 0:
                    out.append(e)
        return out
    # ALIVE: Temp 1 raportował niedawno → heartbeat available=1 z ostatnimi wartościami
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 22.0, "battery": 95}))
    gd.mon_batch.flush(); lora.sent.clear()
    gd.report_liveness()
    entry = sid0_entries()[-1]                    # sid 0 = Temp 1
    assert entry[1].get("a") == 1 and entry[1].get("t") == 22.0, entry
    # DEAD: cisza z2m > offline_after → available=0 (#7)
    gd.last_msg_ts["Temp 1"] = time.time() - 10
    lora.sent.clear()
    gd.report_liveness()
    entry = sid0_entries()[-1]
    assert entry[1].get("a") == 0, entry
    print("✅ GatewayData periodic report + gw-side liveness (available 1→0)")


def test_supervisor_handle_b_merge():
    from modules.data import SupervisorData
    ha, disc = FakeHA(), FakeSupDiscovery()
    sd = SupervisorData(ha, disc)
    # first batch: temp + battery for Temp 1 (sid 0)
    sd.handle_b({"t": "b", "g": "G1", "ts": 1, "d": [[0, {"a": 1, "t": 22.5, "b": 95}]]})
    st = ha.published[("G1", "Temp 1")]
    assert st["temperature"] == 22.5 and st["battery"] == 95 and st["available"] == "ON"
    # second batch: ONLY humidity → must NOT wipe temp/battery (merge fix)
    sd.handle_b({"t": "b", "g": "G1", "ts": 2, "d": [[0, {"a": 1, "h": 47}]]})
    st = ha.published[("G1", "Temp 1")]
    assert st["humidity"] == 47 and st["temperature"] == 22.5 and st["battery"] == 95, st
    assert "last_seen" in st
    # unknown sid → no crash, not published
    sd.handle_b({"t": "b", "g": "G1", "ts": 3, "d": [[99, {"a": 1, "t": 1}]]})
    assert ("G1", None) not in ha.published
    print("✅ SupervisorData.handle_b: sid→dev + MERGE + online cascade")


def test_end_to_end():
    from modules.data import GatewayData, SupervisorData
    gdisc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", gdisc, lora, mon_interval=999, pri_interval=999)
    ha, sdisc = FakeHA(), FakeSupDiscovery()
    sd = SupervisorData(ha, sdisc)
    # gateway: Z2M state → delta → batch → flush → LoRa frame
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 21.7, "humidity": 44, "battery": 90}))
    gd.mon_batch.flush()
    assert lora.sent, "gateway should emit a frame"
    # supervisor: receive frame → merged HA state
    for frame in lora.sent:
        sd.handle_b(json.loads(frame))
    st = ha.published[("G1", "Temp 1")]
    assert st["temperature"] == 21.7 and st["humidity"] == 44 and st["battery"] == 90
    assert st["available"] == "ON"
    print("✅ end-to-end: Z2M → delta → `b` → supervisor → merged HA state")


if __name__ == "__main__":
    tests = [test_imports, test_compute_delta, test_encode_decode_short,
             test_batcher_merge_split, test_gateway_on_z2m_routing,
             test_gateway_handle_req, test_gateway_send_pacing,
             test_gateway_periodic_liveness,
             test_supervisor_handle_b_merge, test_end_to_end]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
            failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
