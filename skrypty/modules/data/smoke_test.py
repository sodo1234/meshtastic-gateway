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


def test_report_interval_live_p3():
    """2026-07-21: report_interval_fn (P3 z dashboardu) nadpisuje interwał NA ŻYWO, podłoga 60s."""
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999, report_interval=900)
    assert gd._report_iv() == 900                    # brak fn → skalar (bez zmian)
    p3 = {"min": 5}                                   # udawany param P3 [min]
    gd.report_interval_fn = lambda: p3["min"] * 60
    assert gd._report_iv() == 300.0                   # P3=5min → 300s (na żywo)
    p3["min"] = 1
    assert gd._report_iv() == 60.0                    # P3=1min → 60s
    p3["min"] = 0                                      # zły/0 → podłoga (chroni LoRa)
    assert gd._report_iv() == 900                     # fn zwraca 0 → fallback skalar
    gd.report_interval_fn = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    assert gd._report_iv() == 900                     # wyjątek fn → fallback, nie crash
    print("✅ report_interval_fn (P3) live override + podłoga 60s + fallback na błąd")


def test_patch4_inactive_gating_per_device():
    """patch #4: bramka NIEAKTYWNA → monitored (temp/hum) + priority (leak/door) raportują ZAWSZE,
    non-monitored (lighting switch) pominięte (bez fałszywego offline). Blanket-gate usunięty."""
    from modules.data import GatewayData
    import json as _j
    disc, lora = FakeGwDiscovery(), FakeLora()
    disc.is_monitored = lambda dev: dev != "Test 1"      # Test 1 = lighting, non-monitored
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999, report_interval=1)
    gd.active_fn = lambda: False                          # bramka NIEAKTYWNA (np. night w dzień)
    gd._start_ts = time.time() - 1000
    for d in disc.devices:                                # wszystkie żywe wg z2m native
        gd._z2m_avail[d] = True
        gd.states[d] = {"temperature": 22.0, "humidity": 40, "battery": 100,
                        "water_leak": False, "state": "ON"}
    gd.report_liveness()
    sids = set()
    for fr in lora.sent:
        for e in _j.loads(fr).get("d", []):
            sids.add(e[0])
    assert 0 in sids and 1 in sids, "monitored temp muszą lecieć gdy nieaktywna (%s)" % sids
    assert 2 in sids, "priority leak musi lecieć gdy nieaktywna (%s)" % sids
    assert 3 not in sids, "non-monitored lighting NIE może lecieć gdy nieaktywna (%s)" % sids
    print("✅ patch #4: nieaktywna → monitored+priority lecą, non-monitored (lighting) pominięty")


def test_gateway_per_type_offline():
    """FIX 2026-06-15: offline_after_fn → timeout per typ (T1/T2/T3). Przy tej samej ciszy
    sensor (krótki timeout) idzie offline, a binary (długi) zostaje online. fn→None = fallback skalar."""
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999,
                     report_interval=1, offline_after=5,
                     offline_after_fn=lambda t: {"sensor": 5, "binary_sensor": 100}.get(t))
    gd._start_ts = time.time() - 1000
    now = time.time()
    gd.last_msg_ts["Temp 1"] = now - 10          # cisza 10s
    gd.last_msg_ts["Leak 1"] = now - 10          # cisza 10s
    gd.states["Temp 1"] = {"temperature": 22.0, "battery": 95}
    gd.states["Leak 1"] = {"water_leak": False, "battery": 90}

    def avail(sid):
        for fr in lora.sent:
            for e in json.loads(fr).get("d", []):
                if e[0] == sid:
                    return e[1].get("a")
        return None
    lora.sent.clear(); gd.report_liveness()
    assert avail(0) == 0, "Temp 1 (sensor, 5s) → offline"
    assert avail(2) == 1, "Leak 1 (binary, 100s) → wciąż online"
    gd.offline_after_fn = lambda t: None         # fallback → skalar offline_after=5
    # B3+B4 (2026-07-31): sweep nie powtarza już niezmienionego stanu, więc żeby sprawdzić samą
    # ścieżkę fallbacku, kasujemy latch dostępności — wtedy offline jest ZMIANĄ i leci jak zawsze.
    gd._alive.clear()
    lora.sent.clear(); gd.report_liveness()
    assert avail(0) == 0, "fallback skalar: sensor offline"
    print("✅ GatewayData per-typ offline (T1/T2/T3) + fallback skalar")


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


def test_supervisor_apply_st():
    """REGRESSION: `st` (sterowanie cmd) szedł przez goły pub_device_state({state,available})
    = full-replace → kasował last_seen + capy (temp/batt). apply_st MUSI mergować + znaczyć czas."""
    from modules.data import SupervisorData
    ha, disc = FakeHA(), FakeSupDiscovery()
    sd = SupervisorData(ha, disc)
    sd.handle_b({"t": "b", "g": "G1", "ts": 1, "d": [[0, {"a": 1, "t": 22.5, "b": 95}]]})
    sd.apply_st("G1", "Temp 1", {"state": "ON"})          # tylko state → musi zachować temp/batt
    st = ha.published[("G1", "Temp 1")]
    assert st["state"] == "ON" and st["temperature"] == 22.5 and st["battery"] == 95, st
    assert "last_seen" in st and st["available"] == "ON", st
    print("✅ apply_st: merge (zachowuje capy) + state + last_seen (st nie kasuje danych/ts)")


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


def test_avail_preserves_last_seen():
    """REGRESSION: pub_device_avail (goły {available}) full-replace kasował last_seen + capy
    na WSPÓŁDZIELONYM retained topicu → dashboard supervisora pokazywał '--' dla wszystkich.
    FIX: avail merguje się w ostatni opublikowany stan. Test na PRAWDZIWYM HAEntities."""
    import json as _json
    from modules.protocol.ha_entities import HAEntities, STATE_PREFIX

    class FakeMqtt:
        def __init__(self): self.retained = {}
        def publish(self, topic, payload, retain=False): self.retained[topic] = payload

    mq = FakeMqtt()
    ha = HAEntities(mq)
    topic = f"{STATE_PREFIX}/g1/temp_1/state"
    # pełny stan z danych (jak handle_b → pub_device_state)
    ha.pub_device_state("G1", "Temp 1", {"available": "ON", "temperature": 22.5,
                                         "battery": 95, "last_seen": "22.06 21:00:00"})
    # urządzenie pada → avail OFF (offline detection, BEZ nowego `b`)
    ha.pub_device_avail("G1", "Temp 1", False)
    st = _json.loads(mq.retained[topic])
    assert st["available"] == "OFF", st
    assert st["last_seen"] == "22.06 21:00:00", st     # ZACHOWANE (był bug: znikało → '--')
    assert st["temperature"] == 22.5 and st["battery"] == 95, st
    # avail przed jakimkolwiek `b` (cache pusty) → publikuje samo {available}, bez wywrotki
    ha.pub_device_avail("G1", "Test 1", True)
    st2 = _json.loads(mq.retained[f"{STATE_PREFIX}/g1/test_1/state"])
    assert st2 == {"available": "ON"}, st2
    # seed z hydratacji → następny avail zachowuje zasiane capy
    ha.seed_device_cache("G1", "Door 1", {"available": "ON", "contact": False, "last_seen": "20.06 10:00:00"})
    ha.pub_device_avail("G1", "Door 1", False)
    st3 = _json.loads(mq.retained[f"{STATE_PREFIX}/g1/door_1/state"])
    assert st3["last_seen"] == "20.06 10:00:00" and st3["available"] == "OFF", st3
    print("✅ pub_device_avail merguje (nie kasuje) last_seen/capy + seed z hydratacji")


def test_batcher_ack_retransmit_and_drop():
    """P1 z ACK: pakiet dostaje id `p`, brak `back` → retransmisja, ACK → cisza,
    a po wyczerpaniu prób pakiet jest PORZUCANY z licznikiem `lost` (widoczna utrata danych)."""
    from modules.data.batcher import Batcher
    sent = []
    b = Batcher("G1", sent.append, interval=999, tag="PRI", ack_wait=0.01, ack_retries=2)
    b.add("2", {"w": 1}); b.flush()
    assert len(sent) == 1 and "p" in sent[0], "pakiet P1 musi nieść id do ACK"
    pid = sent[0]["p"]
    # REGRESJA (live 2026-07-30): zegar ACK NIE może tykać, dopóki ramka czeka na kanał (arbiter
    # transferu / kolejka `_out`) — inaczej wypalamy próby i meldujemy utratę czegoś, co nigdy
    # nie poszło w eter. Dopiero mark_sent() z transportu uruchamia odliczanie.
    time.sleep(0.02); b.retry_unacked()
    assert len(sent) == 1, "bez mark_sent (pakiet nie nadany) NIE MA retransmisji"
    b.mark_sent(pid)
    time.sleep(0.02); b.retry_unacked()
    assert len(sent) == 2, "brak ACK → retransmisja"
    assert b.ack(pid) is True
    time.sleep(0.02); b.retry_unacked()
    assert len(sent) == 2, "po ACK żadnych dalszych wysyłek"
    assert b.ack(pid) is False, "spóźniony dubel ACK nieszkodliwy"
    # bez ACK: 1 wysyłka + ack_retries retransmisji, potem porzucenie (mark_sent = transport nadał)
    b.add("2", {"w": 0}); b.flush()
    pid2 = sent[-1]["p"]
    for _ in range(4):
        b.mark_sent(pid2)
        time.sleep(0.02); b.retry_unacked()
    assert b.stats["lost"] == 1 and b.pending_acks() == 0, b.stats
    assert b.stats["resent"] == 3, b.stats          # 1× dla pierwszego + 2× dla porzuconego
    print("✅ Batcher P1: ACK kasuje retransmisję, brak ACK → retry ×N → porzucenie z alarmem")


def test_pri_bypasses_slot_and_jumps_queue():
    """Alarm (`p`) omija slot (idzie `pri_lora`) i wchodzi na PRZÓD kolejki `_out`,
    monitored zostaje na slotowanym transporcie i na końcu kolejki."""
    from modules.data import GatewayData
    disc, slotted, raw = FakeGwDiscovery(), FakeLora(), FakeLora()
    gd = GatewayData("G1", disc, slotted, pri_lora=raw, mon_interval=999, pri_interval=999,
                     send_spacing=0, pri_ack_wait=5.0)
    gd._send({"t": "b", "g": "G1", "d": [], "p": 7})      # alarm
    gd._send({"t": "b", "g": "G1", "d": []})              # wolumen
    assert len(raw.sent) == 1 and len(slotted.sent) == 1, (raw.sent, slotted.sent)
    assert '"p":7' in raw.sent[0], "alarm musi iść transportem BEZ gate'u slotu"
    # kolejkowanie (send_spacing>0): alarm przed wolumenem, nawet gdy trafił później
    gd2 = GatewayData("G1", disc, slotted, pri_lora=raw, mon_interval=999, pri_interval=999,
                      send_spacing=1.0, pri_ack_wait=5.0)
    gd2._send({"t": "b", "g": "G1", "d": [], "ts": 1})
    gd2._send({"t": "b", "g": "G1", "d": [], "p": 9})
    assert list(gd2._out)[0].get("p") == 9, "alarm ma wyprzedzić zakolejkowany wolumen"
    print("✅ P1 omija slot + wchodzi na przód kolejki `_out` (wolumen zostaje w slocie)")


def test_no_value_heartbeat_only_changes():
    """B3+B4 (2026-07-31): sweep NIE wysyła już `b` dla urządzeń bez zmiany — wskazania lecą
    wyłącznie przy zmianie stanu. Zostaje: bootstrap (1. sweep po starcie) + blob availability
    jako nośnik liveness. Przed zmianą periodyczny sweep był 93% pasma LoRa."""
    from modules.data import GatewayData
    disc, lora = FakeGwDiscovery(), FakeLora()
    blobs = []
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999, send_spacing=0,
                     report_interval=1, offline_after=3600)
    gd.avail_blob_fn = blobs.append
    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 22.0, "battery": 95}))
    gd.mon_batch.flush(); lora.sent.clear()

    gd.report_liveness()                      # 1. sweep = bootstrap (dashboard nie może być pusty)
    gd.mon_batch.flush()
    assert lora.sent, "pierwszy sweep po starcie MUSI wysłać stan (bootstrap)"
    assert blobs, "blob availability musi lecieć (to on niesie teraz liveness)"
    lora.sent.clear()

    gd.report_liveness(); gd.report_liveness()   # kolejne sweepy BEZ zmiany stanu
    gd.mon_batch.flush()
    assert lora.sent == [], f"sweep bez zmiany NIE MOŻE nadawać: {lora.sent}"

    gd.on_z2m("zigbee2mqtt/Temp 1", json.dumps({"temperature": 26.0, "battery": 95}))
    gd.mon_batch.flush()
    assert any('"t":22' not in s for s in lora.sent) and lora.sent, "zmiana stanu MUSI polecieć"
    print("✅ zero heartbeatów wartości: nadaje tylko zmiana (+ bootstrap i blob availability)")


def test_alarm_bypasses_channel_arbiter():
    """Transfer-plik (devmap/kalendarz/ansnap) trzyma kanał nawet ~2 min. Wolumen ma czekać,
    ALARM nie — inaczej wyciek/dym stoi w kolejce. Wyjątek musi działać w OBU miejscach:
    w batcherze (flush) i w drenażu `_out` (sender loop)."""
    from modules.data import GatewayData
    from modules.data.batcher import Batcher

    class BusyArbiter:
        def busy(self): return True

    sent_mon, sent_pri = [], []
    mon = Batcher("G1", sent_mon.append, interval=999, tag="MON", arbiter=BusyArbiter())
    pri = Batcher("G1", sent_pri.append, interval=999, tag="PRI", arbiter=BusyArbiter(),
                  ack_wait=5.0, bypass_arbiter=True)
    mon.add("0", {"t": 21.5}); mon.flush()
    pri.add("2", {"w": 1}); pri.flush()
    assert sent_mon == [], "wolumen MUSI czekać na koniec transferu"
    assert len(sent_pri) == 1 and "p" in sent_pri[0], "alarm przechodzi w trakcie transferu"

    # drenaż `_out`: alarm na przodzie przechodzi mimo busy, sam wolumen — nie
    disc, lora = FakeGwDiscovery(), FakeLora()
    gd = GatewayData("G1", disc, lora, mon_interval=999, pri_interval=999, send_spacing=0.01,
                     pri_ack_wait=5.0)
    gd.arbiter = BusyArbiter()
    gd._send({"t": "b", "g": "G1", "d": []})                  # wolumen
    gd._sender_running = True
    th = threading.Thread(target=gd._sender_loop, daemon=True); th.start()
    time.sleep(0.2)
    assert lora.sent == [], "przy zajętym kanale wolumen nie schodzi z kolejki"
    gd._send({"t": "b", "g": "G1", "d": [], "p": 5})           # alarm → na przód
    time.sleep(0.3); gd._sender_running = False
    assert any('"p":5' in s for s in lora.sent), "alarm musi przejść mimo transferu"
    print("✅ alarm P1 omija arbitra kanału (batcher + drenaż `_out`), wolumen dalej czeka")


if __name__ == "__main__":
    tests = [test_imports, test_compute_delta, test_encode_decode_short,
             test_alarm_bypasses_channel_arbiter, test_no_value_heartbeat_only_changes,
             test_batcher_ack_retransmit_and_drop, test_pri_bypasses_slot_and_jumps_queue,
             test_batcher_merge_split, test_gateway_on_z2m_routing,
             test_gateway_handle_req, test_gateway_send_pacing,
             test_gateway_periodic_liveness, test_report_interval_live_p3,
             test_patch4_inactive_gating_per_device, test_gateway_per_type_offline,
             test_supervisor_handle_b_merge, test_supervisor_apply_st, test_end_to_end,
             test_avail_preserves_last_seen]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
            failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
