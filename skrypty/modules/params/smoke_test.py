"""Smoke test for params module (step 17). No hardware/broker.

Usage:
    cd skrypty
    python3 -m modules.params.smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


class FakeMqtt:
    def __init__(self): self.pub = []
    def publish(self, topic, payload, qos=0, retain=False):
        self.pub.append((topic, payload, retain))
    def subscribe(self, topic, qos=0): pass


def _sync(role):
    sent = []
    mq = FakeMqtt()
    changes = []
    from modules.params import ParamSync
    ps = ParamSync(role, "G1", mq, lambda d: sent.append(d), persist_path=None,
                   on_change=lambda k, v: changes.append((k, v)))
    return ps, sent, mq, changes


def test_imports():
    from modules.params import ParamSync, PARAM_DEFS, PARAM_ORDER
    assert ParamSync and len(PARAM_ORDER) == 12 and "P1" in PARAM_DEFS and "TH" in PARAM_DEFS
    print("✅ imports OK (12 params: P/T + progi TH/TL/HH/HL/BL/BC)")


def test_defaults_and_entities():
    ps, sent, mq, _ = _sync("gateway")
    assert ps.get("P1") == 48 and ps.get("T3") == 120 and ps.get("P3") == 15
    ps.register_entities()
    cfgs = [t for t, _, _ in mq.pub if "/number/" in t and t.endswith("/config")]
    assert len(cfgs) == 12, f"expected 12 number configs, got {len(cfgs)}"
    assert any("lora_g1_param_p1" in t for t in cfgs)
    # FIX 2026-07-07: device block każdej encji musi mieć `name` (HA odrzuca nowe device bez)
    import json as _j
    p1_cfg = [p for t, p, _ in mq.pub if t.endswith("param_p1/config")][0]
    dev = (_j.loads(p1_cfg) if isinstance(p1_cfg, str) else p1_cfg)["device"]
    assert dev.get("name"), "device block bez name — HA odrzuci discovery"
    btns = [t for t, _, _ in mq.pub if "/button/" in t and t.endswith("/config")]
    assert len(btns) >= 2, f"expected >=2 send buttons, got {len(btns)}"
    assert any("send_config" in t for t in btns) and any("send_timeout" in t for t in btns)
    print("✅ defaults + 12 number (device.name OK) + przyciski Send")


def test_gateway_local_set_no_send():
    ps, sent, mq, changes = _sync("gateway")
    ps.handle_local_set("P1", 72)
    assert ps.get("P1") == 72
    assert not sent, "lokalna edycja NIE wysyła — push dopiero przyciskiem Send"
    assert ("P1", 72) in changes          # on_change lokalnie natychmiast (engine reaguje)
    print("✅ gateway: local set → apply + on_change, BEZ auto-send")


def test_send_buttons():
    gw, gsent, _, _ = _sync("gateway")
    gw.handle_local_set("P1", 50); gw.handle_local_set("T1", 40)
    assert not gsent, "edycje nie wysłały same z siebie"
    gw.on_cmd("lora/params/gateway/cmd/send_config")
    assert gsent[-1]["t"] == "param_upd" and set(gsent[-1]["d"]) == {"P1", "P2", "P3"}
    assert gsent[-1]["d"]["P1"] == 50
    gw.on_cmd("lora/params/gateway/cmd/send_timeout")
    assert gsent[-1]["t"] == "param_upd" and set(gsent[-1]["d"]) == {"T1", "T2", "T3"}
    assert gsent[-1]["d"]["T1"] == 40
    # supervisor: Send → proposal `params`
    sup, ssent, _, _ = _sync("supervisor")
    sup.handle_local_set("T2", 33)
    sup.send_timeout()
    assert ssent[-1]["t"] == "params" and set(ssent[-1]["d"]) == {"T1", "T2", "T3"}
    assert ssent[-1]["d"]["T2"] == 33
    print("✅ Send: gateway→param_upd grupy (P1-P3 / T1-T3), supervisor→params proposal")


def test_gateway_clamp():
    ps, sent, mq, _ = _sync("gateway")
    ps.handle_local_set("P1", 9999)       # max 720
    assert ps.get("P1") == 720
    ps.handle_local_set("T1", 0)          # min 1
    assert ps.get("T1") == 1
    print("✅ clamp to min/max")


def test_supervisor_local_set_optimistic():
    ps, sent, mq, _ = _sync("supervisor")
    ps.handle_local_set("T2", 45)
    assert ps.get("T2") == 45             # optimistic lokalnie
    assert not sent, "edycja na mirror nie wysyła od razu — dopiero przycisk Send"
    print("✅ supervisor: local set optimistic, BEZ auto-send")


def test_gateway_applies_proposal_and_confirms():
    ps, sent, mq, changes = _sync("gateway")
    ps.handle_remote({"t": "params", "g": "G1", "d": {"T2": 45, "T3": 200}})
    assert ps.get("T2") == 45 and ps.get("T3") == 200
    assert sent[-1]["t"] == "param_upd" and sent[-1]["d"] == {"T2": 45, "T3": 200}
    assert ("T2", 45) in changes
    print("✅ gateway applies proposal → confirm param_upd + on_change")


def test_supervisor_mirrors_confirmation():
    ps, sent, mq, changes = _sync("supervisor")
    ps.handle_remote({"t": "param_upd", "g": "G1", "d": {"P1": 96, "T1": 15}})
    assert ps.get("P1") == 96 and ps.get("T1") == 15
    assert ("P1", 96) in changes
    print("✅ supervisor mirrors confirmed param_upd")


def test_request_and_push_all():
    gw, gsent, _, _ = _sync("gateway")
    gw.handle_remote({"t": "params_req", "g": "G1"})
    assert gsent[-1]["t"] == "param_upd" and len(gsent[-1]["d"]) == 12
    sup, ssent, _, _ = _sync("supervisor")
    sup.request()
    assert ssent[-1] == {"t": "params_req", "g": "G1"}
    print("✅ params_req → gateway push_all (full set)")


def test_params_hash():
    g1, _, _, _ = _sync("gateway")
    g2, _, _, _ = _sync("gateway")
    assert g1.params_hash() == g2.params_hash()       # same defaults → same hash
    h0 = g1.params_hash()
    g1.handle_local_set("P1", 99)
    assert g1.params_hash() != h0                      # change → hash changes
    assert len(g1.params_hash()) == 8
    print("✅ params_hash stable + changes on edit")


def test_on_mqtt_set_parsing():
    ps, sent, mq, _ = _sync("gateway")
    ps.on_mqtt_set("lora/params/gateway/set/P1", "60")
    assert ps.get("P1") == 60
    ps.on_mqtt_set("lora/params/gateway/set/bogus", "1")   # ignored
    ps.on_mqtt_set("lora/params/gateway/set/T1", "notnum")  # ignored
    print("✅ on_mqtt_set parses topic+payload, ignores junk")


if __name__ == "__main__":
    tests = [test_imports, test_defaults_and_entities, test_gateway_local_set_no_send,
             test_send_buttons, test_gateway_clamp, test_supervisor_local_set_optimistic,
             test_gateway_applies_proposal_and_confirms, test_supervisor_mirrors_confirmation,
             test_request_and_push_all, test_params_hash, test_on_mqtt_set_parsing]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
