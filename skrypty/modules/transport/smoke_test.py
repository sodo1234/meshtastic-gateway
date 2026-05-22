"""Smoke test for transport module. Run locally, no hardware required.

Tests:
  1. Imports work (Dispatcher, MqttTransport, LoraTransport)
  2. Dispatcher routes by 't', handles unknown + malformed + handler exceptions
  3. LoraTransport class is constructible without hardware (only tests __init__ — does NOT call .start())
  4. MqttTransport class is constructible without broker (only tests __init__)

Usage:
    cd skrypty
    python3 -m modules.transport.smoke_test
"""
import sys, json

# Force UTF-8 stdout for Windows console (linux already UTF-8)
try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


def test_imports():
    from modules.transport import Dispatcher, MqttTransport, LoraTransport
    assert Dispatcher and MqttTransport and LoraTransport
    print("✅ imports OK")


def test_dispatcher_routing():
    from modules.transport import Dispatcher
    captured = {}
    d = Dispatcher()
    d.register('hb', lambda data: captured.setdefault('hb', data))
    d.register('cmd', lambda data: captured.setdefault('cmd', data))
    d.dispatch({'t': 'hb', 'g': 'G1', 'up': 100})
    d.dispatch({'t': 'cmd', 'g': 'G1', 'd': 'Test', 'c': 'state', 'v': 'ON'})
    assert captured['hb']['up'] == 100
    assert captured['cmd']['c'] == 'state'
    print("✅ dispatcher routes by t")


def test_dispatcher_raw_json():
    from modules.transport import Dispatcher
    got = []
    d = Dispatcher()
    d.register('ab', lambda data: got.append(data))
    d.dispatch_raw('{"t":"ab","g":"G1","ts":123,"d":[["RA15_S1","th",32.6]]}')
    assert got and got[0]['d'][0][0] == 'RA15_S1'
    print("✅ dispatch_raw parses JSON")


def test_dispatcher_unknown_and_fallback():
    from modules.transport import Dispatcher
    fb = []
    d = Dispatcher()
    d.set_fallback(lambda data: fb.append(data))
    d.dispatch({'t': 'unknown_type', 'x': 1})
    assert fb and fb[0]['t'] == 'unknown_type'
    print("✅ fallback handler invoked for unknown type")


def test_dispatcher_malformed():
    from modules.transport import Dispatcher
    d = Dispatcher()
    d.dispatch_raw("not-json-{[}")
    d.dispatch("not-a-dict")
    d.dispatch({'t': 'unknown'})  # no fallback, no error
    print("✅ malformed input does not crash")


def test_dispatcher_handler_exception_isolated():
    from modules.transport import Dispatcher
    d = Dispatcher()
    d.register('bad', lambda data: 1/0)
    d.dispatch({'t': 'bad'})
    print("✅ handler exception caught (doesn't propagate)")


def test_mqtt_constructible():
    from modules.transport import MqttTransport
    m = MqttTransport(host='127.0.0.1', port=1883, user='u', password='p',
                      on_message=lambda topic, p: None)
    m.subscribe('test/topic')
    assert not m.connected
    print("✅ MqttTransport instantiates (no broker contacted)")


def test_lora_constructible():
    from modules.transport import LoraTransport
    cfg = [{"port": "/dev/null", "enabled": False, "label": "ANT-TEST", "gateways": ["G1"]}]
    l = LoraTransport(ports_cfg=cfg, on_receive=lambda t: None)
    assert l.gw_routes == {}  # populated only by start()
    status = l.get_status()
    assert "ANT-TEST" in status and not status["ANT-TEST"]["connected"]
    print("✅ LoraTransport instantiates (no hardware accessed)")


def test_end_to_end_dispatcher_via_lora_callback():
    """Simulates LoraTransport.on_receive feeding Dispatcher (the production wiring)."""
    from modules.transport import Dispatcher
    d = Dispatcher()
    captured = []
    d.register('hb', lambda data: captured.append(('hb', data['g'])))
    d.register('b',  lambda data: captured.append(('b',  data['g'])))
    # simulate 2 incoming LoRa frames
    d.dispatch_raw('{"t":"hb","g":"G1","up":42,"dev":9}')
    d.dispatch_raw('{"t":"b","g":"G2","ts":100,"d":[[1,{"a":1,"t":22.5}]]}')
    assert captured == [('hb', 'G1'), ('b', 'G2')]
    print("✅ end-to-end: LoRa frame → dispatch_raw → handler")


if __name__ == "__main__":
    tests = [test_imports, test_dispatcher_routing, test_dispatcher_raw_json,
             test_dispatcher_unknown_and_fallback, test_dispatcher_malformed,
             test_dispatcher_handler_exception_isolated,
             test_mqtt_constructible, test_lora_constructible,
             test_end_to_end_dispatcher_via_lora_callback]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}")
            failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
