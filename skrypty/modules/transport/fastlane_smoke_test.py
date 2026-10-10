"""Assert-only smoke tests for LoRa fast lane and codec negotiation.

Run from ``skrypty`` with::

    python -m modules.transport.fastlane_smoke_test
"""
import json
import threading
import time

from .codec import decode, encode
from .fastlane import (DEFAULT_IMMEDIATE_TYPES, is_immediate_frame,
                       lbt_wait_seconds, slot_wait_seconds)
from .lora_transport import LoraTransport


class _Iface:
    def __init__(self, deliver=None):
        self.sent = []
        self.deliver = deliver

    def sendText(self, text):
        self.sent.append((time.monotonic(), text))
        if self.deliver:
            self.deliver(text)

    def close(self):
        pass


class _Busy:
    def busy(self):
        return True


def _running_transport(**kwargs):
    transport = LoraTransport([], tx_cooldown=0, dedup_seconds=0, **kwargs)
    iface = _Iface()
    transport.interfaces['TEST'] = iface
    thread = threading.Thread(target=transport._tx_loop, daemon=True)
    thread.start()
    return transport, iface, thread


def _stop(transport, thread):
    transport.running = False
    thread.join(1.0)
    assert not thread.is_alive()


def _wait_for(predicate, timeout=1.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_type_detection_long_and_short():
    assert is_immediate_frame({'t': 'ping'}, DEFAULT_IMMEDIATE_TYPES)
    assert is_immediate_frame({'t': 'pi'}, DEFAULT_IMMEDIATE_TYPES)
    assert is_immediate_frame('{"t":"params","g":"G1"}', DEFAULT_IMMEDIATE_TYPES)
    assert is_immediate_frame('{"t":"ps","g":"G1"}', DEFAULT_IMMEDIATE_TYPES)
    assert not is_immediate_frame({'t': 'hb'}, DEFAULT_IMMEDIATE_TYPES)
    assert slot_wait_seconds({'t': 'pi'}, DEFAULT_IMMEDIATE_TYPES, 37) == 0
    assert slot_wait_seconds({'t': 'h'}, DEFAULT_IMMEDIATE_TYPES, 37) == 37


def test_immediate_bypasses_slot_and_arbiter_front():
    transport = LoraTransport([], tx_cooldown=0, lbt_quiet=0,
                              codec_role='supervisor')
    iface = _Iface()
    transport.interfaces['TEST'] = iface
    transport.slot_gate = lambda _text: 30.0
    transport.arbiter = _Busy()
    transport.send({'t': 'hb', 'g': 'G1'})
    event = transport.send({'t': 'ping', 'g': 'G1'})
    thread = threading.Thread(target=transport._tx_loop, daemon=True)
    thread.start()
    try:
        assert event.wait(0.5), 'fast-lane frame waited for slot/arbiter'
        assert json.loads(iface.sent[0][1])['t'] == 'ping'
        assert not any(json.loads(text)['t'] == 'hb' for _, text in iface.sent)
    finally:
        _stop(transport, thread)


def test_lbt_waits_and_respects_cap():
    transport, iface, thread = _running_transport(
        lbt_quiet=5.0, lbt_max_wait=0.12, codec_role='supervisor')
    try:
        before = time.monotonic()
        transport._mesh_rx({}, None)  # even a non-text radio frame marks the channel busy
        assert transport._last_rx_time >= before
        event = transport.send({'t': 'ping', 'g': 'G1'})
        assert event.wait(0.6), 'LBT exceeded its cap'
        elapsed = iface.sent[0][0] - before
        assert 0.09 <= elapsed <= 0.45, elapsed
        assert lbt_wait_seconds(before, before + 0.2, before, quiet=5, max_wait=0.1) == 0
    finally:
        _stop(transport, thread)


def test_bulk_stays_slot_gated():
    transport, iface, thread = _running_transport(lbt_quiet=0)
    transport.slot_gate = lambda _text: 10.0
    try:
        transport.send({'t': 'hb', 'g': 'G1'})
        time.sleep(0.25)
        assert not iface.sent, 'bulk frame escaped its slot gate'
    finally:
        _stop(transport, thread)


def test_codec_negotiation_and_dual_decode():
    received = []
    supervisor = LoraTransport([], on_receive=received.append, tx_cooldown=0,
                               codec_role='supervisor', dedup_seconds=0)
    supervisor.send({'t': 'ping', 'g': 'G1'})
    assert json.loads(supervisor._tx_queue[-1]['text'])['t'] == 'ping'
    supervisor._mesh_rx({'decoded': {'text': '{"t":"pong","g":"G1"}'}}, None)
    supervisor._mesh_rx({'decoded': {'text': '{"t":"po","g":"G1"}'}}, None)
    assert [json.loads(text)['t'] for text in received] == ['pong', 'pong']
    assert supervisor.peer_supports_short('G1')
    supervisor.send({'t': 'ping', 'g': 'G1'})
    assert json.loads(supervisor._tx_queue[0]['text'])['t'] == 'pi'
    assert json.loads(supervisor._wire_text({'t': 'ping'}))['t'] == 'ping'

    gateway = LoraTransport([], codec_role='gateway', codec_short_tx=False)
    assert json.loads(gateway._wire_text({'t': 'pong', 'g': 'G1'}))['t'] == 'pong'
    gateway._mesh_rx({'decoded': {'text': '{"t":"sp","g":"G1"}'}}, None)
    assert json.loads(gateway._wire_text({'t': 'pong', 'g': 'G1'}))['t'] == 'po'
    forced = LoraTransport([], codec_role='gateway', codec_short_tx=True)
    assert json.loads(forced._wire_text({'t': 'pong', 'g': 'G9'}))['t'] == 'po'
    assert decode(encode({'t': 'params', 'g': 'G1'})) == {'t': 'params', 'g': 'G1'}


def test_two_transport_loopback_negotiates():
    sup_rx, gw_rx = [], []
    supervisor = LoraTransport([], on_receive=sup_rx.append, tx_cooldown=0,
                               lbt_quiet=0, dedup_seconds=0,
                               codec_role='supervisor')
    gateway = LoraTransport([], on_receive=gw_rx.append, tx_cooldown=0,
                            lbt_quiet=0, dedup_seconds=0,
                            codec_role='gateway', codec_short_tx=True)
    sup_iface = _Iface(lambda text: gateway._mesh_rx({'decoded': {'text': text}}, None))
    gw_iface = _Iface(lambda text: supervisor._mesh_rx({'decoded': {'text': text}}, None))
    supervisor.interfaces['SUP'] = sup_iface
    gateway.interfaces['GW'] = gw_iface
    sup_thread = threading.Thread(target=supervisor._tx_loop, daemon=True)
    gw_thread = threading.Thread(target=gateway._tx_loop, daemon=True)
    sup_thread.start(); gw_thread.start()
    try:
        assert supervisor.send({'t': 'ping', 'g': 'G1'}).wait(0.5)
        assert json.loads(sup_iface.sent[-1][1])['t'] == 'ping'  # unknown peer: long
        assert gateway.send({'t': 'pong', 'g': 'G1'}).wait(0.5)
        assert json.loads(gw_iface.sent[-1][1])['t'] == 'po'     # opt-in seeds negotiation
        assert _wait_for(lambda: supervisor.peer_supports_short('G1'))
        assert supervisor.send({'t': 'sync', 'g': 'G1', 'sec': 1}).wait(0.5)
        assert json.loads(sup_iface.sent[-1][1])['t'] == 'sy'
        assert json.loads(gw_rx[-1])['t'] == 'sync'
        assert json.loads(sup_rx[-1])['t'] == 'pong'
    finally:
        _stop(supervisor, sup_thread)
        _stop(gateway, gw_thread)


def main():
    tests = (
        test_type_detection_long_and_short,
        test_immediate_bypasses_slot_and_arbiter_front,
        test_lbt_waits_and_respects_cap,
        test_bulk_stays_slot_gated,
        test_codec_negotiation_and_dual_decode,
        test_two_transport_loopback_negotiates,
    )
    for test in tests:
        test()
        print(f'OK {test.__name__}')
    print(f'OK fast lane: {len(tests)} tests')


if __name__ == '__main__':
    main()
