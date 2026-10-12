"""Assert-only smoke tests for LoRa fast lane and codec negotiation.

Run from ``skrypty`` with::

    python -m modules.transport.fastlane_smoke_test
"""
import json
import threading
import time

from .codec import decode, encode
from .fastlane import (DEFAULT_IMMEDIATE_TYPES, is_immediate_frame,
                       lbt_wait_or_requeue, lbt_wait_seconds, slot_wait_seconds)
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


def test_lbt_requeues_at_soft_cap_then_sends_after_silence():
    transport, iface, thread = _running_transport(
        lbt_quiet=0.06, lbt_max_wait=0.05, lbt_hard_cap=0.5,
        codec_role='supervisor')
    noisy = threading.Event(); noisy.set()
    noise_thread = threading.Thread(
        target=lambda: _keep_channel_busy(transport, noisy), daemon=True)
    noise_thread.start()
    try:
        before = time.monotonic()
        event = transport.send({'t': 'ping', 'g': 'G1'})
        assert not event.wait(0.12), 'soft LBT cap forced TX on a busy channel'
        noisy.clear(); noise_thread.join(0.2)
        assert event.wait(0.35), 'frame was not retried after channel silence'
        elapsed = iface.sent[0][0] - before
        assert 0.12 <= elapsed < 0.5, elapsed
        assert lbt_wait_seconds(before, before + 0.2, before, quiet=5, max_wait=0.1) == 0
        wait, requeue = lbt_wait_or_requeue(
            before, before + 0.2, before, before, quiet=5, max_wait=0.1, hard_cap=1)
        assert wait == 0 and requeue
    finally:
        noisy.clear()
        _stop(transport, thread)


def _keep_channel_busy(transport, running):
    while running.is_set():
        transport._mesh_rx({}, None)  # także ramka non-text oznacza zajęty kanał
        time.sleep(0.01)


def test_lbt_requeue_does_not_block_other_frames():
    """Ramka czekająca na ciszę (po miękkim limicie) nie może trzymać wątku TX do hard_cap:
    inne ramki z kolejki (np. cal_cack) wychodzą, zanim ping dostanie zgodę."""
    transport, iface, thread = _running_transport(
        lbt_quiet=5.0, lbt_max_wait=0.05, lbt_hard_cap=1.0,
        codec_role='supervisor')
    noisy = threading.Event(); noisy.set()
    noise_thread = threading.Thread(
        target=lambda: _keep_channel_busy(transport, noisy), daemon=True)
    noise_thread.start()
    try:
        ping = transport.send({'t': 'ping', 'g': 'G1'})
        time.sleep(0.02)
        transport.send({'t': 'cal_cack', 'g': 'G1', 'tid': 1, 's': 0})
        assert _wait_for(lambda: any('cal_cack' in t for _, t in iface.sent), 0.4), \
            'requeued fast-lane frame blocked the TX thread'
        assert not ping.is_set(), 'ping escaped LBT on a busy channel'
        assert ping.wait(1.2), 'hard cap did not release the requeued frame'
    finally:
        noisy.clear(); noise_thread.join(0.2)
        _stop(transport, thread)


def test_lbt_hard_cap_forces_control_frame():
    transport, iface, thread = _running_transport(
        lbt_quiet=5.0, lbt_max_wait=0.05, lbt_hard_cap=0.22,
        codec_role='supervisor')
    noisy = threading.Event(); noisy.set()
    noise_thread = threading.Thread(
        target=lambda: _keep_channel_busy(transport, noisy), daemon=True)
    noise_thread.start()
    try:
        before = time.monotonic()
        event = transport.send({'t': 'cmd', 'g': 'G1'})
        assert not event.wait(0.12), 'frame escaped before hard cap on busy channel'
        assert event.wait(0.35), 'hard cap did not release control frame'
        elapsed = iface.sent[0][0] - before
        assert 0.18 <= elapsed <= 0.45, elapsed
    finally:
        noisy.clear(); noise_thread.join(0.2)
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
        test_lbt_requeues_at_soft_cap_then_sends_after_silence,
        test_lbt_hard_cap_forces_control_frame,
        test_lbt_requeue_does_not_block_other_frames,
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
