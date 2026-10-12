"""Reprodukcje trzech podejrzanych regresji v12 (bez sprzetu).

Uruchomienie z katalogu ``skrypty``::

    python tests/test_v12_suspected_bugs.py

Kazdy test opisuje stan poprawny: jest czerwony, gdy podejrzany blad istnieje.
"""
import json
import os
import sys
import threading
import time

# Pozwala uruchomic plik bezposrednio oraz przez pytest z katalogu repo.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _calendar_frames(transfer, slots, tid="v12-apply"):
    payload = transfer.serialize(slots)
    crc = transfer.crc16(payload)
    begin = {"t": "cal_begin", "tid": tid, "g": "G1", "dir": "push",
             "n": 1, "crc": crc}
    chunk = {"t": "cal_chunk", "tid": tid, "s": 0, "d": payload}
    return begin, chunk


def test_calendar_apply_error_never_sends_positive_ack():
    """(a) Czerwony, jesli wyjatek apply nadal konczy sie cal_ack ok=1."""
    from modules.calendar import CalendarTransfer

    sent = []

    def failing_apply(_gw, _direction, _slots):
        raise RuntimeError("kontrolowany blad apply")

    transfer = CalendarTransfer(sent.append, chunk_size=1000,
                                on_received=failing_apply)
    begin, chunk = _calendar_frames(transfer, [[100, 60, 1]])
    transfer.dispatch(begin)
    transfer.dispatch(chunk)  # komplet danych uruchamia apply bez czekania na cal_end

    assert not any(frame.get("t") == "cal_ack" and frame.get("ok") == 1
                   for frame in sent), f"blad apply zostal potwierdzony: {sent}"
    assert "v12-apply" in transfer.incoming
    assert "v12-apply" not in transfer.completed


def test_offline_skip_removes_stale_ack_flag():
    """(b) Czerwony, jesli skip_dev usuwa offline, ale zostawia ack."""
    from modules.anomaly import GatewayOfflineAnomaly

    class Discovery:
        devices = {"DEV1": {"type": "switch"}}
        short_ids = {"DEV1": 7}

    class Data:
        _alive = {"DEV1": False}

    excluded = set()
    monitor = GatewayOfflineAnomaly(
        "G1", Discovery(), Data(), emit=lambda *_args: None,
        get_timeout=lambda _dev: 1, skip_dev=lambda dev: dev in excluded)
    monitor.offline.add("DEV1")
    monitor.ack.add("DEV1")

    excluded.add("DEV1")
    monitor.check()

    assert "DEV1" not in monitor.offline
    assert "DEV1" not in monitor.ack, "skip_dev zostawil flage ack"


def test_lbt_requeue_reaches_hard_cap_without_wedging_tx_thread():
    """(c) Czerwony, jesli appendleft/popleft zapetla TX mimo lbt_hard_cap."""
    from modules.transport import LoraTransport

    class Interface:
        def __init__(self):
            self.sent = []

        def sendText(self, text):
            self.sent.append((time.monotonic(), json.loads(text)))

    hard_cap = 0.18
    transport = LoraTransport([], tx_cooldown=0, dedup_seconds=0,
                              lbt_quiet=5.0, lbt_max_wait=0.025,
                              lbt_hard_cap=hard_cap,
                              codec_role="supervisor")
    interface = Interface()
    transport.interfaces["TEST"] = interface
    keep_busy = threading.Event()
    keep_busy.set()

    def noise():
        while keep_busy.is_set():
            transport._mesh_rx({}, None)
            time.sleep(0.005)

    noise_thread = threading.Thread(target=noise, daemon=True)
    tx_thread = threading.Thread(target=transport._tx_loop, daemon=True)
    noise_thread.start()
    tx_thread.start()
    started = time.monotonic()
    try:
        sent = transport.send({"t": "cmd", "g": "G1"})
        assert sent.wait(0.7), "TX thread utknal w petli requeue po lbt_hard_cap"
        elapsed = interface.sent[0][0] - started
        assert elapsed >= hard_cap * 0.7, "ramka ominela twardy limit LBT"
        assert elapsed < 0.7, f"ramka wyszla za pozno: {elapsed:.3f}s"
    finally:
        keep_busy.clear()
        transport.running = False
        noise_thread.join(0.3)
        tx_thread.join(0.5)
    assert not tx_thread.is_alive(), "watek TX nie daje sie zatrzymac po requeue"


if __name__ == "__main__":
    tests = (
        test_calendar_apply_error_never_sends_positive_ack,
        test_offline_skip_removes_stale_ack_flag,
        test_lbt_requeue_reaches_hard_cap_without_wedging_tx_thread,
    )
    failures = 0
    for test in tests:
        try:
            test()
            print(f"GREEN {test.__name__}")
        except Exception as exc:
            failures += 1
            print(f"RED   {test.__name__}: {exc}")
    raise SystemExit(bool(failures))
