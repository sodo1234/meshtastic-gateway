"""Assert-only smoke tests for the LoRa top-level wire codec.

Run from ``skrypty`` with::

    python -m modules.transport.codec_smoke_test
"""
import ast
import json
import re
import time
from pathlib import Path

from .codec import KEYS, TYPES, decode, encode
from .lora_transport import LoraTransport
from .reliable_transfer import ReliableTransfer
from modules.calendar.transfer import CalendarTransfer


SAMPLES = {
    "ab": {"t": "ab", "g": "G1", "ts": 1700000000, "d": [[1, "th", 31.2]]},
    "ac_all": {"t": "ac_all", "g": "G1", "b": "offline"},
    "ac_b": {"t": "ac_b", "g": "G1", "d": [["Temp 1", "temp"]]},
    "an_d": {"t": "an_d", "g": "G1", "ts": 1700000000, "a": [], "r": [], "h": "abcd"},
    "ansnap_req": {"t": "ansnap_req", "g": "G1"},
    "b": {"t": "b", "g": "G1", "ts": 1700000000, "p": 7,
          "d": [[1, {"a": 1, "temperature": 22.5, "s": "ON"}]]},
    "back": {"t": "back", "g": "G1", "p": 7},
    "cal_ack": {"t": "cal_ack", "g": "G1", "tid": "a1b2", "ok": 0, "miss": [2]},
    "cal_begin": {"t": "cal_begin", "g": "G1", "tid": "a1b2", "dir": "gw_push", "n": 2, "crc": "cafe"},
    "cal_cack": {"t": "cal_cack", "g": "G1", "tid": "a1b2", "s": 0},
    "cal_chunk": {"t": "cal_chunk", "g": "G1", "tid": "a1b2", "s": 0, "d": "eJyrVkrLz1eyUkpKLFKqBQA"},
    "cal_end": {"t": "cal_end", "g": "G1", "tid": "a1b2", "dir": "cal", "n": 2, "crc": "cafe"},
    "cal_pull": {"t": "cal_pull", "g": "G1"},
    "cal_req": {"t": "cal_req", "g": "G1"},
    "cfg": {"t": "cfg", "g": "G1", "to": {"sw": 1800, "sn": 86400}},
    "cmd": {"t": "cmd", "g": "G1", "d": "Relay 1", "c": "state", "v": "ON"},
    "cmdm": {"t": "cmdm", "g": "G1", "d": ["Relay 1", "Relay 2"], "c": "state", "v": "OFF"},
    "db": {"t": "db", "g": "G1", "d": [[1, "Temp 1", "S", "bth"]]},
    "disc": {"t": "disc", "g": "G1", "h": "abcd"},
    "disc_ack": {"t": "disc_ack", "g": "G1"},
    "disc_ap": {"t": "disc_ap", "g": "G1", "hash": "abcd", "mon": [1], "pri": [2]},
    "disc_meta": {"t": "disc_meta", "g": "G1", "hash": "abcd", "dev_n": 199},
    "disc_vio": {"t": "disc_vio", "g": "G1", "d": [["all_on", "b", "All on", 0]]},
    "dsnap_req": {"t": "dsnap_req", "g": "G1"},
    "dump_anom": {"t": "dump_anom", "g": "G1"},
    "hb": {"t": "hb", "g": "G1", "up": 123456, "dev": 199, "mon": 82, "pri": 78,
           "ts": 1700000000, "hash": "abcd", "cal": "cafe", "ph": "1234", "dh": "5678",
           "sh": "9abc", "ah": "def0", "gm": "calendar", "ga": 1, "tq": "synced", "m": 1},
    "hs": {"t": "hs", "g": "G1", "ts": 1700000000, "hash": "abc123def456", "ph": "1234abcd", "cal": "aaaabbbbcccc", "ah": "def0", "dh": "5678", "sh": "9abc"},
    "param_upd": {"t": "param_upd", "g": "G1", "d": {"P1": 20}, "ph": "abcd"},
    "params": {"t": "params", "g": "G1", "d": {"P1": 20, "T1": 1800}},
    "params_req": {"t": "params_req", "g": "G1"},
    "ping": {"t": "ping", "g": "G1"},
    "pong": {"t": "pong", "g": "G1", "up": 123456, "dev": 199, "mon": 82, "pri": 78,
             "ts": 1700000000, "hash": "abcd", "cal": "cafe", "ph": "1234"},
    "req": {"t": "req", "g": "G1", "d": "Temp 1"},
    "rt_ack": {"t": "rt_ack", "g": "G1", "tid": "a1b2", "ok": 0, "miss": [2]},
    "rt_begin": {"t": "rt_begin", "g": "G1", "tid": "a1b2", "kind": "devmap", "n": 3, "crc": "cafe", "win": 4},
    "rt_cack": {"t": "rt_cack", "g": "G1", "tid": "a1b2", "s": 3, "m": [1]},
    "rt_chunk": {"t": "rt_chunk", "g": "G1", "tid": "a1b2", "s": 0, "d": "A" * 90, "w": 1},
    "rt_end": {"t": "rt_end", "g": "G1", "tid": "a1b2", "kind": "ansnap", "n": 3, "crc": "cafe", "hs": ["", "", "abcd"]},
    "set_mode": {"t": "set_mode", "g": "G1", "m": "calendar"},
    "st": {"t": "st", "g": "G1", "d": "Relay 1", "c": "state", "v": "ON"},
    "stm": {"t": "stm", "g": "G1", "c": "state", "d": {"Relay 1": "ON", "Relay 2": "OFF"}},
    "sup_ping": {"t": "sup_ping", "g": "G1"},
    "sup_pong": {"t": "sup_pong", "g": "G1", "sec": 1700000000, "zg": "1111"},
    "sync": {"t": "sync", "g": "G1", "sec": 1700000000},
    "sync_req": {"t": "sync_req", "g": "G1"},
    "vbtn": {"t": "vbtn", "g": "G1", "id": "all_on"},
    "vbtn_ack": {"t": "vbtn_ack", "g": "G1", "id": "all_on"},
    "vsw": {"t": "vsw", "g": "G1", "id": "auto", "v": 1},
    "vsw_st": {"t": "vsw_st", "g": "G1", "id": "auto", "v": 1},
    "zg": {"t": "zg", "g": "G1", "z": "1111"},
}


def packed(frame):
    return json.dumps(frame, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def production_frame_types():
    """AST inventory of builders/registrations plus dynamic ReliableTransfer types."""
    root = Path(__file__).resolve().parents[2]
    paths = [root / "Meshtastic_SCADA.py"]
    paths += [p for p in (root / "modules").rglob("*.py")
              if "__pycache__" not in p.parts and not p.name.endswith("_test.py")
              and p.name != "collision_sim.py"]
    found = set()
    for path in paths:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if (isinstance(key, ast.Constant) and key.value == "t"
                            and isinstance(value, ast.Constant) and isinstance(value.value, str)):
                        found.add(value.value)
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "register" and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)):
                found.add(node.args[0].value)
        if path.name == "reliable_transfer.py":
            found.update("rt_" + suffix for suffix in re.findall(r"self\.p\}_([a-z]+)", source))
    return found


def test_tables_and_inventory():
    for scope, table in TYPES.items():
        assert len(table.values()) == len(set(table.values())), f"kolizja TYPES[{scope}]"
        assert all(1 <= len(short) <= 3 for short in table.values())
    for frame_type, table in KEYS.items():
        assert frame_type in TYPES["t"]
        assert len(table.values()) == len(set(table.values())), f"kolizja KEYS[{frame_type}]"
        assert all(len(long) > 1 and 1 <= len(short) <= 3 for long, short in table.items())
    used = production_frame_types()
    assert used == set(TYPES["t"]), (
        f"brak w TYPES={sorted(used - set(TYPES['t']))}; martwe={sorted(set(TYPES['t']) - used)}")
    assert set(SAMPLES) == set(TYPES["t"])


def test_round_trips_and_old_form():
    for frame_type, sample in SAMPLES.items():
        wire = encode(sample)
        assert wire.get("g") == "G1", frame_type
        assert decode(wire) == sample, (frame_type, wire, decode(wire))
        assert decode(sample) == sample, f"old form changed: {frame_type}"
        assert len(packed(wire)) <= 220, (frame_type, len(packed(wire)))
        assert len(wire) == len(set(wire)), f"wire key collision: {frame_type}"
    assert encode(SAMPLES["b"])["d"][0][1]["temperature"] == 22.5
    unknown = {"t": "future_type", "g": "G9", "future_key": {"long_nested": 1}}
    assert decode(encode(unknown)) == unknown


def test_required_size_reduction():
    for frame_type in ("rt_chunk", "rt_cack", "rt_end", "hb", "cmd", "st", "params"):
        before = len(packed(SAMPLES[frame_type]))
        after = len(packed(encode(SAMPLES[frame_type])))
        assert after < before, f"{frame_type}: {before} -> {after}"


def test_lora_transport_boundary():
    received = []
    transport = LoraTransport([], on_receive=received.append, tx_cooldown=0,
                              codec_short_tx=True)
    transport.send(SAMPLES["rt_begin"])
    wire_text = transport._tx_queue[-1]["text"]
    wire = json.loads(wire_text)
    assert wire["t"] == TYPES["t"]["rt_begin"] and "k" in wire and "kind" not in wire
    transport._mesh_rx({"decoded": {"text": wire_text}}, None)
    assert json.loads(received[-1]) == SAMPLES["rt_begin"]


def _wait_for(predicate, timeout=4.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_reliable_and_calendar_e2e():
    """Two peers exchange real transfer state machines through encode/decode."""
    rt_got, cal_got, wire_frames = [], [], []
    peers = {}

    def bridge(target, frame):
        wire = encode(frame)
        blob = packed(wire)
        assert len(blob) <= 220
        wire_frames.append(wire)
        peers[target].dispatch(decode(json.loads(blob.decode("utf-8"))))

    rt_a = ReliableTransfer(lambda f: bridge("rt_b", f), chunk_size=24, chunk_delay=0,
                            chunk_ack_timeout=1.0, chunk_retries=1, end_retries=1)
    rt_b = ReliableTransfer(lambda f: bridge("rt_a", f), chunk_size=24, chunk_delay=0,
                            chunk_ack_timeout=1.0, chunk_retries=1, end_retries=1,
                            on_received=lambda g, k, p: rt_got.append((g, k, p)))
    peers.update(rt_a=rt_a, rt_b=rt_b)
    payload = {"devices": [[i, "Relay", {"state": "ON"}] for i in range(8)]}
    rt_a.start_send("G1", "devmap", payload)
    assert _wait_for(lambda: bool(rt_got)), "ReliableTransfer timeout"
    assert rt_got == [("G1", "devmap", payload)]

    cal_a = CalendarTransfer(lambda f: bridge("cal_b", f), chunk_size=24, chunk_delay=0,
                             chunk_ack_timeout=1.0, chunk_retries=1, end_retries=1)
    cal_b = CalendarTransfer(lambda f: bridge("cal_a", f), chunk_size=24, chunk_delay=0,
                             chunk_ack_timeout=1.0, chunk_retries=1, end_retries=1,
                             on_received=lambda g, d, p: cal_got.append((g, d, p)))
    peers.update(cal_a=cal_a, cal_b=cal_b)
    slots = [[480, 60, 1], [720, 30, 2], [900, 120, 3]]
    cal_a.start_send("G1", "gw_push", slots)
    assert _wait_for(lambda: bool(cal_got)), "CalendarTransfer timeout"
    assert cal_got == [("G1", "gw_push", slots)]
    assert any(f.get("t") == TYPES["t"]["rt_chunk"] for f in wire_frames)
    assert any(f.get("t") == TYPES["t"]["cal_chunk"] for f in wire_frames)


def main():
    tests = (test_tables_and_inventory, test_round_trips_and_old_form,
             test_required_size_reduction, test_lora_transport_boundary,
             test_reliable_and_calendar_e2e)
    for test in tests:
        test()
        print(f"OK {test.__name__}")
    print(f"OK codec: {len(TYPES['t'])} typów ramek, "
          f"{sum(len(v) for v in KEYS.values())} mapowań kluczy")


if __name__ == "__main__":
    main()
