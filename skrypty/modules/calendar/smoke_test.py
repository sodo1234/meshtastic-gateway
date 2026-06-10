"""Smoke test for calendar module (STEP 16). No hardware/broker.

Usage:
    cd skrypty
    python3 -m modules.calendar.smoke_test
"""
import sys
import time
from datetime import datetime, timedelta

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


def _mgr(**kw):
    from modules.calendar import ScheduleManager
    return ScheduleManager(persist_path=None, gateways=["G1", "G2"], **kw)


def test_imports():
    from modules.calendar import ScheduleManager, DEFAULT_MODE_NAMES
    assert ScheduleManager and DEFAULT_MODE_NAMES[1] == "PRODUKCJA"
    print("✅ imports OK")


def test_detect_mode_length_sorted():
    m = _mgr()
    # 'BRAK PRODUKCJI' musi wygrać z 'PRODUKCJA' (dłuższe słowo pierwsze)
    assert m._detect_mode("BRAK PRODUKCJI") == 0
    assert m._detect_mode("PRODUKCJA") == 1
    assert m._detect_mode("Przerwa śniadaniowa") == 2
    assert m._detect_mode("SERWIS maszyny") == 3
    assert m._detect_mode("cokolwiek innego") == 1        # default PRODUKCJA
    assert m._detect_mode("🟢 PRODUKCJA") == 1            # emoji stripped
    print("✅ _detect_mode: sort po długości + default + emoji")


def test_add_dedup_and_mode():
    m = _mgr()
    s = "2026-06-11 08:00:00"; e = "2026-06-11 16:00:00"
    id1 = m.add_slot("GLOBAL", s, e, note="PRODUKCJA")
    assert id1 and len(m.list_slots("GLOBAL")) == 1
    # ten sam zakres, inny tryb → update (nie duplikat)
    id2 = m.add_slot("GLOBAL", s, e, note="SERWIS")
    assert id2 == id1 and len(m.list_slots("GLOBAL")) == 1
    assert m.list_slots("GLOBAL")[0]["mode"] == 3
    print("✅ add_slot dedup + update trybu")


def test_compact_roundtrip():
    m = _mgr()
    base = datetime(2026, 6, 11, 8, 0, 0)
    m.add_slot("GLOBAL", base.strftime('%Y-%m-%d %H:%M:%S'),
               (base + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'), note="PRODUKCJA")
    compact, h = m.prepare_compact("G1", window_days=30)
    assert len(compact) == 1 and len(compact[0]) == 3 and len(h) == 12
    assert compact[0][1] == 480 and compact[0][2] == 1     # 8h = 480 min, mode 1
    back = m.expand_compact(compact)
    assert back[0]["mode"] == 1 and back[0]["note"] == "PRODUKCJA"
    # hash stabilny
    assert m.prepare_compact("G1", 30)[1] == h
    print("✅ compact pack/unpack + hash stabilny")


def test_effective_and_override():
    m = _mgr()
    m.add_slot("GLOBAL", "2026-06-11 08:00:00", "2026-06-11 16:00:00", note="PRODUKCJA")
    m.add_slot("G1", "2026-06-11 12:00:00", "2026-06-11 13:00:00", note="PRZERWA")
    eff = m.get_effective_schedule("G1")
    assert len(eff) == 2                                   # GLOBAL + override G1
    assert len(m.get_effective_schedule("G2")) == 1        # tylko GLOBAL
    print("✅ effective = GLOBAL + per-gw override")


def test_compute_now_and_next():
    m = _mgr()
    now = datetime.now()
    m.add_slot("GLOBAL", (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'),
               (now + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), note="PRODUKCJA")
    mode, nxt, src = m.compute_now_and_next("G1")
    assert mode == 1 and nxt > time.time() and src in ("global", "override")
    print("✅ compute_now_and_next: aktywny tryb + następna zmiana")


def test_merge():
    m = _mgr()
    incoming = [{"id": "x1", "start": "2026-06-11 08:00:00", "end": "2026-06-11 16:00:00",
                 "mode": 1, "note": "PRODUKCJA", "updated_ts": int(time.time())}]
    assert m.merge_schedule("G1", incoming) == 1
    # ten sam id, nowszy ts → update; starszy → ignor
    older = [dict(incoming[0], mode=3, updated_ts=incoming[0]["updated_ts"] - 100)]
    m.merge_schedule("G1", older)
    assert m.list_slots("G1")[0]["mode"] == 1              # starszy zignorowany
    print("✅ merge_schedule: po updated_ts (newer wins)")


def _frames(slots, chunk_size=140, tid="t1", gw="G1", direction="push"):
    from modules.calendar import CalendarTransfer as CT
    b64 = CT.serialize(slots); crc = CT.crc16(b64)
    chunks = [b64[i:i + chunk_size] for i in range(0, len(b64), chunk_size)] or ['']
    begin = {"t": "cal_begin", "tid": tid, "g": gw, "dir": direction, "n": len(chunks), "crc": crc}
    cf = [{"t": "cal_chunk", "tid": tid, "s": i, "d": c} for i, c in enumerate(chunks)]
    return begin, cf, {"t": "cal_end", "tid": tid}, len(chunks)


def test_transfer_serialize_roundtrip():
    from modules.calendar import CalendarTransfer
    slots = [[100, 480, 1], [600, 60, 2], [700, 120, 3]]
    b64 = CalendarTransfer.serialize(slots)
    assert CalendarTransfer.deserialize(b64) == slots
    assert len(CalendarTransfer.crc16(b64)) == 4
    print("✅ transfer serialize(zlib+b64)/deserialize roundtrip + crc")


def test_transfer_reassembly_ok():
    from modules.calendar import CalendarTransfer
    sent = []; got = []
    rx = CalendarTransfer(lambda m: sent.append(m), chunk_size=20,
                          on_received=lambda gw, d, s: got.append((gw, d, s)))
    slots = [[i * 30, 60, i % 4] for i in range(20)]            # duże → wiele chunków
    begin, cf, end, n = _frames(slots, chunk_size=20)
    assert n > 1, "powinno być wiele chunków"
    rx.dispatch(begin)
    for c in cf:
        rx.dispatch(c)
    res = rx.handle_end(end)
    assert res and res[2] == slots and got and got[0][2] == slots
    assert sent[-1] == {"t": "cal_ack", "tid": "t1", "ok": 1, "miss": []}
    print("✅ transfer reassembly (wiele chunków) → on_received + ACK ok=1")


def test_transfer_missing_chunk_nack():
    from modules.calendar import CalendarTransfer
    sent = []; got = []
    rx = CalendarTransfer(lambda m: sent.append(m), chunk_size=20,
                          on_received=lambda gw, d, s: got.append(s))
    slots = [[i * 30, 60, 1] for i in range(15)]
    begin, cf, end, n = _frames(slots, chunk_size=20)
    rx.dispatch(begin)
    for c in cf[:-1]:                                           # pomiń ostatni chunk
        rx.dispatch(c)
    assert rx.handle_end(end) is None and not got               # brak → NACK, brak on_received
    assert sent[-1]["t"] == "cal_ack" and sent[-1]["ok"] == 0 and sent[-1]["miss"]
    print("✅ transfer brakujący chunk → NACK ok=0+miss, brak on_received")


def test_transfer_crc_mismatch():
    from modules.calendar import CalendarTransfer
    sent = []
    rx = CalendarTransfer(lambda m: sent.append(m), chunk_size=200)
    slots = [[100, 480, 1]]
    begin, cf, end, n = _frames(slots, chunk_size=200)
    rx.dispatch(begin)
    cf[0]["d"] = cf[0]["d"][:-2] + "zz"                          # uszkodź dane
    rx.dispatch(cf[0])
    assert rx.handle_end(end) is None
    assert sent[-1]["ok"] == 0
    print("✅ transfer CRC mismatch → NACK ok=0")


if __name__ == "__main__":
    tests = [test_imports, test_detect_mode_length_sorted, test_add_dedup_and_mode,
             test_compact_roundtrip, test_effective_and_override,
             test_compute_now_and_next, test_merge,
             test_transfer_serialize_roundtrip, test_transfer_reassembly_ok,
             test_transfer_missing_chunk_nack, test_transfer_crc_mismatch]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
