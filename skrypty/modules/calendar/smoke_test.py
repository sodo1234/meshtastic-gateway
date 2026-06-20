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
    base = (datetime.now() + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
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


def test_expand_overlapping_same_start():
    """REGRESJA: dwa sloty o tym samym starcie, różnym czasie trwania (PRZERWA wewnątrz
    PRODUKCJI) muszą przetrwać expand→merge i odtworzyć hash mastera (id po (start,dur))."""
    m = _mgr()
    compact = [[237960, 240, 2], [237960, 480, 1]]              # ten sam start, różny dur
    full = m.expand_compact(compact)
    assert len({s["id"] for s in full}) == 2                    # różne id, brak kolizji
    n = m.merge_schedule("G1", full)
    assert n == 2 and len(m.list_slots("G1")) == 2              # OBA sloty zachowane (nie 1!)
    # hash bramki po merge == hash kompaktu mastera → brak wiecznego driftu/re-sync
    master = _mgr(); master.data["GLOBAL"] = full
    back, h_master = master.prepare_compact("G1", window_days=3650)
    h_gw = m.schedule_hash("G1", 3650)
    assert h_gw == h_master, f"{h_gw} != {h_master}"
    print("✅ expand: nakładające się sloty (ten sam start) → oba zachowane + hash zbieżny")


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


def test_transfer_duplicate_end_reack():
    """Idempotencja cal_end: po udanym odbiorze powtórny cal_end (gdy finalny ACK zginął na RF)
    musi ponownie wysłać ACK ok=1 — inaczej master 'rezygnuje' mimo poprawnego odbioru."""
    from modules.calendar import CalendarTransfer
    sent = []; got = []
    rx = CalendarTransfer(lambda m: sent.append(m), chunk_size=20,
                          on_received=lambda gw, d, s: got.append(s))
    slots = [[i * 30, 60, 1] for i in range(8)]
    begin, cf, end, n = _frames(slots, chunk_size=20)
    rx.dispatch(begin)
    for c in cf:
        rx.dispatch(c)
    assert rx.handle_end(end)[2] == slots                       # 1. odbiór OK → on_received
    assert sent[-1] == {"t": "cal_ack", "tid": "t1", "ok": 1, "miss": []}
    sent.clear()
    assert rx.handle_end(end) is None                           # 2. duplikat: nie złoży drugi raz
    assert sent[-1] == {"t": "cal_ack", "tid": "t1", "ok": 1, "miss": []}  # ...ale re-ACK ok=1
    assert len(got) == 1                                        # on_received tylko raz (bez duplikatu)
    print("✅ transfer duplikat cal_end → idempotentny re-ACK ok=1 (bez ponownego on_received)")


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


def test_build_ics():
    from modules.calendar import build_ics, DEFAULT_MODE_NAMES
    slots = [{"id": "s1", "start": "2026-06-11 08:00:00", "end": "2026-06-11 16:00:00", "mode": 1},
             {"id": "s2", "start": "2026-06-11 16:00:00", "end": "2026-06-11 17:00:00", "mode": 2}]
    ics = build_ics(slots, DEFAULT_MODE_NAMES, cal_name="LoRa G1")
    assert ics.startswith("BEGIN:VCALENDAR") and ics.rstrip().endswith("END:VCALENDAR")
    assert ics.count("BEGIN:VEVENT") == 2
    assert "DTSTART:20260611T080000" in ics and "DTEND:20260611T160000" in ics
    assert "SUMMARY:PRODUKCJA" in ics and "SUMMARY:PRZERWA" in ics
    assert "\r\n" in ics                                   # CRLF wymagane przez ICS
    print("✅ build_ics: VCALENDAR + VEVENT per slot + DTSTART/SUMMARY + CRLF")


def test_parse_ha_events():
    from modules.calendar import parse_ha_events
    events = [
        {"uid": "a", "summary": "🟢 PRODUKCJA", "start": {"dateTime": "2026-06-11T08:00:00"},
         "end": {"dateTime": "2026-06-11T16:00:00"}},
        {"uid": "b", "summary": "SERWIS", "start": {"date": "2026-06-12"}, "end": {"date": "2026-06-13"}},
        {"summary": "broken", "start": {}, "end": {}},      # brak dat → pominięty
    ]
    slots = parse_ha_events(events)
    assert len(slots) == 2                                  # broken pominięty
    assert slots[0]["id"] == "a" and slots[0]["start"] == "2026-06-11T08:00:00"
    assert slots[0]["note"] == "🟢 PRODUKCJA" and slots[0]["origin"] == "ha_api"
    assert slots[1]["start"] == "2026-06-12"                # date całodniowy
    # integracja z managerem: note → wykryty tryb
    m = _mgr()
    m.replace_global([{"start": s["start"].replace("T", " ") + (":00" if len(s["start"]) == 10 else ""),
                       "end": s["end"].replace("T", " ") + (":00" if len(s["end"]) == 10 else ""),
                       "note": s["note"]} for s in slots])
    modes = sorted(sl["mode"] for sl in m.list_slots("GLOBAL"))
    assert modes == [1, 3]                                  # PRODUKCJA + SERWIS wykryte z note
    print("✅ parse_ha_events: dateTime/date, skip pustych, note→tryb przez manager")


def test_calendar_e2e_push():
    """Dokładna ścieżka harnessu STEP 4: supervisor prepare_compact → ramki CalendarTransfer
    → gateway reassembly → on_received → expand_compact → merge → build_ics."""
    from modules.calendar import CalendarTransfer, build_ics, DEFAULT_MODE_NAMES
    # SUPERVISOR (master): zbuduj harmonogram → kompakt do wysyłki
    sup = _mgr()
    base = (datetime.now() + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
    f = '%Y-%m-%d %H:%M:%S'
    sup.add_slot("GLOBAL", base.strftime(f), (base + timedelta(hours=8)).strftime(f), note="PRODUKCJA")
    sup.add_slot("GLOBAL", (base + timedelta(hours=8)).strftime(f),
                 (base + timedelta(hours=8, minutes=30)).strftime(f), note="PRZERWA")
    compact, h = sup.prepare_compact("G1", window_days=30)
    assert len(compact) == 2 and len(h) == 12

    # GATEWAY: odbiór przez transfer (ramki budowane jak w start_send, ale synchronicznie)
    gw = _mgr()
    received = {}
    rx = CalendarTransfer(lambda m: None, chunk_size=24,
                          on_received=lambda g, d, slots: received.update(g=g, d=d, slots=slots))
    begin, chunks, end, n = _frames(compact, chunk_size=24, gw="G1", direction="cal")
    assert n >= 1
    rx.dispatch(begin)
    for c in chunks:
        rx.dispatch(c)
    rx.handle_end(end)
    assert received.get("slots") == compact and received["d"] == "cal"

    # GATEWAY recreate: expand → merge → ICS + bieżący tryb
    full = gw.expand_compact(received["slots"])
    gw.merge_schedule("G1", full)
    eff = gw.get_effective_schedule("G1")
    assert len(eff) == 2
    ics = build_ics(eff, DEFAULT_MODE_NAMES, cal_name="LoRa G1")
    assert ics.count("BEGIN:VEVENT") == 2
    assert "SUMMARY:PRODUKCJA" in ics and "SUMMARY:PRZERWA" in ics
    # hash bramki po merge == hash oczekiwany przez supervisora (brak driftu) → kluczowe dla re-sync
    assert gw.schedule_hash("G1", 30) == h
    print("✅ e2e push: prepare_compact→transfer→expand→merge→ICS + hash zgodny (brak driftu)")


def test_calendar_e2e_reverse():
    """REVERSE gw→sup: bramka prepare_compact → transfer → supervisor on_received → expand
    → sloty gotowe do write_ha_calendar; write→read (parse_ha_events) round-trip zachowuje tryb."""
    from modules.calendar import CalendarTransfer, parse_ha_events
    gw = _mgr()
    b = (datetime.now() + timedelta(days=1)).replace(hour=22, minute=0, second=0, microsecond=0)
    f = '%Y-%m-%d %H:%M:%S'
    gw.add_slot("G1", b.strftime(f), (b + timedelta(hours=8)).strftime(f), note="SERWIS")
    compact, h = gw.prepare_compact("G1", window_days=30)

    sup = _mgr()
    got = {}
    rx = CalendarTransfer(lambda m: None, chunk_size=30,
                          on_received=lambda g, d, slots: got.update(
                              g=g, d=d, full=sup.expand_compact(slots)))
    begin, chunks, end, n = _frames(compact, chunk_size=30, gw="G1", direction="gw_push")
    rx.dispatch(begin)
    for c in chunks:
        rx.dispatch(c)
    rx.handle_end(end)
    assert got["d"] == "gw_push" and len(got["full"]) == 1
    slot = got["full"][0]
    assert slot["mode"] == 3 and slot["note"] == "SERWIS"
    # symulacja zapisu na HA supervisora (calendar.lora_g1) + odczyt z powrotem → tryb zachowany
    evt = {"uid": "x", "summary": slot["note"],
           "start": {"dateTime": slot["start"].replace(" ", "T")},
           "end": {"dateTime": slot["end"].replace(" ", "T")}}
    back = parse_ha_events([evt])
    m2 = _mgr(); m2.replace_global([{"start": back[0]["start"].replace("T", " "),
                                     "end": back[0]["end"].replace("T", " "),
                                     "note": back[0]["note"]}])
    assert m2.list_slots("GLOBAL")[0]["mode"] == 3
    print("✅ e2e reverse: gw_push→expand→HA event→parse round-trip zachowuje tryb (SERWIS)")


if __name__ == "__main__":
    tests = [test_imports, test_detect_mode_length_sorted, test_add_dedup_and_mode,
             test_compact_roundtrip, test_effective_and_override,
             test_compute_now_and_next, test_merge, test_expand_overlapping_same_start,
             test_transfer_serialize_roundtrip, test_transfer_reassembly_ok,
             test_transfer_missing_chunk_nack, test_transfer_duplicate_end_reack,
             test_transfer_crc_mismatch,
             test_build_ics, test_parse_ha_events, test_calendar_e2e_push,
             test_calendar_e2e_reverse]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
