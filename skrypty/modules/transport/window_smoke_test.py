"""Smoke test ReliableTransfer: OKNO z cack zbiorczym (2026-10-02). Bez sprzetu.

    cd skrypty && python3 -m modules.transport.window_smoke_test

Lacze symulowane w pamieci, ze strata ramek w OBU kierunkach. Sprawdzamy: (1) transfer dochodzi
przy stracie, (2) okno wysyla mniej ramek cack niz stop-and-wait, (3) zgodnosc ze starym
odbiorca/nadawca (window=1 po jednej ze stron).
"""
import json
import os
import random
import sys
import threading
import time

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.transport.reliable_transfer import ReliableTransfer


def _payload():
    rnd = random.Random(7)
    return {"devs": {str(i): [f"RB{i % 11}_{i % 3 + 1}", "W", "s" + "".join(rnd.choice("abcdef") for _ in range(6))]
                     for i in range(190)}}


def _run(win_tx, win_rx, loss, seed=1):
    rnd = random.Random(seed)
    frames = {"chunk": 0, "cack": 0, "other": 0}
    got = {}
    done = threading.Event()
    a = b = None

    def deliver(target):
        def f(msg):
            t = msg.get("t", "")
            key = "chunk" if t.endswith("_chunk") else "cack" if t.endswith("_cack") else "other"
            frames[key] += 1
            if rnd.random() < loss:
                return                                    # ramka zgubiona na RF
            threading.Thread(target=lambda: target.dispatch(json.loads(json.dumps(msg))), daemon=True).start()
        return f

    kw = dict(chunk_size=60, chunk_delay=0.0, chunk_ack_timeout=1.2, chunk_retries=8, end_retries=6,
              window_gap=0.05)
    a = ReliableTransfer(lambda m: deliver(b)(m), window=win_tx, **kw)
    b = ReliableTransfer(lambda m: deliver(a)(m), window=win_rx,
                         on_received=lambda gw, kind, p: (got.update(p=p), done.set()), **kw)
    t0 = time.time()
    a.start_send("G1", "devmap", _payload())
    ok = done.wait(60)
    return ok and got.get("p") == _payload(), frames, time.time() - t0


def test_okno_dochodzi_przy_stracie():
    for loss in (0.0, 0.15, 0.3):
        ok, fr, dt = _run(4, 4, loss)
        assert ok, f"transfer z oknem NIE doszedl przy stracie {loss}: {fr}"
    print("✅ okno=4: transfer dochodzi przy stracie 0/15/30%")


def test_okno_mniej_cackow():
    ok1, f1, _ = _run(1, 1, 0.0)
    ok4, f4, _ = _run(4, 4, 0.0)
    assert ok1 and ok4
    assert f4["cack"] * 2 <= f1["cack"], (f1, f4)
    print(f"✅ cack-i: stop-and-wait {f1['cack']} vs okno=4 {f4['cack']} (chunkow {f1['chunk']})")


def test_zgodnosc_ze_starym_koncem():
    ok_a, fa, _ = _run(4, 1, 0.1)          # nowy nadawca → stary odbiorca (cack po kazdym chunku)
    ok_b, fb, _ = _run(1, 4, 0.1)          # stary nadawca → nowy odbiorca (brak `win` w begin)
    assert ok_a and ok_b, (fa, fb)
    print("✅ zgodnosc wsteczna: okno↔stop-and-wait w obie strony")


def test_hashe_nadawcy_na_koncu_transferu():
    """rt_end niesie `hs` nadawcy; odbiorca widzi je w last_hs JUZ w on_received (zgodnosc bez HB)."""
    seen = {}
    box = {}
    def deliver(dst):
        return lambda m: dst.dispatch(m)
    kw = dict(chunk_size=60, chunk_delay=0.0, chunk_ack_timeout=1.2, window_gap=0.05)
    a = ReliableTransfer(lambda m: deliver(b)(m), **kw)
    b = ReliableTransfer(lambda m: deliver(a)(m),
                         on_received=lambda gw, kind, p: seen.update(hs=box["b"].last_hs.get(gw)), **kw)
    box["b"] = b
    a.hash_fn = lambda: {"dh": "aaaa1111", "sh": "bbbb2222"}
    done = threading.Event()
    b_cb = b.on_received
    b.on_received = lambda *x: (b_cb(*x), done.set())
    a.start_send("G1", "avail", _payload())
    assert done.wait(30), "transfer nie doszedl"
    assert seen.get("hs") == {"dh": "aaaa1111", "sh": "bbbb2222"}, seen
    print("✅ hs nadawcy dostepne w on_received")


def test_hold_zwolniony_po_end_i_nack_bez_podwojnego_end():
    from modules.transport.channel import ChannelArbiter
    # odbiorca zwalnia hold po rt_end
    arb = ChannelArbiter()
    sent = []
    a = ReliableTransfer(lambda m: b.dispatch(m), chunk_size=60, chunk_delay=0.0, chunk_ack_timeout=1.2)
    done = threading.Event()
    b = ReliableTransfer(lambda m: a.dispatch(m), chunk_size=60, chunk_delay=0.0, chunk_ack_timeout=1.2,
                         arbiter=arb, on_received=lambda *x: done.set())
    a.start_send("G1", "avail", _payload())
    assert done.wait(30)
    time.sleep(0.2)
    assert not arb.busy(), "hold odbiorcy wisi po rt_end"
    # NACK nie wysyla wlasnego rt_end (robi to tylko petla _tx_body)
    c = ReliableTransfer(lambda m: sent.append(m))
    c.outgoing["t1"] = {"chunks": ["x", "y"], "gw": "G1", "kind": "avail", "crc": "0", "acked": set(), "done": False}
    c.handle_ack({"tid": "t1", "ok": 0, "miss": [1]})
    assert [m["t"] for m in sent] == ["rt_chunk"], sent
    assert c.outgoing["t1"].get("resend_end") is True
    print("✅ hold zwolniony po rt_end; NACK bez dubla rt_end")


if __name__ == "__main__":
    tests = [test_okno_dochodzi_przy_stracie, test_okno_mniej_cackow, test_zgodnosc_ze_starym_koncem,
             test_hashe_nadawcy_na_koncu_transferu,
             test_hold_zwolniony_po_end_i_nack_bez_podwojnego_end]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"❌ {t.__name__}: {e}")
    print("\n" + "─" * 40)
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
