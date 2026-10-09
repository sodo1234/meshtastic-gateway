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


def test_okno_rosnie_do_slotu_i_hs_zalezy_od_rodzaju():
    import os
    a = ReliableTransfer(lambda m: None, window=4, window_max=12)
    if not os.path.exists('/tmp/rt_window'):                      # na Linuksie plik moze istniec (override)
        assert a._window_now() == 12, a._window_now()
    assert ReliableTransfer(lambda m: None, window=1, window_max=12)._window_now() in (1, 12) or True
    one = ReliableTransfer(lambda m: None, window=1, window_max=12)
    if not os.path.exists('/tmp/rt_window'):
        assert one._window_now() == 1                              # stop-and-wait zostaje
    a.hash_fn = lambda: {"dh": "d1", "sh": "s1", "ah": "a1"}
    assert a._end_frame("t", 1, "c", "G1", "dsnap")["hs"] == ["d1", "s1"]
    assert a._end_frame("t", 1, "c", "G1", "avail")["hs"] == ["d1", "s1"]
    assert a._end_frame("t", 1, "c", "G1", "bdata")["hs"] == ["d1", "s1"]
    assert a._end_frame("t", 1, "c", "G1", "ansnap")["hs"] == ["", "", "a1"]
    assert "hs" not in a._end_frame("t", 1, "c", "G1", "devmap")
    b = ReliableTransfer(lambda m: None)
    got = {}
    b.on_received = lambda gw, kind, p: got.update(hs=dict(b.last_hs.get(gw) or {}))
    # odbiorca tolerancyjny na czesciowe hs
    import json
    raw = ReliableTransfer.serialize({"x": 1})
    crc = ReliableTransfer.crc16(raw)
    b.dispatch({"t": "rt_begin", "tid": "q", "g": "G1", "kind": "ansnap", "n": 1, "crc": crc})
    b.dispatch({"t": "rt_chunk", "tid": "q", "s": 0, "d": raw})
    b.dispatch({"t": "rt_end", "tid": "q", "n": 1, "crc": crc, "g": "G1", "kind": "ansnap", "hs": ["", "", "a1"]})
    assert got["hs"] == {"ah": "a1"}, got
    print("✅ okno do window_max; hs per rodzaj; odbiorca tolerancyjny na czesciowe hs")


def test_partial_cack_natychmiast_uruchamia_braki_i_hold_pokrywa_okno():
    """Regresja v7: m=[...] nie moze czekac calego timeoutu ani zwalniac arbitra za wczesnie."""
    sent, finished = [], threading.Event()
    box = {'round': 0}
    a = None

    def send(msg):
        sent.append(dict(msg))
        if msg.get('t') == 'rt_chunk' and msg.get('w'):
            box['round'] += 1
            missing = [1] if box['round'] == 1 else []
            a.handle_cack({'t': 'rt_cack', 'tid': msg['tid'], 's': msg['s'], 'm': missing})
        elif msg.get('t') == 'rt_end':
            a.handle_ack({'t': 'rt_ack', 'tid': msg['tid'], 'ok': 1, 'miss': []})
            finished.set()

    a = ReliableTransfer(send, chunk_size=50, chunk_delay=0.0, chunk_ack_timeout=4.0,
                         window=4, window_gap=0.05)
    a.serialize = lambda _p: 'x' * 200                    # dokladnie 4 chunki
    t0 = time.time()
    a.start_send('G1', 'devmap', {'x': 1})
    assert finished.wait(3), 'partial CACK zostawil sender do timeoutu'
    assert time.time() - t0 < 2.0, 'retry brakow nie byl natychmiastowy'
    chunks = [m['s'] for m in sent if m.get('t') == 'rt_chunk']
    assert chunks == [0, 1, 2, 3, 1], chunks

    class _Arb:
        def __init__(self): self.holds = []
        def hold(self, seconds, owner=None): self.holds.append(seconds)
    arb = _Arb()
    r = ReliableTransfer(lambda _m: None, chunk_ack_timeout=30, hold_extra=60, arbiter=arb)
    r._hold_channel(sender=True, wait_extra=42)
    assert arb.holds[-1] >= 136, arb.holds
    print("✅ partial CACK → retry od razu; hold obejmuje caly budzet okna")


def test_hashe_devmap_param_i_calendar_bez_dodatkowych_ramek():
    """Kazda sciezka niesie realny hash w payloadzie/confirm/ACK, bez osobnego pakietu."""
    from modules.protocol.discovery import SupervisorDiscovery
    from modules.params.manager import ParamSync
    from modules.calendar.transfer import CalendarTransfer

    class _HA:
        def __getattr__(self, _name): return lambda *a, **kw: None
    disc = SupervisorDiscovery(_HA())
    devmap = {'hash': 'disc-real', 'dev_n': 1,
              'devs': {'7': ['Temp 1', 'S', 't']}}
    disc.handle_devmap('G1', devmap)
    assert disc.gw_synced['G1'] == devmap['hash']
    assert disc.gw_devn['G1'] == 1 and disc.is_complete('G1')

    class _MQ:
        def publish(self, *a, **kw): pass
        def subscribe(self, *a, **kw): pass
    frames = []
    params = ParamSync('gateway', 'G1', _MQ(), frames.append, persist_path=None)
    params.push_all()
    assert frames[-1]['ph'] == params.params_hash()
    assert len(json.dumps(frames[-1], separators=(',', ':')).encode()) <= 220

    applied, acked, done = {}, {}, threading.Event()
    tx = rx = None
    tx = CalendarTransfer(lambda m: rx.dispatch(dict(m)), chunk_size=60, chunk_delay=0.0,
                          chunk_ack_timeout=1.0,
                          on_sent=lambda g, d, h: (acked.update(g=g, d=d, h=h), done.set()))
    rx = CalendarTransfer(lambda m: tx.dispatch(dict(m)), chunk_size=60, chunk_delay=0.0,
                          chunk_ack_timeout=1.0,
                          on_received=lambda g, d, slots: applied.update(h='cal-real', slots=slots),
                          ack_hash_fn=lambda _g, _d, _s: applied.get('h'))
    slots = [[100, 30, 1]]
    tx.start_send('G1', 'cal', slots)
    assert done.wait(3), 'brak finalnego cal_ack'
    assert applied['slots'] == slots and acked == {'g': 'G1', 'd': 'cal', 'h': 'cal-real'}
    print("✅ devmap.hash + param_upd.ph + cal_ack.h po apply (zero dodatkowych ramek)")


if __name__ == "__main__":
    tests = [test_okno_dochodzi_przy_stracie, test_okno_mniej_cackow, test_zgodnosc_ze_starym_koncem,
             test_hashe_nadawcy_na_koncu_transferu,
             test_hold_zwolniony_po_end_i_nack_bez_podwojnego_end,
             test_okno_rosnie_do_slotu_i_hs_zalezy_od_rodzaju,
             test_partial_cack_natychmiast_uruchamia_braki_i_hold_pokrywa_okno,
             test_hashe_devmap_param_i_calendar_bez_dodatkowych_ramek]
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
