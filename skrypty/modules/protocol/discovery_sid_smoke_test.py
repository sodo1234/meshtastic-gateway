"""Smoke test: STABLE SID (2026-07-21). No hardware.

sid trwały per-nazwa (persist) zamiast pozycji w sorted() — dodanie/usunięcie urządzenia
NIE przesuwa sidów pozostałych (dryf łamał mapę supervisora po restarcie bramki).

Usage:
    cd skrypty
    python3 -m modules.protocol.discovery_sid_smoke_test
"""
import json
import os
import sys
import tempfile

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.protocol.discovery import GatewayDiscovery, SupervisorDiscovery


def _mk(tmp, names):
    d = GatewayDiscovery("G2", [], [], [], lora=None, sid_persist_path=tmp)
    d.devices = {n: {"type": "sensor", "caps": ["temperature"]} for n in names}
    d._assign_short_ids()
    return d


def test_lock_in_equals_sorted():
    """1. uruchomienie (pusty persist) = sidy jak sorted() — kompat wstecz, zero zerwania."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_lockin.json")
    if os.path.exists(tmp): os.remove(tmp)
    d = _mk(tmp, ["Temp 1", "Temp 2", "Door 1", "Leak 1"])
    assert d.short_ids == {"Door 1": 0, "Leak 1": 1, "Temp 1": 2, "Temp 2": 3}, d.short_ids
    os.remove(tmp)
    print("✅ lock-in = sorted positions (kompat wstecz)")


def test_add_does_not_shift():
    """Dodanie urządzenia sortującego się PIERWSZE nie przesuwa istniejących sidów."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_add.json")
    if os.path.exists(tmp): os.remove(tmp)
    _mk(tmp, ["Temp 1", "Temp 2", "Door 1", "Leak 1"])          # lock-in
    d = _mk(tmp, ["Button 1", "Temp 1", "Temp 2", "Door 1", "Leak 1"])  # "Button 1" < "Door 1"
    assert d.short_ids["Door 1"] == 0 and d.short_ids["Leak 1"] == 1, d.short_ids
    assert d.short_ids["Temp 1"] == 2 and d.short_ids["Temp 2"] == 3, d.short_ids
    assert d.short_ids["Button 1"] == 4, ("nowe → najniższy wolny =4", d.short_ids)
    os.remove(tmp)
    print("✅ dodanie urządzenia NIE przesuwa istniejących; nowe = najniższy wolny sid")


def test_remove_reserves_and_returns_same():
    """Usunięte urządzenie → sid zarezerwowany (nie reuse); powrót odzyskuje TEN SAM sid."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_rm.json")
    if os.path.exists(tmp): os.remove(tmp)
    _mk(tmp, ["Temp 1", "Temp 2", "Door 1", "Leak 1"])          # Door=0
    _mk(tmp, ["Temp 1", "Temp 2", "Leak 1"])                    # Door znika
    d = _mk(tmp, ["Temp 1", "Temp 2", "Door 1", "Leak 1"])      # Door wraca
    assert d.short_ids["Door 1"] == 0, ("Door odzyskuje sid 0", d.short_ids)
    os.remove(tmp)
    print("✅ usunięte→reserved; powracające odzyskuje swój sid")


def test_persist_survives_reload():
    """Mapa przetrwa restart (nowa instancja z tego samego pliku = te same sidy)."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_persist.json")
    if os.path.exists(tmp): os.remove(tmp)
    d1 = _mk(tmp, ["Temp 1", "Door 1", "Leak 1"])
    d2 = _mk(tmp, ["Temp 1", "Door 1", "Leak 1"])               # reload z pliku
    assert d1.short_ids == d2.short_ids, (d1.short_ids, d2.short_ids)
    assert d2._sid_persist, "persist wczytany"
    os.remove(tmp)
    print("✅ persist przetrwa restart (sidy identyczne po reload)")


def test_no_persist_path_falls_back():
    """Bez sid_persist_path → działa (przydział in-memory, jak lock-in)."""
    d = GatewayDiscovery("G2", [], [], [], lora=None, sid_persist_path=None)
    d.devices = {n: {"type": "sensor", "caps": ["temperature"]} for n in ["B", "A", "C"]}
    d._assign_short_ids()
    assert d.short_ids == {"A": 0, "B": 1, "C": 2}, d.short_ids
    print("✅ brak persist_path → fallback in-memory OK")


def _payload(names):
    import json
    return json.dumps([{"friendly_name": n, "ieee_address": f"0x{i:016x}",
                        "type": "EndDevice", "definition": {"model": "SNZB-02P",
                        "exposes": [{"property": "temperature", "type": "numeric"}]}}
                       for i, n in enumerate(names)])


def test_prune_usuniete_z_z2m():
    """REGRESJA 2026-09-17: `parse_z2m` tylko DODAWALO — usuniete urzadzenie zostawalo w mapie.

    Objaw na G2: po skasowaniu 30 duchow carportowych z2m mial 9 urzadzen, a bramka nadal
    raportowala `dev=39` (i taki sam `disc_hash`) az do restartu procesu."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_prune.json")
    if os.path.exists(tmp): os.remove(tmp)
    d = GatewayDiscovery("G2", [], [], [], lora=None, sid_persist_path=tmp)
    d.parse_z2m(_payload(["Test 1", "Test 2", "RA4_2", "RB6_R5"]))
    assert len(d.devices) == 4, d.devices.keys()
    sid_test1 = d.short_ids["Test 1"]
    h_przed = d.disc_hash

    d.parse_z2m(_payload(["Test 1", "Test 2"]))            # duchy skasowane w z2m
    assert set(d.devices) == {"Test 1", "Test 2"}, d.devices.keys()
    assert d.disc_hash != h_przed, "hash disc MUSI sie zmienic po usunieciu urzadzen"
    assert d.short_ids["Test 1"] == sid_test1, "prune nie moze przesunac sidow pozostalych"

    # PUSTA lista NIE czysci mapy (z2m potrafi mrugnac pustka przy restarcie mostka)
    d.parse_z2m("[]")
    assert set(d.devices) == {"Test 1", "Test 2"}, "pusty payload skasowal cala bramke!"

    # powrot urzadzenia pod ta sama nazwa odzyskuje STARY sid (rezerwacja w persist)
    d.parse_z2m(_payload(["Test 1", "Test 2", "RA4_2", "RB6_R5"]))
    assert d.short_ids["Test 1"] == sid_test1
    os.remove(tmp)
    print("✅ prune: usuniete z z2m znikaja z mapy, pusty payload nie czysci, sidy stabilne")


def test_duchy_bez_interview_odfiltrowane():
    """REGRESJA 2026-10-01: z2m G2 mial 37 duchow (interview_completed:false) z sieci carportu."""
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_ghost.json")
    if os.path.exists(tmp): os.remove(tmp)
    d = GatewayDiscovery("G2", ["Duch Monitorowany"], [], [], lora=None, sid_persist_path=tmp)
    lst = json.loads(_payload(["Test 1", "RB4_S11", "Duch Monitorowany"]))
    lst[1]["interview_completed"] = False                  # prawdziwy duch
    lst[2]["interview_completed"] = False                  # ale nazwany w monitored → zostaje
    lst[0]["interview_completed"] = True
    d.parse_z2m(json.dumps(lst))
    assert set(d.devices) == {"Test 1", "Duch Monitorowany"}, d.devices.keys()
    os.remove(tmp)
    print("✅ duchy: interview_completed=false odfiltrowane, monitored zostaje")


def test_devmap_v2_roundtrip_i_rozmiar():
    """Format v2 mapy: ta sama tresc co v1 po rozwinieciu, wyraznie mniej bajtow."""
    import zlib, base64
    tmp = os.path.join(tempfile.gettempdir(), "test_sid_devmap.json")
    if os.path.exists(tmp): os.remove(tmp)
    names = [f"RB{i // 3}_{i % 3 + 1}" for i in range(60)] + ["Temp 1", "Door 1"]
    d = GatewayDiscovery("G1", [], [], [], lora=None, sid_persist_path=tmp)
    d.parse_z2m(_payload(names))
    d.devmap_v2 = False; v1 = d.build_devmap()
    d.devmap_v2 = True; v2 = d.build_devmap()
    assert v2.get('v') == 2 and 'devs' not in v2
    got = {}
    class _HA:
        def __getattr__(self, _n): return lambda *a, **k: None
    sup = SupervisorDiscovery(_HA())
    sup.handle_devmap("G1", v2)
    sup2 = SupervisorDiscovery(_HA()); sup2.handle_devmap("G1", v1)
    assert sup.gw_devices["G1"] == sup2.gw_devices["G1"] and len(sup.gw_devices["G1"]) == 62
    sup.gw_devices["G1"]["Duch"] = {"sid": 99, "type": "sensor", "caps": ""}
    sup.handle_devmap("G1", v2)                          # pelna migawka kasuje nieobecnych
    assert "Duch" not in sup.gw_devices["G1"] and len(sup.gw_devices["G1"]) == 62
    size = lambda p: len(base64.b64encode(zlib.compress(json.dumps(p, separators=(',', ':')).encode(), 9)))
    assert size(v2) < size(v1), (size(v2), size(v1))
    os.remove(tmp)
    print(f"✅ devmap v2: identyczna mapa, {size(v2)} B vs {size(v1)} B")


if __name__ == "__main__":
    tests = [test_lock_in_equals_sorted, test_add_does_not_shift, test_prune_usuniete_z_z2m,
             test_duchy_bez_interview_odfiltrowane, test_devmap_v2_roundtrip_i_rozmiar,
             test_remove_reserves_and_returns_same, test_persist_survives_reload,
             test_no_persist_path_falls_back]
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
