"""Regresja 2026-08-24 — bramka G1 milczała 10 dni mimo żywego procesu.

Trzy bugi, każdy potwierdzony logiem live; ten test pilnuje, żeby nie wróciły:
  1. SEED — retained availability z2m przy starcie NIE może iść jako 194 „zmiany"
     (na G1 = 32 pakiety `b` w sekundę → zapchana kolejka radia → force-close).
  2. WĄTKI — wiszący sendText() nie może mnożyć porzuconych wątków (było 3072, load 1731).
  3. PĘTLE — zwis portu nie może zamrozić pętli reconnectu (była 1 próba przez 10 dni).

Bez sprzętu. Uruchomienie:
    cd skrypty && python3 -m modules.transport.wedge_smoke_test
"""
import sys, time, threading

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


def test_startup_seed_nie_zalewa_lora():
    """1. Pierwsza availability urządzenia = seed (zapamiętaj), NIE ramka LoRa."""
    from modules.data.gateway_data import GatewayData

    class _Disc:
        devices = {f"RA{i}_1": {'type': 'switch'} for i in range(1, 100)}
        short_ids = {d: i for i, d in enumerate(devices)}
        def is_monitored(self, dev): return True

    gd = GatewayData.__new__(GatewayData)          # bez __init__ — testujemy samą regułę
    gd.disc, gd.log = _Disc(), None
    gd.states, gd.last_msg_ts = {}, {}
    gd._alive, gd._z2m_avail, gd._avail_pub, gd._a_sent = {}, {}, {}, {}
    gd.active_fn = None
    sent = []
    gd._enqueue = lambda *a, **k: sent.append(a[0])

    for dev in _Disc.devices:                      # start: retained z2m dla wszystkich naraz
        gd.on_z2m_availability(f"zigbee2mqtt/{dev}/availability", "offline")
    assert sent == [], f"seed startowy poszedł w LoRa: {len(sent)} ramek (ma iść blobem)"
    assert len(gd._alive) == 99, "seed musi zapamiętać stan lokalnie"
    assert gd._z2m_avail["RA1_1"] is False, "blob musi dostać poprawny stan z seedu"

    gd.on_z2m_availability("zigbee2mqtt/RA1_1/availability", "online")   # PRAWDZIWA zmiana
    assert sent == ["RA1_1"], f"realna zmiana po seedzie musi nadać, dostałem {sent}"
    print("✅ 1. seed startowy nie zalewa LoRa, realna zmiana dalej nadaje")


def _transport():
    from modules.transport import LoraTransport
    t = LoraTransport.__new__(LoraTransport)
    t.log, t.lock, t.running = None, threading.Lock(), True
    t.interfaces, t._reconnect_backoff = {}, {}
    t._tx_blocked, t._connect_inflight = {}, {}
    t.tx_timeout, t.tx_hard_timeout, t.connect_timeout = 0.3, 0.6, 0.5
    return t


def test_wiszacy_sendtext_nie_mnozy_watkow():
    """2. Antena z wiszącym sendText() odrzuca kolejne ramki zamiast hodować wątki."""
    class _Wisi:
        def sendText(self, text): time.sleep(30)      # nigdy nie wraca w czasie testu
        def close(self): time.sleep(30)               # close() też wisi (jak na żywej bramce)

    t, iface = _transport(), _Wisi()
    base = threading.active_count()
    assert t._tx_with_timeout('ANT-1', iface, 'a') is False   # 1. TX: wisi → force-close
    assert t._tx_blocked.get('ANT-1') is True, "wiszący sendText musi ustawić flagę blokady"
    for i in range(200):                                      # 200 kolejnych ramek
        assert t._tx_with_timeout('ANT-1', iface, f'x{i}') is False
    grew = threading.active_count() - base
    assert grew <= 3, f"wyciek wątków wrócił: +{grew} przy 200 ramkach (było 3072 na G1)"
    print(f"✅ 2. 200 ramek na wiszącej antenie → +{grew} wątków (limit 3)")


def test_zwis_portu_nie_zamraza_petli():
    """3. Otwarcie portu, które nie wraca, nie może zablokować pętli reconnectu."""
    class _Wisi:
        def __init__(self, **kw): time.sleep(30)      # SerialInterface() nie wraca

    t = _transport()
    t._meshtastic = type('M', (), {'SerialInterface': _Wisi})()
    acfg = {'label': 'ANT-1', 'port': '/dev/ttyUSB0', 'enabled': True}

    start = time.time()
    assert t._connect_one(acfg) is False
    took = time.time() - start
    assert took < 3, f"_connect_one zamroziło wołającego na {took:.1f}s (pętla reconnectu ginie)"
    assert t._connect_inflight.get('ANT-1') is True, "próba w locie musi być oznaczona"

    start = time.time()                               # druga próba: bez czekania, bez nowego wątku
    assert t._connect_one(acfg) is False
    assert time.time() - start < 0.2, "druga próba czekała ponownie zamiast odpaść od razu"
    print(f"✅ 3. zwis portu: _connect_one wrócił w {took:.1f}s, pętla żyje")


def test_wykluczenie_z_anomalii():
    """4. `anomaly.no_anomaly` — sprzęt nieużywany nie zgłasza offline ani stagnacji,
    i NIE zostaje w zbiorach silników (inaczej brudzi hash `oh`, snapshot i dump_anom)."""
    import fnmatch
    from modules.anomaly import GatewayOfflineAnomaly, StagnationEngine

    devs = {'RA1_1': {'type': 'switch'}, 'RA1_S1': {'type': 'binary_sensor'},
            'RB4_S2': {'type': 'binary_sensor'}, 'RA11_R1': {'type': 'switch'}}

    class _Disc:
        devices = devs
        short_ids = {d: i for i, d in enumerate(devs)}
        def is_monitored(self, dev): return True

    class _Data:
        _alive = {d: False for d in devs}
        last_msg_ts = {d: 0.0 for d in devs}       # prehistoria → stagnacja dla wszystkich

    pats = ['RA*_S*', 'RB*_S*']
    skip = lambda d: any(fnmatch.fnmatchcase(d, p) for p in pats)
    assert skip('RA1_S1') and skip('RB4_S2'), "wzorzec musi łapać czujniki ruchu"
    assert not skip('RA1_1') and not skip('RA11_R1'), "wzorzec NIE może łapać przekaźników"

    got = []
    off = GatewayOfflineAnomaly('G1', _Disc(), _Data(), emit=lambda *a: got.append(a),
                                get_timeout=lambda d: 120, skip_dev=skip)
    off.check()
    assert off.offline == {'RA1_1', 'RA11_R1'}, f"czujniki weszły do zbioru offline: {off.offline}"

    st = StagnationEngine('G1', _Disc(), _Data(), get_thresholds=lambda: (1, 1),
                          emit=lambda *a: got.append(a), all_devices=True, skip_dev=skip)
    st.check()
    assert st.stagnant == {'RA1_1', 'RA11_R1'}, f"czujniki weszły w stagnację: {st.stagnant}"
    print("✅ 4. czujniki ruchu poza offline/stagnacją, przekaźniki nietknięte")


def test_dsnap_zbiega_hash():
    """5. `dsnap` jest ZRZUTEM, nie deltą — czyści wartości, których bramka już nie ma,
    więc `dh` schodzi się także wtedy, gdy bramka nie ma ŻADNYCH wartości (carport za dnia)."""
    from modules.data.supervisor_data import SupervisorData
    from modules.data.z2m_reader import state_hash

    class _Disc:
        def devices(self, gw):
            return {'RA1_1': {'sid': 1, 'type': 'switch'}, 'RA1_2': {'sid': 2, 'type': 'switch'}}

    sd = SupervisorData.__new__(SupervisorData)
    sd.disc, sd.log = _Disc(), None
    sd.state = {'G1': {'RA1_1': {'state': 'ON', 'available': True},
                       'RA1_2': {'state': 'OFF', 'available': True}}}

    assert sd.state_hash('G1') != '0', "warunek startowy: lustro ma stare wartości"

    n = sd.clear_values('G1')                       # przychodzi PUSTY zrzut z bramki
    assert n == 2, f"miały się wyczyścić 2 urządzenia, wyczyszczono {n}"
    assert sd.state_hash('G1') == '0', "po pustym zrzucie hash supa musi zejść do '0' (jak bramki)"
    assert sd.state['G1']['RA1_1'].get('available') is True, "dostępność NIE może zniknąć"
    print("✅ 5. pusty dsnap zbiega hash supervisora do '0' (koniec pętli dsnap_req)")


def test_hb_jitter_lamie_zbieznosc_faz():
    """6. HB dwoch bramek nie moze zostac zsynchronizowany: staly interwal + HB poza slotem
    = kolizja co cykl (live 17:16:08 i 17:31:08, oba razy HB G1 przepadl)."""
    from modules.protocol.heartbeat import GatewayHeartbeat as HB

    def mk(gw):
        h = HB.__new__(HB)
        h.gw_id, h.interval = gw, 900
        h.jitter = 0.05 * h.interval
        return h

    g1, g2 = mk('G1'), mk('G2')
    a = [g1._next_gap() for _ in range(50)]
    b = [g2._next_gap() for _ in range(50)]
    assert all(855 <= x <= 945 for x in a), f'jitter poza zakresem +/-5%: {min(a)}..{max(a)}'
    assert len(set(a)) > 40, 'odstep musi sie zmieniac co cykl, inaczej fazy znow sie zejda'

    # symulacja: obie bramki startuja W TEJ SAMEJ sekundzie (najgorszy przypadek)
    t1 = t2 = 0.0
    zderzenia = 0
    for _ in range(200):                      # ~50 h pracy
        t1 += g1._next_gap()
        t2 += g2._next_gap()
        if abs(t1 - t2) < 2.0:                # < airtime ramki => kolizja
            zderzenia += 1
    assert zderzenia <= 4, f'fazy dalej sie schodza: {zderzenia} zderzen na 200 cykli'

    # kontrola negatywna: bez jittera zderzaja sie ZAWSZE
    g1.jitter = g2.jitter = 0.0
    t1 = t2 = 0.0
    bez = sum(1 for _ in range(20)
              if (t1 := t1 + g1._next_gap()) and (t2 := t2 + g2._next_gap()) and abs(t1 - t2) < 2.0)
    assert bez == 20, 'kontrola negatywna: bez jittera MUSI byc kolizja w kazdym cyklu'
    print(f'✅ 6. HB jitter: {zderzenia}/200 zbiegnieć (bez jittera: {bez}/20)')


def test_status_hash_niesie_dostepnosc():
    """7. Bramka nocna W DZIEŃ: zero wartości capów, ale 78 urządzeń NIEDOSTĘPNYCH.
    `sh` musi to opisywać (≠ hash stanu „wszystko OK") — inaczej kafel pokazuje `0`,
    jakby bramka nic nie wiedziała, a ona wie doskonale."""
    from modules.data.gateway_data import GatewayData

    class _Disc:
        devices = {f"RA{i}_2": {'type': 'switch'} for i in range(1, 6)}
        short_ids = {d: i for i, d in enumerate(devices)}
        def is_monitored(self, dev): return True

    gd = GatewayData.__new__(GatewayData)
    gd.disc, gd.log = _Disc(), None
    gd.states, gd.sent_states = {}, {}
    gd._alive, gd._z2m_avail = {}, {}

    gd._z2m_avail = {d: True for d in _Disc.devices}
    h_online = gd.status_hash()
    gd._z2m_avail = {d: False for d in _Disc.devices}
    h_offline = gd.status_hash()

    assert h_online and h_online != '0', 'hash statusu nie może być pusty przy znanych urządzeniach'
    assert h_offline and h_offline != '0',         'BEZ WARTOŚCI, ale z niedostępnością — hash MUSI być niezerowy (to był bug kafla „0")'
    assert h_online != h_offline, 'masowa zmiana dostępności MUSI zmienić hash statusu'
    print('  ✓ 7. status_hash niesie dostępność (online≠offline, żaden nie jest 0)')


def test_kaskada_tylko_w_dol():
    """8. Powrót bramki NIE MOŻE hurtem ustawiać urządzeń jako dostępne — to była przyczyna
    tego, że supervisor w dzień pokazywał 78/78 przekaźników G1 online, choć z2m mówił offline."""
    from modules.protocol.heartbeat import SupervisorHeartbeat

    pub = []

    class _HA:
        def pub_device_avail(self, gw, dev, online): pub.append((gw, dev, online))
        def reg_gateway(self, gw): pass

    sh = SupervisorHeartbeat.__new__(SupervisorHeartbeat)
    sh.ha, sh.log = _HA(), None
    sh.devices_provider = lambda gw: {'RA1_2': {}, 'RA2_2': {}}

    sh._cascade_devices('G1', online=False)
    assert pub and all(p[2] is False for p in pub), 'kaskada w DÓŁ ma działać (bramka milczy)'
    n_down = len(pub)

    import modules.protocol.heartbeat as _hb_mod
    src = open(_hb_mod.__file__, encoding='utf-8').read()
    assert 'self._cascade_devices(gw, online=True)' not in src,         'kaskada W GÓRĘ wróciła — supervisor znów będzie zmyślał dostępność urządzeń'
    print(f'  ✓ 8. kaskada tylko w dół ({n_down} urządzeń zgaszonych, w górę brak)')

def test_heartbeat_miesci_sie_w_ramce():
    """9. HB MUSI zmieścić się w 220 B — Meshtastic ucina PO CICHU.

    Regresja 2026-08-26: dołożenie `sh` (hash statusu) wypchnęło heartbeat G1 na 228 B i od
    19:48 supervisor NIE DOSTAŁ ŻADNEGO HB — mimo że bramka żyła i dalej słała `b`, bloby
    i sup_ping. Objaw był podstępny: dashboard pokazywał bramkę jako online (bo `note_gw_activity`
    reaguje na DOWOLNĄ ramkę), tylko hashe i liczniki zamarzły. Ten test liczy ramkę z REALNYMI
    wartościami z produkcji, żeby kolejne pole nie przepchnęło jej przez sufit po cichu."""
    import json

    # NAJGORSZY REALNY przypadek: długi uptime, wszystkie hashe niepuste, `sh` ≠ `dh`.
    hb = {"t": "hb", "g": "G1", "up": 152457, "dev": 194, "mon": 78, "pri": 0,
          "ts": 1787766486, "hash": "f795e257116c", "ph": "fbade8dc", "cal": "8d0d515f7fa0",
          "m": 0, "tq": "synced", "ah": "c3ba9619", "dh": "e13720b4", "sh": "e92896",
          "gm": "night", "ga": 1}
    n = len(json.dumps(hb, separators=(',', ':')))
    assert n <= 220, (f'HB ma {n} B > 220 B — Meshtastic utnie go PO CICHU i heartbeaty '
                      f'przestaną dochodzić (dokładnie to stało się G1 2026-08-26: ostatni HB '
                      f'19:48, potem cisza mimo żywej bramki). Zanim dołożysz pole do HB, '
                      f'USUŃ jakieś — tak zrobiliśmy z `oh`, które nikt nie czytał.')
    bez_sh = {k: v for k, v in hb.items() if k != 'sh'}
    m_ = len(json.dumps(bez_sh, separators=(',', ':')))
    assert m_ <= 220
    print(f'  ✓ 9. heartbeat {m_} B (hashe zgodne) / {n} B (sh≠dh) — limit 220, zapas {220 - n} B')


if __name__ == '__main__':
    test_startup_seed_nie_zalewa_lora()
    test_wiszacy_sendtext_nie_mnozy_watkow()
    test_zwis_portu_nie_zamraza_petli()
    test_wykluczenie_z_anomalii()
    test_dsnap_zbiega_hash()
    test_hb_jitter_lamie_zbieznosc_faz()
    test_status_hash_niesie_dostepnosc()
    test_kaskada_tylko_w_dol()
    test_heartbeat_miesci_sie_w_ramce()
    print("\n🎯 wedge_smoke_test: 9/9 — regresje G1 (2026-08-24 + 2026-08-26) zabezpieczone")
