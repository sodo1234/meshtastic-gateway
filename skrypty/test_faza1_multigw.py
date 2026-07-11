"""FAZA 1 — walidacja multi-gateway BEZ SPRZĘTU (jeden punkt wejścia).

Dowodzi programowo część Fazy 1, której NIE trzeba sprzętu G2:
  • C / F#1  — anti-collision: slotowane batche G1/G2/G3 rozłączne czasowo
               (warstwa czasowa; reszta = symulacja collision_sim).
  • C#3      — atrybucja danych: ten sam sid w G1 i G2 NIE miesza się
               (SupervisorData kluczuje per `g:` z ramki).

Czego TU NIE ma (wymaga sprzętu — patrz FAZA1_multigateway_validation.md):
  • D  zasięg RF / packet-loss / RSSI    • E  fault injection live
  • PUSH/PULL kalendarza po realnym LoRa

Usage:
    cd skrypty
    python3 test_faza1_multigw.py
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


# ── C / F#1: anti-collision (deleguj do symulacji) ────────────────────────────
def section_anticollision():
    from modules.slotting import collision_sim as cs
    print("\n### C / F#1 — anti-collision (slotowanie) ###")
    cs.test_single_frame_disjoint()
    cs.test_two_gateways_disjoint()
    cs.test_realistic_airtime_safe()
    cs.test_boundary_spill_needs_guard()


# ── C#3: atrybucja sid per-bramka (brak mieszania) ────────────────────────────
class _FakeDisc:
    """SupervisorDiscovery-stub: ten SAM sid=1 w G1 i G2 → różne urządzenia."""
    _MAP = {
        "G1": {"Temp 1": {"sid": 1, "type": "sensor"}},
        "G2": {"Temp 2": {"sid": 1, "type": "sensor"}},   # ten sam sid co G1!
    }
    def devices(self, gw):
        return self._MAP.get(gw, {})


class _FakeHA:
    """HAEntities-stub: zapamiętuje ostatni stan opublikowany per (gw, dev)."""
    def __init__(self):
        self.published = {}            # {(gw,dev): merged_dict}
    def pub_device_state(self, gw, dev, merged):
        self.published[(gw, dev)] = dict(merged)


def test_sid_attribution_no_mixing():
    """Przeplot ramek `b` z g:G1 i g:G2 o IDENTYCZNYM sid=1 → dane lądują w
    osobnych urządzeniach właściwych bramek. To kryterium C#3."""
    from modules.data.supervisor_data import SupervisorData
    ha = _FakeHA()
    sd = SupervisorData(ha, _FakeDisc())

    # przeplot: G1 sid1 temp=20.0, G2 sid1 temp=99.0, potem G1 znów (delta hum)
    sd.handle_b({"t": "b", "g": "G1", "d": [[1, {"a": 1, "t": 20.0}]]})
    sd.handle_b({"t": "b", "g": "G2", "d": [[1, {"a": 1, "t": 99.0}]]})
    sd.handle_b({"t": "b", "g": "G1", "d": [[1, {"a": 1, "h": 45}]]})

    g1 = ha.published.get(("G1", "Temp 1"))
    g2 = ha.published.get(("G2", "Temp 2"))
    assert g1 is not None, "G1/Temp 1 nie zostało opublikowane"
    assert g2 is not None, "G2/Temp 2 nie zostało opublikowane"
    # G1 ma swoją temp 20 + zmergowaną hum 45; G2 ma temp 99 — żadnego przecieku
    assert g1.get("temperature") == 20.0, f"G1 temp skażona: {g1}"
    assert g1.get("humidity") == 45, f"G1 hum nie zmergowana: {g1}"
    assert g2.get("temperature") == 99.0, f"G2 temp skażona przez G1: {g2}"
    assert "humidity" not in g2, f"G2 dostało hum z G1 (mieszanie sid!): {g2}"
    # state wewnętrzny rozłączny per bramka
    assert set(sd.snapshot("G1").keys()) == {"Temp 1"}
    assert set(sd.snapshot("G2").keys()) == {"Temp 2"}
    print("✅ sid=1 w G1 i G2 rozłączne — zero mieszania (C#3)")


def test_unknown_sid_is_isolated():
    """Ramka g:G2 z sid spoza mapy G2 nie tworzy fałszywego wpisu ani nie sięga G1."""
    from modules.data.supervisor_data import SupervisorData
    ha = _FakeHA()
    sd = SupervisorData(ha, _FakeDisc())
    sd.handle_b({"t": "b", "g": "G2", "d": [[7, {"a": 1, "t": 50.0}]]})   # sid 7 nieznany w G2
    assert ("G1", "Temp 1") not in ha.published, "nieznany sid G2 dotknął G1"
    assert not ha.published, f"nieznany sid utworzył wpis: {ha.published}"
    print("✅ nieznany sid izolowany (nie wycieka do innej bramki)")


def section_attribution():
    print("\n### C#3 — atrybucja sid per-bramka ###")
    test_sid_attribution_no_mixing()
    test_unknown_sid_is_isolated()


if __name__ == "__main__":
    print("═" * 52)
    print("FAZA 1 · walidacja multi-gateway (część bez-sprzętowa)")
    print("═" * 52)
    failed = 0
    for section in (section_anticollision, section_attribution):
        try:
            section()
        except AssertionError as e:
            print(f"❌ {section.__name__}: {e}"); failed += 1
        except Exception as e:
            print(f"💥 {section.__name__}: {type(e).__name__}: {e}"); failed += 1
    print(f"\n{'═'*52}")
    if failed:
        print(f"❌ {failed} sekcja(e) FAIL — część bez-sprzętowa Fazy 1 NIE zaliczona")
    else:
        print("✅ CZĘŚĆ BEZ-SPRZĘTOWA FAZY 1 ZALICZONA")
        print("   Pozostaje na sprzęcie: D (RF/zasięg), E (fault), PUSH/PULL kalendarza.")
        print("   → runbook: FAZA1_multigateway_validation.md")
    sys.exit(1 if failed else 0)
