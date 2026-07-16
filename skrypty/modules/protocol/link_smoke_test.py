"""Smoke test for SupervisorLinkProbe (F2) + discovery anti-spam (F7). No hardware/broker.

Usage:
    cd skrypty
    python3 -m modules.protocol.link_smoke_test
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass


class FakeLora:
    def __init__(self): self.sent = []
    def send(self, text): self.sent.append(text)


class FakeClock:
    """Zegar sterowany ręcznie — testy przesuwają czas bez realnego sleep."""
    def __init__(self, t=1_000_000.0): self.t = t
    def __call__(self): return self.t
    def advance(self, dt): self.t += dt


def test_ping_pong_online():
    from modules.protocol import SupervisorLinkProbe
    lora = FakeLora()
    clk = FakeClock()
    states = []
    probe = SupervisorLinkProbe(
        "G1", lora, get_params=lambda: (15, 25, 2),
        on_state=lambda online, lost: states.append((online, lost)), clock=clk)
    assert probe.online is True, "start optymistyczny (cichy start)"
    # wymuś natychmiastowy ping (next_ping_ts w przeszłości)
    probe._next_ping_ts = clk.t - 1
    probe.check()
    assert lora.sent and __import__('json').loads(lora.sent[-1]) == {"t": "sup_ping", "g": "G1"}
    probe.handle_sup_pong({"g": "G1"})
    assert probe.online is True
    print("✅ ping emitted + pong → online")


def test_timeout_retries_offline_then_note_rx():
    from modules.protocol import SupervisorLinkProbe
    lora = FakeLora()
    clk = FakeClock()
    states = []
    probe = SupervisorLinkProbe(
        "G1", lora, get_params=lambda: (15, 25, 2),   # interval=15min, timeout=25s, retries=2
        on_state=lambda online, lost: states.append((online, lost)), clock=clk)
    probe._next_ping_ts = clk.t - 1
    probe.check()                                     # próba 1/3 (initial)
    assert len(lora.sent) == 1
    clk.advance(26); probe.check()                     # timeout → retry 2/3
    assert len(lora.sent) == 2 and probe.online is True
    clk.advance(26); probe.check()                     # timeout → retry 3/3
    assert len(lora.sent) == 3 and probe.online is True
    clk.advance(26); probe.check()                     # timeout, retries wyczerpane → OFFLINE
    assert probe.online is False and probe.lost_pongs == 1
    assert states and states[-1] == (False, 1)
    print("✅ timeout×(retries+1) → offline + lost_pongs=1")

    probe.note_rx()                                    # dowolny ruch od supervisora → online
    assert probe.online is True
    assert states[-1] == (True, 1), "lost_pongs licznik epizodów — NIE zerowany przez note_rx"
    print("✅ note_rx() przywraca online (dowolny ruch od supervisora)")


def test_discovery_anti_spam_and_force():
    from modules.protocol import GatewayDiscovery

    class _Lora:
        def __init__(self): self.sent = []
        def send(self, text): self.sent.append(text)

    lora = _Lora()
    disc = GatewayDiscovery(gw_id="G1", monitored_names=[], priority_names=[],
                            vio_config=[], lora=lora)
    disc.parse_z2m('[]')          # buduje disc_hash stabilny (pusta lista urządzeń)
    n0 = len(lora.sent)
    disc.send_discovery_with_delay(delay=0)             # pierwsze wysłanie — hash inny niż None
    n1 = len(lora.sent)
    assert n1 > n0, "pierwsze wysłanie musi coś wysłać"
    disc.send_discovery_with_delay(delay=0)             # ten sam hash <180s → anti-spam, NIC
    n2 = len(lora.sent)
    assert n2 == n1, "drugie wysłanie w oknie 180s z tym samym hashem NIE powinno nic wysłać"
    disc.send_discovery_with_delay(delay=0, force=True)  # force=True omija guard
    n3 = len(lora.sent)
    assert n3 > n2, "force=True musi ominąć anti-spam"
    print("✅ discovery anti-spam 180s (disc_meta+disc_vio) + force=True bypass")


if __name__ == "__main__":
    tests = [test_ping_pong_online, test_timeout_retries_offline_then_note_rx,
             test_discovery_anti_spam_and_force]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*40}\n{len(tests)-failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
