"""FAZA 1 — symulacja anti-collision (krok 7) BEZ SPRZĘTU.

Dowodzi (lub obala) rdzeń Fazy 1: czy slotowane batche `b` wielu bramek
NIGDY nie nakładają się czasowo na współdzielonym paśmie 868 MHz.

Metoda: używamy PRAWDZIWEGO `SlotScheduler` z modułu (jego jedyna decyzja
czasowa to `seconds_until_slot`), ale zamiast realnego `time.sleep` sterujemy
czasem wirtualnie. Każdy frame `b` zajmuje antenę przez `airtime`, po nim
`tx_cooldown` (honor LoRa). Generujemy ślad TX każdej bramki przez N cykli
i sprawdzamy parami, czy interwały TX różnych bramek się przecinają.

To NIE zastępuje testu RF na sprzęcie (sekcja D FAZA1) — modeluje WYŁĄCZNIE
warstwę czasową slotowania (sekcja C / kryterium F#1). Zasięg, packet-loss,
RSSI mierzymy na żywo.

Usage:
    cd skrypty
    python3 -m modules.slotting.collision_sim            # pełny raport + asercje
    python3 -m modules.slotting.collision_sim --frames 3 # batch wielochunkowy
"""
import sys

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

from modules.slotting import SlotScheduler


# ── Model parametrów łącza (zgodne z config.py) ───────────────────────────────
# airtime: czas nadawania jednego frame `b` w powietrzu. ~120 B przy presecie
# Meshtastic LongFast/868 to rząd ~0.5-1.5 s. Bierzemy 1.0 s jako rozsądny
# default i osobno testujemy wariant pesymistyczny.
DEFAULT_AIRTIME = 1.0
DEFAULT_COOLDOWN = 3.0          # lora.tx_cooldown
DEFAULT_SLOT = 20.0            # slotting.slot_seconds
DEFAULT_MON_INTERVAL = 30.0   # data.mon_interval (jak często bramka chce batch `b`)


class Interval:
    __slots__ = ("gw", "start", "end", "seq")

    def __init__(self, gw, start, end, seq):
        self.gw, self.start, self.end, self.seq = gw, start, end, seq

    def overlaps(self, other):
        # rozłączne jeśli jeden kończy się <= zanim drugi się zaczyna
        return self.start < other.end and other.start < self.end

    def __repr__(self):
        return f"{self.gw}#{self.seq} [{self.start:7.2f},{self.end:7.2f}]"


def gateway_tx_trace(gw_id, gateways, *, slot_seconds, airtime, cooldown,
                     mon_interval, frames_per_batch, duration, jitter=0.0,
                     guard_band=0.0):
    """Ślad TX jednej bramki: lista Interval na osi czasu [0, duration).

    Model: co `mon_interval` powstaje batch (frames_per_batch ramek). Każdy frame
    czeka na okno slotu (PRAWDZIWY SlotScheduler.seconds_until_slot), zajmuje
    `airtime`, po nim `cooldown` zanim następny frame jest gotowy. Bramka
    serializuje własny TX (half-duplex).

    guard_band: jeśli >0, frame NIE startuje gdy do końca okna zostało < airtime
    (przesuwa się na następny cykl). To proponowana mitygacja spill-over.
    """
    slot = SlotScheduler(gw_id, gateways=gateways, slot_seconds=slot_seconds)
    s_start, s_end = slot.window()
    cycle = slot.cycle

    intervals = []
    seq = 0
    # deterministyczny "jitter" zależny od bramki (bez Math.random — odtwarzalne)
    jphase = (hash(gw_id) % 1000) / 1000.0 * jitter

    batch_t = 0.0
    prev_end = -1e9
    while batch_t < duration:
        ready = max(batch_t + jphase, prev_end + cooldown)
        for _f in range(frames_per_batch):
            tx_start = _next_window_start(slot, ready, airtime, guard_band,
                                          s_start, s_end, cycle)
            tx_end = tx_start + airtime
            if tx_start >= duration:
                break
            intervals.append(Interval(gw_id, tx_start, tx_end, seq))
            seq += 1
            prev_end = tx_end
            ready = tx_end + cooldown      # następny chunk tego batcha po cooldown
        batch_t += mon_interval
    return intervals, slot


def _next_window_start(slot, ready, airtime, guard_band, s_start, s_end, cycle):
    """Czas, w którym frame faktycznie wejdzie na antenę.

    Bazuje na PRAWDZIWYM slot.seconds_until_slot(ready). Jeśli guard_band aktywny
    i frame nie zmieści się do końca okna, przesuwamy na start następnego cyklu.
    """
    wait = slot.seconds_until_slot(now=ready)
    start = ready + wait
    if guard_band > 0.0:
        # pozycja startu w cyklu
        pos = start % cycle
        # ile zostało do końca naszego okna
        remaining = s_end - pos
        if 0 <= remaining < (airtime + guard_band):
            # nie zmieścimy się — skok na początek naszego okna w następnym cyklu
            start += (cycle - pos) + s_start
    return start


def find_collisions(traces):
    """Wszystkie pary nakładających się interwałów z RÓŻNYCH bramek.

    O(n log n): sortuj po start, porównuj tylko z aktywnymi (sweep line).
    """
    all_iv = sorted((iv for t in traces for iv in t), key=lambda x: x.start)
    collisions = []
    active = []
    for iv in all_iv:
        active = [a for a in active if a.end > iv.start]   # usuń zakończone
        for a in active:
            if a.gw != iv.gw and a.overlaps(iv):
                collisions.append((a, iv))
        active.append(iv)
    return collisions


def run_scenario(name, gateways, *, slot_seconds=DEFAULT_SLOT,
                 airtime=DEFAULT_AIRTIME, cooldown=DEFAULT_COOLDOWN,
                 mon_interval=DEFAULT_MON_INTERVAL, frames_per_batch=1,
                 cycles=12, jitter=0.0, guard_band=0.0, verbose=True):
    cycle = slot_seconds * len(gateways)
    duration = cycle * cycles
    traces = []
    sched = {}
    for gw in gateways:
        iv, slot = gateway_tx_trace(
            gw, gateways, slot_seconds=slot_seconds, airtime=airtime,
            cooldown=cooldown, mon_interval=mon_interval,
            frames_per_batch=frames_per_batch, duration=duration,
            jitter=jitter, guard_band=guard_band)
        traces.append(iv)
        sched[gw] = slot
    collisions = find_collisions(traces)
    total_tx = sum(len(t) for t in traces)
    if verbose:
        print(f"\n── {name} ──")
        print(f"   bramki={gateways} slot={slot_seconds:.0f}s cykl={cycle:.0f}s "
              f"× {cycles} = {duration:.0f}s")
        print(f"   airtime={airtime}s cooldown={cooldown}s mon_int={mon_interval}s "
              f"frames/batch={frames_per_batch} jitter={jitter}s guard={guard_band}s")
        for gw in gateways:
            s0, s1 = sched[gw].window()
            print(f"   {gw}: okno {s0:.0f}-{s1:.0f}s  TX={len(next(t for t in traces if t and t[0].gw==gw))} ramek")
        if collisions:
            print(f"   ❌ KOLIZJE: {len(collisions)} par na {total_tx} ramek")
            for a, b in collisions[:5]:
                print(f"      {a}  ✕  {b}")
            if len(collisions) > 5:
                print(f"      … +{len(collisions)-5} więcej")
        else:
            print(f"   ✅ ZERO kolizji na {total_tx} ramek TX ({cycles} cykli)")
    return collisions, total_tx


# ── Asercje (test) ────────────────────────────────────────────────────────────
def test_single_frame_disjoint():
    """Batche 1-ramkowe G1/G2/G3, realny airtime → muszą być rozłączne."""
    col, n = run_scenario("Pojedyncza ramka /batch (G1+G2+G3)",
                          ["G1", "G2", "G3"], frames_per_batch=1, verbose=False)
    assert n > 0, "symulacja nie wygenerowała TX"
    assert not col, f"kolizje przy 1 ramce/batch: {col[:3]}"
    print("✅ 1 ramka/batch: zero kolizji (rdzeń F#1)")


def test_two_gateways_disjoint():
    col, n = run_scenario("Dwie bramki (realny scenariusz Gliwice)",
                          ["G1", "G2"], frames_per_batch=1, verbose=False)
    assert not col, f"kolizje G1/G2: {col[:3]}"
    print("✅ G1+G2: zero kolizji")


def test_realistic_airtime_safe():
    """Realny airtime ~1s (≈120 B) + flush dryfujący względem cyklu (mon_interval
    względnie pierwszy z cyklem) → slotowanie MUSI być rozłączne. To jest faktyczny
    warunek pracy Gliwic."""
    col, n = run_scenario("Realny: airtime 1s, flush dryfuje (mon=23)",
                          ["G1", "G2", "G3"], airtime=1.0, mon_interval=23,
                          cycles=40, verbose=False)
    assert not col, f"kolizje przy realnym airtime 1s: {col[:3]}"
    print(f"✅ realny airtime 1s + dryf flushu: zero kolizji na {n} ramek (40 cykli)")


def test_boundary_spill_needs_guard():
    """WORST-CASE: duży airtime (2.5s) + flush dryfujący przez okno → frame może
    wystartować tuż przed granicą okna i wejść w slot kolejnej bramki (spill-over).
    Bez guard-band kolizje WYSTĘPUJĄ; guard-band = airtime musi je wyzerować.
    Dowodzi, że mitygacja działa i jest potrzebna dla wolnych/dużych ramek."""
    pess = dict(airtime=2.5, mon_interval=23, cycles=40, verbose=False)
    col_no_guard, _ = run_scenario("WC bez guard", ["G1", "G2", "G3"],
                                   guard_band=0.0, **pess)
    col_guard, _ = run_scenario("WC z guard", ["G1", "G2", "G3"],
                                guard_band=2.5, **pess)
    assert col_no_guard, "scenariusz worst-case nie wygenerował spill-over (test pozorny)"
    assert not col_guard, f"guard-band nie wyeliminował spill-over: {col_guard[:3]}"
    print(f"✅ worst-case: bez guard {len(col_no_guard)} spill-over → z guard-band=airtime 0 "
          f"(rekomendacja: guard-band gdy airtime≥2s)")


def _run_tests():
    tests = [test_single_frame_disjoint, test_two_gateways_disjoint,
             test_realistic_airtime_safe, test_boundary_spill_needs_guard]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            print(f"❌ {t.__name__}: {e}"); failed += 1
    print(f"\n{'─'*48}\n{len(tests)-failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="FAZA 1 — symulacja anti-collision")
    ap.add_argument("--frames", type=int, default=1, help="ramek na batch (chunk split)")
    ap.add_argument("--airtime", type=float, default=DEFAULT_AIRTIME)
    ap.add_argument("--cycles", type=int, default=12)
    ap.add_argument("--guard", type=float, default=0.0, help="guard-band [s]")
    ap.add_argument("--gateways", default="G1,G2,G3")
    ap.add_argument("--test", action="store_true", help="uruchom asercje (CI)")
    args = ap.parse_args()

    if args.test:
        sys.exit(1 if _run_tests() else 0)

    print("═" * 48)
    print("FAZA 1 · symulacja anti-collision (warstwa czasowa)")
    print("═" * 48)
    gws = [g.strip() for g in args.gateways.split(",") if g.strip()]
    # Scenariusz główny wg parametrów CLI
    run_scenario("Scenariusz CLI", gws, frames_per_batch=args.frames,
                 airtime=args.airtime, cycles=args.cycles, guard_band=args.guard)
    # Referencja: realny warunek pracy (airtime ~1s) z flushem dryfującym
    run_scenario("Realny: airtime 1s, flush dryfuje (mon=23, 40 cykli)",
                 ["G1", "G2", "G3"], airtime=1.0, mon_interval=23, cycles=40)
    # Worst-case: duży airtime → spill-over bez guard-band, znika z guard-band
    run_scenario("Worst-case: airtime 2.5s, dryf, BEZ guard-band",
                 ["G1", "G2", "G3"], airtime=2.5, mon_interval=23, cycles=40)
    run_scenario("Worst-case: airtime 2.5s, dryf, guard-band 2.5s",
                 ["G1", "G2", "G3"], airtime=2.5, mon_interval=23, cycles=40,
                 guard_band=2.5)
    print("\nWNIOSEK: przy airtime ~1s (≈120 B) slotowanie jest rozłączne bez guard-band.")
    print("Dla wolnych/dużych ramek (airtime≥2s) potrzebny guard-band = airtime.")
    print("Weryfikacja czasowa ≠ test RF. Zasięg/packet-loss/RSSI → sekcja D na sprzęcie.")
