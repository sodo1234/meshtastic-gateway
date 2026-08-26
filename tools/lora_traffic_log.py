#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dziennik ruchu LoRa: zbiera RX/TX do TSV i zestawia dwie strony łącza.

PO CO: pojedynczy log mówi tylko, co dana maszyna *myśli*, że się stało. Dopiero ZESTAWIENIE
dwóch stron pokazuje prawdę — bramka twierdzi „nadałem HB up=907", supervisor go nie ma ⇒ ramka
zginęła na RF. Bez tego „gubimy pakiety" jest wrażeniem, nie pomiarem.

DLACZEGO OSOBNY PLIK, A NIE SAM LOG HARNESSU: harness pisze do /tmp (ginie po reboocie), miesza
ruch z diagnostyką i rotuje się. Tu zostaje zwarty, trwały TSV w ~/meshtastic/logs/, który
przeżywa restart i da się porównać nawet po tygodniu.

DLACZEGO NIE RUSZAMY HARNESSU: on już loguje KAŻDĄ ramkę (`📥 RX:` / `📤 TX:`). Doginanie kodu
produkcyjnego bramki po to, żeby zapisać to samo drugi raz, byłoby ryzykiem bez zysku.

TRYBY
  --collect            śledź log harnessu i dopisuj do TSV (praca ciągła; działa BEZ internetu)
  --compare A B        zestaw dwa TSV (np. z supa i z G1) → straty, niespójności, rozkład typów
  --stats PLIK         szybki przekrój jednego pliku (co ile, jakie typy, jaka godzina)

Format TSV (jedna ramka = jeden wiersz, grep/awk-friendly):
  data_iso  DIR  gw  typ  bajty  klucz  PEŁNA_RAMKA_JSON
  klucz = to, po czym da się dopasować ramkę na drugiej stronie:
          hb → up=<uptime>,  rt_*/cal_* → tid=<tid>[/s=<nr chunku>],  b → ts=<ts>,  reszta → -

  Ostatnia kolumna to CAŁA oryginalna ramka. Bez niej analiza jest ograniczona do tego, co
  ktoś z góry przewidział w kolumnach — a sens zbierania jest właśnie taki, żeby dało się
  PÓŹNIEJ zapytać o coś, o czym dziś nie pomyśleliśmy (progi, wartości, pola, rozmiary).
  JSON nie zawiera tabulatorów ani nowych linii, więc nie psuje formatu.

Użycie NA MASZYNIE (supervisor i bramka — ten sam plik, rola wykrywana z ~/meshtastic/.role):
    nohup venv/bin/python tools/lora_traffic_log.py --collect >/dev/null 2>&1 &
    venv/bin/python tools/lora_traffic_log.py --compare logs/traffic_sup_*.tsv /tmp/g1.tsv
"""
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict

HOME = os.path.expanduser('~')
BASE = os.path.join(HOME, 'meshtastic')
ZRODLO = '/tmp/test_transport.log'          # żywy log harnessu (prefix `test_` → ta ścieżka)
KATALOG = os.path.join(BASE, 'logs')

# Wymagamy dosłownego „RX: " / „TX: " tuż przed JSON-em. Luźniejszy wzorzec łapał też
# `⏭️ RF-dup (…s) odrzucony: {…}` — czyli ODRZUCONY duplikat radiowy — i zawyżał odbiór.
LINIA = re.compile(r'^(\d{2}:\d{2}:\d{2})\b.*?\[(RX|TX)\].*?\b(?:RX|TX): (\{.*\})\s*$')


def rola():
    try:
        r = open(os.path.join(BASE, '.role'), encoding='utf-8').read().strip()
    except OSError:
        r = 'gateway'
    return 'sup' if r.startswith('sup') else 'gw'


def klucz(d):
    """Odcisk ramki, po którym da się ją znaleźć po drugiej stronie łącza."""
    t = d.get('t', '')
    if t in ('hb', 'pong'):
        return f"up={d.get('up', '-')}"
    if 'tid' in d:
        s = d.get('s')
        return f"tid={d['tid']}" + (f"/s={s}" if s is not None else '')
    if t == 'b':
        return f"ts={d.get('ts', '-')}"
    return '-'


def sciezka_dnia(kto, dzien):
    os.makedirs(KATALOG, exist_ok=True)
    return os.path.join(KATALOG, f'traffic_{kto}_{dzien}.tsv')


class Strumienie:
    """Dwa pliki na dobę: `sup` (wszystko, co supervisor nadał/odebrał) i `g1` (tylko ruch
    dotyczący G1). G1 traci internet, więc jedynym miejscem, gdzie obie strony da się zapisać,
    jest supervisor — jego log zawiera i własne TX, i wszystko odebrane po LoRa."""

    def __init__(self):
        self._f, self._dzien = {}, None

    def dla(self, kto, dzien):
        if dzien != self._dzien:
            self.zamknij()
            self._dzien = dzien
        if kto not in self._f:
            self._f[kto] = open(sciezka_dnia(kto, dzien), 'a', encoding='utf-8')
        return self._f[kto]

    def flush(self):
        for f in self._f.values():
            f.flush()

    def zamknij(self):
        for f in self._f.values():
            f.close()
        self._f = {}


def collect():
    rol = rola()
    print(f'[traffic] rola={rol} zrodlo={ZRODLO} katalog={KATALOG}', flush=True)
    poprzednia_godz, dzien = None, time.strftime('%Y-%m-%d')
    out = Strumienie()

    while True:
        try:
            f = open(ZRODLO, encoding='utf-8', errors='replace')
        except OSError:
            time.sleep(5)
            continue
        f.seek(0, os.SEEK_END)                       # tylko NOWE ramki — nie duplikujemy historii
        rozmiar = os.path.getsize(ZRODLO)

        while True:
            linia = f.readline()
            if not linia:
                # harness bywa restartowany → log skraca się; wtedy otwieramy od nowa
                try:
                    teraz = os.path.getsize(ZRODLO)
                except OSError:
                    break
                if teraz < rozmiar:
                    f.close()
                    break
                rozmiar = teraz
                out.flush()
                time.sleep(1.0)
                continue

            m = LINIA.match(linia.strip())
            if not m:
                continue
            godz, kier, surowy = m.groups()
            try:
                d = json.loads(surowy)
            except ValueError:
                continue

            # log harnessu nie ma daty; cofnięcie się zegara = północ
            if poprzednia_godz and godz < poprzednia_godz:
                dzien = time.strftime('%Y-%m-%d')
            poprzednia_godz = godz

            gw = str(d.get('g', '-'))
            wiersz = '\t'.join((f'{dzien} {godz}', kier, gw, str(d.get('t', '?')),
                                str(len(surowy)), klucz(d), surowy)) + '\n'

            out.dla(rol, dzien).write(wiersz)        # pełny obraz strony supervisora
            if gw and gw != '-':                      # + osobny plik per bramka (g1, g2…)
                out.dla(gw.lower(), dzien).write(wiersz)


def backfill(zrodlo=None):
    """Jednorazowo przerób CAŁY istniejący log harnessu na TSV (historia sprzed startu kolektora).

    Kolektor celowo startuje od KOŃCA pliku, żeby nie dublować wpisów przy każdym restarcie —
    więc bez tego historia przepada. Wynik idzie do osobnych plików `*_backfill.tsv`, żeby nigdy
    nie zmieszał się z zapisem na żywo (i żeby powtórne uruchomienie nie psuło danych)."""
    zrodlo = zrodlo or ZRODLO
    rol = rola()

    # Log harnessu ma tylko HH:MM:SS. Wielodniowy plik trzeba więc zakotwiczyć: liczymy ile razy
    # zegar cofa się przez północ (= ile dób obejmuje), i startujemy tyle dni PRZED datą
    # ostatniej modyfikacji pliku. Bez tego wszystkie wiersze dostawały dzisiejszą datę i raport
    # pokazywał bzdury w rodzaju „20:03 -> 06:00".
    def przejscia(sc):
        ile, poprz = 0, None
        for ln in open(sc, encoding='utf-8', errors='replace'):
            mm = LINIA.match(ln.strip())
            if not mm:
                continue
            g = mm.group(1)
            if poprz and g < poprz:
                ile += 1
            poprz = g
        return ile

    dni_wstecz = przejscia(zrodlo)
    start = time.localtime(os.path.getmtime(zrodlo) - dni_wstecz * 86400)
    dzien = time.strftime('%Y-%m-%d', start)
    pliki, poprzednia_godz, n = {}, None, 0

    def daj(kto):
        if kto not in pliki:
            pliki[kto] = open(sciezka_dnia(f'{kto}', dzien).replace('.tsv', '_backfill.tsv'),
                              'w', encoding='utf-8')
        return pliki[kto]

    for linia in open(zrodlo, encoding='utf-8', errors='replace'):
        m = LINIA.match(linia.strip())
        if not m:
            continue
        godz, kier, surowy = m.groups()
        try:
            d = json.loads(surowy)
        except ValueError:
            continue
        if poprzednia_godz and godz < poprzednia_godz:      # północ → następna doba
            dzien = time.strftime('%Y-%m-%d',
                                  time.localtime(time.mktime(time.strptime(dzien, '%Y-%m-%d')) + 86400))
        poprzednia_godz = godz
        gw = str(d.get('g', '-'))
        wiersz = '\t'.join((f'{dzien} {godz}', kier, gw, str(d.get('t', '?')),
                            str(len(surowy)), klucz(d), surowy)) + '\n'
        daj(rol).write(wiersz)
        if gw != '-':
            daj(gw.lower()).write(wiersz)
        n += 1
    for f in pliki.values():
        f.close()
    print(f'backfill: {n} ramek z {zrodlo} -> ' + ', '.join(sorted(pliki)))


def wczytaj(sciezka):
    """Czyta 6 kolumn (stary format) albo 7 (z pełną ramką) — pliki sprzed zmiany nadal działają."""
    wiersze = []
    for ln in open(sciezka, encoding='utf-8', errors='replace'):
        cz = ln.rstrip('\n').split('\t')
        if len(cz) >= 6:
            r = dict(zip(('ts', 'dir', 'gw', 't', 'n', 'key'), cz[:6]))
            r['raw'] = cz[6] if len(cz) > 6 else ''
            wiersze.append(r)
    return wiersze


def stats(sciezka):
    w = wczytaj(sciezka)
    print(f'{sciezka}: {len(w)} ramek')
    if not w:
        return
    print(f'  zakres: {w[0]["ts"]} -> {w[-1]["ts"]}')
    for kier in ('RX', 'TX'):
        c = Counter(x['t'] for x in w if x['dir'] == kier)
        bajty = sum(int(x['n']) for x in w if x['dir'] == kier)
        print(f'  {kier}: {sum(c.values()):5d} ramek, {bajty:7d} B  {dict(c.most_common(8))}')


def compare(a, b):
    """A = strona nadająca (np. bramka), B = strona odbierająca (np. supervisor) — i odwrotnie."""
    wa, wb = wczytaj(a), wczytaj(b)
    print(f'A={a}: {len(wa)} ramek | B={b}: {len(wb)} ramek\n')

    for nazwa, nadawca, odbiorca in (('A->B', wa, wb), ('B->A', wb, wa)):
        tx = defaultdict(list)
        for x in nadawca:
            if x['dir'] == 'TX' and x['key'] != '-':
                tx[(x['t'], x['key'])].append(x['ts'])
        rx = {(x['t'], x['key']) for x in odbiorca if x['dir'] == 'RX' and x['key'] != '-'}

        zgubione = {k: v for k, v in tx.items() if k not in rx}
        laczne = sum(len(v) for v in tx.values())
        strata = sum(len(v) for v in zgubione.values())
        proc = (100.0 * strata / laczne) if laczne else 0.0
        print(f'=== {nazwa}: nadano {laczne} identyfikowalnych ramek, '
              f'NIE DOTARŁO {strata} ({proc:.1f}%)')
        wg_typu = Counter(t for (t, _k) in zgubione)
        if wg_typu:
            print('    wg typu:', dict(wg_typu.most_common()))
        for (t, k), kiedy in sorted(zgubione.items(), key=lambda kv: kv[1][0])[:15]:
            print(f'    zgubione  {kiedy[0]}  {t:10s} {k}')
        print()

    print('=== wolumen wg typu (A / B) ===')
    ca = Counter(x['t'] for x in wa)
    cb = Counter(x['t'] for x in wb)
    for t in sorted(set(ca) | set(cb), key=lambda t: -(ca[t] + cb[t]))[:12]:
        print(f'   {t:12s} {ca[t]:6d} / {cb[t]:6d}')


def gaps(sciezka, hb_okres=900):
    """Straty wykryte z JEDNEGO pliku (strona supervisora) — bez logu z bramki.

    Bramka nie mówi nam, czego nie dowiozła, ale zdradza to sama treść ramek:
      • HB niosą rosnące `up` co ~`hb_okres` s → dziura w sekwencji = zgubiony heartbeat,
      • transfery numerują chunki (`tid/s=N`) → brak numeru = zgubiony chunk.
    To jedyny sposób na liczenie strat, gdy carport jest odcięty od sieci."""
    w = wczytaj(sciezka)
    print(f'{sciezka}: {len(w)} ramek\n')

    hb = defaultdict(list)
    for x in w:
        if x['dir'] == 'RX' and x['t'] == 'hb' and x['key'].startswith('up='):
            try:
                hb[x['gw']].append((int(x['key'][3:]), x['ts']))
            except ValueError:
                pass
    for gw, lista in sorted(hb.items()):
        lista.sort()
        braki = 0
        print(f'=== HB od {gw}: {len(lista)} odebranych')
        for (u1, t1), (u2, t2) in zip(lista, lista[1:]):
            luka = u2 - u1
            ile = round(luka / hb_okres) - 1
            if ile >= 1:
                braki += ile
                print(f'    DZIURA {t1} -> {t2}: up {u1}->{u2} ({luka}s) = {ile} zgubionych HB')
        oczek = len(lista) + braki
        proc = (100.0 * braki / oczek) if oczek else 0.0
        print(f'    RAZEM zgubionych HB: {braki}/{oczek} ({proc:.1f}%)\n')

    chunki = defaultdict(set)
    for x in w:
        if '/s=' in x['key']:
            tid, s = x['key'].split('/s=')
            try:
                chunki[(x['gw'], x['t'], tid)].add(int(s))
            except ValueError:
                pass
    niepelne = 0
    for (gw, t, tid), s in sorted(chunki.items()):
        brak = sorted(set(range(max(s) + 1)) - s)
        if brak:
            niepelne += 1
            print(f'=== {gw} {t} {tid}: brakuje chunkow {brak[:12]} (z {max(s)+1})')
    print(f'transfery z dziurami: {niepelne}/{len(chunki)}')


def zatory(sciezka, prog=20, okno_bulk=6):
    """Wykryj ZAPCHANIE kanału i ruch, który dało się wysłać ZBIORCZO.

    PO CO: łącze LoRa ma twardy sufit — `tx_cooldown` 3 s oznacza maks. ~20 ramek/min
    z JEDNEGO nadajnika, a kanał dzielą supervisor i wszystkie bramki. Gdy zmieni się stan
    wielu urządzeń naraz (harmonogram gasi przekaźniki, ktoś klika „wszystkie ON"), ruch
    idzie POJEDYNCZO: jedna ramka `cmd` i jedna `st` na urządzenie. Przy 78 przekaźnikach
    to setki ramek — kanał stoi na kilkanaście minut, a HB i alarmy czekają w kolejce.

    Zmierzone live 2026-08-24 22:14-22:21: 104× `cmd` + 67× `st`, pik 42 ramki/min.

    Ten raport czyta TSV SUPERVISORA — czyli to, co realnie dotarło po Meshtastic. Działa
    bez dostępu do bramki (G1 nie ma stałego internetu), więc nadaje się do wykrywania takich
    sytuacji po fakcie i do mierzenia, czy optymalizacja pomogła.

    prog       — ile ramek/min uznajemy za zapchanie (domyślnie 20 = sufit jednego nadajnika)
    okno_bulk  — ile wpisów mieści się w jednej ramce zbiorczej (batcher pakuje 6)
    """
    w = wczytaj(sciezka)
    if not w:
        print(f'{sciezka}: pusty'); return
    minuty, typy = {}, {}
    for x in w:
        m = x['ts'][:16]
        a = minuty.setdefault(m, [0, 0]); a[0] += 1; a[1] += int(x['n'])
        typy.setdefault(m, Counter())[(x['dir'], x['t'], x['gw'])] += 1

    print(f'{sciezka}: {len(w)} ramek, {w[0]["ts"]} -> {w[-1]["ts"]}')
    print(f'próg zapchania: {prog} ramek/min (tx_cooldown 3 s = ~20 ramek/min na nadajnik)')

    zle = sorted((m for m, v in minuty.items() if v[0] > prog),
                 key=lambda m: -minuty[m][0])
    print()
    print(f'=== MINUTY POWYŻEJ PROGU: {len(zle)} z {len(minuty)} ===')
    for m in zle[:15]:
        c, b = minuty[m]
        # szacunek airtime: LongFast ~100 B/s (rząd wielkości, do porównań między oknami)
        print(f'   {m}  {c:3d} ramek  {b:5d} B  ~{b/100:4.1f}s airtime/60s = {b/100/60*100:4.1f}% kanału')
        for (d, t, g), n in typy[m].most_common(3):
            print(f'        {d} {t:<10} {g:<3} ×{n}')

    print()
    print('=== RUCH, KTÓRY DAŁO SIĘ WYSŁAĆ ZBIORCZO ===')
    print(f'(≥{okno_bulk} ramek tego samego typu w jednej minucie — batcher mieści {okno_bulk}/ramkę)')
    lacznie = 0
    for m in sorted(typy):
        for (d, t, g), n in typy[m].items():
            if n >= okno_bulk and t in ('cmd', 'st', 'b', 'ab'):
                zbiorczo = -(-n // okno_bulk)          # sufit dzielenia
                osz = n - zbiorczo
                lacznie += osz
                print(f'   {m}  {d} {t:<5} {g:<3} {n:3d} ramek -> {zbiorczo:2d} zbiorczych '
                      f'(oszczędność {osz})')
    print()
    if lacznie:
        print(f'RAZEM DO ODZYSKANIA: {lacznie} ramek ~= {lacznie*3/60:.1f} min zajętego kanału')
        print('(przy tx_cooldown 3 s każda zaoszczędzona ramka to 3 s wolnego łącza)')
    else:
        print('Brak ruchu nadającego się do spakowania zbiorczo — kanał używany oszczędnie.')


def main():
    if '--collect' in sys.argv:
        collect()
    elif '--congestion' in sys.argv:
        i = sys.argv.index('--congestion')
        if len(sys.argv) <= i + 1:
            print('uzycie: --congestion <plik.tsv>   (np. logs/traffic_sup_2026-08-24.tsv)')
            return 2
        zatory(sys.argv[i + 1])
    elif '--gaps' in sys.argv:
        gaps(sys.argv[sys.argv.index('--gaps') + 1])
    elif '--backfill' in sys.argv:
        i = sys.argv.index('--backfill')
        backfill(sys.argv[i + 1] if len(sys.argv) > i + 1 else None)
    elif '--compare' in sys.argv:
        i = sys.argv.index('--compare')
        compare(sys.argv[i + 1], sys.argv[i + 2])
    elif '--stats' in sys.argv:
        stats(sys.argv[sys.argv.index('--stats') + 1])
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
