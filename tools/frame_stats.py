#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Statystyki ramek z logu supervisora dla dwóch okien czasu (stara vs nowa wersja).

Uruchamiane NA supervisorze (log ma tylko HH:MM:SS — dzień liczony z cofnięć zegara):
  python3 frame_stats.py /tmp/test_transport.log "0 11:58:00" "2 10:47:00" "4 11:10:00" "4 23:59:59"
Okno = (dzień, godzina) od początku pliku; dzień 0 = pierwsza linia.
"""
import json
import re
import sys
from collections import defaultdict

TS = re.compile(r'^(\d\d):(\d\d):(\d\d) ')


def windows(path):
    day, prev = 0, -1
    with open(path, encoding='utf-8', errors='replace') as f:
        for ln in f:
            m = TS.match(ln)
            if not m:
                continue
            sec = int(m[1]) * 3600 + int(m[2]) * 60 + int(m[3])
            if prev - sec > 6 * 3600:          # północ
                day += 1
            prev = sec
            yield day * 86400 + sec, ln


def parse_at(s):
    d, t = s.split()
    h, m, x = map(int, t.split(':'))
    return int(d) * 86400 + h * 3600 + m * 60 + x


def stats(path, a, b):
    rx = defaultdict(list)                     # (gw, typ) → rozmiary
    ev = defaultdict(int)
    first = last = None
    for ts, ln in windows(path):
        if not (a <= ts <= b):
            continue
        first = first if first is not None else ts
        last = ts
        i = ln.find('RX: {')
        if i >= 0:
            raw = ln[i + 4:].strip()
            try:
                fr = json.loads(raw)
            except ValueError:
                continue
            rx[(fr.get('g', '-'), fr.get('t'))].append(len(raw.encode('utf-8')))
            if fr.get('t') in ('b', 'st', 'stm') and 'dh' in fr:
                ev[f"{fr.get('g')}_hash_w_ramce"] += 1
        for key, pat in (('dsnap_req', '"t":"dsnap_req"'), ('rozjazd', 'rozjazd hasha danych'),
                         ('zg_push', 'zgodność'), ('rf_dup', 'RF-dup'), ('hb_stale', 'pomijam decyzje')):
            if pat in ln:
                ev[key] += 1
    hours = max((last - first) / 3600, 1e-9) if first is not None else 0
    return rx, ev, hours


def main():
    path = sys.argv[1]
    old = (parse_at(sys.argv[2]), parse_at(sys.argv[3]))
    new = (parse_at(sys.argv[4]), parse_at(sys.argv[5]))
    out = {}
    for name, (a, b) in (('old', old), ('new', new)):
        rx, ev, h = stats(path, a, b)
        out[name] = {
            'hours': round(h, 2),
            'frames': {f'{g}/{t}': {'n': len(v), 'per_h': round(len(v) / h, 1) if h else 0,
                                    'avg_B': round(sum(v) / len(v), 1), 'max_B': max(v)}
                       for (g, t), v in sorted(rx.items()) if g in ('G1', 'G2')},
            'events_per_h': {k: round(v / h, 2) if h else 0 for k, v in ev.items()},
        }
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
