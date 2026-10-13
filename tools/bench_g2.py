#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Benchmark latencji G2 <-> supervisor na zywo (v11+ fast lane) + kontrola slotu G2.

Uruchamiany na PC workera (SSH/Tailscale do obu maszyn). Nic nie zmienia w konfiguracji:
publikuje te same tematy MQTT, ktore wysyla dashboard, i mierzy czas do odpowiedzi
widocznej w logu supervisora (`📥 RX: {...}`, ramki juz zdekodowane do dlugich nazw).

Scenariusze (kazdy N razy, przerwa --gap s):
  ping     lora/supervisor/cmd/G2/ping           -> RX pong   (g=G2)
  ctrl     lora/g2/<safe>/set  ON/OFF naprzemian -> RX st/stm (d=<urzadzenie>, v=wartosc)
  params   lora/params/supervisor/g2/cmd/send_config -> RX param_upd (g=G2)
  anomaly  komenda SSH na G2 (domyslnie /tmp/avail_pub.py <dev> offline/online)
           -> RX ab/an_d z kodem "do" (offline) i "dn" (powrot)
  air      analiza /tmp/lora_air.log z G2: bulk tylko w oknie slotu G2

Uzycie (z katalogu repo):
  python tools/bench_g2.py --sup td@100.79.111.24 --gw td@100.98.155.78 --device "Test 1"
  python tools/bench_g2.py --only ping,params -n 10
  python tools/bench_g2.py --selftest          # bez sieci: test parserow
Hasla MQTT czyta config.py NA MASZYNIE (jak Meshtastic_SCADA.py); skrypt ich nie zna.
"""
import argparse
import json
import os
import shlex
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'skrypty'))
from modules.transport.fastlane import DEFAULT_IMMEDIATE_TYPES  # noqa: E402

# Baseline z issue #3: stara wersja 2f126bf i v9 (mediany w sekundach).
BASELINE = {'ping': (6, 60), 'ctrl': (31, 38), 'params': (7, 32), 'anomaly': (11, 19)}

# Publikacja MQTT wykonywana NA supervisorze, poswiadczenia z jego config.py.
_PUB = r'''
import sys
import config
import paho.mqtt.client as mqtt
topic, payload = sys.argv[1], sys.argv[2]
c = getattr(config, 'CONFIG', None) or config.SUPERVISOR_CONFIG
m = c['mqtt']
try:
    cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    cl = mqtt.Client()
if m.get('user'):
    cl.username_pw_set(m['user'], m.get('pass') or None)
cl.connect(m.get('host', 'localhost'), int(m.get('port', 1883)), 30)
cl.loop_start()
cl.publish(topic, payload).wait_for_publish(5)
cl.loop_stop(); cl.disconnect()
'''


def parse_rx(line):
    """Linia logu SCADA -> dict ramki albo None."""
    i = line.find('RX: {')
    if i < 0:
        return None
    try:
        frame = json.loads(line[i + 4:].strip())
    except ValueError:
        return None
    return frame if isinstance(frame, dict) else None


class SupLog:
    """`tail -F` logu supervisora przez SSH; ramki RX ze znacznikiem czasu PC (monotonic)."""

    def __init__(self, host, path):
        self.proc = subprocess.Popen(['ssh', host, f'tail -n0 -F {shlex.quote(path)}'],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                     text=True, encoding='utf-8', errors='replace')
        self.frames = []
        self.cv = threading.Condition()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            frame = parse_rx(line)
            if frame is not None:
                with self.cv:
                    self.frames.append((time.monotonic(), frame))
                    self.cv.notify_all()

    def wait(self, since, match, timeout):
        """Pierwsza ramka po `since` spelniajaca `match`; zwraca opoznienie [s] albo None."""
        end = since + timeout
        with self.cv:
            while True:
                for ts, frame in self.frames:
                    if ts >= since and match(frame):
                        return ts - since
                left = end - time.monotonic()
                if left <= 0:
                    return None
                self.cv.wait(min(left, 1.0))

    def close(self):
        self.proc.terminate()


def ssh(host, cmd, stdin=None, timeout=30):
    return subprocess.run(['ssh', host, cmd], input=stdin, text=True, capture_output=True,
                          timeout=timeout)


def publish(args, topic, payload):
    d = args.sup_dir                                  # ~ musi zostac poza cudzyslowem (rozwija powloka zdalna)
    d = '~/' + shlex.quote(d[2:]) if d.startswith('~/') else shlex.quote(d)
    cmd = (f'cd {d} && venv/bin/python - '
           f'{shlex.quote(topic)} {shlex.quote(payload)}')
    r = ssh(args.sup, cmd, stdin=_PUB)
    if r.returncode != 0:
        raise RuntimeError(f'mqtt publish {topic}: {r.stderr.strip()[-300:]}')


def _is_gw(frame, gw):
    return frame.get('g') == gw


def match_ctrl(gw, dev, val):
    def m(f):
        if not _is_gw(f, gw):
            return False
        if f.get('t') == 'st':
            return f.get('d') == dev and str(f.get('v', '')).upper() == val
        if f.get('t') == 'stm':
            return str((f.get('d') or {}).get(dev, '')).upper() == val
        return False
    return m


def match_anomaly(gw, code):
    def m(f):
        if not _is_gw(f, gw):
            return False
        j = lambda k: json.dumps(f.get(k), ensure_ascii=False)
        if f.get('t') == 'ab':
            return f'"{code}"' in j('d')
        # an_d (delta): dodane w 'a', zdjete w 'r'; dn = zdjecie 'do'
        return f.get('t') == 'an_d' and (f'"{code}"' in j('a') or (code == 'dn' and '"do"' in j('r')))
    return m


def run_case(log, name, trigger, match, n, gap, timeout):
    out = []
    for i in range(n):
        since = time.monotonic()
        trigger(i)
        dt = log.wait(since, match(i), timeout)
        out.append(dt)
        print(f'  {name} #{i + 1}: ' + (f'{dt:5.1f} s' if dt is not None else f'TIMEOUT >{timeout:.0f} s'),
              flush=True)
        time.sleep(gap)
    return out


def verdict(name, times, factor):
    ok = [t for t in times if t is not None]
    lost = len(times) - len(ok)
    if not ok:
        return f'{name:8s} FAIL  0/{len(times)} odpowiedzi'
    med, worst = statistics.median(ok), max(ok)
    old, v9 = BASELINE.get(name, (None, None))
    limit = old * factor if old else None
    status = 'PASS' if lost == 0 and (limit is None or med <= limit) else 'FAIL'
    base = f' | stara {old} s, v9 {v9} s, limit {limit:.0f} s' if old else ''
    return (f'{name:8s} {status}  mediana {med:.1f} s, max {worst:.1f} s, '
            f'zgubione {lost}/{len(times)}{base}')


def slot_window(gw, slots, cycle=60):
    """(start, end) okna bramki w cyklu; slots = {"G1": 40, "G2": 10, "G3": 10}."""
    start = 0
    for g, length in slots.items():
        if g == gw:
            return start % cycle, (start + length) % cycle or cycle
        start += length
    raise KeyError(gw)


def air_violations(lines, window, immediate, slack=1.0):
    """Ramki bulk nadane poza oknem slotu. Linia: 'HH:MM:SS.mmm typ rozmiar'."""
    a, b = window
    bad, total = [], 0
    for ln in lines:
        parts = ln.split()
        if len(parts) < 3 or ':' not in parts[0]:
            continue
        hh, mm, ss = parts[0].split(':')
        sec = float(ss) % 60
        typ = parts[1]
        if typ in immediate:
            continue
        total += 1
        if not (a - slack <= sec < b + slack):
            bad.append(ln.strip())
    return total, bad


def selftest():
    f = parse_rx('10:00:01 [INFO] [RX] 📥 RX: {"t":"pong","g":"G2","hash":"ab"}')
    assert f == {'t': 'pong', 'g': 'G2', 'hash': 'ab'}
    assert parse_rx('10:00:01 [INFO] [TX] 📤 TX: {"t":"ping"}') is None
    assert match_ctrl('G2', 'Test 1', 'ON')({'t': 'st', 'g': 'G2', 'd': 'Test 1', 'v': 'on'})
    assert match_ctrl('G2', 'Test 1', 'ON')({'t': 'stm', 'g': 'G2', 'd': {'Test 1': 'ON'}})
    assert not match_ctrl('G2', 'Test 1', 'ON')({'t': 'st', 'g': 'G1', 'd': 'Test 1', 'v': 'ON'})
    assert match_anomaly('G2', 'do')({'t': 'ab', 'g': 'G2', 'd': [[5, 'do', None]]})
    assert not match_anomaly('G2', 'do')({'t': 'ab', 'g': 'G2', 'd': [[5, 'dn', None]]})
    slots = {'G1': 20, 'G2': 20, 'G3': 20}
    assert slot_window('G2', slots) == (20, 40) and slot_window('G3', slots) == (40, 60)
    imm = frozenset(DEFAULT_IMMEDIATE_TYPES)
    total, bad = air_violations(['10:00:21.100 hb 120', '10:00:12.000 b 90',
                                 '10:00:12.500 pong 40'], (20, 40), imm)
    assert total == 2 and bad == ['10:00:12.000 b 90'], (total, bad)
    print('selftest OK')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--sup', default='td@100.79.111.24')
    ap.add_argument('--gw', default='td@100.98.155.78')
    ap.add_argument('--gw-id', default='G2')
    ap.add_argument('--sup-dir', default='~/meshtastic')
    ap.add_argument('--sup-log', default='/tmp/test_transport.log')
    ap.add_argument('--prefix', default='lora')
    ap.add_argument('--device', default='Test 1', help='urzadzenie do testu sterowania/anomalii')
    ap.add_argument('--anomaly-cmd', default='cd ~/meshtastic && venv/bin/python /tmp/avail_pub.py {dev} {state}',
                    help='komenda SSH na bramce; {dev} i {state}=offline|online')
    ap.add_argument('--only', default='ping,ctrl,params,anomaly,air')
    ap.add_argument('-n', type=int, default=5)
    ap.add_argument('--gap', type=float, default=15.0, help='przerwa miedzy probami [s]')
    ap.add_argument('--timeout', type=float, default=120.0)
    ap.add_argument('--factor', type=float, default=1.2, help='PASS gdy mediana <= stara*factor')
    ap.add_argument('--slots', default='{"G1":20,"G2":20,"G3":20}')
    ap.add_argument('--selftest', action='store_true')
    args = ap.parse_args()
    if args.selftest:
        return selftest()

    gw, gl = args.gw_id, args.gw_id.lower()
    only = set(args.only.split(','))
    results = {}
    log = SupLog(args.sup, args.sup_log)
    time.sleep(2)                                     # tail musi wystartowac przed pierwszym triggerem
    try:
        if 'ping' in only:
            print('== ping')
            results['ping'] = run_case(
                log, 'ping', lambda i: publish(args, f'{args.prefix}/supervisor/cmd/{gw}/ping', 'PRESS'),
                lambda i: lambda f: _is_gw(f, gw) and f.get('t') == 'pong',
                args.n, args.gap, args.timeout)
        if 'ctrl' in only:
            print('== ctrl')
            safe = args.device.replace(' ', '_').lower()
            vals = lambda i: 'ON' if i % 2 == 0 else 'OFF'
            results['ctrl'] = run_case(
                log, 'ctrl', lambda i: publish(args, f'{args.prefix}/{gl}/{safe}/set', vals(i)),
                lambda i: match_ctrl(gw, args.device, vals(i)),
                args.n, args.gap, args.timeout)
        if 'params' in only:
            print('== params')
            results['params'] = run_case(
                log, 'params',
                lambda i: publish(args, f'{args.prefix}/params/supervisor/{gl}/cmd/send_config', 'PRESS'),
                lambda i: lambda f: _is_gw(f, gw) and f.get('t') == 'param_upd',
                args.n, args.gap, args.timeout)
        if 'anomaly' in only:
            print('== anomaly (offline -> do, online -> dn)')
            state = lambda i: 'offline' if i % 2 == 0 else 'online'

            def trig(i):
                cmd = args.anomaly_cmd.format(dev=shlex.quote(args.device), state=state(i))
                r = ssh(args.gw, cmd)
                if r.returncode != 0:
                    raise RuntimeError(f'anomaly-cmd: {r.stderr.strip()[-300:]}')
            results['anomaly'] = run_case(
                log, 'anomaly', trig,
                lambda i: match_anomaly(gw, 'do' if state(i) == 'offline' else 'dn'),
                args.n * 2, args.gap, args.timeout)   # pary offline/online: urzadzenie konczy online
    finally:
        log.close()

    print('\n== wynik')
    for name, times in results.items():
        print(verdict(name, times, args.factor))
    if 'air' in only:
        r = ssh(args.gw, 'tail -n 3000 /tmp/lora_air.log')
        window = slot_window(gw, json.loads(args.slots))
        total, bad = air_violations(r.stdout.splitlines(), window, frozenset(DEFAULT_IMMEDIATE_TYPES))
        status = 'PASS' if total and not bad else ('BRAK DANYCH' if not total else 'FAIL')
        print(f'air      {status}  bulk {total} ramek, poza oknem {window[0]}-{window[1]} s: {len(bad)}')
        for ln in bad[:10]:
            print(f'           {ln}')


if __name__ == '__main__':
    main()
