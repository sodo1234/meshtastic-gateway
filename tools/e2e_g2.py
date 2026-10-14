#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pełny test E2E na żywo: G2 <-> supervisor. Każdy krok = wyzwalacz (MQTT jak z dashboardu,
publikacja z2m na bramce albo komenda SSH) + oczekiwane linie w logu sup/G2 (regex).

  python tools/e2e_g2.py                 # wszystkie kroki
  python tools/e2e_g2.py --only ping,ctrl
  python tools/e2e_g2.py --list

Nic nie zostawia zmienionego: parametry/tryb/anomalie są przywracane w krokach `*_restore`.
"""
import argparse
import json
import re
import shlex
import subprocess
import sys
import threading
import time

SUP, GW = 'td@100.79.111.24', 'td@100.98.155.78'
LOG = '/tmp/test_transport.log'

_PUB = r'''
import sys, json, config, paho.mqtt.client as mqtt
c = getattr(config, 'CONFIG', None) or config.SUPERVISOR_CONFIG
m = c['mqtt']
try: cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError: cl = mqtt.Client()
if m.get('user'): cl.username_pw_set(m['user'], m.get('pass') or None)
cl.connect(m.get('host', 'localhost'), int(m.get('port', 1883)), 30); cl.loop_start()
for i in range(1, len(sys.argv), 2):
    cl.publish(sys.argv[i], sys.argv[i + 1]).wait_for_publish(5)
cl.loop_stop(); cl.disconnect()
'''

_GET = r'''
import sys, time, config, paho.mqtt.client as mqtt
c = getattr(config, 'CONFIG', None) or config.SUPERVISOR_CONFIG
m = c['mqtt']; out = {}
try: cl = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError: cl = mqtt.Client()
if m.get('user'): cl.username_pw_set(m['user'], m.get('pass') or None)
cl.on_message = lambda *a: out.__setitem__('v', a[-1].payload.decode())
cl.connect(m.get('host', 'localhost'), int(m.get('port', 1883)), 30)
cl.subscribe(sys.argv[1]); cl.loop_start(); time.sleep(2); cl.loop_stop()
print(out.get('v', ''))
'''


def ssh(host, cmd, stdin=None, timeout=60):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', host, cmd], input=stdin, text=True,
                          capture_output=True, timeout=timeout, encoding='utf-8', errors='replace')


def pub(host, *pairs):
    args = ' '.join(shlex.quote(str(x)) for x in pairs)
    r = ssh(host, f'cd ~/meshtastic && venv/bin/python - {args}', stdin=_PUB)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[-300:])


def z2m(dev, payload):
    return (f'zigbee2mqtt/{dev}', json.dumps(payload))


def get_retained(topic):
    r = ssh(SUP, f'cd ~/meshtastic && venv/bin/python - {shlex.quote(topic)}', stdin=_GET)
    return r.stdout.strip()


class Tail:
    def __init__(self, host):
        self.lines, self.cv = [], threading.Condition()
        self.p = subprocess.Popen(['ssh', '-o', 'BatchMode=yes', host, f'tail -n0 -F {LOG}'],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                                  encoding='utf-8', errors='replace')
        threading.Thread(target=self._rd, daemon=True).start()

    def _rd(self):
        for ln in self.p.stdout:
            with self.cv:
                self.lines.append((time.monotonic(), ln.rstrip()))
                self.cv.notify_all()

    def wait(self, since, rx, deadline):
        pat = re.compile(rx)
        with self.cv:
            while True:
                for ts, ln in self.lines:
                    if ts >= since and pat.search(ln):
                        return ts - since, ln
                if time.monotonic() >= deadline:
                    return None, None
                self.cv.wait(1.0)


G = '"g":"G2"'
RX = lambda t, extra='': rf'RX: \{{"t":"{t}"[^\n]*{extra}'


def anom(code, side='a'):   # an_d: dodanie w "a", powrót do normy = zdjęcie w "r" (ten sam kod)
    return rf'RX: \{{"t":"an_d",{G},.*"{side}":\[(\[\d+,"\w+"[^]]*\],)*\[\d+,"{code}"[,\]]'


# (nazwa, grupa, wyzwalacz, [(log, regex)], timeout) — log: 'sup' | 'gw'
def steps():
    S = lambda *p: lambda: pub(SUP, *p)
    W = lambda *p: lambda: pub(GW, *p)
    return [
        # ── CORE: ping/pong/HB/czas ──
        ('ping', 'core', S('lora/supervisor/cmd/G2/ping', 'PRESS'), [('sup', RX('pong', G))], 120),
        ('ping_all', 'core', S('lora/supervisor/cmd/ping_all', 'PRESS'), [('sup', RX('pong', G))], 120),
        ('sync_g2', 'time', S('lora/supervisor/cmd/G2/sync', 'PRESS'), [('gw', r'RX: \{"t":"sync"')], 120),
        ('sync_all', 'time', S('lora/supervisor/cmd/sync_all', 'PRESS'), [('gw', r'RX: \{"t":"sync"')], 120),
        ('gw_sync_req', 'time', W('lora/gw/g2/cmd/sync_req', 'PRESS'),
         [('sup', RX('sync_req', G)), ('gw', r'RX: \{"t":"sync"')], 150),
        ('gw_ping_hb', 'core', W('lora/gw/g2/cmd/ping', 'PRESS'), [('sup', RX('hb', G))], 120),
        # ── DISCOVERY ──
        ('disc_g2', 'disc', S('lora/supervisor/cmd/G2/disc', 'PRESS'),
         [('sup', RX('disc_meta', G)), ('sup', RX('disc_vio', G))], 420),
        ('disc_all', 'disc', S('lora/supervisor/cmd/discovery_all', 'PRESS'), [('sup', RX('disc_meta', G))], 180),
        ('gw_disc', 'disc', W('lora/gw/g2/cmd/discovery', 'PRESS'), [('sup', RX('disc_meta', G))], 180),
        # ── STEROWANIE + opcja A (hash w ramce b) ──
        ('ctrl_on', 'ctrl', S('lora/g2/test_1/set', 'ON'),
         [('sup', r'RX: \{"t":"stm?",' + G + r'.*Test 1.*ON')], 150),
        ('ctrl_off', 'ctrl', S('lora/g2/test_1/set', 'OFF'),
         [('sup', r'RX: \{"t":"stm?",' + G + r'.*Test 1.*OFF')], 150),
        ('refresh', 'data', S('lora/supervisor/req/g2/temp_1', 'PRESS'),
         [('gw', r'RX: \{"t":"req"'), ('sup', r'RX: \{"t":"b",' + G + r',"ts":\d+,"d":\[\[5,')], 150),
        ('vswitch_on', 'ctrl', S('lora/g2/vio/vs_test/set', 'ON'), [('sup', RX('vsw_st', G + '.*"v":1'))], 150),
        ('vswitch_off', 'ctrl', S('lora/g2/vio/vs_test/set', 'OFF'), [('sup', RX('vsw_st', G + '.*"v":0'))], 150),
        ('vbutton', 'ctrl', S('lora/g2/vio/vb_test/press', 'PRESS'), [('gw', r'RX: \{"t":"vbtn"')], 150),
        # ── DANE z sensorów: pojedyncze + zbiorcze ──
        ('data_single', 'data', W(*z2m('Temp 1', {'temperature': 23.4, 'humidity': 47, 'battery': 100, 'linkquality': 200})),
         [('sup', r'RX: \{"t":"b",' + G + r'.*\[5,\{[^]]*"t":23\.4')], 150),
        ('data_batch', 'data', W(*z2m('Temp 1', {'temperature': 21.1, 'humidity': 41, 'battery': 100, 'linkquality': 200}),
                                 *z2m('Temp 2', {'temperature': 20.2, 'humidity': 52, 'battery': 100, 'linkquality': 200})),
         [('sup', r'RX: \{"t":"b",' + G + r'.*\[[56],\{.*\[[56],\{')], 180),
        # ── ALARMY (priority, ACK) + anomalie wszystkich typów ──
        ('alarm_leak', 'alarm', W(*z2m('Leak 1', {'water_leak': True, 'battery': 90, 'linkquality': 200})),
         [('sup', r'RX: \{"t":"b",' + G + r'.*\[3,\{[^]]*"w":1.*"p":'), ('sup', r'TX.*"t":"back","g":"G2"'),
          ('sup', anom('wl'))], 150),
        ('alarm_leak_off', 'alarm', W(*z2m('Leak 1', {'water_leak': False, 'battery': 90, 'linkquality': 200})),
         [('sup', anom('wl', 'r'))], 150),
        ('alarm_smoke', 'alarm', W(*z2m('Leak 1', {'water_leak': False, 'smoke': True, 'battery': 90, 'linkquality': 200})),
         [('sup', anom('sk'))], 150),
        ('alarm_smoke_off', 'alarm', W(*z2m('Leak 1', {'water_leak': False, 'smoke': False, 'battery': 90, 'linkquality': 200})),
         [('sup', anom('sk', 'r'))], 150),
        ('anom_high', 'anom', W(*z2m('Temp 1', {'temperature': 55.0, 'humidity': 95, 'battery': 3, 'linkquality': 200})),
         [('sup', anom('th')), ('sup', anom('hh')), ('sup', anom('cb'))], 180),
        ('anom_low', 'anom', W(*z2m('Temp 1', {'temperature': 2.0, 'humidity': 10, 'battery': 20, 'linkquality': 200})),
         [('sup', anom('tl')), ('sup', anom('hl')), ('sup', anom('lb'))], 180),
        ('anom_ok', 'anom', W(*z2m('Temp 1', {'temperature': 22.0, 'humidity': 45, 'battery': 100, 'linkquality': 200})),
         [('sup', anom('tl', 'r')), ('sup', anom('hl', 'r')), ('sup', anom('lb', 'r'))], 180),
        ('anom_offline', 'anom', W('zigbee2mqtt/Temp 2/availability', '{"state":"offline"}'), [('sup', anom('do'))], 180),
        ('anom_online', 'anom', W('zigbee2mqtt/Temp 2/availability', '{"state":"online"}'),
         [('sup', anom('do', 'r'))], 180),
        # ── CLEAR (kubełek, per-anomalia) + DUMP (blob ansnap) ──
        ('clear_prep', 'clear', W(*z2m('Temp 1', {'temperature': 55.0, 'humidity': 45, 'battery': 100, 'linkquality': 200})),
         [('sup', anom('th'))], 180),
        ('clear_other', 'clear', S('lora/supervisor/cmd/G2/clear_other', 'PRESS'),
         [('sup', r'TX.*"t":"ac_all","g":"G2","b":"other"'), ('gw', r'RX: \{"t":"ac_all"')], 150),
        ('clear_restore', 'clear', W(*z2m('Temp 1', {'temperature': 22.0, 'humidity': 45, 'battery': 100, 'linkquality': 200})),
         [('gw', r'Temp 1')], 60),
        ('dump_anom', 'bulk', S('lora/supervisor/cmd/G2/dump_anom', 'PRESS'),
         [('sup', r'RX: \{"t":"rt_end".*' + G + '.*"kind":"ansnap"'), ('sup', r'ansnap \[G2\]')], 400),
        ('gw_dump', 'bulk', W('lora/gw/g2/cmd/dump', 'PRESS'), [('sup', r'ansnap \[G2\]')], 400),
        # ── PARAMETRY: sup→gw (config/timeout/progi) i gw→sup ──
        ('param_th_sup', 'params', S('lora/params/supervisor/g2/set/TH', '34', 'lora/params/supervisor/g2/cmd/send_threshold', 'PRESS'),
         [('gw', r'RX: \{"t":"params".*"TH":34'), ('sup', RX('param_upd', G + '.*"TH":34'))], 180),
        ('param_th_restore', 'params', S('lora/params/supervisor/g2/set/TH', '35', 'lora/params/supervisor/g2/cmd/send_threshold', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"TH":35'))], 180),
        ('param_t1_sup', 'params', S('lora/params/supervisor/g2/set/T1', '61', 'lora/params/supervisor/g2/cmd/send_timeout', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"T1":61'))], 180),
        ('param_t1_restore', 'params', S('lora/params/supervisor/g2/set/T1', '60', 'lora/params/supervisor/g2/cmd/send_timeout', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"T1":60'))], 180),
        ('param_p3_sup', 'params', S('lora/params/supervisor/g2/set/P3', '16', 'lora/params/supervisor/g2/cmd/send_config', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"P3":16'))], 180),
        ('param_p3_restore', 'params', S('lora/params/supervisor/g2/set/P3', '15', 'lora/params/supervisor/g2/cmd/send_config', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"P3":15'))], 180),
        ('param_hh_gw', 'params', W('lora/params/gateway/set/HH', '71', 'lora/params/gateway/cmd/send_threshold', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"HH":71'))], 180),
        ('param_hh_restore', 'params', W('lora/params/gateway/set/HH', '70', 'lora/params/gateway/cmd/send_threshold', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"HH":70'))], 180),
        # ── STAGNACJA: P1=1 h (bateryjne) → urządzenie milczące >1 h (Door 1) dostaje sg; przywrócenie 48 h ──
        ('stag_on', 'stag', S('lora/params/supervisor/g2/set/P1', '1', 'lora/params/supervisor/g2/cmd/send_config', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"P1":1[,}]')), ('sup', anom('sg'))], 600),
        ('stag_restore', 'stag', S('lora/params/supervisor/g2/set/P1', '48', 'lora/params/supervisor/g2/cmd/send_config', 'PRESS'),
         [('sup', RX('param_upd', G + '.*"P1":48'))], 180),
        # ── TRYB BRAMKI: z sup i z bramki ──
        ('mode_sup_day', 'mode', S('lora/supervisor/cmd/set_mode_g2', 'Dzienna'),
         [('gw', r'tryb bramki -> Dzienna|"t":"set_mode"'), ('sup', RX('hb', G + '.*"gm":"day"'))], 180),
        ('mode_gw_all', 'mode', W('lora/gw/g2/cmd/set_mode', 'Całodobowa'), [('sup', RX('hb', G + '.*"gm":"all-time"'))], 180),
        # ── KALENDARZ: push sup→gw, pull, push gw→sup ──
        ('cal_push', 'cal', S('lora/supervisor/cmd/G2/calendar', 'PRESS'), [('sup', RX('cal_ack', G))], 400),
        ('cal_pull', 'cal', S('lora/supervisor/cmd/G2/pull_calendar', 'PRESS'), [('sup', r'gw_push G2')], 400),
        ('gw_cal_push', 'cal', W('lora/gw/g2/cmd/push_schedule', 'PRESS'), [('sup', r'gw_push G2')], 400),
        ('gw_cal_sync', 'cal', W('lora/gw/g2/cmd/sync_schedule', 'PRESS'), [('sup', RX('cal_req', G))], 400),
    ]


def check_data_flag(after_s=8):
    """Opcja A: po `st` zgodność DANE ma być ✅ bez czekania na HB."""
    time.sleep(after_s)
    raw = get_retained('lora/gw/g2/hashes')
    try:
        h = json.loads(raw)
    except ValueError:
        return False, raw[:200]
    return '✅' in str(h.get('data_ok', '')), f"data_ok={h.get('data_ok')} all={h.get('all_ok')}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', default='')
    ap.add_argument('--skip', default='')
    ap.add_argument('--gap', type=float, default=8.0)
    ap.add_argument('--list', action='store_true')
    a = ap.parse_args()
    all_steps = steps()
    if a.list:
        for s in all_steps:
            print(f'{s[1]:7s} {s[0]}')
        return
    only = set(filter(None, a.only.split(',')))
    skip = set(filter(None, a.skip.split(',')))
    sel = [s for s in all_steps if (not only or s[0] in only or s[1] in only)
           and s[0] not in skip and s[1] not in skip]
    tails = {'sup': Tail(SUP), 'gw': Tail(GW)}
    time.sleep(3)
    res = []
    for name, grp, trig, exps, to in sel:
        since = time.monotonic()
        try:
            trig()
        except Exception as e:
            res.append((name, grp, 'ERROR', str(e)[:120])); print(f'❌ {name}: trigger {e}', flush=True)
            continue
        deadline = since + to
        miss, times = [], []
        for log, rx in exps:
            dt, ln = tails[log].wait(since, rx, deadline)
            if dt is None:
                miss.append(f'{log}:{rx[:60]}')
            else:
                times.append(dt)
        ok = not miss
        note = f'{max(times):.1f}s' if ok else 'BRAK ' + ' | '.join(miss)
        if ok and grp == 'ctrl' and name.startswith('ctrl'):
            fok, fnote = check_data_flag()
            ok, note = fok, f'{note}; DANE: {fnote}'
        res.append((name, grp, 'PASS' if ok else 'FAIL', note))
        print(f"{'✅' if ok else '❌'} {name:18s} {note}", flush=True)
        time.sleep(a.gap)
    for t in tails.values():
        t.p.terminate()
    print('\n== WYNIK')
    for r in res:
        print(f'{r[2]:5s} {r[1]:7s} {r[0]:18s} {r[3]}')
    print(f"\n{sum(r[2] == 'PASS' for r in res)}/{len(res)} PASS")


if __name__ == '__main__':
    main()
