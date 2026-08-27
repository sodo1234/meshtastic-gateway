#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kafel przekaznika G1 ma w DZIEN pokazywac OFFLINE + OFF, nie ostatni stan z nocy.

DLACZEGO SAMA DOSTEPNOSC NIE WYSTARCZA: lustro `binary_sensor.lora_g1_*_available` potrafi byc
nieaktualne (blob `avail` przychodzi po LoRa co kilka godzin, a przy zapchanym kanale rzadziej).
Zmierzone 2026-08-27 18:06: sup pokazywal 78/78 DOSTEPNYCH, choc bramka w tym samym czasie
raportowala w HB `ga=0` (`sensor.lora_gateway_g1_gw_g1_tryb_stan` = „wstrzymana").

ZRODLO PRAWDY, KTORE JEST ZAWSZE SWIEZE: tryb bramki. Bramka NOCNA „wstrzymana" = z definicji
dzien = przekazniki carportu bez zasilania. To nie domysl, tylko stan zgloszony przez sama
bramke w heartbeacie. Wiec kafel liczy `off` jako: availability=off LUB bramka wstrzymana.

Efekt: w dzien wszystkie 78 kafli = szara wtyczka `mdi:power-plug-off`, przygaszone, „OFFLINE",
bez zoltej zarowki „ON" sprzed doby. Alarmy pozostaja wyciszone (supresja trybem po stronie
bramki) — supervisor WIDZI, ale nie SPAMUJE.
"""
import asyncio, json, sys, time
sys.path.insert(0, '/home/td/meshtastic'); import config
import websockets

DRY = '--dry' in sys.argv
TOK = config.CONFIG['ha_api']['token']
ZN = '/*GW_SUSPEND_FIX*/'
STAN = "sensor.lora_gateway_g1_gw_g1_tryb_stan"

# `susp` = bramka wstrzymana (ga=0). Encja publikuje 'aktywna' albo 'wstrzymana (...)'.
SUSP = ("var gs=states['%s'];var susp=(gs&&String(gs.state).indexOf('wstrzyman')===0);" % STAN)
PROLOG = ("var e=entity;var slug=e?e.entity_id.replace(/^switch[.]/,''):'';"
          "var av=states['binary_sensor.'+slug+'_available'];" + SUSP +
          "var off=(!e||e.state==='unavailable'||e.state==='unknown'||!av||av.state==='off'||susp);")

NOWE = {
    'icon':   "[[[ %s%s return off?'mdi:power-plug-off':(e.state==='on'?'mdi:lightbulb-on':'mdi:lightbulb-off-outline'); ]]]" % (ZN, PROLOG),
    'color':  "[[[ %s%s return off?'#525252':(e.state==='on'?'#facc15':'#525252'); ]]]" % (ZN, PROLOG),
    'border': "[[[ %s%s return off?'1px solid #1f1f1f':(e.state==='on'?'1px solid #facc15':'1px solid #1f1f1f'); ]]]" % (ZN, PROLOG),
    'opacity': "[[[ %s%s return off?'0.55':'1'; ]]]" % (ZN, PROLOG),
}


async def cmd(ws, msg, mid):
    await ws.send(json.dumps({**msg, 'id': mid}))
    while True:
        r = json.loads(await ws.recv())
        if r.get('id') == mid:
            return r


def _styl(lista, klucz, wartosc):
    for poz in lista:
        if klucz in poz:
            poz[klucz] = wartosc; return 'zmieniono'
    lista.append({klucz: wartosc}); return 'dodano'


def patch(cfg):
    tpl = (cfg.get('button_card_templates') or {}).get('carport_relay')
    if not tpl:
        return []
    if ZN in json.dumps(tpl, ensure_ascii=False):
        return ['juz poprawiony']
    zm = []
    tpl['icon'] = NOWE['icon']; zm.append('icon')
    st = tpl.setdefault('styles', {})
    zm.append('icon.color:' + _styl(st.setdefault('icon', []), 'color', NOWE['color']))
    zm.append('card.border:' + _styl(st.setdefault('card', []), 'border', NOWE['border']))
    zm.append('card.opacity:' + _styl(st['card'], 'opacity', NOWE['opacity']))
    # linijka `info` liczy wlasne `off` — niech mowi to samo co ikona
    info = (tpl.get('custom_fields') or {}).get('info')
    if info and "var off=(!e||!av||av.state==='off');" in info:
        tpl['custom_fields']['info'] = info.replace(
            "var off=(!e||!av||av.state==='off');",
            SUSP + "var off=(!e||!av||av.state==='off'||susp);", 1)
        zm.append('info.off')
    return zm


async def go():
    async with websockets.connect('ws://localhost:8123/api/websocket', max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({'type': 'auth', 'access_token': TOK}))
        if json.loads(await ws.recv()).get('type') != 'auth_ok':
            raise SystemExit('auth_failed')
        dash = (await cmd(ws, {'type': 'lovelace/dashboards/list'}, 1)).get('result') or []
        stamp = time.strftime('%Y%m%d_%H%M%S'); mid = 10
        for d in dash:
            p = d.get('url_path')
            if p != 'lora-scada-g1':
                continue                      # tylko dashboard G1 — G2 jest calodobowa
            mid += 1
            r = await cmd(ws, {'type': 'lovelace/config', 'url_path': p}, mid)
            if not r.get('success'):
                continue
            cfg = r['result']; oryg = json.dumps(cfg, ensure_ascii=False)
            zm = patch(cfg)
            print('  %-16s %s' % (p, ', '.join(zm) or 'brak szablonu'))
            if not zm or zm == ['juz poprawiony'] or DRY:
                continue
            bak = '/tmp/dash_backup_%s_%s.json' % (p, stamp)
            open(bak, 'w', encoding='utf-8').write(oryg)
            mid += 1
            s = await cmd(ws, {'type': 'lovelace/config/save', 'url_path': p, 'config': cfg}, mid)
            print('  %-16s zapis: %s  backup=%s' % (p, 'OK' if s.get('success') else s.get('error'), bak))

asyncio.run(go())
