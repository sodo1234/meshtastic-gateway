#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dodaj sekcje REPEATERY do widoku przekaznikow (supervisor i/lub bramka G1).

PO CO (polecenie usera 2026-09-17): 25 repeaterow ZBMINIR2 przedluza zasieg Zigbee do dalszych
szyn. Nie steruje sie nimi i NIE zmieniaja stanu, wiec byly na dashboardach CALKOWICIE
niewidoczne — padniety repeater objawial sie jako „przekazniki za nim nie reaguja", bez zadnej
wskazowki gdzie szukac. Kafel ma odpowiadac na jedno pytanie: KTORY repeater zyje.

ZASADY, ktorych ten skrypt pilnuje (CARPORT — zasady naczelne):
  • NIE jest generatorem dashboardu — czyta biezaca konfiguracje i dokłada/podmienia WYLACZNIE
    sekcje repeaterow. Reczne kafle zostaja nietkniete.
  • Reuzywa istniejacy styl: nowy szablon `carport_repeater` DZIEDZICZY po `carport_button`
    i nadpisuje tylko ikone i linie info (repeater jest sieciowy — bateria nie ma sensu).
  • Idempotentny: drugie uruchomienie podmienia sekcje, nie dubluje ich.

Konfiguracja wchodzi na STDIN (JSON), wychodzi na STDOUT (JSON) — dzieki temu backup i podglad
robi sie po stronie wolajacego, a skrypt niczego nie zapisuje na dysku maszyny.

Uzycie:
    cat dash.json | python3 add_repeater_tiles.py --rola sup|gw > dash_nowy.json
"""
import json
import sys

# Repeatery pogrupowane wg szyny, na ktorej wisza (uklad = jak sekcje przekaznikow).
# TYLKO RB6 (user 2026-09-17). Z 25 repeaterow monitorujemy 7: `RB6_R1-R7` realnie przeciagaja
# sygnal dalej. RA8/RA11/RA14/RB10 (18 szt.) byly extenderami sieci dla czujek ruchu, z ktorych
# prawdopodobnie rezygnujemy — usuniete z `monitored`, z whitelisty supervisora i z obu dashboardow.
SZYNY = [
    ('RB6', [f'RB6_R{i}' for i in range(1, 8)]),
]
NAGLOWEK = 'REPEATERY'

# Linia info: czas ostatniego kontaktu, LQI, zyje/martwy + ostrzezenie o stagnacji.
# Baterii NIE pokazujemy — ZBMINIR2 jest zasilany sieciowo, „🔋 --" tylko mylilo.
# `t` (czas): na supervisorze jest encja `sensor.<slug>_last_seen`; bramka G1 jej NIE ma
# (to encja mostu MQTT). Zamiast martwego '--' bierzemy tam `last_changed` encji
# available — czyli kiedy repeater ostatnio zmienil stan zycia. Linia niesie wtedy
# realna informacje po OBU stronach, bez rozjezdzania sie szablonow.
INFO = ("[[[ /*REPEATER*/"
        "var av=entity;"
        "var slug=av?av.entity_id.replace(/^binary_sensor[.]/,'').replace(/_available$/,''):'';"
        "var lqe=states['sensor.'+slug+'_link_quality']||states['sensor.'+slug+'_lqi'];"
        "var lq=(lqe&&lqe.state!=='unknown'&&lqe.state!=='unavailable')?parseInt(lqe.state):null;"
        "var se=states['sensor.'+slug+'_last_seen'];"
        "var t=(se&&se.state!=='unknown'&&se.state!=='unavailable')?se.state:null;"
        "if(!t&&av&&av.last_changed){var d=new Date(av.last_changed);"
        "var p=function(n){return (n<10?'0':'')+n;};"
        "t=p(d.getDate())+'.'+p(d.getMonth()+1)+' '+p(d.getHours())+':'+p(d.getMinutes());}"
        "if(!t){t='--';}"
        "var sg=states['binary_sensor.'+slug+'_stagnation'];"
        "var stag=!!(sg&&sg.state==='on');"
        "var alive=!!(av&&av.state==='on');"
        "var dot=alive?'<span style=\"color:#4ade80;\">● LIVE</span>'"
        ":'<span style=\"color:#f87171;font-weight:800;\">● MARTWY</span>';"
        "if(stag){dot+=' <span style=\"color:#facc15;\">⏳STAGNACJA</span>';}"
        "return '<div style=\"display:flex;flex-direction:column;gap:2px;font-size:9px;"
        "color:#737373;line-height:1.3;white-space:nowrap;\">'"
        "+'<span>🕐 '+t+'</span>'"
        "+'<span>📶 '+(lq!=null?lq:'--')+'</span>'"
        "+'<span>'+dot+'</span></div>'; ]]]")

IKONA = ("[[[ /*REPEATER*/var av=entity;return (av&&av.state==='on')"
         "?'mdi:access-point':'mdi:access-point-off'; ]]]")

SZABLON = {
    'template': 'carport_button',        # dziedziczenie: caly styl/layout zostaje bez zmian
    'icon': IKONA,
    'custom_fields': {'info': INFO},
}


# Wzorzec entity_id rozni sie miedzy maszynami i to NIE jest przypadek:
#   supervisor — encje tworzy most MQTT:  binary_sensor.lora_ra8_r1_ra8_r1_available
#   bramka G1  — encje tworzy lokalny z2m: binary_sensor.ra8_r1_available
# Dlatego wzorzec jest parametrem, a nie zaszyty w kodzie (zasada: dashboard bramki = kod
# supervisora 1:1, TYLKO z podmiana nazw encji).
WZORCE = {
    'sup': 'binary_sensor.lora_{s}_{s}_available',
    'gw':  'binary_sensor.{s}_available',
}


def _eid(wzor, dev):
    return wzor.format(s=dev.lower())


def _sekcja(wzor, szyna, devs):
    return {
        'type': 'grid',
        'cards': [
            {'type': 'custom:button-card', 'template': 'lora_hdr',
             'name': f'{NAGLOWEK} {szyna} ({len(devs)})',
             'layout_options': {'grid_columns': 4}},
            {'type': 'grid', 'columns': 3, 'square': False,
             'cards': [{'type': 'custom:button-card', 'template': 'carport_repeater',
                        'entity': _eid(wzor, d), 'name': d} for d in devs]},
        ],
    }


def _naglowek_sekcji(sec):
    try:
        return sec['cards'][0].get('name', '')
    except Exception:
        return ''


def patch(cfg, wzor, view_path='carport-relays'):
    cfg.setdefault('button_card_templates', {})['carport_repeater'] = SZABLON

    views = [v for v in cfg.get('views', []) if v.get('path') == view_path]
    if not views:
        raise SystemExit(f'BLAD: brak widoku {view_path!r} — nie zgaduje, gdzie wstawic')
    v = views[0]
    if v.get('type') != 'sections':
        raise SystemExit(f'BLAD: widok {view_path!r} nie jest typu sections')

    stare = [i for i, s in enumerate(v['sections'])
             if _naglowek_sekcji(s).startswith(NAGLOWEK)]
    for i in reversed(stare):                       # idempotencja: wywal poprzednie
        v['sections'].pop(i)
    v['sections'].extend(_sekcja(wzor, sz, devs) for sz, devs in SZYNY)
    return cfg, len(stare)


def main():
    rola = sys.argv[sys.argv.index('--rola') + 1] if '--rola' in sys.argv else 'sup'
    if rola not in WZORCE:
        raise SystemExit(f'BLAD: --rola musi byc jedna z {sorted(WZORCE)}')
    wzor = WZORCE[rola]
    view = 'carport-relays'
    if '--view' in sys.argv:
        view = sys.argv[sys.argv.index('--view') + 1]
    cfg = json.load(sys.stdin)
    cfg, podmienione = patch(cfg, wzor, view)
    ile = sum(len(d) for _, d in SZYNY)
    print(f'sekcji repeaterow: {len(SZYNY)} ({ile} urzadzen), '
          f'podmienionych starych: {podmienione}', file=sys.stderr)
    json.dump(cfg, sys.stdout, ensure_ascii=False)


if __name__ == '__main__':
    main()
