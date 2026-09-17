#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utworz automatyzacje harmonogramu OPARTE NA GRUPACH ZIGBEE, obok istniejacych.

PO CO (user 2026-09-17): `Schedule ON`/`OFF` wysylaly 52 pojedyncze polecenia `switch`, a potem
po minucie POWTARZALY wszystkie 52 na slepo. Przy takiej liczbie urzadzen naraz czesc
przekaznikow sie nie przelaczala. Nowe automatyzacje robia to tak, jak `GroupCaster` w bramce:

  1. SZESC groupcastow Zigbee (`zigbee2mqtt/GRP_*/set`), sekwencyjnie, z odstepem — jeden
     multicast na grupe zamiast 12 unicastow.
  2. Odczekanie, az z2m rozglosi realne stany.
  3. DOSLANIE TYLKO TYCH przekaznikow, ktore NIE sa w stanie docelowym (dwie rundy).
     `unavailable`/`unknown` licza sie jako brakujace — tak samo jak w `GroupCaster`.

GWARANCJA bierze sie z punktu 3, nie z groupcastu. Dzieki temu automatyzacja jest POPRAWNA
takze **zanim** przekazniki zostana wpisane do grup (`bind_relay_groups.py` wymaga napiecia,
wiec da sie go odpalic dopiero po zmierzchu): przy pustych grupach groupcast nic nie zrobi,
a caly ruch wykona dosylka. Zero ryzyka, ze noc zostanie bez przelaczenia.

FAZA 1 (`_1`) NIE JEST RUSZANA — swieci na stale, dopoki przekazniki maja zasilanie.
Lista 52 encji jest CZYTANA Z ISTNIEJACYCH automatyzacji, nie generowana — zeby nie rozjechac
sie z tym, co realnie stoi na obiekcie.

Stare automatyzacje NIE sa kasowane — zostaja wylaczone (`automation.turn_off`), zeby dalo sie
wrocic jednym kliknieciem.

Uzycie NA G1:  cd ~/meshtastic && venv/bin/python /tmp/make_group_automations.py [--dry-run]
Powrot:        venv/bin/python /tmp/make_group_automations.py --rollback
"""
import json
import sys
import time
import urllib.request

sys.path.insert(0, '/home/td/meshtastic')
from config import CONFIG                                    # noqa: E402

STARE = {'1774458295533': 'Schedule ON', '1774458640306': 'Schedule OFF'}
GRUPY = ['GRP_RA1_6', 'GRP_RA7_12', 'GRP_RA13_15', 'GRP_RB1_3', 'GRP_RB4_7', 'GRP_RB8_11']
ODSTEP_GRUP = 2          # s miedzy groupcastami — nie zalewaj koordynatora
PO_GROUPCAST = 10        # s zanim sprawdzimy stany (z2m musi zdazyc je rozglosic)
MIEDZY_DOSYLKAMI = 20    # s miedzy runda 1 a 2
DRY = '--dry-run' in sys.argv
ROLLBACK = '--rollback' in sys.argv

ha = CONFIG['ha_api']
H = {'Authorization': 'Bearer ' + ha['token'], 'Content-Type': 'application/json'}


def get(p):
    return json.load(urllib.request.urlopen(urllib.request.Request(ha['url'] + p, headers=H), timeout=25))


def post(p, body):
    r = urllib.request.Request(ha['url'] + p, data=json.dumps(body).encode(), headers=H, method='POST')
    return urllib.request.urlopen(r, timeout=25).read().decode()[:160]


def encje_ze_starej(aid):
    """52 encje `_2`/`_3` prosto z dzialajacej automatyzacji — zrodlo prawdy o obiekcie."""
    cfg = get(f'/api/config/automation/config/{aid}')
    for a in cfg['actions']:
        t = (a.get('target') or {}).get('entity_id')
        if isinstance(t, list) and len(t) > 10:
            return t
    raise SystemExit(f'BLAD: nie znalazlem listy encji w automatyzacji {aid}')


def zbuduj(nazwa, wyzwalacz, stan_docelowy, bool_akcja, przekazniki, opis):
    usluga = 'switch.turn_off' if stan_docelowy == 'off' else 'switch.turn_on'
    # jedno wyrazenie, dwa opakowania — zeby warunek i akcja NIE mogly sie rozjechac
    wyrazenie = f"przekazniki | reject('is_state','{stan_docelowy}') | list"
    brakujace = '{{ ' + wyrazenie + ' }}'
    warunek = '{{ (' + wyrazenie + ' | count) > 0 }}'
    akcje = [{'action': bool_akcja, 'target': {'entity_id': 'input_boolean.schedule_switch'}}]
    for i, g in enumerate(GRUPY):
        akcje.append({'action': 'mqtt.publish',
                      'data': {'topic': f'zigbee2mqtt/{g}/set',
                               'payload': json.dumps({'state': stan_docelowy.upper()})}})
        if i + 1 < len(GRUPY):
            akcje.append({'delay': {'seconds': ODSTEP_GRUP}})
    akcje.append({'delay': {'seconds': PO_GROUPCAST}})
    for runda in (1, 2):
        akcje.append({
            'alias': f'Doslij tylko brakujace — runda {runda}',
            'if': [{'condition': 'template', 'value_template': warunek}],
            'then': [{'action': usluga, 'target': {'entity_id': brakujace}}]})
        if runda == 1:
            akcje.append({'delay': {'seconds': MIEDZY_DOSYLKAMI}})
    return {
        'alias': nazwa,
        'description': opis,
        'mode': 'single',
        'triggers': [wyzwalacz],
        'conditions': [{'condition': 'template',
                        'value_template': '{{ trigger.calendar_event.summary == "BRAK PRODUKCJI" }}\n'}],
        'variables': {'przekazniki': przekazniki},
        'actions': akcje,
    }


def main():
    if ROLLBACK:
        for aid, n in STARE.items():
            post('/api/services/automation/turn_on', {'entity_id': _eid(n)})
            print(f'   ↩️  wlaczone z powrotem: {n}')
        for n in ('Schedule ON — GRUPY', 'Schedule OFF — GRUPY'):
            e = _eid(n)
            if e:
                post('/api/services/automation/turn_off', {'entity_id': e})
                print(f'   ⏸️  wylaczone: {n}')
        return 0

    przekazniki = encje_ze_starej('1774458295533')
    assert len(przekazniki) == 52, f'oczekiwano 52 encji faz 2+3, jest {len(przekazniki)}'
    assert not [e for e in przekazniki if e.endswith('_1')], 'faza 1 NIE moze byc na liscie!'
    print(f'== encje z istniejacej automatyzacji: {len(przekazniki)} (fazy 2 i 3, bez fazy 1)')

    nowe = [
        ('Schedule ON — GRUPY', {'trigger': 'calendar.event_started',
                                 'target': {'entity_id': 'calendar.lora_g1'}}, 'off',
         'input_boolean.turn_on',
         'Start wydarzenia BRAK PRODUKCJI (22:15) → GASI fazy 2 i 3. '
         '6 groupcastow Zigbee + doslanie tylko brakujacych (2 rundy). Faza 1 nietkniete.'),
        ('Schedule OFF — GRUPY', {'trigger': 'calendar.event_ended',
                                  'target': {'entity_id': 'calendar.lora_g1'}}, 'on',
         'input_boolean.turn_off',
         'Koniec wydarzenia BRAK PRODUKCJI (05:45) → ZAPALA fazy 2 i 3. '
         '6 groupcastow Zigbee + doslanie tylko brakujacych (2 rundy). Faza 1 nietkniete.'),
    ]
    utworzone = []
    for nazwa, wyz, stan, b_akcja, opis in nowe:
        cfg = zbuduj(nazwa, wyz, stan, b_akcja, przekazniki, opis)
        aid = str(int(time.time() * 1000))
        cfg['id'] = aid
        if DRY:
            print(f'   [dry] {nazwa}: {len(cfg["actions"])} akcji '
                  f'({len(GRUPY)} groupcastow + 2 rundy dosylki)')
            continue
        print(f'   ✅ {nazwa} (id={aid}) | {post(f"/api/config/automation/config/{aid}", cfg)}')
        utworzone.append(nazwa)
        time.sleep(2)
    if DRY:
        return 0

    time.sleep(3)
    for aid, n in STARE.items():                      # stare: WYLACZ, nie kasuj
        e = _eid(n)
        if e:
            post('/api/services/automation/turn_off', {'entity_id': e})
            print(f'   ⏸️  wylaczone (zostaje, do powrotu): {n} → {e}')
    for n in utworzone:                               # nowe: wlacz
        e = _eid(n)
        if e:
            post('/api/services/automation/turn_on', {'entity_id': e})
            print(f'   ▶️  wlaczone: {n} → {e}')
    return 0


def _eid(alias):
    for e in get('/api/states'):
        if e['entity_id'].startswith('automation.') and e['attributes'].get('friendly_name') == alias:
            return e['entity_id']
    return None


if __name__ == '__main__':
    sys.exit(main())
