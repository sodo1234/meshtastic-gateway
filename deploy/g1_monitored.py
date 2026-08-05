#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""G1 (carport): docelowa lista `monitored` = RA1-15_{1,2,3} + RB1-13_{1,2,3} = 84 przekaźniki.

PO CO: dziś G1 ma `monitored` = 194 z 194 urządzeń (A4 w planie sprzedażowym), więc tryb
bramki nic nie gating'uje, a sweep obejmuje wszystko — przy 93% całego ruchu LoRa systemu.
Zawężenie do 84 realnie sterowanych przekaźników jest największą pojedynczą oszczędnością
pasma, jaka została do zrobienia.

⚠️ DO POTWIERDZENIA PRZY G1: mirror supervisora ma dziś RA=45 (komplet) ale RB=33, czyli
   RB1-11. RB12_x i RB13_x (6 urządzeń) NIE istnieją w lustrze — bo dotychczasowa
   mirror_whitelist stała na 78 i je odcinała. Zanim wpiszesz 84 na stałe, uruchom ten
   skrypt NA G1 z --check: powie, czy RB12/RB13 faktycznie są w zigbee2mqtt carportu.
   Jeśli ich nie ma, docelowa liczba to 78, a nie 84 — i trzeba ustalić dlaczego.

Użycie:
    python3 deploy/g1_monitored.py                 # wypisz listę + snippet do config.py
    python3 deploy/g1_monitored.py --check         # NA G1: porównaj listę z realnym z2m
    python3 deploy/g1_monitored.py --whitelist     # snippet mirror_whitelist dla supervisora
"""
import json
import sys

RA_MAX = 15          # RA1..RA15
RB_MAX = 13          # RB1..RB13  (dziś w lustrze tylko do RB11 — patrz uwaga wyżej)
KANALY = (1, 2, 3)   # każdy przekaźnik ma 3 wyjścia


def lista():
    out = []
    for prefix, mx in (('RA', RA_MAX), ('RB', RB_MAX)):
        for n in range(1, mx + 1):
            for k in KANALY:
                out.append(f'{prefix}{n}_{k}')
    return out


def snippet(nazwa, items, wciecie=8):
    """Sformatuj listę po 6 w wierszu — czytelne w config.py i w diffie."""
    pad = ' ' * wciecie
    wiersze = [pad + ', '.join(f'"{x}"' for x in items[i:i + 6]) + ','
               for i in range(0, len(items), 6)]
    return f'    "{nazwa}": [\n' + '\n'.join(wiersze) + f'\n    ],'


def check_z2m(items):
    """NA G1: czy każda nazwa z listy istnieje w zigbee2mqtt tej bramki."""
    sys.path.insert(0, '/home/td/meshtastic')
    try:
        import config as c
        from modules.data.z2m_reader import Z2MReader          # noqa: F401
    except Exception as e:
        print(f'❌ uruchom to NA BRAMCE G1 (brak config/modules): {e}')
        return 1
    import paho.mqtt.client as mqtt

    m = c.CONFIG['mqtt']
    znalezione = set()

    def on_msg(_c, _u, msg):
        try:
            for d in json.loads(msg.payload.decode()):
                if d.get('friendly_name'):
                    znalezione.add(d['friendly_name'])
        except Exception:
            pass

    cli = mqtt.Client(client_id='g1-monitored-check')
    cli.username_pw_set(m['user'], m['pass'])
    cli.on_message = on_msg
    cli.connect(m['host'], m['port'], 20)
    cli.subscribe('zigbee2mqtt/bridge/devices')
    cli.loop_start()
    import time
    time.sleep(6)
    cli.loop_stop()

    if not znalezione:
        print('❌ brak odpowiedzi z zigbee2mqtt/bridge/devices — czy z2m działa?')
        return 1

    brak = [x for x in items if x not in znalezione]
    nadmiar = sorted(n for n in znalezione
                     if (n.startswith('RA') or n.startswith('RB')) and n not in items)
    print(f'z2m widzi {len(znalezione)} urządzeń; lista docelowa: {len(items)}')
    print(f'✅ obecnych z listy : {len(items) - len(brak)}')
    if brak:
        print(f'❌ BRAK w z2m ({len(brak)}): {brak}')
    if nadmiar:
        print(f'ℹ️  przekaźniki w z2m spoza listy ({len(nadmiar)}): {nadmiar}')
    if not brak:
        print('✅ komplet — listę można wpisać do config.py')
    return 0 if not brak else 2


if __name__ == '__main__':
    items = lista()
    if '--check' in sys.argv:
        sys.exit(check_z2m(items))
    if '--whitelist' in sys.argv:
        print('# supervisor config.py → CONFIG["mirror_whitelist"]["G1"]')
        print(snippet('G1', items, wciecie=12))
    else:
        print(f'# G1 monitored — {len(items)} przekaźników '
              f'(RA1-{RA_MAX} + RB1-{RB_MAX}, po {len(KANALY)} kanały)')
        print(snippet('monitored', items))
