#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Zbuduj dashboard „LoRa SCADA" dla G1 z działającego wzorca G2 (2026-08-13).

PO CO: zasada projektu — „wszystko co umie supervisor, bramka MUSI umieć LOKALNIE na swoim HA",
a dashboard bramki to kod supervisora 1:1 z podmianą encji. G1 jako JEDYNA nie miała u siebie
widoku SCADA (ma tylko `carport-dashboard` z przekaźnikami i ruchem).

CO PORTUJEMY: widoki SYSTEMOWE — „Bramka", „Harmonogram", „Parametry". Ich encje to `gw_g1_*`
/ `lora_g1_*`, więc mapują się 1:1. Widoki „Sterowanie" i „Pomiary" z G2 celowo POMIJAMY:
odwołują się wyłącznie do urządzeń testbedu (Test 1/2, Temp 1/2, Leak 1, Door 1), których na
carporcie nie ma — a odpowiedniki G1 (78 przekaźników + 82 czujniki ruchu) już żyją w
`carport-dashboard`, którego NIE dotykamy.

DLACZEGO GLOBALNA PODMIANA g2→g1 JEST BEZPIECZNA: policzone na wzorcu — wszystkie wystąpienia
to tożsamość bramki (identyfikatory encji `lora_gateway_g2_gw_g2_*`, topiki `lora/g2/...`,
domyślka JS `gw||'G2'`, etykiety „BRAMKA G2"). Żadne `g2` nie znaczy tam nic innego.

Uruchamiaj LOKALNIE (buduje plik), potem wgraj przez `apply_dashboard_g1.py` NA G1.

    python homeassistant/port_scada_dashboard_g1.py <g2_dump.json> <g1_entities.json> <out.json>
"""
import json
import re
import sys

PORTOWANE = ('scada-gw', 'scada-cal', 'scada-param')   # path widoków systemowych


def podmien(blob: str) -> str:
    """g2→g1 w każdej postaci, w jakiej tożsamość bramki występuje we wzorcu."""
    blob = blob.replace('gw_g2', 'gw_g1').replace('lora_g2', 'lora_g1')
    blob = blob.replace('gateway_g2', 'gateway_g1').replace('lora/g2/', 'lora/g1/')
    blob = blob.replace('an_g2', 'an_g1').replace('supg2', 'supg1')
    blob = blob.replace("'G2'", "'G1'").replace('"G2"', '"G1"')
    blob = blob.replace('G2</span', 'G1</span').replace('BRAMKA G2', 'BRAMKA G1')
    blob = blob.replace('BRAMKI G2', 'BRAMKI G1').replace('Bramka G2', 'Bramka G1')
    blob = re.sub(r'\bg2\b', 'g1', blob)
    return blob


def main():
    src, ents_path, out = sys.argv[1], sys.argv[2], sys.argv[3]
    cfg = json.load(open(src, encoding='utf-8'))
    istnieje = set(json.load(open(ents_path, encoding='utf-8')))

    widoki = [v for v in cfg.get('views', []) if v.get('path') in PORTOWANE]
    print(f'portuję {len(widoki)}/{len(cfg.get("views", []))} widoków:',
          [v.get('path') for v in widoki])

    nowe = json.loads(podmien(json.dumps({'views': widoki}, ensure_ascii=False)))['views']

    # Pokrycie: każdy identyfikator encji z wynikowego configu musi istnieć na G1.
    # `domena.czasownik` to WYWOŁANIE SERWISU (np. button.press w tap_action), nie encja —
    # bez tego filtra raport pokazuje fałszywy brak.
    SERWISY = {'press', 'turn_on', 'turn_off', 'toggle', 'select_option', 'set_value', 'reload'}
    ref, brak = set(), set()
    for v in nowe:
        for tok in json.dumps(v, ensure_ascii=False).split('"'):
            if re.fullmatch(r'(sensor|binary_sensor|switch|button|select|number|calendar)\.[a-z0-9_]+', tok):
                if tok.split('.', 1)[1] not in SERWISY:
                    ref.add(tok)
    for e in sorted(ref):
        if e not in istnieje:
            brak.add(e)

    print(f'encji w dashboardzie: {len(ref)} | istnieje na G1: {len(ref) - len(brak)} | BRAK: {len(brak)}')
    for e in sorted(brak):
        print('   BRAK:', e)          # ASCII — konsola Windows (cp1250) nie zniesie znaczków

    json.dump({'views': nowe}, open(out, 'w', encoding='utf-8'), ensure_ascii=False)
    print('zapisano ->', out)
    return 1 if brak else 0


if __name__ == '__main__':
    sys.exit(main())
