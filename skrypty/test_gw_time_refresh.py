#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sprawdzian reguly z `refresh_gw_time` (Meshtastic_SCADA.py, run_supervisor).

Pilnuje jednej rzeczy: kafel „Tryb — stan" ma sie zmieniac o zmierzchu SAM, bez heartbeata,
bo bramka nocna nie nadaje HB przez cala noc. Reguła dla day/night to czysta funkcja
wschodu/zachodu — ta sama, ktora liczy GatewayMode._is_daytime na bramce.
Uruchom: venv/bin/python test_gw_time_refresh.py
"""
import time, calendar
from modules.gateway_mode import GatewayMode, mode_active

aktywna = mode_active                                      # reguła ma JEDNO źródło: modules/gateway_mode


def _ts(h, m=0):
    lt = list(time.localtime()); lt[3], lt[4], lt[5] = h, m, 0
    return time.mktime(time.struct_time(tuple(lt)))


def main():
    # carport: bramka NOCNA — w dzien wstrzymana, po zmierzchu aktywna
    assert aktywna('night', True) is False
    assert aktywna('night', False) is True
    assert aktywna('day', True) is True and aktywna('day', False) is False
    assert aktywna('all-time', True) is True
    assert aktywna('calendar', True, last=True) is True   # bez HB trzymamy ostatnia znana
    assert aktywna('calendar', True, last=False) is False

    # zmierzch faktycznie przelacza is_day (wspolrzedne carportu, solar)
    gm = GatewayMode(operating_mode='night', lat=52.2, lon=21.0)
    d = gm.day_info()
    sr, ss = d['sunrise_min'], d['sunset_min']
    assert 0 < sr < ss < 24 * 60, (sr, ss)
    assert gm.day_info(_ts(sr // 60, sr % 60 + 5))['is_day'] is True
    assert gm.day_info(_ts(ss // 60, ss % 60))['is_day'] is False      # minuta zmierzchu = juz noc
    # a w tej samej dobie kafel przechodzi wstrzymana → aktywna bez zadnego HB
    assert aktywna('night', gm.day_info(_ts(12))['is_day']) is False
    assert aktywna('night', gm.day_info(_ts(23))['is_day']) is True
    print('OK: 10/10  (swit %02d:%02d, zmierzch %02d:%02d)' % (sr // 60, sr % 60, ss // 60, ss % 60))


if __name__ == '__main__':
    main()
