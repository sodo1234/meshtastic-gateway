#!/usr/bin/env python3
"""Dodaje do automatyzacji motion warunek: czujka OFFLINE lub lqi=0 → jej stan NIE brany
pod uwagę. Zastępuje warunki OR (dowolna on) / AND (wszystkie off) w gałęziach `choose`
szablonem z pętlą, który liczy tylko czujki online (state on/off) + lqi>0.

Zachowuje 1:1: triggery, warunki (noc + motion_enabled), akcje (switche), id/alias.
Uruchom: python add_offline_lqi_filter.py  (in-place na motion_*.yaml w tym katalogu)
"""
import glob
import os
import yaml

SC = os.path.dirname(os.path.abspath(__file__))


def sensor_list_from_actions(choose_branch):
    """Wyciąga entity_id czujek z warunków OR/AND gałęzi choose."""
    conds = choose_branch.get("conditions", [])
    if conds and isinstance(conds[0], dict) and "conditions" in conds[0]:
        return [c["entity_id"] for c in conds[0]["conditions"] if "entity_id" in c]
    return []


def any_on_template(sensors):
    """Jinja: true gdy DOWOLNA czujka online+lqi>0 jest 'on' (offline/lqi0 pominięte)."""
    lst = "[" + ", ".join("'%s'" % s for s in sensors) + "]"
    return (
        "{% set ss = " + lst + " %}\n"
        "{% set ns = namespace(any_on=false) %}\n"
        "{% for s in ss %}\n"
        "  {% set lqi = 'sensor.' ~ s.split('.')[1] | replace('_occupancy','') ~ '_lqi' %}\n"
        "  {% if states(s) == 'on' and states(lqi) | int(0) > 0 %}{% set ns.any_on = true %}{% endif %}\n"
        "{% endfor %}\n")


def transform(path):
    with open(path, encoding="utf-8") as f:
        auto = yaml.safe_load(f)
    actions = auto.get("actions", [])
    if not actions or "choose" not in actions[0]:
        return False
    branches = actions[0]["choose"]
    if len(branches) < 2:
        return False
    sensors = sensor_list_from_actions(branches[0])
    if not sensors:
        return False
    tmpl = any_on_template(sensors)
    # gałąź 0 = ZAPAL strefę gdy dowolna czujka (online+lqi>0) on
    branches[0]["conditions"] = [{"condition": "template",
                                  "value_template": tmpl + "{{ ns.any_on }}"}]
    # gałąź 1 = ZGAŚ strefę gdy ŻADNA online+lqi>0 czujka nie jest on (offline pominięte → gaśnie)
    branches[1]["conditions"] = [{"condition": "template",
                                  "value_template": tmpl + "{{ not ns.any_on }}"}]
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(auto, f, allow_unicode=True, sort_keys=False, default_flow_style=False, width=4096)
    return True


if __name__ == "__main__":
    n = 0
    for p in sorted(glob.glob(SC + "/motion_*.yaml")):
        if transform(p):
            print("OK", os.path.basename(p))
            n += 1
        else:
            print("SKIP", os.path.basename(p))
    print("zmodyfikowano:", n)
