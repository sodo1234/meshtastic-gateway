"""Carport — widok ALARMY (mechanizm anomalii, HACS-free, deliverable YAML).

Anomalie hali carport = DOSTĘPNOŚĆ urządzeń (z2m availability → encja `unavailable`).
Bramka carport HA nie publikuje sensorów anomalii (brak lora_*_an_*), a deliverable to
sam dashboard-YAML (nie ruszamy gatewaya/z2m) → anomalie liczone po stronie dashboardu:

  1) PODSUMOWANIE — custom:button-card z licznikiem offline (JS po liście encji),
     zielone gdy 0, czerwone gdy >0. Osobno przekaźniki i czujniki ruchu.
  2) LISTY OFFLINE — core `entity-filter` (BEZ HACS): auto-kurczy się do samych
     `unavailable`; `show_empty: false` chowa kartę gdy wszystko OK.

Encje wyciągane WPROST z carport_dashboard.styled.yaml (źródło prawdy tego co wdrożone).
Reuzywa button_card_templates (lora_hdr/lora_base) z tego samego dashboardu po wklejeniu.

Output: views_anomaly.yaml — wklej wpis do listy `views:` (raw config editor).
"""
import os, re, yaml

SC = os.path.dirname(os.path.abspath(__file__))
STYLED = SC + "/carport_dashboard.styled.yaml"

txt = open(STYLED, encoding="utf-8").read()

# entity_id realnych urządzeń hali (pomijamy wirtualne lora_* bramki)
def uniq(pat):
    seen, out = set(), []
    for m in re.findall(pat, txt):
        if m not in seen:
            seen.add(m); out.append(m)
    return out

relays = [e for e in uniq(r"switch\.[a-z0-9_]+") if not e.startswith("switch.lora_")]
motion = [e for e in uniq(r"binary_sensor\.[a-z0-9_]+_occupancy") if not e.startswith("binary_sensor.lora_")]

print("relays:", len(relays), "| motion:", len(motion))


def js_count(ids):
    arr = "[" + ",".join("'%s'" % i for i in ids) + "]"
    return ("[[[ var a=%s; var n=a.filter(function(i){var e=states[i]; "
            "return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n; ]]]" % arr)


def hdr(text):
    return {"type": "custom:button-card", "template": "lora_hdr", "name": text,
            "layout_options": {"grid_columns": 4}}


def count_tile(name, ids, ok_color, icon):
    n_js = js_count(ids)
    total = len(ids)
    return {
        "type": "custom:button-card", "template": "lora_base",
        "name": "%s (0-%d)" % (name, total),
        "show_state": False, "show_label": True,
        "label": n_js,
        "icon": icon,
        "tap_action": {"action": "none"},
        "styles": {
            "card": [{"height": "70px"}, {"padding": "10px 12px"},
                     {"border": "[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'1px solid #f87171':'1px solid #1f1f1f'; ]]]"
                      % ("[" + ",".join("'%s'" % i for i in ids) + "]")}],
            "icon": [{"width": "24px"}, {"height": "24px"},
                     {"color": "[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'#f87171':'%s'; ]]]"
                      % ("[" + ",".join("'%s'" % i for i in ids) + "]", ok_color)}],
            "name": [{"font-size": "10px"}, {"font-weight": 700}, {"color": "#e5e5e5"},
                     {"white-space": "nowrap"}, {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
            "label": [{"font-size": "22px"}, {"font-weight": 800},
                      {"color": "[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'#f87171':'%s'; ]]]"
                       % ("[" + ",".join("'%s'" % i for i in ids) + "]", ok_color)}],
        },
    }


def status_tile(all_ids):
    arr = "[" + ",".join("'%s'" % i for i in all_ids) + "]"
    lab = ("[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?('ALERT — '+n+' offline'):'WSZYSTKO OK'; ]]]" % arr)
    col = ("[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'#f87171':'#4ade80'; ]]]" % arr)
    return {
        "type": "custom:button-card", "template": "lora_base",
        "name": "STATUS SYSTEMU", "show_state": False, "show_label": True,
        "label": lab, "tap_action": {"action": "none"},
        "icon": "[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'mdi:alert':'mdi:check-circle'; ]]]" % arr,
        "styles": {
            "card": [{"height": "70px"}, {"padding": "10px 12px"},
                     {"border": "[[[ var a=%s; var n=a.filter(function(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}).length; return n>0?'1px solid #f87171':'1px solid #14532d'; ]]]" % arr}],
            "icon": [{"width": "24px"}, {"height": "24px"}, {"color": col}],
            "name": [{"font-size": "10px"}, {"font-weight": 700}, {"color": "#9ca3af"}],
            "label": [{"font-size": "13px"}, {"font-weight": 800}, {"color": col}],
        },
    }


def offline_filter(title, ids, card_type="entities"):
    """core entity-filter — pokazuje TYLKO unavailable/unknown; chowa kartę gdy pusto."""
    return {
        "type": "entity-filter",
        "entities": ids,
        "conditions": [{"condition": "state", "state": ["unavailable", "unknown"]}],
        "show_empty": False,
        "card": {"type": card_type, "title": title, "state_color": True},
        "layout_options": {"grid_columns": 4},
    }


view = {
    "title": "Alarmy", "path": "carport-alarms", "icon": "mdi:alert", "type": "sections",
    "sections": [
        {"type": "grid", "cards": [
            hdr("PODSUMOWANIE ANOMALII"),
            status_tile(relays + motion),
            count_tile("Przekazniki offline", relays, "#facc15", "mdi:toggle-switch-variant"),
            count_tile("Czujniki ruchu offline", motion, "#4ade80", "mdi:motion-sensor"),
        ]},
        {"type": "grid", "cards": [
            hdr("PRZEKAZNIKI OFFLINE"),
            offline_filter("Przekazniki niedostepne", relays),
        ]},
        {"type": "grid", "cards": [
            hdr("CZUJNIKI RUCHU OFFLINE"),
            offline_filter("Czujniki niedostepne", motion),
        ]},
    ],
}

out = SC + "/views_anomaly.yaml"
open(out, "w", encoding="utf-8").write(
    "# Carport — widok ALARMY (mechanizm anomalii: dostepnosc z2m, HACS-free).\n"
    "# Wklej ten wpis do listy `views:` dashboardu carport (raw config editor).\n"
    "# Wymaga: button_card_templates lora_hdr/lora_base (sa juz w styled.yaml) + z2m availability enabled.\n"
    + yaml.dump(view, allow_unicode=True, sort_keys=False, default_flow_style=False, width=10000))
print("zapisano:", out)
print("relays:", len(relays), "| motion:", len(motion), "| razem monitorowane:", len(relays) + len(motion))
