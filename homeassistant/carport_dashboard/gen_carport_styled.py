"""Carport dashboard w STYLU projektu (custom:button-card + szablony lora_*).
Zrodlo prawdy = carport_inventory.json (z2m bridge/devices: friendly_name, model,
has_battery, has_occupancy, has_state). Reuzywa button_card_templates z baseline lora-gw.

ZMIANY 2026-07-02:
- USUNIETE przekazniki z '_R' w nazwie (RA8_R*, RA11_R*, RB6_R*, RB10_R*, RA14_R1) = 25 szt.
- Kazdy kafel przekaznika/czujnika ma STOPKE: last_changed (timestamp) + bateria (czujniki) +
  availability (online/offline — stan 'unavailable' gdy z2m oznaczy offline).
"""
import os, re, json, yaml

SC = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(SC))
# ZRODLO PRAWDY = realne entity_id z zywego HA (get_states), NIE slug(friendly_name)
# — bo HA entity_id bywa rozjechany z biezaca nazwa z2m (device przemianowany po utworzeniu encji).
ent = json.load(open(SC + "/carport_real_entities.json", encoding="utf-8"))
if isinstance(ent, dict) and "result" in ent:
    ent = ent["result"]
base = json.load(open(SC + "/../dashboards_baseline/lora-gw.json", encoding="utf-8"))
TEMPLATES = base["button_card_templates"]

batt_set = set(ent["battery_ids"])
# primary przekaznik ZBMINIR2 = switch.raN_N / rbN_N / test_N (bez sufiksu-capability, bez _R)
RELAY_RE = re.compile(r"^switch\.(r[ab]\d+_\d+|test_\d+)$", re.I)
OCC_SUFFIX = re.compile(r"\s*(zaj.to\S*|occupancy)\s*$", re.I)

relays, motion = {}, {}
for eid, fn in ent["switches"]:
    if RELAY_RE.match(eid):                        # primary relay, auto-wyklucza _R i switche pomocnicze
        relays[fn.strip()] = (eid, None)
for eid, fn in ent["occupancy"]:                   # WSZYSTKIE realne czujniki ruchu (82)
    nm = OCC_SUFFIX.sub("", fn).strip()
    beid = eid.replace("binary_sensor.", "sensor.").replace("_occupancy", "_battery")
    motion[nm] = (eid, beid if beid in batt_set else None)


def row(fn):
    m = re.match(r"(R[AB]\d+)", fn)
    return m.group(1) if m else "INNE"


def nat(s):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", str(s))]


def grp(m):
    g = {}
    for name, v in m.items():
        g.setdefault(row(name), []).append((name, v))
    for k in g:
        g[k].sort(key=lambda z: nat(z[0]))
    return g


def hdr(text):
    return {"type": "custom:button-card", "template": "lora_hdr", "name": text,
            "layout_options": {"grid_columns": 4}}


def _footer_js(with_batt):
    """Stopka kafla (JS relatywny do 'entity'): 🕐 last_changed · 🔋 bateria% · ● availability.
    Bateria wyliczana z entity_id czujnika (binary_sensor.*_occupancy -> sensor.*_battery)."""
    # stopka 2-liniowa (nic nie ucinane w 1/3 szer.): linia1 🕐timestamp 📶LQ, linia2 🔋bateria ●status
    batt = "''"
    if with_batt:
        batt = ("(function(){var beid=e.entity_id.replace('binary_sensor.','sensor.').replace('_occupancy','_battery');"
                "var be=states[beid];var bs=(be&&be.state!=='unknown'&&be.state!=='unavailable')?be.state+'%':'--';"
                "return '🔋 '+bs+'  ';})()")
    return (
        "[[[ var e=entity;"
        "var slug=e?e.entity_id.replace(/^(switch|binary_sensor)\\./,'').replace(/_occupancy$/,''):'';"
        "var lqe=states['sensor.'+slug+'_lqi'];"
        "var lq=(lqe&&lqe.state!=='unknown'&&lqe.state!=='unavailable')?parseInt(lqe.state):null;"
        "var off=(!e||e.state==='unavailable'||e.state==='unknown'||lq===0);"     # LQ=0 → offline
        "var lc=e?new Date(e.last_changed):null;"
        "var t=lc?(('0'+lc.getDate()).slice(-2)+'.'+('0'+(lc.getMonth()+1)).slice(-2)+' '+"
        "('0'+lc.getHours()).slice(-2)+':'+('0'+lc.getMinutes()).slice(-2)):'--';"
        "var lqs=(lq!=null)?lq:'--';"
        "var dot=off?'<span style=\"color:#f87171;font-weight:800;\">● OFFLINE</span>'"
        ":'<span style=\"color:#4ade80;\">● online</span>';"
        "return '<div style=\"display:flex;flex-direction:column;gap:2px;font-size:9px;"
        "color:#737373;line-height:1.3;white-space:nowrap;\">'"
        "+'<span>🕐 '+t+'   📶 '+lqs+'</span>'"
        "+'<span>'+" + batt + "+dot+'</span></div>'; ]]]")


def _tile_template(oncolor, icon, with_batt, tap):
    """Szablon button-card reuzywalny przez wszystkie kafle danego typu (DRY, maly payload)."""
    return {
        "template": "lora_base", "show_state": False, "show_label": False,
        "tap_action": tap, "icon": icon,
        "custom_fields": {"info": _footer_js(with_batt)},
        "styles": {
            "card": [{"height": "90px"}, {"padding": "9px 12px"},
                     {"border": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')return '1px solid #f87171'; return e.state==='on'?'1px solid %s':'1px solid #1f1f1f'; ]]]" % oncolor}],
            "grid": [{"grid-template-areas": "\"i n\" \"i info\""},
                     {"grid-template-columns": "24px 1fr"}, {"grid-template-rows": "auto auto"},
                     {"align-items": "center"}, {"column-gap": "10px"}, {"row-gap": "3px"}],
            "icon": [{"width": "22px"}, {"height": "22px"},
                     {"color": "[[[ var e=entity; if(!e||e.state==='unavailable'||e.state==='unknown')return '#f87171'; return e.state==='on'?'%s':'#525252'; ]]]" % oncolor}],
            "name": [{"font-size": "11px"}, {"font-weight": 700}, {"color": "#e5e5e5"},
                     {"justify-self": "start"}, {"white-space": "nowrap"}, {"overflow": "hidden"}, {"text-overflow": "ellipsis"}],
            "custom_fields": {"info": [{"justify-self": "start"}]},
        },
    }


# szablony rejestrowane w button_card_templates
CARPORT_TEMPLATES = {
    "carport_relay": _tile_template("#facc15",
        "[[[ return entity.state==='on'?'mdi:lightbulb-on':'mdi:lightbulb-off-outline'; ]]]",
        False, {"action": "toggle"}),
    "carport_motion": _tile_template("#4ade80", "mdi:motion-sensor", True, {"action": "more-info"}),
}


def relay_tile(name, v):
    return {"type": "custom:button-card", "template": "carport_relay", "entity": v[0], "name": name}


def motion_tile(name, v):
    return {"type": "custom:button-card", "template": "carport_motion", "entity": v[0], "name": name}


def stat_tile(name, eid):
    return {"type": "custom:button-card", "template": "lora_stat", "entity": eid, "name": name}


def device_section(title, items, tile_fn):
    # naglowek pelna szerokosc + kafle po 3 obok siebie (1/3 szerokosci)
    tiles = {"type": "grid", "columns": 3, "square": False,
             "cards": [tile_fn(n, v) for n, v in items]}
    return {"type": "grid", "cards": [hdr("%s (%d)" % (title, len(items))), tiles]}


rel = grp(relays); mot = grp(motion)
ALL_TEMPLATES = dict(TEMPLATES); ALL_TEMPLATES.update(CARPORT_TEMPLATES)
dash = {"title": "Carport", "button_card_templates": ALL_TEMPLATES, "views": []}

# Widok 1: Bramka
gw_stats = [("Synced", "binary_sensor.lora_gw_g1_synced"), ("Tryb pracy", "sensor.lora_gw_g1_operating_mode"),
            ("Tryb sync", "sensor.lora_gw_g1_sync_mode"), ("Uptime", "sensor.lora_gw_g1_uptime"),
            ("TX count", "sensor.lora_gw_g1_tx_count"), ("Batcher kolejka", "sensor.lora_gw_g1_batcher_pending"),
            ("Batcher wyslane", "sensor.lora_gw_g1_batcher_flushed")]
gw_time = [("Czas SCADA", "sensor.lora_gw_g1_scada_time"), ("Czas systemowy", "sensor.lora_gw_g1_system_time"),
           ("Offset", "sensor.lora_gw_g1_time_offset")]
gw_param = [("P1", "number.lora_gw_g1_param_p1"), ("P2", "number.lora_gw_g1_param_p2"), ("P3", "number.lora_gw_g1_param_p3")]
dash["views"].append({"title": "Bramka", "path": "carport-gw", "icon": "mdi:radio-tower", "type": "sections",
    "sections": [
        {"type": "grid", "cards": [hdr("STATUS BRAMKI G1")] + [stat_tile(n, e) for n, e in gw_stats]},
        {"type": "grid", "cards": [hdr("CZAS / SYNCHRONIZACJA")] + [stat_tile(n, e) for n, e in gw_time]},
        {"type": "grid", "cards": [hdr("PARAMETRY P1-P3")] + [stat_tile(n, e) for n, e in gw_param]},
    ]})

# Widok 2: Harmonogram
dash["views"].append({"title": "Harmonogram", "path": "carport-schedule", "icon": "mdi:calendar-clock", "type": "sections",
    "sections": [
        {"type": "grid", "cards": [hdr("HARMONOGRAM PRODUKCJI"),
            {"type": "calendar", "entities": ["calendar.lora_g1"], "initial_view": "dayGridMonth",
             "layout_options": {"grid_columns": 4}}]},
        {"type": "grid", "cards": [hdr("TRYB BIEZACY"),
            stat_tile("Tryb pracy", "sensor.lora_gw_g1_operating_mode"),
            stat_tile("Tryb sync", "sensor.lora_gw_g1_sync_mode")]},
    ]})

# Widok 3: Przekazniki (bez _R)
dash["views"].append({"title": "Przekazniki", "path": "carport-relays", "icon": "mdi:toggle-switch-variant",
    "type": "sections", "sections": [device_section(k, rel[k], relay_tile) for k in sorted(rel, key=nat)]})

# Widok 4: Czujniki ruchu
dash["views"].append({"title": "Czujniki ruchu", "path": "carport-motion", "icon": "mdi:motion-sensor",
    "type": "sections", "sections": [device_section(k, mot[k], motion_tile) for k in sorted(mot, key=nat)]})

out = SC + "/carport_dashboard.styled.yaml"
open(out, "w", encoding="utf-8").write(
    "# Carport dashboard — STYL PROJEKTU (custom:button-card, dark #0a0a0a).\n"
    "# Przekazniki _R USUNIETE. Kazdy kafel: timestamp + bateria(czujniki) + availability.\n"
    "# Import: raw config editor -> wklej calosc.\n"
    + yaml.dump(dash, allow_unicode=True, sort_keys=False, default_flow_style=False, width=10000))
json.dump(dash, open(SC + "/carport_dash_styled.json", "w", encoding="utf-8"), ensure_ascii=False)
n_relay = sum(len(v) for v in rel.values()); n_mot = sum(len(v) for v in mot.values())
print("zapisano:", out)
print("przekazniki (bez _R):", n_relay, "| czujniki ruchu:", n_mot, "| relay rows:", len(rel), "| motion rows:", len(mot))
