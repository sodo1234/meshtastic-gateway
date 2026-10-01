#!/usr/bin/env python3
"""Instrukcja obsługi / runbook LoRa SCADA jako PPTX — styl spójny z gen_presentation.py
(dark #0a0a0a, cyjan #22d3ee). Źródło treści: docs/RUNBOOK.md. Odpal: python gen_runbook_deck.py"""
import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

BG    = RGBColor(0x0a, 0x0a, 0x0a); CARD = RGBColor(0x14, 0x14, 0x14)
CYAN  = RGBColor(0x22, 0xd3, 0xee); GREEN = RGBColor(0x4a, 0xde, 0x80)
AMBER = RGBColor(0xfb, 0xbf, 0x24); RED = RGBColor(0xf8, 0x71, 0x71)
WHITE = RGBColor(0xe5, 0xe5, 0xe5); GREY = RGBColor(0x73, 0x73, 0x73)
DGREY = RGBColor(0x52, 0x52, 0x52); LINE = RGBColor(0x1f, 0x1f, 0x1f)

prs = Presentation(); prs.slide_width = Inches(13.333); prs.slide_height = Inches(7.5)
SW, SH = prs.slide_width, prs.slide_height; BLANK = prs.slide_layouts[6]
_n = [0]


def slide():
    s = prs.slides.add_slide(BLANK)
    bg = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, SW, SH)
    bg.fill.solid(); bg.fill.fore_color.rgb = BG; bg.line.fill.background(); bg.shadow.inherit = False
    return s


def box(s, x, y, w, h, fill=None, line=None, lw=1):
    sh = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    if fill is None: sh.fill.background()
    else: sh.fill.solid(); sh.fill.fore_color.rgb = fill
    if line is None: sh.line.fill.background()
    else: sh.line.color.rgb = line; sh.line.width = Pt(lw)
    sh.shadow.inherit = False; return sh


def txt(s, x, y, w, h, runs, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, sp=1.0):
    tb = s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True; tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.06); tf.margin_top = tf.margin_bottom = Inches(0.02)
    first = True
    for line in runs:
        p = tf.paragraphs[0] if first else tf.add_paragraph(); first = False
        p.alignment = align; p.line_spacing = sp; p.space_after = Pt(2)
        for (t, sz, col, bold) in line:
            r = p.add_run(); r.text = t; r.font.size = Pt(sz); r.font.color.rgb = col
            r.font.bold = bold; r.font.name = "Consolas"
    return tb


def header(s, kicker, title):
    box(s, 0.5, 0.45, 0.12, 0.55, fill=CYAN)
    txt(s, 0.75, 0.42, 12, 0.35, [[(kicker, 11, DGREY, True)]])
    txt(s, 0.73, 0.66, 12.2, 0.7, [[(title, 26, WHITE, True)]])
    _n[0] += 1
    txt(s, 0.75, 7.0, 8, 0.3, [[("LoRa Zigbee SCADA — Instrukcja obsługi", 9, DGREY, False)]])
    txt(s, 11.8, 7.0, 1.2, 0.3, [[(str(_n[0]), 9, DGREY, True)]], align=PP_ALIGN.RIGHT)


def table(s, x, y, colw, headers, rows, hcol=CYAN, fs=11, hfs=11, rh=0.42, sp=1.05):
    """Prosta tabela: colw=[szerokości kolumn], nagłówek + wiersze. Auto-wysokość wiersza rh."""
    total = sum(colw)
    box(s, x - 0.12, y - 0.12, total + 0.24, rh * (len(rows) + 1) + 0.3, fill=CARD, line=LINE)
    cx = x
    for j, hd in enumerate(headers):
        txt(s, cx, y, colw[j], rh, [[(hd, hfs, hcol, True)]], sp=sp)
        cx += colw[j]
    box(s, x, y + rh - 0.03, total, 0.02, fill=LINE)
    for i, row in enumerate(rows):
        cx = x; ry = y + rh * (i + 1) + 0.05
        for j, cell in enumerate(row):
            col = WHITE
            if isinstance(cell, tuple): cell, col = cell
            txt(s, cx, ry, colw[j], rh, [[(cell, fs, col, False)]], sp=sp)
            cx += colw[j]


def bullets(s, x, y, w, items, fs=13, mark="▸", mcol=CYAN, sp=1.3):
    rows = []
    for it in items:
        if isinstance(it, tuple):
            rows.append([(mark + "  ", fs, mcol, True), (it[0], fs, WHITE, True), ("  " + it[1], fs, GREY, False)])
        else:
            rows.append([(mark + "  ", fs, mcol, True), (it, fs, WHITE, False)])
    txt(s, x, y, w, 5.0, rows, sp=sp)


# ══════════════════ 1. TYTUŁ ══════════════════
s = slide()
box(s, 0.9, 2.3, 0.16, 2.2, fill=CYAN)
txt(s, 1.3, 2.2, 11, 0.5, [[("LoRa ZIGBEE SCADA", 15, CYAN, True)]])
txt(s, 1.28, 2.75, 11.5, 1.5, [[("Instrukcja obsługi", 46, WHITE, True)],
                               [("Uruchamianie skryptów · obsługa dashboardu", 20, GREY, False)]])
txt(s, 1.3, 4.9, 11, 0.9, [[("Runbook operacyjny — rozproszony BMS Zigbee → LoRa → Supervisor → Home Assistant", 15, WHITE, False)],
                           [("Bramka = źródło prawdy · supervisor agreguje · każda bramka autonomiczna", 13, DGREY, False)]])
txt(s, 1.3, 6.7, 11, 0.4, [[("2026-07-21  ·  kod step-by-step (Meshtastic_SCADA.py + modules/*)  ·  NIE legacy v38", 11, DGREY, True)]])

# ══════════════════ 2. AGENDA ══════════════════
s = slide(); header(s, "PRZEGLĄD", "Zakres instrukcji")
col1 = ["Architektura i maszyny (GW / SUP / hosty Tailscale)",
        "Wymagania wstępne (SSH, venv, tokeny HA, MQTT)",
        "Uruchamianie skryptów — Launcher :8765 + fallback SSH/tmux",
        "Wdrożenie dashboardu (gen_unified_dashboard.py)"]
col2 = ["Obsługa dashboardu — 7 zakładek",
        "Tryb bramki (sync z obu stron) + Parametry (Send)",
        "Alarmy / anomalie / Clear",
        "Health-check + Troubleshooting + pliki stanu"]
box(s, 0.75, 1.7, 6.0, 4.6, fill=CARD, line=LINE); bullets(s, 1.0, 2.0, 5.5, col1)
box(s, 7.05, 1.7, 5.55, 4.6, fill=CARD, line=LINE); bullets(s, 7.3, 2.0, 5.1, col2)

# ══════════════════ 3. ARCHITEKTURA ══════════════════
s = slide(); header(s, "SYSTEM", "Architektura i maszyny")
def node(x, y, w, h, title, sub, col):
    box(s, x, y, w, h, fill=CARD, line=col)
    txt(s, x, y + 0.12, w, 0.4, [[(title, 14, col, True)]], align=PP_ALIGN.CENTER)
    txt(s, x, y + 0.55, w, h - 0.5, [[(l, 10, WHITE, False)] for l in sub], align=PP_ALIGN.CENTER, sp=1.05)
node(4.7, 1.6, 3.9, 1.15, "SUPERVISOR (G0)", ["Xeon · HA Supervised", "agregacja wszystkich bramek"], CYAN)
txt(s, 4.7, 2.82, 3.9, 0.35, [[("▲  LoRa 868 MHz · 220B · 3s  ▼", 11, AMBER, True)]], align=PP_ALIGN.CENTER)
for i, (x, nm, sub, col) in enumerate([
        (0.95, "BRAMKA G1", ["Z2M + HA lokalne", "~100 dev · autonomiczna"], GREEN),
        (4.9, "BRAMKA G2", ["testbed (dom)", "Heltec V3 SX1262"], DGREY),
        (8.85, "BRAMKA G3", ["Z2M + HA lokalne", "~100 dev"], DGREY)]):
    node(x, 3.35, 3.5, 1.15, nm, sub, col)
table(s, 0.95, 5.05, [3.0, 2.6, 3.0, 3.0],
      ["Rola", "Nazwa (launcher)", "IP Tailscale", "Uwaga"],
      [["Bramka produkcyjna", "carport-g1", "100.93.121.31", ("REALNY site", AMBER)],
       ["Bramka testbed", "carport-g2", "100.98.155.78", ("do testów", GREEN)],
       ["Supervisor", "supervisor", "100.79.111.24", "G0"]], fs=11, rh=0.4)

# ══════════════════ 4. URUCHAMIANIE — LAUNCHER ══════════════════
s = slide(); header(s, "URUCHAMIANIE", "Skrypty przez Launcher (zalecane)")
box(s, 0.75, 1.6, 6.0, 5.0, fill=CARD, line=LINE)
txt(s, 1.0, 1.75, 5.5, 0.4, [[("WEB UI  ·  http://localhost:8765", 13, CYAN, True)]])
bullets(s, 1.0, 2.35, 5.5, [
    ("1. Host", "carport-g2 / supervisor / …"),
    ("2. Skrypt", "Meshtastic_SCADA.py"),
    ("3. Start", "rola gw (bramka) / sup (supervisor)"),
    ("4. tail", "podgląd logu na żywo (nowa karta)"),
], fs=12, sp=1.5)
txt(s, 1.0, 5.4, 5.5, 1.0, [[("Launcher robi robust-kill + weryfikację —", 10, GREY, False)],
                            [("nie kłamie, że stopped, gdy proces żyje.", 10, GREY, False)]], sp=1.15)
box(s, 7.05, 1.6, 5.55, 5.0, fill=CARD, line=LINE)
txt(s, 7.3, 1.75, 5.1, 0.4, [[("API (automatyzacja)", 13, CYAN, True)]])
code = ["# start bramki G2",
        "curl -X POST :8765/api/start \\",
        "  -d '{\"host\":\"carport-g2\",",
        "       \"role\":\"gw\",",
        "       \"script\":\"Meshtastic_SCADA.py\"}'",
        "",
        "# status operacji (async → poll)",
        "curl :8765/api/op?host=carport-g2&role=gw",
        "  → {\"busy\":false,\"ok\":true,",
        "     \"result\":\"started ...\"}",
        "",
        "# stop",
        "curl -X POST :8765/api/stop -d ..."]
txt(s, 7.3, 2.35, 5.1, 4.2, [[(l if l else " ", 10, (GREEN if l.startswith("#") else WHITE), False)] for l in code], sp=1.1)

# ══════════════════ 5. URUCHAMIANIE — FALLBACK SSH/TMUX ══════════════════
s = slide(); header(s, "URUCHAMIANIE", "Fallback — SSH + tmux (gdy launcher pada)")
box(s, 0.75, 1.6, 11.85, 3.5, fill=CARD, line=LINE)
sshc = ["ssh td@100.98.155.78                 # carport-g2  (user td, klucz)",
        "cd ~/meshtastic",
        "tmux new -d -s gw 'venv/bin/python -u Meshtastic_SCADA.py 2>>/tmp/test_transport.stderr.log'",
        "tmux has-session -t gw && echo UP     # weryfikacja startu",
        "tail -f /tmp/test_transport.log       # log runtime",
        "",
        "# STOP:",
        "tmux kill-session -t gw ; pkill -f 'python -u Meshtastic_SCADA.py'"]
txt(s, 1.0, 1.8, 11.4, 3.2, [[(l if l else " ", 12, (GREEN if l.strip().startswith("#") else WHITE), False)] for l in sshc], sp=1.25)
txt(s, 0.75, 5.35, 11.85, 1.4, [
    [("Sesje tmux:  gw  (bramka)  ·  sup  (supervisor).   Log skryptów test_*: /tmp/test_transport.log", 12, CYAN, True)],
    [("Deploy kodu: scp pliku do ~/meshtastic/… → ast-check → restart roli z launchera.", 12, WHITE, False)],
    [("NIE deployować config.py z repo — remote ma prawdziwe tokeny/listy monitored/priority/porty.", 12, AMBER, True)],
], sp=1.35)

# ══════════════════ 6. DASHBOARD — WDROŻENIE + ZAKŁADKI ══════════════════
s = slide(); header(s, "DASHBOARD", "Wdrożenie + 7 zakładek (lora-scada)")
box(s, 0.75, 1.6, 11.85, 0.95, fill=CARD, line=LINE)
txt(s, 1.0, 1.72, 11.4, 0.8, [
    [("Wdrożenie (NA maszynie, osobno GW i SUP): ", 12, CYAN, True),
     ("venv/bin/python homeassistant/gen_unified_dashboard.py", 12, WHITE, False)],
    [("→ Lovelace url_path=lora-scada. Encje z MQTT Discovery (retained), dashboard tylko układa.", 11, GREY, False)],
], sp=1.2)
table(s, 0.95, 3.0, [2.9, 1.9, 6.4],
      ["Zakładka", "path", "Do czego"],
      [[("Bramka {GW}", CYAN), "scada-gw", "widok-wzorzec bramki (stan, urządzenia)"],
       [("Sterowanie", CYAN), "scada-ctl", "switche / światła, przyciski sterujące"],
       [("Pomiary", CYAN), "scada-meas", "kafle temp / wilgotność / bateria + wykresy"],
       [("Alarmy", CYAN), "scada-alarm", "lista anomalii + Clear (offline / bateria / inne)"],
       [("Harmonogram", CYAN), "scada-cal", "kalendarz produkcji + tryb bramki"],
       [("Parametry", CYAN), "scada-param", "P / T / Progi + przyciski Send"],
       [("Diagnostyka", CYAN), "scada-diag", "HB, czas, hashe zgodności, link supervisora"]],
      fs=11, rh=0.4)

# ══════════════════ 7. POMIARY ══════════════════
s = slide(); header(s, "DASHBOARD · POMIARY", "Kafle temp / wilgotność / leak / door")
box(s, 0.75, 1.6, 6.0, 5.0, fill=CARD, line=LINE)
txt(s, 1.0, 1.75, 5.5, 0.4, [[("ODŚWIEŻANIE", 13, CYAN, True)]])
bullets(s, 1.0, 2.35, 5.5, [
    "Cykliczny raport co P3 min (domyśl. 15) — NIEZALEŻNIE od progów anomalii",
    "Dodatkowo zdarzeniowo przy zmianie ≥0,5°C / ≥2%",
    "Kafle mają force_update → nie zamarzają przy stałej wartości",
    ("last_seen", "= ostatni realny raport (uczciwy wskaźnik świeżości)"),
    ("Refresh", "przycisk per-urządzenie → natychmiastowa wartość (LoRa)"),
], fs=11.5, sp=1.35)
box(s, 7.05, 1.6, 5.55, 5.0, fill=CARD, line=LINE)
txt(s, 7.3, 1.75, 5.1, 0.4, [[("ENCJE (przykład G2)", 13, GREEN, True)]])
ent = ["sensor.lora_g2_temp_1_temperature", "sensor.lora_g2_temp_1_humidity",
       "sensor.lora_g2_temp_1_battery", "binary_sensor..leak_1_water_leak",
       "binary_sensor..door_1_contact", "sensor.lora_g2_<dev>_last_seen",
       "binary_sensor..<dev>_available", "button.lora_g2_<dev>_refresh"]
txt(s, 7.3, 2.35, 5.1, 4.0, [[(e, 11, WHITE, False)] for e in ent], sp=1.5)

# ══════════════════ 8. TRYB BRAMKI ══════════════════
s = slide(); header(s, "DASHBOARD · HARMONOGRAM", "Tryb bramki — sterowany z OBU stron, synchronizowany")
table(s, 0.95, 1.7, [2.6, 2.0, 6.7],
      ["Etykieta", "Tryb", "Znaczenie"],
      [[("Całodobowa", GREEN), "all-time", "zawsze aktywna"],
       [("Dzienna", CYAN), "day", "aktywna w dzień (świt–zmierzch solarnie)"],
       [("Nocna", CYAN), "night", "aktywna w nocy"],
       [("Kalendarz", CYAN), "calendar", "aktywność wg harmonogramu produkcji"]], fs=12, rh=0.45)
box(s, 0.75, 4.4, 11.85, 2.2, fill=CARD, line=LINE)
bullets(s, 1.0, 4.6, 11.3, [
    "Zmiana działa i na bramce, i na supervisorze — round-trip przez LoRa (set_mode) + potwierdzenie w HB (gm). Oba selecty odbijają realny tryb.",
    "Monitorowane + priority (temp / hum / leak / door) raportują ZAWSZE, także gdy bramka nieaktywna.",
    "Tłumione są tylko urządzenia oświetleniowe (non-monitored) — by nie zgłaszać fałszywego offline.",
], fs=12, mcol=GREEN, sp=1.3)

# ══════════════════ 9. PARAMETRY ══════════════════
s = slide(); header(s, "DASHBOARD · PARAMETRY", "Edytuj pole → naciśnij Wyślij (bramka = master)")
table(s, 0.7, 1.65, [1.0, 3.3, 1.2, 0.9],
      ["Klucz", "Nazwa", "Domyśl.", "Jedn."],
      [["P1", "Stagnation bateryjne", "48", "h"], ["P2", "Stagnation sieciowe", "24", "h"],
       ["P3", "Raportowanie temp", "15", "min"], ["P4", "Ping sup interwał", "15", "min"],
       ["T1", "Offline switch/light", "30", "min"], ["T2", "Offline temp/hum", "60", "min"],
       ["T3", "Offline door/leak/motion", "120", "min"], ["T4", "Ping sup timeout", "25", "s"],
       ["TH/TL", "Próg temp. wys./nis.", "35/10", "°C"], ["HH/HL", "Próg wilg. wys./nis.", "70/20", "%"],
       ["BL/BC", "Próg bat. nis./kryt.", "25/15", "%"], ["PR", "Ping sup retry", "2", "x"]],
      fs=10.5, hfs=11, rh=0.365)
box(s, 7.35, 1.65, 5.25, 2.4, fill=CARD, line=LINE)
txt(s, 7.6, 1.8, 4.8, 0.4, [[("GRUPY WYSYŁKI (przyciski Send)", 12, CYAN, True)]])
bullets(s, 7.6, 2.35, 4.8, [("Wyślij Config", "P1 P2 P3 P4"),
                            ("Wyślij Timeout", "T1 T2 T3 T4"),
                            ("Wyślij Progi", "TH TL HH HL BL BC PR")], fs=12, sp=1.5)
box(s, 7.35, 4.25, 5.25, 2.35, fill=CARD, line=RGBColor(0x3a, 0x3a, 0x1f))
txt(s, 7.6, 4.4, 4.8, 2.1, [
    [("AUTO-HEAL parametrów", 12, GREEN, True)],
    [("Rozjazd mirror↔master (zgubiony pakiet)", 11, WHITE, False)],
    [("leczy się sam — supervisor dociąga", 11, WHITE, False)],
    [("wartości od bramki. Chyba że masz", 11, WHITE, False)],
    [("niewysłaną lokalną edycję → naciśnij Send.", 11, AMBER, False)],
], sp=1.2)

# ══════════════════ 10. TROUBLESHOOTING ══════════════════
s = slide(); header(s, "EKSPLOATACJA", "Troubleshooting (najczęstsze objawy)")
table(s, 0.7, 1.6, [3.6, 4.2, 4.1],
      ["Objaw", "Przyczyna", "Działanie"],
      [[("Kafle temp/leak/door zamrożone", AMBER), "bramka nieaktywna (ga=0) lub proces stanął", "sprawdź HB gm/ga; tryb Całodobowa / proces żyje"],
       ["Świeży czas, stara wartość", "pakiet b zgubiony na LoRa (avail-blob podtrzymuje)", "poczekaj cykl / Refresh; last_seen mówi prawdę"],
       ["Encja zamrożona mimo świeżego state", "brak force_update / stale-registry HA", "re-gen dashboard / republish config; ost. restart HA MQTT"],
       ["Urządzenie offline mimo że żyje", "timeout T1/T2/T3 za krótki dla bateryjnego", "podnieś T2/T3 (Parametry → Wyślij Timeout)"],
       [("ROZJAZD parametrów utrzymuje się", AMBER), "zgubiony param_upd / niewysłana edycja", "naciśnij Send (master) — auto-heal ruszy"],
       [("MCP ha-gw/ha-sup = 401", RED), "token wygasł / zrewokowany", "aktualizuj HA_TOKEN (config.py + .claude.json) + restart"],
       ["Launcher: endpointy timeoutują", "za dużo osieroconych sesji SSH (Windows)", "sprawdź liczbę ssh; zamknij karty tail; restart launchera"]],
      fs=10, hfs=11, rh=0.6)

# ══════════════════ 11. HEALTH-CHECK + PLIKI STANU ══════════════════
s = slide(); header(s, "EKSPLOATACJA", "Health-check + pliki stanu")
box(s, 0.75, 1.6, 6.0, 5.0, fill=CARD, line=LINE)
txt(s, 1.0, 1.75, 5.5, 0.4, [[("SZYBKIE KOMENDY", 13, CYAN, True)]])
hc = ["# proces żyje?", "pgrep -af 'python -u Meshtastic_SCADA.py'", "",
      "# ostatni HB (tryb gm, aktywność ga):", "grep '\"t\":\"hb\"' /tmp/test_transport.log | tail -1", "",
      "# HA API odpowiada?", "curl -H 'Authorization: Bearer $TOKEN' \\", "     localhost:8123/api/"]
txt(s, 1.0, 2.35, 5.5, 4.1, [[(l if l else " ", 10.5, (GREEN if l.startswith("#") else WHITE), False)] for l in hc], sp=1.25)
box(s, 7.05, 1.6, 5.55, 5.0, fill=CARD, line=LINE)
txt(s, 7.3, 1.75, 5.1, 0.4, [[("PLIKI STANU (na maszynie)", 13, GREEN, True)]])
files = [("lora_params.json", "parametry (master)"), ("lora_params_sup_<gw>.json", "mirror params"),
         ("lora_mode_gw.json", "tryb bramki (ponad config)"), ("lora_sid_map_<gw>.json", "stabilne sidy"),
         ("lora_anomaly_ids.json", "aktywne anomalie"), ("lora_schedule_gw.json", "harmonogram"),
         ("test_transport.log", "log runtime")]
rows = [[("/tmp/" + f, 10.5, CYAN, False), ("  " + d, 10.5, WHITE, False)] for f, d in files]
txt(s, 7.3, 2.35, 5.1, 4.1, rows, sp=1.7)

# ══════════════════ 12. ZASADY ══════════════════
s = slide(); header(s, "PODSUMOWANIE", "Zasady bezpieczeństwa operacyjnego")
box(s, 0.75, 1.7, 11.85, 4.4, fill=CARD, line=RGBColor(0x1f, 0x3a, 0x1f))
bullets(s, 1.1, 2.05, 11.3, [
    ("Bramka = źródło prawdy.", "Dashboard supervisora = odwzorowanie bramki (podmiana encji)."),
    ("Testuj na G2 (testbed),", "nie na produkcyjnym G1, dopóki nie potwierdzone live."),
    ("Restart supervisora bezpieczny", "(bramki autonomiczne); restart bramki = krótka utrata monitoringu tej lokalizacji."),
    ("Nie zmieniać stylu/mechanik", "dashboardu ad-hoc — trzymać się wzorca."),
    ("Zmiana tokenu HA", "= config.py (obie maszyny) + .claude.json + restart roli + restart Claude Code (MCP)."),
], fs=13, mcol=GREEN, sp=1.7)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "RUNBOOK_prezentacja.pptx")
prs.save(OUT)
print("OK ->", OUT, "| slajdów:", len(prs.slides._sldIdLst))
