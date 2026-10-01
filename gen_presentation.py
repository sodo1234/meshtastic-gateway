#!/usr/bin/env python3
"""Generuje prezentację PPT: LoRa Zigbee SCADA (step1-5 refaktor) — status + procedury
testowe wszystkich funkcjonalności. Stylistyka spójna z dashboardem (dark #0a0a0a, cyjan
#22d3ee). v38 = legacy monolit, NIE jest już używane (patrz CLAUDE.md)."""
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR

BG    = RGBColor(0x0a, 0x0a, 0x0a)
CARD  = RGBColor(0x14, 0x14, 0x14)
CYAN  = RGBColor(0x22, 0xd3, 0xee)
GREEN = RGBColor(0x4a, 0xde, 0x80)
AMBER = RGBColor(0xfb, 0xbf, 0x24)
RED   = RGBColor(0xf8, 0x71, 0x71)
WHITE = RGBColor(0xe5, 0xe5, 0xe5)
GREY  = RGBColor(0x73, 0x73, 0x73)
DGREY = RGBColor(0x52, 0x52, 0x52)

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)
SW, SH = prs.slide_width, prs.slide_height
BLANK = prs.slide_layouts[6]


def slide():
    s = prs.slides.add_slide(BLANK)
    bg = s.shapes.add_shape(1, 0, 0, SW, SH)
    bg.fill.solid(); bg.fill.fore_color.rgb = BG; bg.line.fill.background()
    bg.shadow.inherit = False
    return s


def box(s, x, y, w, h, fill=None, line=None):
    from pptx.enum.shapes import MSO_SHAPE
    sh = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid(); sh.fill.fore_color.rgb = fill
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line; sh.line.width = Pt(1)
    sh.shadow.inherit = False
    return sh


def txt(s, x, y, w, h, runs, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, sp=1.0):
    tb = s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True; tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.05); tf.margin_top = tf.margin_bottom = Inches(0.02)
    first = True
    for line in runs:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.alignment = align; p.line_spacing = sp; p.space_after = Pt(2)
        for (t, sz, col, bold) in line:
            r = p.add_run(); r.text = t
            r.font.size = Pt(sz); r.font.color.rgb = col; r.font.bold = bold
            r.font.name = "Consolas"
    return tb


def header(s, kicker, title):
    box(s, 0.5, 0.45, 0.12, 0.55, fill=CYAN)
    txt(s, 0.75, 0.42, 12, 0.35, [[(kicker, 11, DGREY, True)]])
    txt(s, 0.73, 0.66, 12.2, 0.7, [[(title, 26, WHITE, True)]])


def footer(s, n):
    txt(s, 0.75, 7.0, 8, 0.3, [[("LoRa Zigbee SCADA — BMS rozproszony", 9, DGREY, False)]])
    txt(s, 11.8, 7.0, 1.2, 0.3, [[(str(n), 9, DGREY, True)]], align=PP_ALIGN.RIGHT)


def proc_slide(n, kicker, title, steps, expect, status):
    """Slajd procedury testowej: kroki (lewa) + oczekiwany wynik (prawa) + pasek statusu."""
    s = slide(); header(s, kicker, title)
    # status pill
    scol = {"OK": GREEN, "WARN": AMBER, "BLOK": RED}[status[0]]
    box(s, 10.4, 0.5, 2.45, 0.5, fill=CARD, line=scol)
    txt(s, 10.4, 0.52, 2.45, 0.46, [[("● " + status[1], 11, scol, True)]], align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    # kroki
    box(s, 0.75, 1.55, 7.2, 5.1, fill=CARD, line=RGBColor(0x1f,0x1f,0x1f))
    txt(s, 1.0, 1.7, 6.8, 0.4, [[("PROCEDURA TESTOWA", 12, CYAN, True)]])
    rows = []
    for i, st in enumerate(steps, 1):
        rows.append([(f"{i}. ", 12, CYAN, True), (st, 12, WHITE, False)])
    txt(s, 1.0, 2.15, 6.8, 4.4, rows, sp=1.15)
    # oczekiwany wynik
    box(s, 8.15, 1.55, 4.45, 5.1, fill=CARD, line=RGBColor(0x1f,0x1f,0x1f))
    txt(s, 8.4, 1.7, 4.0, 0.4, [[("OCZEKIWANY WYNIK", 12, GREEN, True)]])
    erows = [[("✓ ", 11, GREEN, True), (e, 11, WHITE, False)] for e in expect]
    txt(s, 8.4, 2.15, 4.0, 4.4, erows, sp=1.15)
    footer(s, n)


# ── 1. TYTUŁ ──
s = slide()
box(s, 0, 0, SW, SH, fill=BG)
box(s, 0.9, 2.5, 0.16, 2.0, fill=CYAN)
txt(s, 1.3, 2.4, 11, 0.5, [[("LoRa ZIGBEE SCADA", 15, CYAN, True)]])
txt(s, 1.28, 2.95, 11.5, 1.4, [[("Rozproszony BMS", 46, WHITE, True)],
                                [("Zigbee → LoRa → Supervisor → Home Assistant", 20, GREY, False)]])
txt(s, 1.3, 5.0, 11, 0.8, [[("Status systemu i procedury testowe wszystkich funkcjonalności", 16, WHITE, False)],
                            [("3 bramki × ~100 urządzeń · 868 MHz · EU duty 10% · + carport 199 dev (site realny)", 13, DGREY, False)]])
txt(s, 1.3, 6.6, 11, 0.4, [[("2026-07-12  ·  Step 1–5 verified live  ·  unified dashboard + carport + audyt LoRa = WIP (checkpoint 76eac47, niezweryfikowane)", 11, DGREY, True)]])

# ── 2. ARCHITEKTURA ──
s = slide(); header(s, "PRZEGLĄD", "Architektura systemu")
def node(x, y, w, h, title, sub, col):
    box(s, x, y, w, h, fill=CARD, line=col)
    txt(s, x, y+0.15, w, 0.5, [[(title, 15, col, True)]], align=PP_ALIGN.CENTER)
    txt(s, x, y+0.62, w, h-0.6, [[(l, 10, WHITE, False)] for l in sub], align=PP_ALIGN.CENTER, sp=1.05)
node(4.7, 1.7, 3.9, 1.25, "SUPERVISOR (G0)", ["Xeon · HA Supervised", "3 anteny LoRa · agregacja", "calendar.lora_global (master)"], CYAN)
txt(s, 4.7, 3.05, 3.9, 0.4, [[("▲  LoRa 868 MHz · 220B · 3s cooldown  ▼", 11, AMBER, True)]], align=PP_ALIGN.CENTER)
for i,(x,nm) in enumerate([(0.95,"BRAMKA G1"),(4.9,"BRAMKA G2"),(8.85,"BRAMKA G3")]):
    col = GREEN if i==0 else DGREY
    node(x+0.0, 3.6, 3.5, 1.5, nm, ["Z2M + ConBee/SONOFF","HA lokalne · ~100 dev","Heltec V3 (SX1262)"], col)
txt(s, 0.95, 5.3, 11.5, 1.4, [
    [("G1 = AKTYWNA (live test bed: 100.98.155.78)   ·   G0 = 100.79.111.24   ·   Carport = site realny (100.104.38.14, 105 przekaźniki + 82 czujniki ruchu)", 12, GREEN, True)],
    [("Filozofia: każda bramka autonomiczna (lokalne Z2M+HA), supervisor agreguje, minimalizacja ruchu LoRa.", 12, WHITE, False)],
    [("Constraint: 220B max payload, 3.0s+jitter cooldown, half-duplex, EU duty cycle 10%. Bulk (devmap/avail/kalendarz) = zlib+chunk+CRC (ReliableTransfer).", 11, GREY, False)],
], sp=1.2)
footer(s, 2)

# ── 3. STATUS GOTOWOŚCI ──
s = slide(); header(s, "PODSUMOWANIE", "Status gotowości do wdrożenia")
box(s, 0.75, 1.55, 6.0, 5.1, fill=CARD, line=RGBColor(0x1f,0x3a,0x1f))
txt(s, 1.0, 1.7, 5.5, 0.4, [[("DZIAŁA (zweryfikowane live)", 13, GREEN, True)]])
ok = ["Detekcja offline/online (z2m → bramka)","Sync availability bramka → supervisor (spójny)",
      "z2m native availability (faza2) — switche prawdziwy stan","Anomalie: offline / bateria / stagnacja",
      "Popup z listą + clear per-anomalia (decoupled)","Kalendarz PUSH + PULL (TX-gate naprawił pętlę 07-07)",
      "Tryb pracy bramki (day/night/all-time)","LQI / link quality (force_update)",
      "Sterowanie switch + cmd retry→offline","Parametry T1/T2/T3 + Send Config",
      "Scrub sekretów przy git add (filtr scrubcfg, zweryfikowany)"]
txt(s, 1.0, 2.2, 5.5, 4.4, [[("✓ ", 11, GREEN, True),(o, 11, WHITE, False)] for o in ok], sp=1.18)
box(s, 7.0, 1.55, 5.6, 5.1, fill=CARD, line=RGBColor(0x3a,0x2f,0x1f))
txt(s, 7.25, 1.7, 5.2, 0.4, [[("WYMAGA INTERWENCJI (sprzęt/decyzja)", 13, AMBER, True)]])
todo = [("BLOK","Unified dashboard (7 zakładek) — zbudowany, NIGDY nie live-testowany na obu maszynach"),
        ("BLOK","Carport (site realny 199 dev) — zintegrowany w kodzie, NIE zweryfikowany end-to-end po rebase"),
        ("WARN","chunk_size 140→60 na kanale rt (devmap/avail blob) — fix dzisiejszy, TYLKO ast.parse, zero live"),
        ("WARN","Cały WIP (~50 plików step4/step5 rebase) scommitowany jako checkpoint 76eac47, BEZ prefiksu verified:"),
        ("INFO","Krok 8 (sterowanie pełne) i Krok 17 (parametry bidir) z planu 17")]
rows=[]
for tag,t in todo:
    col={"BLOK":RED,"WARN":AMBER,"INFO":DGREY}[tag]
    rows.append([(f"[{tag}] ",11,col,True),(t,11,WHITE,False)])
txt(s, 7.25, 2.25, 5.2, 4.3, rows, sp=1.3)
footer(s, 3)

# ── 4..N PROCEDURY ──
proc_slide(4, "FUNKCJA 01", "Detekcja offline / online urządzeń",
    ["Na bramce: odłącz zasilanie urządzenia Zigbee (np. Temp 1) lub wyjmij baterię.",
     "Odczekaj > timeout danego typu (T1 switch 58min / T2 sensor 60min / T3 door-leak 120min).",
     "Obserwuj log bramki: '💀 <dev> OFFLINE (cisza z2m ...)'.",
     "Sprawdź dashboard bramki (zakładka Bramka) — kafel OFFLINE rośnie, urządzenie czerwone.",
     "Sprawdź supervisor: sensor.lora_gw_g1_devices_offline + binary_sensor ...available = off.",
     "Podłącz urządzenie → następny raport z2m → powrót.",],
    ["Bramka i supervisor pokazują TO SAMO urządzenie offline.",
     "Licznik = lista = *_available (spójność).",
     "Po powrocie: '🟢 ONLINE' i licznik spada.",
     "Reconcile co 900s łapie zgubione pakiety."],
    ("OK","DZIAŁA"))

proc_slide(5, "FUNKCJA 02", "Anomalie + popup z listą + clear",
    ["Wywołaj anomalię: offline (jak F01), niska bateria (<25%), lub stagnacja (brak zmian 72h).",
     "Na dashboardzie supervisora (widok Bramki) kliknij kafel OFFLINE / BATERIA / INNE.",
     "Otworzy się popup z dynamiczną listą: urządzenie · bramka · typ · data godzina.",
     "Kliknij wiersz konkretnej anomalii → clear tej jednej anomalii.",
     "Lub użyj 'WYCZYŚĆ WSZYSTKIE <typ>' (z potwierdzeniem).",],
    ["Popup pokazuje listę z timestampem DD.MM HH:MM.",
     "Klik wiersza → '🗑️ clear ręczny ... n=1' w logu.",
     "Realne offline wraca po reconcile (poprawne).",
     "Każda anomalia z nazwą + ID bramki."],
    ("OK","DZIAŁA"))

proc_slide(6, "FUNKCJA 03", "Kalendarz PUSH (supervisor → bramka)",
    ["Na supervisorze edytuj calendar.lora_global — dodaj wydarzenie (PRODUKCJA/PRZERWA/SERWIS).",
     "Naciśnij przycisk 'Sync Calendar All' (lub per-bramka 'Sync Calendar').",
     "Obserwuj log supervisora: 'HA → N slotów' → cal_begin/chunk/end.",
     "Obserwuj log bramki: '📥 Odebrano N slotów' → 'ICS zapisany' → 'HA reload'.",
     "Sprawdź calendar.lora_g1 na bramce — ma nowe wydarzenie.",
     "Sprawdź sensor ...tryb i ...hash_kalendarza (zmiana).",],
    ["calendar.lora_g1 = calendar.lora_global (forward).",
     "Hash kalendarza zmienia się i zgadza po obu str.",
     "Tryb produkcji aktualny + następna zmiana.",
     "Drift-hash w HB auto-resyncuje.",
     "Transfer z CRC + ACK + retransmit."],
    ("OK","DZIAŁA"))

proc_slide(7, "FUNKCJA 04", "Kalendarz PULL (bramka → supervisor)",
    ["Na bramce edytuj harmonogram lokalnie (wymaga gw_calendar_id w config).",
     "Naciśnij 'Push Schedule Up' na dashboardzie bramki.",
     "Obserwuj log bramki: 'Begin gw_push' → cal_chunk.",
     "Obserwuj supervisor: 'Begin gw_push' → 'Odebrano' → mirror calendar.lora_g1 + timestamp.",
     "NAPRAWIONE 2026-07-07: root cause = backpressure (sendText blokuje legalnie przy pełnej",
     "kolejce radia) mylony ze zwisem → force-close → reconnect → pętla 3 dni. TX-gate: po",
     "tx_timeout WARN + czekaj do tx_hard_timeout zamiast ubijać interfejs.",],
    ["Transfer kończy się (begin→chunk→end→ack) bez pętli reconnect.",
     "Mirror-only (bez pętli, master = global) + timestamp na dashboardzie.",
     "chunk_size 90→60 (mitygacja RF, ec17b1d) + TX-gate razem = zweryfikowane live.",
     "Zweryfikowane na żywo 2026-07-07 (commit 4ddcc69)."],
    ("OK","DZIAŁA"))

proc_slide(8, "FUNKCJA 05", "Tryb pracy bramki (day / night / all-time)",
    ["W config bramki ustaw gateway_mode.operating_mode = 'day' lub 'night' (+ lat/lon lub godziny).",
     "Restart procesu bramki (przez Launcher).",
     "Sprawdź sensor ...tryb_pracy_bramki = Dzienna/Nocna/Całodobowa.",
     "W oknie NIEAKTYWNYM: urządzenia bez zasilania NIE są zgłaszane jako offline (supresja).",
     "Sprawdź pole 'ga' w HB (1=aktywna, 0=wstrzymana) i kartę na dashboardzie.",],
    ["Tryb widoczny jako encja + karta (Harmonogram).",
     "Supresja offline gdy bramka nieaktywna.",
     "Solar (sunrise equation) lub stałe godziny.",
     "all-time = brak supresji (domyślnie)."],
    ("OK","DZIAŁA*"))

proc_slide(9, "FUNKCJA 06", "Sterowanie + dashboardy + LQI",
    ["Dashboard bramki → zakładka Sterowanie → kliknij switch (Test 1/2) → ON/OFF.",
     "Obserwuj: komenda → Z2M → potwierdzenie 'st' → odbicie stanu w HA.",
     "Brak potwierdzenia: retry ×2 (15s) → '💀 brak st → OFFLINE'.",
     "Zakładka Pomiary: temp/wilgotność + wykresy 24h; Alarmy: leak/door.",
     "Każdy kafel: ● online/offline · 🔋 bateria · 📶 LQI (link quality).",],
    ["Switch sterowalny dwukierunkowo (tap→toggle).",
     "Zmiana zewnętrzna też odbita (propagacja z2m).",
     "LQI świeży (force_update co raport).",
     "Ikony w osobnej kolumnie (czytelne)."],
    ("OK","DZIAŁA"))

proc_slide(10, "FUNKCJA 07", "Audyt LoRa: chunk_size 60 na kanale rt (devmap + avail blob)",
    ["PRIORYTET NA DZIŚ. Na bramce wywołaj pełny discovery (devmap, do 199 urządzeń)",
     "przez przycisk manualny — obserwuj log: 'rt_begin devmap' → liczbę chunków.",
     "Policz retransmisje (log 'brak cack — retry') przy chunk_size=60 vs poprzednim 140/90.",
     "Poczekaj na pełny sweep availability (report_interval) → 'avail blob' w logu bramki.",
     "Na supervisorze: sprawdź 'rt' odbiór — devmap zarejestrował WSZYSTKIE urządzenia,",
     "avail blob zaktualizował dostępność WSZYSTKICH sid bez utraty.",],
    ["Devmap kończy się (rt_end + ack ok) bez przerwania.",
     "Mniej retransmisji na chunk niż przy 140B (cel fixu).",
     "Avail blob = 1 transfer zamiast ~33 pakietów `b`.",
     "Zero regresji w arbiter/backpressure (nie ruszane)."],
    ("WARN","DO PRZETESTOWANIA"))

proc_slide(11, "FUNKCJA 08", "Unified dashboard — 7 zakładek (bramka + supervisor)",
    ["Otwórz dashboard bramki i supervisora — sprawdź 7 zakładek: Bramka-wzór, Sterowanie,",
     "Pomiary, Alarmy, Harmonogram, Parametry, Diagnostyka (ikony + routing path).",
     "Bramka: kliknij po kolei każdą zakładkę — sprawdź czy karty renderują się bez błędu.",
     "Sterowanie: przełącz Test 1/2 + Virtual IO. Pomiary: wykresy 24h Temp 1/2.",
     "Alarmy: listy offline/bateria/inne z clear per-wiersz. Harmonogram: tryb+PULL/PUSH.",
     "Parametry: 12 pól per-bramka. Diagnostyka: hash/uptime/link/czas.",],
    ["Wszystkie 7 zakładek renderują się na OBU maszynach.",
     "apexcharts + card-mod działają (doinstalowane na gw).",
     "Brak duplikacji/rozjazdu ze starymi dashboardami.",
     "Popupy clear działają jak w poprzedniej wersji."],
    ("BLOK","NIGDY NIE TESTOWANE LIVE"))

proc_slide(12, "FUNKCJA 09", "Carport — site produkcyjny (105 przekaźników + 82 czujniki ruchu)",
    ["Na bramce carport (100.104.38.14) wywołaj discovery — sprawdź że wszystkie 199",
     "urządzeń (ZBMINIR2 przekaźniki + SNZB-03P ruch) rejestrują się poprawnie.",
     "Wywołaj ruch przed czujnikiem — sprawdź automatyzację motion→relay (lokalna z2m/HA,",
     "NIE przez LoRa) + czy stan przekaźnika odbija się na dashboardzie carportu.",
     "Sprawdź widoki: views_relays_and_motion.yaml + views_anomaly.yaml.",
     "Zweryfikuj z2m_availability.patch.yaml — dostępność przekaźników (mains) poprawna.",],
    ["199 urządzeń zarejestrowanych (bez ghost/duplikatów).",
     "Motion→relay lokalnie natychmiastowe (nie czeka na LoRa).",
     "Dashboard carportu pokazuje spójny stan z bramką.",
     "Zero regresji względem multi-gw (G1=carport, G2=domowy testbed)."],
    ("BLOK","NIE ZWERYFIKOWANE PO REBASE"))

# ── N+1: TODO USERA ──
s = slide(); header(s, "DLA CIEBIE", "Co musisz zrobić Ty (ręce / decyzje)")
items = [
    (RED,  "1. Live test chunk_size=60 na kanale rt (devmap/avail blob)", "Fix dzisiejszy (audyt LoRa) — TYLKO ast.parse, zero live. Przetestuj na fizycznej bramce/supervisorze (Tailscale) zanim uznasz za działające. Zob. Funkcja 07."),
    (RED,  "2. Live test unified dashboard (7 zakładek)", "Zbudowany, nigdy nie odpalony na obu maszynach naraz. Zob. Funkcja 08."),
    (RED,  "3. Weryfikacja carportu po rebase step4/step5", "Site realny 199 dev — sprawdź że nic się nie zepsuło po rebase na step4+pliki. Zob. Funkcja 09."),
    (AMBER,"4. Commit checkpoint 76eac47 = punkt powrotu, NIE 'verified:'", "Cały WIP (~50 plików) scommitowany świadomie bez prefiksu verified — dopiero po live-testach 1-3 rozważ osobne commity 'verified:' per funkcja."),
    (DGREY,"5. Decyzja: kroki 8 i 17", "Czy budujemy pełne sterowanie (krok 8) i parametry bidirectional (krok 17) z planu 17-kroków? — nowa praca, nie bug."),
]
y = 1.55
for col, t, d in items:
    box(s, 0.75, y, 0.12, 0.95, fill=col)
    txt(s, 1.0, y, 11.6, 0.45, [[(t, 13, col if col!=DGREY else WHITE, True)]])
    txt(s, 1.0, y+0.42, 11.6, 0.55, [[(d, 11, GREY, False)]], sp=1.05)
    y += 1.05
footer(s, 13)

# ── N+2: WDROŻENIE / PODSUMOWANIE ──
s = slide(); header(s, "WDROŻENIE", "Gotowość i procedura wdrożenia")
box(s, 0.75, 1.6, 5.7, 5.0, fill=CARD, line=RGBColor(0x1f,0x1f,0x1f))
txt(s, 1.0, 1.75, 5.2, 0.4, [[("OCENA GOTOWOŚCI", 13, CYAN, True)]])
txt(s, 1.0, 2.25, 5.2, 4.2, [
    [("Rdzeń (monitoring + anomalie + PUSH+PULL kalendarza + z2m availability)",12,WHITE,False)],
    [("   → GOTOWY, zweryfikowany live (commit 4ddcc69).",12,GREEN,True)],
    [("",6,WHITE,False)],
    [("Step4/step5 rebase + unified dashboard + carport + audyt LoRa",12,WHITE,False)],
    [("   → checkpoint 76eac47, NIEWERYFIKOWANE live.",12,RED,True)],
    [("",6,WHITE,False)],
    [("Werdykt: rdzeń production-ready NA WERSJI 4ddcc69;",12,WHITE,True)],
    [("bieżący WIP (unified dashboard/carport/chunk_size)",12,WHITE,True)],
    [("wymaga 3 live-testów (F07-F09) przed uznaniem za gotowe.",12,WHITE,True)],
], sp=1.15)
box(s, 6.65, 1.6, 5.95, 5.0, fill=CARD, line=RGBColor(0x1f,0x1f,0x1f))
txt(s, 6.9, 1.75, 5.5, 0.4, [[("PROCEDURA WDROŻENIA (per bramka)", 13, CYAN, True)]])
dep = ["Postaw Heltec V3 + ConBee/SONOFF + Z2M + HA lokalne.",
       "Ustaw .role (gateway/supervisor) w ~/meshtastic/.",
       "Wpisz sekrety in-place (MQTT_PASS + HA_TOKEN) — NIE scp configu.",
       "Skonfiguruj monitored[] / priority[] / T1-T3 / kalendarz.",
       "Start przez Launcher (:8765) → Meshtastic_SCADA.py.",
       "Wygeneruj dashboard: gen_gateway_dashboard.py + patche sup.",
       "Smoke: ping_all → HB → discovery → batch → anomalia testowa.",
       "Walidacja: offline-sync + PUSH/PULL kalendarza + sterowanie.",
       "NOWE (przed uznaniem za gotowe): F07 chunk_size=60, F08 unified",
       "dashboard 7 zakładek, F09 carport end-to-end po rebase."]
txt(s, 6.9, 2.25, 5.5, 4.2, [[(f"{i}. ",11,CYAN,True),(d,11,WHITE,False)] for i,d in enumerate(dep,1)], sp=1.2)
footer(s, 14)

import os
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Procedura_testowa_SCADA_2026-07-12.pptx")
prs.save(out)
print("zapisano:", out, "| slajdów:", len(prs.slides._sldIdLst))
