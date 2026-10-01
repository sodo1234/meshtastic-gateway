#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Instrukcja WDROZENIA LoRa SCADA jako PPTX — dla osoby nietechnicznej.

Styl spojny z gen_runbook_deck.py (dark #0a0a0a, cyjan #22d3ee).
Odpal:  python docs/gen_wdrozenie_deck.py <folder_wyjsciowy>
"""
import os
import sys
# Hasło NIE trafia do repo (deck jest generowany lokalnie). Ustaw przed generacją:
#   $env:LORA_SSH_PASS="…"   (PowerShell)  /  export LORA_SSH_PASS=…   (bash)
SSH_PASS = os.environ.get('LORA_SSH_PASS', '<hasło — patrz menedżer haseł>')

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

OUT = sys.argv[1] if len(sys.argv) > 1 else '.'
SHOTS = os.path.expanduser('~')

BG = RGBColor(0x0a, 0x0a, 0x0a); CARD = RGBColor(0x14, 0x14, 0x14)
CYAN = RGBColor(0x22, 0xd3, 0xee); GREEN = RGBColor(0x4a, 0xde, 0x80)
AMBER = RGBColor(0xfb, 0xbf, 0x24); RED = RGBColor(0xf8, 0x71, 0x71)
WHITE = RGBColor(0xe5, 0xe5, 0xe5); GREY = RGBColor(0x73, 0x73, 0x73)
DGREY = RGBColor(0x52, 0x52, 0x52); LINE = RGBColor(0x1f, 0x1f, 0x1f)
TERM = RGBColor(0x0f, 0x17, 0x0f)

prs = Presentation(); prs.slide_width = Inches(13.333); prs.slide_height = Inches(7.5)
SW, SH = prs.slide_width, prs.slide_height; BLANK = prs.slide_layouts[6]
_n = [0]


def slide():
    s = prs.slides.add_slide(BLANK)
    bg = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, SW, SH)
    bg.fill.solid(); bg.fill.fore_color.rgb = BG
    bg.line.fill.background(); bg.shadow.inherit = False
    return s


def box(s, x, y, w, h, fill=CARD, line=LINE, lw=1):
    sh = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    sh.adjustments[0] = 0.04
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid(); sh.fill.fore_color.rgb = fill
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line; sh.line.width = Pt(lw)
    sh.shadow.inherit = False
    return sh


def txt(s, x, y, w, h, runs, align=PP_ALIGN.LEFT, sp=1.0):
    tb = s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    first = True
    for line in runs:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.alignment = align; p.line_spacing = sp
        for (t, sz, col, bold) in line:
            r = p.add_run(); r.text = t
            r.font.size = Pt(sz); r.font.color.rgb = col; r.font.bold = bold
            r.font.name = 'Segoe UI'
    return tb


def mono(s, x, y, w, lines, fs=13, col=GREEN):
    """Blok terminala — duza, czytelna czcionka monospace na ciemnym tle."""
    h = 0.16 + len(lines) * (fs / 72.0 * 1.55)
    box(s, x, y, w, h, fill=TERM, line=RGBColor(0x1f, 0x3f, 0x1f))
    tb = s.shapes.add_textbox(Inches(x + 0.16), Inches(y + 0.08), Inches(w - 0.32), Inches(h - 0.16))
    tf = tb.text_frame; tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    for i, ln in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.line_spacing = 1.15
        r = p.add_run(); r.text = ln
        r.font.size = Pt(fs); r.font.name = 'Consolas'
        r.font.color.rgb = GREY if ln.strip().startswith('#') else col
    return y + h


def header(s, kicker, title, num=True):
    if num:
        _n[0] += 1
    txt(s, 0.62, 0.36, 11, 0.3, [[(kicker.upper(), 11, CYAN, True)]])
    txt(s, 0.62, 0.66, 12.1, 0.7, [[(title, 27, WHITE, True)]])
    if num:
        txt(s, 12.1, 0.36, 0.7, 0.4, [[(f'{_n[0]:02d}', 22, DGREY, True)]], align=PP_ALIGN.RIGHT)
    ln = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.62), Inches(1.42), Inches(12.1), Pt(1))
    ln.fill.solid(); ln.fill.fore_color.rgb = LINE; ln.line.fill.background(); ln.shadow.inherit = False


def bullets(s, x, y, w, items, fs=14, mark='▸', mcol=CYAN, sp=1.45):
    tb = s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(5))
    tf = tb.text_frame; tf.word_wrap = True
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    for i, it in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.line_spacing = sp
        r = p.add_run(); r.text = mark + '  '
        r.font.size = Pt(fs); r.font.color.rgb = mcol; r.font.bold = True; r.font.name = 'Segoe UI'
        if isinstance(it, tuple):
            head, rest = it
            r = p.add_run(); r.text = head
            r.font.size = Pt(fs); r.font.color.rgb = WHITE; r.font.bold = True; r.font.name = 'Segoe UI'
            r = p.add_run(); r.text = rest
            r.font.size = Pt(fs); r.font.color.rgb = GREY; r.font.name = 'Segoe UI'
        else:
            r = p.add_run(); r.text = it
            r.font.size = Pt(fs); r.font.color.rgb = WHITE; r.font.name = 'Segoe UI'
    return tb


def warn(s, x, y, w, tytul, tresc, col=AMBER):
    h = 0.62 + 0.30 * (1 + len(tresc) // 95)
    box(s, x, y, w, h, fill=RGBColor(0x1a, 0x14, 0x08), line=col)
    txt(s, x + 0.22, y + 0.14, w - 0.44, 0.3, [[('⚠  ' + tytul, 13, col, True)]])
    txt(s, x + 0.22, y + 0.46, w - 0.44, h - 0.5, [[(tresc, 12, WHITE, False)]], sp=1.25)
    return y + h


def pic(s, name, x, y, w):
    p = os.path.join(SHOTS, name)
    if os.path.exists(p):
        s.shapes.add_picture(p, Inches(x), Inches(y), width=Inches(w))
        return True
    return False


def table(s, x, y, colw, headers, rows, fs=12, rh=0.40):
    cy = y
    cx = x
    for i, hh in enumerate(headers):
        txt(s, cx, cy, colw[i], 0.3, [[(hh.upper(), 10, CYAN, True)]])
        cx += colw[i]
    cy += 0.34
    for r in rows:
        ln = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(cy - 0.04), Inches(sum(colw)), Pt(0.75))
        ln.fill.solid(); ln.fill.fore_color.rgb = LINE; ln.line.fill.background(); ln.shadow.inherit = False
        cx = x
        for i, c in enumerate(r):
            col = WHITE if i == 0 else GREY
            bold = (i == 0)
            txt(s, cx, cy + 0.06, colw[i], 0.34, [[(str(c), fs, col, bold)]])
            cx += colw[i]
        cy += rh
    return cy


# ─────────────────────────────── SLAJDY ───────────────────────────────

# 1. TYTUL
s = slide()
txt(s, 0.9, 2.25, 11.5, 0.4, [[('INSTRUKCJA WDROŻENIA — KROK PO KROKU', 13, CYAN, True)]])
txt(s, 0.9, 2.7, 11.5, 1.2, [[('LoRa Zigbee SCADA', 46, WHITE, True)]])
txt(s, 0.9, 3.8, 11.5, 0.6, [[('Jak postawić cały system od zera na nowym sprzęcie', 17, GREY, False)]])
box(s, 0.9, 4.7, 5.4, 0.9, fill=CARD, line=LINE)
txt(s, 1.15, 4.88, 5.0, 0.6, [[('Dla kogo: ', 12, GREY, False), ('osoba z minimalną wiedzą o Linuksie', 12, WHITE, True)],
                              [('Czas: ', 12, GREY, False), ('ok. 2-3 h na jedną maszynę', 12, WHITE, True)]], sp=1.35)
box(s, 6.6, 4.7, 5.8, 0.9, fill=CARD, line=LINE)
txt(s, 6.85, 4.88, 5.3, 0.6, [[('Wersja: ', 12, GREY, False), ('2026-08-25', 12, WHITE, True)],
                              [('Pliki do wdrożenia: ', 12, GREY, False), ('folder obok tej instrukcji', 12, WHITE, True)]], sp=1.35)

# 2. NAJWAZNIEJSZE: to juz nie sa 2 pliki
s = slide(); header(s, 'zacznij tutaj', 'To już NIE są dwa pliki .py')
txt(s, 0.62, 1.65, 12.1, 0.5,
    [[('Kiedyś: ', 14, GREY, False), ('cały gateway w jednym pliku, cały supervisor w drugim. ', 14, WHITE, False),
      ('Uruchamiało się ', 14, GREY, False), ('python gateway.py', 14, GREEN, True), (' i działało.', 14, GREY, False)]])
txt(s, 0.62, 2.15, 12.1, 0.5,
    [[('Teraz: ', 14, GREY, False), ('kod jest podzielony na moduły, a konfiguracja siedzi w osobnym pliku.', 14, WHITE, False)]])
warn(s, 0.62, 2.72, 12.1,
     'Co to zmienia dla Ciebie w praktyce — właściwie nic',
     'Nadal uruchamiasz JEDEN plik: Meshtastic_SCADA.py. Reszta to biblioteki, które ten plik sam sobie wczytuje. '
     'Musisz tylko skopiować CAŁY katalog, a nie pojedynczy plik — i raz uzupełnić config.py.')
txt(s, 0.62, 3.95, 12.1, 0.4, [[('Tak wygląda katalog na maszynie po wdrożeniu:', 13, CYAN, True)]])
mono(s, 0.62, 4.35, 8.2, [
    '~/meshtastic/',
    '   Meshtastic_SCADA.py    <-- TEN plik uruchamiasz (gateway I supervisor)',
    '   config.py                <-- ustawienia TEJ maszyny (tokeny, lista urzadzen)',
    '   .role                    <-- jedno slowo: gateway albo supervisor',
    '   modules/                 <-- 55 plikow biblioteki, NIE ruszasz',
    '   tools/                   <-- narzedzia diagnostyczne',
    '   venv/                    <-- srodowisko Pythona (tworzysz komenda)',
], fs=12)
box(s, 9.1, 4.35, 3.62, 1.95, fill=CARD, line=CYAN)
txt(s, 9.35, 4.55, 3.2, 1.6,
    [[('Ten sam plik', 12, CYAN, True)],
     [('Meshtastic_SCADA.py', 12, GREEN, True)],
     [('działa i jako bramka,', 12, WHITE, False)],
     [('i jako supervisor.', 12, WHITE, False)],
     [('O roli decyduje plik', 12, GREY, False)],
     [('.role', 12, GREEN, True)]], sp=1.3)

# 3. ARCHITEKTURA
s = slide(); header(s, 'jak to działa', 'Trzy maszyny, jedno radio')
txt(s, 0.62, 1.7, 12.1, 0.4,
    [[('Urządzenia Zigbee → bramka → ', 14, WHITE, False), ('radio LoRa 868 MHz', 14, CYAN, True),
      (' → supervisor → dashboard', 14, WHITE, False)]])
table(s, 0.62, 2.3, [2.6, 2.7, 6.8],
      ['maszyna', 'adres (Tailscale)', 'co robi'],
      [['G1 — carport', '100.68.79.100', 'PRODUKCJA. 194 urządzenia: 78 przekaźników + 82 czujniki ruchu'],
       ['G2 — testbed', '100.98.155.78', 'Domowy, 6 urządzeń. Do sprawdzania zmian w kodzie'],
       ['G0 — supervisor', '100.79.111.24', 'Zbiera dane z obu bramek, tu patrzysz na dashboard']])
warn(s, 0.62, 4.15, 12.1, 'Bramki NIE mają stałego internetu',
     'To nie usterka, tylko założenie projektu. Cała łączność idzie po radiu LoRa. Jeśli nie możesz się połączyć '
     'z bramką przez SSH — sprawdź, czy nadaje, patrząc na dashboard supervisora. Brak pingu NIE oznacza awarii.', col=CYAN)
txt(s, 0.62, 5.5, 12.1, 0.4, [[('Login do wszystkich trzech maszyn: ', 13, GREY, False),
                               ('td', 13, GREEN, True), ('  /  hasło: ', 13, GREY, False), (SSH_PASS, 13, GREEN, True),
                               ('   (to samo działa jako sudo)', 13, GREY, False)]])

# 4. CALA INSTALACJA = 2 KROKI
s = slide(); header(s, 'najważniejszy slajd', 'Cała instalacja to DWA kroki')
box(s, 0.62, 1.7, 5.9, 2.05, fill=CARD, line=CYAN, lw=2)
txt(s, 0.92, 1.9, 5.4, 1.7,
    [[('KROK 1', 13, CYAN, True)],
     [('Odtwórz backup Home Assistant', 17, WHITE, True)],
     [('', 6, WHITE, False)],
     [('HA → Ustawienia → System →', 12, GREY, False)],
     [('Kopie zapasowe → Wgraj kopię', 12, GREY, False)]], sp=1.25)
box(s, 6.82, 1.7, 5.9, 2.05, fill=CARD, line=GREEN, lw=2)
txt(s, 7.12, 1.9, 5.4, 1.7,
    [[('KROK 2', 13, GREEN, True)],
     [('Uruchom jeden plik', 17, WHITE, True)],
     [('', 6, WHITE, False)],
     [('./ZAINSTALUJ.sh', 13, GREEN, True)],
     [('Zapyta, czy to bramka czy supervisor.', 12, GREY, False)]], sp=1.25)
txt(s, 0.62, 4.0, 12.1, 0.4, [[('I to wszystko. Poniżej — co dokładnie robi każdy z tych kroków.', 13, WHITE, False)]])
y = mono(s, 0.62, 4.45, 12.1, [
    '# skopiuj ten folder na maszyne (pendrive albo scp), potem:',
    'cd <ten-folder>',
    'chmod +x ZAINSTALUJ.sh',
    './ZAINSTALUJ.sh',
], fs=13)
warn(s, 0.62, y + 0.25, 12.1, 'Skrypt można puszczać wielokrotnie',
     'Nie zepsuje tego, co już działa. Jeśli config.py istnieje — NIE nadpisze go. '
     'Jeśli czegoś zabrakło (np. nie była podpięta antena), popraw i uruchom ponownie.', col=GREEN)

# 5. CO PRZYNOSI BACKUP HA
s = slide(); header(s, 'krok 1', 'Co przynosi backup Home Assistant')
txt(s, 0.62, 1.68, 12.1, 0.4,
    [[('Backup HA to nie tylko „ustawienia HA". Zawiera prawie całą konfigurację systemu:', 13, WHITE, False)]])
table(s, 0.62, 2.2, [4.3, 7.8],
      ['co', 'dlaczego to ważne'],
      [['Dodatek Mosquitto', 'Broker MQTT — bez niego nic się nie połączy'],
       ['Dodatek Zigbee2MQTT', 'RAZEM ze sparowanymi urządzeniami i ustawieniem availability'],
       ['Dashboardy', 'Wszystkie ekrany, gotowe'],
       ['HACS + karty', 'button-card, card-mod, browser_mod — w tych samych wersjach'],
       ['Nazwy encji i urządzeń', 'Ponad 1000 nazw, których nie chcesz wpisywać ręcznie'],
       ['TOKENY dostępu', 'Dlatego config.py z tego folderu zadziała BEZ ZMIAN']])
warn(s, 0.62, 5.05, 12.1, 'To dlatego nie musisz nic wpisywać do config.py',
     'Gdyby backupu nie było, trzeba by ręcznie wygenerować nowy token w HA i wkleić go do config.py. '
     'Po odtworzeniu backupu stary token nadal działa — plik z katalogu 2_CONFIGI pasuje od razu.', col=GREEN)

# 6. CO ROBI SKRYPT
s = slide(); header(s, 'krok 2', 'Co robi ZAINSTALUJ.sh')
txt(s, 0.62, 1.65, 12.1, 0.4, [[('Wszystko, czego backup HA nie obejmuje — czyli warstwę systemową:', 13, WHITE, False)]])
bullets(s, 0.62, 2.15, 12.1, [
    ('Pyta w okienkach ', '— rola maszyny, port anteny, ustawienia HA (szczegóły na następnym slajdzie)'),
    ('Doinstalowuje pakiety ', '— python3, python3-venv, tmux (jeśli już są, pomija)'),
    ('Kopiuje program ', '— do ~/meshtastic i tworzy środowisko Pythona z bibliotekami'),
    ('Wykrywa antenę LoRa SAM ', '— znajduje ścieżkę by-id i wpisuje ją do config.py'),
    ('Włącza autostart ', '— wraz z autologinem, żeby system wstał po zaniku prądu'),
    ('Sprawdza, czy działa ', '— uruchamia testy i wypisuje wynik'),
], fs=13, sp=1.65)
warn(s, 0.62, 5.35, 12.1, 'Czego skrypt NIE zrobi',
     'Nie zainstaluje Debiana ani Home Assistant — to robisz raz, ręcznie. Nie odtworzy też backupu HA, '
     'bo tylko Home Assistant potrafi go rozpakować.')

# 6b. OKNA KONFIGURACYJNE
s = slide(); header(s, 'krok 2', 'O co zapyta instalator')
txt(s, 0.62, 1.65, 12.1, 0.4,
    [[('Pięć pytań w okienkach. ', 13, WHITE, False),
      ('Nic nie zmienia, dopóki nie zatwierdzisz podsumowania.', 13, AMBER, True)]])
table(s, 0.62, 2.15, [3.5, 8.6],
      ['pytanie', 'co wybierasz'],
      [['Rola maszyny', 'G1 (carport) / G2 (testowa) / SUP (supervisor) — z listy'],
       ['Antena LoRa', 'Wykrywa sama. Gdy znajdzie kilka — wybierasz z listy. Można wpisać ręcznie.'],
       ['Backup HA?', 'TAK = token przyjdzie z backupu. NIE = poprosi o wklejenie tokenu.'],
       ['Zaawansowane', 'Adres HA i hasło MQTT. Domyślnie NIE — większość instalacji nie wymaga.'],
       ['Podsumowanie', 'Pokazuje wszystkie ustawienia. Dopiero OK uruchamia instalację.']])
warn(s, 0.62, 4.6, 12.1, 'Instalator sam pilnuje najgroźniejszej pułapki',
     'Do config.py wpisuje PEŁNĄ ścieżkę anteny z /dev/serial/by-id/ — nigdy numeru ttyUSB0. '
     'Pokaże też wykrytą ścieżkę koordynatora Zigbee, żebyś wkleił ją w ustawienia dodatku Zigbee2MQTT.', col=GREEN)
txt(s, 0.62, 5.9, 12.1, 0.7,
    [[('Łączysz się przez SSH? Dodaj ', 12, GREY, False), ('-t', 12, GREEN, True),
      (', żeby okienka działały:  ', 12, GREY, False), ('ssh -t td@100.68.79.100', 12, GREEN, True)],
     [('Bez terminala instalator zada te same pytania zwykłym tekstem.', 12, GREY, False)]], sp=1.4)

# 7. TAK WYGLADA POPRAWNY PRZEBIEG
s = slide(); header(s, 'krok 2', 'Tak wygląda poprawny przebieg instalacji')
mono(s, 0.62, 1.7, 12.1, [
    '=== 1/7  Pakiety systemowe ===',
    '  [OK]   wszystkie pakiety juz sa',
    '=== 2/7  Kod programu ===',
    '  [OK]   skopiowano harness + modules + tools',
    '=== 3/7  Srodowisko Pythona ===',
    '  [OK]   biblioteki zainstalowane (19 pakietow)',
    '=== 4/7  Rola maszyny ===',
    '  [OK]   .role = supervisor  (sesja tmux: sup)',
    '=== 5/7  Konfiguracja ===',
    '  [OK]   znaleziono antene LoRa: /dev/serial/by-id/usb-Silicon_Labs_CP2102...',
    '  [OK]   config.py wskazuje wlasciwy port',
    '=== 6/7  Autostart po zaniku pradu ===',
    '  [OK]   autostart wpiety',
    '  [OK]   autologin GNOME juz wlaczony',
    '=== 7/7  Sprawdzenie ===',
    '  [OK]   modules.transport.smoke_test -> 11/11 passed',
    '  [OK]   testy regresji -> 6/6',
    '  [OK]   config.py wczytuje sie poprawnie',
    '',
    '  INSTALACJA ZAKONCZONA - bez bledow',
], fs=11)
txt(s, 0.62, 6.75, 12.1, 0.4,
    [[('Szukasz jednej linii na końcu: ', 12, GREY, False), ('INSTALACJA ZAKONCZONA — bez bledow', 12, GREEN, True),
      ('. Jeśli są błędy, skrypt policzy je i wypisze na czerwono.', 12, GREY, False)]])

# 8. CO JEST W FOLDERZE
s = slide(); header(s, 'zawartość', 'Co masz w tym folderze')
table(s, 0.62, 1.75, [4.3, 7.8],
      ['pozycja', 'co z tym zrobić'],
      [['ZAINSTALUJ.sh', 'URUCHAMIASZ. To jedyny plik, który odpalasz.'],
       ['INSTRUKCJA_WDROZENIA.pdf', 'Ten dokument.'],
       ['1_SKOPIUJ_NA_MASZYNE/', 'Skrypt bierze to sam. Nic tu nie robisz.'],
       ['2_CONFIGI/', 'Skrypt wybiera właściwy plik sam, na podstawie Twojej odpowiedzi.'],
       ['3_DASHBOARDY/', 'Zapas na wypadek, gdyby backup HA nie miał dashboardów.'],
       ['4_DODATKI/', 'Meshtastic CLI, ustawienia radia, Launcher (opcjonalny).']])
warn(s, 0.62, 4.75, 12.1, 'Katalogi 1 i 2 obsługuje skrypt — Ty ich nie dotykasz',
     'Muszą tylko leżeć obok ZAINSTALUJ.sh. Jeśli skopiujesz sam skrypt bez nich, zgłosi błąd i się zatrzyma.', col=CYAN)
txt(s, 0.62, 5.95, 12.1, 0.4,
    [[('Awaryjnie, gdyby backupu HA nie było: ', 12, GREY, False),
      ('dashboardy z katalogu 3 wklejasz w HA ręcznie, a token generujesz w HA i wklejasz do config.py.', 12, WHITE, False)]])

# 9. PORT ANTENY — pulapka
s = slide(); header(s, 'jedyna pułapka', 'Porty USB — dlaczego skrypt wykrywa je sam')
y = mono(s, 0.62, 1.7, 12.1, [
    '# tak wygladaja urzadzenia widziane po NAZWIE (a nie po numerze)',
    'ls -l /dev/serial/by-id/',
    '',
    'usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0 -> ttyUSB0   # radio LoRa',
    'usb-SONOFF_SONOFF_Dongle_Plus_MG24_5a1c...-if00-port0                 -> ttyUSB1   # Zigbee',
], fs=11)
warn(s, 0.62, y + 0.3, 12.1, 'Numery ttyUSB0 / ttyUSB1 potrafią zamienić się miejscami',
     'Zdarzyło się to naprawdę: Zigbee2MQTT miał wpisane /dev/ttyUSB0, a po restarcie pod tym numerem znalazło się '
     'radio LoRa. Zigbee nie wstał przez 10 dni, a dodatkowo psuł łączność radiową. Dlatego skrypt wpisuje pełną '
     'ścieżkę z by-id, która zawiera nazwę urządzenia i nigdy się nie zmienia.', col=RED)
txt(s, 0.62, y + 1.85, 12.1, 0.4,
    [[('Tę samą zasadę zastosuj w ustawieniach dodatku Zigbee2MQTT (pole „port"), jeśli kiedyś będziesz je zmieniać.', 13, WHITE, False)]])

# 10. URUCHOMIENIE
s = slide(); header(s, 'po instalacji', 'Uruchomienie')
txt(s, 0.62, 1.7, 12.1, 0.4, [[('Po restarcie maszyny program wstaje SAM. Ręcznie uruchamiasz tak:', 13, WHITE, False)]])
y = mono(s, 0.62, 2.15, 12.1, [
    'cd ~/meshtastic',
    'venv/bin/python -u Meshtastic_SCADA.py',
], fs=13)
txt(s, 0.62, y + 0.3, 12.1, 0.4, [[('Co powinieneś zobaczyć w pierwszych sekundach:', 13, CYAN, True)]])
mono(s, 0.62, y + 0.72, 12.1, [
    'STEP 5 - ANOMALIE + TRYB BRAMKI',
    'Role: GATEWAY (G1)',
    '[SLOT]  slotowanie ON: G1 okno 0-20s / cykl 60s',
    '[MQTT]  Connected',
    '[ANT]   ANT-1 (/dev/serial/by-id/usb-Silicon_Labs_CP2102...) connected',
    '[DISC]  Z2M: 194 devices, 78 monitored',
], fs=11, col=CYAN)
txt(s, 0.62, y + 2.35, 12.1, 0.4,
    [[('Linia ', 12, GREY, False), ('[ANT] ... connected', 12, GREEN, True),
      (' jest najważniejsza — oznacza, że radio odpowiada.', 12, GREY, False)]])


# 14. LAUNCHER (screenshot)
s = slide(); header(s, 'ułatwienie', 'Launcher — start i stop bez wpisywania komend')
txt(s, 0.62, 1.62, 12.1, 0.4,
    [[('Opcjonalne narzędzie (katalog ', 12, GREY, False), ('4_DODATKI/launcher_opcjonalny', 12, GREEN, True),
      ('). Uruchamiasz na swoim komputerze, otwierasz w przeglądarce ', 12, GREY, False),
      ('localhost:8765', 12, GREEN, True), ('.', 12, GREY, False)]])
if not pic(s, 'launcher.png', 0.62, 2.15, 12.1):
    txt(s, 0.62, 2.6, 12.1, 0.4, [[('(zrzut ekranu Launchera)', 12, DGREY, False)]])
txt(s, 0.62, 6.75, 12.1, 0.4,
    [[('Zasada: ', 12, AMBER, True), ('programy uruchamiaj z Launchera, nie ręcznie w terminalu — inaczej mogą powstać dwie kopie walczące o radio.', 12, GREY, False)]])

# 15. WERYFIKACJA (screenshot)
s = slide(); header(s, 'sprawdzenie', 'Skąd wiesz, że wszystko działa')
txt(s, 0.62, 1.6, 12.1, 0.4,
    [[('Otwórz dashboard supervisora. Tak wygląda system, który działa poprawnie:', 13, WHITE, False)]])
if not pic(s, 'sup_scada_gw.png', 2.35, 2.05, 8.6):
    txt(s, 0.62, 2.6, 12.1, 0.4, [[('(zrzut ekranu dashboardu)', 12, DGREY, False)]])
txt(s, 0.62, 6.55, 12.1, 0.7,
    [[('ONLINE', 12, GREEN, True), (' przy nazwie bramki  ·  ', 12, GREY, False),
      ('WSZYSTKO ZGODNE ✅', 12, GREEN, True), ('  ·  wszystkie 5 flag na zielono: DISC, PARAM, KALENDARZ, ANOMALIE, DANE', 12, GREY, False)],
     [('Kafelek OFFLINE pokazuje liczbę urządzeń, które nie odpowiadają. Zero albo kilka = dobrze.', 12, GREY, False)]], sp=1.35)

# 16. PULAPKI
s = slide(); header(s, 'zapamiętaj', 'Pięć rzeczy, które psują ten system najczęściej')
bullets(s, 0.62, 1.75, 12.1, [
    ('Numery portów USB ', '— zawsze /dev/serial/by-id/..., nigdy /dev/ttyUSB0. Numery się przestawiają.'),
    ('config.py jest INNY na każdej maszynie ', '— tokeny, lista urządzeń, port radia. Nigdy nie kopiuj go między maszynami bez poprawek.'),
    ('Nie kasuj urządzeń w Home Assistant ', '— zmieniaj im nazwę. Skasowanie gubi historię i psuje ekrany.'),
    ('Nie uruchamiaj skryptów gen_*_dashboard.py ', '— nadpiszą ręcznie ułożone ekrany.'),
    ('Przekaźniki na carporcie mają prąd tylko po zmierzchu ', '— w dzień pokazują się jako niedostępne i to jest NORMALNE. '
     'Systemu nie oceniaj w ciągu dnia.'),
], fs=13, sp=1.75)

# 17. DIAGNOSTYKA
s = slide(); header(s, 'diagnostyka', 'Gdy coś nie działa albo łącze się zapycha')
txt(s, 0.62, 1.65, 12.1, 0.4, [[('Program zapisuje CAŁY ruch radiowy do plików. Działa bez internetu.', 13, WHITE, False)]])
y = mono(s, 0.62, 2.1, 12.1, [
    '# podsumowanie ruchu z danego dnia',
    'venv/bin/python tools/lora_traffic_log.py --stats logs/traffic_sup_2026-08-24.tsv',
    '',
    '# CO ZAPYCHA LACZE + ile ramek dalo sie wyslac zbiorczo',
    'venv/bin/python tools/lora_traffic_log.py --congestion logs/traffic_sup_2026-08-24.tsv',
    '',
    '# ile pakietow zginelo naprawde (zestawia obie strony)',
    'venv/bin/python tools/lora_traffic_log.py --compare plik_supervisora.tsv plik_bramki.tsv',
], fs=11)
txt(s, 0.62, y + 0.3, 12.1, 0.4, [[('Gdy program w ogóle nie wstaje:', 13, CYAN, True)]])
mono(s, 0.62, y + 0.72, 12.1, [
    'tail -30 /tmp/test_transport.stderr.log     # ostatnie bledy',
    'tail -50 /tmp/test_transport.log            # zywy log pracy',
    'ls -l /dev/serial/by-id/                    # czy radio jest widoczne',
], fs=11, col=AMBER)

# 18. KONIEC
s = slide()
txt(s, 0.9, 2.6, 11.5, 0.5, [[('GOTOWE', 13, CYAN, True)]])
txt(s, 0.9, 3.0, 11.5, 0.9, [[('System stoi', 40, WHITE, True)]])
bullets(s, 0.9, 4.2, 11.5, [
    'Dashboard supervisora pokazuje ONLINE i WSZYSTKO ZGODNE',
    'Po restarcie prądu system wstaje sam',
    'Ruch radiowy zapisuje się do plików — można go później przeanalizować',
], fs=14, sp=1.6)

os.makedirs(OUT, exist_ok=True)
p = os.path.join(OUT, 'INSTRUKCJA_WDROZENIA.pptx')
prs.save(p)
print('zapisano:', p, '| slajdow:', len(prs.slides.__iter__.__self__._sldIdLst))
