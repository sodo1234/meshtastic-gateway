"""Generuje docs/PROTOKOL_LORA.md z tabel codec.py.

Uruchom z katalogu ``skrypty``: ``python docs/gen_protokol.py``.
"""
import html
import json
import sys
from pathlib import Path


SKRYPTY = Path(__file__).resolve().parents[1]
if str(SKRYPTY) not in sys.path:
    sys.path.insert(0, str(SKRYPTY))

from modules.transport.codec import KEYS, TYPES, encode  # noqa: E402
from modules.transport.codec_smoke_test import SAMPLES  # noqa: E402


CATEGORIES = {
    "Łącze, czas i zgodność": ["hb", "ping", "pong", "sup_ping", "sup_pong", "sync", "sync_req", "zg"],
    "Sterowanie i dane": ["b", "back", "cmd", "cmdm", "req", "st", "stm", "set_mode", "vbtn", "vbtn_ack", "vsw", "vsw_st"],
    "Discovery i migawki": ["db", "disc", "disc_ack", "disc_ap", "disc_meta", "disc_vio", "dsnap_req"],
    "Anomalie": ["ab", "ac_all", "ac_b", "an_d", "ansnap_req", "dump_anom"],
    "Parametry": ["cfg", "param_upd", "params", "params_req"],
    "ReliableTransfer": ["rt_begin", "rt_chunk", "rt_cack", "rt_end", "rt_ack"],
    "Kalendarz": ["cal_begin", "cal_chunk", "cal_cack", "cal_end", "cal_ack", "cal_pull", "cal_req"],
}


PURPOSE = {
    "ab": "Paczka zdarzeń anomalii.", "ac_all": "Kasowanie całego kubełka anomalii.",
    "ac_b": "Kasowanie wskazanych anomalii.", "an_d": "Delta anomalii z kontrolnym hashem.",
    "ansnap_req": "Żądanie pełnej migawki anomalii.", "b": "Batch stanu i dostępności urządzeń.",
    "back": "ACK batcha priorytetowego.", "cal_ack": "Końcowy ACK/NACK kalendarza.",
    "cal_begin": "Otwarcie transferu kalendarza.", "cal_cack": "ACK pojedynczego chunka kalendarza.",
    "cal_chunk": "Fragment skompresowanego kalendarza.", "cal_end": "Zamknięcie transferu kalendarza.",
    "cal_pull": "Polecenie wysłania kalendarza z bramki.", "cal_req": "Prośba bramki o kalendarz supervisora.",
    "cfg": "Zgodność ze starą ramką konfiguracji.", "cmd": "Sterowanie jednym urządzeniem.",
    "cmdm": "Sterowanie grupą urządzeń.", "db": "Stara, porcjowana mapa urządzeń.",
    "disc": "Żądanie discovery/mapy.", "disc_ack": "Stary ACK discovery.",
    "disc_ap": "Lekka mapa monitored/priority.", "disc_meta": "Metadane i hash mapy urządzeń.",
    "disc_vio": "Mapa wirtualnych wejść/wyjść.", "dsnap_req": "Żądanie pełnej migawki stanu.",
    "dump_anom": "Wymuszenie pełnego raportu anomalii.", "hb": "Heartbeat bramki z licznikami i hashami.",
    "param_upd": "Potwierdzony stan parametrów bramki.", "params": "Propozycja parametrów z supervisora.",
    "params_req": "Żądanie pełnego stanu parametrów.", "ping": "Sonda bramki.",
    "pong": "Odpowiedź bramki z diagnostyką HB.", "req": "Odświeżenie jednego urządzenia.",
    "rt_ack": "Końcowy ACK/NACK ReliableTransfer.", "rt_begin": "Otwarcie transferu pliku/payloadu.",
    "rt_cack": "ACK chunka lub okna chunków.", "rt_chunk": "Fragment skompresowanego payloadu.",
    "rt_end": "Zamknięcie transferu wraz z hashami.", "set_mode": "Zmiana trybu pracy bramki.",
    "st": "Potwierdzenie stanu jednego urządzenia.", "stm": "Zbiorcze potwierdzenia stanów.",
    "sup_ping": "Sonda łącza bramka→supervisor.", "sup_pong": "Odpowiedź supervisora z czasem i zgodnością.",
    "sync": "Synchronizacja czasu bramki.", "sync_req": "Żądanie synchronizacji czasu.",
    "vbtn": "Naciśnięcie wirtualnego przycisku.", "vbtn_ack": "ACK wirtualnego przycisku.",
    "vsw": "Ustawienie wirtualnego przełącznika.", "vsw_st": "Potwierdzenie stanu wirtualnego przełącznika.",
    "zg": "Push czterech flag zgodności.",
}


WHEN = {
    "hb": "Okresowo i po zmianie trybu.", "ping": "Przycisk/test startowy lub retry sondy.",
    "pong": "Po odebraniu `ping`.", "sup_ping": "Po ciszy na łączu do supervisora.",
    "sup_pong": "Natychmiast po `sup_ping`.", "sync": "Na żądanie lub przy złej jakości czasu.",
    "sync_req": "Gdy bramka potrzebuje czasu.", "zg": "Po zmianie wyniku zgodności.",
    "b": "Po zmianie/flushu batchera i przy pełnym sweepie.", "back": "Po priorytetowym `b` z polem `p`.",
    "cmd": "Klik lub retry dla jednego urządzenia.", "cmdm": "Flush grupy komend.",
    "req": "Klik Odśwież.", "st": "Po potwierdzeniu stanu przez Z2M.",
    "stm": "Flush okna zbiorczych statusów.", "set_mode": "Zmiana selecta trybu.",
    "vbtn": "Klik wirtualnego przycisku.", "vbtn_ack": "Po obsłużeniu `vbtn`.",
    "vsw": "Zmiana wirtualnego przełącznika.", "vsw_st": "Po zastosowaniu `vsw`.",
    "db": "Fallback discovery bez transferu plikowego.", "disc": "Start, przycisk lub rozjazd hasha.",
    "disc_ack": "Tylko kompatybilność starego discovery.", "disc_ap": "Po zmianie zbiorów monitored/priority.",
    "disc_meta": "Przy ogłoszeniu mapy lub meta-only.", "disc_vio": "Podczas discovery VIO.",
    "dsnap_req": "Gdy hash danych/statusu się rozjeżdża.",
    "ab": "Po flushu bufora anomalii.", "ac_all": "Klik kasowania kubełka.",
    "ac_b": "Klik kasowania pojedynczej anomalii.", "an_d": "Po dodaniu/usunięciu anomalii.",
    "ansnap_req": "Przy rozjeździe hasha anomalii.", "dump_anom": "Przycisk lub cykl reconcile.",
    "cfg": "Wyłącznie dla zgodności ze starym peerem.", "param_upd": "Po lokalnej zmianie lub zastosowaniu `params`.",
    "params": "Po kliknięciu Wyślij konfigurację.", "params_req": "Start lub auto-heal parametrów.",
    "rt_begin": "Raz na początek każdego transferu.", "rt_chunk": "Dla każdego fragmentu/retransmisji.",
    "rt_cack": "Po chunku albo końcu okna.", "rt_end": "Po wszystkich chunkach i przy retry końca.",
    "rt_ack": "Po złożeniu/zweryfikowaniu transferu.",
    "cal_begin": "Raz na początek synchronizacji kalendarza.", "cal_chunk": "Dla każdego fragmentu/retransmisji.",
    "cal_cack": "Po każdym fragmencie kalendarza.", "cal_end": "Po wszystkich fragmentach i przy retry.",
    "cal_ack": "Po złożeniu/zweryfikowaniu kalendarza.", "cal_pull": "Klik Pull Calendar.",
    "cal_req": "Gdy bramka nie ma kalendarza.",
}


G_TO_SUP = {"ab", "an_d", "b", "cal_req", "db", "disc_ack", "disc_ap", "disc_meta", "disc_vio",
            "hb", "param_upd", "pong", "st", "stm", "sup_ping", "sync_req", "vbtn_ack", "vsw_st"}
SUP_TO_G = {"ac_all", "ac_b", "ansnap_req", "back", "cal_pull", "cfg", "cmd", "cmdm", "disc",
            "dsnap_req", "dump_anom", "params", "params_req", "ping", "req", "set_mode", "sup_pong",
            "sync", "vbtn", "vsw", "zg"}
BOTH = {"rt_begin", "rt_chunk", "rt_cack", "rt_end", "rt_ack",
        "cal_begin", "cal_chunk", "cal_cack", "cal_end", "cal_ack"}
REACTIVE_SUP = {"rt_cack", "rt_ack", "cal_cack", "cal_ack", "back", "sup_pong"}


FIELD_INFO = {
    "t": ("enum", "typ ramki"), "g": ("str", "identyfikator/adres bramki"),
    "ts": ("int", "czas Unix"), "d": ("array/object", "dane ramki"),
    "a": ("array", "dodane anomalie"), "r": ("array", "usunięte anomalie"),
    "h": ("str", "hash lub hash supervisora"), "hash": ("str", "hash mapy"),
    "dev_n": ("int", "liczba urządzeń"), "p": ("int", "id batcha priorytetowego"),
    "b": ("str", "kubełek anomalii"), "tid": ("str", "id transferu"),
    "kind": ("enum", "rodzaj payloadu"), "dir": ("enum", "kierunek/tryb kalendarza"),
    "n": ("int", "liczba chunków"), "s": ("int", "numer chunka"),
    "crc": ("str", "CRC/MD5-4 skompresowanej całości"), "win": ("int", "rozmiar okna"),
    "w": ("int", "znacznik końca okna"), "m": ("array/int", "braki lub tryb"),
    "miss": ("array[int]", "brakujące chunki"), "ok": ("0/1", "wynik transferu"),
    "hs": ("array[str]", "hashe końcowe danych/statusu/anomalii"),
    "up": ("int", "uptime w sekundach"), "dev": ("int", "liczba urządzeń"),
    "mon": ("int/array", "monitored: liczba lub SID-y"), "pri": ("int/array", "priority: liczba lub SID-y"),
    "cal": ("str", "hash kalendarza"), "ph": ("str", "hash parametrów"),
    "dh": ("str", "hash danych"), "sh": ("str", "hash statusu"), "ah": ("str", "hash anomalii"),
    "gm": ("str", "tryb bramki"), "ga": ("0/1", "aktywność bramki"), "tq": ("str", "jakość czasu"),
    "c": ("str", "capability"), "v": ("any", "wartość"), "id": ("str", "id VIO"),
    "sec": ("int", "czas Unix supervisora"), "zg": ("str", "flagi zgodności"),
    "z": ("str", "flagi zgodności"), "to": ("object", "timeouty legacy"),
}


def dumps(frame):
    return json.dumps(frame, separators=(",", ":"), ensure_ascii=False)


def direction(frame_type):
    if frame_type in BOTH:
        return "obie"
    if frame_type in G_TO_SUP:
        return "G→SUP"
    if frame_type in SUP_TO_G:
        return "SUP→G"
    raise KeyError(frame_type)


def slot_rule(frame_type):
    if frame_type in BOTH:
        return "TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast"
    if frame_type in G_TO_SUP:
        return "slot bramki"
    if frame_type in REACTIVE_SUP:
        return "natychmiastowa odpowiedź SUP"
    return "slot supervisora"


def fields_cell(frame_type, sample):
    key_map = KEYS.get(frame_type, {})
    parts = []
    for key in sample:
        wire = key_map.get(key, key)
        typ, meaning = FIELD_INFO.get(key, ("any", "pole protokołu"))
        parts.append(f"`{key}→{wire}` ({typ}): {meaning}")
    return "<br>".join(parts)


def code(value):
    return "<code>" + html.escape(value).replace("|", "&#124;") + "</code>"


def generate():
    assert set(SAMPLES) == set(TYPES["t"])
    listed = {item for frames in CATEGORIES.values() for item in frames}
    assert listed == set(TYPES["t"]), (sorted(set(TYPES["t"]) - listed), sorted(listed - set(TYPES["t"])))

    lines = [
        "# Protokół LoRa — skrócony JSON",
        "",
        "> Plik generowany przez `python docs/gen_protokol.py`. Nie edytuj tabel ręcznie; ",
        "> źródłem prawdy są `modules/transport/codec.py:TYPES` i `KEYS`.",
        "",
        "Kodek działa wyłącznie na granicy `LoraTransport`: przed `json.dumps` przy TX i zaraz po ",
        "`json.loads` przy RX. Kod domenowy, dispatchery i dashboardy nadal używają długich nazw. ",
        "Skracane są tylko klucze najwyższego poziomu; zagnieżdżone payloady pozostają bez zmian. ",
        "Dekoder akceptuje również starą, długą postać. `g` nigdy nie jest przemianowane.",
        "",
        "## Tabele źródłowe TYPES / KEYS",
        "",
        "### Typy ramek (`t`)",
        "",
        "| Długa nazwa | Na radiu |",
        "|---|---|",
    ]
    for long, short in TYPES["t"].items():
        lines.append(f"| `{long}` | `{short}` |")

    lines += ["", "### Rodzaje payloadów ReliableTransfer (`kind`)", "", "| Długa nazwa | Na radiu |", "|---|---|"]
    for long, short in TYPES["kind"].items():
        lines.append(f"| `{long}` | `{short}` |")
    lines += ["", "### Kierunki kalendarza (`dir`)", "", "| Długa nazwa | Na radiu |", "|---|---|"]
    for long, short in TYPES["dir"].items():
        lines.append(f"| `{long}` | `{short}` |")
    lines += ["", "### Klucze najwyższego poziomu", "", "| Zakres ramki | Długi klucz | Na radiu |", "|---|---|---|"]
    for frame_type, table in KEYS.items():
        for long, short in table.items():
            lines.append(f"| `{frame_type}` | `{long}` | `{short}` |")

    lines += [
        "", "## Katalog ramek", "",
        "Reguła slotów: bramka nadaje wyłącznie w swoim slocie. Supervisor nadaje ramki inicjowane ",
        "w slocie supervisora; odpowiedzi `cack`/`ack`, `back` i `sup_pong` wysyła natychmiast.",
    ]
    for category, frames in CATEGORIES.items():
        lines += [
            "", f"### {category}", "",
            "| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |",
            "|---|---|---|---|---|---|---|",
        ]
        for frame_type in frames:
            sample = SAMPLES[frame_type]
            wire = encode(sample)
            before, after = dumps(sample), dumps(wire)
            example = f"{code(before)} ({len(before.encode('utf-8'))} B)<br>{code(after)} ({len(after.encode('utf-8'))} B)"
            lines.append(
                f"| `{frame_type}` → `{TYPES['t'][frame_type]}` | {direction(frame_type)} | "
                f"{PURPOSE[frame_type]} | {fields_cell(frame_type, sample)} | {WHEN[frame_type]} | "
                f"{slot_rule(frame_type)} | {example} |")

    lines += [
        "", "## Payloady ReliableTransfer", "",
        "| `kind` długi→krótki | Kierunek | Zawartość |",
        "|---|---|---|",
        "| `devmap→md` | G→SUP | pełna mapa urządzeń |",
        "| `avail→av` | G→SUP | pełna migawka dostępności |",
        "| `bdata→bd` | G→SUP | skompresowany batch danych |",
        "| `ansnap→an` | G→SUP | pełna migawka anomalii |",
        "| `dsnap→ds` | G→SUP | pełna migawka stanu/statusu |",
        "| `mass_offline→mo` | G→SUP | masowe oznaczenie urządzeń offline |",
        "",
        "## Niezmienniki", "",
        "- Każda adresowana ramka zachowuje identyfikator bramki w jednoliterowym kluczu `g`.",
        "- Nieznane typy i klucze przechodzą przez dekoder bez zmian.",
        "- `decode(encode(frame)) == frame` dla wszystkich ramek katalogu.",
        "- Każdy przykład radiowy ma nie więcej niż 220 B; większe dane używają transferu chunkowanego.",
        "- `tid`, `crc`, `hs`, `ph` i `ts` już mają 1–3 znaki, więc pozostają bez zmian.",
        "- Krótkie klucze istniejące od dawna (`g`, `d`, `h`, `m`, `s`, `p`, `v`, `c` itd.) nie są przemianowywane.",
        "",
    ]
    out = Path(__file__).with_name("PROTOKOL_LORA.md")
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wygenerowano {out}: {len(TYPES['t'])} typów ramek")


if __name__ == "__main__":
    generate()

