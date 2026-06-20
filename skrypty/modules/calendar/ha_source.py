"""HA Calendar jako źródło harmonogramu (STEP 4 / krok 16) — strona SUPERVISORA.

Supervisor czyta encję `calendar.lora_global` (+ ewentualnie per-bramka `calendar.lora_g1`)
przez HA REST API, mapuje eventy na sloty i ładuje do ScheduleManager (replace_global).
Tryb NIE jest zgadywany tutaj — zostaje `note=summary`, a ScheduleManager._detect_mode
rozpoznaje tryb po tytule (sort po długości: 'BRAK PRODUKCJI' przed 'PRODUKCJA').

ROZDZIAŁ:
  - parse_ha_events(events)         → czysta funkcja (lista slotów), smoke-test
  - fetch_ha_calendar(url, token …) → I/O urllib (GET /api/calendars/<id>)
  - reload_local_calendar(url, …)   → I/O urllib (POST reload local_calendar) — strona bramki
"""
import json
import time
import urllib.request


def parse_ha_events(events):
    """Czysta: lista eventów HA REST → lista slotów {id,start,end,note,origin}.
    Obsługuje start/end jako dateTime (z czasem) lub date (cały dzień)."""
    slots = []
    for evt in events or []:
        start = evt.get('start', {})
        end = evt.get('end', {})
        if isinstance(start, dict):
            start = start.get('dateTime') or start.get('date') or ''
        if isinstance(end, dict):
            end = end.get('dateTime') or end.get('date') or ''
        if not start or not end:
            continue
        slots.append({
            "id": evt.get('uid', f"ha_{len(slots)}"),
            "start": start,
            "end": end,
            "note": (evt.get('summary') or '')[:30],
            "origin": "ha_api",
        })
    return slots


def fetch_ha_calendar(url, token, calendar_id='calendar.lora_global',
                      days=90, timeout=10, logger=None):
    """I/O: pobierz eventy z HA REST i sparsuj na sloty. [] gdy brak tokenu/błąd."""
    def _log(lvl, msg):
        if logger:
            getattr(logger, lvl, logger.info)('CAL', msg)
    if not token:
        _log('warn', f"⚠️ brak HA tokenu — pomijam {calendar_id}")
        return []
    base = url.rstrip('/')
    start = time.strftime('%Y-%m-%dT00:00:00')
    end = time.strftime('%Y-%m-%dT23:59:59', time.localtime(time.time() + days * 86400))
    req_url = f"{base}/api/calendars/{calendar_id}?start={start}&end={end}"
    try:
        req = urllib.request.Request(req_url, headers={
            'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            events = json.loads(resp.read())
        slots = parse_ha_events(events)
        _log('info', f"📅 pobrano {len(slots)} eventów z {calendar_id}")
        return slots
    except Exception as e:
        _log('error', f"❌ HA fetch {calendar_id}: {e}")
        return []


def write_ha_calendar(url, token, calendar_id, slots, mode_names,
                      days=90, timeout=10, logger=None):
    """I/O (supervisor, REVERSE sync gw→sup): nadpisz encję `calendar.lora_<gw>` na HA
    supervisora harmonogramem z bramki (GET istniejące → DELETE → POST nowe).
    Port supervisor_v38._sync_to_ha_calendar. Zwraca (deleted, created) lub (0,0)."""
    def _log(lvl, msg):
        if logger:
            getattr(logger, lvl, logger.info)('CAL', msg)
    if not token:
        _log('info', f"📅 {calendar_id} → brak HA tokenu, pomijam zapis (reverse)")
        return (0, 0)
    base = url.rstrip('/')
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    start = time.strftime('%Y-%m-%dT00:00:00')
    end = time.strftime('%Y-%m-%dT23:59:59', time.localtime(time.time() + days * 86400))
    try:
        req = urllib.request.Request(
            f"{base}/api/calendars/{calendar_id}?start={start}&end={end}", headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            existing = json.loads(resp.read())
        deleted = 0
        for evt in existing:
            uid = evt.get('uid', '')
            if not uid:
                continue
            try:
                dr = urllib.request.Request(
                    f"{base}/api/calendars/{calendar_id}/{uid}", method='DELETE', headers=headers)
                urllib.request.urlopen(dr, timeout=timeout)
                deleted += 1
            except Exception:
                pass
        created = 0
        for slot in slots:
            mode = slot.get('mode', 0)
            body = json.dumps({
                "summary": mode_names.get(mode, f"MODE_{mode}"),
                "dtstart": str(slot.get('start', '')).replace(' ', 'T'),
                "dtend": str(slot.get('end', '')).replace(' ', 'T'),
            }).encode()
            try:
                pr = urllib.request.Request(
                    f"{base}/api/calendars/{calendar_id}", data=body, headers=headers, method='POST')
                urllib.request.urlopen(pr, timeout=timeout)
                created += 1
            except Exception as e:
                _log('warn', f"POST {calendar_id}: {e}")
        _log('info', f"✅ HA {calendar_id}: skasowano {deleted}, utworzono {created}")
        return (deleted, created)
    except Exception as e:
        _log('error', f"❌ HA write {calendar_id}: {e}")
        return (0, 0)


def reload_local_calendar(url, token, timeout=10, logger=None):
    """I/O (bramka): przeładuj integrację local_calendar po zapisie ICS, by HA wczytał plik."""
    def _log(lvl, msg):
        if logger:
            getattr(logger, lvl, logger.info)('CAL', msg)
    if not token:
        return False
    base = url.rstrip('/')
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    try:
        req = urllib.request.Request(f"{base}/api/config/config_entries/entry", headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            entries = json.loads(resp.read())
        for entry in entries:
            if entry.get('domain') == 'local_calendar':
                eid = entry['entry_id']
                rr = urllib.request.Request(
                    f"{base}/api/config/config_entries/entry/{eid}/reload",
                    data=b'', headers=headers, method='POST')
                urllib.request.urlopen(rr, timeout=timeout)
                _log('info', f"✅ HA reload local_calendar {eid[:8]}")
                return True
    except Exception as e:
        _log('warn', f"⚠️ HA reload nieudany: {e} — restart HA ręcznie / poczekaj")
    return False
