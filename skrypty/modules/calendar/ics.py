"""ICS generation for the gateway local calendar (STEP 4 / krok 16).

Port treści ICS z gateway_v38._write_ics_file — ROZDZIELONE na:
  - build_ics(slots, mode_names, cal_name)  → czysta funkcja (string), smoke-test
  - write_ics_atomic(path, content, logger) → I/O: atomic tmp→rename + preserve owner
Gateway po odebraniu harmonogramu (CalendarTransfer.on_received → ScheduleManager.merge)
woła build_ics(...) i write_ics_atomic(...) aby HA local_calendar zobaczył nowe eventy.

Format slotu (z ScheduleManager): {start:"YYYY-MM-DD HH:MM:SS", end:..., mode:int, id, note}.
"""
import os
import time


def _ics_dt(val):
    """'2026-06-11 08:00:00' → '20260611T080000' (forma ICS bez TZ = local floating)."""
    return str(val).replace('-', '').replace(':', '').replace(' ', 'T')


def build_ics(slots, mode_names, cal_name="LoRa"):
    """Czysta funkcja: lista slotów → tekst ICS (CRLF). Jeden VEVENT na slot.
    UID zawiera timestamp → HA zawsze widzi 'nowe' eventy po aktualizacji (v38 trick)."""
    now = int(time.time())
    lines = [
        'BEGIN:VCALENDAR',
        'VERSION:2.0',
        'PRODID:-//LoRa BMS//Gateway//EN',
        f'X-WR-CALNAME:{cal_name}',
    ]
    for slot in slots:
        mode = slot.get('mode', 0)
        name = mode_names.get(mode, f"MODE_{mode}")
        sid = slot.get('id', f"s{_ics_dt(slot.get('start', ''))}")
        uid = f"lora-{sid}-{now}@lora-bms"
        lines += [
            'BEGIN:VEVENT',
            f'UID:{uid}',
            f'DTSTART:{_ics_dt(slot.get("start", ""))}',
            f'DTEND:{_ics_dt(slot.get("end", ""))}',
            f'SUMMARY:{name}',
            f'DESCRIPTION:mode={mode}',
            'END:VEVENT',
        ]
    lines.append('END:VCALENDAR')
    return '\r\n'.join(lines) + '\r\n'


def write_ics_atomic(path, content, logger=None):
    """I/O: zapisz ICS atomowo (tmp→os.replace) zachowując właściciela pliku/katalogu
    (Docker HA = inny uid). Zwraca True/False. Nie rzuca — loguje błąd uprawnień."""
    def _log(lvl, msg):
        if logger:
            getattr(logger, lvl, logger.info)('CAL', msg)
    try:
        orig_uid = orig_gid = None
        ref = path if os.path.exists(path) else os.path.dirname(path)
        if ref and os.path.exists(ref):
            st = os.stat(ref)
            orig_uid, orig_gid = st.st_uid, st.st_gid
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
        if orig_uid is not None and hasattr(os, 'chown'):
            try:
                os.chown(tmp, orig_uid, orig_gid)
            except Exception:
                pass
        os.replace(tmp, path)
        _log('info', f"✅ ICS zapisany → {path}")
        return True
    except PermissionError as e:
        _log('error', f"❌ ICS brak uprawnień: {e}  (chown {os.path.dirname(path)})")
        return False
    except Exception as e:
        _log('error', f"❌ ICS zapis nieudany: {e}")
        return False
