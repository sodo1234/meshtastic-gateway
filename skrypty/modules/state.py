"""Katalog stanu na DYSKU (v12.16, 2026-10-21, polecenie usera).

/tmp na bramkach i supie to tmpfs: brak prądu albo reboot kasował mapę sid, wysłany stan, hashe,
parametry z dashboardu, tryb, kalendarz, lustro supa i listę anomalii. System odbudowywał to przez
rozjazd hashy (minuty transferów), a parametry wracały do config.py. Teraz stan żyje w STATE_DIR.

Migracja: przy pierwszym imporcie kopiujemy istniejące /tmp/lora_* do STATE_DIR (tylko gdy w
STATE_DIR jeszcze ich nie ma) — przejście na dysk nie gubi bieżącego stanu.
"""
import glob
import os
import shutil

STATE_DIR = os.environ.get('LORA_STATE_DIR') or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'state')   # ~/meshtastic/state
_TMP = '/tmp/lora_'


def _migrate():
    os.makedirs(STATE_DIR, exist_ok=True)
    for src in glob.glob(_TMP + '*'):
        dst = os.path.join(STATE_DIR, os.path.basename(src))
        if os.path.isfile(src) and not os.path.exists(dst):
            try:
                shutil.copy2(src, dst)
            except OSError:
                pass


def remap(obj):
    """Ścieżki '/tmp/lora_*' (w configach maszyn) → STATE_DIR. Rekurencyjnie, w miejscu."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            obj[k] = remap(v)
    elif isinstance(obj, list):
        return [remap(v) for v in obj]
    elif isinstance(obj, str) and obj.startswith(_TMP):
        return os.path.join(STATE_DIR, os.path.basename(obj))
    return obj


_migrate()


if __name__ == '__main__':
    c = {'a': {'persist_path': '/tmp/lora_x.json', 'log_file': '/tmp/test_transport.log'}}
    remap(c)
    assert c['a']['persist_path'] == os.path.join(STATE_DIR, 'lora_x.json'), c
    assert c['a']['log_file'] == '/tmp/test_transport.log', 'log zostaje w /tmp'
    print('OK state:', STATE_DIR)
