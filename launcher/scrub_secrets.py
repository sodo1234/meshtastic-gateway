"""Scrub hardcoded secrets from scripts before git commit.

Bezpieczeństwo: NIE trzyma hasła na sztywno w tym pliku (kiedyś trzymał → wyciek
do historii git). Wzorzec hasła do wyczyszczenia pobiera z:
  1) zmiennej środowiskowej SCRUB_MQTT_PASS, albo
  2) lokalnego skrypty/config.py (jego prawdziwa wartość jest trzymana poza gitem).
- hasło MQTT -> 'REPLACE_ME'
- dowolny JWT (eyJ...) -> '' (gotowe pod wstrzyknięcie z env)
"""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGETS = [
    "skrypty/gateway_v10.py",
    "skrypty/gateway_v23.py",
    "skrypty/gateway_v38.py",
    "skrypty/supervisor_v10.py",
    "skrypty/supervisor_v23.py",
    "skrypty/supervisor_v38.py",
    "launcher/app.py",
]


def _mqtt_pass():
    """Wzorzec hasła do scrubowania — z env lub z lokalnego config.py. Nigdy hardcode tutaj."""
    v = os.environ.get("SCRUB_MQTT_PASS")
    if v:
        return v
    cfg = ROOT / "skrypty" / "config.py"
    if cfg.exists():
        m = re.search(r'MQTT_PASS\s*=\s*"([^"]+)"', cfg.read_text(encoding="utf-8"))
        if m and m.group(1) and m.group(1) not in ("REPLACE_ME", ""):
            return m.group(1)
    return None


JWT_PATTERN = re.compile(r'eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+')
_mqtt = _mqtt_pass()
_mqtt_pat = re.compile(re.escape(_mqtt)) if _mqtt else None

changed = 0
for rel in TARGETS:
    p = ROOT / rel
    if not p.exists():
        print(f"SKIP {rel} (missing)")
        continue
    src = p.read_text(encoding='utf-8')
    new = _mqtt_pat.sub('REPLACE_ME', src) if _mqtt_pat else src
    new = JWT_PATTERN.sub('', new)
    if new != src:
        p.write_text(new, encoding='utf-8')
        print(f"OK   {rel}")
        changed += 1
    else:
        print(f"--   {rel} (no secrets found)")

if _mqtt is None:
    print("NOTE: brak wzorca hasla (ustaw SCRUB_MQTT_PASS lub MQTT_PASS w skrypty/config.py) — scrubbowano tylko JWT")
print(f"\n{changed}/{len(TARGETS)} files modified")
