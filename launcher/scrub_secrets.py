"""Scrub hardcoded secrets from all scripts before git commit.
- MQTT password 'REPLACE_ME' -> 'REPLACE_ME'
- Any JWT (eyJ...) -> '' (empty string, ready for env injection later)
Run once before initial commit.
"""
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

MQTT_PASS_PATTERN = re.compile(r'REPLACE_ME')
JWT_PATTERN = re.compile(r'eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+')

changed = 0
for rel in TARGETS:
    p = ROOT / rel
    if not p.exists():
        print(f"SKIP {rel} (missing)")
        continue
    src = p.read_text(encoding='utf-8')
    new = MQTT_PASS_PATTERN.sub('REPLACE_ME', src)
    new = JWT_PATTERN.sub('', new)
    if new != src:
        p.write_text(new, encoding='utf-8')
        n_mqtt = len(MQTT_PASS_PATTERN.findall(src))
        n_jwt = len(JWT_PATTERN.findall(src))
        print(f"OK   {rel}  mqtt={n_mqtt}  jwt={n_jwt}")
        changed += 1
    else:
        print(f"--   {rel} (no secrets found)")

print(f"\n{changed}/{len(TARGETS)} files modified")
