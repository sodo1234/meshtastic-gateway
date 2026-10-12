"""Hook PreToolUse(Agent): pilnuje delegacji. Czysty kod, bez wywolan API.
1. Agent bez `model` -> deny (zasada z CLAUDE.md).
2. Model drozszy niz poziom z JEV dla tej sesji (0-1 a Opus/Fable) -> ostrzezenie w kontekscie;
   deny dopiero z JEV_ENFORCE=1 (najpierw tydzien logow).
Nigdy nie zawiesza narzedzia: blad hooka = przepusc.
"""
import json, os, sys

LOG = os.path.join(os.path.dirname(__file__), "jev_log.jsonl")
EXPENSIVE = ("opus", "fable")


def out(kind, text):
    key = "permissionDecisionReason" if kind == "deny" else "additionalContext"
    body = {"hookEventName": "PreToolUse", key: text}
    if kind == "deny":
        body["permissionDecision"] = "deny"
    print(json.dumps({"hookSpecificOutput": body}, ensure_ascii=False))


try:
    d = json.load(sys.stdin)
    model = (d.get("tool_input") or {}).get("model")
except Exception:
    sys.exit(0)

if not model:
    out("deny", "Agent bez parametru model. Podaj haiku/sonnet/opus/fable wg poziomu delegacji (CLAUDE.md).")
    sys.exit(0)

level = None
try:
    with open(LOG, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("sid") == d.get("session_id") and r.get("conf", 0) >= 0.85:
                level = str(r.get("level"))
except (OSError, ValueError):
    pass

if level in ("0", "1") and any(m in model.lower() for m in EXPENSIVE):
    msg = f"[JEV] ten prompt ocenony na poziom {level}, a Agent dostaje model {model}. Uzyj haiku albo zrob sam."
    out("deny" if os.environ.get("JEV_ENFORCE") == "1" else "warn", msg)
