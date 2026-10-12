"""Hook UserPromptSubmit: JEV podpowiada poziom delegacji (0-3) jako jedna linia kontekstu.
Cichy no-op gdy: brak klucza, blad, pewnosc < 0.85, prompt krotszy niz 40 znakow, slash-komenda.
Nigdy nie blokuje promptu; zadnych uprawnien nie decyduje.
"""
import json, os, sys

sys.path.insert(0, os.path.dirname(__file__))
from jev_decide import ask, LEVELS

try:
    prompt = json.load(sys.stdin).get("prompt", "")
except Exception:
    sys.exit(0)
if len(prompt) < 40 or prompt.lstrip().startswith("/"):
    sys.exit(0)
res = ask({"zadanie": prompt[:1500]}, {"level": {
    "type": "choice", "criteria": LEVELS,
    "instructions": "Ktory poziom delegacji najlepiej pasuje do zadania?"}})
a = (res or {}).get("answers", {}).get("level")
if a and a.get("confidence", 0) >= 0.85:
    msg = (f"[JEV] sugerowany poziom delegacji: {a['choice']} (pewnosc {a['confidence']:.2f}). "
           "Poziomy: 0 sam, 1 Gemini, 2 Codex, 3 claude -p na kluczu API. "
           "Zywe systemy (G1/G2/sup/deploy) nigdy ponizej 2. Przy watpliwosci decyduj sam.")
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                              "additionalContext": msg}}, ensure_ascii=False))
