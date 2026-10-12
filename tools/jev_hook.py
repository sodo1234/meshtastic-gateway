"""Hook UserPromptSubmit: JEV wskazuje poziom delegacji (0-3), wynik trafia do kontekstu i do jev_log.jsonl.
Cichy no-op gdy: brak klucza, blad, pewnosc < 0.85, prompt < 40 znakow, slash-komenda,
oraz dla danych zakladowych (Faktury/energetyka) - te NIE wychodza poza maszyne.
Nigdy nie blokuje promptu; zadnych uprawnien nie decyduje.
"""
import json, os, re, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from jev_decide import ask, LEVELS

LOG = os.path.join(os.path.dirname(__file__), "jev_log.jsonl")
# dane zakladowe: nie wysylamy do zewnetrznego API (feedback_corporate_data_local_only)
PRIVATE = re.compile(r"faktur|licznik|kwh|mwh|tauron|energety|chp|rdn|kwadrans|book2|n8n\\faktury", re.I)

try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
prompt, cwd = d.get("prompt", ""), d.get("cwd", "")
LOCK = os.path.join(os.path.dirname(__file__), "jev_private_sessions.txt")
sid = d.get("session_id") or ""
try:
    locked = sid in open(LOCK, encoding="utf-8").read().split()
except OSError:
    locked = False
if PRIVATE.search(prompt) or PRIVATE.search(cwd):
    # sesja dotknela danych zakladowych: do konca sesji NIC nie idzie do JEV (kolejne prompty bywaja bez slow kluczowych)
    if not locked and sid:
        with open(LOCK, "a", encoding="utf-8") as f:
            f.write(sid + "\n")
    sys.exit(0)
if locked or len(prompt) < 40 or prompt.lstrip().startswith("/"):
    sys.exit(0)

t0 = time.time()
try:
    res = ask({"zadanie": prompt[:1500]}, {"level": {
        "type": "choice", "criteria": LEVELS,
        "instructions": "Ktory poziom delegacji najlepiej pasuje do zadania?"}})
except Exception:
    sys.exit(0)
a = (res or {}).get("answers", {}).get("level")
conf = (a or {}).get("confidence", 0)
try:
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": int(t0), "sid": d.get("session_id"), "len": len(prompt),
                            "level": (a or {}).get("choice"), "conf": conf,
                            "ms": int((time.time() - t0) * 1000)}) + "\n")
except OSError:
    pass
if a and conf >= 0.85:
    lvl = a["choice"]
    if str(lvl) == "0":
        msg = f"[JEV] poziom 0 (pewnosc {conf:.2f}): zrob sam, bez sub-agenta."
    else:
        msg = (f"[JEV] poziom {lvl} (pewnosc {conf:.2f}): DELEGUJ, nie rob sam. "
               "1 Haiku, 2 Opus/Codex, 3 Fable. Zawsze podaj model w Agent. "
               "Zywe systemy (G1/G2/sup/deploy) nigdy ponizej 2. Odstap tylko z uzasadnieniem.")
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                              "additionalContext": msg}}, ensure_ascii=False))
