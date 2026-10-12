"""Dispatcher zadan: JEV wybiera poziom, wykonuje tani worker (oszczedza limit subskrypcji Claude).

  python tools/dispatch.py [--level N] [--cwd DIR] [--budget USD] "zadanie"

Poziomy: 0 = zadanie dla sesji lokalnej (nic nie uruchamia), 1 = Gemini CLI (Vertex),
2 = Codex (licencja ChatGPT), 3 = `claude -p --bare` na kluczu API (kredyty, nie subskrypcja).
Zasady: JEV tylko PODNOSI/ustala poziom przy pewnosci >= 0.85; zadania dotykajace zywych
systemow (G1/G2/sup/deploy/produkcja) dostaja min. poziom 2; poziom 3 bez ANTHROPIC_API_KEY
jest ODMAWIANY (inaczej claude -p zjadlby subskrypcje). Wynik: ostatnie 1500 znakow + log JSONL.
"""
import json, os, re, subprocess, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from jev_decide import ask, LEVELS

CODEX = os.path.expanduser("~/.claude/plugins/cache/openai-codex/codex/1.0.6/scripts/codex-companion.mjs")
LOG = os.path.join(os.path.dirname(__file__), "dispatch.log")
LIVE = re.compile(r"\b(G1|G2|sup\w*|deploy|wdr\w+|produkc\w+|ssh|restart)\b", re.I)


def pick_level(text):
    res = ask({"zadanie": text}, {"level": {
        "type": "choice", "criteria": LEVELS,
        "instructions": "Ktory poziom delegacji najlepiej pasuje do zadania?"}})
    a = (res or {}).get("answers", {}).get("level")
    lvl, conf = (int(a["choice"]), a["confidence"]) if a else (None, 0.0)
    if lvl is None or conf < 0.85:
        lvl = 2                      # niepewny JEV -> sredni poziom, nie 0/1
    if LIVE.search(text):
        lvl = max(lvl, 2)            # zywe systemy: nigdy ponizej Codexa
    return lvl, conf


def run(level, text, cwd, budget):
    if level == 0:
        return "POZIOM 0: zadanie dla sesji lokalnej (nic nie uruchomiono)."
    if level == 1:
        cmd = ["gemini", "--skip-trust", "-m", "gemini-3-pro-preview", "-p", text]
    elif level == 2:
        cmd = ["node", CODEX, "task", "--write", "--fresh", "--effort", "high", text]
    else:
        key = os.environ.get("DISPATCH_ANTHROPIC_KEY")
        if not key:
            return "ODMOWA: brak DISPATCH_ANTHROPIC_KEY (bez klucza poszloby z subskrypcji). Uruchom USTAW-KLUCZ-API-CLAUDE.ps1."
        env = dict(os.environ, ANTHROPIC_API_KEY=key)   # klucz tylko dla tego procesu
        cmd = ["claude", "-p", text, "--bare", "--model", "sonnet",
               "--max-budget-usd", str(budget), "--permission-mode", "acceptEdits"]
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=1800,
                       encoding="utf-8", errors="replace", shell=(os.name == "nt" and level == 1),
                       env=env if level == 3 else None)
    return (p.stdout or p.stderr or "")[-1500:]


def main():
    a = sys.argv[1:]
    opt = {"--level": None, "--cwd": os.getcwd(), "--budget": "2"}
    while a and a[0] in opt:
        opt[a[0]] = a[1]; a = a[2:]
    if not a:
        sys.exit(__doc__)
    text = " ".join(a)
    conf = 1.0
    level = int(opt["--level"]) if opt["--level"] is not None else None
    if level is None:
        level, conf = pick_level(text)
    out = run(level, text, opt["--cwd"], opt["--budget"])
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": int(time.time()), "level": level, "conf": conf,
                            "task": text[:200]}, ensure_ascii=False) + "\n")
    print(f"[dispatch] poziom {level} (pewnosc JEV {conf:.2f})\n{out}")


if __name__ == "__main__":
    main()
