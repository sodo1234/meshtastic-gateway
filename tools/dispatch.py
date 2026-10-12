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
SECRET = re.compile(
    r"(?i)(?:\bBearer\s+\S+|\bsk-[A-Za-z0-9_-]+|\bAIza[A-Za-z0-9_-]+|\b[A-Za-z_][A-Za-z0-9_]*KEY\s*=\s*\S+)"
)


def safe_summary(value, limit=200):
    return SECRET.sub("[REDACTED]", str(value or ""))[:limit]


def pick_level(text):
    try:
        res = ask({"zadanie": text}, {"level": {
            "type": "choice", "criteria": LEVELS,
            "instructions": "Ktory poziom delegacji najlepiej pasuje do zadania?"}})
    except Exception:
        res = None
    a = (res or {}).get("answers", {}).get("level")
    lvl, conf = (int(a["choice"]), a["confidence"]) if a else (None, 0.0)
    if lvl is None or conf < 0.85:
        lvl = 2                      # niepewny JEV -> sredni poziom, nie 0/1
    if LIVE.search(text):
        lvl = max(lvl, 2)            # zywe systemy: nigdy ponizej Codexa
    return lvl, conf


def run(level, text, cwd, budget):
    if level == 0:
        return "local", "POZIOM 0: zadanie dla sesji lokalnej (nic nie uruchomiono).", "local"
    if level == 1:
        model = "gemini-3-pro-preview"
        cmd = ["gemini", "--skip-trust", "-m", "gemini-3-pro-preview", "-p", text]
    elif level == 2:
        model = "codex-companion/high"
        cmd = ["node", CODEX, "task", "--write", "--fresh", "--effort", "high", text]
    else:
        model = "claude-sonnet"
        key = os.environ.get("DISPATCH_ANTHROPIC_KEY")
        if not key:
            return model, "ODMOWA: brak DISPATCH_ANTHROPIC_KEY (bez klucza poszloby z subskrypcji).", "refused"
        env = dict(os.environ, ANTHROPIC_API_KEY=key)   # klucz tylko dla tego procesu
        cmd = ["claude", "-p", text, "--bare", "--model", "sonnet",
               "--max-budget-usd", str(budget), "--permission-mode", "acceptEdits"]
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=1800,
                           encoding="utf-8", errors="replace",
                           env=env if level == 3 else None)
        return model, (p.stdout or p.stderr or "")[-1500:], "ok" if p.returncode == 0 else f"exit {p.returncode}"
    except subprocess.TimeoutExpired as e:
        out = e.stdout or e.stderr or "przekroczono limit 1800 s"
        return model, str(out)[-1500:], "timeout"
    except Exception as e:
        return model, f"{type(e).__name__}: {e}"[-1500:], "error"


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
    started = time.time()
    model, out, status = run(level, text, opt["--cwd"], opt["--budget"])
    elapsed_ms = int((time.time() - started) * 1000)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": int(time.time()), "level": level, "conf": conf,
                            "model": model, "elapsed_ms": elapsed_ms, "status": status,
                            "task": safe_summary(text), "result": safe_summary(out)},
                           ensure_ascii=False) + "\n")
    print(f"[dispatch] poziom {level} (pewnosc JEV {conf:.2f})\n{out}")


if __name__ == "__main__":
    main()
