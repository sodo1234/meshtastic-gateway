"""Podglad na zywo pracy workerow: ogon wyjscia kazdego *.out z katalogu zlecen + dispatch.log.
Uruchom: python live.py [katalog]  ->  http://localhost:8977/   (stdlib, tylko odczyt)
Katalog domyslny: <TEMP>/dz  (tam kazdy worker pisze: codex.out, gemini.out, ...)
"""
import json, sys, tempfile, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.gettempdir()) / "dz"
DISPATCH = Path(__file__).resolve().parents[1] / "dispatch.log"
LINKS = {"Sesja chmurowa (nadzorca)": "https://claude.ai/code/session_01Nm6Ej54jYfitBroqNjAiPS",
         "Issue #3 (kanal)": "https://github.com/sodo1234/meshtastic-gateway/issues/3"}
ACTIVE_S = 25  # ponytail: "pracuje" = plik zmieniony w ostatnich 25 s; gemini -p pisze dopiero na koncu


def tail(p, n):
    try:
        return p.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]
    except OSError:
        return []


def snapshot():
    now = time.time()
    workers = []
    for p in sorted(DIR.glob("*.out"), key=lambda x: -x.stat().st_mtime):
        age = now - p.stat().st_mtime
        lines = [l for l in tail(p, 60) if "DEP0190" not in l and "trace-deprecation" not in l]
        workers.append({"name": p.stem, "age": int(age), "active": age < ACTIVE_S, "lines": lines[-25:]})
    return {"workers": workers, "dispatch": tail(DISPATCH, 15), "links": LINKS, "t": time.strftime("%H:%M:%S")}


PAGE = """<!doctype html><meta charset=utf-8><title>Workerzy na zywo</title>
<style>body{background:#0a0a0a;color:#ddd;font:13px/1.4 Consolas,monospace;margin:16px}
.w{border:1px solid #333;border-radius:6px;margin:0 0 12px;padding:8px}h3{margin:0 0 6px;font-size:14px}
.on{color:#4ade80}.off{color:#888}pre{margin:0;white-space:pre-wrap;word-break:break-word;max-height:300px;overflow:auto}
a{color:#7cb7ff;margin-right:14px}</style>
<div id=l></div><div id=o></div><h3>dispatch.log</h3><pre id=d></pre>
<script>
const e=s=>s.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
async function r(){try{const j=await (await fetch('/api/live')).json();
l.innerHTML=Object.entries(j.links).map(([k,v])=>`<a href="${v}" target=_blank>${e(k)}</a>`).join('')+' &nbsp;'+j.t;
o.innerHTML=j.workers.map(w=>`<div class=w><h3>${e(w.name)} <span class=${w.active?'on':'off'}>${w.active?'● pracuje':'○ '+(w.age<3600?w.age+' s temu':'skonczony')}</span></h3><pre>${e(w.lines.join('\\n'))||'(brak wyjscia)'}</pre></div>`).join('')||'<p>brak zlecen w '+'${DIR}'+'</p>';
d.textContent=j.dispatch.join('\\n')||'(pusty)'}catch(x){}}
r();setInterval(r,3000)</script>""".replace("${DIR}", str(DIR).replace("\\", "/"))


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/live"):
            body, ct = json.dumps(snapshot()).encode(), "application/json"
        else:
            body, ct = PAGE.encode(), "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    DIR.mkdir(exist_ok=True)
    ThreadingHTTPServer(("0.0.0.0", 8977), H).serve_forever()
