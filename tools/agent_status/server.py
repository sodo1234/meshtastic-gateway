"""Agent status panel: Codex / Gemini CLI on local + G2 + SUP (+G1). Stdlib only, read-only.

Run: python server.py  ->  http://<host>:8977/   (JSON: /api/status)
Never prints secret values: env vars are reported as yes/no only.
"""
import json, os, shutil, subprocess, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT = 8977
INTERVAL = 20
DISPATCH_LOG = Path(__file__).resolve().parents[1] / "dispatch.log"
HOSTS = {"G2": "td@100.98.155.78", "SUP": "td@100.79.111.24", "G1": "td@100.68.79.100"}
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6"]
REPO, ISSUE = "sodo1234/meshtastic-gateway", "3"
NOWIN = getattr(subprocess, "CREATE_NO_WINDOW", 0)

# Remote probe: prints KEY=VALUE lines. Patterns use [c]odex so pgrep doesn't match this script.
REMOTE = r'''
[ -s ~/.nvm/nvm.sh ] && . ~/.nvm/nvm.sh >/dev/null 2>&1
export PATH="$HOME/.local/bin:$HOME/.npm-global/bin:$HOME/bin:$PATH"
v(){ command -v "$1" >/dev/null 2>&1 && timeout 10 "$1" --version 2>/dev/null | head -1 | tr -d '\r'; }
echo "codex_ver=$(v codex)"
echo "gemini_ver=$(v gemini)"
echo "codex_proc=$(pgrep -af '[c]odex' | grep -vc 'codex-companion')"
echo "companion_proc=$(pgrep -fc '[c]odex-companion')"
echo "gemini_proc=$(pgrep -fc '[g]emini')"
m(){ [ -d "$1" ] && find "$1" -type f -printf '%T@\n' 2>/dev/null | sort -n | tail -1; }
echo "codex_last=$(m ~/.codex/sessions)"
echo "gemini_last=$(m ~/.gemini/tmp)"
[ -n "$TYPESAFE_API_KEY" ] && echo typesafe=1 || echo typesafe=0
[ "$GOOGLE_GENAI_USE_VERTEXAI" = "true" ] && echo vertex=1 || echo vertex=0
'''

STATE = {"machines": {}, "channel": [], "updated": None}
LOCK = threading.Lock()


def run(cmd, timeout=25, input=None):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=input,
                           stdin=None if input is not None else subprocess.DEVNULL, creationflags=NOWIN,
                           encoding="utf-8", errors="replace")
        return r.returncode, r.stdout
    except Exception:
        return -1, ""


def iso(ts):
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).isoformat(timespec="seconds") if ts else None
    except ValueError:
        return None


def shape(d, online=True):
    n = lambda k: int(d.get(k) or 0) if str(d.get(k) or "0").isdigit() else 0
    procs = {"codex": n("codex_proc"), "codex-companion": n("companion_proc"), "gemini": n("gemini_proc")}
    return {
        "online": online,
        "codex": {"version": d.get("codex_ver") or None, "last_activity": iso(d.get("codex_last"))},
        "gemini": {"version": d.get("gemini_ver") or None, "last_activity": iso(d.get("gemini_last"))},
        "processes": procs,
        "running_total": sum(procs.values()),
        "TYPESAFE_API_KEY_set": d.get("typesafe") == "1",
        "vertexai": d.get("vertex") == "1",
    }


def newest_mtime(p):
    p = Path(p)
    if not p.is_dir():
        return ""
    # ponytail: full rglob every 20 s; cap/index if session dirs grow huge
    return str(max((f.stat().st_mtime for f in p.rglob("*") if f.is_file()), default="") or "")


def probe_local():
    d = {}
    for tool in ("codex", "gemini"):
        exe = shutil.which(tool)
        if exe:
            _, out = run([exe, "--version"], timeout=15)
            d[tool + "_ver"] = (out.strip().splitlines() or [""])[0]
    # codex = native codex.exe; companion/gemini = node.exe running those scripts (skips sh/bash wrappers)
    ps = ("Get-CimInstance Win32_Process | ForEach-Object { $c=[string]$_.CommandLine; "
          "if($_.Name -eq 'codex.exe'){'codex'} elseif($_.Name -eq 'node.exe'){ "
          "if($c -match 'codex-companion'){'companion'} elseif($c -match 'gemini'){'gemini'} } }")
    _, out = run(["powershell", "-NoProfile", "-Command", ps])
    lines = out.split()
    d["codex_proc"] = str(lines.count("codex"))
    d["companion_proc"] = str(lines.count("companion"))
    d["gemini_proc"] = str(lines.count("gemini"))
    home = Path.home()
    d["codex_last"] = newest_mtime(home / ".codex" / "sessions")
    d["gemini_last"] = newest_mtime(home / ".gemini" / "tmp")
    d["typesafe"] = "1" if os.environ.get("TYPESAFE_API_KEY") else "0"
    d["vertex"] = "1" if os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "").lower() == "true" else "0"
    return shape(d)


def probe_remote(target):
    # Windows text-mode pipe turns \n into \r\n, strip CR remotely
    rc, out = run(SSH + [target, r"tr -d '\r' | bash -l -s"], input=REMOTE)
    if rc == 255 or not out:
        return {"online": False, "target": target}
    d = dict(l.split("=", 1) for l in out.splitlines() if "=" in l)
    return dict(shape(d), target=target)


def probe_channel():
    rc, out = run(["gh", "issue", "view", ISSUE, "--repo", REPO, "--json", "comments"])
    if rc != 0:
        return [{"author": "-", "time": None, "text": "gh niedostepne / brak dostepu"}]
    comments = json.loads(out).get("comments", [])[-5:]
    return [{"author": (c.get("author") or {}).get("login"), "time": c.get("createdAt"),
             "text": " ".join((c.get("body") or "").split())[:120]} for c in reversed(comments)]


def dispatch_entries(limit=50):
    """Read a bounded tail and tolerate old/malformed JSONL records."""
    try:
        lines = DISPATCH_LOG.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
    except OSError:
        return []
    entries = []
    for line in reversed(lines):
        try:
            item = json.loads(line)
        except (TypeError, ValueError):
            continue
        entries.append({k: item.get(k) for k in
                        ("ts", "level", "model", "elapsed_ms", "status", "task", "result")})
    return entries


def collect():
    while True:
        res = {}
        jobs = [threading.Thread(target=lambda: res.__setitem__("local", probe_local()))]
        jobs += [threading.Thread(target=lambda k=k, t=t: res.__setitem__(k, probe_remote(t))) for k, t in HOSTS.items()]
        jobs.append(threading.Thread(target=lambda: res.__setitem__("_ch", probe_channel())))
        for j in jobs: j.start()
        for j in jobs: j.join(60)
        ch = res.pop("_ch", [])
        with LOCK:
            STATE["machines"] = {k: res.get(k, {"online": False}) for k in ["local", *HOSTS]}
            STATE["channel"] = ch
            STATE["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        time.sleep(INTERVAL)


PAGE = r"""<!doctype html><html lang="pl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Agent Status</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1b1f24;--mut:#6a737d;--ok:#1a7f37;--bad:#cf222e;--bd:#d8dee4}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--card:#161b22;--fg:#e6edf3;--mut:#8b949e;--ok:#3fb950;--bad:#f85149;--bd:#30363d}}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,sans-serif;padding:16px}
h1{font-size:20px;margin:0 0 4px}h2{font-size:16px;margin:20px 0 8px}.mut{color:var(--mut);font-size:13px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(260px,1fr))}
.c{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:12px}
.c h3{margin:0 0 6px;font-size:16px;display:flex;justify-content:space-between}
.ok{color:var(--ok)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse}td{padding:2px 0;vertical-align:top}
td:last-child{text-align:right}.cm{border-bottom:1px solid var(--bd);padding:8px 0;word-break:break-word}
.log{overflow-x:auto}.log table{min-width:760px}.log th,.log td{padding:7px 8px;border-bottom:1px solid var(--bd);text-align:left}
.log th{color:var(--mut);font-size:12px}.log td:last-child{text-align:left}.result{max-width:430px;white-space:pre-wrap;word-break:break-word}
</style></head><body>
<h1>Agent Status</h1><div class="mut" id="u">ladowanie...</div>
<div class="grid" id="m"></div><h2>Dispatch</h2><div class="c log" id="dispatch"></div>
<h2>Kanal (issue #3)</h2><div class="c" id="ch"></div>
<script>
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const ago=t=>{if(!t)return'-';const s=(Date.now()-new Date(t))/1e3;return s<90?Math.round(s)+' s temu':s<5400?Math.round(s/60)+' min temu':s<172800?Math.round(s/3600)+' h temu':Math.round(s/86400)+' d temu'};
const yn=b=>b?'<span class="ok">tak</span>':'<span class="mut">nie</span>';
const duration=ms=>ms==null?'-':ms<1000?ms+' ms':(ms/1000).toFixed(1)+' s';
async function load(){try{const [d,logs]=await Promise.all([fetch('/api/status').then(r=>r.json()),fetch('/api/dispatch').then(r=>r.json())]);
document.getElementById('u').textContent='aktualizacja: '+(d.updated?new Date(d.updated).toLocaleTimeString():'-');
document.getElementById('m').innerHTML=Object.entries(d.machines).map(([n,m])=>!m.online?
`<div class="c"><h3>${esc(n)}<span class="bad">offline</span></h3><div class="mut">${esc(m.target||'')}</div></div>`:
`<div class="c"><h3>${esc(n)}<span class="${m.running_total?'ok':'mut'}">${m.running_total} uruch.</span></h3><table>
<tr><td>codex</td><td>${esc(m.codex.version||'brak')}</td></tr><tr><td class="mut">ost. aktywnosc</td><td>${ago(m.codex.last_activity)}</td></tr>
<tr><td>gemini</td><td>${esc(m.gemini.version||'brak')}</td></tr><tr><td class="mut">ost. aktywnosc</td><td>${ago(m.gemini.last_activity)}</td></tr>
<tr><td>procesy codex / companion / gemini</td><td>${m.processes.codex} / ${m.processes['codex-companion']} / ${m.processes.gemini}</td></tr>
<tr><td>TYPESAFE_API_KEY</td><td>${yn(m.TYPESAFE_API_KEY_set)}</td></tr><tr><td>Vertex AI</td><td>${yn(m.vertexai)}</td></tr></table></div>`).join('');
document.getElementById('dispatch').innerHTML=logs.length?`<table><thead><tr><th>czas</th><th>poziom</th><th>model</th><th>trwanie</th><th>wynik</th></tr></thead><tbody>${logs.map(x=>`<tr><td>${x.ts?new Date(x.ts*1000).toLocaleString():'-'}</td><td>${esc(x.level)}</td><td>${esc(x.model||'-')}</td><td>${duration(x.elapsed_ms)}</td><td class="result"><b class="${x.status==='ok'||x.status==='local'?'ok':x.status?'bad':'mut'}">${esc(x.status||'-')}</b>${x.result?' — '+esc(x.result):x.task?' — '+esc(x.task):''}</td></tr>`).join('')}</tbody></table>`:'<span class="mut">brak wpisow</span>';
document.getElementById('ch').innerHTML=d.channel.map(c=>`<div class="cm"><b>${esc(c.author)}</b> <span class="mut">${ago(c.time)}</span><br>${esc(c.text)}</div>`).join('')||'<span class="mut">brak</span>';
}catch(e){document.getElementById('u').textContent='blad: '+e}}
load();setInterval(load,5000);
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/status"):
            with LOCK:
                body, ctype = json.dumps(STATE, ensure_ascii=False).encode(), "application/json"
        elif self.path.startswith("/api/dispatch"):
            body, ctype = json.dumps(dispatch_entries(), ensure_ascii=False).encode(), "application/json"
        elif self.path in ("/", "/index.html"):
            body, ctype = PAGE.encode(), "text/html; charset=utf-8"
        else:
            self.send_error(404); return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):  # no request logging
        pass


if __name__ == "__main__":
    threading.Thread(target=collect, daemon=True).start()
    print(f"agent_status on http://0.0.0.0:{PORT}/")
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
