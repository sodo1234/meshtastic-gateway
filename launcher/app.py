#!/usr/bin/env python3
"""Meshtastic Launcher - Flask web UI for remote start/stop/tail of gateway+supervisor."""
import json, os, re, subprocess, threading, time, queue
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, render_template, request, Response, stream_with_context

# Windows: suppress the console window that ssh.exe/scp.exe/wt.exe would flash
# on every subprocess call when the launcher has no visible console of its own.
CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

SSH_USER = "td"
REMOTE_DIR = "~/meshtastic"
REMOTE_VENV = "~/meshtastic/venv/bin/python"
LAN_HOSTS = [
    ("100.93.121.31", "carport-g1"),   # PRODUKCJA fizyczna carport = G1 (Tailscale)
    ("100.98.155.78", "carport-g2"),   # stara bramka = G2
    ("100.79.111.24", "supervisor"),   # supervisor G0
]

# role -> tmux session name + remote log paths (logger + stderr) + script-name hint for pgrep
ROLE_META = {
    "gw":  {"tmux": "gw",  "remote_log": "/tmp/gateway.log",
            "stderr_log": "/tmp/gateway.stderr.log", "pgrep": "gateway.*\\.py"},
    "sup": {"tmux": "sup", "remote_log": "/tmp/supervisor.log",
            "stderr_log": "/tmp/supervisor.stderr.log", "pgrep": "supervisor.*\\.py"},
}


def resolve_meta(role, script=""):
    """Return meta for role, overriding log paths when script is a test_* file."""
    base = ROLE_META[role]
    if script and script.startswith("test_"):
        return {**base,
                "remote_log": "/tmp/test_transport.log",
                "stderr_log": "/tmp/test_transport.stderr.log",
                "pgrep": "test_step[0-9].*\\.py"}
    return base

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True
_tail_threads = {}  # key=(host,role) -> {"thread":..., "queue":..., "stop":Event}


SSH_BASE_OPTS = [
    "-o", "BatchMode=yes",
    # ConnectTimeout 5→20: relay Tailscale ("fra") bywa laggy — 5s ucinało połączenie przed
    # handshakiem → status fałszywie "stopped". 20s daje relay czas na ustanowienie TCP/banner.
    "-o", "ConnectTimeout=20",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=2",
]

# PIDs of long-lived ssh processes spawned by Launcher (SSE streams).
# Anything NOT in this set is fair game for the janitor.
_protected_ssh_pids = set()
_protected_lock = threading.Lock()


_host_ip_cache = {"map": {}, "ts": 0.0}
_host_ip_lock = threading.Lock()


def _host_ip_map():
    """Cache nazwa→IP (Tailscale wszystkie peery, też offline + LAN), TTL 60s.
    SSH po IP omija MagicDNS, które przez laggy relay zawodzi (nazwa 'gateway' nie resolwuje)."""
    with _host_ip_lock:
        if time.time() - _host_ip_cache["ts"] < 60 and _host_ip_cache["map"]:
            return _host_ip_cache["map"]
        m = {}
        try:
            p = subprocess.run(["tailscale", "status"], capture_output=True, text=True,
                               timeout=5, creationflags=CREATE_NO_WINDOW)
            for line in p.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[0] and parts[0][0].isdigit():
                    m[parts[1]] = parts[0]            # name → ip (niezależnie od online/offline)
        except Exception:
            pass
        for ip, name in LAN_HOSTS:
            m.setdefault(name, ip)
        if m:
            _host_ip_cache["map"] = m
            _host_ip_cache["ts"] = time.time()
        return m or _host_ip_cache["map"]


def _resolve_host(host):
    """Nazwa Tailscale/LAN → IP (pewniejsze niż MagicDNS). IP/nieznane → bez zmian."""
    if not host or host[0].isdigit():
        return host
    return _host_ip_map().get(host, host)


def ssh(host, cmd, timeout=35, check=False):
    """Run remote cmd over ssh. Returns (rc, stdout, stderr). SINGLE-SHOT (bez retry!).

    Retry tu był BŁĘDEM: 3 próby × timeout trzymały slot połączenia przeglądarki ~100s, a że
    przeglądarka ma ~6 slotów/host, WOLNA maszyna (relay w dołku) głodziła SZYBKĄ → blokada
    krzyżowa, opóźnienia, „czasem nie zaczyta". Single-shot = szybki sukces/szybki fail → slot
    wolny od razu → obie maszyny ładują się RÓWNOLEGLE i niezależnie. Odporność dają: cache
    last-good (zwraca natychmiast przy padzie) + frontend auto-retry (rozłożony, nie blokuje)."""
    full = ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{_resolve_host(host)}", cmd]
    try:
        # stdin=DEVNULL: Windows ssh.exe hangs when inherited stdin is a socket
        # (Flask request threads) — explicit DEVNULL prevents that
        p = subprocess.run(full, stdin=subprocess.DEVNULL,
                           capture_output=True, text=True, timeout=timeout,
                           encoding='utf-8', errors='replace',
                           creationflags=CREATE_NO_WINDOW)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def _ssh_janitor():
    """Kill orphan ssh.exe processes not in _protected_ssh_pids.
    Prevents accumulation from terminated SSE streams where Windows
    TerminateProcess doesn't propagate cleanly to remote sshd."""
    import time as _t
    try:
        import psutil
    except ImportError:
        return  # psutil not installed → janitor disabled
    while True:
        _t.sleep(60)
        try:
            now = _t.time()
            with _protected_lock:
                protected = set(_protected_ssh_pids)
            for proc in psutil.process_iter(['pid', 'name', 'create_time', 'ppid']):
                try:
                    if proc.info['name'] and proc.info['name'].lower() == 'ssh.exe':
                        if proc.info['pid'] in protected: continue
                        age = now - proc.info['create_time']
                        if age > 90 and proc.info['ppid'] == os.getpid():
                            proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except Exception:
            pass


def discover_tailscale():
    """Return list of (ip, name) for online tailnet Linux peers."""
    try:
        p = subprocess.run(["tailscale", "status"], capture_output=True, text=True, timeout=5,
                           creationflags=CREATE_NO_WINDOW)
        out = []
        for line in p.stdout.splitlines():
            parts = line.split()
            if len(parts) < 5: continue
            ip, name, _user, osys = parts[0], parts[1], parts[2], parts[3]
            rest = " ".join(parts[4:])
            if osys != "linux": continue
            if "offline" in rest.lower(): continue
            out.append({"ip": ip, "name": name})
        return out
    except Exception:
        return []


# Cache last-good (USB + skrypty) — TYLKO in-memory, BEZ background-warmera (to on zapychał relay).
# Raz wykryte przeżywa dołek relaya: gdy on-demand SSH padnie, oddajemy ostatni dobry wynik
# zamiast pustki. Wypełniany wyłącznie udanymi zapytaniami z UI (zero ruchu w tle).
_usb_cache = {}
_scripts_cache = {}


def list_scripts(host):
    """List *.py scripts in REMOTE_DIR on host (cache last-good przy padzie SSH)."""
    rc, out, _ = ssh(host, f"ls {REMOTE_DIR}/*.py 2>/dev/null | xargs -n1 basename")
    scripts = [s.strip() for s in out.splitlines() if s.strip()] if rc == 0 else []
    if scripts:
        _scripts_cache[host] = scripts
    elif host in _scripts_cache:
        return list(_scripts_cache[host])
    return scripts


def list_usb_devices(host):
    """Return list of detected serial USB devices on host.
    Combines /dev/serial/by-id/ (stable, preferred) with ttyACM*/ttyUSB* (raw)."""
    cmd = (
        "echo '--BYID--'; find /dev/serial/by-id -maxdepth 1 -type l -printf '%f\\t%l\\n' 2>/dev/null; "
        "echo '--RAW--'; ls /dev/ttyACM* /dev/ttyUSB* 2>/dev/null"
    )
    rc, out, _ = ssh(host, cmd)
    devs = []
    section = None
    for line in out.splitlines():
        line = line.rstrip()
        if line == '--BYID--': section='byid'; continue
        if line == '--RAW--': section='raw'; continue
        if not line: continue
        if section == 'byid':
            if '\t' not in line: continue
            name, target = line.split('\t', 1)
            tty = '/dev/' + target.split('/')[-1]
            label = name.replace('usb-', '').replace('-if00', '')
            devs.append({'tty': tty, 'byid': f'/dev/serial/by-id/{name}', 'label': label})
        elif section == 'raw':
            if not any(d['tty'] == line for d in devs):
                devs.append({'tty': line, 'byid': line, 'label': line.split('/')[-1]})
    if devs:
        _usb_cache[host] = devs
    elif host in _usb_cache:
        return list(_usb_cache[host])           # relay dip → ostatni dobry wynik
    return devs


def ensure_tmux(host):
    """Install tmux on host if missing (idempotent)."""
    rc, out, _ = ssh(host, "command -v tmux >/dev/null && echo OK || echo MISSING")
    if "OK" in out: return True
    # Need sudo; password is in CONFIG
    rc, _, err = ssh(host, "echo 'REPLACE_ME' | sudo -S apt-get install -y tmux 2>&1 | tail -3", timeout=60)
    return rc == 0


PORT_PATCHER = r'''
import re, sys, os, ast
script, new_port = sys.argv[1], sys.argv[2]
src = open(script, encoding="utf-8").read()
orig = src

# Singular gateway: 'mesh_port' / 'meshtastic_port' : 'value'
src = re.sub(
    r"""(["'](?:mesh_port|meshtastic_port)["']\s*:\s*)["'][^"']*["']""",
    lambda m: m.group(1) + repr(new_port), src)

# Supervisor list: 'mesh_ports': [ {"port":"X", ..., "enabled":True, ...}, ... ]
# Strategy: find first dict literal containing "enabled": True, replace its "port" value.
def patch_first_enabled(s):
    list_m = re.search(r"""["']mesh_ports["']\s*:\s*\[""", s)
    if not list_m: return s
    # Match each dict literal {...} (no nested {}), check for enabled True, find first
    out, idx = s, list_m.end()
    for m in re.finditer(r"\{[^{}]*\}", s[idx:]):
        block = m.group(0)
        if re.search(r"""["']enabled["']\s*:\s*True""", block):
            new_block = re.sub(
                r"""(["']port["']\s*:\s*)["'][^"']*["']""",
                lambda mm: mm.group(1) + repr(new_port), block, count=1)
            return s[:idx+m.start()] + new_block + s[idx+m.end():]
    return s
src = patch_first_enabled(src)

if src == orig:
    print("NO_CHANGE"); sys.exit(0)

# Validate before write
try: ast.parse(src)
except SyntaxError as e: print("AST_FAIL:", e); sys.exit(2)

bak = script + ".bak"
if not os.path.exists(bak): open(bak, "w", encoding="utf-8").write(orig)
open(script, "w", encoding="utf-8").write(src)
print("PATCHED")
'''


def patch_script_port(host, script, port):
    """Patch CONFIG mesh_port / mesh_ports[*].port in target .py to `port`.

    Handles two CONFIG shapes:
      - Singular (gateway):  'mesh_port': 'path'
      - List (supervisor):   'mesh_ports': [ {"port": "X", "enabled": True, ...}, ... ]
                             → replaces port of FIRST entry with enabled=True
    Idempotent, AST-validates before writing, keeps .bak on first patch.
    """
    if not port:
        return True, "no port override"
    import shlex
    # ssh ... python3 - <<EOF — pass patcher via stdin to avoid quoting hell
    remote_cmd = f"cd {REMOTE_DIR} && python3 - {shlex.quote(script)} {shlex.quote(port)}"
    full = ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{_resolve_host(host)}", remote_cmd]
    try:
        p = subprocess.run(full, input=PORT_PATCHER, capture_output=True, text=True,
                           timeout=15, encoding='utf-8', errors='replace',
                           creationflags=CREATE_NO_WINDOW)
        out = (p.stdout or "").strip(); err = (p.stderr or "").strip()
        ok = p.returncode == 0 and ("PATCHED" in out or "NO_CHANGE" in out)
        return ok, out or err
    except subprocess.TimeoutExpired:
        return False, "timeout"


# ── Remote config.py edit (view + save) ──
# config.py trzyma CONFIG bramki/supervisora (porty, listy urządzeń, tokeny).
# Edycja przez UI zamiast ręcznego ssh — zapis waliduje AST i robi .bak (jak PORT_PATCHER).
CONFIG_REMOTE_PATH = f"{REMOTE_DIR}/config.py"

CONFIG_WRITER = r'''
import sys, os, ast, base64
path = os.path.expanduser(sys.argv[1])     # Python open() NIE rozwija ~ (tylko shell)
content = base64.b64decode(sys.argv[2].encode()).decode("utf-8")
try:
    ast.parse(content)
except SyntaxError as e:
    print("AST_FAIL: line %s: %s" % (e.lineno, e.msg)); sys.exit(2)
orig = open(path, encoding="utf-8").read() if os.path.exists(path) else ""
open(path + ".bak", "w", encoding="utf-8").write(orig)          # last-known-good przed zapisem
with open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)
print("SAVED %d" % len(content))
'''


def read_remote_config(host):
    """Return (content, err). Reads {REMOTE_DIR}/config.py over ssh."""
    rc, out, err = ssh(host, f"cat {CONFIG_REMOTE_PATH}", timeout=15)
    if rc != 0:
        return None, (err.strip() or "read failed")
    return out, None


def write_remote_config(host, content):
    """AST-validate + .bak + write config.py on host. Returns (ok, msg)."""
    import base64, shlex
    b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    remote_cmd = f"python3 - {shlex.quote(CONFIG_REMOTE_PATH)} {shlex.quote(b64)}"
    full = ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{_resolve_host(host)}", remote_cmd]
    try:
        p = subprocess.run(full, input=CONFIG_WRITER, capture_output=True, text=True,
                           timeout=15, encoding="utf-8", errors="replace",
                           creationflags=CREATE_NO_WINDOW)
        out = (p.stdout or "").strip(); err = (p.stderr or "").strip()
        ok = p.returncode == 0 and out.startswith("SAVED")
        return ok, (out or err or "unknown error")
    except subprocess.TimeoutExpired:
        return False, "timeout"


GUI_ENV = ("export XDG_RUNTIME_DIR=/run/user/1000; "
           "export WAYLAND_DISPLAY=wayland-0; "
           "export DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus; "
           "export DISPLAY=:0; ")


def launch_gnome_terminal(host, session, title):
    """Best-effort: open gnome-terminal on Debian's GUI session, attached to tmux.
    Silently no-op if no GUI session available."""
    cmd = (
        f"{GUI_ENV}"
        f"command -v gnome-terminal >/dev/null && "
        f"nohup gnome-terminal --title='{title}' "
        f"-- bash -c 'tmux attach -t {session}' >/dev/null 2>&1 & disown; "
        f"true"
    )
    ssh(host, cmd, timeout=5)


def close_gnome_terminals(host, session_title):
    """Best-effort: close gnome-terminal windows matching title (after tmux killed)."""
    # gnome-terminal-server holds windows; killing it closes ALL — too aggressive.
    # Instead: wmctrl on Wayland is limited; rely on tmux death cascading.
    # As fallback: find terminal processes whose tmux attach session matches.
    cmd = (
        f"{GUI_ENV}"
        f"pkill -f 'tmux attach -t {session_title}' 2>/dev/null; true"
    )
    ssh(host, cmd, timeout=5)


def robust_kill(host, role, session, tries=4):
    """Kill tmux session + ALL python (prod+test) that could hold the serial port,
    then VERIFY nothing survived; retry up to `tries`. Returns (ok, detail).

    Dlaczego pętla+weryfikacja: relay 'fra' bywa laggy — pojedynczy SSH potrafi nie
    dolecieć, a wtedy proces ŻYJE DALEJ i trzyma port CP2102 → kolejny Start dostaje
    '[Errno 5] multiple access on port'. Stary kod strzelał raz i zawsze zwracał True
    (UI mówił 'stopped', proces biegł). Tu: SIGTERM→(grace)→SIGKILL i sprawdzamy pgrep,
    aż realnie zniknie. SIGKILL gwarantuje zwolnienie fd portu szeregowego przez OS."""
    base = ROLE_META[role]
    # -f matchuje pełną linię poleceń (venv/bin/python -u <script>); zabijamy prod ORAZ test.
    # Trik nawiasowy [p]ython: regex łapie 'python' w realnych procesach, ale NIE łapie
    # literału '[p]ython' w cmdline powłoki sh -c która odpala ten sam pkill/pgrep — bez tego
    # pgrep/pkill matchował SAM SIEBIE → wieczne 'ALIVE' (false positive) i kill własnego shella.
    pats = [f"[p]ython.*{base['pgrep']}", "[p]ython.*test_step"]
    kill_cmd = ("tmux kill-session -t %s 2>/dev/null; " % session
                + "".join(f"pkill -f '{p}' 2>/dev/null; " for p in pats)
                + "sleep 0.5; "
                + "".join(f"pkill -9 -f '{p}' 2>/dev/null; " for p in pats)
                + "true")
    verify_cmd = (f"tmux has-session -t {session} 2>/dev/null && echo ALIVE; "
                  + "".join(f"pgrep -f '{p}' >/dev/null 2>&1 && echo ALIVE; " for p in pats)
                  + "echo DONE")
    last = ""
    for _ in range(tries):
        ssh(host, kill_cmd, timeout=12)
        rc, out, _ = ssh(host, verify_cmd, timeout=12)
        last = out
        if rc == 0 and "DONE" in out and "ALIVE" not in out:
            return True, "killed+verified"
        time.sleep(1)
    return False, f"still alive after {tries} tries: {last.strip()!r}"


def start_remote(host, role, script, port=None, extra_args=""):
    meta = resolve_meta(role, script)
    is_test = script.startswith("test_")
    session = meta["tmux"]
    if port and not is_test:
        ok, msg = patch_script_port(host, script, port)
        if not ok:
            return False, f"port patch failed: {msg}"
    stderr_log = meta["stderr_log"]
    # Pre-kill ROBUSTNY: zwolnij port zanim wystartujemy (inaczej 2 procesy = multiple access).
    killed, kdetail = robust_kill(host, role, session)
    if not killed:
        return False, f"nie zwolniono portu (stary proces żyje): {kdetail}"
    ssh(host, f": > {stderr_log}; true")            # truncate stale stderr
    args_str = f" {extra_args}" if extra_args else ""
    cmd = (f"cd {REMOTE_DIR} && "
           f"tmux new -d -s {session} 'venv/bin/python -u {script}{args_str} 2>>{stderr_log}'")
    ssh(host, cmd, timeout=10)
    # Weryfikuj że sesja realnie wstała (start SSH też bywa zjadany przez dołek relaya) — retry x3.
    up = False
    for _ in range(3):
        rc, out, _ = ssh(host, f"tmux has-session -t {session} 2>/dev/null && echo UP", timeout=10)
        if "UP" in out:
            up = True
            break
        ssh(host, cmd, timeout=10)                  # ponów start jeśli pierwszy nie dolaciał
        time.sleep(1)
    if up:
        launch_gnome_terminal(host, session, f"{role.upper()} • {script}")
    return up, ("started on %s" % (port or "default port") if up else "tmux session nie wstała (relay?)")


def stop_remote_full(host, role, script=""):
    """Stop tmux + kill script (verified) + close attached GUI terminals.
    Zwraca False jeśli proces NIE zginął — UI wtedy nie kłamie że 'stopped'."""
    meta = resolve_meta(role, script)
    session = meta["tmux"]
    close_gnome_terminals(host, session)
    ok, _ = robust_kill(host, role, session)
    return ok


def stop_remote(host, role, script=""):
    return stop_remote_full(host, role, script)


def status_remote(host, role, script=""):
    meta = resolve_meta(role, script)
    cmd = (
        f"tmux has-session -t {meta['tmux']} 2>/dev/null && echo TMUX_OK; "
        f"pgrep -af 'python.*{meta['pgrep']}' | grep -v pgrep | head -1"
    )
    # Retry x2: laggy relay potrafi zerwać/timeoutować pojedynczy SSH → fałszywe 'stopped'.
    # Pusty wynik (brak TMUX_OK i pid) = prawdopodobnie błąd łącza, nie martwy proces → spróbuj jeszcze raz.
    out = ""
    for _ in range(2):
        rc, out, _ = ssh(host, cmd)
        if "TMUX_OK" in out or any(l.strip()[:1].isdigit() for l in out.splitlines() if l.strip()):
            break
    tmux_ok = "TMUX_OK" in out
    pid = None
    mode = None
    for line in out.splitlines():
        line = line.strip()
        if line and not line.startswith("TMUX_OK") and line.split(" ")[0].isdigit():
            pid = line.split(" ")[0]
            break
    running = tmux_ok or bool(pid)
    if running:
        mode = "tmux" if tmux_ok else "manual"
    return {"running": running, "pid": pid, "mode": mode, "log": meta["remote_log"]}


def pull_log(host, role, script=""):
    """scp remote log → local logs/ with date stamp."""
    meta = resolve_meta(role, script)
    today = datetime.now().strftime("%Y-%m-%d")
    local = LOG_DIR / f"{role}_{host.replace('.', '_')}_{today}.log"
    cmd = ["scp", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
           f"{SSH_USER}@{_resolve_host(host)}:{meta['remote_log']}", str(local)]
    p = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                       capture_output=True, text=True, timeout=30,
                       creationflags=CREATE_NO_WINDOW)
    if p.returncode == 0:
        return {"ok": True, "path": str(local), "size": local.stat().st_size}
    return {"ok": False, "err": p.stderr or p.stdout}


def open_tail_wt(host, role):
    """Open new wt.exe tab with live tail."""
    log = ROLE_META[role]["remote_log"]
    title = f"{role.upper()}-{host}"
    # Launch wt.exe directly (no intermediate `cmd /c start`, which flashed a
    # console window). wt opens its own GUI tab; CREATE_NO_WINDOW keeps the
    # launching side windowless.
    subprocess.Popen(
        ["wt.exe", "new-tab", "--title", title,
         "ssh", f"{SSH_USER}@{_resolve_host(host)}", f"tail -F {log}"],
        creationflags=CREATE_NO_WINDOW)
    return True


# ── Background log sync (every 30s, pulls running role logs into project) ──
def log_sync_loop():
    while True:
        time.sleep(30)
        try:
            # naive: try pulling whatever is registered as "running" via /status calls in browser
            # for MVP just scan known active sessions across discovered hosts
            for h in [hp["ip"] for hp in discover_tailscale()] + [ip for ip, _ in LAN_HOSTS]:
                for role in ROLE_META:
                    st = status_remote(h, role)
                    if st["running"]:
                        pull_log(h, role)
        except Exception:
            pass


# ── SSE live tail per (host, meta) ──
def tail_generator(host, role):
    return tail_generator_meta(host, ROLE_META[role])


def tail_generator_meta(host, meta):
    files = f"{meta['remote_log']} {meta['stderr_log']}"
    # exec → ssh channel close propagates SIGHUP to tail; --pid=1 dies if shell vanishes
    cmd = (f"touch {meta['stderr_log']} 2>/dev/null; "
           f"exec tail -n 80 -F {files} 2>/dev/null")
    proc = subprocess.Popen(
        ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{_resolve_host(host)}", cmd],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        encoding='utf-8', errors='replace',
        creationflags=CREATE_NO_WINDOW)
    with _protected_lock:
        _protected_ssh_pids.add(proc.pid)
    try:
        for line in iter(proc.stdout.readline, ""):
            if not line: break
            yield f"data: {json.dumps({'line': line.rstrip()})}\n\n"
    finally:
        with _protected_lock:
            _protected_ssh_pids.discard(proc.pid)
        try: proc.terminate()
        except Exception: pass
        try: proc.wait(timeout=2)
        except Exception:
            try: proc.kill()
            except Exception: pass


# ── Routes ──
@app.route("/")
def index():
    resp = app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/hosts")
def api_hosts():
    return jsonify({
        "tailscale": discover_tailscale(),
        "lan": [{"ip": ip, "name": name} for ip, name in LAN_HOSTS],
    })


@app.route("/api/scripts")
def api_scripts():
    host = request.args.get("host")
    return jsonify({"scripts": list_scripts(host) if host else []})


@app.route("/api/usb-devices")
def api_usb():
    host = request.args.get("host")
    return jsonify({"devices": list_usb_devices(host) if host else []})


@app.route("/api/status")
def api_status():
    host = request.args.get("host")
    role = request.args.get("role")
    script = request.args.get("script", "")
    if not host or role not in ROLE_META:
        return jsonify({"error": "host & role required"}), 400
    return jsonify(status_remote(host, role, script))


# ── Monitoring logu: gubienie pakietów + nieprawidłowości (item #3) ──
# Skanuje okno logu z tmux pane obu harnessów pod kątem sygnałów utraty pakietów
# (lost_pong, NACK/retransmisja, rx_timeout/watchdog/antena, offline) oraz
# nieprawidłowości (błędy/wyjątki, ostrzeżenia, stagnacja). Zwraca zdrowie + liczniki.
import re as _re

_MONITOR_RULES = [
    ("lost_pong", "loss",  r"lost_pong|sup_link OFFLINE|💀 sup_link",           "utrata pong / link supervisora"),
    ("nack",      "loss",  r"NACK|retransmi|rt_.*(?:timeout|retry)|CRC.*(?:zły|mismatch|fail)", "NACK / retransmisja pliku"),
    ("rx_to",     "loss",  r"rx[_ ]?timeout|watchdog|urb .*stopped|-32|Timed out|antena.*(?:zwis|reset)", "rx timeout / watchdog / antena"),
    ("dev_off",   "loss",  r"💀.*OFFLINE|OFFLINE \(brak|cisza z2m",             "urządzenie OFFLINE"),
    ("err",       "irreg", r"Traceback|Exception|\[ERROR\]|\bERROR\b|❌",        "błąd / wyjątek"),
    ("warn",      "irreg", r"⚠️|\[WARN\]|\bWARN\b",                              "ostrzeżenie"),
    ("stag",      "irreg", r"STAGNACJA|stagnat",                                "stagnacja"),
]


@app.route("/api/monitor")
def api_monitor():
    host = request.args.get("host")
    role = request.args.get("role")
    script = request.args.get("script", "")
    try:
        lines = max(200, min(6000, int(request.args.get("lines", 3000))))
    except (TypeError, ValueError):
        lines = 3000
    if not host or role not in ROLE_META:
        return jsonify({"error": "host & role required"}), 400
    meta = resolve_meta(role, script)
    rc, out, _ = ssh(host, f"tmux capture-pane -pt {meta['tmux']} -S -{lines} -p 2>/dev/null")
    text = out or ""
    rows = text.splitlines()
    if not rows:
        return jsonify({"host": host, "role": role, "lines": 0, "health": "—",
                        "loss": 0, "irreg": 0, "tx": 0, "rx": 0, "rules": {},
                        "note": "brak logu (harness stopped?)"})
    rules = {}
    for key, cat, rx, human in _MONITOR_RULES:
        m = [l for l in rows if _re.search(rx, l)]
        rules[key] = {"n": len(m), "cat": cat, "label": human,
                      "last": (m[-1].strip()[-140:] if m else "")}
    loss = sum(v["n"] for v in rules.values() if v["cat"] == "loss")
    irreg = sum(v["n"] for v in rules.values() if v["cat"] == "irreg")
    tx = len(_re.findall(r"\bTX:", text))
    rx_n = len(_re.findall(r"\bRX:", text))
    # gubienie pakietów = priorytet item #3 → każda strata podnosi status; kilka ostrzeżeń tolerowane
    if loss >= 3 or irreg >= 8:
        health = "PROBLEM"
    elif loss >= 1 or irreg >= 3:
        health = "UWAGA"
    else:
        health = "OK"
    return jsonify({"host": host, "role": role, "lines": len(rows), "tx": tx, "rx": rx_n,
                    "loss": loss, "irreg": irreg, "health": health, "rules": rules})


# ── Async operacje (start/stop) — przycisk wraca NATYCHMIAST, robota leci w tle ──
# Stary kod robił robust_kill+verify (do ~50s) SYNCHRONICZNIE → przeglądarka wisiała na
# fetchu = „nieresponsywne". Teraz: POST kolejkuje robotę w wątku i wraca od razu; frontend
# pollinguje /api/op (szybkie, bez SSH) i odblokowuje przycisk gdy operacja się kończy.
_ops = {}                       # "host|role" -> {action, busy, ok, result, ts}
_ops_lock = threading.Lock()


def _op_key(host, role):
    return f"{host}|{role}"


def _run_op(host, role, action, fn):
    key = _op_key(host, role)
    with _ops_lock:
        _ops[key] = {"action": action, "busy": True, "ok": None, "result": "", "ts": time.time()}

    def _worker():
        try:
            ok, msg = fn()
        except Exception as e:
            ok, msg = False, f"wyjątek: {e}"
        with _ops_lock:
            _ops[key] = {"action": action, "busy": False, "ok": bool(ok),
                         "result": str(msg), "ts": time.time()}
    threading.Thread(target=_worker, daemon=True, name=f"op-{action}-{role}").start()


@app.route("/api/start", methods=["POST"])
def api_start():
    d = request.get_json()
    host, role, script = d.get("host"), d.get("role"), d.get("script")
    port, extra_args = d.get("port"), d.get("extra_args", "")
    if not all([host, role, script]) or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host, role, script required"}), 400
    with _ops_lock:
        if _ops.get(_op_key(host, role), {}).get("busy"):
            return jsonify({"ok": True, "busy": True, "msg": "operacja w toku"})

    def _do():
        ensure_tmux(host)
        return start_remote(host, role, script, port=port, extra_args=extra_args)
    _run_op(host, role, "start", _do)
    return jsonify({"ok": True, "busy": True, "queued": True})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    d = request.get_json()
    host, role, script = d.get("host"), d.get("role"), d.get("script", "")
    if not host or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host & role required"}), 400
    with _ops_lock:
        if _ops.get(_op_key(host, role), {}).get("busy"):
            return jsonify({"ok": True, "busy": True, "msg": "operacja w toku"})

    def _do():
        ok = stop_remote(host, role, script)
        return ok, ("zatrzymano (port wolny)" if ok else "proces NIE zginął (relay?)")
    _run_op(host, role, "stop", _do)
    return jsonify({"ok": True, "busy": True, "queued": True})


@app.route("/api/op")
def api_op():
    host = request.args.get("host"); role = request.args.get("role")
    with _ops_lock:
        st = _ops.get(_op_key(host, role))
    return jsonify(st or {"busy": False, "action": None, "ok": None, "result": ""})


@app.route("/api/pull-log", methods=["POST"])
def api_pull():
    d = request.get_json()
    return jsonify(pull_log(d.get("host"), d.get("role"), d.get("script", "")))


@app.route("/api/open-tail", methods=["POST"])
def api_open_tail():
    d = request.get_json()
    open_tail_wt(d.get("host"), d.get("role"))
    return jsonify({"ok": True})


@app.route("/api/config")
def api_config_get():
    host = request.args.get("host")
    if not host:
        return jsonify({"ok": False, "err": "host required"}), 400
    content, err = read_remote_config(host)
    if err:
        return jsonify({"ok": False, "err": err}), 500
    return jsonify({"ok": True, "content": content, "path": CONFIG_REMOTE_PATH})


@app.route("/api/config-save", methods=["POST"])
def api_config_save():
    d = request.get_json()
    host, content = d.get("host"), d.get("content")
    if not host or content is None:
        return jsonify({"ok": False, "err": "host & content required"}), 400
    ok, msg = write_remote_config(host, content)
    return jsonify({"ok": ok, "msg": msg})


@app.route("/api/stream")
def api_stream():
    host = request.args.get("host")
    role = request.args.get("role")
    script = request.args.get("script", "")
    if not host or role not in ROLE_META:
        return Response("host & role required", status=400)
    meta = resolve_meta(role, script)
    return Response(stream_with_context(tail_generator_meta(host, meta)),
                    mimetype="text/event-stream")


if __name__ == "__main__":
    threading.Thread(target=log_sync_loop, daemon=True).start()
    threading.Thread(target=_ssh_janitor, daemon=True).start()
    app.run(host="127.0.0.1", port=8765, debug=False, threaded=True)
