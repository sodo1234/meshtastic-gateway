#!/usr/bin/env python3
"""Meshtastic Launcher - Flask web UI for remote start/stop/tail of gateway+supervisor."""
import json, os, re, subprocess, threading, time, queue
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, render_template, request, Response, stream_with_context

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

SSH_USER = "td"
REMOTE_DIR = "~/meshtastic"
REMOTE_VENV = "~/meshtastic/venv/bin/python"
LAN_HOSTS = [
    ("192.168.50.142", "debian-dev"),
    ("192.168.50.49", "debian-new"),
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
    "-o", "ConnectTimeout=5",
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ServerAliveInterval=10",
    "-o", "ServerAliveCountMax=2",
]

# PIDs of long-lived ssh processes spawned by Launcher (SSE streams).
# Anything NOT in this set is fair game for the janitor.
_protected_ssh_pids = set()
_protected_lock = threading.Lock()


def ssh(host, cmd, timeout=15, check=False):
    """Run remote cmd over ssh. Returns (rc, stdout, stderr)."""
    full = ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{host}", cmd]
    try:
        # stdin=DEVNULL: Windows ssh.exe hangs when inherited stdin is a socket
        # (Flask request threads) — explicit DEVNULL prevents that
        p = subprocess.run(full, stdin=subprocess.DEVNULL,
                           capture_output=True, text=True, timeout=timeout,
                           encoding='utf-8', errors='replace')
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
        p = subprocess.run(["tailscale", "status"], capture_output=True, text=True, timeout=5)
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


def list_scripts(host):
    """List *.py scripts in REMOTE_DIR on host."""
    rc, out, _ = ssh(host, f"ls {REMOTE_DIR}/*.py 2>/dev/null | xargs -n1 basename")
    if rc != 0: return []
    return [s.strip() for s in out.splitlines() if s.strip()]


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
    full = ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{host}", remote_cmd]
    try:
        p = subprocess.run(full, input=PORT_PATCHER, capture_output=True, text=True,
                           timeout=15, encoding='utf-8', errors='replace')
        out = (p.stdout or "").strip(); err = (p.stderr or "").strip()
        ok = p.returncode == 0 and ("PATCHED" in out or "NO_CHANGE" in out)
        return ok, out or err
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


def start_remote(host, role, script, port=None, extra_args=""):
    meta = resolve_meta(role, script)
    is_test = script.startswith("test_")
    session = meta["tmux"]
    if port and not is_test:
        ok, msg = patch_script_port(host, script, port)
        if not ok:
            return False, f"port patch failed: {msg}"
    # Kill tmux session + ALL python scripts that could hold the serial port
    # (both production and test scripts for this role)
    base = ROLE_META[role]
    stderr_log = meta["stderr_log"]
    ssh(host, f"tmux kill-session -t {session} 2>/dev/null; "
              f"pkill -f 'python.*{base['pgrep']}' 2>/dev/null; "
              f"pkill -f 'python.*test_step' 2>/dev/null; "
              f": > {stderr_log}; true")
    args_str = f" {extra_args}" if extra_args else ""
    cmd = (f"cd {REMOTE_DIR} && "
           f"tmux new -d -s {session} 'venv/bin/python -u {script}{args_str} 2>>{stderr_log}'")
    rc, out, err = ssh(host, cmd, timeout=10)
    if rc == 0:
        launch_gnome_terminal(host, session, f"{role.upper()} • {script}")
    return rc == 0, (err or out or f"started on {port or 'default port'}")


def stop_remote_full(host, role, script=""):
    """Stop tmux + kill script + close attached GUI terminals."""
    meta = resolve_meta(role, script)
    session = meta["tmux"]
    pgrep_pat = meta["pgrep"]
    close_gnome_terminals(host, session)
    ssh(host, f"tmux kill-session -t {session} 2>/dev/null; "
              f"pkill -f 'python.*{pgrep_pat}' 2>/dev/null; true")
    return True


def stop_remote(host, role, script=""):
    return stop_remote_full(host, role, script)


def status_remote(host, role, script=""):
    meta = resolve_meta(role, script)
    cmd = (
        f"tmux has-session -t {meta['tmux']} 2>/dev/null && echo TMUX_OK; "
        f"pgrep -af 'python.*{meta['pgrep']}' | grep -v pgrep | head -1"
    )
    rc, out, _ = ssh(host, cmd)
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
           f"{SSH_USER}@{host}:{meta['remote_log']}", str(local)]
    p = subprocess.run(cmd, stdin=subprocess.DEVNULL,
                       capture_output=True, text=True, timeout=30)
    if p.returncode == 0:
        return {"ok": True, "path": str(local), "size": local.stat().st_size}
    return {"ok": False, "err": p.stderr or p.stdout}


def open_tail_wt(host, role):
    """Open new wt.exe tab with live tail."""
    log = ROLE_META[role]["remote_log"]
    title = f"{role.upper()}-{host}"
    cmd = f'cmd /c start "" wt.exe new-tab --title {title} ssh {SSH_USER}@{host} "tail -F {log}"'
    subprocess.Popen(cmd, shell=True)
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
        ["ssh", *SSH_BASE_OPTS, f"{SSH_USER}@{host}", cmd],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        encoding='utf-8', errors='replace')
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


@app.route("/api/start", methods=["POST"])
def api_start():
    d = request.get_json()
    host, role, script = d.get("host"), d.get("role"), d.get("script")
    port, extra_args = d.get("port"), d.get("extra_args", "")
    if not all([host, role, script]) or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host, role, script required"}), 400
    ensure_tmux(host)
    ok, msg = start_remote(host, role, script, port=port, extra_args=extra_args)
    return jsonify({"ok": ok, "msg": msg, "port": port})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    d = request.get_json()
    host, role, script = d.get("host"), d.get("role"), d.get("script", "")
    if not host or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host & role required"}), 400
    stop_remote(host, role, script)
    return jsonify({"ok": True})


@app.route("/api/pull-log", methods=["POST"])
def api_pull():
    d = request.get_json()
    return jsonify(pull_log(d.get("host"), d.get("role"), d.get("script", "")))


@app.route("/api/open-tail", methods=["POST"])
def api_open_tail():
    d = request.get_json()
    open_tail_wt(d.get("host"), d.get("role"))
    return jsonify({"ok": True})


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
