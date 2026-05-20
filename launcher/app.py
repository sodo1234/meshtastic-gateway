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
LAN_HOSTS = [("192.168.50.49", "debian-dev")]

# role -> tmux session name + remote log paths (logger + stderr) + script-name hint for pgrep
ROLE_META = {
    "gw":  {"tmux": "gw",  "remote_log": "/tmp/gateway.log",
            "stderr_log": "/tmp/gateway.stderr.log", "pgrep": "gateway.*\\.py"},
    "sup": {"tmux": "sup", "remote_log": "/tmp/supervisor.log",
            "stderr_log": "/tmp/supervisor.stderr.log", "pgrep": "supervisor.*\\.py"},
}

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.jinja_env.auto_reload = True
_tail_threads = {}  # key=(host,role) -> {"thread":..., "queue":..., "stop":Event}


def ssh(host, cmd, timeout=15, check=False):
    """Run remote cmd over ssh. Returns (rc, stdout, stderr)."""
    full = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=accept-new",
            f"{SSH_USER}@{host}", cmd]
    try:
        p = subprocess.run(full, capture_output=True, text=True, timeout=timeout,
                           encoding='utf-8', errors='replace')
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


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


def patch_script_port(host, script, port):
    """Patch CONFIG mesh_port / meshtastic_port in target .py to `port`.
    Idempotent. Keeps .bak only on first patch."""
    if not port:
        return True, "no port override"
    # sed: replace value after 'mesh_port' or 'meshtastic_port' keys.
    # Using '~' as s-command separator (path contains /, pattern contains |, both unsafe).
    sed = (
        f"cd {REMOTE_DIR} && "
        f"[ ! -f {script}.bak ] && cp {script} {script}.bak; "
        f"sed -i -E \"s~(['\\\"](mesh_port|meshtastic_port)['\\\"][[:space:]]*:[[:space:]]*)['\\\"][^'\\\"]*['\\\"]~\\\\1'{port}'~g\" {script} && "
        f"grep -E \"(mesh_port|meshtastic_port)\" {script} | head -2"
    )
    rc, out, err = ssh(host, sed)
    return rc == 0, out.strip() or err


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


def start_remote(host, role, script, port=None):
    meta = ROLE_META[role]
    session = meta["tmux"]
    # patch port if provided
    if port:
        ok, msg = patch_script_port(host, script, port)
        if not ok:
            return False, f"port patch failed: {msg}"
    # kill old session + any manually-started instance of this script + truncate stderr
    pgrep_pat = meta["pgrep"]
    stderr_log = meta["stderr_log"]
    ssh(host, f"tmux kill-session -t {session} 2>/dev/null; "
              f"pkill -f 'python.*{pgrep_pat}' 2>/dev/null; "
              f": > {stderr_log}; true")
    # python -u = unbuffered; stderr redirected to separate file for crash capture
    cmd = (f"cd {REMOTE_DIR} && "
           f"tmux new -d -s {session} 'venv/bin/python -u {script} 2>>{stderr_log}'")
    rc, out, err = ssh(host, cmd, timeout=10)
    if rc == 0:
        # Open visible terminal on Debian's GUI (NoMachine sees it)
        launch_gnome_terminal(host, session, f"{role.upper()} • {script}")
    return rc == 0, (err or out or f"started on {port or 'default port'}")


def stop_remote_full(host, role):
    """Stop tmux + kill script + close attached GUI terminals."""
    meta = ROLE_META[role]
    session = meta["tmux"]
    pgrep_pat = meta["pgrep"]
    close_gnome_terminals(host, session)
    ssh(host, f"tmux kill-session -t {session} 2>/dev/null; "
              f"pkill -f 'python.*{pgrep_pat}' 2>/dev/null; true")
    return True


def stop_remote(host, role):
    return stop_remote_full(host, role)


def status_remote(host, role):
    meta = ROLE_META[role]
    # Check both: tmux session (launcher-started) OR pgrep on script name (manual start)
    cmd = (
        f"tmux has-session -t {meta['tmux']} 2>/dev/null && echo TMUX_OK; "
        f"pgrep -af 'python.*{meta['pgrep']}' | head -1"
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


def pull_log(host, role):
    """scp remote log → local logs/ with date stamp."""
    meta = ROLE_META[role]
    today = datetime.now().strftime("%Y-%m-%d")
    local = LOG_DIR / f"{role}_{host.replace('.', '_')}_{today}.log"
    cmd = ["scp", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
           f"{SSH_USER}@{host}:{meta['remote_log']}", str(local)]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
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


# ── SSE live tail per (host, role) ──
def tail_generator(host, role):
    meta = ROLE_META[role]
    # tail BOTH the script's own log AND the captured stderr — so crashes are visible.
    # --pid 1 trick removed; we just follow both files (tail -F handles missing files).
    files = f"{meta['remote_log']} {meta['stderr_log']}"
    cmd = f"touch {meta['stderr_log']} 2>/dev/null; tail -n 80 -F {files} 2>/dev/null"
    proc = subprocess.Popen(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
         f"{SSH_USER}@{host}", cmd],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        encoding='utf-8', errors='replace')
    try:
        for line in iter(proc.stdout.readline, ""):
            if not line: break
            yield f"data: {json.dumps({'line': line.rstrip()})}\n\n"
    finally:
        proc.terminate()


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
    if not host or role not in ROLE_META:
        return jsonify({"error": "host & role required"}), 400
    return jsonify(status_remote(host, role))


@app.route("/api/start", methods=["POST"])
def api_start():
    d = request.get_json()
    host, role, script, port = d.get("host"), d.get("role"), d.get("script"), d.get("port")
    if not all([host, role, script]) or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host, role, script required"}), 400
    ensure_tmux(host)
    ok, msg = start_remote(host, role, script, port=port)
    return jsonify({"ok": ok, "msg": msg, "port": port})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    d = request.get_json()
    host, role = d.get("host"), d.get("role")
    if not host or role not in ROLE_META:
        return jsonify({"ok": False, "err": "host & role required"}), 400
    stop_remote(host, role)
    return jsonify({"ok": True})


@app.route("/api/pull-log", methods=["POST"])
def api_pull():
    d = request.get_json()
    return jsonify(pull_log(d.get("host"), d.get("role")))


@app.route("/api/open-tail", methods=["POST"])
def api_open_tail():
    d = request.get_json()
    open_tail_wt(d.get("host"), d.get("role"))
    return jsonify({"ok": True})


@app.route("/api/stream")
def api_stream():
    host = request.args.get("host")
    role = request.args.get("role")
    if not host or role not in ROLE_META:
        return Response("host & role required", status=400)
    return Response(stream_with_context(tail_generator(host, role)),
                    mimetype="text/event-stream")


if __name__ == "__main__":
    threading.Thread(target=log_sync_loop, daemon=True).start()
    app.run(host="127.0.0.1", port=8765, debug=False, threaded=True)
