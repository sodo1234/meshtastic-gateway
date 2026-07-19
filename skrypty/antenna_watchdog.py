#!/usr/bin/env python3
"""Antenna USB stall watchdog — #1 (2026-07-19).

Monitoruje log jądra (journalctl -kf) pod kątem stalla CP210x (`urb ... stopped: -32`,
`failed to submit urb`), po którym sterownik cp210x NIE wznawia endpointu RX i ttyUSB
umiera do resetu USB. Ten watchdog robi wtedy unbind/bind portu USB → ttyUSB wraca, a
launcher/harness (serial auto-reconnect) łapie port z powrotem.

Uzupełnia recovery w harnessie (który radzi sobie z reconnectem serial, ale NIE z
driver-level stallem endpointu). Runbook (memory 24.06): brltty mask + unbind/bind portu.

Uruchamiać jako root (systemd — patrz antenna-watchdog.service). Bezpieczny: debounce,
reset tylko konkretnego CP2102 (idVendor 10c4), nic destrukcyjnego.
"""
import glob
import os
import re
import subprocess
import time

STALL_RE = re.compile(r'cp210x.*(urb.*stopped|-32|failed to submit)', re.I)
DEBOUNCE_S = 60          # nie resetuj częściej niż raz/min (stall bywa spamowany w logu)
SILICON_LABS_VID = '10c4'


def find_cp210x_port():
    """Zwróć nazwę portu USB (np. '2-1') dla CP2102 (Silicon Labs), albo None."""
    for dev in glob.glob('/sys/bus/usb/devices/*'):
        try:
            with open(os.path.join(dev, 'idVendor')) as f:
                if f.read().strip() == SILICON_LABS_VID:
                    return os.path.basename(dev)
        except OSError:
            continue
    return None


def reset_usb(port):
    try:
        with open('/sys/bus/usb/drivers/usb/unbind', 'w') as f:
            f.write(port)
        time.sleep(2)
        with open('/sys/bus/usb/drivers/usb/bind', 'w') as f:
            f.write(port)
        return True
    except OSError as e:
        print(f'[watchdog] reset error: {e}', flush=True)
        return False


def main():
    print('[watchdog] antenna-watchdog start (CP210x urb -32 → auto USB reset)', flush=True)
    last_reset = 0.0
    proc = subprocess.Popen(
        ['journalctl', '-kf', '-n', '0', '--no-pager'],
        stdout=subprocess.PIPE, text=True, bufsize=1)
    try:
        for line in proc.stdout:
            if not STALL_RE.search(line):
                continue
            now = time.time()
            if now - last_reset < DEBOUNCE_S:
                continue
            port = find_cp210x_port()
            print(f'[watchdog] STALL: {line.strip()[:120]} → reset port={port}', flush=True)
            if port and reset_usb(port):
                last_reset = now
                print(f'[watchdog] ✅ USB reset {port} OK — ttyUSB powinno wrócić', flush=True)
            else:
                print('[watchdog] ⚠️ brak portu CP2102 lub reset nieudany', flush=True)
    finally:
        proc.terminate()


if __name__ == '__main__':
    main()
