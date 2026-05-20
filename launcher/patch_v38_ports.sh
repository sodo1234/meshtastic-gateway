#!/bin/bash
# Patches mesh_port in gateway_v38.py and first ANT-1 entry in supervisor_v38.py
# Run on Debian after fresh restore from .bak
set -e
cd ~/meshtastic

GW_PORT='/dev/serial/by-id/usb-Espressif_Systems_heltec_wifi_lora_32_v4__16_MB_FLASH__2_MB_PSRAM__441BF670AFC0-if00'
SUP_PORT='/dev/serial/by-id/usb-Espressif_Systems_heltec_wifi_lora_32_v4__16_MB_FLASH__2_MB_PSRAM__F85B1BA59F3C-if00'

# gateway_v38.py: replace the single "mesh_port" value
python3 - "$GW_PORT" <<'PY'
import sys, re, pathlib
port = sys.argv[1]
p = pathlib.Path.home() / "meshtastic/gateway_v38.py"
src = p.read_text()
src = re.sub(r'("mesh_port"\s*:\s*)["\'][^"\']+["\']',
             lambda m: m.group(1) + repr(port), src, count=1)
p.write_text(src)
PY

# supervisor_v38.py: replace first port value (the ANT-1 line)
python3 - "$SUP_PORT" <<'PY'
import sys, re, pathlib
port = sys.argv[1]
p = pathlib.Path.home() / "meshtastic/supervisor_v38.py"
src = p.read_text()
src = re.sub(r'("port"\s*:\s*)["\'][^"\']+["\']',
             lambda m: m.group(1) + repr(port), src, count=1)
p.write_text(src)
PY

echo "=== gateway_v38 ==="
grep mesh_port gateway_v38.py | head -1
echo "=== supervisor_v38 ==="
grep -A1 mesh_ports supervisor_v38.py | head -3
