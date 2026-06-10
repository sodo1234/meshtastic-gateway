"""Z2M delta + short-key encoding for the `b` (batch) protocol.

Delta thresholds ported from gateway_v10.py:686-690 (source of truth):
  temperature ≥ 0.5°C, humidity ≥ 2%, battery/binary/state = any change.
Only changed caps are shipped → minimal LoRa traffic.

Short keys (CLAUDE.md `b`):  a=avail t=temp h=hum b=batt s=state
                             c=contact o=occupancy w=water_leak k=smoke r=brightness
                             q=linkquality (Zigbee LQI 0-255, ride-along)
"""

# Z2M cap (full name) → short key used inside `b` payloads
CAP_SHORT = {
    'temperature': 't', 'humidity': 'h', 'battery': 'b', 'state': 's',
    'contact': 'c', 'occupancy': 'o', 'water_leak': 'w', 'smoke': 'k',
    'brightness': 'r', 'linkquality': 'q',
}
SHORT_CAP = {v: k for k, v in CAP_SHORT.items()}

# numeric caps that use a magnitude threshold; everything else = any change
DEFAULT_THRESHOLDS = {'temperature': 0.5, 'humidity': 2.0}

# caps per device type that we report (mirrors v10 _send_status)
TYPE_CAPS = {
    'sensor': ('temperature', 'humidity', 'battery'),
    'binary_sensor': ('contact', 'occupancy', 'water_leak', 'smoke', 'battery'),
    'switch': ('state', 'brightness'),
    'light': ('state', 'brightness'),
}


def compute_delta(old, new, dtype, thresholds=None):
    """Return {cap: value} for caps that changed beyond threshold, vs old state.

    First observation (cap absent in old) always counts as a change.
    """
    th = thresholds or DEFAULT_THRESHOLDS
    changed = {}
    for cap in TYPE_CAPS.get(dtype, ()):
        if cap not in new:
            continue
        nv = new[cap]
        if cap not in old:
            changed[cap] = nv
            continue
        ov = old[cap]
        if cap in th:
            try:
                if abs(float(nv) - float(ov)) >= th[cap]:
                    changed[cap] = nv
            except (TypeError, ValueError):
                if nv != ov:
                    changed[cap] = nv
        elif nv != ov:
            changed[cap] = nv
    return changed


def _norm(cap, value):
    """Normalize a Z2M value to the compact wire form for its short key."""
    if cap == 'temperature':
        try:
            return round(float(value), 1)
        except (TypeError, ValueError):
            return value
    if cap in ('humidity', 'battery', 'brightness', 'linkquality'):
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            return value
    if cap == 'state':
        return 1 if str(value).upper() == 'ON' else 0
    if cap in ('contact', 'occupancy', 'water_leak', 'smoke'):
        return 1 if value else 0
    return value


def encode_short(caps_dict, available=True):
    """Encode {cap: value} (+availability) into a short-key dict for a `b` entry.

    {'temperature':22.5,'battery':95} → {'a':1,'t':22.5,'b':95}
    """
    out = {'a': 1 if available else 0}
    for cap, val in caps_dict.items():
        sk = CAP_SHORT.get(cap)
        if sk:
            out[sk] = _norm(cap, val)
    return out


def decode_short(short_fields):
    """Inverse of encode_short → {ha_json_key: value} for HA's value_template.

    Supervisor side: maps short keys back to the value_json.* names the
    entities (ha_entities.py) read: temperature/humidity/battery/state/...
    plus availability as 'ON'/'OFF'.
    """
    out = {}
    for sk, val in short_fields.items():
        if sk == 'a':
            out['available'] = 'ON' if val else 'OFF'
            continue
        cap = SHORT_CAP.get(sk)
        if not cap:
            continue
        if cap == 'state':
            out['state'] = 'ON' if val else 'OFF'
        elif cap in ('contact', 'occupancy', 'water_leak', 'smoke'):
            out[cap] = bool(val)
        else:
            out[cap] = val
    return out
