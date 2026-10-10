"""Wire codec for the LoRa JSON protocol.

Application code always uses the descriptive (long) protocol names.  Only this
module translates the top level of a frame at the LoRa transport boundary.
Nested payloads are deliberately left untouched.

``TYPES`` and ``KEYS`` are the protocol's authoritative abbreviation tables.
The additional scopes in ``TYPES`` cover discriminator values carried by the
reliable-transfer and calendar frames; they cost airtime just like ``t``.
"""
from collections.abc import Mapping


TYPES = {
    "t": {
        "ab": "a",
        "ac_all": "aa",
        "ac_b": "ac",
        "an_d": "ad",
        "ansnap_req": "ar",
        "b": "b",
        "back": "bk",
        "cal_ack": "ca",
        "cal_begin": "cb",
        "cal_cack": "cc",
        "cal_chunk": "ck",
        "cal_end": "ce",
        "cal_pull": "cl",
        "cal_req": "cr",
        "cfg": "cf",
        "cmd": "c",
        "cmdm": "cm",
        "db": "d",
        "disc": "dc",
        "disc_ack": "da",
        "disc_ap": "dp",
        "disc_meta": "dm",
        "disc_vio": "dv",
        "dsnap_req": "dr",
        "dump_anom": "du",
        "hb": "h",
        "param_upd": "pu",
        "params": "ps",
        "params_req": "pr",
        "ping": "pi",
        "pong": "po",
        "req": "q",
        "rt_ack": "ra",
        "rt_begin": "rb",
        "rt_cack": "rc",
        "rt_chunk": "rk",
        "rt_end": "re",
        "set_mode": "sm",
        "st": "s",
        "stm": "m",
        "sup_ping": "sp",
        "sup_pong": "so",
        "sync": "sy",
        "sync_req": "sr",
        "vbtn": "vb",
        "vbtn_ack": "va",
        "vsw": "v",
        "vsw_st": "vt",
        "zg": "z",
    },
    "kind": {
        "ansnap": "an",
        "avail": "av",
        "bdata": "bd",
        "devmap": "md",
        "dsnap": "ds",
        "mass_offline": "mo",
    },
    "dir": {
        "cal": "cal",
        "gw_push": "gp",
        "push": "psh",
    },
}


# Existing one-character keys (g, d, h, m, p, s, ...) retain their established
# per-frame meanings.  Longer keys are shortened only where the frame scope is
# unambiguous (``win`` is included because the saved two bytes recur per window).
KEYS = {
    "cal_ack": {"miss": "mi"},
    "disc_ap": {"hash": "hh"},
    "disc_meta": {"hash": "hh", "dev_n": "dn"},
    "hb": {"hash": "hh"},
    "pong": {"hash": "hh"},
    "rt_ack": {"miss": "mi"},
    "rt_begin": {"kind": "k", "win": "w"},
    "rt_end": {"kind": "k"},
}


_TYPE_DECODE = {
    scope: {short: long for long, short in table.items()}
    for scope, table in TYPES.items()
}
_KEY_DECODE = {
    frame_type: {short: long for long, short in table.items()}
    for frame_type, table in KEYS.items()
}


def encode(frame_dict):
    """Return a shallow, wire-form copy of *frame_dict*.

    Unknown types and keys pass through.  Nested values retain their identity,
    so compact device-state dictionaries and blob payloads are never rewritten.
    """
    if not isinstance(frame_dict, Mapping):
        return frame_dict
    long_type = frame_dict.get("t")
    key_map = KEYS.get(long_type, {})
    out = {}
    for key, value in frame_dict.items():
        wire_key = key_map.get(key, key)
        if key == "t":
            value = TYPES["t"].get(value, value)
        elif key in ("kind", "dir"):
            value = TYPES[key].get(value, value)
        out[wire_key] = value
    return out


def decode(frame_dict):
    """Return a shallow, application-form copy of a received frame.

    Both abbreviated and already-long frames are accepted.  This is required
    during rolling deployments where the other endpoint may still send the old
    representation.
    """
    if not isinstance(frame_dict, Mapping):
        return frame_dict
    wire_type = frame_dict.get("t")
    long_type = _TYPE_DECODE["t"].get(wire_type, wire_type)
    key_map = _KEY_DECODE.get(long_type, {})
    out = {}
    for key, value in frame_dict.items():
        long_key = key_map.get(key, key)
        if long_key == "t":
            value = long_type
        elif long_key in ("kind", "dir"):
            value = _TYPE_DECODE[long_key].get(value, value)
        out[long_key] = value
    return out


def long_type(type_value):
    """Normalize a long or abbreviated ``t`` value to its long spelling."""
    return _TYPE_DECODE["t"].get(type_value, type_value)
