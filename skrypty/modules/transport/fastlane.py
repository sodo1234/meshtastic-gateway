"""Pure helpers for low-latency LoRa frames.

The transport owns queueing and sleeping.  This module only classifies frames
and calculates gate/LBT waits, which keeps gateway and supervisor policy equal
and makes the timing rules testable without radio hardware.
"""
import json
from collections.abc import Mapping

from .codec import long_type


DEFAULT_IMMEDIATE_TYPES = (
    "ping", "pong", "sup_ping", "sup_pong", "cmd", "st", "stm",
    "sync", "params", "params_req", "param_upd", "zg",
)


def frame_type(frame):
    """Return the normalized long ``t`` value from a dict or JSON string."""
    try:
        data = json.loads(frame) if isinstance(frame, str) else frame
        if not isinstance(data, Mapping):
            return ""
        return long_type(data.get("t", ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""


def immediate_types(values=None):
    """Normalize configured long/short type names to an immutable long set."""
    source = DEFAULT_IMMEDIATE_TYPES if values is None else values
    return frozenset(long_type(value) for value in source if isinstance(value, str))


def is_immediate_frame(frame, values=None):
    """Whether *frame* belongs to the configured fast lane."""
    return frame_type(frame) in immediate_types(values)


def slot_wait_seconds(frame, values, scheduled_wait):
    """Return zero for fast-lane frames, otherwise the scheduler's wait."""
    if is_immediate_frame(frame, values):
        return 0.0
    try:
        return max(0.0, float(scheduled_wait))
    except (TypeError, ValueError):
        return 0.0


def lbt_wait_seconds(last_rx_time, now, wait_started, quiet=2.0, max_wait=6.0):
    """Calculate the next listen-before-talk sleep, capped from *wait_started*.

    Times must use the same monotonic clock.  A non-positive/unknown RX time
    means that the channel has not been observed busy and needs no LBT delay.
    """
    quiet = max(0.0, float(quiet))
    max_wait = max(0.0, float(max_wait))
    if quiet == 0.0 or not last_rx_time:
        return 0.0
    quiet_left = quiet - max(0.0, now - last_rx_time)
    cap_left = max_wait - max(0.0, now - wait_started)
    if quiet_left <= 0.0 or cap_left <= 0.0:
        return 0.0
    return min(quiet_left, cap_left)
