"""params — step 17: bidirectional parameter sync (gateway = master).

ParamSync exposes six tunables as HA `number` entities on both brokers and keeps
them in sync over LoRa (push-pull). Drives stagnation (P1/P2) and offline (T1/T2/T3).
"""
from .manager import ParamSync, PARAM_DEFS, PARAM_ORDER

__all__ = ["ParamSync", "PARAM_DEFS", "PARAM_ORDER"]
