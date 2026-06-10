"""data — Step 3: device data streaming (batch monitored/priority + refresh).

Modules:
  - Batcher        — buffers deltas, flushes compact `b` packets (per stream)
  - GatewayData    — Z2M state → delta → monitored/priority batchers (gateway side)
  - SupervisorData — `b` packets → merged HA entity states (supervisor side)
  - z2m_reader     — delta thresholds + short-key encode/decode helpers
"""
from .batcher import Batcher
from .gateway_data import GatewayData
from .supervisor_data import SupervisorData
from .z2m_reader import (compute_delta, encode_short, decode_short,
                         CAP_SHORT, SHORT_CAP, TYPE_CAPS)

__all__ = [
    "Batcher", "GatewayData", "SupervisorData",
    "compute_delta", "encode_short", "decode_short",
    "CAP_SHORT", "SHORT_CAP", "TYPE_CAPS",
]
