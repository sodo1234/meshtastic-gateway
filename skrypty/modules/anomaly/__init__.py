"""anomaly — steps 12/15: device offline timeout + stagnation.

  - StagnationEngine — gateway: brak z2m przez P1/P2 h → `ab`(sg)/`sc`
  - OfflineMonitor   — supervisor: brak wiadomości > T1/T2/T3 min → available OFF/ON
"""
from .stagnation import StagnationEngine
from .offline import OfflineMonitor

__all__ = ["StagnationEngine", "OfflineMonitor"]
