"""anomaly — steps 12/13/15 (+ step 5 batch): offline + battery + stagnation.

  - StagnationEngine — gateway: brak z2m przez P1/P2 h → `sg`/`sc`
  - OfflineMonitor   — supervisor: brak wiadomości > T1/T2/T3 min → available OFF/ON
  - BatteryMonitor   — gateway: low<25 / critical<15 dla ALL devices → `lb`/`cb`/`bo`
  - AnomalyBatcher   — gateway: bufor anomalii (dedup per sid+kategoria) → `ab` P2, split
"""
from .stagnation import StagnationEngine
from .offline import OfflineMonitor, GatewayOfflineAnomaly
from .battery import BatteryMonitor
from .temphum import TempHumMonitor
from .batcher import AnomalyBatcher, CODE_CAT
from .store import AnomalyStore
from .reconciler import AnomalyReconciler

__all__ = ["StagnationEngine", "OfflineMonitor", "GatewayOfflineAnomaly",
           "BatteryMonitor", "TempHumMonitor", "AnomalyBatcher", "CODE_CAT",
           "AnomalyStore", "AnomalyReconciler"]
