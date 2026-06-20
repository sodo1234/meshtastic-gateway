"""Slotting / anti-collision (STEP 4 / krok 7).

SlotScheduler — bramka TX-uje tylko w swoim oknie sekundowym minuty (G1:0-19, G2:20-39,
G3:40-59) → 3 bramki nie kolidują na współdzielonym paśmie 868.
SafeWindow — supervisor wysyła do bramki PO usłyszeniu od niej (half-duplex friendly).
"""
from .slotting import SlotScheduler, SafeWindow

__all__ = ["SlotScheduler", "SafeWindow"]
