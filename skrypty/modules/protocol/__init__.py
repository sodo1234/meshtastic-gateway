"""protocol — Step 2: Ping/Pong/HB, HA entities, Discovery.

Modules:
  - HAEntities          — MQTT discovery → creates entities in HA
  - GatewayHeartbeat    — periodic HB, responds ping→pong (gateway side)
  - SupervisorHeartbeat — processes HB, updates HA (supervisor side)
  - GatewayDiscovery    — Z2M parsing, compact db, sends over LoRa (gateway side)
  - SupervisorDiscovery — receives db, registers device entities in HA (supervisor side)
  - SupervisorLinkProbe — F2: aktywny ping/pong bramka↔supervisor, wskaźnik jakości łącza
"""
from .ha_entities import HAEntities
from .heartbeat import GatewayHeartbeat, SupervisorHeartbeat
from .discovery import GatewayDiscovery, SupervisorDiscovery
from .supervisor_link import SupervisorLinkProbe

__all__ = [
    "HAEntities",
    "GatewayHeartbeat", "SupervisorHeartbeat",
    "GatewayDiscovery", "SupervisorDiscovery",
    "SupervisorLinkProbe",
]
