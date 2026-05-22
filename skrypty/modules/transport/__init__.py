"""transport — Step 1 of refactoring plan.

Three building blocks, all with constructor injection:
  - Dispatcher       — routes parsed JSON dicts to handlers by `data['t']`
  - MqttTransport    — paho wrapper; on_message(topic, payload_str) → user callback
  - LoraTransport    — Meshtastic SerialInterface manager; on_receive(text) → user callback

Wiring pattern (in gateway/supervisor main):

    dispatcher = Dispatcher(logger=log)
    dispatcher.register('cfg', handle_cfg)
    dispatcher.register('cmd', handle_cmd)
    # ...

    mqtt = MqttTransport(host, port, user, pwd,
                         on_message=lambda topic, payload: route_mqtt(topic, payload),
                         logger=log)
    lora = LoraTransport(ports_cfg=CONFIG['mesh_ports'],
                         reconnect_cfg=CONFIG['mesh_reconnect'],
                         on_receive=dispatcher.dispatch_raw,
                         logger=log)
    mqtt.start(); lora.start()
"""
from .dispatcher import Dispatcher
from .mqtt_transport import MqttTransport
from .lora_transport import LoraTransport

__all__ = ["Dispatcher", "MqttTransport", "LoraTransport"]
