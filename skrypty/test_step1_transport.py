#!/usr/bin/env python3
"""
Step 1 Integration Test — Transport Module (MQTT + LoRa + Dispatcher)

Testuje TYLKO warstwę transportu:
  - MqttTransport: connect do Mosquitto, odbiór wiadomości
  - LoraTransport: connect do Heltec, odbiór/wysyłka ramek
  - Dispatcher: routing po polu "t" (2 testowe typy, nie protokół v38)

Surowa ramka wchodzi → log. Testowa ramka wychodzi → log.
"""
import logging
logging.getLogger("meshtastic").setLevel(logging.WARNING)
logging.getLogger("meshtastic.serial_interface").setLevel(logging.WARNING)

import json, os, signal, sys, time, threading
from datetime import datetime
from logging.handlers import RotatingFileHandler

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import CONFIG, ROLE
from modules.transport import Dispatcher, MqttTransport, LoraTransport


class Log:
    C = {'DEBUG': '\033[36m', 'INFO': '\033[32m', 'WARN': '\033[33m', 'ERROR': '\033[31m'}
    R = '\033[0m'
    def __init__(self):
        self._h = RotatingFileHandler(CONFIG['log_file'], maxBytes=2_000_000, backupCount=2)
        self._h.setFormatter(logging.Formatter('%(message)s'))
        self._f = logging.getLogger('test_transport')
        self._f.addHandler(self._h); self._f.setLevel(logging.DEBUG)
    def _w(self, l, c, m):
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"{ts} {self.C.get(l,'')}[{l}] [{c}]{self.R} {m}", flush=True)
        self._f.info(f"{ts} [{l}] [{c}] {m}")
    def debug(self, c, m): self._w('DEBUG', c, m)
    def info(self, c, m): self._w('INFO', c, m)
    def warn(self, c, m): self._w('WARN', c, m)
    def error(self, c, m): self._w('ERROR', c, m)
    def close(self):
        try: self._h.close()
        except: pass


def main():
    log = Log()
    log.info('MAIN', '=' * 50)
    log.info('MAIN', f'STEP 1 TRANSPORT TEST — {CONFIG["id"]} ({ROLE})')
    log.info('MAIN', '=' * 50)

    rx_count = {'mqtt': 0, 'lora': 0}
    running = threading.Event()
    running.set()

    dispatcher = Dispatcher(logger=log)
    dispatcher.register('test_ping', lambda d: log.info('DISPATCH', f'test_ping received: {d}'))
    dispatcher.register('test_echo', lambda d: log.info('DISPATCH', f'test_echo received: {d}'))
    dispatcher.set_fallback(lambda d: log.info('DISPATCH', f'passthrough t={d.get("t")} | {json.dumps(d,separators=(",",":"))[:100]}'))
    log.info('MAIN', 'Dispatcher: 2 test handlers + fallback (log-only)')

    mqtt_cfg = CONFIG['mqtt']
    def on_mqtt(topic, payload):
        rx_count['mqtt'] += 1
        log.info('MQTT-RX', f'{topic} ({len(payload)}B)')

    mqtt = MqttTransport(
        host=mqtt_cfg['host'], port=mqtt_cfg['port'],
        user=mqtt_cfg['user'], password=mqtt_cfg['pass'],
        client_id=f"test_{CONFIG['id']}_{int(time.time())}",
        on_message=on_mqtt, logger=log)
    mqtt.subscribe('zigbee2mqtt/bridge/devices')
    mqtt.subscribe('zigbee2mqtt/+')

    log.info('MQTT', f'Connecting {mqtt_cfg["host"]}:{mqtt_cfg["port"]}...')
    mqtt.start()
    if mqtt.wait_connected(timeout=10):
        log.info('MQTT', 'OK')
    else:
        log.error('MQTT', 'TIMEOUT')

    # LoRa — gateway ma mesh_port, supervisor ma mesh_ports[]
    if 'mesh_port' in CONFIG:
        ports_cfg = [{"port": CONFIG['mesh_port'], "enabled": True,
                      "label": "ANT-1", "gateways": [CONFIG['id']]}]
    else:
        ports_cfg = CONFIG.get('mesh_ports', [])

    def on_lora(text):
        rx_count['lora'] += 1
        dispatcher.dispatch_raw(text)

    lora = LoraTransport(
        ports_cfg=ports_cfg,
        reconnect_cfg=CONFIG.get('mesh_reconnect', {}),
        on_receive=on_lora, logger=log)

    log.info('LORA', f'Connecting...')
    lora.start()
    for label, info in lora.get_status().items():
        log.info('LORA', f'{label}: {"OK" if info["connected"] else "FAIL"}')

    def tx_test():
        time.sleep(15)
        if not lora.interfaces:
            log.warn('TX', 'No antenna — skip'); return
        msg = json.dumps({"t":"test_ping","g":CONFIG['id'],"ts":int(time.time())}, separators=(',',':'))
        log.info('TX', f'Sending: {msg}')
        lora.send(msg)
    threading.Thread(target=tx_test, daemon=True).start()

    def stats():
        while running.is_set():
            time.sleep(60)
            log.info('STATS', f'MQTT rx={rx_count["mqtt"]} | LoRa rx={rx_count["lora"]}')
    threading.Thread(target=stats, daemon=True).start()

    log.info('MAIN', 'Running. Ctrl+C to stop.')
    signal.signal(signal.SIGINT, lambda *_: running.clear())
    signal.signal(signal.SIGTERM, lambda *_: running.clear())
    try:
        while running.is_set(): time.sleep(0.5)
    except KeyboardInterrupt: pass

    log.info('STATS', f'Final: MQTT rx={rx_count["mqtt"]} | LoRa rx={rx_count["lora"]}')
    lora.stop(); mqtt.stop()
    log.info('MAIN', 'Done.')
    log.close()


if __name__ == '__main__':
    main()
