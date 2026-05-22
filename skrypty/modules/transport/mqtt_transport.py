"""MqttTransport — thin paho-mqtt wrapper.

Connect, manage subscriptions across reconnects, publish dict/str payloads.
Hands incoming messages to on_message(topic, payload_str) — typically wired to
Dispatcher.dispatch_raw for protocol routing, or a custom callback for non-protocol
topics (e.g. Z2M bridge/events).
"""
import json
import threading

import paho.mqtt.client as mqtt


class MqttTransport:
    def __init__(self, host, port=1883, user=None, password=None, client_id=None,
                 on_message=None, on_connect=None, logger=None):
        self.host, self.port = host, port
        self.user, self.password = user, password
        self.client_id = client_id
        self._on_message_cb = on_message
        self._on_connect_cb = on_connect
        self.log = logger
        self._subscriptions = []
        self._connected = threading.Event()
        try:
            self.client = mqtt.Client(client_id=client_id,
                                      callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            self.client = mqtt.Client(client_id=client_id) if client_id else mqtt.Client()
        if user:
            self.client.username_pw_set(user, password)
        self.client.on_connect = self._on_connect_internal
        self.client.on_message = self._on_message_internal

    def subscribe(self, topic, qos=0):
        """Add subscription. Applied immediately if connected, else on next connect."""
        self._subscriptions.append((topic, qos))
        if self._connected.is_set():
            self.client.subscribe(topic, qos)

    def publish(self, topic, payload, qos=0, retain=False):
        """Publish dict (auto-JSON, compact separators) or raw str/bytes."""
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, separators=(',', ':'))
        return self.client.publish(topic, payload, qos=qos, retain=retain)

    def start(self):
        """Connect + start background network loop. Non-blocking."""
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        try:
            self.client.disconnect()
        except Exception:
            pass

    def wait_connected(self, timeout=10):
        return self._connected.wait(timeout=timeout)

    @property
    def connected(self):
        return self._connected.is_set()

    def _on_connect_internal(self, client, userdata, flags, rc, properties=None):
        self._connected.set()
        for topic, qos in self._subscriptions:
            self.client.subscribe(topic, qos)
        if self.log:
            self.log.info('MQTT', f"✅ Connected rc={rc}, {len(self._subscriptions)} sub(s)")
        if self._on_connect_cb:
            try:
                self._on_connect_cb(rc)
            except Exception as e:
                if self.log: self.log.error('MQTT', f"on_connect raised: {e}")

    def _on_message_internal(self, client, userdata, msg):
        if not self._on_message_cb:
            return
        try:
            payload = msg.payload.decode('utf-8', errors='replace')
            self._on_message_cb(msg.topic, payload)
        except Exception as e:
            if self.log:
                self.log.error('MQTT', f"on_message error: {e}")
