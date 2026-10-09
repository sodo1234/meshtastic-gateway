"""Dispatcher — routes incoming protocol messages to registered handlers by `data['t']`.

Design: flat switch table (pattern from gateway_v10.py:819 _handle_lora). No business logic here.
Transport layers (MQTT, LoRa) feed messages via dispatch() / dispatch_raw().
"""
import json
import time


class Dispatcher:
    def __init__(self, logger=None, gateway_id=None):
        self._handlers = {}
        self._fallback = None
        self.log = logger
        # Gateway-side address filter.  Frames without `g` remain global for
        # rolling-deploy compatibility; the supervisor leaves gateway_id=None.
        self.gateway_id = gateway_id
        self.foreign_dropped = 0
        self._last_foreign_log = 0.0

    def register(self, msg_type, handler):
        """Register handler for protocol type (e.g. 'hb', 'cmd', 'cfg', 'b', 'ab')."""
        self._handlers[msg_type] = handler

    def unregister(self, msg_type):
        self._handlers.pop(msg_type, None)

    def set_fallback(self, handler):
        """Handler invoked when msg type has no registered handler."""
        self._fallback = handler

    def dispatch(self, data):
        """Route a parsed dict to its handler. Catches handler exceptions."""
        if not isinstance(data, dict):
            if self.log: self.log.warn('DISP', f"non-dict payload: {type(data).__name__}")
            return
        foreign_gw = data.get('g')
        if (self.gateway_id is not None and foreign_gw is not None
                and foreign_gw != self.gateway_id):
            self.foreign_dropped += 1
            now = time.monotonic()
            if self.log and now - self._last_foreign_log >= 60.0:
                self._last_foreign_log = now
                self.log.debug('DISP', f"foreign g={foreign_gw} dropped "
                               f"(mine={self.gateway_id}, total={self.foreign_dropped})")
            return
        t = data.get('t')
        h = self._handlers.get(t)
        if h is None:
            if self._fallback: self._fallback(data)
            elif self.log: self.log.warn('DISP', f"unknown type {t!r}")
            return
        try:
            h(data)
        except Exception as e:
            if self.log: self.log.error('DISP', f"handler {t!r} raised: {e}")

    def dispatch_raw(self, text):
        """Parse JSON string then dispatch. Tolerates malformed input."""
        try:
            data = json.loads(text)
        except Exception as e:
            if self.log: self.log.error('DISP', f"bad JSON: {e}: {text[:120]}")
            return
        self.dispatch(data)

    def registered_types(self):
        return list(self._handlers.keys())
