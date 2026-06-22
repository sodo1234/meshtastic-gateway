"""SupervisorData — Step 3 supervisor side: `b` packets → HA entity states.

Receives batched deltas, reverse-maps short id → device name (via
SupervisorDiscovery), MERGES into the device's retained state JSON and
republishes. The merge is the key Step 3 fix: ha.pub_device_state full-replaces
the retained topic, so a partial delta (just temp) must not wipe humidity/battery.

Any incoming `b` ⇒ device available=ON + last_seen refresh (online cascade).
"""
import time

from .z2m_reader import decode_short


def _now_str():
    return time.strftime('%d.%m %H:%M:%S')      # data + godzina dla last_seen


class SupervisorData:
    def __init__(self, ha_entities, discovery, logger=None, on_avail=None):
        self.ha = ha_entities          # HAEntities — pub_device_state
        self.disc = discovery          # SupervisorDiscovery — gw_devices (sid map)
        self.log = logger
        self.on_avail = on_avail       # callback(gw, dev, available:bool) — do licznika offline
        self.state = {}                # {gw: {dev: {ha_json fields}}} — merged
        self.last_msg_ts = {}          # {(gw,dev): epoch} — ostatnia wiadomość (dla offline)

    def handle_b(self, data):
        """{t:b,g:G1,ts:T,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}"""
        gw = data.get('g', '?')
        items = data.get('d', [])
        applied = 0
        for entry in items:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            sid, short = entry[0], entry[1]
            dev = self._dev_from_sid(gw, sid)
            if not dev:
                if self.log:
                    self.log.warn('DATA', f'b {gw}: nieznany sid={sid} (czekam na db)')
                continue
            fields = decode_short(short if isinstance(short, dict) else {})
            fields.setdefault('available', 'ON')      # online cascade
            fields['last_seen'] = _now_str()
            merged = self._merge(gw, dev, fields)
            self.ha.pub_device_state(gw, dev, merged)
            self.last_msg_ts[(gw, dev)] = time.time()    # reset zegara offline
            if self.on_avail:                            # licznik offline: available z `b`
                self.on_avail(gw, dev, str(merged.get('available', 'ON')).upper() == 'ON')
            applied += 1
        if self.log and applied:
            self.log.info('DATA', f'📊 b {gw}: {applied}/{len(items)} urządzeń zaktualizowanych')

    def hydrate(self, gw, dev, fields):
        """Startowy seed stanu z RETAINED encji HA (lora/<gl>/<dev>/state) — żeby pierwszy
        częściowy `b` po restarcie nie skasował reszty capów (merge ma od czego startować).
        Seeduje TYLKO gdy brak w pamięci → świeży `b` (handle_b) ma pierwszeństwo."""
        cur = self.state.setdefault(gw, {})
        if dev not in cur and isinstance(fields, dict) and fields:
            cur[dev] = dict(fields)
            # zasiej też cache ha_entities → pub_device_avail nie skasuje capów przed 1. `b`
            if hasattr(self.ha, 'seed_device_cache'):
                self.ha.seed_device_cache(gw, dev, fields)
            if self.on_avail and 'available' in fields:   # zasiej licznik offline z retained
                self.on_avail(gw, dev, str(fields['available']).upper() == 'ON')
            if self.log:
                self.log.debug('DATA', f'💧 hydrate {gw}/{dev}: {list(fields)}')
            return True
        return False

    # ── state merge (retained JSON is full-replace) ─────
    def _merge(self, gw, dev, fields):
        cur = self.state.setdefault(gw, {}).setdefault(dev, {})
        cur.update(fields)
        return dict(cur)

    # ── sid → device name (from registered db) ──────────
    def _dev_from_sid(self, gw, sid):
        for name, info in self.disc.devices(gw).items():
            if info.get('sid') == sid:
                return name
        return None

    def snapshot(self, gw=None):
        """Current merged states — for smoke tests / diagnostics."""
        return self.state if gw is None else self.state.get(gw, {})
