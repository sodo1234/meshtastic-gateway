"""Discovery — Z2M device parsing + compact LoRa protocol.

Extracted from gateway_v38.py:1765-1811, 2371-2426.
Gateway: parses Z2M, builds short IDs, sends disc_meta + db over LoRa.
Supervisor: receives db, registers device entities in HA.
"""
import json
import hashlib
import os
import time


TYPE_MAP = {'sensor': 'S', 'switch': 'W', 'light': 'L', 'binary_sensor': 'B'}
TYPE_REV = {v: k for k, v in TYPE_MAP.items()}
CAPS_MAP = {'temperature': 't', 'humidity': 'h', 'battery': 'b', 'state': 's',
            'brightness': 'r', 'contact': 'c', 'occupancy': 'o',
            'water_leak': 'w', 'smoke': 'k'}
CAPS_REV = {v: k for k, v in CAPS_MAP.items()}


def encode_caps(caps):
    return ''.join(CAPS_MAP.get(c, c[0]) for c in caps)


def analyze_z2m_device(dev):
    """Parse a single Z2M bridge/devices entry → {type, caps, mains_powered}."""
    info = {'type': 'sensor', 'caps': [], 'mains_powered': False}
    pw = dev.get('power_source', '')
    if 'Mains' in pw or 'DC' in pw:
        info['mains_powered'] = True
    for exp in dev.get('definition', {}).get('exposes', []):
        t = exp.get('type')
        if t == 'switch':
            info['type'] = 'switch'
            info['caps'].append('state')
            info['mains_powered'] = True
        elif t == 'light':
            info['type'] = 'light'
            info['caps'].append('state')
            info['mains_powered'] = True
            for f in exp.get('features', []):
                if f.get('name') == 'brightness':
                    info['caps'].append('brightness')
        elif t in ('numeric', 'binary'):
            n = exp.get('name', '').lower()
            for key, dt, cap in [
                ('temperature', 'sensor', 'temperature'),
                ('humidity', 'sensor', 'humidity'),
                ('battery', None, 'battery'),
                ('contact', 'binary_sensor', 'contact'),
                ('occupancy', 'binary_sensor', 'occupancy'),
                ('water_leak', 'binary_sensor', 'water_leak'),
                ('smoke', 'binary_sensor', 'smoke'),
            ]:
                if key in n:
                    if dt:
                        info['type'] = dt
                    if cap not in info['caps']:
                        info['caps'].append(cap)
                    break
    return info


class GatewayDiscovery:
    """Gateway side: parses Z2M, builds compact db, sends over LoRa."""

    def __init__(self, gw_id, monitored_names, priority_names, vio_config,
                 lora, logger=None, max_payload=150):
        self.gw_id = gw_id
        self.monitored_names = monitored_names
        self.priority_names = priority_names
        self.vio_config = vio_config or []
        self.lora = lora
        self.log = logger
        self.max_payload = max_payload
        self.devices = {}
        self.short_ids = {}
        self.short_rev = {}
        self.disc_hash = ""
        # (a) gdy ustawione: send_discovery wysyła MAPĘ jako skompresowany blob (ReliableTransfer
        # kind='devmap') zamiast pętli `db` 2-dev/pakiet (199 dev: 94 pkt → ~10 chunków). Wpięcie:
        # discovery.map_send_fn = lambda payload: rt.start_send(gw_id,'devmap',payload). None = stary db.
        self.map_send_fn = None
        self._devmap_hash = None      # anti-spam: nie re-wysyłaj devmap dla tego samego hash w oknie
        self._devmap_ts = 0.0
        # F7 (cichy start): to samo dla pary disc_meta+disc_vio — obserwowany spam ×3/40s przy
        # starcie (kilka wyzwalaczy naraz wysyła identyczny hash). devmap miał już swój gate.
        self._meta_hash = None
        self._meta_ts = 0.0

    def is_monitored(self, dev):
        if dev in self.priority_names:
            return True
        if not self.monitored_names:
            return True
        return dev in self.monitored_names

    def parse_z2m(self, payload):
        """Parse zigbee2mqtt/bridge/devices JSON payload."""
        try:
            devs = json.loads(payload)
        except Exception:
            return
        for dev in devs:
            name = dev.get('friendly_name')
            if not name or name == 'Coordinator':
                continue
            info = analyze_z2m_device(dev)
            info['monitored'] = self.is_monitored(name)
            info['priority'] = name in self.priority_names
            self.devices[name] = info
        self._assign_short_ids()
        self._compute_hash()
        if self.log:
            mon = sum(1 for d in self.devices.values() if d.get('monitored'))
            self.log.info('DISC', f'🔭 Z2M: {len(self.devices)} devices, '
                          f'{mon} monitored, hash={self.disc_hash}')

    def _assign_short_ids(self):
        # Override 2026-06-25: sid dla WSZYSTKICH urządzeń bramki (anomalie muszą działać dla
        # każdego, też nie-monitored / nie-autodiscover). Identyfikacja po nazwie u supervisora
        # przez db (sid→nazwa). Dane (batch t/h) nadal tylko monitored — gate w GatewayData.
        all_devs = sorted(self.devices.keys())
        self.short_ids = {name: idx for idx, name in enumerate(all_devs)}
        self.short_rev = {idx: name for name, idx in self.short_ids.items()}

    def _compute_hash(self):
        all_devs_h = {}
        for dev, info in sorted(self.devices.items()):   # WSZYSTKIE urządzenia (anomalie dla każdego)
            sid = self.short_ids.get(dev, -1)
            all_devs_h[dev] = {"type": info['type'],
                               "caps": sorted(info['caps']), "sid": sid}
        vio_defs = [(v['id'], v['type']) for v in self.vio_config]
        raw = json.dumps({"devs": all_devs_h, "vio": vio_defs},
                         separators=(',', ':'), sort_keys=True)
        self.disc_hash = hashlib.sha256(raw.encode()).hexdigest()[:12]

    def send_discovery(self, data=None):
        """Send without delay (backward compat). Use send_discovery_with_delay for LoRa."""
        self.send_discovery_with_delay(0)

    def send_discovery_with_delay(self, delay=4.0, meta_only=False, force=False):
        """Send disc_meta + disc_vio + db with delay between packets (LoRa cooldown).

        meta_only=True → wyślij TYLKO disc_meta (lekki advertise hash), pomiń disc_vio+db.
        Używane przez anti-spam: gdy supervisor już ma aktualny zestaw (hash zgodny), bramka
        nie marnuje LoRa na pełne `db` — odsyła sam hash, supervisor potwierdza zgodność.

        F7 (cichy start): anti-spam 180s na PARZE disc_meta+disc_vio — ten sam disc_hash
        wysłany <180s temu → NIC (devmap ma już własny gate niżej). force=True (jawne żądanie
        `disc` z supervisora / ręczny przycisk Discovery) omija ten guard."""
        import time as _time
        now = _time.time()
        if not force and self.disc_hash == self._meta_hash and (now - self._meta_ts) < 180:
            if self.log:
                self.log.info('DISC', f'⏭️ disc_meta/disc_vio pominięte (ten sam hash <180s) — '
                              f'anti-spam cichego startu')
            return
        self._meta_hash = self.disc_hash
        self._meta_ts = now
        meta = {'t': 'disc_meta', 'g': self.gw_id,
                'hash': self.disc_hash,
                'dev_n': len(self.devices)}                # WSZYSTKIE urządzenia
        self.lora.send(json.dumps(meta, separators=(',', ':')))
        if self.log:
            self.log.info('DISC', f'📤 disc_meta: hash={self.disc_hash} dev_n={meta["dev_n"]}'
                          f'{" (meta_only — anti-spam)" if meta_only else ""}')
        if meta_only:
            return

        if self.vio_config:
            if delay > 0:
                _time.sleep(delay)
            vio_items = []
            for vs in self.vio_config:
                entry = [vs['id'], vs['type'][0], vs.get('name', vs['id'])]
                if vs['type'] == 'switch':
                    entry.append(0)
                vio_items.append(entry)
            vio_pkt = {'t': 'disc_vio', 'g': self.gw_id, 'd': vio_items}
            self.lora.send(json.dumps(vio_pkt, separators=(',', ':')))
            if self.log:
                self.log.info('DISC', f'📤 disc_vio: {len(vio_items)} items')

        # (a) MAPA: skompresowany blob (devmap) przez ReliableTransfer — JEDEN transfer zamiast
        # ~94 pakietów `db`. Fallback do starej pętli `db` gdy map_send_fn niewpięte.
        if self.map_send_fn:
            import time as _t
            now = _t.time()
            # anti-spam: nie startuj nowego devmap gdy ten sam hash wysłany <180s temu
            # (supervisor pytał o disc w pętli bo devmap nie lądował → nowy rt_begin za każdym razem = zator)
            if self.disc_hash == self._devmap_hash and (now - self._devmap_ts) < 180:
                if self.log:
                    self.log.info('DISC', '⏭️ devmap pominięty (ten sam hash <180s) — anti-spam')
                return
            payload = self.build_devmap()
            self.map_send_fn(payload)
            self._devmap_hash = self.disc_hash
            self._devmap_ts = now
            if self.log:
                self.log.info('DISC', f'📤 devmap: {len(payload.get("devs", {}))} urządzeń → ReliableTransfer (1 blob)')
            return

        items = []
        for dev, info in self.devices.items():   # db: WSZYSTKIE urządzenia (sid→nazwa u supervisora)
            sid = self.short_ids.get(dev, -1)
            type_char = TYPE_MAP.get(info['type'], '?')
            caps_str = encode_caps(info.get('caps', []))
            items.append([sid, dev, type_char, caps_str])

        for pkt in self._batch_split(items):
            if delay > 0:
                _time.sleep(delay)
            self.lora.send(json.dumps(pkt, separators=(',', ':')))

        if self.log:
            self.log.info('DISC', f'📤 db: {len(items)} devices sent')

    def _batch_split(self, items):
        if not items:
            return []
        packets, current = [], []
        for item in items:
            test = current + [item]
            test_pkt = json.dumps({"t": "db", "g": self.gw_id, "d": test},
                                  separators=(',', ':'))
            if len(test_pkt) > self.max_payload and current:
                packets.append({"t": "db", "g": self.gw_id, "d": current})
                current = [item]
            else:
                current = test
        if current:
            packets.append({"t": "db", "g": self.gw_id, "d": current})
        return packets

    # ── (a) MAPA URZĄDZEŃ jako skompresowany plik (ReliableTransfer kind='devmap') ──
    def build_devmap(self):
        """Snapshot mapy sid→(nazwa,typ,caps) dla WSZYSTKICH urządzeń jako payload
        ReliableTransfer (JSON→zlib→base64→chunk+CRC w warstwie rt). Zastępuje pętlę
        `db` po ~2 dev/pakiet (199 dev = 94 pakiety) JEDNYM transferem (~10 chunków zlib).
        Wysyłany RAZ na starcie + WYŁĄCZNIE RĘCZNIE (przycisk). Supervisor rejestruje encje
        z devs identycznie jak z `db`. hash = weryfikacja spójności (== disc_hash)."""
        devs = {}
        for dev, info in self.devices.items():
            sid = self.short_ids.get(dev, -1)
            devs[str(sid)] = [dev, TYPE_MAP.get(info['type'], '?'),
                              encode_caps(info.get('caps', []))]
        return {'hash': self.disc_hash, 'dev_n': len(self.devices), 'devs': devs}

    # ── (b) AUTODISCOVERY monitored+priority — OSOBNA funkcja (oddzielona od pełnej mapy) ──
    def send_autodiscovery(self):
        """Lekki advertise TYLKO zbiorów monitored+priority (sid-y) — bez pełnego `db`/devmap.
        Pozwala odświeżyć „które urządzenia są istotne" tanio (1 pakiet), gdy zmieni się skład
        monitored/priority, nie ruszając ciężkiej mapy 199 urządzeń. Typ `disc_ap` P0."""
        mon = [self.short_ids[d] for d, i in self.devices.items()
               if i.get('monitored') and not i.get('priority') and d in self.short_ids]
        pri = [self.short_ids[d] for d, i in self.devices.items()
               if i.get('priority') and d in self.short_ids]
        pkt = {'t': 'disc_ap', 'g': self.gw_id, 'hash': self.disc_hash,
               'mon': sorted(mon), 'pri': sorted(pri)}
        self.lora.send(json.dumps(pkt, separators=(',', ':')))
        if self.log:
            self.log.info('DISC', f'📤 autodiscovery: mon={len(mon)} pri={len(pri)} (osobno od devmap)')
        return pkt

    def get_devices_summary(self):
        return {name: info for name, info in self.devices.items()}


class SupervisorDiscovery:
    """Supervisor side: processes incoming disc_meta + db, registers HA entities."""

    def __init__(self, ha_entities, logger=None, request_disc_fn=None, disc_retry=90,
                 persist_path=None):
        """
        request_disc_fn: callable(gw) → sends a `disc` request over LoRa. When set,
                         the supervisor auto-requests a fresh device list whenever an
                         incoming HB/disc_meta hash differs from the synced one.
        disc_retry:      seconds before re-requesting if db never arrived.
        persist_path:    F7 (cichy start) — plik {gw:{devices,synced}} przetrwały restart.
                         Po restarcie supervisora hashe zgadzają się od razu → note_hash nie
                         żąda niczego → koniec sztormu devmap/db po starcie. Encje re-rejestracja
                         NIEPOTRZEBNA (configi MQTT discovery są retained w brokerze).
        """
        self.ha = ha_entities
        self.log = logger
        self.request_disc_fn = request_disc_fn
        self.disc_retry = disc_retry
        self.persist_path = persist_path
        self.gw_devices = {}  # {gw: {dev: {type, caps, sid}}}
        self.gw_hashes = {}   # {gw: last advertised hash (disc_meta/hb)}
        self.gw_synced = {}   # {gw: hash for which device list is registered}
        self._pending = {}    # {gw: (hash, ts)} — disc requested, awaiting db
        self.gw_devn = {}     # {gw: expected device count (from disc_meta dev_n)}
        self.gw_disc_ts = {}  # {gw: ts of last disc_meta / re-request}
        self.gw_disc_try = {} # {gw: completeness re-request count}
        self._load()

    # ── persistence (F7) ────────────────────────────────
    def _load(self):
        if not self.persist_path or not os.path.exists(self.persist_path):
            return
        try:
            raw = json.load(open(self.persist_path, encoding="utf-8"))
            for gw, d in raw.items():
                self.gw_devices[gw] = d.get('devices', {})
                self.gw_synced[gw] = d.get('synced', '')
            if self.log:
                self.log.info('DISC', f'💾 persist wczytany: {len(raw)} bramek '
                              f'({sum(len(v) for v in self.gw_devices.values())} urządzeń)')
        except Exception as e:
            if self.log:
                self.log.warn('DISC', f'persist load: {e}')

    def _save(self):
        if not self.persist_path:
            return
        try:
            raw = {gw: {'devices': self.gw_devices.get(gw, {}),
                       'synced': self.gw_synced.get(gw, '')}
                  for gw in self.gw_devices}
            json.dump(raw, open(self.persist_path, "w", encoding="utf-8"),
                      separators=(',', ':'))
        except Exception as e:
            if self.log:
                self.log.warn('DISC', f'persist save: {e}')

    def handle_disc_meta(self, data):
        gw = data.get('g', '?')
        self.gw_hashes[gw] = data.get('hash', '')
        self.gw_devn[gw] = int(data.get('dev_n', 0) or 0)
        self.gw_disc_ts[gw] = time.time()
        if self.log:
            self.log.info('DISC', f'📋 disc_meta {gw}: hash={data.get("hash")} '
                          f'dev_n={data.get("dev_n")} → czekam na db')

    def check_complete(self, retry_after=18, max_try=2):
        """Re-request disc if the device list is incomplete (lost db packet).
        Event-driven, capped — runs only after an incomplete discovery."""
        now = time.time()
        for gw, expected in list(self.gw_devn.items()):
            if not expected:
                continue
            have = len(self.gw_devices.get(gw, {}))
            if have >= expected:
                self.gw_disc_try[gw] = 0
                continue
            if now - self.gw_disc_ts.get(gw, 0) < retry_after:
                continue
            if self.gw_disc_try.get(gw, 0) >= max_try:
                continue
            self.gw_disc_try[gw] = self.gw_disc_try.get(gw, 0) + 1
            self.gw_disc_ts[gw] = now
            if self.log:
                self.log.warn('DISC', f'🔁 {gw} lista niepełna ({have}/{expected}) — '
                              f're-request disc ({self.gw_disc_try[gw]}/{max_try})')
            if self.request_disc_fn:
                self.request_disc_fn(gw)

    def note_hash(self, gw, hash_):
        """Compare advertised hash (from hb/pong/disc_meta) with synced device list.
        On mismatch → auto-request discovery (once per disc_retry window)."""
        if not gw or not hash_:
            return
        self.gw_hashes[gw] = hash_
        if hash_ == self.gw_synced.get(gw):
            self._pending.pop(gw, None)
            return
        prev = self._pending.get(gw)
        now = time.time()
        if prev and prev[0] == hash_ and (now - prev[1]) < self.disc_retry:
            return  # already requested recently for this hash
        self._pending[gw] = (hash_, now)
        old = self.gw_synced.get(gw, '∅')
        if self.log:
            self.log.info('DISC', f'🔄 {gw} hash {old}→{hash_} — auto-discovery (żądam disc)')
        if self.request_disc_fn:
            self.request_disc_fn(gw)

    def handle_disc_vio(self, data):
        gw = data.get('g', '?')
        for item in data.get('d', []):
            if len(item) < 3:
                continue
            vid, tp, name = item[0], item[1], item[2]
            val = item[3] if len(item) > 3 else 0
            if tp == 's':
                self.ha.reg_vswitch(gw, vid, name, val)
            elif tp == 'b':
                self.ha.reg_vbutton(gw, vid, name)
        if self.log:
            self.log.info('DISC', f'📋 disc_vio {gw}: {len(data.get("d",[]))} items')

    def handle_db(self, data):
        gw = data.get('g', '?')
        if gw not in self.gw_devices:
            self.gw_devices[gw] = {}
        for item in data.get('d', []):
            if len(item) < 4:
                continue
            sid, name, type_char, caps_str = item
            dtype = TYPE_REV.get(type_char, 'sensor')
            caps = caps_str if isinstance(caps_str, str) else ''
            self.gw_devices[gw][name] = {'sid': sid, 'type': dtype, 'caps': caps}
            if dtype == 'switch':
                self.ha.reg_switch_dev(gw, name)
            elif dtype == 'binary_sensor':
                self.ha.reg_binary(gw, name, caps)
            else:
                self.ha.reg_sensor(gw, name, caps)
        # device list now matches the last advertised hash → mark synced
        self.gw_synced[gw] = self.gw_hashes.get(gw, '')
        self._pending.pop(gw, None)
        self._save()          # F7: przetrwaj restart supervisora (koniec sztormu re-transferu)
        if self.log:
            self.log.info('DISC', f'📋 db {gw}: {len(data.get("d",[]))} urządzeń '
                          f'zarejestrowanych (synced hash={self.gw_synced[gw]})')

    def handle_devmap(self, gw, payload):
        """(a) Odbiór skompresowanej MAPY urządzeń (ReliableTransfer kind='devmap').
        payload = {'hash','dev_n','devs':{sid:[name,type_char,caps]}}. Rejestruje encje
        identycznie jak handle_db (jedna ścieżka rejestracji), po czym oznacza synced."""
        devs = (payload or {}).get('devs', {})
        if gw not in self.gw_devices:
            self.gw_devices[gw] = {}
        for sid, item in devs.items():
            if not isinstance(item, (list, tuple)) or len(item) < 3:
                continue
            name, type_char, caps_str = item[0], item[1], item[2]
            dtype = TYPE_REV.get(type_char, 'sensor')
            caps = caps_str if isinstance(caps_str, str) else ''
            try:
                sid_i = int(sid)
            except (TypeError, ValueError):
                sid_i = sid
            self.gw_devices[gw][name] = {'sid': sid_i, 'type': dtype, 'caps': caps}
            if dtype == 'switch':
                self.ha.reg_switch_dev(gw, name)
            elif dtype == 'binary_sensor':
                self.ha.reg_binary(gw, name, caps)
            else:
                self.ha.reg_sensor(gw, name, caps)
        h = (payload or {}).get('hash', '') or self.gw_hashes.get(gw, '')
        self.gw_hashes[gw] = h
        self.gw_synced[gw] = h
        self._pending.pop(gw, None)
        self._save()          # F7: przetrwaj restart supervisora (koniec sztormu re-transferu)
        if self.log:
            self.log.info('DISC', f'📋 devmap {gw}: {len(devs)} urządzeń zarejestrowanych '
                          f'(synced hash={h})')

    @staticmethod
    def _safe(s):
        return s.replace(' ', '_').lower()

    def find_device(self, gw, safe):
        """Reverse-map a topic-safe segment back to the original device name."""
        for name in self.gw_devices.get(gw, {}):
            if self._safe(name) == safe:
                return name
        return None

    def devices(self, gw):
        return self.gw_devices.get(gw, {})
