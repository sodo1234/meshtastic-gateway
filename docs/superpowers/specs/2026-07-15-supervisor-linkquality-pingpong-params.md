# Spec — Supervisor linkquality mirror + gateway→supervisor ping/pong + per-gateway params

Date: 2026-07-15
Project: Meshtastic LoRa SCADA (step5 — `test_step5_anomaly.py` + `modules/`)
Source of truth: local repo `Documents/Meshtastic Gateway/skrypty/` → deployed to remote `~/meshtastic/` on gateway (100.98.155.78) and supervisor (100.79.111.24).
Status: APPROVED design, pending implementation plan.

## Context

After the 2026-07-15 MQTT cleanup, z2m on the gateway holds 7 real devices, mirrored to
the supervisor over LoRa by the harness. Three gaps remain:

1. The supervisor mirror carries battery/temp/hum/available/last_seen but **no linkquality** —
   `z2m_reader.py` defines the `q` (LQI) short-key and `decode_short` reverses it, but `TYPE_CAPS`
   omits linkquality, so it is never shipped, and the mirror discovery makes no LQI entity.
2. Ping/pong exists only **supervisor→gateway** (`SupervisorHeartbeat` pings, `GatewayHeartbeat.handle_ping`
   pongs). The gateway cannot tell whether the supervisor is alive.
3. `ParamSync` on the supervisor stores a **single global** param set with HA entity ids hardcoded to
   `supg1` / `lora_sup_param_*` — so it only represents one gateway. The per-gateway *send path*
   (`cmd/<gw>/send_x`, `_send_group(target_gw)`) already exists but has no per-gateway *storage/entities*.

## Non-goals (YAGNI)

- No data buffering / retransmission queue when supervisor is offline (passive indicator only).
- No new Send button or new param *group* — ping params reuse the existing P / T / Progi groups.
- No change to the existing supervisor→gateway heartbeat direction.
- No periodic LoRa traffic added for linkquality (ride-along only).

## Feature 1 — Linkquality ride-along in the mirror

**Requirement:** each mirrored device on the supervisor exposes a `linkquality` sensor whose value
reflects the Zigbee LQI (0–255) read from z2m on the gateway, transmitted with **zero extra LoRa
packets** (piggyback only).

**Gateway ship** (`modules/data/z2m_reader.py` + batcher in `data/gateway_data.py`):
- `TYPE_CAPS` stays unchanged — linkquality is NOT part of change-detection, so it never triggers a send.
- Introduce a `RIDE_ALONG_CAPS = ('linkquality',)` concept: when `compute_delta` returns a non-empty
  change set for a device (device is already being shipped), the caller injects the device's *current*
  linkquality (from the live z2m state cache) into the caps dict before `encode_short`.
- Net effect: LQI appears in a `b` entry only when that device is already transmitting for another reason.

**Supervisor mirror** (`modules/protocol/ha_entities.py`):
- Add a `sensor.<dev>_linkquality` per mirrored device:
  - `device_class: signal_strength`, `state_class: measurement`, `unit_of_measurement: "LQI"`,
    `icon: mdi:signal`, `enabled_by_default: true`, `value_template: "{{ value_json.linkquality }}"`.
- `decode_short` already emits `linkquality` into the mirror state JSON — no change needed there.

**Acceptance:**
- After a device change propagates, `sensor.lora_<dev>_linkquality` exists on the supervisor HA, enabled,
  with a 0–255 value, grouped under that device.
- A window with only linkquality changing (no temp/battery/state change) produces **no** new `b` send
  (verify via LoRa TX log — packet count unchanged).

## Feature 2 — Gateway→supervisor link ping (passive indicator)

**Requirement:** the gateway periodically pings the supervisor; the supervisor replies pong; the gateway
shows a "Link Supervisor" online/offline status and a lost-pong counter. Timeout + retry threshold are
tunable per gateway (see Feature 3). No action beyond the indicator.

**New module** `modules/protocol/supervisor_link.py`:
- `SupervisorLinkProbe` (gateway side):
  - Loop every `interval` (from param P4): send `{"t":"sup_ping","g":gw}` over LoRa.
  - Wait `timeout` (param T4) for `{"t":"sup_pong"}`; if none, resend; after `retries` (param PR) failed
    attempts → mark supervisor **offline** and increment lost-pong counter.
  - Any `sup_pong`, OR any inbound LoRa message known to originate from the supervisor, → **online**
    (reset counter of the current probe).
  - Exposes `status()` → {online: bool, lost_pongs: int, last_pong_ts}.
- Supervisor side: `handle_sup_ping(data)` → send `{"t":"sup_pong","g":data.get("g")}`. Register `sup_ping`
  on the supervisor dispatcher. Stateless, reactive (sent immediately, not slotted — like existing pong).

**Gateway HA entities** (`ha_entities.py`, under the `LoRa Gateway Gx` device):
- `binary_sensor` "Link Supervisor" — `device_class: connectivity` (on=online).
- `sensor` "Zgubione pong" — monotonically-increasing counter, `state_class: total_increasing`.

**Bandwidth:** default `interval` (P4) = 15 min; ping + pong = 2 tiny packets / interval. Reactive pong is
immediate (not slotted).

**Acceptance:**
- With supervisor running: gateway "Link Supervisor" = online; killing the supervisor harness flips it to
  offline after `(retries+1)×timeout`; restarting flips it back to online on the next successful ping.
- "Zgubione pong" increments once per declared-offline event.
- `sup_ping`/`sup_pong` never block or delay reactive traffic (verified in TX log ordering).

## Feature 3 — Per-gateway params on the supervisor

**Requirement:** every gateway known to the supervisor has its **own** param set (values + HA `number`
entities + Send buttons) under its own `LoRa Gateway Gx` device. Applies to all existing params
(P1–P3, T1–T3, TH/TL/HH/HL/BL/BC) plus the new ping params (P4, T4, PR).

**Refactor `modules/params/manager.py`:**
- Add ping params to `PARAM_DEFS` in the existing groups (no new group / button):
  - `P4` — "Ping sup interwał" — unit `min`, default 15, min 1, max 1440 — group `CONFIG_KEYS` (button "Wyślij Config").
  - `T4` — "Ping sup timeout" — unit `s`, default 25, min 5, max 300 — group `TIMEOUT_KEYS` (button "Wyślij Timeout").
  - `PR` — "Ping sup próg retry" — unit `×`, default 2, min 0, max 10 — group `THRESHOLD_KEYS` (button "Wyślij Progi").
  - Append to `PARAM_ORDER`.
- De-hardcode the supervisor entity ids: `_eid`/`_btn_uid` supervisor branch → `lora_sup{gw}_param_*`
  (was `supg1` / `lora_sup_param_*`). Device block already uses `gw_id`.
- **Supervisor owns one `ParamSync` per gateway**: create lazily when a gateway first appears
  (HB/pong/devmap), keyed by `gw_id`, with `persist_path = params_<gw>.json`. The gateway remains master.
- Inbound routing: supervisor dispatch of `param_upd` / `params` routes by `data['g']` to that gateway's
  `ParamSync` (creating it if unseen).
- Send buttons on the supervisor already carry the `<gw>` segment (`cmd/<gw>/send_x`) — each per-gw
  instance registers its own buttons so the topic naturally targets that gateway.

**Gateway side:** unchanged model — each gateway masters its own P4/T4/PR and drives its
`SupervisorLinkProbe` from `param_sync.get('P4'|'T4'|'PR')` live (like the anomaly engines read TH/TL).

**Acceptance:**
- With two gateways (G1, G2) known, the supervisor HA shows two independent `LoRa Gateway Gx` devices,
  each with its own 15 number entities (12 + 3 ping) and 3 Send buttons; editing G1's T4 and pressing
  "Wyślij Timeout" changes only G1's gateway, not G2.
- Values persist across supervisor restart (per-gw json).
- Existing single-gateway behavior is preserved (G1 continues to work).

## Testing & deploy

- Develop in `skrypty/`; deploy to both remotes; **run side-by-side with the launcher and verify live**
  (HA WebSocket queries + Playwright), never assume — per project methodology (CARPORT ZASADY,
  LoRa rebuild handoff).
- Update/extend the module smoke tests: `data/smoke_test.py` (LQI ride-along encode/decode),
  a new `protocol/supervisor_link` smoke (ping→pong→offline/online), `params/smoke_test.py`
  (per-gw instances + P4/T4/PR clamp/group routing).
- Bandwidth regression: confirm linkquality adds zero standalone sends and ping cost is 2 packets/interval.

## Open questions

- None blocking. Ping `interval` (P4) default 15 min is provisional; tunable live once deployed.
