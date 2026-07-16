# ROUTE_PLAN: F2/F3/F5/F7 — LoRa SCADA step5 (local repo only, NO deploy)

Repo root: `C:\Users\sodoj\Documents\Meshtastic Gateway` (branch step-04-calendar).
Code lives in `skrypty/`. Spec (approved): `docs/superpowers/specs/2026-07-15-supervisor-linkquality-pingpong-params.md`.

## HARD RULES
1. Touch ONLY: `skrypty/modules/params/manager.py`, `skrypty/modules/protocol/heartbeat.py`,
   `skrypty/modules/protocol/discovery.py`, NEW `skrypty/modules/protocol/supervisor_link.py`,
   `skrypty/modules/protocol/__init__.py`, `skrypty/test_step5_anomaly.py`, smoke tests in
   `skrypty/modules/params/smoke_test.py` + NEW `skrypty/modules/protocol/link_smoke_test.py`,
   and `skrypty/modules/protocol/ha_entities.py` (only the two additions named below).
2. Do NOT touch `skrypty/modules/transport/` (verified) nor the NESTED duplicate
   `skrypty/modules/protocol/protocol/` (legacy copy — leave as is). Do NOT touch PLAN.md.
3. `skrypty/test_step5_anomaly.py` is 131 KB — edit surgically, never reformat. After EVERY
   file edit run `python -c "import ast; ast.parse(open(FILE,encoding='utf-8').read())"`.
4. NO deploy, NO git commit, NO network calls to remote machines. Local edits + local smoke runs only.
5. No new HA param groups/buttons: new params join existing groups (Config/Timeout/Progi).
6. JSON on the wire: `separators=(',',':')`; LoRa payload budget ~150 B/packet; reactive replies
   (pong) are sent immediately (never slotted/queued).
7. Comments in code: Polish, style matching surrounding code.

## EXISTING FACTS (verified — do not rediscover)
- `ParamSync` (modules/params/manager.py): PARAM_DEFS keys P1,P2,P3,T1,T2,T3,TH,TL,HH,HL,BL,BC;
  groups CONFIG_KEYS=[P1,P2,P3], TIMEOUT_KEYS=[T1,T2,T3], THRESHOLD_KEYS=[TH..BC]; role
  'gateway'|'supervisor'; supervisor uid hardcoded `lora_supg1_param_*` / `lora_sup_param_<btn>`;
  topics `lora/params/<role>/set|state|cmd/...`; `on_cmd` already parses optional
  `cmd/<gw>/<send_x>` segment and `_send_group(keys,label,target_gw)` sends proposal
  `{"t":"params","g":gw,"d":{...}}`; `params_hash()` returns 8-hex over all values.
- `GatewayHeartbeat` (protocol/heartbeat.py): `build_payload(pkt_type)` merges `diag_fn()` dict
  into hb/pong. `SupervisorHeartbeat.handle_hb` stores `hash` and calls `self.ha.reg_gateway(gw)`.
- `GatewayDiscovery.send_discovery_with_delay(delay, meta_only)` (protocol/discovery.py) sends
  disc_meta + disc_vio + devmap; devmap has anti-spam (same hash <180 s skip) via
  `self._devmap_hash/_devmap_ts`; disc_meta/disc_vio have NO anti-spam (observed spam ×3 in 40 s).
- `SupervisorDiscovery`: `gw_devices/gw_hashes/gw_synced` in-memory only (lost on restart →
  causes the startup re-transfer storm); `note_hash` auto-requests disc on mismatch;
  `handle_devmap(gw, payload)` registers entities.
- Harness `skrypty/test_step5_anomaly.py` (single file, role from `.role`/config):
  - gateway wiring: `param_sync` exists; `dispatcher.register('ping', ...)` line ~599;
    MQTT cmd handler `lora/gw/<gl>/cmd/ping` line ~792; `handle_dump_anom(d)` def ~line 439;
    `reg_gw_local_stats(gw)`/`pub_gw_stats` in modules/protocol/ha_entities.py already create
    gateway-local entities incl. `sup_link` (binary) and `sup_last_rx` fed from dict `gw_stats`
    (`{'last_sup_rx_ts','last_sup_rx','last_sync','time_offset','_last_sync_ts'}`, ~line 256,
    `SUP_LINK_TIMEOUT` ~line 258) — REUSE this path for F2 indicator.
  - supervisor wiring: `sup_disc = SupervisorDiscovery(...)` ~1122; `sup_hb = SupervisorHeartbeat(...)`
    ~1734 (`send_ping_fn`, `ping_timeout=wd.get('ping_timeout',25)`); `dispatcher.register('hb'/'pong', on_hb)`
    ~1785; supervisor ParamSync instance exists (role='supervisor') — find via `ParamSync(` search;
    `augment_mirror(gw)` exists (defined after reg_stagnant) and is called from on_db + devmap branch.
  - supervisor sends `sync` time packets somewhere (search `"t": "sync"` / `'t':'sync'`) — reuse
    that send function for F5 sync_req reply.

## TASKS

### T1 — F3: ParamSync per-gateway (manager.py + harness sup wiring)
1. PARAM_DEFS += :
   `"P4": {"default":15,"min":1,"max":1440,"step":1,"unit":"min","name":"Ping sup interwał","icon":"mdi:timer-sync"}`
   `"T4": {"default":25,"min":5,"max":300,"step":1,"unit":"s","name":"Ping sup timeout","icon":"mdi:timer-alert"}`
   `"PR": {"default":2,"min":0,"max":10,"step":1,"unit":"x","name":"Ping sup retry","icon":"mdi:repeat"}`
   PARAM_ORDER += ["P4","T4","PR"]; CONFIG_KEYS+=["P4"]; TIMEOUT_KEYS+=["T4"]; THRESHOLD_KEYS+=["PR"].
2. De-hardcode supervisor ids: `_eid` supervisor branch → `f"lora_sup{self.gw_id.lower()}_param_{key.lower()}"`
   (for gw_id="G1" this equals today's `lora_supg1_param_*` → zero breakage);
   `_btn_uid` supervisor branch → `f"lora_sup{self.gw_id.lower()}_param_{eid}"`.
3. Per-gw topics on supervisor (gateway role topics UNCHANGED): add `self.tp = role` for gateway,
   for supervisor `self.tp = f"supervisor/{gw_id.lower()}"`; replace every
   `f"...{self.sp}/params/{self.role}/..."` with `{self.sp}/params/{self.tp}/...` in
   register_entities/_publish_state(s)/subscribe/on_mqtt_set/on_cmd parsing. In `on_cmd`, the split
   token becomes `f"/params/{self.tp}/cmd/"`.
4. Harness supervisor side: replace the single supervisor ParamSync with a lazy registry:
   `sup_params = {}` + `def get_sup_params(gw): ...` creating
   `ParamSync('supervisor', gw, mqtt, send_fn=..., persist_path=f"/tmp/lora_params_sup_{gw.lower()}.json", ...)`
   then `register_entities()+subscribe()` on first creation. Call `get_sup_params(gw)`
   from `on_hb` (after `sup_hb.handle_hb`) so each gateway that heartbeats gets its instance.
   Route inbound: dispatcher 'param_upd'/'params_req' handlers → `get_sup_params(d.get('g')).handle_remote(d)`.
   MQTT on_mqtt routing for `lora/params/supervisor/<gl>/...` → matching instance.
   Keep gateway-role ParamSync usage untouched.

### T2 — F2: modules/protocol/supervisor_link.py (new) + gateway wiring
New class `SupervisorLinkProbe(gw_id, lora, get_params, on_state, logger=None, tick=1.0)`:
- `get_params()` → `(interval_min, timeout_s, retries)` read live (from param_sync P4/T4/PR).
- Thread loop (daemon, `start()/stop()`): every `interval_min*60` s send
  `{"t":"sup_ping","g":gw_id}` via `lora.send(json.dumps(...))`; await pong `timeout_s`;
  retry up to `retries`; exhausted → offline (increment `lost_pongs`, once per episode).
- `handle_sup_pong(data)` (dispatcher 'sup_pong') → online, clears probe.
- `note_rx()` — ANY inbound from supervisor counts as alive (call from harness where
  `gw_stats['last_sup_rx_ts']` is updated) → resets probe/online.
- `on_state(online: bool, lost_pongs: int)` callback → harness updates
  `gw_stats['sup_link']='ON'/'OFF'` + new key `sup_lost_pong=<int>` then `pub_gw_stats`.
- `status()` → dict for diagnostics.
Supervisor side (harness): `dispatcher.register('sup_ping', ...)` → immediately
`send_to_all({"t":"sup_pong","g":data.get("g")})` (reactive, target gw id included).
Gateway HA: extend the `reg_gw_local_stats` sensors list in modules/protocol/ha_entities.py with
`("sup_lost_pong","Sup Lost Pong","{{ value_json.sup_lost_pong | default(0) }}","mdi:sync-alert",None,"sensor")`
(pattern identical to neighbours). Export new class in `modules/protocol/__init__.py`.

### T3 — F7: quiet start (hash handshake, no eager re-push)
1. discovery.py `send_discovery_with_delay`: add the same 180 s same-hash anti-spam (fields
   `_meta_hash/_meta_ts`) for the disc_meta+disc_vio pair (devmap already guarded); a `disc`
   REQUEST from supervisor (explicit ask) bypasses the guard via new kwarg `force=False` —
   harness passes force=True only in the explicit `disc` request handler and the manual button.
2. SupervisorDiscovery persistence: ctor kwarg `persist_path=None`; `_save()` (json dump of
   `{gw: {devices, synced}}`) called at end of `handle_db`/`handle_devmap`; `_load()` in ctor
   fills `gw_devices` + `gw_synced` (entity re-registration NOT needed — MQTT discovery configs
   are retained). Harness: pass `persist_path='/tmp/lora_sup_devices.json'`.
   Result: on sup restart hashes match → `note_hash` requests NOTHING → no devmap/db storm.
3. Anomaly hash: gateway HB `diag_fn` already carries `hash`(disc)+`cal`+`ph`; add `anh` =
   anomaly-store hash (find gateway-side `_offline_hash()` ~line 409 in harness and the an_d
   snapshot hash in modules/anomaly/reconciler.py — expose `snapshot_hash()` if absent:
   8-hex md5 of sorted active (dev,code) pairs). Supervisor `on_hb`: compare `anh` with its own
   store hash (AnomalyStore — add matching `snapshot_hash(gw)`); mismatch → existing dump_anom
   request path (respect its 90 s guard). Params: in `get_sup_params(gw)` creation DON'T call
   `.request()` blindly — only when hb `ph` differs from instance `params_hash()`.
   (modules/anomaly/reconciler.py and the AnomalyStore file may be edited ONLY to add the
   read-only `snapshot_hash` helpers — nothing else.)

### T4 — F5: per-gw commands from the GATEWAY side
Gateway-local HA buttons (same mechanism as `reg_gw_buttons_local` in ha_entities.py — add there):
`("dump","Dump anomalii","mdi:database-export")` and `("sync_req","Sync czasu","mdi:clock-sync")`.
Harness gateway MQTT cmd handler (~line 792 block) add branches:
- `cmd/dump` → call `handle_dump_anom({'g': gw_id})` (existing function).
- `cmd/sync_req` → lora send `{"t":"sync_req","g":gw_id}`.
Supervisor: `dispatcher.register('sync_req', ...)` → reuse the existing per-gw time-sync send
(search how button `sync` / `lora/supervisor/cmd/<gl>/sync` triggers it; call the same function).
Anomaly clear from gateway already exists (`lora/gw/cmd/clear_anomaly`) — verify and leave.

### T5 — Smoke tests (offline, no MQTT broker: use stub objects like existing smoke tests)
- params/smoke_test.py: extend — (a) P4/T4/PR present+clamped+in groups; (b) two supervisor
  instances (G1,G2) have distinct uids/topics/persist and route `param_upd` by g; (c) gateway
  topics unchanged (`lora/params/gateway/set/P1`).
- protocol/link_smoke_test.py (new): fake lora (records sends) + fake clock — ping emitted,
  pong → online, timeout×(retries+1) → offline + lost_pongs=1, `note_rx` keeps online;
  plus discovery anti-spam: second `send_discovery_with_delay` within 180 s same hash sends
  NOTHING (fake lora), `force=True` bypasses.
Run them: `cd skrypty && python -m modules.params.smoke_test` etc. — all must pass.

### T6 — Wire protocol summary (keep ≤150 B)
`{"t":"sup_ping","g":"G2"}` / `{"t":"sup_pong","g":"G2"}` /
`{"t":"sync_req","g":"G2"}` / hb extra field `"anh":"xxxxxxxx"`.

## DELIVERABLE
Edited files + new files, all ast-clean, smoke tests passing. Write a short BUILD_REPORT.md at
repo root: what changed per file, smoke-test outputs, open questions. No commit.
