# LoRa Zigbee SCADA System — v38

## Cel projektu
Rozproszony system BMS (Building Management System) do monitorowania i sterowania infrastrukturą hal produkcyjnych. Setki czujników Zigbee w wielu lokalizacjach → LoRa (Meshtastic) → centralny supervisor → dashboard Home Assistant.

**Kontekst**: Monitoring temp/hum w halach, kontrola oświetlenia, wykrywanie wycieków/pożarów/dostępu. Harmonogram produkcji steruje trybami bramek.

**Filozofia**: Niska krytyczność. Każda bramka autonomiczna (lokalne Z2M+HA). Supervisor agreguje. Minimalizacja ruchu LoRa.

## Architektura
```
SUPERVISOR (G0) — Xeon, Debian, HA Supervised, 3 anteny LoRa (1/bramka)
         LoRa 868MHz | 220B max, 3.0s cooldown
    G1 (100+dev)     G2 (100+dev)     G3 (100+dev)
    Xeon+Z2M+HA      Xeon+Z2M+HA      Xeon+Z2M+HA
```

## Technologia
- Zigbee: Aqara/SONOFF/Tuya + Z2M (ConBee II)
- LoRa: Meshtastic, Heltec V3 (ESP32-S3+SX1262), 868MHz, 5dBi
- Python 3 (threading, paho-mqtt, meshtastic.serial_interface)
- HA Supervised, Debian 12, Mosquitto
- Dashboard: button-card, browser_mod, apexcharts-card, card_mod (dark #0a0a0a)
- Dev: Ryzen 7800X3D, 32GB DDR5, RTX 5070 Ti 16GB

## Ograniczenia LoRa
220B max (150B operational), 3.0s+jitter cooldown, half-duplex, Meshtastic duty cycle EU 10%. Target: 100+dev/bramka, 3 bramki, ~4-6 pkt/min/bramka.

## Nazewnictwo encji HA
```
GATEWAY:      sensor.lora_g1_total, _monitored, _priority, _offline, _low_battery, _anomaly, _last_seen
              binary_sensor.lora_g1_status
              button.lora_g1_ping, _disc, _dump, _clear
DEVICES:      sensor.lora_g1_temp_1_temp, _humi, _batt, _last_seen
              binary_sensor.lora_g1_temp_1_available
              switch.lora_g1_test_1
ANOMALY:      sensor.lora_g1_an_offline, _an_battery, _an_other  (state=count, attr.items=[...])
SUPERVISOR:   button.lora_sup_ping_all, _disc_all, _dump_all, _clear_offline, _clear_battery, _clear_other, _send_config
              number.lora_sup_timeout_switch, _timeout_sensor, _timeout_binary
PARAMS:       number.lora_g1_param_p1, _p2, _p3
VSWITCH:      switch.lora_g1_vs_test, button.lora_g1_vb_test
```
Zasady: `lora_` prefix, gateway=`g1_`, supervisor=`sup_`, device=`{gw}_{dev}_{cap}`, anomaly=`{gw}_an_{cat}`, vswitch=`{gw}_vs_{id}`

## Protokół — Short JSON
```
Types:    S=sensor, W=switch, L=light, B=binary_sensor
Caps:     t=temp, h=hum, b=battery, s=state, r=brightness, c=contact, o=occupancy, w=water_leak, k=smoke
Anomaly:  do=offline, lb=low_battery, cb=critical_battery, th=temp_high, tl=temp_low, hh=hum_high, hl=hum_low, sg=stagnation, sk=smoke, wl=water_leak
Recovery: to=temp_ok, bo=battery_ok, ho=hum_ok, dn=device_online, sc=stagnation_clear
```

### Gateway → Supervisor
| Type | P | Format |
|------|---|--------|
| hb/pong | P0 | `{t:hb,g:G1,up:N,dev:N,mon:N,pri:N,air:F,z2m:N,hash:H,cal:H,m:D}` |
| db | P0 | `{t:db,g:G1,d:[[sid,"Name","S","bth"],...]}` |
| disc_meta | P0 | `{t:disc_meta,g:G1,hash:H,dev_n:N}` |
| disc_vio | P0 | `{t:disc_vio,g:G1,d:[["vs_id","s","Name",1],...]}` |
| vsw_st | P0 | `{t:vsw_st,g:G1,id:ID,v:0/1}` |
| param_upd | P0 | `{t:param_upd,g:G1,p1:V}` |
| b (prio) | P1 | `{t:b,g:G1,ts:T,d:[[sid,{a:1,w:0,b:100}],...]}` |
| ab | P2 | `{t:ab,g:G1,ts:T,d:[[sid,"th",32.6],...]}` |
| b (mon) | P3 | `{t:b,g:G1,ts:T,d:[[sid,{a:1,t:22.5,h:45,b:95}],...]}` |

### Supervisor → Gateway
| Type | Format |
|------|--------|
| cfg | `{t:cfg,g:G1,to:{sw:1800,sn:86400}}` |
| sync | `{t:sync,sec:T,sun:day,seq:N}` |
| ping/disc/dump_anom | `{t:X,g:G1}` |
| cmd | `{t:cmd,g:G1,d:"Test 1",c:"state",v:"ON"}` |
| vsw | `{t:vsw,g:G1,id:"vs_test",v:1}` |
| params | `{t:params,g:G1,p1:20}` |
| ac_b | `{t:ac_b,g:G1,d:[["Dev","temp_high"],...]}` |
| req | `{t:req,g:G1,d:"Temp 2"}` |

## Priority: P0=System, P1=Priority(10s), P2=Anomaly(30s), P3=Monitored(30s)

## System anomalii (kompletny)
- **Offline**: ALL devices, timeout CONFIG, grace 120s
- **Battery**: ALL devices, low<25%, critical<15%
- **Temp/Hum**: monitored only, thresholds CONFIG
- **Stagnation**: ALL devices (switch/light), 72h no change
- Transport: one-shot → buffer → flush 30s → ab P2, chunk split >150B
- Auto-clear: per-type CONFIG, checks SPECIFIC type (not OR)
- Reconciliation: dump_anom co 30min
- Dashboard: per-gateway JSON sensor attributes (NOT auto-entities)
- Ghost prune: remove anomalies for devices gone from discovery

## Kalendarz
HA Calendar → schedules.json → LoRa compressed → gateway → ICS → local HA
mode_names: {0:"BRAK PRODUKCJI", 1:"PRODUKCJA", 2:"PRZERWA", 3:"SERWIS"}
_detect_mode sorted by length DESC. Auto-sync via cal hash in HB. Per-bramka override.

## Anti-collision
- Safe window: supervisor sends AFTER receiving from gateway
- Slotowanie: G1:0-19s, G2:20-39s, G3:40-59s (do implementacji)
- 3 anteny TX dedykowane per bramka

## Startup
- Gateway: MQTT→Z2M retained→states→HB→discovery→flush ALL→mark last_report
- Supervisor: load persistence→mark all offline→wait HB→staggered cfg→disc→dump

## Persistence
/tmp/lora_anomaly_ids.json, /tmp/lora_gw_devices.json, /tmp/schedules.json, /tmp/lora_params.json, gw_state.json

## CONFIG (PRESERVE!)
Gateway: id, mesh_port, monitored[], priority_devices[], batcher, lora, mqtt, ha_api.token, custom_params, anomaly thresholds, timeouts
Supervisor: id, mesh_ports[], gateways[], heartbeat_interval, lora, mqtt, ha_api.token, mode_names

## Root Causes Fixed (v19→v38)
1. sid_map empty→disc on first hash  2. Anomaly dedup→separate ab P2  3. Buffer spam→one-shot+dump_anom  4. Packet loss→150B,jitter,3.0s  5. Calendar PRODUKCJA→mode=None,sort by length  6. Popup missing→sensor-attribute  7. Disc spam→30s cooldown+P0  8. Offline flapping→120s grace  9. Cmd collision→safe window  10. Auto-clear→SPECIFIC type  11. Restart→persist list  12. VSwitch→no seq  13. ICS→unique UIDs  14. Antenna→force-close

## Refactoring Plan — 17 kroków
```
TRANSPORT (zamknięty po weryfikacji)
  1. Transport MQTT + LoRa + Dispatcher
  2. Antena reconnect + port recovery
IDENTYFIKACJA
  3. Encje HA: device + gateway status
  4. Autodiscovery MQTT + VSwitch/VButton
CORE PROTOCOL
  5. Ping/Pong/Heartbeat
  6. Time sync
  7. Slotowanie + anti-collision + safe window
STEROWANIE
  8. ON/OFF + status + retry + timeout → offline
DANE
  9. Batch monitored (30s, delta)
 10. Batch priority (10s)
 11. Refresh last known value
ANOMALIE (kompletny system)
 12. Offline ALL + grace 120s
 13. Battery ALL
 14. Temp/hum monitored
 15. Stagnation ALL 72h
     + ab transport + auto-clear + dump_anom + dashboard lists + ghost prune
HARMONOGRAM + PARAMETRY
 16. Kalendarz push/pull + hash sync
 17. Parametry bidirectional
```

## Zasady refactoringu
- NEVER modify "verified" files (commit "verified:" prefix)
- Each module independently importable, constructor injection
- Transport: send/receive — never changes. Dispatcher: grows per step
- `ast.parse()` after every edit, `python3 -c "from module import Class"` must pass
- JSON: `separators=(',',':')`, preserve CONFIG values, threads daemon=True
