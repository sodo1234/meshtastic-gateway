"""Smoke test konwencji nazw encji HA MQTT discovery, bez brokera i sprzętu.

Uruchomienie:
    cd skrypty
    python -m modules.protocol.naming_smoke_test
"""
import json
import sys

from .ha_entities import HAEntities


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def slugify(value):
    """Odwzorowanie reguły HA: małe litery, separatory jako pojedyncze `_`."""
    chars = [char.lower() if char.isalnum() else "_" for char in value]
    return "_".join(part for part in "".join(chars).split("_") if part)


def expected_entity_id(device_name, entity_name):
    device_slug = slugify(device_name)
    if entity_name is None:
        return device_slug
    return f"{device_slug}_{slugify(entity_name)}"


class FakeMqtt:
    def __init__(self):
        self.pub = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.pub.append((topic, payload, retain))


def _configs(mqtt):
    configs = []
    for topic, payload, retain in mqtt.pub:
        if not topic.endswith("/config"):
            continue
        cfg = json.loads(payload) if isinstance(payload, str) else payload
        configs.append((topic.split("/")[1], cfg, retain))
    return configs


def _entity_ids(mqtt):
    return {
        f"{domain}.{expected_entity_id(cfg['device']['name'], cfg.get('name'))}"
        for domain, cfg, _ in _configs(mqtt)
    }


def _unique_ids(mqtt):
    return [cfg["unique_id"] for _, cfg, _ in _configs(mqtt)]


def test_device_entity_names_and_uids():
    mqtt = FakeMqtt()
    ha = HAEntities(mqtt)
    ha.reg_sensor("G2", "Temp 1", "thb")
    ha.reg_binary("G2", "Door 1", "cb")
    ha.reg_switch_dev("G2", "Test 1")
    ha.reg_sensor("G1", "Temp 1", "t")

    entity_ids = _entity_ids(mqtt)
    expected_g2 = {
        "sensor.lora_g2_temp_1_temperature",
        "sensor.lora_g2_temp_1_humidity",
        "sensor.lora_g2_temp_1_battery",
        "sensor.lora_g2_temp_1_last_seen",
        "binary_sensor.lora_g2_temp_1_available",
        "binary_sensor.lora_g2_door_1_contact",
        "sensor.lora_g2_door_1_battery",
        "binary_sensor.lora_g2_door_1_available",
        "switch.lora_g2_test_1",
        "binary_sensor.lora_g2_test_1_available",
        "sensor.lora_g2_test_1_last_seen",
    }
    assert expected_g2 <= entity_ids, sorted(expected_g2 - entity_ids)
    assert "sensor.lora_g1_temp_1_temperature" in entity_ids
    assert "sensor.lora_g1_temp_1_temperature" != "sensor.lora_g2_temp_1_temperature"

    # Lista sprzed zmiany nazw: unique_id jest tożsamością i musi pozostać nietknięty.
    expected_uids = [
        "lora_g2_temp_1_temp",
        "lora_g2_temp_1_humi",
        "lora_g2_temp_1_batt",
        "lora_g2_temp_1_last_seen",
        "lora_g2_temp_1_available",
        "lora_g2_door_1_contact",
        "lora_g2_door_1_batt",
        "lora_g2_door_1_available",
        "lora_g2_test_1_switch",
        "lora_g2_test_1_available",
        "lora_g2_test_1_last_seen",
        "lora_g1_temp_1_temp",
        "lora_g1_temp_1_last_seen",
        "lora_g1_temp_1_available",
    ]
    assert _unique_ids(mqtt) == expected_uids
    assert all(retain for _, _, retain in _configs(mqtt))
    print("✅ encje urządzeń G1/G2 bez kolizji; unique_id bez zmian")


def test_supervisor_gateway_names_and_uids():
    mqtt = FakeMqtt()
    ha = HAEntities(mqtt, gw_name_fmt="LoRa {gw}")
    ha.reg_gateway("G2")
    ha.reg_gw_controls("G2")

    entity_ids = _entity_ids(mqtt)
    assert "sensor.lora_g2_uptime" in entity_ids
    assert "binary_sensor.lora_g2_status" in entity_ids
    assert "button.lora_g2_ping" in entity_ids
    assert {cfg["device"]["name"] for _, cfg, _ in _configs(mqtt)} == {"LoRa G2"}

    # Te wartości są historycznymi unique_id; zmieniają się tylko nazwy encji i urządzenia.
    expected_uids = [
        "lora_gw_g2_status",
        "lora_gw_g2_uptime",
        "lora_gw_g2_last_seen",
        "lora_gw_g2_devices_total",
        "lora_gw_g2_devices_monitored",
        "lora_gw_g2_devices_priority",
        "lora_gw_g2_devices_offline",
        "lora_gw_g2_ping",
        "lora_gw_g2_discovery",
        "lora_gw_g2_sync",
    ]
    assert _unique_ids(mqtt) == expected_uids
    print("✅ encje bramki supervisora pod LoRa G2; unique_id bez zmian")


def test_gateway_default_device_name_unchanged():
    mqtt = FakeMqtt()
    ha = HAEntities(mqtt)
    ha.reg_gateway("G2")
    configs = _configs(mqtt)
    assert configs
    assert {cfg["device"]["name"] for _, cfg, _ in configs} == {"LoRa Gateway G2"}
    assert _unique_ids(mqtt) == [
        "lora_gw_g2_status",
        "lora_gw_g2_uptime",
        "lora_gw_g2_last_seen",
        "lora_gw_g2_devices_total",
        "lora_gw_g2_devices_monitored",
        "lora_gw_g2_devices_priority",
        "lora_gw_g2_devices_offline",
    ]
    print("✅ domyślna nazwa urządzenia roli gateway bez zmian")


def test_virtual_io_names_per_role():
    mqtt = FakeMqtt()
    HAEntities(mqtt).reg_vswitch("G2", "vs_test", "Test Switch")
    configs = _configs(mqtt)
    assert {
        (cfg["device"]["name"], cfg["name"],
         f"{domain}.{expected_entity_id(cfg['device']['name'], cfg['name'])}")
        for domain, cfg, _ in configs
    } == {("LoRa Virtual I/O G2", "LoRa Test Switch",
           "switch.lora_virtual_i_o_g2_lora_test_switch")}

    mqtt = FakeMqtt()
    HAEntities(mqtt, vio_name_fmt="LoRa {gw} Virtual I/O",
               vio_entity_prefix="").reg_vswitch("G2", "vs_test", "Test Switch")
    configs = _configs(mqtt)
    assert {
        (cfg["device"]["name"], cfg["name"],
         f"{domain}.{expected_entity_id(cfg['device']['name'], cfg['name'])}")
        for domain, cfg, _ in configs
    } == {("LoRa G2 Virtual I/O", "Test Switch",
           "switch.lora_g2_virtual_i_o_test_switch")}
    print("✅ Virtual I/O: gateway bez zmian, supervisor wg nowej konwencji")


if __name__ == "__main__":
    tests = [
        test_device_entity_names_and_uids,
        test_supervisor_gateway_names_and_uids,
        test_gateway_default_device_name_unchanged,
        test_virtual_io_names_per_role,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception as exc:
            print(f"❌ {test.__name__}: {exc}")
            failed += 1
    print(f"\n{'─' * 40}\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(0 if failed == 0 else 1)
