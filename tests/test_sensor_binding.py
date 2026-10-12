"""Entity <-> coordinator binding: lookups see all tiers, listeners only the own tier."""

from __future__ import annotations

import asyncio

from homeassistant.helpers.entity import Entity

from custom_components.rct_power import RctData
from custom_components.rct_power.lib.const import DOMAIN, EntityUpdatePriority
from custom_components.rct_power.lib.device_info_helpers import (
    get_battery_device_info,
    get_inverter_device_info,
)
from custom_components.rct_power.lib.entity import RctPowerSensorEntity
from custom_components.rct_power.sensor import async_setup_entry, coordinators_for

from .stubs import StubCoordinator, invalid, make_entry, oid, valid

INVERTER_SN = "INV-SN-1"
BMS_SN = "BMS-SN-7"


def make_stubs() -> dict[EntityUpdatePriority, StubCoordinator]:
    stubs = {
        priority: StubCoordinator(priority.name) for priority in EntityUpdatePriority
    }
    stubs[EntityUpdatePriority.STATIC].data.update(
        {
            response.object_id: response
            for response in (
                valid("inverter_sn", INVERTER_SN),
                valid("android_description", "Garage inverter"),
                valid("svnversion", "1234"),
                valid("battery.bms_sn", BMS_SN),
                valid("battery.bms_software_version", 77),
            )
        }
    )
    return stubs


def set_up_entities(
    stubs: dict[EntityUpdatePriority, StubCoordinator],
) -> list[RctPowerSensorEntity]:
    entry = make_entry(runtime_data=RctData(stubs))  # type: ignore[arg-type]
    added: list[Entity] = []
    asyncio.run(async_setup_entry(None, entry, added.extend))  # type: ignore[arg-type]
    assert added
    return added  # type: ignore[return-value]


def entity_by_key(
    entities: list[RctPowerSensorEntity], key: str, priority: EntityUpdatePriority
) -> RctPowerSensorEntity:
    (entity,) = [
        entity
        for entity in entities
        if entity.entity_description.key == key
        and entity.entity_description.update_priority == priority
    ]
    return entity


def test_coordinators_for_puts_the_own_tier_first() -> None:
    stubs = make_stubs()
    data = RctData(stubs)  # type: ignore[arg-type]
    entities = set_up_entities(stubs)

    for entity in entities:
        own = stubs[entity.entity_description.update_priority]
        coordinators, update_coordinators = coordinators_for(
            data, entity.entity_description
        )
        assert coordinators[0] is own
        assert len(coordinators) == len(stubs)
        assert {id(c) for c in coordinators} == {id(c) for c in stubs.values()}
        assert update_coordinators == [own]
        assert entity.coordinators == coordinators
        assert entity.update_coordinators == [own]


def test_listener_is_registered_on_the_own_tier_only() -> None:
    stubs = make_stubs()
    entities = set_up_entities(stubs)

    for entity in entities:
        asyncio.run(entity.async_added_to_hass())

    assert sum(len(stub.listeners) for stub in stubs.values()) == len(entities)
    for entity in entities:
        own = stubs[entity.entity_description.update_priority]
        registered_on = [
            stub
            for stub in stubs.values()
            for listener in stub.listeners
            if getattr(listener, "__self__", None) is entity
        ]
        assert registered_on == [own], entity.entity_description.key


def test_manual_update_refreshes_the_own_tier_only() -> None:
    stubs = make_stubs()
    entity = entity_by_key(
        set_up_entities(stubs), "g_sync.p_ac_sc[0]", EntityUpdatePriority.FREQUENT
    )

    asyncio.run(entity.async_update())

    for priority, stub in stubs.items():
        expected = 1 if priority is EntityUpdatePriority.FREQUENT else 0
        assert stub.async_request_refresh.await_count == expected, priority


def test_device_info_resolves_the_static_serials_for_every_tier() -> None:
    stubs = make_stubs()
    entities = set_up_entities(stubs)
    checked: set[EntityUpdatePriority] = set()

    for entity in entities:
        get_device_info = entity.entity_description.get_device_info
        if get_device_info is get_inverter_device_info:
            serial = INVERTER_SN
        elif get_device_info is get_battery_device_info:
            serial = BMS_SN
        else:
            continue
        device_info = entity.device_info
        assert device_info is not None
        identifiers = device_info["identifiers"]
        assert (DOMAIN, serial) in identifiers, entity.entity_description.key
        assert not any("None" in identifier for identifier in identifiers)
        if get_device_info is get_battery_device_info:
            assert device_info["via_device"] == (DOMAIN, INVERTER_SN)
        checked.add(entity.entity_description.update_priority)

    assert EntityUpdatePriority.FREQUENT in checked
    assert EntityUpdatePriority.INFREQUENT in checked


def test_an_entity_bound_to_its_own_tier_alone_would_lose_the_serial() -> None:
    """Negative control: why lookups must keep every coordinator (B1)."""
    stubs = make_stubs()
    entity = entity_by_key(
        set_up_entities(stubs), "g_sync.p_ac_sc[0]", EntityUpdatePriority.FREQUENT
    )
    own_only = RctPowerSensorEntity(
        coordinators=[stubs[EntityUpdatePriority.FREQUENT]],  # type: ignore[list-item]
        config_entry=entity.config_entry,
        entity_description=entity.entity_description,
    )

    device_info = own_only.device_info
    assert device_info is not None
    assert (DOMAIN, "None") in device_info["identifiers"]


def test_an_id_in_two_tiers_resolves_to_the_own_tier() -> None:
    stubs = make_stubs()
    frequent = stubs[EntityUpdatePriority.FREQUENT]
    infrequent = stubs[EntityUpdatePriority.INFREQUENT]
    frequent_key = "g_sync.p_ac_sc[0]"
    infrequent_key = "battery.soh"
    frequent.data[oid(frequent_key)] = valid(frequent_key, 100.0)
    infrequent.data[oid(frequent_key)] = valid(frequent_key, 999.0)
    infrequent.data[oid(infrequent_key)] = valid(infrequent_key, 0.5)
    frequent.data[oid(infrequent_key)] = valid(infrequent_key, 0.9)
    entities = set_up_entities(stubs)

    frequent_entity = entity_by_key(
        entities, frequent_key, EntityUpdatePriority.FREQUENT
    )
    infrequent_entity = entity_by_key(
        entities, infrequent_key, EntityUpdatePriority.INFREQUENT
    )

    assert (
        frequent_entity.get_valid_api_response_value_by_id(oid(frequent_key)) == 100.0
    )
    assert (
        infrequent_entity.get_valid_api_response_value_by_id(oid(infrequent_key)) == 0.5
    )


def test_an_invalid_own_response_is_not_masked_by_another_tier() -> None:
    stubs = make_stubs()
    key = "g_sync.p_ac_sc[0]"
    stubs[EntityUpdatePriority.FREQUENT].data[oid(key)] = invalid(key)
    stubs[EntityUpdatePriority.INFREQUENT].data[oid(key)] = valid(key, 999.0)
    entity = entity_by_key(set_up_entities(stubs), key, EntityUpdatePriority.FREQUENT)

    assert entity.available is False
    assert entity.get_valid_api_response_value_by_id(oid(key)) is None


def test_grid_phases_resolve_per_tier() -> None:
    """The phases are read by two tiers; each entity shows its own tier."""
    stubs = make_stubs()
    realtime = stubs[EntityUpdatePriority.REALTIME]
    frequent = stubs[EntityUpdatePriority.FREQUENT]
    for phase in range(3):
        key = f"g_sync.p_ac_sc[{phase}]"
        realtime.data[oid(key)] = valid(key, 1000.0 + phase)
        frequent.data[oid(key)] = valid(key, 2000.0 + phase)
    entities = set_up_entities(stubs)

    for phase in range(3):
        key = f"g_sync.p_ac_sc[{phase}]"
        realtime_entity = entity_by_key(entities, key, EntityUpdatePriority.REALTIME)
        frequent_entity = entity_by_key(entities, key, EntityUpdatePriority.FREQUENT)
        assert realtime_entity.native_value == 1000.0 + phase
        assert frequent_entity.native_value == 2000.0 + phase
        assert (
            realtime_entity.name
            == f"RCT Power Storage Grid Power P{phase + 1} Realtime"
        )
        assert realtime_entity.unique_id == f"entry-id-realtime-g_sync.p_ac_sc[{phase}]"
        assert realtime_entity.device_info == frequent_entity.device_info


def test_an_invalid_realtime_phase_is_not_masked_by_the_frequent_tier() -> None:
    stubs = make_stubs()
    key = "g_sync.p_ac_sc[1]"
    stubs[EntityUpdatePriority.REALTIME].data[oid(key)] = invalid(key, "STALE")
    stubs[EntityUpdatePriority.FREQUENT].data[oid(key)] = valid(key, 2001.0)
    entities = set_up_entities(stubs)

    assert (
        entity_by_key(entities, key, EntityUpdatePriority.REALTIME).available is False
    )
    assert entity_by_key(entities, key, EntityUpdatePriority.FREQUENT).available is True
