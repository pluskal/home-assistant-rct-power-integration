"""Sensor platform for RCT Power."""

from __future__ import annotations

from collections.abc import Callable

from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import Entity

from . import RctConfigEntry, RctData
from .coordinator import RctPowerDataUpdateCoordinator
from .lib.entities import (
    battery_sensor_entity_descriptions,
    bitfield_sensor_entity_descriptions,
    inverter_sensor_entity_descriptions,
)
from .lib.entity import (
    RctPowerBitfieldSensorEntity,
    RctPowerEntityDescription,
    RctPowerSensorEntity,
)


def coordinators_for(
    data: RctData, entity_description: RctPowerEntityDescription
) -> tuple[list[RctPowerDataUpdateCoordinator], list[RctPowerDataUpdateCoordinator]]:
    """Return all coordinators with the entity's own tier first, and its own tier.

    Lookups consult every coordinator (the device info needs the static serial
    numbers) but prefer the entity's own tier, so an object read by two tiers
    resolves to the tier the entity belongs to. Only the own tier triggers state
    writes.
    """
    own = data.update_coordinators[entity_description.update_priority]
    others = [
        coordinator
        for coordinator in data.update_coordinators.values()
        if coordinator is not own
    ]
    return [own, *others], [own]


async def async_setup_entry(
    hass: HomeAssistant,
    entry: RctConfigEntry,
    async_add_entities: Callable[[list[Entity]], None],
) -> None:
    """Setup sensor platform."""
    data = entry.runtime_data

    def create_entity(
        entity_class: type[RctPowerSensorEntity],
        entity_description: RctPowerEntityDescription,
    ) -> RctPowerSensorEntity:
        coordinators, update_coordinators = coordinators_for(data, entity_description)
        return entity_class(
            coordinators=coordinators,
            update_coordinators=update_coordinators,
            config_entry=entry,
            entity_description=entity_description,
        )

    battery_sensor_entities = [
        create_entity(RctPowerSensorEntity, entity_description)
        for entity_description in battery_sensor_entity_descriptions
    ]

    inverter_sensor_entities = [
        create_entity(RctPowerSensorEntity, entity_description)
        for entity_description in inverter_sensor_entity_descriptions
    ]

    bitfield_sensor_entities = [
        create_entity(RctPowerBitfieldSensorEntity, entity_description)
        for entity_description in bitfield_sensor_entity_descriptions
    ]

    async_add_entities(
        [
            *battery_sensor_entities,
            *inverter_sensor_entities,
            *bitfield_sensor_entities,
        ]
    )
