"""Entity descriptions of the realtime tier."""

from __future__ import annotations

from homeassistant.components.sensor import SensorStateClass

from custom_components.rct_power.lib.const import EntityUpdatePriority
from custom_components.rct_power.lib.device_info_helpers import (
    get_inverter_device_info,
)
from custom_components.rct_power.lib.entities import all_entity_descriptions
from custom_components.rct_power.lib.entity import resolve_object_infos

from .stubs import oid

PHASE_KEYS = [f"g_sync.p_ac_sc[{phase}]" for phase in range(3)]


def object_ids_of(priority: EntityUpdatePriority) -> set[int]:
    """The id set a tier polls, built as async_setup_entry builds it."""
    return {
        object_info.object_id
        for entity_description in all_entity_descriptions
        if entity_description.update_priority == priority
        for object_info in resolve_object_infos(entity_description)
    }


def realtime_descriptions() -> list:
    return [
        description
        for description in all_entity_descriptions
        if description.update_priority == EntityUpdatePriority.REALTIME
    ]


def test_the_realtime_tier_polls_the_three_grid_phases_only() -> None:
    assert object_ids_of(EntityUpdatePriority.REALTIME) == {
        oid(key) for key in PHASE_KEYS
    }


def test_the_frequent_tier_still_polls_the_grid_phases() -> None:
    assert {oid(key) for key in PHASE_KEYS} <= object_ids_of(
        EntityUpdatePriority.FREQUENT
    )


def test_realtime_descriptions() -> None:
    descriptions = realtime_descriptions()

    assert [description.key for description in descriptions] == PHASE_KEYS
    for phase, description in enumerate(descriptions):
        assert description.unique_id == f"realtime-g_sync.p_ac_sc[{phase}]"
        assert description.name == f"Grid Power P{phase + 1} Realtime"
        assert description.object_names is None
        assert description.get_device_info is get_inverter_device_info
        assert description.state_class == SensorStateClass.MEASUREMENT


def test_the_frequent_grid_phase_descriptions_are_unchanged() -> None:
    for phase, key in enumerate(PHASE_KEYS):
        (description,) = [
            description
            for description in all_entity_descriptions
            if description.key == key
            and description.update_priority == EntityUpdatePriority.FREQUENT
        ]
        # the derived unique id (entry id + object id) keeps the history
        assert description.unique_id is None
        assert description.name == f"Grid Power P{phase + 1}"


def test_unique_ids_do_not_collide() -> None:
    unique_ids = [
        description.unique_id
        or "-".join(
            str(object_info.object_id)
            for object_info in resolve_object_infos(description)
        )
        for description in all_entity_descriptions
    ]
    assert len(unique_ids) == len(set(unique_ids))
