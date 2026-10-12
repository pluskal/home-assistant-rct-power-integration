"""Options: the realtime interval, the >= 1 s validator and in-place changes."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import custom_components.rct_power as rct_power
from custom_components.rct_power import RctData, async_update_entry_options, config_flow
from custom_components.rct_power.config_flow import OPTIONS_SCHEMA
from custom_components.rct_power.const import ConfScanInterval, ScanIntervalDefault
from custom_components.rct_power.coordinator import RctPowerDataUpdateCoordinator
from custom_components.rct_power.lib.const import EntityUpdatePriority

from .stubs import StubCoordinator, make_entry, oid

# HA installs probatio as `voluptuous`; take the exception class the schema uses
Invalid = config_flow.vol.Invalid

ALL_KEYS = [key.value for key in ConfScanInterval]

CURRENT = {
    ConfScanInterval.REALTIME.value: 20,
    ConfScanInterval.FREQUENT.value: 30,
    ConfScanInterval.INFREQUENT.value: 180,
    ConfScanInterval.STATIC.value: 3600,
}


def test_schema_defaults() -> None:
    assert OPTIONS_SCHEMA({}) == {
        "realtime_scan_interval": 20,
        "frequent_scan_interval": 30,
        "infrequent_scan_interval": 180,
        "static_scan_interval": 3600,
    }
    assert ScanIntervalDefault.REALTIME == 20


@pytest.mark.parametrize("key", ALL_KEYS)
@pytest.mark.parametrize("value", [0, -1, "zero"])
def test_schema_rejects_intervals_below_one_second(key: str, value: Any) -> None:
    with pytest.raises(Invalid):
        OPTIONS_SCHEMA({key: value})


@pytest.mark.parametrize("key", ALL_KEYS)
def test_schema_accepts_one_second(key: str) -> None:
    assert OPTIONS_SCHEMA({key: 1})[key] == 1
    assert OPTIONS_SCHEMA({key: "2"})[key] == 2


def make_coordinator_stubs() -> dict[EntityUpdatePriority, StubCoordinator]:
    stubs: dict[EntityUpdatePriority, Any] = {}
    for priority, key in (
        (EntityUpdatePriority.REALTIME, ConfScanInterval.REALTIME),
        (EntityUpdatePriority.FREQUENT, ConfScanInterval.FREQUENT),
        (EntityUpdatePriority.INFREQUENT, ConfScanInterval.INFREQUENT),
        (EntityUpdatePriority.STATIC, ConfScanInterval.STATIC),
    ):
        stub = StubCoordinator(priority.name)
        stub.update_interval = timedelta(seconds=CURRENT[key.value])  # type: ignore[attr-defined]
        stub.async_set_scan_interval = MagicMock()  # type: ignore[attr-defined]
        stubs[priority] = stub
    return stubs


def run_listener(
    options: dict[str, Any],
) -> tuple[MagicMock, dict[EntityUpdatePriority, Any], MagicMock]:
    stubs = make_coordinator_stubs()
    entry = make_entry(runtime_data=RctData(stubs), options=options)  # type: ignore[arg-type]
    hass = MagicMock()
    hass.config_entries.async_reload = AsyncMock()
    asyncio.run(async_update_entry_options(hass, entry))
    return hass, stubs, entry


def test_a_realtime_change_is_applied_in_place() -> None:
    hass, stubs, _ = run_listener({**CURRENT, "realtime_scan_interval": 2})

    hass.config_entries.async_reload.assert_not_awaited()
    stubs[
        EntityUpdatePriority.REALTIME
    ].async_set_scan_interval.assert_called_once_with(timedelta(seconds=2))
    for priority in (
        EntityUpdatePriority.FREQUENT,
        EntityUpdatePriority.INFREQUENT,
        EntityUpdatePriority.STATIC,
    ):
        stubs[priority].async_set_scan_interval.assert_not_called()


@pytest.mark.parametrize(
    "key",
    ["frequent_scan_interval", "infrequent_scan_interval", "static_scan_interval"],
)
def test_any_other_change_reloads_the_entry(key: str) -> None:
    hass, stubs, entry = run_listener({**CURRENT, key: CURRENT[key] + 1})

    hass.config_entries.async_reload.assert_awaited_once_with(entry.entry_id)
    stubs[EntityUpdatePriority.REALTIME].async_set_scan_interval.assert_not_called()


def test_options_saved_before_the_realtime_tier_existed() -> None:
    old_options = {
        key: value for key, value in CURRENT.items() if "realtime" not in key
    }
    hass, stubs, _ = run_listener(old_options)

    hass.config_entries.async_reload.assert_not_awaited()
    stubs[
        EntityUpdatePriority.REALTIME
    ].async_set_scan_interval.assert_called_once_with(timedelta(seconds=20))


class LoopStub:
    """hass.loop with a settable clock; call_at records the scheduled timers."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now
        self.handles: list[MagicMock] = []

    def time(self) -> float:
        return self.now

    def call_at(self, when: float, callback: Any, *args: Any) -> MagicMock:
        handle = MagicMock(name=f"timer@{when}")
        handle.when = when
        self.handles.append(handle)
        return handle


def coordinator_on(loop: LoopStub, seconds: int) -> RctPowerDataUpdateCoordinator:
    hass = MagicMock()
    hass.loop = loop
    return RctPowerDataUpdateCoordinator(
        hass=hass,
        entry=make_entry(),
        name_suffix="realtime",
        client=MagicMock(),
        object_ids=[],
        update_interval=timedelta(seconds=seconds),
    )


def test_coordinator_applies_a_new_interval_in_place() -> None:
    loop = LoopStub()
    coordinator = coordinator_on(loop, 20)

    coordinator.async_set_scan_interval(timedelta(seconds=2))

    assert coordinator.update_interval == timedelta(seconds=2)
    # the value HA's scheduler reads for the next refresh
    assert coordinator._update_interval_seconds == 2.0
    # no refresh was pending (no listeners), so nothing is scheduled
    assert loop.handles == []


def test_a_shorter_interval_moves_the_pending_refresh() -> None:
    loop = LoopStub(1000.3)
    coordinator = coordinator_on(loop, 20)
    coordinator._schedule_refresh()  # what HA does at the end of a refresh
    (pending,) = loop.handles
    assert pending.when == 1000 + coordinator._microsecond + 20

    loop.now = 1015.7  # 20 -> 2 s saved 15 s later
    coordinator.async_set_scan_interval(timedelta(seconds=2))

    pending.cancel.assert_called_once_with()
    assert loop.handles[-1].when == 1015 + coordinator._microsecond + 2
    assert coordinator._unsub_refresh is loop.handles[-1].cancel


def test_a_longer_interval_keeps_the_pending_refresh() -> None:
    loop = LoopStub(1000.3)
    coordinator = coordinator_on(loop, 2)
    coordinator._schedule_refresh()
    (pending,) = loop.handles

    loop.now = 1001.0  # the abort: 2 -> 30 s
    coordinator.async_set_scan_interval(timedelta(seconds=30))

    pending.cancel.assert_not_called()
    assert loop.handles == [pending]
    assert coordinator._update_interval_seconds == 30.0


def set_up_with(options: dict[str, Any]) -> tuple[dict[str, MagicMock], list[str], Any]:
    created: dict[str, MagicMock] = {}
    refreshed: list[str] = []

    def create_coordinator(**kwargs: Any) -> MagicMock:
        suffix = kwargs["name_suffix"]
        coordinator = MagicMock(name=suffix)
        coordinator.kwargs = kwargs

        async def first_refresh() -> None:
            refreshed.append(suffix)

        coordinator.async_config_entry_first_refresh = first_refresh
        created[suffix] = coordinator
        return coordinator

    hass = MagicMock()
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    entry = make_entry(options=options)
    with (
        patch.object(rct_power, "RctPowerApiClient"),
        patch.object(
            rct_power, "RctPowerDataUpdateCoordinator", side_effect=create_coordinator
        ),
    ):
        assert asyncio.run(rct_power.async_setup_entry(hass, entry)) is True
    return created, refreshed, entry


def test_setup_creates_the_realtime_tier() -> None:
    created, refreshed, entry = set_up_with({})

    realtime = created["realtime"].kwargs
    assert realtime["update_interval"] == timedelta(seconds=20)
    assert sorted(realtime["object_ids"]) == sorted(
        oid(f"g_sync.p_ac_sc[{phase}]") for phase in range(3)
    )
    # the frequent tier fails a dead inverter first
    assert refreshed == ["frequent", "realtime", "infrequent", "static"]
    assert entry.runtime_data.update_coordinators == {
        EntityUpdatePriority.REALTIME: created["realtime"],
        EntityUpdatePriority.FREQUENT: created["frequent"],
        EntityUpdatePriority.INFREQUENT: created["infrequent"],
        EntityUpdatePriority.STATIC: created["static"],
    }
    entry.add_update_listener.assert_called_once_with(async_update_entry_options)


def test_setup_uses_the_saved_realtime_interval() -> None:
    created, _, _ = set_up_with({"realtime_scan_interval": 2})

    assert created["realtime"].kwargs["update_interval"] == timedelta(seconds=2)
    assert created["frequent"].kwargs["update_interval"] == timedelta(seconds=30)
