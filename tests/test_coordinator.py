"""Bounded staleness of the realtime tier (retain + watchdog, monotonic clock)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from homeassistant.core import HassJob, HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed

import custom_components.rct_power.coordinator as coordinator_module
from custom_components.rct_power.coordinator import RctPowerDataUpdateCoordinator
from custom_components.rct_power.lib.api import (
    ApiResponse,
    ApiResponseBatch,
    InvalidApiResponse,
    ValidApiResponse,
)

from .stubs import SERIAL, invalid, make_entry, oid, valid

KEYS = [f"g_sync.p_ac_sc[{phase}]" for phase in range(3)]
IDS = [oid(key) for key in KEYS]
INTERVAL = timedelta(seconds=2)
TOLERANCE = timedelta(seconds=10)


class Clock:
    """Stands in for time.monotonic inside the coordinator module."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class FakeLoop:
    """hass.loop on the fake clock; call_at records the timers."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.timers: list[MagicMock] = []

    def time(self) -> float:
        return self.clock.now

    def call_at(self, when: float, callback: Any, *args: Any) -> MagicMock:
        timer = MagicMock(name=f"timer@{when}")
        timer.when = when
        timer.armed_at = self.clock.now
        timer.callback = callback
        timer.args = args
        self.timers.append(timer)
        return timer


class FakeClient:
    """Waits `wait` s for the connection lock, then reads for `duration` s."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.result: dict[int, ApiResponse] | BaseException = {}
        self.wait = 0.0
        self.duration = 0.25
        self.batch = True

    def answer(
        self,
        values: dict[str, float | None],
        duration: float = 0.25,
        wait: float = 0.0,
        batch: bool = True,
    ) -> None:
        """Queue a per-object result; None means a failed read of that object."""
        self.result = {
            oid(key): valid(key, value) if value is not None else invalid(key)
            for key, value in values.items()
        }
        self.duration = duration
        self.wait = wait
        self.batch = batch

    def fail(
        self, exc: BaseException, duration: float = 0.25, wait: float = 0.0
    ) -> None:
        self.result = exc
        self.duration = duration
        self.wait = wait

    async def async_get_data(self, object_ids: list[int]) -> dict[int, ApiResponse]:
        assert object_ids == IDS
        self.clock.now += self.wait
        read_started = self.clock.now
        self.clock.now += self.duration
        if isinstance(self.result, BaseException):
            raise self.result
        if not self.batch:
            return dict(self.result)
        return ApiResponseBatch(self.result, read_started=read_started)


class Harness:
    def __init__(
        self,
        stale_tolerance: timedelta | None = TOLERANCE,
        update_interval: timedelta = INTERVAL,
        schedule_refreshes: bool = False,
    ) -> None:
        self.clock = Clock()
        self.loop = FakeLoop(self.clock)
        self.client = FakeClient(self.clock)
        self.schedule_refreshes = schedule_refreshes
        hass = MagicMock()
        hass.loop = self.loop
        self.coordinator = RctPowerDataUpdateCoordinator(
            hass=hass,
            entry=make_entry(),
            name_suffix="realtime",
            client=self.client,  # type: ignore[arg-type]
            object_ids=IDS,
            update_interval=update_interval,
            stale_tolerance=stale_tolerance,
        )
        self.coordinator.async_update_listeners = MagicMock()  # type: ignore[method-assign]
        self.logger = MagicMock()
        self._patches = [
            patch.object(
                coordinator_module,
                "time",
                SimpleNamespace(monotonic=self.clock.monotonic),
            ),
            patch.object(coordinator_module, "LOGGER", self.logger),
        ]
        for p in self._patches:
            p.start()

    def stop(self) -> None:
        for p in self._patches:
            p.stop()

    def refresh_at(self, start: float) -> dict[int, ApiResponse]:
        """One refresh as DataUpdateCoordinator._async_refresh does it."""
        self.clock.now = start
        self.coordinator.data = asyncio.run(self.coordinator._async_update_data())
        if self.schedule_refreshes:  # HA does this when the tier has listeners
            self.coordinator._schedule_refresh()
        return self.coordinator.data

    @property
    def watchdogs(self) -> list[MagicMock]:
        return [
            timer
            for timer in self.loop.timers
            if timer.callback == self.coordinator._async_mark_stale
        ]

    @property
    def refreshes(self) -> list[MagicMock]:
        return [
            timer
            for timer in self.loop.timers
            if timer.callback != self.coordinator._async_mark_stale
        ]

    @property
    def last_delay(self) -> float:
        timer = self.watchdogs[-1]
        return timer.when - timer.armed_at

    def fire_watchdog(self) -> None:
        timer = self.watchdogs[-1]
        timer.callback(*timer.args)

    def value(self, key: str) -> Any:
        response = self.coordinator.data[oid(key)]
        return (
            response.value if isinstance(response, ValidApiResponse) else response.cause
        )

    def debug_messages(self) -> list[str]:
        return [
            call.args[0] % call.args[1:] for call in self.logger.debug.call_args_list
        ]


def harness(stale_tolerance: timedelta | None = TOLERANCE, **kwargs: Any) -> Harness:
    return Harness(stale_tolerance, **kwargs)


# (a) tiers without a tolerance keep the upstream behaviour


@pytest.mark.parametrize(
    "exc", [OSError("refused"), TimeoutError(), UpdateFailed("eof")]
)
def test_without_tolerance_errors_propagate(exc: Exception) -> None:
    h = harness(stale_tolerance=None)
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        first = h.refresh_at(1000.0)
        h.client.fail(exc)
        with pytest.raises(type(exc)):
            asyncio.run(h.coordinator._async_update_data())
        assert h.coordinator.data is first
        assert h.watchdogs == []
    finally:
        h.stop()


def test_without_tolerance_failed_objects_are_not_retained() -> None:
    h = harness(stale_tolerance=None)
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.answer({KEYS[0]: None, KEYS[1]: 2.0, KEYS[2]: 2.0})
        h.refresh_at(1002.0)
        assert h.value(KEYS[0]) == "OBJECT_READ_TIMEOUT"
        assert h.watchdogs == []
    finally:
        h.stop()


# (b) a failed object keeps its previous valid response within the tolerance


def test_a_failed_object_is_retained_within_the_tolerance() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)  # valid_since = 1000.0 (start of the read)

        retained_at = []
        for start in (1002.0, 1004.0, 1006.0, 1008.0, 1009.75):
            h.client.answer({KEYS[0]: None, KEYS[1]: start, KEYS[2]: start})
            h.refresh_at(start)
            retained_at.append(h.value(KEYS[0]))
            assert h.value(KEYS[1]) == start
        # the last fetch ended at 1010.0: exactly the tolerance, still retained
        assert retained_at == [1.0] * 5

        h.client.answer({KEYS[0]: None, KEYS[1]: 1011.0, KEYS[2]: 1011.0})
        h.refresh_at(1011.0)
        assert h.value(KEYS[0]) == "OBJECT_READ_TIMEOUT"
        assert isinstance(h.coordinator.data[IDS[0]], InvalidApiResponse)
    finally:
        h.stop()


def test_per_object_bound_is_tolerance_plus_one_interval() -> None:
    """Other objects keep re-arming the watchdog; the failed one still expires."""
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        last_retained_end = None
        start = 1000.0
        while True:
            start += INTERVAL.total_seconds()
            h.client.answer({KEYS[0]: None, KEYS[1]: start, KEYS[2]: start})
            h.refresh_at(start)
            # the watchdog follows the fresh objects
            assert h.last_delay == pytest.approx(10.0 - 0.25)
            if h.value(KEYS[0]) != 1.0:
                break
            last_retained_end = h.clock.now
        assert last_retained_end is not None
        assert last_retained_end - 1000.0 <= TOLERANCE.total_seconds()
        assert h.clock.now - 1000.0 <= (TOLERANCE + INTERVAL).total_seconds() + 0.25
    finally:
        h.stop()


def test_a_valid_read_refreshes_the_retention_anchor() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.answer(dict.fromkeys(KEYS, 2.0))
        h.refresh_at(1008.0)  # valid_since = 1008.0
        h.client.answer({KEYS[0]: None, KEYS[1]: 3.0, KEYS[2]: 3.0})
        h.refresh_at(1017.0)  # ends 1017.25: 9.25 s after 1008.0
        assert h.value(KEYS[0]) == 2.0
    finally:
        h.stop()


def test_retention_counts_from_the_start_of_the_read_not_the_lock_wait() -> None:
    h = harness()
    try:
        # queued 6 s behind another tier's batch, then read at 1006.0
        h.client.answer(dict.fromkeys(KEYS, 1.0), wait=6.0)
        h.refresh_at(1000.0)
        h.client.answer({KEYS[0]: None, KEYS[1]: 2.0, KEYS[2]: 2.0})
        h.refresh_at(1015.0)  # ends 1015.25: 9.25 s after the read started
        assert h.value(KEYS[0]) == 1.0
    finally:
        h.stop()


# (c) any exception of the fetch is a failed read of every object, never raised


@pytest.mark.parametrize(
    "exc",
    [
        OSError("Connection refused"),
        TimeoutError(),
        UpdateFailed("Read stream closed"),
        RuntimeError("unexpected"),
    ],
)
def test_any_exception_is_a_connection_error(exc: Exception) -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.fail(exc)
        h.refresh_at(1002.0)
        assert [h.value(key) for key in KEYS] == [1.0, 1.0, 1.0]  # retained

        h.refresh_at(1011.0)
        assert [h.value(key) for key in KEYS] == ["CONNECTION_ERROR"] * 3
        assert any(
            message.startswith(f"Error fetching rct_power {SERIAL} realtime data: ")
            for message in h.debug_messages()
        )
    finally:
        h.stop()


def test_cancellation_is_not_swallowed() -> None:
    h = harness()
    try:
        h.client.fail(asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(h.coordinator._async_update_data())
    finally:
        h.stop()


# (d) nothing to retain before the first valid read


def test_first_fetch_failing_yields_invalid_responses() -> None:
    h = harness()
    try:
        assert h.coordinator.data is None
        h.client.fail(TimeoutError())
        h.refresh_at(1000.0)
        assert [h.value(key) for key in KEYS] == ["CONNECTION_ERROR"] * 3
        assert h.watchdogs == []

        h.client.answer({KEYS[0]: None, KEYS[1]: 5.0, KEYS[2]: 5.0})
        h.refresh_at(1002.0)
        assert h.value(KEYS[0]) == "OBJECT_READ_TIMEOUT"
        assert h.value(KEYS[1]) == 5.0
    finally:
        h.stop()


# (e) the watchdog


def test_watchdog_is_anchored_to_the_start_of_the_read() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        assert len(h.watchdogs) == 1
        assert h.last_delay == pytest.approx(9.75)

        # waiting 6.5 s for the connection lock does not age the values
        h.client.answer(dict.fromkeys(KEYS, 2.0), wait=6.5)
        h.refresh_at(1002.0)
        assert len(h.watchdogs) == 2
        assert h.last_delay == pytest.approx(9.75)
        h.watchdogs[0].cancel.assert_called_once_with()  # the previous one

        # a slow read (connect, per-object timeouts) does
        h.client.answer(dict.fromkeys(KEYS, 3.0), duration=3.0)
        h.refresh_at(1012.0)
        assert h.last_delay == pytest.approx(7.0)
    finally:
        h.stop()


@pytest.mark.parametrize("wait", [8.04, 20.0])
def test_a_read_queued_behind_a_long_lock_holder_stays_valid(wait: float) -> None:
    """8.04 s = the longest healthy frequent batch measured; 20 s = a wedged one."""
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.answer(dict.fromkeys(KEYS, 2.0), wait=wait)
        h.refresh_at(1002.0)

        assert [h.value(key) for key in KEYS] == [2.0, 2.0, 2.0]
        assert h.last_delay == pytest.approx(9.75)
        assert not any("marked stale" in m for m in h.debug_messages())
    finally:
        h.stop()


def test_a_client_without_a_read_start_is_measured_from_the_fetch() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0), wait=6.5, batch=False)
        h.refresh_at(1000.0)
        assert h.last_delay == pytest.approx(3.25)

        h.client.answer(dict.fromkeys(KEYS, 2.0), batch=False)
        h.refresh_at(1002.0)
        h.client.fail(OSError("refused"))  # an error is no missing read start
        h.refresh_at(1004.0)
        notes = [m for m in h.debug_messages() if "does not report" in m]
        assert notes == [
            f"rct_power {SERIAL} realtime: the client does not report when its "
            "reads start; data ages count from the start of the fetch, including "
            "any wait for the connection"
        ]
    finally:
        h.stop()


def test_a_client_with_a_read_start_logs_no_fallback_note() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.fail(OSError("refused"))
        h.refresh_at(1002.0)
        assert not any("does not report" in m for m in h.debug_messages())
    finally:
        h.stop()


@pytest.mark.parametrize("duration", [10.0, 12.0])
def test_a_read_that_took_the_whole_tolerance_is_stale_at_once(duration: float) -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.fire_watchdog()  # fired at 1010 while the slow read below was running

        h.client.answer({KEYS[0]: 2.0, KEYS[1]: 2.0, KEYS[2]: None}, duration=duration)
        h.refresh_at(1002.0)

        assert [h.value(key) for key in KEYS] == [
            "STALE",
            "STALE",
            "OBJECT_READ_TIMEOUT",
        ]
        # no watchdog for values that are never shown, so no flash either
        assert len(h.watchdogs) == 1
        assert (
            f"rct_power {SERIAL} realtime data marked stale: "
            f"the read took {duration:.1f} s" in h.debug_messages()
        )

        # the next quick read is shown again
        h.client.answer(dict.fromkeys(KEYS, 3.0))
        h.refresh_at(1015.0)
        assert [h.value(key) for key in KEYS] == [3.0, 3.0, 3.0]
        assert len(h.watchdogs) == 2
    finally:
        h.stop()


def test_a_read_just_under_the_tolerance_is_shown() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0), duration=9.9)
        h.refresh_at(1000.0)
        assert [h.value(key) for key in KEYS] == [1.0, 1.0, 1.0]
        assert h.last_delay == pytest.approx(0.1)
    finally:
        h.stop()


def test_a_retained_only_fetch_does_not_rearm_the_watchdog() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.client.fail(OSError("refused"))
        h.refresh_at(1002.0)
        h.client.answer(dict.fromkeys(KEYS, None))
        h.refresh_at(1004.0)

        assert len(h.watchdogs) == 1
        h.watchdogs[0].cancel.assert_not_called()
        assert [h.value(key) for key in KEYS] == [1.0, 1.0, 1.0]
    finally:
        h.stop()


def test_watchdog_marks_valid_responses_stale() -> None:
    h = harness()
    try:
        h.client.answer({KEYS[0]: 1.0, KEYS[1]: 1.0, KEYS[2]: None})
        h.refresh_at(1000.0)
        h.client.fail(TimeoutError(), duration=20.0)  # connection timeout

        h.clock.now = 1010.0
        h.fire_watchdog()

        assert [h.value(key) for key in KEYS] == [
            "STALE",
            "STALE",
            "OBJECT_READ_TIMEOUT",
        ]
        h.coordinator.async_update_listeners.assert_called_once_with()
        assert (
            f"rct_power {SERIAL} realtime data marked stale after 10.0 s"
            in h.debug_messages()
        )

        # the fetch that was blocked returns a failure: nothing to retain
        h.refresh_at(1002.0)
        assert [h.value(key) for key in KEYS] == ["CONNECTION_ERROR"] * 3
        assert len(h.watchdogs) == 1

        # the next successful read makes the data valid again and re-arms
        h.client.answer(dict.fromkeys(KEYS, 7.0))
        h.refresh_at(1030.0)
        assert [h.value(key) for key in KEYS] == [7.0, 7.0, 7.0]
        assert len(h.watchdogs) == 2
    finally:
        h.stop()


def test_watchdog_without_valid_data_does_nothing() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.coordinator.data = {
            object_id: InvalidApiResponse(
                object_id=object_id, time=datetime.now(), cause="X"
            )
            for object_id in IDS
        }
        h.fire_watchdog()
        h.coordinator.async_update_listeners.assert_not_called()
        assert not any("marked stale" in m for m in h.debug_messages())
    finally:
        h.stop()


# (f) in-place interval change


def test_a_longer_tolerance_rearms_a_running_watchdog() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.clock.now = 1001.0

        h.coordinator.async_set_scan_interval(
            timedelta(seconds=5), timedelta(seconds=25)
        )

        assert h.coordinator.update_interval == timedelta(seconds=5)
        assert h.coordinator.stale_tolerance == timedelta(seconds=25)
        h.watchdogs[0].cancel.assert_called_once_with()
        assert h.watchdogs[-1].when == pytest.approx(1025.0)
    finally:
        h.stop()


@pytest.mark.parametrize(
    ("old", "offset"),
    [(20, 15.0), (30, 25.0)],
)
def test_lowering_the_interval_in_place_leaves_no_unavailable_window(
    old: int, offset: float
) -> None:
    """Old interval -> 2 s saved `offset` s after the last read (review B)."""
    h = harness(
        stale_tolerance=timedelta(seconds=5 * old),
        update_interval=timedelta(seconds=old),
        schedule_refreshes=True,
    )
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)  # read 1000.0-1000.25, next refresh at ~1020/1030
        (old_watchdog,) = h.watchdogs
        (old_refresh,) = h.refreshes

        changed = h.clock.now = 1000.25 + offset
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=2), timedelta(seconds=10)
        )

        # the deadline moves to min(old deadline, now + new tolerance) ...
        old_watchdog.cancel.assert_called_once_with()
        shrunk = h.watchdogs[-1]
        assert shrunk.when == pytest.approx(changed + 10.0)
        assert shrunk.when < old_watchdog.when
        # ... and the pending refresh comes within the new interval, well before
        old_refresh.cancel.assert_called_once_with()
        moved = h.refreshes[-1]
        assert moved.when <= changed + 2.5
        assert shrunk.when > moved.when + 0.25
        assert [h.value(key) for key in KEYS] == [1.0, 1.0, 1.0]

        # the moved refresh reads; from then on the new tolerance applies
        h.client.answer(dict.fromkeys(KEYS, 2.0))
        h.refresh_at(moved.when)
        assert [h.value(key) for key in KEYS] == [2.0, 2.0, 2.0]
        shrunk.cancel.assert_called_once_with()
        assert h.last_delay == pytest.approx(9.75)
        assert not any("marked stale" in m for m in h.debug_messages())
    finally:
        h.stop()


def test_lowering_the_interval_with_a_blocked_fetch_hides_after_the_new_tolerance() -> (
    None
):
    """20 -> 2 s saved 15 s after a read while the next fetch hangs: hidden one
    new tolerance after the change, not one old tolerance after the read."""
    h = harness(
        stale_tolerance=timedelta(seconds=100), update_interval=timedelta(seconds=20)
    )
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.clock.now = 1015.25
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=2), timedelta(seconds=12)
        )
        assert h.watchdogs[-1].when == pytest.approx(1027.25)

        h.clock.now = 1027.25  # the moved fetch is still hanging
        h.fire_watchdog()
        assert [h.value(key) for key in KEYS] == ["STALE"] * 3
        assert (
            f"rct_power {SERIAL} realtime data marked stale after 27.2 s"
            in h.debug_messages()
        )
    finally:
        h.stop()


def test_successive_shrinks_start_from_the_armed_deadline() -> None:
    """30 -> 5 -> 2 s within two seconds: the second shrink must not fall back to
    'last read + previous tolerance', which has already passed."""
    h = harness(
        stale_tolerance=timedelta(seconds=150), update_interval=timedelta(seconds=30)
    )
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.clock.now = 1028.0
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=5), timedelta(seconds=25)
        )
        assert h.watchdogs[-1].when == pytest.approx(1053.0)

        h.clock.now = 1030.0
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=2), timedelta(seconds=12)
        )
        assert h.watchdogs[-1].when == pytest.approx(1042.0)
        assert h.last_delay == pytest.approx(12.0)
    finally:
        h.stop()


def test_a_longer_tolerance_never_moves_the_deadline_earlier() -> None:
    h = harness(
        stale_tolerance=timedelta(seconds=100), update_interval=timedelta(seconds=20)
    )
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.clock.now = 1015.0
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=2), timedelta(seconds=10)
        )
        assert h.watchdogs[-1].when == pytest.approx(1025.0)

        # 2 -> 3 s: last read + 15 s = 1015 would already have passed
        h.clock.now = 1016.0
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=3), timedelta(seconds=15)
        )
        assert h.watchdogs[-1].when == pytest.approx(1025.0)
    finally:
        h.stop()


def test_a_shorter_tolerance_applies_to_retention_at_once() -> None:
    h = harness(
        stale_tolerance=timedelta(seconds=100), update_interval=timedelta(seconds=20)
    )
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.clock.now = 1015.0
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=2), timedelta(seconds=10)
        )

        h.client.answer({KEYS[0]: None, KEYS[1]: 2.0, KEYS[2]: 2.0})
        h.refresh_at(1017.0)
        assert h.value(KEYS[0]) == "OBJECT_READ_TIMEOUT"  # 17 s old > 10 s
        assert h.value(KEYS[1]) == 2.0
    finally:
        h.stop()


def test_set_scan_interval_without_a_running_watchdog_does_not_arm() -> None:
    h = harness()
    try:
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=5), timedelta(seconds=25)
        )
        assert h.watchdogs == []

        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        h.fire_watchdog()  # data is STALE, the watchdog is gone
        h.coordinator.async_set_scan_interval(
            timedelta(seconds=10), timedelta(seconds=50)
        )
        assert len(h.watchdogs) == 1
    finally:
        h.stop()


def test_set_scan_interval_without_tolerance_cancels_the_watchdog() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)

        h.coordinator.async_set_scan_interval(timedelta(seconds=30), None)

        h.watchdogs[0].cancel.assert_called_once_with()
        assert len(h.watchdogs) == 1
        assert h.coordinator.stale_tolerance is None
    finally:
        h.stop()


# (g) shutdown


def test_shutdown_cancels_the_watchdog() -> None:
    h = harness()
    try:
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)

        asyncio.run(h.coordinator.async_shutdown())

        h.watchdogs[0].cancel.assert_called_once_with()
    finally:
        h.stop()


def test_a_read_finishing_after_shutdown_arms_nothing() -> None:
    """A manual refresh runs outside the entry's tasks and can outlive the unload."""
    h = harness()
    try:
        asyncio.run(h.coordinator.async_shutdown())
        h.client.answer(dict.fromkeys(KEYS, 1.0))
        h.refresh_at(1000.0)
        assert h.watchdogs == []
    finally:
        h.stop()


def test_the_watchdog_is_cancelled_when_home_assistant_stops() -> None:
    loop = asyncio.new_event_loop()
    try:
        clock = Clock()
        client = FakeClient(clock)
        client.answer(dict.fromkeys(KEYS, 1.0))
        hass = MagicMock()
        hass.loop = loop
        coordinator = RctPowerDataUpdateCoordinator(
            hass=hass,
            entry=make_entry(),
            name_suffix="realtime",
            client=client,  # type: ignore[arg-type]
            object_ids=IDS,
            update_interval=INTERVAL,
            stale_tolerance=TOLERANCE,
        )
        with patch.object(
            coordinator_module, "time", SimpleNamespace(monotonic=clock.monotonic)
        ):
            coordinator.data = loop.run_until_complete(coordinator._async_update_data())
        assert coordinator._unsub_stale_watchdog is not None
        timer = coordinator._unsub_stale_watchdog.__self__  # type: ignore[attr-defined]
        assert isinstance(timer, asyncio.TimerHandle)
        assert isinstance(timer._args[0], HassJob)

        # stage 2 of HomeAssistant.async_stop
        HomeAssistant._cancel_cancellable_timers(SimpleNamespace(loop=loop))  # type: ignore[arg-type]

        assert timer.cancelled()
    finally:
        loop.close()
