from __future__ import annotations

import time
from datetime import datetime, timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import (
    CALLBACK_TYPE,
    HassJob,
    HassJobType,
    HomeAssistant,
    callback,
)
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import LOGGER
from .lib.api import (
    ApiResponse,
    ApiResponseValue,
    InvalidApiResponse,
    RctPowerApiClient,
    RctPowerData,
    ValidApiResponse,
)
from .lib.const import DOMAIN


class RctPowerDataUpdateCoordinator(DataUpdateCoordinator[RctPowerData]):
    """Class to manage fetching data from the rct power inverter API."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        name_suffix: str,
        client: RctPowerApiClient,
        object_ids: list[int],
        update_interval: timedelta | None = None,
        stale_tolerance: timedelta | None = None,
    ) -> None:
        self.client = client
        self.object_ids = object_ids
        # A tier with a stale tolerance never raises and never freezes: a
        # failed read reuses the previous valid response while that is younger
        # than the tolerance, and a watchdog marks the data STALE when no
        # successful read arrives within the tolerance, whatever blocks the
        # next one (connection timeout, hung socket, a wedged batch ahead).
        # Ages are measured on the monotonic clock from when the read that
        # produced the data started on the bus, i.e. after the wait for the
        # connection lock (a healthy batch of another tier is not staleness)
        # but including the connect.
        self.stale_tolerance = stale_tolerance
        self._valid_since: dict[int, float] = {}
        self._last_valid_read_started: float | None = None
        self._stale_deadline: float | None = None
        self._unsub_stale_watchdog: CALLBACK_TYPE | None = None
        self._logged_missing_read_start = False
        super().__init__(
            hass=hass,
            config_entry=entry,
            logger=LOGGER,
            name=f"{DOMAIN} {entry.unique_id} {name_suffix}",
            update_interval=update_interval,
        )
        # like HA's own refresh timer: a HassJob as the timer's first argument
        # lets HA cancel the watchdog when it stops
        self._stale_watchdog_job = HassJob(
            self._async_mark_stale,
            f"{self.name} stale watchdog",
            job_type=HassJobType.Callback,
            cancel_on_shutdown=True,
        )

    def get_latest_response(
        self, object_id: int
    ) -> ValidApiResponse | InvalidApiResponse | None:
        return self.data.get(object_id)

    def get_valid_value_or(
        self, object_id: int, default_value: ApiResponseValue
    ) -> ApiResponseValue:
        latest_response = self.get_latest_response(object_id)

        if isinstance(latest_response, ValidApiResponse):
            return latest_response.value
        return default_value

    def has_valid_value(self, object_id: int) -> bool:
        return isinstance(self.get_latest_response(object_id), ValidApiResponse)

    @callback
    def async_set_scan_interval(
        self, update_interval: timedelta, stale_tolerance: timedelta | None
    ) -> None:
        """Apply a changed interval in place (no entry reload).

        A shorter interval moves a pending refresh to the new interval from
        now, so the first read at the new rate does not wait for the old
        schedule; otherwise the new interval is used when the next refresh is
        scheduled, i.e. once the running or pending refresh has finished.

        A longer tolerance moves the deadline of a running watchdog out at
        once, to the last valid read + the new tolerance (never earlier than
        before). A shorter tolerance applies to retention at the next completed
        fetch; the watchdog deadline moves to min(old deadline, now + new
        tolerance) at once: never later than before, and the moved refresh
        still has a full new tolerance to land, so a lowered interval never
        blanks the entities on a healthy link.
        """
        previous_interval = self.update_interval
        previous_tolerance = self.stale_tolerance
        self.update_interval = update_interval
        self.stale_tolerance = stale_tolerance

        if stale_tolerance is None:
            self._cancel_stale_watchdog()
        elif (
            previous_tolerance is not None
            and self._stale_deadline is not None
            and self._last_valid_read_started is not None
        ):
            seconds = stale_tolerance.total_seconds()
            if stale_tolerance > previous_tolerance:
                self._arm_stale_watchdog_at(
                    max(self._stale_deadline, self._last_valid_read_started + seconds)
                )
            elif stale_tolerance < previous_tolerance:
                self._arm_stale_watchdog_at(
                    min(self._stale_deadline, time.monotonic() + seconds)
                )

        if (
            previous_interval is not None
            and update_interval < previous_interval
            and self._unsub_refresh is not None
        ):
            self._schedule_refresh()

    async def _async_update_data(self) -> RctPowerData:
        if self.stale_tolerance is None:
            return await self.client.async_get_data(object_ids=self.object_ids)

        fetch_started = time.monotonic()
        read_started: float | None
        try:
            data = await self.client.async_get_data(object_ids=self.object_ids)
        except Exception as exc:  # noqa: BLE001 - tolerant tier, see the constructor
            LOGGER.debug("Error fetching %s data: %r", self.name, exc)
            data = {
                object_id: InvalidApiResponse(
                    object_id=object_id, time=datetime.now(), cause="CONNECTION_ERROR"
                )
                for object_id in self.object_ids
            }
            read_started = fetch_started
        else:
            # the client reports when the read started (after the lock wait); a
            # client that does not is measured from the start of the fetch
            read_started = getattr(data, "read_started", None)
            if read_started is None:
                read_started = fetch_started
                if not self._logged_missing_read_start:
                    self._logged_missing_read_start = True
                    LOGGER.debug(
                        "%s: the client does not report when its reads start; "
                        "data ages count from the start of the fetch, "
                        "including any wait for the connection",
                        self.name,
                    )

        now = time.monotonic()
        tolerance = self.stale_tolerance.total_seconds()
        has_valid = any(
            isinstance(response, ValidApiResponse) for response in data.values()
        )

        if has_valid and now - read_started >= tolerance:
            # the read itself took the whole tolerance (slow connect, per-object
            # timeouts): its values are already too old to be shown, and
            # publishing them would only flash the entities available
            LOGGER.debug(
                "%s data marked stale: the read took %.1f s",
                self.name,
                now - read_started,
            )
            return {
                object_id: self._stale(response)
                if isinstance(response, ValidApiResponse)
                else response
                for object_id, response in data.items()
            }

        result: RctPowerData = {}
        for object_id, response in data.items():
            if isinstance(response, ValidApiResponse):
                self._valid_since[object_id] = read_started
            else:
                response = self._retained_or(object_id, response, now)
            result[object_id] = response

        if has_valid:
            self._last_valid_read_started = read_started
            self._arm_stale_watchdog(read_started)

        return result

    def _retained_or(
        self, object_id: int, response: ApiResponse, now: float
    ) -> ApiResponse:
        """Return the previous valid response while it is young enough."""
        previous = (self.data or {}).get(object_id)
        valid_since = self._valid_since.get(object_id)

        if (
            isinstance(previous, ValidApiResponse)
            and valid_since is not None
            and self.stale_tolerance is not None
            and now - valid_since <= self.stale_tolerance.total_seconds()
        ):
            return previous
        return response

    @staticmethod
    def _stale(response: ValidApiResponse) -> InvalidApiResponse:
        return InvalidApiResponse(
            object_id=response.object_id, time=datetime.now(), cause="STALE"
        )

    @callback
    def _arm_stale_watchdog(self, read_started: float) -> None:
        """Fire `stale_tolerance` after the start of the read of the data."""
        if self.stale_tolerance is None:
            self._cancel_stale_watchdog()
            return

        self._arm_stale_watchdog_at(read_started + self.stale_tolerance.total_seconds())

    @callback
    def _arm_stale_watchdog_at(self, deadline: float) -> None:
        """Fire at `deadline` (a time.monotonic() value), at once if it passed."""
        self._cancel_stale_watchdog()

        if self._shutdown_requested:
            return

        loop = self.hass.loop
        self._stale_deadline = deadline
        self._unsub_stale_watchdog = loop.call_at(
            loop.time() + max(deadline - time.monotonic(), 0.0),
            self._async_mark_stale,
            self._stale_watchdog_job,
        ).cancel

    @callback
    def _cancel_stale_watchdog(self) -> None:
        if self._unsub_stale_watchdog is not None:
            self._unsub_stale_watchdog()
            self._unsub_stale_watchdog = None
        self._stale_deadline = None

    @callback
    def _async_mark_stale(self, _job: HassJob | None = None) -> None:
        self._unsub_stale_watchdog = None
        self._stale_deadline = None

        if self.stale_tolerance is None or not any(
            isinstance(response, ValidApiResponse)
            for response in (self.data or {}).values()
        ):
            return

        age = (
            time.monotonic() - self._last_valid_read_started
            if self._last_valid_read_started is not None
            else self.stale_tolerance.total_seconds()
        )
        LOGGER.debug("%s data marked stale after %.1f s", self.name, age)
        self.data = {
            object_id: self._stale(response)
            if isinstance(response, ValidApiResponse)
            else response
            for object_id, response in self.data.items()
        }
        self.async_update_listeners()

    async def async_shutdown(self) -> None:
        self._cancel_stale_watchdog()
        await super().async_shutdown()
