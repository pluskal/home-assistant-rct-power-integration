"""Test doubles for the fixture-free unit tests (no running Home Assistant)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from homeassistant.const import CONF_PORT
from rctclient.registry import REGISTRY

from custom_components.rct_power.const import (
    CONF_ENTITY_PREFIX,
    CONF_HOSTNAME,
    DEFAULT_ENTITY_PREFIX,
    DEFAULT_PORT,
)
from custom_components.rct_power.lib.api import (
    ApiResponse,
    ApiResponseValue,
    InvalidApiResponse,
    ValidApiResponse,
)

SERIAL = "0065A4636148"


def oid(object_name: str) -> int:
    return REGISTRY.get_by_name(object_name).object_id


def valid(object_name: str, value: ApiResponseValue) -> ValidApiResponse:
    return ValidApiResponse(
        object_id=oid(object_name), time=datetime.now(), value=value
    )


def invalid(object_name: str, cause: str = "OBJECT_READ_TIMEOUT") -> InvalidApiResponse:
    return InvalidApiResponse(
        object_id=oid(object_name), time=datetime.now(), cause=cause
    )


class StubCoordinator:
    """Just enough of RctPowerDataUpdateCoordinator for the entity layer."""

    def __init__(self, name: str, responses: list[ApiResponse] | None = None) -> None:
        self.name = name
        self.data: dict[int, ApiResponse] = {
            response.object_id: response for response in responses or []
        }
        self.last_update_success = True
        self.listeners: list[Callable[[], None]] = []
        self.async_request_refresh = AsyncMock()

    def get_latest_response(self, object_id: int) -> ApiResponse | None:
        return self.data.get(object_id)

    def async_add_listener(
        self, update_callback: Callable[[], None], context: Any = None
    ) -> Callable[[], None]:
        self.listeners.append(update_callback)
        return lambda: None

    def __repr__(self) -> str:
        return f"StubCoordinator({self.name})"


def make_entry(
    runtime_data: Any = None, options: dict[str, Any] | None = None
) -> MagicMock:
    entry = MagicMock()
    entry.entry_id = "entry-id"
    entry.unique_id = SERIAL
    entry.data = {
        CONF_HOSTNAME: "inverter.local",
        CONF_PORT: DEFAULT_PORT,
        CONF_ENTITY_PREFIX: DEFAULT_ENTITY_PREFIX,
    }
    entry.options = dict(options or {})
    entry.runtime_data = runtime_data
    entry.pref_disable_polling = False
    return entry
