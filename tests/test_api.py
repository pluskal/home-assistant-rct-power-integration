"""The client reports when a read started on the bus."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import custom_components.rct_power.lib.api as api_module
from custom_components.rct_power.lib.api import ApiResponseBatch, RctPowerApiClient

from .stubs import oid, valid

KEY = "g_sync.p_ac_sc[0]"


def test_read_started_is_taken_after_the_lock_wait() -> None:
    clock = SimpleNamespace(now=100.0)
    client = RctPowerApiClient(hostname="inverter.local", port=8899)
    reader = MagicMock()
    reader.at_eof.return_value = False
    writer = MagicMock()
    connected_at: list[float] = []

    async def open_connection(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        connected_at.append(clock.now)
        clock.now += 0.05  # connecting is part of the read
        return reader, writer

    async def read_object(reader: object, writer: object, object_id: int):
        clock.now += 0.06
        return valid(KEY, 1234.0)

    async def scenario() -> ApiResponseBatch:
        await client._connection_lock.acquire()  # another tier's batch
        task = asyncio.create_task(client.async_get_data([oid(KEY)]))
        await asyncio.sleep(0)  # the read is queued on the lock now
        clock.now = 108.04  # the other batch took 8.04 s
        client._connection_lock.release()
        return await task

    with (
        patch.object(api_module, "time", SimpleNamespace(monotonic=lambda: clock.now)),
        patch.object(api_module, "open_connection", open_connection),
        patch.object(client, "_read_object", read_object),
    ):
        batch = asyncio.run(scenario())

    assert isinstance(batch, ApiResponseBatch)
    assert batch.read_started == 108.04
    assert connected_at == [108.04]
    assert batch[oid(KEY)].value == 1234.0
    writer.close.assert_called_once_with()


def test_a_batch_is_a_plain_mapping_for_the_other_tiers() -> None:
    batch = ApiResponseBatch({oid(KEY): valid(KEY, 1.0)}, read_started=5.0)
    assert batch == {oid(KEY): batch[oid(KEY)]}
    assert batch.get(oid(KEY)) is batch[oid(KEY)]
    assert ApiResponseBatch(read_started=1.0) == {}
