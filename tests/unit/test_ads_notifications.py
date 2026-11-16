"""Unit tests for aioads.ads_notifications."""

import asyncio
import struct
import unittest
from collections.abc import Callable
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.ads_notifications import (
    NotificationManager,
    NotificationValue,
    OverflowPolicy,
)
from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import AdsSymbolParser
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.commands.ads_add_notification import (
    AdsAddNotificationResponse,
    TransmissionMode,
)
from aioads.commands.ads_command import AdsCommandId, AdsCommandState
from aioads.commands.ads_write import AdsWriteResponse
from aioads.deserialize_dispatcher import DeserializeDispatcher
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
)
from aioads.stream import AdsStream
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_stream,
    make_transport,
)


def make_symbol_info() -> SymbolInfo:
    """A 4-byte INT32 symbol, parseable without a populated datatype table."""
    return SymbolInfo(
        idx_group=0x4020,
        idx_offset=0,
        idx_length=4,
        data_type=AdsSymbolDataType.INT32,
        symbol_flags=AdsSymbolFlags(0),
        symbol_name="MAIN.VALUE",
        type_name="DINT",
        comment="",
    )


def build_notification(handle: int, timestamp: int, value_bytes: bytes) -> bytes:
    """Build a device-notification payload with a single stamp and sample."""
    sample = (
        handle.to_bytes(4, "little")
        + len(value_bytes).to_bytes(4, "little")
        + value_bytes
    )
    stamp = timestamp.to_bytes(8, "little") + (1).to_bytes(4, "little") + sample
    return (0).to_bytes(4, "little") + (1).to_bytes(4, "little") + stamp


def make_notification_header(source: AmsAddress) -> AmsHeader:
    """AMS header of a device notification sent to us by ``source``."""
    return AmsHeader(
        target_ams_address=make_ams_address(net_id="9.9.9.9.1.1", port=30000),
        source_ams_address=source,
        command_id=AdsCommandId.DEVICE_NOTIFICATION,
        command_flags=AdsCommandState.ADS_COMMAND | AdsCommandState.ADS_REQUEST,
        command_length=0,
        error_code=AdsErrorCode(0),
        invoke_id=0,
    )


def add_response(handle: int) -> tuple[AmsHeader, AdsStream]:
    """Transport return value for an AddNotification request."""
    payload = AdsAddNotificationResponse(
        AdsErrorCode(0), notification_handle=handle
    ).serialize()
    return (make_ams_header(), make_stream(payload))


def delete_response(error_code: int = 0) -> tuple[AmsHeader, AdsStream]:
    """Transport return value for a DeleteNotification request."""
    payload = AdsWriteResponse(AdsErrorCode(error_code)).serialize()
    return (make_ams_header(), make_stream(payload))


class FakeRemote:
    """
    Scripted stand-in for the remote side of ``transport.request``: hands out
    notification handles and records which handles were deleted.
    """

    def __init__(self, first_handle: int = 42) -> None:
        self.next_handle = first_handle
        self.added: list[int] = []
        self.deleted: list[int] = []
        self.delete_error: Exception | None = None
        self.delete_error_code = 0
        self.add_gate: asyncio.Event | None = None

    async def request(
        self, command_payload: bytes, command_id: AdsCommandId, ams_address: AmsAddress
    ) -> tuple[AmsHeader, AdsStream]:
        """Answer add / delete notification requests."""
        if command_id == AdsCommandId.ADD_DEVICE_NOTIFICATION:
            if self.add_gate is not None:
                await self.add_gate.wait()
            handle = self.next_handle
            self.next_handle += 1
            self.added.append(handle)
            return add_response(handle)
        if command_id == AdsCommandId.DELETE_DEVICE_NOTIFICATION:
            self.deleted.append(int.from_bytes(command_payload, "little"))
            if self.delete_error is not None:
                raise self.delete_error
            return delete_response(self.delete_error_code)
        raise AssertionError(f"Unexpected command {command_id!r}")


async def wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Let the event loop run until ``predicate`` holds."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


async def settle() -> None:
    """Give background tasks (pump, consumers, removals) time to run."""
    for _ in range(20):
        await asyncio.sleep(0)


class NotificationTestCase(unittest.IsolatedAsyncioTestCase):
    """Shared arrangement: a manager on a real transport with a scripted remote."""

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.remote = FakeRemote()
        self.transport.request = AsyncMock(side_effect=self.remote.request)
        # Real typed collaborators; only the methods under control are replaced.
        self.cache = AdsSymbolCache(
            self.transport,
            DeserializeDispatcher(offload_size=1024 * 1024),
            self.address,
            batch_size=500,
        )
        self.cache.read_symbol_info_by_name = AsyncMock(
            return_value=make_symbol_info()
        )
        self.manager = NotificationManager(
            transport=self.transport,
            dst_address=self.address,
            symbol_cache=self.cache,
            parser=AdsSymbolParser([]),
        )
        self.registry = self.manager.registry
        self.callback = AsyncMock()

    async def asyncTearDown(self) -> None:
        await self.registry.shutdown()

    def subscribe(
        self,
        callback=None,
        queue_size: int = 1000,
        overflow: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ):
        """Subscribe MAIN.VALUE with default settings."""
        return self.manager.create_notification(
            "MAIN.VALUE",
            callback or self.callback,
            TransmissionMode.CYCLIC,
            50,
            100,
            queue_size,
            overflow,
        )

    def feed(self, handle: int, value: int, timestamp: int = 1234) -> None:
        """Simulate the transport receiving a notification from the remote."""
        payload = build_notification(handle, timestamp, struct.pack("<i", value))
        self.registry.feed(
            make_notification_header(self.address), make_stream(payload)
        )


class TestNotificationManager(NotificationTestCase):

    async def test_create_notification_yields_handle_from_add_response(self) -> None:
        # Act
        async with self.subscribe() as handle:
            yielded = handle

        # Assert
        self.assertEqual(yielded, 42)

    async def test_create_notification_removes_handle_on_exit(self) -> None:
        # Act
        async with self.subscribe():
            pass

        # Assert
        self.assertEqual(self.remote.deleted, [42])

    async def test_create_notification_removes_handle_when_body_raises(self) -> None:
        # Act
        with self.assertRaises(RuntimeError):
            async with self.subscribe():
                raise RuntimeError("boom")

        # Assert
        self.assertEqual(self.remote.deleted, [42])

    async def test_create_notification_failed_delete_does_not_raise(self) -> None:
        # Arrange
        self.remote.delete_error = ConnectionError("Connection lost")

        # Act
        async with self.subscribe():
            pass

        # Assert — the removal was attempted, the failure stays internal
        self.assertEqual(self.remote.deleted, [42])

    async def test_close_removes_all_notifications_of_the_manager(self) -> None:
        # Arrange
        stack = [self.subscribe(), self.subscribe()]
        for context in stack:
            await context.__aenter__()

        # Act
        await self.manager.close()

        # Assert
        self.assertCountEqual(self.remote.deleted, [42, 43])


class TestNotificationDelivery(NotificationTestCase):

    async def test_feed_dispatches_parsed_value_to_callback(self) -> None:
        # Act
        async with self.subscribe():
            self.feed(handle=42, value=7)
            await settle()

        # Assert
        self.callback.assert_awaited_once_with(
            [NotificationValue(handle=42, timestamp=1234, value=7)]
        )

    async def test_feed_multiple_frames_are_delivered_as_one_batch(self) -> None:
        # Act
        async with self.subscribe():
            for value in (1, 2, 3):
                self.feed(handle=42, value=value)
            await settle()

        # Assert
        self.callback.assert_awaited_once()
        delivered = self.callback.await_args.args[0]
        self.assertEqual([item.value for item in delivered], [1, 2, 3])

    async def test_feed_large_batch_is_decoded_in_thread_and_delivered(self) -> None:
        # Arrange — force the thread offload path for every batch
        self.registry.OFFLOAD_SIZE = 0

        # Act
        async with self.subscribe():
            self.feed(handle=42, value=7)
            await wait_until(lambda: self.callback.await_count == 1)

        # Assert
        self.assertEqual(self.callback.await_args.args[0][0].value, 7)

    async def test_feed_after_failing_callback_still_delivers_next_value(self) -> None:
        # Arrange
        self.callback.side_effect = [RuntimeError("boom"), None]

        # Act
        with self.assertLogs("aioads.ads_notifications", level="ERROR"):
            async with self.subscribe():
                self.feed(handle=42, value=1)
                await settle()
                self.feed(handle=42, value=2)
                await settle()

        # Assert
        self.assertEqual(self.callback.await_count, 2)

    async def feed_while_callback_blocked(
        self, queue_size: int, overflow: OverflowPolicy
    ) -> tuple[list[list[int]], int]:
        """
        Deliver value 1, then feed 2..5 while the callback is still busy with
        it. Returns the batches the callback received and the dropped count.
        """
        release = asyncio.Event()
        received: list[list[int]] = []

        async def slow_callback(values: list[NotificationValue]) -> None:
            received.append([item.value for item in values])
            await release.wait()

        async with self.subscribe(slow_callback, queue_size, overflow):
            self.feed(handle=42, value=1)
            await settle()
            for value in (2, 3, 4, 5):
                self.feed(handle=42, value=value)
                await settle()
            release.set()
            await settle()
            dropped = self.registry.subscriptions[0].dropped
        return received, dropped

    async def test_feed_overflow_drop_oldest_keeps_most_recent_values(self) -> None:
        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            received, dropped = await self.feed_while_callback_blocked(
                queue_size=2, overflow=OverflowPolicy.DROP_OLDEST
            )

        # Assert
        self.assertEqual(received, [[1], [4, 5]])
        self.assertEqual(dropped, 2)

    async def test_feed_overflow_drop_newest_keeps_first_values(self) -> None:
        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            received, dropped = await self.feed_while_callback_blocked(
                queue_size=2, overflow=OverflowPolicy.DROP_NEWEST
            )

        # Assert
        self.assertEqual(received, [[1], [2, 3]])
        self.assertEqual(dropped, 2)

    async def test_feed_overflow_latest_only_delivers_most_recent_value(self) -> None:
        # Act — queue_size is ignored for this policy
        received, dropped = await self.feed_while_callback_blocked(
            queue_size=100, overflow=OverflowPolicy.LATEST_ONLY
        )

        # Assert
        self.assertEqual(received, [[1], [5]])
        self.assertEqual(dropped, 3)

    async def test_feed_overflow_latest_only_does_not_log_warning(self) -> None:
        # Act / Assert — replacing values is the expected mode of operation
        with self.assertNoLogs("aioads.ads_notifications", level="WARNING"):
            await self.feed_while_callback_blocked(
                queue_size=100, overflow=OverflowPolicy.LATEST_ONLY
            )

    async def test_feed_malformed_frame_is_counted_and_does_not_raise(self) -> None:
        # Arrange — stream header announces a stamp that is missing
        payload = (0).to_bytes(4, "little") + (1).to_bytes(4, "little")

        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            self.registry.feed(
                make_notification_header(self.address), make_stream(payload)
            )
            await settle()

        # Assert
        self.assertEqual(self.registry.decode_errors, 1)

    async def test_feed_after_exit_does_not_reach_callback(self) -> None:
        # Arrange
        async with self.subscribe():
            pass

        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            self.feed(handle=42, value=7)
            await settle()

        # Assert
        self.callback.assert_not_awaited()

    async def test_feed_same_handle_from_other_remote_skips_callback(self) -> None:
        # Arrange — handles are only unique per remote address
        other = make_ams_address(net_id="7.7.7.7.1.1")
        payload = build_notification(42, 1, struct.pack("<i", 1))

        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            async with self.subscribe():
                self.registry.feed(
                    make_notification_header(other), make_stream(payload)
                )
                await settle()

        # Assert
        self.callback.assert_not_awaited()


class TestNotificationHandleSafety(NotificationTestCase):

    async def test_feed_unknown_handle_requests_removal_once(self) -> None:
        # Act — the orphan fires twice before the removal completed
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            self.feed(handle=999, value=1)
            self.feed(handle=999, value=2)
            await settle()

        # Assert
        self.assertEqual(self.remote.deleted, [999])

    async def test_feed_new_handle_before_add_response_is_not_removed(self) -> None:
        # Arrange — the add response is still in flight when the handle fires
        self.remote.add_gate = asyncio.Event()
        context = self.subscribe()
        entering = asyncio.create_task(context.__aenter__())
        await settle()

        # Act
        self.feed(handle=42, value=1)
        await settle()
        self.remote.add_gate.set()
        await entering
        await settle()

        # Assert
        self.assertEqual(self.remote.deleted, [])

    async def test_feed_unknown_handle_during_add_is_removed_afterwards(self) -> None:
        # Arrange
        self.remote.add_gate = asyncio.Event()
        context = self.subscribe()
        entering = asyncio.create_task(context.__aenter__())
        await settle()

        # Act
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            self.feed(handle=999, value=1)
            await settle()
            self.remote.add_gate.set()
            await entering
            await settle()

        # Assert
        self.assertEqual(self.remote.deleted, [999])

    async def test_subscribe_beyond_limit_raises_value_error(self) -> None:
        # Arrange
        self.registry.MAX_SUBSCRIPTIONS = 1

        # Act / Assert
        async with self.subscribe():
            with self.assertRaises(ValueError):
                async with self.subscribe():
                    pass

    async def test_on_reconnected_retries_failed_removal(self) -> None:
        # Arrange
        self.remote.delete_error = ConnectionError("Connection lost")
        with self.assertLogs("aioads.ads_notifications", level="WARNING"):
            async with self.subscribe():
                pass
        self.remote.delete_error = None

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(self.remote.deleted, [42, 42])

    async def test_on_reconnected_does_not_retry_removal_of_invalid_handle(
        self,
    ) -> None:
        # Arrange — 1812: the remote does not know the handle anymore
        self.remote.delete_error_code = 1812
        async with self.subscribe():
            pass

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(self.remote.deleted, [42])

    async def test_on_reconnected_registers_subscription_again(self) -> None:
        # Act
        async with self.subscribe():
            self.registry.on_reconnected()
            await settle()
            added = list(self.remote.added)

        # Assert
        self.assertEqual(added, [42, 43])

    async def test_on_reconnected_removes_previous_handle(self) -> None:
        # Act
        async with self.subscribe():
            self.registry.on_reconnected()
            await settle()
            deleted = list(self.remote.deleted)

        # Assert
        self.assertEqual(deleted, [42])

    async def test_on_reconnected_keeps_reporting_original_handle(self) -> None:
        # Act
        async with self.subscribe():
            self.registry.on_reconnected()
            await settle()
            self.feed(handle=43, value=5)
            await settle()

        # Assert
        self.callback.assert_awaited_once_with(
            [NotificationValue(handle=42, timestamp=1234, value=5)]
        )

    async def test_shutdown_removes_remaining_handles(self) -> None:
        # Arrange
        context = self.subscribe()
        await context.__aenter__()

        # Act
        await self.registry.shutdown()

        # Assert
        self.assertEqual(self.remote.deleted, [42])


if __name__ == "__main__":
    unittest.main()
