"""Unit tests for aioads.ads_notifications."""

import asyncio
import logging
import struct
import unittest
from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from parameterized import parameterized

from aioads.ads_error_codes import AdsErrorCode
from aioads.ads_notifications import (
    NotificationManager,
    NotificationRegistry,
    NotificationValue,
    OverflowPolicy,
    Subscription,
    TNotificationCallback,
    decode_frames,
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
from aioads.transport import INotificationSink
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_stream,
    make_transport,
)

LOGGER_NAME = "aioads.ads_notifications"


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


def make_value(value: int, handle: int = 42) -> NotificationValue:
    """A decoded notification value as the pump hands it to a subscription."""
    return NotificationValue(handle=handle, timestamp=1234, value=value)


def make_subscription(
    callback: TNotificationCallback | None = None,
    queue_size: int = 1000,
    overflow: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    handle: int = 42,
) -> Subscription:
    """A subscription for the INT32 fixture symbol that is not registered anywhere."""
    subscription = Subscription(
        owner=None,
        address=make_ams_address(),
        symbol_info=make_symbol_info(),
        parser=AdsSymbolParser([]),
        callback=callback or AsyncMock(),
        mode=TransmissionMode.CYCLIC,
        cycle_time=50,
        max_delay=100,
        queue_size=queue_size,
        overflow=overflow,
    )
    subscription.handle = handle
    subscription.remote_handle = handle
    return subscription


def build_notification(handle: int, timestamp: int, value_bytes: bytes) -> bytes:
    """Build a device-notification payload with a single stamp and sample."""
    sample = (
        handle.to_bytes(4, "little")
        + len(value_bytes).to_bytes(4, "little")
        + value_bytes
    )
    stamp = timestamp.to_bytes(8, "little") + (1).to_bytes(4, "little") + sample
    return (0).to_bytes(4, "little") + (1).to_bytes(4, "little") + stamp


def make_frame(
    handle: int, value: int, address: AmsAddress | None = None
) -> tuple[AmsAddress, AdsStream]:
    """A queued notification frame carrying one INT32 sample."""
    payload = build_notification(handle, 1234, struct.pack("<i", value))
    return (address or make_ams_address(), make_stream(payload))


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


def remote_responder(
    first_handle: int = 42,
    delete_error: Exception | None = None,
    delete_error_code: int = 0,
    add_gate: asyncio.Event | None = None,
):
    """
    Build a ``side_effect`` for ``transport.request`` that answers like a
    remote: add requests get ascending handles, delete requests succeed
    unless an error is configured. ``add_gate`` holds add responses back.
    """
    handles = iter(range(first_handle, first_handle + 1000))

    async def respond(
        command_payload: bytes, command_id: AdsCommandId, ams_address: AmsAddress
    ) -> tuple[AmsHeader, AdsStream]:
        del command_payload, ams_address
        if command_id == AdsCommandId.ADD_DEVICE_NOTIFICATION:
            if add_gate is not None:
                await add_gate.wait()
            return add_response(next(handles))
        if delete_error is not None:
            raise delete_error
        return delete_response(delete_error_code)

    return respond


def deleted_handles(request: AsyncMock) -> list[int]:
    """The handles of all delete requests sent through the mocked ``request``."""
    return [
        int.from_bytes(call.kwargs["command_payload"], "little")
        for call in request.call_args_list
        if call.kwargs["command_id"] == AdsCommandId.DELETE_DEVICE_NOTIFICATION
    ]


def add_request_count(request: AsyncMock) -> int:
    """The number of add requests sent through the mocked ``request``."""
    return sum(
        1
        for call in request.call_args_list
        if call.kwargs["command_id"] == AdsCommandId.ADD_DEVICE_NOTIFICATION
    )


async def wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """Let the event loop (and worker threads) run until ``predicate`` holds."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


async def settle() -> None:
    """Hand control to the background tasks (pump, consumers, removals)."""
    for _ in range(20):
        await asyncio.sleep(0)


def silence_logger(test: unittest.TestCase) -> None:
    """Keep expected warnings of the module under test out of the test output."""
    handler = logging.NullHandler()
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(handler)
    test.addCleanup(logger.removeHandler, handler)


class OtherSink(INotificationSink):
    """A notification sink that is not a NotificationRegistry."""

    def feed(self, ams_header: AmsHeader, payload: AdsStream) -> None:
        return None

    def on_reconnected(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None


class TestNotificationValue(unittest.TestCase):

    def test_datetime_converts_filetime_to_utc_datetime(self) -> None:
        # Arrange — 100 ns ticks between 1601-01-01 and 1970-01-01
        value = NotificationValue(handle=1, timestamp=116444736000000000, value=0)

        # Act
        result = value.datetime

        # Assert
        self.assertEqual(result, datetime(1970, 1, 1, tzinfo=timezone.utc))


class TestDecodeFrames(unittest.TestCase):

    def setUp(self) -> None:
        self.address = make_ams_address()
        self.subscription = make_subscription(handle=42)
        self.routes = {(self.address, 42): self.subscription}

    def test_decode_frames_known_handle_returns_value_for_subscription(self) -> None:
        # Arrange
        frames = [make_frame(handle=42, value=7)]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        self.assertEqual(result.values, {self.subscription: [make_value(7)]})

    def test_decode_frames_multiple_frames_keeps_wire_order(self) -> None:
        # Arrange
        frames = [make_frame(handle=42, value=value) for value in (3, 1, 2)]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        decoded = [item.value for item in result.values[self.subscription]]
        self.assertEqual(decoded, [3, 1, 2])

    def test_decode_frames_handle_without_route_is_reported_unknown(self) -> None:
        # Arrange
        frames = [make_frame(handle=999, value=7)]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        self.assertEqual(result.unknown, {(self.address, 999)})

    def test_decode_frames_same_handle_from_other_remote_is_reported_unknown(
        self,
    ) -> None:
        # Arrange — handles are only unique per remote address
        other = make_ams_address(net_id="7.7.7.7.1.1")
        frames = [make_frame(handle=42, value=7, address=other)]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        self.assertEqual(result.unknown, {(other, 42)})

    def test_decode_frames_undecodable_sample_counts_error(self) -> None:
        # Arrange — an INT32 sample that carries only a single byte
        payload = build_notification(42, 1234, b"\x01")
        frames = [(self.address, make_stream(payload))]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        self.assertEqual(result.errors, 1)

    def test_decode_frames_truncated_frame_counts_error(self) -> None:
        # Arrange — the stream header announces a stamp that is missing
        payload = (0).to_bytes(4, "little") + (1).to_bytes(4, "little")
        frames = [(self.address, make_stream(payload))]

        # Act
        result = decode_frames(frames, self.routes)

        # Assert
        self.assertEqual(result.errors, 1)


class TestSubscription(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        silence_logger(self)
        self.release = asyncio.Event()
        self.received: list[list[int]] = []

    async def blocking_callback(self, values: list[NotificationValue]) -> None:
        """Records each batch and then blocks until ``self.release`` is set."""
        self.received.append([item.value for item in values])
        await self.release.wait()

    async def start_busy_subscription(
        self, queue_size: int, overflow: OverflowPolicy
    ) -> Subscription:
        """A started subscription whose callback is still busy with value 1."""
        subscription = make_subscription(self.blocking_callback, queue_size, overflow)
        subscription.start()
        self.addAsyncCleanup(subscription.stop)
        subscription.push([make_value(1)])
        await settle()
        return subscription

    def test_init_with_queue_size_zero_raises_value_error(self) -> None:
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            make_subscription(queue_size=0)

        self.assertIn("queue_size", str(ctx.exception))

    def test_decode_primitive_sample_returns_value(self) -> None:
        # Arrange
        subscription = make_subscription()
        sample = make_stream(struct.pack("<i", 7))

        # Act
        result = subscription.decode(sample)

        # Assert
        self.assertEqual(result, 7)

    async def test_push_delivers_values_to_callback_as_one_batch(self) -> None:
        # Arrange
        callback = AsyncMock()
        subscription = make_subscription(callback)
        subscription.start()
        self.addAsyncCleanup(subscription.stop)

        # Act
        subscription.push([make_value(1), make_value(2)])
        await settle()

        # Assert
        callback.assert_awaited_once_with([make_value(1), make_value(2)])

    async def test_push_after_failing_callback_still_delivers_next_value(self) -> None:
        # Arrange
        callback = AsyncMock(side_effect=[RuntimeError("boom"), None])
        subscription = make_subscription(callback)
        subscription.start()
        self.addAsyncCleanup(subscription.stop)
        subscription.push([make_value(1)])
        await settle()

        # Act
        subscription.push([make_value(2)])
        await settle()

        # Assert
        callback.assert_awaited_with([make_value(2)])

    async def test_push_failing_callback_logs_error(self) -> None:
        # Arrange
        subscription = make_subscription(AsyncMock(side_effect=RuntimeError("boom")))
        subscription.start()
        self.addAsyncCleanup(subscription.stop)

        # Act / Assert
        with self.assertLogs(LOGGER_NAME, level="ERROR"):
            subscription.push([make_value(1)])
            await settle()

    async def test_push_after_stop_does_not_call_callback(self) -> None:
        # Arrange
        callback = AsyncMock()
        subscription = make_subscription(callback)
        subscription.start()
        await subscription.stop()

        # Act
        subscription.push([make_value(1)])
        await settle()

        # Assert
        callback.assert_not_awaited()

    @parameterized.expand(
        [
            ("drop_oldest", OverflowPolicy.DROP_OLDEST, 2, [4, 5]),
            ("drop_newest", OverflowPolicy.DROP_NEWEST, 2, [2, 3]),
            ("latest_only_ignores_queue_size", OverflowPolicy.LATEST_ONLY, 100, [5]),
        ]
    )
    async def test_push_beyond_queue_size_delivers_values_by_policy(
        self, _name: str, overflow: OverflowPolicy, queue_size: int, expected: list[int]
    ) -> None:
        # Arrange
        subscription = await self.start_busy_subscription(queue_size, overflow)

        # Act — values 2..5 arrive while the callback is busy
        for value in (2, 3, 4, 5):
            subscription.push([make_value(value)])
        self.release.set()
        await settle()

        # Assert
        self.assertEqual(self.received[1], expected)

    @parameterized.expand(
        [
            ("drop_oldest", OverflowPolicy.DROP_OLDEST, 2, 2),
            ("drop_newest", OverflowPolicy.DROP_NEWEST, 2, 2),
            ("latest_only", OverflowPolicy.LATEST_ONLY, 100, 3),
        ]
    )
    async def test_push_beyond_queue_size_counts_dropped_values(
        self, _name: str, overflow: OverflowPolicy, queue_size: int, expected: int
    ) -> None:
        # Arrange
        subscription = await self.start_busy_subscription(queue_size, overflow)

        # Act
        for value in (2, 3, 4, 5):
            subscription.push([make_value(value)])

        # Assert
        self.assertEqual(subscription.dropped, expected)

    @parameterized.expand(
        [
            ("drop_oldest", OverflowPolicy.DROP_OLDEST),
            ("drop_newest", OverflowPolicy.DROP_NEWEST),
        ]
    )
    async def test_push_beyond_queue_size_logs_warning(
        self, _name: str, overflow: OverflowPolicy
    ) -> None:
        # Arrange
        subscription = await self.start_busy_subscription(1, overflow)

        # Act / Assert
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            subscription.push([make_value(2), make_value(3)])

    async def test_push_latest_only_replacing_value_does_not_log_warning(self) -> None:
        # Arrange
        subscription = await self.start_busy_subscription(
            1, OverflowPolicy.LATEST_ONLY
        )

        # Act / Assert — replacing values is the expected mode of operation
        with self.assertNoLogs(LOGGER_NAME, level="WARNING"):
            subscription.push([make_value(2), make_value(3)])


class TestNotificationRegistry(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        silence_logger(self)
        self.transport = make_transport()
        self.address = make_ams_address()
        # Background stub: the remote answers every add / delete request.
        self.transport.request = AsyncMock(side_effect=remote_responder())
        self.registry = NotificationRegistry(self.transport)

    async def asyncTearDown(self) -> None:
        await self.registry.shutdown()

    async def subscribe(
        self,
        callback: TNotificationCallback | None = None,
        registry: NotificationRegistry | None = None,
        owner: object = None,
    ) -> Subscription:
        """Subscribe the INT32 fixture symbol with default settings."""
        return await (registry or self.registry).subscribe(
            owner=owner,
            address=self.address,
            symbol_info=make_symbol_info(),
            parser=AdsSymbolParser([]),
            callback=callback or AsyncMock(),
            mode=TransmissionMode.CYCLIC,
            cycle_time=50,
            max_delay=100,
        )

    def feed(self, handle: int, value: int, address: AmsAddress | None = None) -> None:
        """Simulate the transport receiving a notification from the remote."""
        payload = build_notification(handle, 1234, struct.pack("<i", value))
        self.registry.feed(
            make_notification_header(address or self.address), make_stream(payload)
        )

    # ------------------------------------------------------------------ attach

    def test_attach_sets_registry_as_notification_sink_of_transport(self) -> None:
        # Arrange
        transport = make_transport()

        # Act
        registry = NotificationRegistry.attach(transport)

        # Assert
        self.assertIs(transport.notification_sink, registry)

    def test_attach_twice_returns_same_registry(self) -> None:
        # Arrange
        transport = make_transport()
        first = NotificationRegistry.attach(transport)

        # Act
        second = NotificationRegistry.attach(transport)

        # Assert
        self.assertIs(second, first)

    def test_attach_with_foreign_sink_raises_runtime_error(self) -> None:
        # Arrange
        transport = make_transport()
        transport.set_notification_sink(OtherSink())

        # Act / Assert
        with self.assertRaises(RuntimeError) as ctx:
            NotificationRegistry.attach(transport)

        self.assertIn("different notification sink", str(ctx.exception))

    # --------------------------------------------------------------- subscribe

    async def test_subscribe_returns_subscription_with_handle_from_add_response(
        self,
    ) -> None:
        # Act
        subscription = await self.subscribe()

        # Assert
        self.assertEqual(subscription.handle, 42)

    async def test_subscribe_adds_subscription_to_active_subscriptions(self) -> None:
        # Act
        subscription = await self.subscribe()

        # Assert
        self.assertEqual(self.registry.subscriptions, [subscription])

    async def test_subscribe_beyond_limit_raises_value_error(self) -> None:
        # Arrange
        registry = NotificationRegistry(self.transport, max_subscriptions=1)
        self.addAsyncCleanup(registry.shutdown)
        await self.subscribe(registry=registry)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await self.subscribe(registry=registry)

        self.assertIn("Maximum number", str(ctx.exception))

    # ------------------------------------------------------------- unsubscribe

    async def test_unsubscribe_requests_removal_of_handle(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())
        subscription = await self.subscribe()

        # Act
        await self.registry.unsubscribe(subscription)

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_unsubscribe_removes_subscription_from_active_subscriptions(
        self,
    ) -> None:
        # Arrange
        subscription = await self.subscribe()

        # Act
        await self.registry.unsubscribe(subscription)

        # Assert
        self.assertEqual(self.registry.subscriptions, [])

    async def test_unsubscribe_twice_requests_removal_once(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())
        subscription = await self.subscribe()
        await self.registry.unsubscribe(subscription)

        # Act
        await self.registry.unsubscribe(subscription)

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_unsubscribe_failed_removal_logs_warning(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            side_effect=remote_responder(delete_error=ConnectionError("lost"))
        )
        subscription = await self.subscribe()

        # Act / Assert — the failure is reported but never raised to the caller
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            await self.registry.unsubscribe(subscription)

    async def test_unsubscribe_all_removes_only_handles_of_owner(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())
        owner = object()
        await self.subscribe(owner=owner)
        await self.subscribe(owner=object())

        # Act
        await self.registry.unsubscribe_all(owner)

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    # -------------------------------------------------------------------- feed

    async def test_feed_dispatches_parsed_value_to_callback(self) -> None:
        # Arrange
        callback = AsyncMock()
        await self.subscribe(callback)

        # Act
        self.feed(handle=42, value=7)
        await settle()

        # Assert
        callback.assert_awaited_once_with([make_value(7)])

    async def test_feed_multiple_frames_delivers_one_batch(self) -> None:
        # Arrange
        callback = AsyncMock()
        await self.subscribe(callback)

        # Act
        for value in (1, 2, 3):
            self.feed(handle=42, value=value)
        await settle()

        # Assert
        callback.assert_awaited_once_with(
            [make_value(1), make_value(2), make_value(3)]
        )

    async def test_feed_batch_above_offload_size_delivers_value(self) -> None:
        # Arrange — every batch takes the worker thread path
        self.registry = NotificationRegistry(self.transport, offload_size=0)
        callback = AsyncMock()
        await self.subscribe(callback)

        # Act
        self.feed(handle=42, value=7)
        await wait_until(lambda: callback.await_count > 0)

        # Assert
        callback.assert_awaited_once_with([make_value(7)])

    async def test_feed_malformed_frame_counts_decode_error(self) -> None:
        # Arrange — the stream header announces a stamp that is missing
        payload = (0).to_bytes(4, "little") + (1).to_bytes(4, "little")

        # Act
        self.registry.feed(
            make_notification_header(self.address), make_stream(payload)
        )
        await settle()

        # Assert
        self.assertEqual(self.registry.decode_errors, 1)

    async def test_feed_beyond_pending_limit_counts_dropped_frames(self) -> None:
        # Act — more frames than the ingress holds arrive before the pump runs
        for value in range(NotificationRegistry.MAX_PENDING_FRAMES + 3):
            self.feed(handle=42, value=value)

        # Assert
        self.assertEqual(self.registry.dropped_frames, 3)

    async def test_feed_after_unsubscribe_does_not_reach_callback(self) -> None:
        # Arrange
        callback = AsyncMock()
        subscription = await self.subscribe(callback)
        await self.registry.unsubscribe(subscription)

        # Act
        self.feed(handle=42, value=7)
        await settle()

        # Assert
        callback.assert_not_awaited()

    async def test_feed_same_handle_from_other_remote_skips_callback(self) -> None:
        # Arrange — handles are only unique per remote address
        callback = AsyncMock()
        await self.subscribe(callback)

        # Act
        self.feed(handle=42, value=7, address=make_ams_address(net_id="7.7.7.7.1.1"))
        await settle()

        # Assert
        callback.assert_not_awaited()

    async def test_feed_unknown_handle_requests_removal_once(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())

        # Act — the orphan fires twice before its removal completed
        self.feed(handle=999, value=1)
        self.feed(handle=999, value=2)
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [999])

    async def test_feed_unknown_handle_logs_warning(self) -> None:
        # Act / Assert
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self.feed(handle=999, value=1)
            await settle()

    async def test_feed_new_handle_before_add_response_is_not_removed(self) -> None:
        # Arrange — the add response is still in flight when the handle fires
        add_gate = asyncio.Event()
        self.transport.request = AsyncMock(
            side_effect=remote_responder(add_gate=add_gate)
        )
        subscribing = asyncio.create_task(self.subscribe())
        await settle()

        # Act
        self.feed(handle=42, value=1)
        await settle()
        add_gate.set()
        await subscribing
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [])

    async def test_feed_unknown_handle_during_add_is_removed_afterwards(self) -> None:
        # Arrange
        add_gate = asyncio.Event()
        self.transport.request = AsyncMock(
            side_effect=remote_responder(add_gate=add_gate)
        )
        subscribing = asyncio.create_task(self.subscribe())
        await settle()

        # Act
        self.feed(handle=999, value=1)
        await settle()
        add_gate.set()
        await subscribing
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [999])

    # ---------------------------------------------------------- on_reconnected

    async def test_on_reconnected_retries_failed_removal(self) -> None:
        # Arrange — the removal fails while the connection is down
        self.transport.request = AsyncMock(
            side_effect=remote_responder(delete_error=ConnectionError("lost"))
        )
        subscription = await self.subscribe()
        await self.registry.unsubscribe(subscription)
        self.transport.request = AsyncMock(side_effect=remote_responder())

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_on_reconnected_does_not_retry_removal_of_invalid_handle(
        self,
    ) -> None:
        # Arrange — 1812: the remote does not know the handle anymore
        self.transport.request = AsyncMock(
            side_effect=remote_responder(delete_error_code=1812)
        )
        subscription = await self.subscribe()
        await self.registry.unsubscribe(subscription)
        self.transport.request = AsyncMock(side_effect=remote_responder())

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [])

    async def test_on_reconnected_registers_subscription_again(self) -> None:
        # Arrange
        await self.subscribe()
        self.transport.request = AsyncMock(
            side_effect=remote_responder(first_handle=43)
        )

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(add_request_count(self.transport.request), 1)

    async def test_on_reconnected_removes_previous_handle(self) -> None:
        # Arrange
        await self.subscribe()
        self.transport.request = AsyncMock(
            side_effect=remote_responder(first_handle=43)
        )

        # Act
        self.registry.on_reconnected()
        await settle()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_on_reconnected_keeps_reporting_original_handle(self) -> None:
        # Arrange — after the reconnect the remote knows the symbol as handle 43
        callback = AsyncMock()
        await self.subscribe(callback)
        self.transport.request = AsyncMock(
            side_effect=remote_responder(first_handle=43)
        )
        self.registry.on_reconnected()
        await settle()

        # Act
        self.feed(handle=43, value=5)
        await settle()

        # Assert
        callback.assert_awaited_once_with([make_value(5, handle=42)])

    # ---------------------------------------------------------------- shutdown

    async def test_shutdown_removes_remaining_handles(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())
        await self.subscribe()

        # Act
        await self.registry.shutdown()

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])


class TestNotificationManager(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        silence_logger(self)
        self.transport = make_transport()
        self.address = make_ams_address()
        # Background stubs: the remote answers and the symbol is known.
        self.transport.request = AsyncMock(side_effect=remote_responder())
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

    async def asyncTearDown(self) -> None:
        await self.manager.registry.shutdown()

    def create_notification(
        self,
        queue_size: int = 1000,
        overflow: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ):
        """Subscribe MAIN.VALUE with default settings."""
        return self.manager.create_notification(
            "MAIN.VALUE",
            AsyncMock(),
            TransmissionMode.CYCLIC,
            50,
            100,
            queue_size,
            overflow,
        )

    def test_init_attaches_registry_to_transport(self) -> None:
        # Act
        sink = self.transport.notification_sink

        # Assert
        self.assertIs(sink, self.manager.registry)

    async def test_create_notification_yields_handle_from_add_response(self) -> None:
        # Act
        async with self.create_notification() as handle:
            yielded = handle

        # Assert
        self.assertEqual(yielded, 42)

    async def test_create_notification_resolves_symbol_by_name(self) -> None:
        # Arrange
        self.cache.read_symbol_info_by_name = AsyncMock(
            return_value=make_symbol_info()
        )

        # Act
        async with self.create_notification():
            pass

        # Assert
        self.cache.read_symbol_info_by_name.assert_awaited_once_with("MAIN.VALUE")

    async def test_create_notification_passes_queue_size_to_subscription(self) -> None:
        # Act
        async with self.create_notification(queue_size=1):
            subscription = self.manager.registry.subscriptions[0]
            subscription.push([make_value(1), make_value(2)])

        # Assert — a queue of one value had to drop the other one
        self.assertEqual(subscription.dropped, 1)

    async def test_create_notification_passes_overflow_policy_to_subscription(
        self,
    ) -> None:
        # Act
        async with self.create_notification(overflow=OverflowPolicy.DROP_NEWEST):
            subscription = self.manager.registry.subscriptions[0]

        # Assert
        self.assertIs(subscription.overflow, OverflowPolicy.DROP_NEWEST)

    async def test_create_notification_removes_handle_on_exit(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())

        # Act
        async with self.create_notification():
            pass

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_create_notification_removes_handle_when_body_raises(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())

        # Act
        with self.assertRaises(RuntimeError):
            async with self.create_notification():
                raise RuntimeError("boom")

        # Assert
        self.assertEqual(deleted_handles(self.transport.request), [42])

    async def test_close_removes_all_notifications_of_the_manager(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(side_effect=remote_responder())
        await self.enterAsyncContext(self.create_notification())
        await self.enterAsyncContext(self.create_notification())

        # Act
        await self.manager.close()

        # Assert
        self.assertCountEqual(deleted_handles(self.transport.request), [42, 43])


if __name__ == "__main__":
    unittest.main()
