"""Unit tests for aioads.ads_notifications."""

import struct
import unittest
from unittest.mock import AsyncMock

from aioads.ads_notifications import NotificationManager, NotificationValue
from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import AdsSymbolParser
from aioads.commands.ads_add_notification import (
    AdsAddNotificationResponse,
    TransmissionMode,
)
from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_write import AdsWriteResponse
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
)
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


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
    sample = handle.to_bytes(4, "little") + \
        len(value_bytes).to_bytes(4, "little") + value_bytes
    stamp = timestamp.to_bytes(8, "little") + \
        (1).to_bytes(4, "little") + sample
    return (0).to_bytes(4, "little") + (1).to_bytes(4, "little") + stamp


def add_response(handle: int):
    """Transport return value for an AddNotification request."""
    payload = AdsAddNotificationResponse(
        AdsErrorCode(0), notification_handle=handle).serialize()
    return (make_ams_header(), make_stream(payload))


def delete_response():
    """Transport return value for a DeleteNotification request."""
    payload = AdsWriteResponse(AdsErrorCode(0)).serialize()
    return (make_ams_header(), make_stream(payload))


class TestNotificationManager(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        # Real typed collaborators; only the methods under control are replaced.
        self.cache = AdsSymbolCache(self.transport, self.address, batch_size=500)
        self.parser = AdsSymbolParser([])
        self.manager = NotificationManager(
            transport=self.transport,
            dst_address=self.address,
            symbol_cache=self.cache,
            parser=self.parser,
        )
        self.info = make_symbol_info()

    async def test_create_notification_yields_handle_from_add_response(self) -> None:
        # Arrange
        self.cache.read_symbol_info_by_name = AsyncMock(return_value=self.info)
        self.transport.request = AsyncMock(
            side_effect=[add_response(42), delete_response()])

        # Act
        async with self.manager.create_notification(
            "MAIN.VALUE", AsyncMock(), TransmissionMode.CYCLIC, 50, 100
        ) as handle:
            yielded = handle

        # Assert
        self.assertEqual(yielded, 42)

    async def test_create_notification_removes_handle_on_exit(self) -> None:
        # Arrange
        self.cache.read_symbol_info_by_name = AsyncMock(return_value=self.info)
        self.transport.request = AsyncMock(
            side_effect=[add_response(42), delete_response()])

        # Act
        async with self.manager.create_notification(
            "MAIN.VALUE", AsyncMock(), TransmissionMode.CYCLIC, 50, 100
        ):
            pass

        # Assert — the second transport call is the delete request
        last_call = self.transport.request.call_args_list[-1]
        self.assertEqual(
            last_call.kwargs["command_id"], AdsCommandId.DELETE_DEVICE_NOTIFICATION
        )

    async def test_on_notification_received_dispatches_parsed_value_to_callback(self) -> None:
        # Arrange
        callback = AsyncMock()
        self.cache.read_symbol_info_by_name = AsyncMock(return_value=self.info)
        self.transport.request = AsyncMock(
            side_effect=[add_response(42), delete_response()])

        # Act
        async with self.manager.create_notification(
            "MAIN.VALUE", callback, TransmissionMode.CYCLIC, 50, 100
        ):
            payload = build_notification(
                handle=42, timestamp=1234, value_bytes=struct.pack("<i", 7))
            await self.manager.on_notification_received(make_ams_header(), make_stream(payload))

        # Assert
        callback.assert_awaited_once()
        delivered = callback.await_args.args[0]
        self.assertEqual(delivered[0], NotificationValue(
            handle=42, timestamp=1234, value=7))

    async def test_on_notification_received_unknown_handle_skips_callback(self) -> None:
        # Arrange — no notification registered; transport answers the cleanup delete
        callback = AsyncMock()
        self.transport.request = AsyncMock(return_value=delete_response())
        payload = build_notification(
            handle=999, timestamp=1, value_bytes=struct.pack("<i", 1))

        # Act
        await self.manager.on_notification_received(make_ams_header(), make_stream(payload))

        # Assert
        callback.assert_not_awaited()

    async def test_on_notification_received_unknown_handle_requests_removal(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=delete_response())
        payload = build_notification(
            handle=999, timestamp=1, value_bytes=struct.pack("<i", 1))

        # Act
        await self.manager.on_notification_received(make_ams_header(), make_stream(payload))

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"],
            AdsCommandId.DELETE_DEVICE_NOTIFICATION,
        )


if __name__ == "__main__":
    unittest.main()
