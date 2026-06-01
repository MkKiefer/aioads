"""Unit tests for aioads.commands.ads_add_notification."""

import unittest
from struct import Struct
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_add_notification import (
    AdsAddNotificationCommand,
    AdsAddNotificationResponse,
    TransmissionMode,
)
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsAddNotificationResponse(unittest.TestCase):

    def test_serialize_packs_error_code_and_handle(self) -> None:
        # Arrange
        response = AdsAddNotificationResponse(error_code=AdsErrorCode(0), notification_handle=99)

        # Act
        result = response.serialize()

        # Assert
        self.assertEqual(result, Struct("<II").pack(0, 99))

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AdsAddNotificationResponse(error_code=AdsErrorCode(0), notification_handle=4242)

        # Act
        restored = AdsAddNotificationResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_short_stream_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"\x00\x00\x00\x00")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            AdsAddNotificationResponse.deserialize(stream)

        self.assertIn("Invalid data length", str(ctx.exception))


class TestAdsAddNotificationCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = AdsAddNotificationCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
            idx_group=0x4020,
            idx_offset=0,
            length=4,
            transmission_mode=TransmissionMode.CYCLIC,
            max_delay=100,
            cycle_time=50,
        )

    def test_serialize_packs_all_fields_and_reserved_block(self) -> None:
        # Act
        result = self.command.serialize()

        # Assert
        expected = Struct("<IIIIII16s").pack(
            0x4020, 0, 4, TransmissionMode.CYCLIC, 100, 50, bytes(16)
        )
        self.assertEqual(result, expected)

    async def test_request_returns_notification_handle(self) -> None:
        # Arrange
        payload = AdsAddNotificationResponse(AdsErrorCode(0), notification_handle=7).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        response = await self.command.request()

        # Assert
        self.assertEqual(response.notification_handle, 7)

    async def test_request_uses_add_device_notification_command_id(self) -> None:
        # Arrange
        payload = AdsAddNotificationResponse(AdsErrorCode(0), notification_handle=1).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"],
            AdsCommandId.ADD_DEVICE_NOTIFICATION,
        )

    async def test_request_ams_error_raises_ams_header_error(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(error_code=7), make_stream(b""))
        )

        # Act / Assert
        with self.assertRaises(AdsAmsHeaderError):
            await self.command.request()

    async def test_request_command_error_raises_command_error(self) -> None:
        # Arrange
        payload = AdsAddNotificationResponse(AdsErrorCode(1814), notification_handle=0).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1814)


if __name__ == "__main__":
    unittest.main()
