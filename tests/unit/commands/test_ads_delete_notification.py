"""Unit tests for aioads.commands.ads_delete_notification."""

import unittest
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_delete_notification import AdsDeleteNotificationCommand
from aioads.commands.ads_write import AdsWriteResponse
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsDeleteNotificationCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = AdsDeleteNotificationCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
            notification_handle=0x01020304,
        )

    def test_serialize_packs_handle_as_four_little_endian_bytes(self) -> None:
        # Act
        result = self.command.serialize()

        # Assert
        self.assertEqual(result, (0x01020304).to_bytes(4, "little"))

    async def test_request_returns_write_response(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()))
        )

        # Act
        response = await self.command.request()

        # Assert
        self.assertEqual(response.error_code, 0)

    async def test_request_uses_delete_device_notification_command_id(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"],
            AdsCommandId.DELETE_DEVICE_NOTIFICATION,
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
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(1812)).serialize()))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1812)


if __name__ == "__main__":
    unittest.main()
