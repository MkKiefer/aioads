"""Unit tests for aioads.commands.ads_write."""

import unittest
from struct import Struct
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_write import AdsWriteCommand, AdsWriteResponse
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsWriteResponse(unittest.TestCase):

    def test_serialize_packs_error_code_as_four_bytes(self) -> None:
        # Arrange
        response = AdsWriteResponse(error_code=AdsErrorCode(0))

        # Act
        result = response.serialize()

        # Assert
        self.assertEqual(result, (0).to_bytes(4, "little"))

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AdsWriteResponse(error_code=AdsErrorCode(1796))

        # Act
        restored = AdsWriteResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_short_stream_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"\x00\x00")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            AdsWriteResponse.deserialize(stream)

        self.assertIn("Invalid data length", str(ctx.exception))


class TestAdsWriteCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = AdsWriteCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
            idx_group=0x4020,
            idx_offset=8,
            payload=b"\xde\xad",
        )

    def test_serialize_packs_header_and_payload(self) -> None:
        # Act
        result = self.command.serialize()

        # Assert
        self.assertEqual(result, Struct("<III2s").pack(0x4020, 8, 2, b"\xde\xad"))

    async def test_request_returns_write_response(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()))
        )

        # Act
        response = await self.command.request()

        # Assert
        self.assertEqual(response.error_code, 0)

    async def test_request_uses_write_command_id(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"], AdsCommandId.WRITE
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
            return_value=(make_ams_header(), make_stream(AdsWriteResponse(AdsErrorCode(1796)).serialize()))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1796)


if __name__ == "__main__":
    unittest.main()
