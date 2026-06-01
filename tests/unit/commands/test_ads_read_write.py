"""Unit tests for aioads.commands.ads_read_write."""

import unittest
from struct import Struct
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_read import AdsReadResponse
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsReadWriteCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = AdsReadWriteCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
            idx_group=0xF009,
            idx_offset=0,
            read_length=0xFFFF,
            write_length=3,
            write_data=b"abc",
        )

    def test_serialize_header_packs_group_offset_and_lengths(self) -> None:
        # Act
        result = self.command.serialize_header()

        # Assert
        self.assertEqual(result, Struct("<IIII").pack(0xF009, 0, 0xFFFF, 3))

    def test_serialize_appends_payload_after_header(self) -> None:
        # Act
        result = self.command.serialize()

        # Assert
        self.assertEqual(result, self.command.serialize_header() + b"abc")

    async def test_request_returns_read_header_and_payload_substream(self) -> None:
        # Arrange
        payload = AdsReadResponse(AdsErrorCode(0), length=2).serialize() + b"\xaa\xbb"
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        read_header, data_stream = await self.command.request()

        # Assert
        self.assertEqual(read_header.length, 2)
        self.assertEqual(data_stream.read(2), b"\xaa\xbb")

    async def test_request_uses_read_write_command_id(self) -> None:
        # Arrange
        payload = AdsReadResponse(AdsErrorCode(0), length=0).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"], AdsCommandId.READ_WRITE
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
        payload = AdsReadResponse(AdsErrorCode(1794), length=0).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1794)


if __name__ == "__main__":
    unittest.main()
