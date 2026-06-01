"""Unit tests for aioads.commands.ads_read."""

import unittest
from struct import Struct
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_read import AdsReadCommand, AdsReadResponse
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsReadResponse(unittest.TestCase):

    def test_serialize_packs_error_code_and_length(self) -> None:
        # Arrange
        response = AdsReadResponse(error_code=AdsErrorCode(0), length=4)

        # Act
        result = response.serialize()

        # Assert
        self.assertEqual(result, Struct("<II").pack(0, 4))

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AdsReadResponse(error_code=AdsErrorCode(0), length=16)

        # Act
        restored = AdsReadResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_advances_stream_by_eight_bytes(self) -> None:
        # Arrange
        stream = make_stream(Struct("<II").pack(0, 1))

        # Act
        AdsReadResponse.deserialize(stream)

        # Assert
        self.assertEqual(stream.tell(), 8)


class TestAdsReadCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        # Real typed transport (Pattern A); request is replaced per test.
        self.transport = make_transport()
        self.command = AdsReadCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
            idx_group=0x4020,
            idx_offset=0,
            length=4,
        )

    def test_serialize_packs_group_offset_and_length(self) -> None:
        # Act
        result = self.command.serialize()

        # Assert
        self.assertEqual(result, Struct("<III").pack(0x4020, 0, 4))

    async def test_request_returns_read_header_and_payload_substream(self) -> None:
        # Arrange
        payload = AdsReadResponse(AdsErrorCode(0), length=4).serialize() + b"\x01\x02\x03\x04"
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        read_header, data_stream = await self.command.request()

        # Assert
        self.assertEqual(read_header.length, 4)
        self.assertEqual(data_stream.read(4), b"\x01\x02\x03\x04")

    async def test_request_uses_read_command_id(self) -> None:
        # Arrange
        payload = AdsReadResponse(AdsErrorCode(0), length=0).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"], AdsCommandId.READ
        )

    async def test_request_ams_error_raises_ams_header_error(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(error_code=7), make_stream(b""))
        )

        # Act / Assert
        with self.assertRaises(AdsAmsHeaderError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 7)

    async def test_request_command_error_raises_command_error(self) -> None:
        # Arrange
        payload = AdsReadResponse(AdsErrorCode(1793), length=0).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1793)


if __name__ == "__main__":
    unittest.main()
