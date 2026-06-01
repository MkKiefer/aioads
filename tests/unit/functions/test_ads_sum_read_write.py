"""Unit tests for aioads.functions.ads_sum_read_write."""

import unittest
from unittest.mock import AsyncMock

from aioads.commands.ads_read import AdsReadResponse
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.ads_error_codes import AdsErrorCode
from aioads.functions.ads_sum_read_write import AdsSumReadWrite
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


class TestAdsSumReadWrite(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.commands = [
            AdsReadWriteCommand(self.transport, self.address, 0xF009, 0, 4, 2, b"ab"),
            AdsReadWriteCommand(self.transport, self.address, 0xF009, 0, 2, 3, b"xyz"),
        ]

    def _function(self, commands) -> AdsSumReadWrite:
        return AdsSumReadWrite(
            transport=self.transport, ams_address=self.address, commands=commands
        )

    def test_serialize_places_all_headers_before_all_payloads(self) -> None:
        # Arrange
        function = self._function(self.commands)

        # Act
        result = function.serialize()

        # Assert
        expected = (
            self.commands[0].serialize_header()
            + self.commands[1].serialize_header()
            + b"ab"
            + b"xyz"
        )
        self.assertEqual(result, expected)

    async def test_execute_empty_commands_raises_value_error(self) -> None:
        # Arrange
        function = self._function([])

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("At least one command", str(ctx.exception))

    async def test_execute_too_many_commands_raises_value_error(self) -> None:
        # Arrange
        many = [
            AdsReadWriteCommand(self.transport, self.address, 0, 0, 1, 0, b"")
            for _ in range(501)
        ]
        function = self._function(many)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("maximum is 500", str(ctx.exception))

    async def test_execute_returns_read_response_and_data_per_command(self) -> None:
        # Arrange: per-command (error, length) headers, then per-command payloads
        body = (
            AdsReadResponse(AdsErrorCode(0), length=4).serialize()
            + AdsReadResponse(AdsErrorCode(0), length=2).serialize()
            + b"\x01\x02\x03\x04"
            + b"\xaa\xbb"
        )
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(body)))
        )
        function = self._function(self.commands)

        # Act
        response = await function.execute()

        # Assert
        self.assertEqual(response[0][0].length, 4)
        self.assertEqual(response[1][1].read(2), b"\xaa\xbb")


if __name__ == "__main__":
    unittest.main()
