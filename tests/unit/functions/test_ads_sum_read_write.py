"""Unit tests for aioads.functions.ads_sum_read_write."""

import unittest
from unittest.mock import AsyncMock

from parameterized import parameterized

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

    def _function(self, commands, batch_size: int = 500) -> AdsSumReadWrite:
        return AdsSumReadWrite(
            transport=self.transport,
            ams_address=self.address,
            commands=commands,
            batch_size=batch_size,
        )

    def test_serialize_places_all_headers_before_all_payloads(self) -> None:
        # Arrange
        function = self._function(self.commands)

        # Act
        result = function.serialize(self.commands)

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

    @parameterized.expand([
        ("zero", 0),
        ("above_maximum", 501),
    ])
    async def test_execute_batch_size_out_of_range_raises_value_error(
        self, _name: str, batch_size: int
    ) -> None:
        # Arrange
        function = self._function(self.commands, batch_size=batch_size)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("between 1 and 500", str(ctx.exception))

    async def test_execute_splits_commands_into_batches(self) -> None:
        # Arrange: 3 commands with batch_size 2 -> one request with 2 commands,
        # one request with the remaining command
        commands = self.commands + [
            AdsReadWriteCommand(self.transport, self.address, 0xF009, 0, 1, 1, b"z")
        ]
        first_batch_body = (
            AdsReadResponse(AdsErrorCode(0), length=4).serialize()
            + AdsReadResponse(AdsErrorCode(0), length=2).serialize()
            + b"\x01\x02\x03\x04"
            + b"\xaa\xbb"
        )
        second_batch_body = (
            AdsReadResponse(AdsErrorCode(0), length=1).serialize() + b"\xcc"
        )
        self.transport.request = AsyncMock(
            side_effect=[
                (make_ams_header(), make_stream(make_read_payload(first_batch_body))),
                (make_ams_header(), make_stream(make_read_payload(second_batch_body))),
            ]
        )
        function = self._function(commands, batch_size=2)

        # Act
        response = await function.execute()

        # Assert
        self.assertEqual(self.transport.request.await_count, 2)
        self.assertEqual(len(response), 3)
        self.assertEqual(response[2][1].read(1), b"\xcc")

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
