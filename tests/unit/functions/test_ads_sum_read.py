"""Unit tests for aioads.functions.ads_sum_read."""

import unittest
from unittest.mock import AsyncMock

from parameterized import parameterized

from aioads.commands.ads_read import AdsReadCommand
from aioads.functions.ads_sum_read import AdsSumRead
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


class TestAdsSumRead(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.commands = [
            AdsReadCommand(self.transport, self.address, 0x4020, 0, 4),
            AdsReadCommand(self.transport, self.address, 0x4020, 8, 2),
        ]

    def _function(self, commands, batch_size: int = 500) -> AdsSumRead:
        return AdsSumRead(
            transport=self.transport,
            ams_address=self.address,
            commands=commands,
            batch_size=batch_size,
        )

    def test_serialize_concatenates_each_command_payload(self) -> None:
        # Arrange
        function = self._function(self.commands)

        # Act
        result = function.serialize(self.commands)

        # Assert
        self.assertEqual(
            result, self.commands[0].serialize() + self.commands[1].serialize()
        )

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

    async def test_execute_empty_commands_raises_value_error(self) -> None:
        # Arrange
        function = self._function([])

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("At least one command", str(ctx.exception))

    async def test_execute_splits_commands_into_batches(self) -> None:
        # Arrange: 3 commands with batch_size 2 -> one request with 2 commands,
        # one request with the remaining command
        commands = self.commands + [
            AdsReadCommand(self.transport, self.address, 0x4020, 16, 1)
        ]
        first_batch_body = (
            (0).to_bytes(4, "little") + (0).to_bytes(4, "little")
            + b"\x01\x02\x03\x04" + b"\xaa\xbb"
        )
        second_batch_body = (0).to_bytes(4, "little") + b"\xcc"
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

    async def test_execute_returns_response_and_data_per_command(self) -> None:
        # Arrange: 2 error codes (4 bytes each) followed by 4 + 2 data bytes
        body = (
            (0).to_bytes(4, "little") + (0).to_bytes(4, "little")
            + b"\x01\x02\x03\x04" + b"\xaa\xbb"
        )
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(body)))
        )
        function = self._function(self.commands)

        # Act
        response = await function.execute()

        # Assert
        self.assertEqual(response[0][1].read(4), b"\x01\x02\x03\x04")
        self.assertEqual(response[1][1].read(2), b"\xaa\xbb")

    async def test_execute_maps_per_command_error_codes(self) -> None:
        # Arrange
        body = (
            (0).to_bytes(4, "little") + (1796).to_bytes(4, "little")
            + b"\x00\x00\x00\x00" + b"\x00\x00"
        )
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(body)))
        )
        function = self._function(self.commands)

        # Act
        response = await function.execute()

        # Assert
        self.assertEqual(response[1][0].error_code, 1796)


if __name__ == "__main__":
    unittest.main()
