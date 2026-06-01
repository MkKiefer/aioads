"""Unit tests for aioads.functions.ads_sum_read."""

import unittest
from unittest.mock import AsyncMock

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

    def _function(self, commands) -> AdsSumRead:
        return AdsSumRead(
            transport=self.transport, ams_address=self.address, commands=commands
        )

    def test_serialize_concatenates_each_command_payload(self) -> None:
        # Arrange
        function = self._function(self.commands)

        # Act
        result = function.serialize()

        # Assert
        self.assertEqual(
            result, self.commands[0].serialize() + self.commands[1].serialize()
        )

    async def test_execute_empty_commands_raises_value_error(self) -> None:
        # Arrange
        function = self._function([])

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("At least one command", str(ctx.exception))

    async def test_execute_too_many_commands_raises_value_error(self) -> None:
        # Arrange
        many = [AdsReadCommand(self.transport, self.address, 0, 0, 1) for _ in range(501)]
        function = self._function(many)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("maximum is 500", str(ctx.exception))

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
