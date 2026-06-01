"""Unit tests for aioads.functions.ads_symbol_table_version."""

import unittest
from unittest.mock import AsyncMock

from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_symbol_table_version import SymbolTableVersion
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


class TestSymbolTableVersion(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = SymbolTableVersion(
            transport=self.transport,
            ams_address=make_ams_address(),
        )

    def test_serialize_returns_empty_payload(self) -> None:
        # Act / Assert
        self.assertEqual(self.function.serialize(), b"")

    async def test_execute_returns_single_byte_version(self) -> None:
        # Arrange
        payload = make_read_payload(b"\x05")
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        version = await self.function.execute()

        # Assert
        self.assertEqual(version, 5)

    async def test_execute_reads_from_symbol_table_version_group(self) -> None:
        # Arrange
        payload = make_read_payload(b"\x01")
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.function.execute()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"].name,
            "READ",
        )

    async def test_execute_command_error_raises_command_error(self) -> None:
        # Arrange: the inner read command reports a device error
        payload = make_read_payload(b"", error_code=1793)
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.function.execute()

        self.assertEqual(ctx.exception.error_code, 1793)


if __name__ == "__main__":
    unittest.main()
