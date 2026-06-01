"""Unit tests for aioads.functions.ads_symbol_upload."""

import unittest
from unittest.mock import AsyncMock

from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
)
from aioads.functions.ads_symbol_upload import AdsSymbolUpload
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


def make_symbol_info(name: str) -> SymbolInfo:
    """Build a minimal SymbolInfo fixture."""
    return SymbolInfo(
        idx_group=0x4020,
        idx_offset=0,
        idx_length=4,
        data_type=AdsSymbolDataType.INT32,
        symbol_flags=AdsSymbolFlags(0),
        symbol_name=name,
        type_name="DINT",
        comment="",
    )


class TestAdsSymbolUpload(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.symbols = [make_symbol_info("MAIN.A"), make_symbol_info("MAIN.B")]
        self.tree = b"".join(s.serialize() for s in self.symbols)

    def test_serialize_returns_empty_payload(self) -> None:
        # Arrange
        function = AdsSymbolUpload(self.transport, self.address, tree_size=len(self.tree))

        # Act / Assert
        self.assertEqual(function.serialize(), b"")

    async def test_execute_deserializes_all_symbols_in_tree(self) -> None:
        # Arrange
        function = AdsSymbolUpload(self.transport, self.address, tree_size=len(self.tree))
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(self.tree)))
        )

        # Act
        result = await function.execute()

        # Assert
        self.assertEqual(result, self.symbols)

    async def test_execute_tree_size_mismatch_raises_value_error(self) -> None:
        # Arrange
        function = AdsSymbolUpload(self.transport, self.address, tree_size=len(self.tree) + 1)
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(self.tree)))
        )

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("Expected tree size", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
