"""Unit tests for aioads.functions.ads_symbol_datatype_upload."""

import unittest
from unittest.mock import AsyncMock

from aioads.functions.ads_symbol_datatype_by_name import SymbolDataTypeResponse
from aioads.functions.ads_symbol_datatype_upload import AdsSymbolDataTypeUpload
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
)
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


def make_datatype(name: str) -> SymbolDataTypeResponse:
    """Build a minimal SymbolDataTypeResponse fixture."""
    return SymbolDataTypeResponse(
        version=1,
        hash_value=0,
        type_hash_value=0,
        size=4,
        offs=0,
        data_type=AdsSymbolDataType.INT32,
        flags=AdsSymbolFlags(0),
        name=name,
        type_name="DINT",
        comment="",
        array=[],
        sub_items=[],
    )


class TestAdsSymbolDataTypeUpload(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.datatypes = [make_datatype("TypeA"), make_datatype("TypeB")]
        self.blob = b"".join(d.serialize() for d in self.datatypes)

    def test_serialize_returns_empty_payload(self) -> None:
        # Arrange
        function = AdsSymbolDataTypeUpload(self.transport, self.address, dt_size=len(self.blob))

        # Act / Assert
        self.assertEqual(function.serialize(), b"")

    async def test_execute_deserializes_all_datatypes(self) -> None:
        # Arrange
        function = AdsSymbolDataTypeUpload(self.transport, self.address, dt_size=len(self.blob))
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(self.blob)))
        )

        # Act
        result = await function.execute()

        # Assert
        self.assertEqual(result, self.datatypes)

    async def test_execute_size_mismatch_raises_value_error(self) -> None:
        # Arrange
        function = AdsSymbolDataTypeUpload(self.transport, self.address, dt_size=len(self.blob) + 1)
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(self.blob)))
        )

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            await function.execute()

        self.assertIn("Expected datatype size", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
