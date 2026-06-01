"""Unit tests for aioads.functions.ads_symbol_datatype_by_name."""

import unittest
from unittest.mock import AsyncMock

from aioads.functions.ads_symbol_datatype_by_name import (
    AdsDatatypeArrayInfo,
    AdsSymbolDataTypeByName,
    SymbolDataTypeResponse,
)
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


def make_datatype(
    name: str = "MyStruct",
    array=None,
    sub_items=None,
) -> SymbolDataTypeResponse:
    """Build a SymbolDataTypeResponse fixture."""
    return SymbolDataTypeResponse(
        version=1,
        hash_value=0xAABBCCDD,
        type_hash_value=0x11223344,
        size=8,
        offs=0,
        data_type=AdsSymbolDataType.BIGTYPE,
        flags=AdsSymbolFlags.PERSISTENT,
        name=name,
        type_name="",
        comment="a comment",
        array=array if array is not None else [],
        sub_items=sub_items if sub_items is not None else [],
    )


class TestAdsDatatypeArrayInfo(unittest.TestCase):

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AdsDatatypeArrayInfo(l_bound=0, e_elements=10)

        # Act
        restored = AdsDatatypeArrayInfo.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)


class TestSymbolDataTypeResponse(unittest.TestCase):

    def test_flat_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = make_datatype()

        # Act
        restored = SymbolDataTypeResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_serialize_with_array_round_trips(self) -> None:
        # Arrange
        original = make_datatype(array=[AdsDatatypeArrayInfo(0, 5)])

        # Act
        restored = SymbolDataTypeResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored.array, [AdsDatatypeArrayInfo(0, 5)])

    def test_serialize_with_nested_sub_items_round_trips(self) -> None:
        # Arrange
        child = make_datatype(name="Field1")
        original = make_datatype(name="Parent", sub_items=[child])

        # Act
        restored = SymbolDataTypeResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_advances_to_end_of_entry(self) -> None:
        # Arrange
        serialized = make_datatype().serialize()
        stream = make_stream(serialized)

        # Act
        SymbolDataTypeResponse.deserialize(stream)

        # Assert
        self.assertEqual(stream.tell(), len(serialized))


class TestAdsSymbolDataTypeByName(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = AdsSymbolDataTypeByName(
            transport=self.transport,
            ams_address=make_ams_address(),
            datatype_name="MyStruct",
        )

    def test_serialize_null_terminates_datatype_name(self) -> None:
        # Act
        result = self.function.serialize()

        # Assert
        self.assertEqual(result, b"MyStruct\x00")

    async def test_execute_returns_deserialized_datatype(self) -> None:
        # Arrange
        datatype = make_datatype()
        payload = make_read_payload(datatype.serialize())
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        result = await self.function.execute()

        # Assert
        self.assertEqual(result, datatype)


if __name__ == "__main__":
    unittest.main()
