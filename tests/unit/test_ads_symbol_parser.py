"""Unit tests for aioads.ads_symbol_parser."""

import struct
import unittest

from parameterized import parameterized

from aioads.ads_symbol_parser import AdsSymbolParser, PrimitiveTypeParser
from aioads.functions.ads_symbol_datatype_by_name import SymbolDataTypeResponse
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
)
from tests.builders import make_stream


def make_datatype(
    name: str,
    *,
    size: int,
    data_type: AdsSymbolDataType = AdsSymbolDataType.BIGTYPE,
    type_name: str = "",
    offs: int = 0,
    array=None,
    sub_items=None,
) -> SymbolDataTypeResponse:
    """Build a SymbolDataTypeResponse for populating the parser type lookup."""
    return SymbolDataTypeResponse(
        version=1,
        hash_value=0,
        type_hash_value=0,
        size=size,
        offs=offs,
        data_type=data_type,
        flags=AdsSymbolFlags(0),
        name=name,
        type_name=type_name,
        comment="",
        array=array if array is not None else [],
        sub_items=sub_items if sub_items is not None else [],
    )


class TestPrimitiveTypeParser(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = PrimitiveTypeParser()

    def test_parse_int32_returns_signed_integer(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<i", -5))

        # Act
        result = self.parser.parse(AdsSymbolDataType.INT32, "DINT", stream)

        # Assert
        self.assertEqual(result, -5)

    def test_parse_bool_returns_true(self) -> None:
        # Arrange
        stream = make_stream(b"\x01")

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIT, "BOOL", stream)

        # Assert
        self.assertTrue(result)

    def test_parse_real32_returns_float(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<f", 1.5))

        # Act
        result = self.parser.parse(AdsSymbolDataType.REAL32, "REAL", stream)

        # Assert
        self.assertAlmostEqual(result, 1.5, places=5)

    def test_parse_void_returns_none(self) -> None:
        # Arrange
        stream = make_stream(b"")

        # Act
        result = self.parser.parse(AdsSymbolDataType.VOID, "", stream)

        # Assert
        self.assertIsNone(result)

    def test_parse_string_stops_at_null_terminator(self) -> None:
        # Arrange
        stream = make_stream(b"Hi\x00\x00\x00\x00")

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.STRING, "STRING(5)", stream)

        # Assert
        self.assertEqual(result, "Hi")

    def test_parse_string_invalid_length_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"abc")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.parser.parse(AdsSymbolDataType.STRING, "STRING(x)", stream)

        self.assertIn("Invalid string type name", str(ctx.exception))

    def test_parse_wstring_decodes_utf16(self) -> None:
        # Arrange
        raw = "Hi".encode("utf-16-le") + b"\x00\x00\x00\x00"
        stream = make_stream(raw)

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.WSTRING, "WSTRING(3)", stream)

        # Assert
        self.assertEqual(result, "Hi")


class TestAdsSymbolParser(unittest.TestCase):

    def setUp(self) -> None:
        self.parser = AdsSymbolParser([])

    def test_parse_primitive_delegates_to_primitive_parser(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<h", 7))

        # Act
        result = self.parser.parse(AdsSymbolDataType.INT16, "INT", stream)

        # Assert
        self.assertEqual(result, 7)

    def test_parse_reference_returns_hex_with_caret(self) -> None:
        # Arrange
        stream = make_stream(bytes(range(8)))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "REFERENCE TO ST_Foo", stream)

        # Assert
        self.assertEqual(result, "0001020304050607^")

    def test_parse_pointer_returns_hex_with_asterisk(self) -> None:
        # Arrange
        stream = make_stream(bytes(range(8)))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "POINTER TO ST_Foo", stream)

        # Assert
        self.assertEqual(result, "0001020304050607*")

    def test_parse_one_dimensional_array_returns_list(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<hhh", 1, 2, 3))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.INT16, "ARRAY [0..2] OF INT16", stream
        )

        # Assert
        self.assertEqual(result, [1, 2, 3])

    def test_parse_two_dimensional_array_returns_nested_list(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<hhhh", 1, 2, 3, 4))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.INT16, "ARRAY [0..1,0..1] OF INT16", stream
        )

        # Assert
        self.assertEqual(result, [[1, 2], [3, 4]])

    def test_parse_unparseable_array_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.parser.parse(AdsSymbolDataType.INT16,
                              "ARRAY OF nonsense", stream)

        self.assertIn("Cannot parse array type", str(ctx.exception))

    def test_parse_const_max_string_reads_fixed_block(self) -> None:
        # Arrange
        raw = b"Hello".ljust(255, b"\x00")
        stream = make_stream(raw)

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "Tc2_System.T_MaxString", stream
        )

        # Assert
        self.assertEqual(result, "Hello")

    def test_parse_struct_with_sub_items_returns_dict(self) -> None:
        # Arrange
        sub_a = make_datatype(
            "a", size=2, data_type=AdsSymbolDataType.INT16, type_name="INT16", offs=0)
        sub_b = make_datatype(
            "b", size=2, data_type=AdsSymbolDataType.INT16, type_name="INT16", offs=2)
        struct_type = make_datatype(
            "ST_Pair", size=4, sub_items=[sub_a, sub_b])
        self.parser.update_datatypes([struct_type])
        stream = make_stream(struct.pack("<hh", 10, 20))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "ST_Pair", stream)

        # Assert
        self.assertEqual(result, {"a": 10, "b": 20})

    def test_parse_struct_unknown_type_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.parser.parse(AdsSymbolDataType.BIGTYPE, "ST_Missing", stream)

        self.assertIn("unknown type name", str(ctx.exception))

    @parameterized.expand([
        ("BOOL", b"\x01", True),
        ("INT", struct.pack("<h", -7), -7),
        ("WORD", struct.pack("<H", 512), 512),
        ("DINT", struct.pack("<i", -100_000), -100_000),
        ("LWORD", struct.pack("<Q", 2**40), 2**40),
        ("LREAL", struct.pack("<d", 2.5), 2.5),
    ])
    def test_parse_bigtype_with_elementary_name_resolves_by_name(
        self, type_name: str, raw: bytes, expected: object
    ) -> None:
        """TwinCAT 2 reports BIGTYPE even for elementary types like INT."""
        # Arrange
        stream = make_stream(raw)

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, type_name, stream)

        # Assert
        self.assertEqual(result, expected)

    def test_parse_time_returns_milliseconds(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<I", 90_500))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, "TIME", stream)

        # Assert
        self.assertEqual(result, 90_500)

    def test_parse_ltime_returns_nanoseconds(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<Q", 1_500_000_000))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, "LTIME", stream)

        # Assert
        self.assertEqual(result, 1_500_000_000)

    def test_parse_time_of_day_returns_milliseconds_since_midnight(self) -> None:
        # Arrange
        milliseconds_since_midnight = (13 * 3600 + 37 * 60 + 5) * 1000 + 250
        stream = make_stream(struct.pack("<I", milliseconds_since_midnight))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, "TOD", stream)

        # Assert
        self.assertEqual(result, milliseconds_since_midnight)

    def test_parse_date_returns_epoch_seconds(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<I", 1_752_364_800))  # 2025-07-13

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, "DATE", stream)

        # Assert
        self.assertEqual(result, 1_752_364_800)

    def test_parse_date_and_time_returns_epoch_seconds(self) -> None:
        # Arrange
        # 2025-07-13T11:45:06Z
        stream = make_stream(struct.pack("<I", 1_752_407_106))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, "DT", stream)

        # Assert
        self.assertEqual(result, 1_752_407_106)

    @parameterized.expand([
        ("LTIME",),
        ("LTOD",),
        ("LTIME_OF_DAY",),
        ("LDATE",),
        ("LDT",),
        ("LDATE_AND_TIME",),
    ])
    def test_parse_long_datetime_types_return_nanoseconds(self, type_name: str) -> None:
        # Arrange
        stream = make_stream(struct.pack("<Q", 1_752_407_106_000_000_000))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, type_name, stream)

        # Assert
        self.assertEqual(result, 1_752_407_106_000_000_000)

    @parameterized.expand([
        ("TIME_OF_DAY",),
        ("DATE_AND_TIME",),
    ])
    def test_parse_long_form_datetime_aliases_return_integer(self, type_name: str) -> None:
        # Arrange
        stream = make_stream(struct.pack("<I", 42_000))

        # Act
        result = self.parser.parse(AdsSymbolDataType.BIGTYPE, type_name, stream)

        # Assert
        self.assertEqual(result, 42_000)

    def test_parse_array_of_time_returns_millisecond_list(self) -> None:
        # Arrange
        stream = make_stream(struct.pack("<II", 1000, 2000))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "ARRAY [0..1] OF TIME", stream
        )

        # Assert
        self.assertEqual(result, [1000, 2000])

    def test_parse_struct_with_time_sub_item_returns_millisecond_field(self) -> None:
        # Arrange
        sub_time = make_datatype("duration", size=4, type_name="TIME", offs=0)
        struct_type = make_datatype("ST_Timed", size=4, sub_items=[sub_time])
        self.parser.update_datatypes([struct_type])
        stream = make_stream(struct.pack("<I", 250))

        # Act
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "ST_Timed", stream)

        # Assert
        self.assertEqual(result, {"duration": 250})

    def test_update_datatypes_registers_new_type_for_lookup(self) -> None:
        # Arrange
        struct_type = make_datatype(
            "ST_Single",
            size=2,
            sub_items=[make_datatype(
                "v", size=2, data_type=AdsSymbolDataType.INT16, type_name="INT16")],
        )

        # Act
        self.parser.update_datatypes([struct_type])
        result = self.parser.parse(
            AdsSymbolDataType.BIGTYPE, "ST_Single", make_stream(
                struct.pack("<h", 42))
        )

        # Assert
        self.assertEqual(result, {"v": 42})


if __name__ == "__main__":
    unittest.main()
