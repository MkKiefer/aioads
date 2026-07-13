"""
ADS Symbol Parser Module
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import contextmanager
import re
import struct
from typing import Any
from aioads.functions.ads_symbol_datatype_by_name import SymbolDataTypeResponse
from aioads.functions.ads_symbol_info_by_name_ex import AdsSymbolDataType
from aioads.stream import AdsStream


@contextmanager
def debug_cursor(raw_data: AdsStream, expected_consumed: int):
    """Debugging tool to verify bytes consumed from stream cursor."""
    start_pos = raw_data.tell()
    yield
    end_pos = raw_data.tell()
    consumed = end_pos - start_pos
    if consumed != expected_consumed:
        print("Place your debugger here!")


class ISymbolParser(ABC):
    """
    The interface for all symbol parsers.
    This interface should allow to easily swap between different parser implementations 
    """

    @abstractmethod
    def parse(
        self, data_type: AdsSymbolDataType, type_name: str, raw_data: AdsStream
    ) -> Any:
        """Parse the value for a specific datatype and type name"""
        raise NotImplementedError

    @abstractmethod
    def update_datatypes(self, data_types: list[SymbolDataTypeResponse]) -> None:
        """Updates the list of remote datatypes"""
        raise NotImplementedError


class PrimitiveTypeParser(ISymbolParser):
    """
    The simplest parser of them all, it can only parse simple primitive types.
    """

    def __init__(self) -> None:
        self._struct_def: dict[
            AdsSymbolDataType, struct.Struct
        ] = {
            AdsSymbolDataType.BIT: struct.Struct("<?"),
            AdsSymbolDataType.INT8: struct.Struct("<b"),
            AdsSymbolDataType.UINT8: struct.Struct("<B"),
            AdsSymbolDataType.INT16: struct.Struct("<h"),
            AdsSymbolDataType.UINT16: struct.Struct("<H"),
            AdsSymbolDataType.INT32: struct.Struct("<i"),
            AdsSymbolDataType.UINT32: struct.Struct("<I"),
            AdsSymbolDataType.INT64: struct.Struct("<q"),
            AdsSymbolDataType.UINT64: struct.Struct("<Q"),
            AdsSymbolDataType.REAL32: struct.Struct("<f"),
            AdsSymbolDataType.REAL64: struct.Struct("<d")
        }

    def parse(self, data_type: AdsSymbolDataType, type_name: str,  raw_data: AdsStream) -> Any:
        """Parse a primitive data type."""
        if data_type == AdsSymbolDataType.VOID:
            return None
        if data_type == AdsSymbolDataType.STRING:
            return self._parse_string(type_name, raw_data)
        if data_type == AdsSymbolDataType.WSTRING:
            return self._parse_wstring(type_name, raw_data)

        struct_def = self._struct_def[data_type]
        return raw_data.read_struct(struct_def)[0]

    def update_datatypes(self, data_types: list[SymbolDataTypeResponse]) -> None:
        pass

    def _parse_string(self, type_name: str, raw_data: AdsStream) -> str | None:
        """
        The type_name is expected to be always in the format STRING(n) where n is the length of the string.
        Example: STRING(80)
        """
        # STRING = 7 chars cut off "STRING(" and ")"
        length = type_name[7:-1]
        if not length.isdigit():
            raise ValueError(
                f"Invalid string type name: {type_name} expected {length} to be a number"
            )
        raw_bytes = raw_data.read(int(length) + 1)  # +1 for null terminator)
        terminator = raw_bytes.find(b"\x00")
        if terminator == -1:
            return raw_bytes.decode("cp1252")
        return raw_bytes[:terminator].decode("cp1252")

    def _parse_wstring(self, type_name: str, raw_data: AdsStream) -> str | None:
        """
        The type_name is expected to be always in the format WSTRING(n) where n is the length of the string.
        Example: WSTRING(80)
        """
        # WSTRING = 8 chars cut off "WSTRING(" and ")"
        length = type_name[8:-1]
        if not length.isdigit():
            raise ValueError(
                f"Invalid wstring type name: {type_name} expected {length} to be a number"
            )
        str_length = int(length) * 2  # each char is 2 bytes
        raw_bytes = raw_data.read(str_length + 2)
        val_frag = raw_bytes.split(b"\x00\x00")[0]
        if len(val_frag) % 2 != 0:
            val_frag += b"\x00"
        return val_frag.decode("utf-16-le")


class AdsSymbolParser(ISymbolParser):
    """
    Symbol parser for complex structures like:
    - structs
    - arrays
    """

    _regex_array_1D = re.compile(r"ARRAY \[(-?\d+)..(-?\d+)\] OF (.*)")
    _regex_array_2D = re.compile(
        r"ARRAY \[(-?\d+)..(-?\d+)\,(-?\d+)..(-?\d+)\] OF (.*)"
    )

    REF = "REFERENCE TO"
    PTR = "POINTER TO"
    ARR = "ARRAY"

    def __init__(self, data_types: list[SymbolDataTypeResponse]):
        self._type_lookup = {dt.name: dt for dt in data_types}
        # Resolution of IEC 61131-3 elementary type names to their wire encoding.
        self._elementary_type_names: dict[str, AdsSymbolDataType] = {
            "BOOL": AdsSymbolDataType.BIT,
            "BIT": AdsSymbolDataType.BIT,
            "SINT": AdsSymbolDataType.INT8,
            "USINT": AdsSymbolDataType.UINT8,
            "BYTE": AdsSymbolDataType.UINT8,
            "INT": AdsSymbolDataType.INT16,
            "UINT": AdsSymbolDataType.UINT16,
            "WORD": AdsSymbolDataType.UINT16,
            "DINT": AdsSymbolDataType.INT32,
            "UDINT": AdsSymbolDataType.UINT32,
            "DWORD": AdsSymbolDataType.UINT32,
            "LINT": AdsSymbolDataType.INT64,
            "ULINT": AdsSymbolDataType.UINT64,
            "LWORD": AdsSymbolDataType.UINT64,
            "REAL": AdsSymbolDataType.REAL32,
            "LREAL": AdsSymbolDataType.REAL64,
            "TIME": AdsSymbolDataType.UINT32,
            "TOD": AdsSymbolDataType.UINT32,
            "TIME_OF_DAY": AdsSymbolDataType.UINT32,
            "DATE": AdsSymbolDataType.UINT32,
            "DT": AdsSymbolDataType.UINT32,
            "DATE_AND_TIME": AdsSymbolDataType.UINT32,
            "LTIME": AdsSymbolDataType.UINT64,
            "LTOD": AdsSymbolDataType.UINT64,
            "LTIME_OF_DAY": AdsSymbolDataType.UINT64,
            "LDATE": AdsSymbolDataType.UINT64,
            "LDT": AdsSymbolDataType.UINT64,
            "LDATE_AND_TIME": AdsSymbolDataType.UINT64,
        }
        self._const_type_names: dict[str, Callable[[str, AdsStream], Any]] = {
            "Tc2_System.T_MaxString": lambda type_name, raw_data: raw_data.read(255)
            .decode("latin-1")
            .rstrip("\x00"),
            "Tc2_System.T_AmsNetID": lambda type_name, raw_data: raw_data.read(26)
            .decode("latin-1")
            .rstrip("\x00"),
        }

        self._primitive_parser = PrimitiveTypeParser()

    def update_datatypes(self, data_types: list[SymbolDataTypeResponse]) -> None:
        """
        Update the internal datatype lookup with new data types.
        """
        updated_type_map = {dt.name: dt for dt in data_types}
        self._type_lookup.update(updated_type_map)

    def parse_array(
        self, data_type: AdsSymbolDataType, type_name: str, raw_data: AdsStream
    ) -> list[Any]:
        """Parse an array data type."""
        match_1d = self._regex_array_1D.match(type_name)
        if match_1d:
            lower_bound = int(match_1d.group(1))
            upper_bound = int(match_1d.group(2))
            element_type = match_1d.group(3).strip()
            array_values = []
            for _ in range(lower_bound, upper_bound + 1):
                array_values.append(self.parse(
                    data_type, element_type, raw_data))
            return array_values
        match_2d = self._regex_array_2D.match(type_name)
        if match_2d:
            lower_bound_1 = int(match_2d.group(1))
            upper_bound_1 = int(match_2d.group(2))
            lower_bound_2 = int(match_2d.group(3))
            upper_bound_2 = int(match_2d.group(4))
            element_type = match_2d.group(5).strip()
            array_values = []
            for _ in range(lower_bound_1, upper_bound_1 + 1):
                dimension_values = []
                for _ in range(lower_bound_2, upper_bound_2 + 1):
                    dimension_values.append(
                        self.parse(data_type, element_type, raw_data)
                    )
                array_values.append(dimension_values)
            return array_values

        raise ValueError(f"Cannot parse array type: {type_name}")

    def parse_struct(
        self, type_name: str, raw_data: AdsStream
    ) -> dict[str, Any] | list[Any] | str:
        """
        Parse a structured data type (struct).
        """
        type_info = self._type_lookup.get(type_name)

        if not type_info:
            raise ValueError(
                f"Failed to parse struct with unknown type name: {type_name}"
            )

        struct_origin = raw_data.tell()

        # Parse fields inside the struct
        if type_info.sub_items:
            # structured type
            sub_item_values: dict[str, Any] = {}
            for sub_item in type_info.sub_items:
                # sometimes we have a gap in the struct fields (This ensures correct alignment)
                raw_data.seek(struct_origin + sub_item.offs)
                sub_item_values[sub_item.name] = self.parse(
                    sub_item.data_type, sub_item.type_name, raw_data
                )
            # Ensure we move the cursor to the end of the struct
            raw_data.seek(struct_origin + type_info.size)
            return sub_item_values

        if type_info.array:
            # array type
            array_values = []
            for dimensions in type_info.array:
                dimension_values = []
                for _ in range(dimensions.e_elements):
                    dimension_values.append(
                        self.parse(type_info.data_type,
                                   type_info.type_name, raw_data)
                    )
                array_values.append(dimension_values)
            if len(array_values) == 1:
                return array_values[0]
            raw_data.seek(struct_origin + type_info.size)
            return array_values

        # Ensure we always consume the full struct size (We may have then consumed a pointer or something like that)
        unknown_struct_data = raw_data.read(type_info.size)

        #! If we have no type_name this is likely a indicator for a recursive struct
        if type_info.type_name == "":
            return unknown_struct_data.hex()

        return self.parse(type_info.data_type, type_info.type_name, raw_data)

    def parse(
        self, data_type: AdsSymbolDataType, type_name: str, raw_data: AdsStream
    ) -> Any:
        """
        Parse raw data based on the provided data type and type name.
        """

        # Parse of constant types
        if type_name in self._const_type_names:
            # ?: Are this really constant types?
            val = self._const_type_names[type_name](type_name, raw_data)
            return val

        if type_name.startswith(self.REF):
            return f"{raw_data.read(8).hex()}^"

        if type_name.startswith(self.PTR):
            return f"{raw_data.read(8).hex()}*"

        # Parsing of basetype arrays
        if type_name.startswith(self.ARR):
            return self.parse_array(data_type, type_name, raw_data)

        # Parsing of basetype values
        if data_type != AdsSymbolDataType.BIGTYPE:
            return self._primitive_parser.parse(data_type, type_name, raw_data)

        # TwinCAT 2 reports BIGTYPE for some elementary types
        # like INT, UINT, REAL, etc. So we need to resolve them by name.
        if type_name in self._elementary_type_names:
            return self._primitive_parser.parse(
                self._elementary_type_names[type_name], type_name, raw_data
            )

        # Parsing of structured types
        if data_type == AdsSymbolDataType.BIGTYPE:
            return self.parse_struct(type_name, raw_data)

        raise ValueError(
            f"Cannot parse data type: {data_type.name} / {type_name}")
