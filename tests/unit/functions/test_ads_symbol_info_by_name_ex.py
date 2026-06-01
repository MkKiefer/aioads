"""Unit tests for aioads.functions.ads_symbol_info_by_name_ex."""

import unittest
from unittest.mock import AsyncMock

from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
    SymbolInfoByNameEx,
    SymbolInfoByNameExSumRead,
)
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


def make_symbol_info(symbol_name: str = "MAIN.fbExample") -> SymbolInfo:
    """Build a representative SymbolInfo fixture."""
    return SymbolInfo(
        idx_group=0x4020,
        idx_offset=16,
        idx_length=4,
        data_type=AdsSymbolDataType.INT32,
        symbol_flags=AdsSymbolFlags.READONLY,
        symbol_name=symbol_name,
        type_name="DINT",
        comment="example comment",
    )


class TestSymbolInfo(unittest.TestCase):

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = make_symbol_info()

        # Act
        restored = SymbolInfo.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_advances_to_end_of_entry(self) -> None:
        # Arrange
        serialized = make_symbol_info().serialize()
        stream = make_stream(serialized)

        # Act
        SymbolInfo.deserialize(stream)

        # Assert
        self.assertEqual(stream.tell(), len(serialized))

    def test_deserialize_preserves_data_type_enum(self) -> None:
        # Arrange
        original = make_symbol_info()

        # Act
        restored = SymbolInfo.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertIs(restored.data_type, AdsSymbolDataType.INT32)


class TestSymbolInfoByNameEx(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = SymbolInfoByNameEx(
            transport=self.transport,
            ams_address=make_ams_address(),
            symbol_name="main.value",
        )

    def test_serialize_uppercases_and_null_terminates_name(self) -> None:
        # Act
        result = self.function.serialize()

        # Assert
        self.assertEqual(result, b"MAIN.VALUE\x00")

    async def test_execute_returns_deserialized_symbol_info(self) -> None:
        # Arrange
        symbol_info = make_symbol_info("MAIN.VALUE")
        payload = make_read_payload(symbol_info.serialize())
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        result = await self.function.execute()

        # Assert
        self.assertEqual(result, symbol_info)


class TestSymbolInfoByNameExSumRead(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = SymbolInfoByNameExSumRead(
            transport=self.transport,
            ams_address=make_ams_address(),
            symbol_names=["main.a", "main.b"],
        )

    def test_create_sub_commands_one_per_symbol_name(self) -> None:
        # Act
        commands = self.function.create_sub_commands()

        # Assert
        self.assertEqual(len(commands), 2)

    def test_create_sub_commands_uppercase_null_terminated_payload(self) -> None:
        # Act
        commands = self.function.create_sub_commands()

        # Assert
        self.assertEqual(commands[0].payload, b"MAIN.A\x00")

    async def test_execute_returns_error_code_and_symbol_info_per_entry(self) -> None:
        # Arrange: sum-read-write response = N error codes, N lengths, then payloads
        info_a = make_symbol_info("MAIN.A")
        info_b = make_symbol_info("MAIN.B")
        body_a = info_a.serialize()
        body_b = info_b.serialize()
        sum_body = (
            (0).to_bytes(4, "little") + len(body_a).to_bytes(4, "little")
            + (0).to_bytes(4, "little") + len(body_b).to_bytes(4, "little")
            + body_a + body_b
        )
        payload = make_read_payload(sum_body)
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        results = await self.function.execute()

        # Assert
        self.assertEqual([info for _, info in results], [info_a, info_b])


if __name__ == "__main__":
    unittest.main()
