"""Unit tests for aioads.ams_tcp_header."""

import unittest

from aioads.ams_tcp_header import AmsTcpHeader


class TestAmsTcpHeader(unittest.TestCase):

    def test_serialize_packs_preamble_and_length(self) -> None:
        # Arrange
        header = AmsTcpHeader(length=38)

        # Act
        result = header.serialize()

        # Assert
        self.assertEqual(result, (0x0000).to_bytes(2, "little") + (38).to_bytes(4, "little"))

    def test_serialize_produces_fixed_size_bytes(self) -> None:
        # Arrange
        header = AmsTcpHeader(length=1024)

        # Act
        result = header.serialize()

        # Assert
        self.assertEqual(len(result), AmsTcpHeader.FIXED_SIZE)

    def test_deserialize_valid_preamble_returns_length(self) -> None:
        # Arrange
        data = (0x0000).to_bytes(2, "little") + (256).to_bytes(4, "little")

        # Act
        header = AmsTcpHeader.deserialize(data)

        # Assert
        self.assertEqual(header.length, 256)

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AmsTcpHeader(length=4096)

        # Act
        restored = AmsTcpHeader.deserialize(original.serialize())

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_invalid_preamble_raises_value_error(self) -> None:
        # Arrange
        data = (0x1234).to_bytes(2, "little") + (10).to_bytes(4, "little")

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            AmsTcpHeader.deserialize(data)

        self.assertIn("Invalid preamble", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
