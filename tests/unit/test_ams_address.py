"""Unit tests for aioads.ams_address."""

import unittest

from aioads.ams_address import AmsAddress


class TestAmsAddress(unittest.TestCase):

    def test_serialize_packs_net_id_octets_and_port_little_endian(self) -> None:
        # Arrange
        address = AmsAddress(net_id="1.2.3.4.5.6", port=851)

        # Act
        result = address.serialize()

        # Assert
        self.assertEqual(result, bytes([1, 2, 3, 4, 5, 6]) + (851).to_bytes(2, "little"))

    def test_serialize_produces_eight_bytes(self) -> None:
        # Arrange
        address = AmsAddress(net_id="192.168.0.1.1.1", port=48898)

        # Act
        result = address.serialize()

        # Assert
        self.assertEqual(len(result), 8)

    def test_deserialize_reconstructs_net_id_and_port(self) -> None:
        # Arrange
        data = bytes([10, 20, 30, 40, 50, 60]) + (502).to_bytes(2, "little")

        # Act
        address = AmsAddress.deserialize(data)

        # Assert
        self.assertEqual(address.net_id, "10.20.30.40.50.60")
        self.assertEqual(address.port, 502)

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AmsAddress(net_id="127.0.0.1.1.1", port=10000)

        # Act
        restored = AmsAddress.deserialize(original.serialize())

        # Assert
        self.assertEqual(restored, original)

    def test_is_frozen_and_rejects_attribute_assignment(self) -> None:
        # Arrange
        address = AmsAddress(net_id="1.1.1.1.1.1", port=1)

        # Act / Assert
        with self.assertRaises(Exception):
            address.port = 2  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
