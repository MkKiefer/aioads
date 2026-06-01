"""Unit tests for aioads.stream."""

import unittest
from struct import Struct

from aioads.stream import AdsStream


class TestAdsStream(unittest.TestCase):

    def setUp(self) -> None:
        self.data = bytes(range(16))  # 0x00..0x0f
        self.stream = AdsStream(memoryview(self.data))

    def test_length_returns_total_byte_count(self) -> None:
        # Act
        result = self.stream.length

        # Assert
        self.assertEqual(result, 16)

    def test_tell_on_fresh_stream_returns_zero(self) -> None:
        # Act
        result = self.stream.tell()

        # Assert
        self.assertEqual(result, 0)

    def test_read_returns_requested_bytes_and_advances(self) -> None:
        # Act
        chunk = self.stream.read(4)

        # Assert
        self.assertEqual(chunk, bytes([0, 1, 2, 3]))
        self.assertEqual(self.stream.tell(), 4)

    def test_read_returns_bytes_instance_not_memoryview(self) -> None:
        # Act
        chunk = self.stream.read(2)

        # Assert
        self.assertIsInstance(chunk, bytes)

    def test_read_view_returns_memoryview_and_advances(self) -> None:
        # Act
        view = self.stream.read_view(3)

        # Assert
        self.assertIsInstance(view, memoryview)
        self.assertEqual(view.tobytes(), bytes([0, 1, 2]))
        self.assertEqual(self.stream.tell(), 3)

    def test_read_struct_unpacks_and_advances_by_struct_size(self) -> None:
        # Arrange
        struct_def = Struct("<I")

        # Act
        (value,) = self.stream.read_struct(struct_def)

        # Assert
        self.assertEqual(value, 0x03020100)
        self.assertEqual(self.stream.tell(), 4)

    def test_seek_sets_position(self) -> None:
        # Act
        self.stream.seek(8)

        # Assert
        self.assertEqual(self.stream.tell(), 8)

    def test_seek_to_length_is_allowed(self) -> None:
        # Act
        self.stream.seek(16)

        # Assert
        self.assertEqual(self.stream.tell(), 16)

    def test_seek_negative_position_raises_value_error(self) -> None:
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.stream.seek(-1)

        self.assertIn("out of bounds", str(ctx.exception))

    def test_seek_past_length_raises_value_error(self) -> None:
        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.stream.seek(17)

        self.assertIn("out of bounds", str(ctx.exception))

    def test_sub_stream_shares_data_from_current_position(self) -> None:
        # Arrange
        self.stream.seek(4)

        # Act
        sub = self.stream.sub_stream(4)

        # Assert
        self.assertEqual(sub.length, 4)
        self.assertEqual(sub.read(4), bytes([4, 5, 6, 7]))

    def test_sub_stream_advances_parent_position(self) -> None:
        # Act
        self.stream.sub_stream(5)

        # Assert
        self.assertEqual(self.stream.tell(), 5)

    def test_sub_stream_exceeding_available_data_raises_value_error(self) -> None:
        # Arrange
        self.stream.seek(14)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            self.stream.sub_stream(4)

        self.assertIn("exceeds available data", str(ctx.exception))

    def test_sub_stream_has_independent_position(self) -> None:
        # Arrange
        sub = self.stream.sub_stream(8)

        # Act
        sub.read(2)

        # Assert
        self.assertEqual(sub.tell(), 2)
        self.assertEqual(self.stream.tell(), 8)


if __name__ == "__main__":
    unittest.main()
