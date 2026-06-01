"""Unit tests for aioads.ams_header."""

import unittest

from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.commands.ads_command import AdsCommandId, AdsCommandState
from aioads.stream import AdsStream


class TestAmsHeader(unittest.TestCase):

    def setUp(self) -> None:
        self.header = AmsHeader(
            target_ams_address=AmsAddress(net_id="1.2.3.4.5.6", port=851),
            source_ams_address=AmsAddress(net_id="6.5.4.3.2.1", port=350),
            command_id=AdsCommandId.READ,
            command_flags=AdsCommandState.ADS_REQUEST | AdsCommandState.ADS_COMMAND,
            command_length=12,
            error_code=AdsErrorCode(0),
            invoke_id=42,
        )

    def test_serialize_produces_fixed_size_bytes(self) -> None:
        # Act
        result = self.header.serialize()

        # Assert
        self.assertEqual(len(result), AmsHeader.FIXED_SIZE)

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        stream = AdsStream(memoryview(self.header.serialize()))

        # Act
        restored = AmsHeader.deserialize(stream)

        # Assert
        self.assertEqual(restored, self.header)

    def test_deserialize_reconstructs_command_id_enum(self) -> None:
        # Arrange
        stream = AdsStream(memoryview(self.header.serialize()))

        # Act
        restored = AmsHeader.deserialize(stream)

        # Assert
        self.assertIsInstance(restored.command_id, AdsCommandId)
        self.assertEqual(restored.command_id, AdsCommandId.READ)

    def test_deserialize_reconstructs_error_code_with_description(self) -> None:
        # Arrange
        header = AmsHeader(
            target_ams_address=AmsAddress(net_id="1.1.1.1.1.1", port=1),
            source_ams_address=AmsAddress(net_id="2.2.2.2.2.2", port=2),
            command_id=AdsCommandId.WRITE,
            command_flags=AdsCommandState.ADS_RESPONSE,
            command_length=0,
            error_code=AdsErrorCode(1808),
            invoke_id=1,
        )
        stream = AdsStream(memoryview(header.serialize()))

        # Act
        restored = AmsHeader.deserialize(stream)

        # Assert
        self.assertEqual(restored.error_code.description, "symbol not found")

    def test_deserialize_advances_stream_by_fixed_size(self) -> None:
        # Arrange
        stream = AdsStream(memoryview(self.header.serialize()))

        # Act
        AmsHeader.deserialize(stream)

        # Assert
        self.assertEqual(stream.tell(), AmsHeader.FIXED_SIZE)


if __name__ == "__main__":
    unittest.main()
