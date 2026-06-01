"""Unit tests for aioads.commands.ads_command."""

import unittest

from aioads.commands.ads_command import (
    AdsCommandId,
    AdsCommandState,
    ICommand,
)


class TestAdsCommandId(unittest.TestCase):

    def test_read_has_protocol_value_two(self) -> None:
        # Assert
        self.assertEqual(AdsCommandId.READ, 2)

    def test_read_write_has_protocol_value_nine(self) -> None:
        # Assert
        self.assertEqual(AdsCommandId.READ_WRITE, 9)

    def test_construct_from_int_returns_enum_member(self) -> None:
        # Act
        result = AdsCommandId(4)

        # Assert
        self.assertEqual(result, AdsCommandId.READ_STATE)


class TestAdsCommandState(unittest.TestCase):

    def test_request_and_command_combine_into_flag(self) -> None:
        # Act
        combined = AdsCommandState.ADS_COMMAND | AdsCommandState.ADS_REQUEST

        # Assert
        self.assertEqual(int(combined), 0x0004)

    def test_response_flag_membership_in_combined_value(self) -> None:
        # Arrange
        combined = AdsCommandState.ADS_RESPONSE | AdsCommandState.ADS_COMMAND

        # Act / Assert
        self.assertIn(AdsCommandState.ADS_RESPONSE, combined)

    def test_response_flag_absent_from_request_only_value(self) -> None:
        # Arrange
        value = AdsCommandState.ADS_COMMAND

        # Act / Assert
        self.assertNotIn(AdsCommandState.ADS_RESPONSE, value)


class TestICommand(unittest.TestCase):

    def test_cannot_instantiate_abstract_interface(self) -> None:
        # Act / Assert
        with self.assertRaises(TypeError):
            ICommand()  # type: ignore[abstract]


if __name__ == "__main__":
    unittest.main()
