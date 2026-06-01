"""Unit tests for aioads.ads_error_codes."""

import unittest

from aioads.ads_error_codes import AdsErrorCode, AdsErrorCodes


class TestAdsErrorCodes(unittest.TestCase):

    def test_description_known_code_returns_mapped_text(self) -> None:
        # Act
        result = AdsErrorCodes.description(1808)

        # Assert
        self.assertEqual(result, "symbol not found")

    def test_description_zero_returns_no_error(self) -> None:
        # Act
        result = AdsErrorCodes.description(0)

        # Assert
        self.assertEqual(result, "no error")

    def test_description_unknown_code_returns_fallback(self) -> None:
        # Act
        result = AdsErrorCodes.description(999999)

        # Assert
        self.assertEqual(result, "Unknown error code")


class TestAdsErrorCode(unittest.TestCase):

    def test_is_subclass_of_int(self) -> None:
        # Arrange
        code = AdsErrorCode(7)

        # Assert
        self.assertIsInstance(code, int)

    def test_description_returns_text_for_underlying_value(self) -> None:
        # Arrange
        code = AdsErrorCode(6)

        # Act
        result = code.description

        # Assert
        self.assertEqual(result, "target port not found   ADS Server not started")

    def test_ok_is_true_for_zero(self) -> None:
        # Arrange
        code = AdsErrorCode(0)

        # Act / Assert
        self.assertTrue(code.ok)

    def test_ok_is_false_for_nonzero(self) -> None:
        # Arrange
        code = AdsErrorCode(1)

        # Act / Assert
        self.assertFalse(code.ok)

    def test_description_unknown_value_returns_fallback(self) -> None:
        # Arrange
        code = AdsErrorCode(424242)

        # Act
        result = code.description

        # Assert
        self.assertEqual(result, "Unknown error code")


if __name__ == "__main__":
    unittest.main()
