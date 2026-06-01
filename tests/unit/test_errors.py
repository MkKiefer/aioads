"""Unit tests for aioads.errors."""

import unittest

from aioads.ads_error_codes import AdsErrorCode
from aioads.errors import AdsAmsHeaderError


class TestAdsAmsHeaderError(unittest.TestCase):

    def setUp(self) -> None:
        self.error = AdsAmsHeaderError(AdsErrorCode(1808))

    def test_is_an_exception(self) -> None:
        # Assert
        self.assertIsInstance(self.error, Exception)

    def test_stores_error_code(self) -> None:
        # Assert
        self.assertEqual(self.error.error_code, 1808)

    def test_description_contains_code_and_text(self) -> None:
        # Act
        result = self.error.description()

        # Assert
        self.assertIn("1808", result)
        self.assertIn("symbol not found", result)

    def test_str_returns_description(self) -> None:
        # Act / Assert
        self.assertEqual(str(self.error), self.error.description())

    def test_repr_contains_class_name_and_description(self) -> None:
        # Act
        result = repr(self.error)

        # Assert
        self.assertIn("AdsAmsHeaderError", result)
        self.assertIn("symbol not found", result)

    def test_can_be_raised_and_caught(self) -> None:
        # Act / Assert
        with self.assertRaises(AdsAmsHeaderError) as ctx:
            raise AdsAmsHeaderError(AdsErrorCode(7))

        self.assertEqual(ctx.exception.error_code, 7)


if __name__ == "__main__":
    unittest.main()
