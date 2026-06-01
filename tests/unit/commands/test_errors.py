"""Unit tests for aioads.commands.errors."""

import unittest

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.errors import AdsCommandError


class TestAdsCommandError(unittest.TestCase):

    def setUp(self) -> None:
        self.error = AdsCommandError(AdsErrorCode(1793), "extra context")

    def test_is_an_exception(self) -> None:
        # Assert
        self.assertIsInstance(self.error, Exception)

    def test_stores_error_code(self) -> None:
        # Assert
        self.assertEqual(self.error.error_code, 1793)

    def test_description_contains_code_and_text(self) -> None:
        # Act
        result = self.error.description()

        # Assert
        self.assertIn("1793", result)
        self.assertIn("Service is not supported by server", result)

    def test_description_contains_extra_args(self) -> None:
        # Act
        result = self.error.description()

        # Assert
        self.assertIn("extra context", result)

    def test_str_returns_description(self) -> None:
        # Act / Assert
        self.assertEqual(str(self.error), self.error.description())

    def test_repr_contains_class_name(self) -> None:
        # Act
        result = repr(self.error)

        # Assert
        self.assertIn("AdsCommandError", result)


if __name__ == "__main__":
    unittest.main()
