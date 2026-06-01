"""Unit tests for aioads.functions.ads_function."""

import unittest

from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction


class TestAdsFunctionSymbolGroup(unittest.TestCase):

    def test_get_info_by_name_has_expected_group(self) -> None:
        # Assert
        self.assertEqual(AdsFunctionSymbolGroup.GET_INFO_BY_NAME, 0xF009)

    def test_sum_read_write_has_expected_group(self) -> None:
        # Assert
        self.assertEqual(AdsFunctionSymbolGroup.ADSIGRP_SUM_READ_WRITE, 0xF082)

    def test_toggle_route_enable_has_expected_group(self) -> None:
        # Assert
        self.assertEqual(AdsFunctionSymbolGroup.ADSIGRP_TOGGLE_ROUTE_ENABLE, 0x328)


class TestIAdsFunction(unittest.TestCase):

    def test_cannot_instantiate_abstract_interface(self) -> None:
        # Act / Assert
        with self.assertRaises(TypeError):
            IAdsFunction()  # type: ignore[abstract]


if __name__ == "__main__":
    unittest.main()
