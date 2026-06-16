"""
Integration tests for reading symbols from the configured endpoint.
"""
from tests.integration.base import IntegrationTestCase


class TestReadSymbols(IntegrationTestCase):
    """Read the symbols defined in the `[symbols]` section of the config."""

    async def test_read_symbol_info(self) -> None:
        symbol_name = self.config["symbols"]["single"]
        symbol_info = await self.client.read_symbol_info_by_name(symbol_name)
        self.assertGreater(symbol_info.idx_length, 0)

    async def test_read_single_symbol(self) -> None:
        symbol_name = self.config["symbols"]["single"]
        value = await self.client.read_symbol_by_name(symbol_name)
        self.assertIsNotNone(value)

    async def test_read_multiple_symbols(self) -> None:
        symbol_names = set(self.config["symbols"]["multiple"])
        results = await self.client.read_symbols_by_names(symbol_names)
        self.assertEqual(set(results), symbol_names)
        for symbol_name, result in results.items():
            self.assertTrue(
                result.error_code.ok,
                f"Reading '{symbol_name}' failed: {result.error_code!r}",
            )

    async def test_get_symbols(self) -> None:
        symbols = await self.client.get_symbols()
        self.assertGreater(len(symbols), 0)
