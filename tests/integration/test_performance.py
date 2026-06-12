"""
Integration performance tests against fixed time budgets.

The budgets below are not benchmarks: they are generous upper bounds tuned to
pass under the worst expected conditions (transport, network latency, PLC
load). A failure means a pathological slowdown, not a small regression.
Adjust the constants below when they no longer match the test environment.
"""
import time

from tests.integration.base import IntegrationTestCase

# Number of sequential single-symbol reads and the time budget for all of them.
SINGLE_READ_COUNT = 500
SINGLE_READ_MAX_SECONDS = 29.0

# Number of sequential multi-symbol reads and the time budget for all of them.
MULTI_READ_COUNT = 100
MULTI_READ_MAX_SECONDS = 7.0


class TestPerformance(IntegrationTestCase):
    """Read repeatedly from the configured endpoint and check the time budgets."""

    async def test_sequential_single_reads(self) -> None:
        symbol_name = self.config["symbols"]["single"]
        start = time.perf_counter()
        for _ in range(SINGLE_READ_COUNT):
            await self.client.read_symbol_by_name(symbol_name)
        duration = time.perf_counter() - start
        self.assertLessEqual(
            duration,
            SINGLE_READ_MAX_SECONDS,
            f"{SINGLE_READ_COUNT} single reads took {duration:.2f}s "
            f"({SINGLE_READ_COUNT / duration:.1f} reads/s)",
        )

    async def test_sequential_multi_reads(self) -> None:
        symbol_names = set(self.config["symbols"]["multiple"])
        start = time.perf_counter()
        for _ in range(MULTI_READ_COUNT):
            await self.client.read_symbols_by_names(symbol_names)
        duration = time.perf_counter() - start
        self.assertLessEqual(
            duration,
            MULTI_READ_MAX_SECONDS,
            f"{MULTI_READ_COUNT} multi reads took {duration:.2f}s "
            f"({MULTI_READ_COUNT / duration:.1f} reads/s)",
        )
