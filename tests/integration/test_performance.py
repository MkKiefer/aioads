"""
Integration performance tests against fixed time budgets.

The budgets below are not benchmarks: they are generous upper bounds tuned to
pass under the worst expected conditions (transport, network latency, PLC
load). A failure means a pathological slowdown, not a small regression.
Adjust the constants below when they no longer match the test environment.
"""
import asyncio
import time

from aioads.ads_notifications import NotificationValue
from aioads.commands.ads_add_notification import TransmissionMode
from tests.integration.base import IntegrationTestCase

# Number of sequential single-symbol reads and the time budget for all of them.
SINGLE_READ_COUNT = 500
SINGLE_READ_MAX_SECONDS = 29.0

# Number of sequential multi-symbol reads and the time budget for all of them.
MULTI_READ_COUNT = 100
MULTI_READ_MAX_SECONDS = 7.0

# Number of notifications to receive, how often the PLC sends one (set this to
# the cycle time of the PLC task so every cycle is sent) and the time budget
# for all of them. Without any delay they take COUNT * CYCLE_TIME (4.0s).
NOTIFICATION_COUNT = 1000
NOTIFICATION_CYCLE_TIME_MS = 4
NOTIFICATION_MAX_SECONDS = 5.0


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
        print(
            f"\n{SINGLE_READ_COUNT} single reads took {duration:.2f}s "
            f"({SINGLE_READ_COUNT / duration:.1f} reads/s), budget {SINGLE_READ_MAX_SECONDS:.2f}s"
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
        print(
            f"\n{MULTI_READ_COUNT} multi reads took {duration:.2f}s "
            f"({MULTI_READ_COUNT / duration:.1f} reads/s), budget {MULTI_READ_MAX_SECONDS:.2f}s"
        )

    async def test_cyclic_notifications(self) -> None:
        symbol_name = self.config["symbols"]["notification"]
        expected = NOTIFICATION_COUNT * NOTIFICATION_CYCLE_TIME_MS / 1000
        received: list[NotificationValue] = []
        finished = asyncio.Event()
        duration = 0.0

        async def callback(values: list[NotificationValue]) -> None:
            nonlocal duration
            received.extend(values)
            if len(received) >= NOTIFICATION_COUNT and not finished.is_set():
                duration = time.perf_counter() - start
                finished.set()

        start = time.perf_counter()
        async with self.client.subscribe_notification(
            symbol_name,
            callback,
            TransmissionMode.CYCLIC,
            cycle_time=NOTIFICATION_CYCLE_TIME_MS,
            max_delay=0,
        ):
            try:
                await asyncio.wait_for(finished.wait(), NOTIFICATION_MAX_SECONDS)
            except TimeoutError:
                self.fail(
                    f"Received only {len(received)} of {NOTIFICATION_COUNT} notifications "
                    f"within {NOTIFICATION_MAX_SECONDS}s (expected all after {expected:.2f}s)"
                )

        # The symbol is a cycle counter sent every cycle: each value must be
        # the previous one plus 1, otherwise a cycle was missed (or repeated).
        counters = [item.value for item in received[:NOTIFICATION_COUNT]]
        missed = [
            (previous, current)
            for previous, current in zip(counters, counters[1:])
            if current != previous + 1
        ]
        self.assertEqual(
            missed, [], f"{len(missed)} gaps in the cycle counter (previous, current)"
        )
        self.assertLessEqual(
            duration,
            NOTIFICATION_MAX_SECONDS,
            f"{NOTIFICATION_COUNT} notifications took {duration:.2f}s, expected {expected:.2f}s "
            f"(one every {NOTIFICATION_CYCLE_TIME_MS}ms)",
        )
        print(
            f"\n{NOTIFICATION_COUNT} notifications took {duration:.2f}s, expected {expected:.2f}s "
            f"(one every {NOTIFICATION_CYCLE_TIME_MS}ms), budget {NOTIFICATION_MAX_SECONDS:.2f}s, "
            f"{len(missed)} cycles missed"
        )
