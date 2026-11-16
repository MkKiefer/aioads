"""
Integration performance tests against fixed time budgets.

The budgets below are not benchmarks: they are generous upper bounds tuned to
pass under the worst expected conditions (transport, network latency, PLC
load). A failure means a pathological slowdown, not a small regression.
Adjust the constants below when they no longer match the test environment.
"""
import asyncio
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass

from aioads.ads_notifications import NotificationRegistry
from aioads.ads_notifications import NotificationValue
from aioads.commands.ads_add_notification import TransmissionMode
from tests.integration.base import IntegrationTestCase

# Number of sequential single-symbol reads and the time budget for all of them.
SINGLE_READ_COUNT = 500
SINGLE_READ_MAX_SECONDS = 29.0

# Number of sequential multi-symbol reads and the time budget for all of them.
MULTI_READ_COUNT = 100
MULTI_READ_MAX_SECONDS = 7.0

# How long notifications are recorded and how often the PLC samples the symbol.
NOTIFICATION_SECONDS = 5.0
NOTIFICATION_CYCLE_TIME_MS = 10
# Number of parallel subscriptions used to put load on decoding and delivery.
NOTIFICATION_LOAD_SUBSCRIPTIONS = 50
# Minimum share of the nominal sample count (duration / cycle time) that must
# arrive. The PLC rounds the cycle time up to a multiple of its task cycle.
NOTIFICATION_MIN_SAMPLE_SHARE = 0.5
# Budgets for the lag of a sample (see `NotificationStats`), in milliseconds.
NOTIFICATION_MAX_MEDIAN_LAG_MS = 25.0
NOTIFICATION_MAX_P99_LAG_MS = 100.0

# Offset between the Windows FILETIME epoch (1601) and the Unix epoch (1970) in 100 ns ticks.
FILETIME_UNIX_EPOCH = 116_444_736_000_000_000
FILETIME_TICKS_PER_MS = 10_000

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotificationStats:
    """
    Outcome of a notification recording.

    The lag of a sample is the time between the PLC taking it (its timestamp)
    and our callback receiving it: transport, framing, decoding and delivery.
    The clocks of the PLC and of the test host are not synchronised, so the
    sample with the smallest difference is taken as zero and every other lag
    is the delay on top of that best case.
    """

    samples: int
    batches: int
    median_lag_ms: float
    p99_lag_ms: float
    max_lag_ms: float
    dropped_values: int
    dropped_frames: int
    decode_errors: int

    def __str__(self) -> str:
        return (
            f"{self.samples} samples in {self.batches} batches "
            f"({self.samples / NOTIFICATION_SECONDS:.0f} samples/s), "
            f"lag median {self.median_lag_ms:.2f}ms / p99 {self.p99_lag_ms:.2f}ms / "
            f"max {self.max_lag_ms:.2f}ms, dropped {self.dropped_values} values / "
            f"{self.dropped_frames} frames, {self.decode_errors} decode errors"
        )


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


class TestNotificationPerformance(IntegrationTestCase):
    """
    Subscribe to the symbol defined as `symbols.notification` in the config and
    check how far the delivered values lag behind the PLC.

    Run with `--log-cli-level=INFO` to see the measured numbers of a passing run.
    """

    async def record(self, subscriptions: int) -> NotificationStats:
        """
        Record all values of `subscriptions` parallel cyclic subscriptions for
        NOTIFICATION_SECONDS. The PLC sends every sample immediately
        (`max_delay=0`), so a late sample was delayed on the way to the callback.
        """
        symbol_name = self.config["symbols"]["notification"]
        registry = NotificationRegistry.attach(self.client.transport)
        offsets: list[int] = []
        batches = 0
        recording = False

        async def callback(values: list[NotificationValue]) -> None:
            nonlocal batches
            if not recording:
                return
            now = time.time_ns() // 100 + FILETIME_UNIX_EPOCH
            batches += 1
            offsets.extend(now - value.timestamp for value in values)

        async with AsyncExitStack() as stack:
            for _ in range(subscriptions):
                await stack.enter_async_context(
                    self.client.subscribe_notification(
                        symbol_name,
                        callback,
                        TransmissionMode.CYCLIC,
                        cycle_time=NOTIFICATION_CYCLE_TIME_MS,
                        max_delay=0,
                    )
                )
            # Only record while all subscriptions are active.
            recording = True
            await asyncio.sleep(NOTIFICATION_SECONDS)
            recording = False
            dropped_values = sum(sub.dropped for sub in registry.subscriptions)

        self.assertGreater(len(offsets), 0, "No notification was received")
        offsets.sort()
        best = offsets[0]

        def lag_ms(share: float) -> float:
            index = min(len(offsets) - 1, int(len(offsets) * share))
            return (offsets[index] - best) / FILETIME_TICKS_PER_MS

        stats = NotificationStats(
            samples=len(offsets),
            batches=batches,
            median_lag_ms=lag_ms(0.5),
            p99_lag_ms=lag_ms(0.99),
            max_lag_ms=lag_ms(1.0),
            dropped_values=dropped_values,
            dropped_frames=registry.dropped_frames,
            decode_errors=registry.decode_errors,
        )
        LOGGER.info("%d subscription(s): %s", subscriptions, stats)
        return stats

    def assert_within_budget(self, stats: NotificationStats, subscriptions: int) -> None:
        """Check that nothing was lost and the lag stayed within the budgets."""
        nominal = subscriptions * NOTIFICATION_SECONDS * 1000 / NOTIFICATION_CYCLE_TIME_MS
        self.assertEqual(
            (stats.dropped_values, stats.dropped_frames, stats.decode_errors),
            (0, 0, 0),
            str(stats),
        )
        self.assertGreaterEqual(
            stats.samples, nominal * NOTIFICATION_MIN_SAMPLE_SHARE, str(stats)
        )
        self.assertLessEqual(stats.median_lag_ms, NOTIFICATION_MAX_MEDIAN_LAG_MS, str(stats))
        self.assertLessEqual(stats.p99_lag_ms, NOTIFICATION_MAX_P99_LAG_MS, str(stats))

    async def test_single_notification_lag(self) -> None:
        stats = await self.record(subscriptions=1)
        self.assert_within_budget(stats, subscriptions=1)

    async def test_parallel_notifications_lag(self) -> None:
        stats = await self.record(subscriptions=NOTIFICATION_LOAD_SUBSCRIPTIONS)
        self.assert_within_budget(stats, subscriptions=NOTIFICATION_LOAD_SUBSCRIPTIONS)
