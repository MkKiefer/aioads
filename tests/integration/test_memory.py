"""
Integration tests that watch the Python heap for memory leaks.

Measurements use `tracemalloc` from the standard library, so only allocations
made by Python code are counted (which is all of aioads). `gc.collect()` runs
before every snapshot so the numbers only contain objects that are still
reachable, not garbage awaiting collection.

Reported parts per lifecycle:
1. Startup cost   - connect + datatype upload + first symbol cache fill.
2. Runtime cost   - steady-state growth over many reads (the leak signal)
                    plus the transient peak a batch of reads needs on top.
3. Cleanup residue - what is still allocated after disconnect, relative to
                    the pre-connect baseline.

The residue of a single lifecycle legitimately contains one-time allocations
(module-level loggers, enum value caches, interned strings), so the decisive
leak check is `test_connect_cycles_do_not_leak`: after the first cycle has
absorbed those one-time costs, further cycles must not grow the heap.

Like the performance tests, the budgets below are generous upper bounds, not
benchmarks. A failure means a leak or a pathological regression, not a small
change. Adjust the constants when they no longer match the test environment.
"""
import asyncio
import gc
import tracemalloc

import unittest

from tests.integration.config import CONFIG_PATH
from tests.integration.config import create_client
from tests.integration.config import load_config

# Upper bound for connect + datatype upload + first cache fill. Depends on the
# size of the PLC project, so this is a loose sanity bound only.
STARTUP_MAX_BYTES = 20 * 1024 * 1024

# Reads executed before measuring, to let lazy one-time allocations
# (struct caches, buffers growing to working size) settle.
WARMUP_READS = 25

# Reads in the measured window. Each iteration performs one single-symbol read
# and one multi-symbol read. Steady-state reads must not accumulate memory, so
# the growth budget is small and independent of the read count.
MEASURED_READS = 1500
RUNTIME_GROWTH_MAX_BYTES = 16 * 1024

# Transient memory a batch of reads may need on top of the steady state
# (request/response buffers, parsed values before they are dropped).
RUNTIME_PEAK_MAX_BYTES = 8 * 1024 * 1024

# Allowed leftover after disconnecting a single client, relative to the
# pre-connect baseline. Covers legitimate one-time allocations that persist
# for the lifetime of the interpreter.
RESIDUE_MAX_BYTES = 1 * 1024 * 1024

# Full connect/read/disconnect cycles in the measured window and their total
# growth budget. Growth proportional to the cycle count means a real leak.
MEASURED_CYCLES = 5
CYCLE_GROWTH_MAX_BYTES = 64 * 1024


async def _drain_cancelled_timers() -> None:
    """
    Force asyncio to evict cancelled timer handles from its schedule.

    Every transport request arms a `wait_for` timeout; the response arrives
    long before it fires, so the timer is cancelled — but cancelled
    `TimerHandle`s stay reachable in the loop's schedule until asyncio's lazy
    cleanup runs (only once cancelled handles exceed 100 and half the heap).
    Heap snapshots would otherwise measure a random phase of that saw-tooth
    (tens of KiB) instead of actual growth. Scheduling and cancelling enough
    dummy timers pushes the cancelled ratio over the cleanup threshold, so
    the next loop iteration compacts the schedule deterministically.
    """
    loop = asyncio.get_running_loop()
    handles = [loop.call_later(3600, lambda: None) for _ in range(256)]
    for handle in handles:
        handle.cancel()
    await asyncio.sleep(0)


async def _heap_bytes() -> int:
    """Drain cancelled timers, collect garbage and return the traced heap size."""
    await _drain_cancelled_timers()
    gc.collect()
    return tracemalloc.get_traced_memory()[0]


def _fmt(num_bytes: int) -> str:
    """Format a byte count as a signed KiB value for readable reports."""
    return f"{num_bytes / 1024:+,.1f} KiB"


@unittest.skipUnless(
    CONFIG_PATH.exists(),
    "Integration config missing: copy config.example.toml to config.toml",
)
class TestMemory(unittest.IsolatedAsyncioTestCase):
    """
    Memory lifecycle tests for reading symbols by name.

    Does not use `IntegrationTestCase`: these tests must control the client
    lifecycle themselves to measure connect and disconnect.
    """

    async def asyncSetUp(self) -> None:
        self.config = load_config()
        self.symbol_single = self.config["symbols"]["single"]
        self.symbols_multiple = set(self.config["symbols"]["multiple"])
        # Trigger lazy imports (e.g. MQTT transports) before any baseline is
        # taken, so module-level memory is not attributed to the client.
        create_client(self.config)
        tracemalloc.start()

    async def asyncTearDown(self) -> None:
        tracemalloc.stop()

    async def _read_once(self, client) -> None:
        """One iteration of the read patterns under test."""
        await client.read_symbol_by_name(self.symbol_single)
        await client.read_symbols_by_names(self.symbols_multiple)

    async def test_read_symbols_memory_lifecycle(self) -> None:
        """Measure startup cost, runtime growth and cleanup residue."""
        baseline = await _heap_bytes()

        # Part 1: startup - connect (datatype upload) + first cache fill.
        client = create_client(self.config)
        await client.connect()
        await self._read_once(client)
        startup_cost = await _heap_bytes() - baseline

        # Part 2: runtime - steady-state growth and transient peak.
        for _ in range(WARMUP_READS):
            await self._read_once(client)
        before_reads = await _heap_bytes()
        tracemalloc.reset_peak()
        for _ in range(MEASURED_READS):
            await self._read_once(client)
        runtime_peak = tracemalloc.get_traced_memory()[1] - before_reads
        runtime_growth = await _heap_bytes() - before_reads

        # Part 3: cleanup - disconnect, drop the client, measure the residue.
        await client.disconnect()
        del client
        residue = await _heap_bytes() - baseline

        print(
            f"\nMemory lifecycle report"
            f"\n  1. startup (connect + first caching): {_fmt(startup_cost)}"
            f"\n  2. runtime growth over {MEASURED_READS} reads: "
            f"{_fmt(runtime_growth)} "
            f"({runtime_growth / MEASURED_READS:+.1f} B/read), "
            f"transient peak {_fmt(runtime_peak)}"
            f"\n  3. cleanup residue vs. baseline: {_fmt(residue)}"
        )

        with self.subTest(part="1-startup"):
            self.assertLessEqual(
                startup_cost,
                STARTUP_MAX_BYTES,
                f"Startup allocated {_fmt(startup_cost)}",
            )
        with self.subTest(part="2-runtime"):
            self.assertLessEqual(
                runtime_growth,
                RUNTIME_GROWTH_MAX_BYTES,
                f"{MEASURED_READS} reads grew the heap by {_fmt(runtime_growth)} "
                f"({runtime_growth / MEASURED_READS:+.1f} B/read), "
                f"reads are accumulating memory",
            )
            self.assertLessEqual(
                runtime_peak,
                RUNTIME_PEAK_MAX_BYTES,
                f"Reads transiently needed {_fmt(runtime_peak)} on top of "
                f"the steady state",
            )
        with self.subTest(part="3-cleanup"):
            self.assertLessEqual(
                residue,
                RESIDUE_MAX_BYTES,
                f"{_fmt(residue)} still allocated after disconnect",
            )

    async def test_connect_cycles_do_not_leak(self) -> None:
        """Repeated connect/read/disconnect cycles must not grow the heap."""
        async def one_cycle() -> None:
            client = create_client(self.config)
            await client.connect()
            await self._read_once(client)
            await client.disconnect()

        # The first cycle absorbs one-time allocations that live for the
        # rest of the interpreter (loggers, enum caches, interned strings).
        await one_cycle()
        baseline = await _heap_bytes()

        for _ in range(MEASURED_CYCLES):
            await one_cycle()
        growth = await _heap_bytes() - baseline

        print(
            f"\nConnect cycle report: {MEASURED_CYCLES} cycles grew the heap "
            f"by {_fmt(growth)} ({growth / MEASURED_CYCLES:+,.0f} B/cycle)"
        )
        self.assertLessEqual(
            growth,
            CYCLE_GROWTH_MAX_BYTES,
            f"{MEASURED_CYCLES} connect/read/disconnect cycles grew the heap "
            f"by {_fmt(growth)} ({growth / MEASURED_CYCLES:+,.0f} B/cycle), "
            f"the client lifecycle is leaking",
        )
