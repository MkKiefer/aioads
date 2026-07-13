"""Unit tests for aioads.ads_symbol_cache."""

import unittest
from unittest.mock import AsyncMock

from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.commands.ads_read import AdsReadResponse
from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
)
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


def make_symbol_info(name: str = "MAIN.VALUE") -> SymbolInfo:
    """Build a SymbolInfo fixture."""
    return SymbolInfo(
        idx_group=0x4020,
        idx_offset=0,
        idx_length=4,
        data_type=AdsSymbolDataType.INT32,
        symbol_flags=AdsSymbolFlags(0),
        symbol_name=name,
        type_name="DINT",
        comment="",
    )


def single_read_response(info: SymbolInfo):
    """Transport return value for a single SymbolInfoByNameEx read."""
    return (make_ams_header(), make_stream(make_read_payload(info.serialize())))


def bulk_read_response(infos, error_code: int = 0):
    """Transport return value for a SymbolInfoByNameExSumRead batch read."""
    headers = b""
    bodies = b""
    for info in infos:
        body = info.serialize()
        headers += AdsReadResponse(AdsErrorCode(error_code), length=len(body)).serialize()
        bodies += body
    return (make_ams_header(), make_stream(make_read_payload(headers + bodies)))


class TestAdsSymbolCache(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.cache = AdsSymbolCache(
            transport=self.transport,
            dst_address=self.address,
            ttl_seconds=3600,
            batch_size=500,
        )
        self.info = make_symbol_info("MAIN.VALUE")

    async def test_read_symbol_info_by_name_returns_symbol_info(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=single_read_response(self.info))

        # Act
        result = await self.cache.read_symbol_info_by_name("MAIN.VALUE")

        # Assert
        self.assertEqual(result, self.info)

    async def test_read_symbol_info_by_name_second_call_uses_cache(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=single_read_response(self.info))

        # Act — call twice, only the first should reach the transport
        await self.cache.read_symbol_info_by_name("MAIN.VALUE")
        await self.cache.read_symbol_info_by_name("MAIN.VALUE")

        # Assert
        self.transport.request.assert_called_once()

    async def test_read_symbol_info_by_name_is_case_insensitive_for_cache(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=single_read_response(self.info))

        # Act
        await self.cache.read_symbol_info_by_name("MAIN.VALUE")
        await self.cache.read_symbol_info_by_name("main.value")

        # Assert
        self.transport.request.assert_called_once()

    async def test_read_symbol_info_by_name_expired_entry_refetches(self) -> None:
        # Arrange — zero TTL means an entry is never valid on the next lookup.
        # A fresh stream is built per call so the second fetch reads from the start.
        cache = AdsSymbolCache(
            self.transport, self.address, ttl_seconds=0, batch_size=500
        )
        self.transport.request = AsyncMock(
            side_effect=lambda **kwargs: single_read_response(self.info)
        )

        # Act
        await cache.read_symbol_info_by_name("MAIN.VALUE")
        await cache.read_symbol_info_by_name("MAIN.VALUE")

        # Assert
        self.assertEqual(self.transport.request.call_count, 2)

    async def test_read_symbol_infos_by_names_returns_info_per_symbol(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=bulk_read_response([self.info]))

        # Act
        results = await self.cache.read_symbol_infos_by_names({"MAIN.VALUE"})

        # Assert
        self.assertEqual(results["MAIN.VALUE"][1], self.info)

    async def test_read_symbol_infos_by_names_populates_cache(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(return_value=bulk_read_response([self.info]))

        # Act — bulk read then a single read for the same name
        await self.cache.read_symbol_infos_by_names({"MAIN.VALUE"})
        await self.cache.read_symbol_info_by_name("MAIN.VALUE")

        # Assert — single read served from cache, transport hit only by the bulk read
        self.transport.request.assert_called_once()

    async def test_read_symbol_info_by_name_cached_error_raises_command_error(self) -> None:
        # Arrange — bulk read caches the symbol with a device error code
        self.transport.request = AsyncMock(
            return_value=bulk_read_response([self.info], error_code=1808)
        )
        await self.cache.read_symbol_infos_by_names({"MAIN.VALUE"})

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.cache.read_symbol_info_by_name("MAIN.VALUE")

        self.assertEqual(ctx.exception.error_code, 1808)


class TestAdsSymbolCacheMonitor(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.cache = AdsSymbolCache(
            self.transport, make_ams_address(), batch_size=500
        )
        # Symbol-table-version read keeps the monitor loop body from erroring.
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(make_read_payload(b"\x01")))
        )

    async def test_stop_without_start_does_not_raise(self) -> None:
        # Act / Assert — stopping a monitor that was never started is a no-op
        await self.cache.stop_cache_monitor()

    async def test_start_then_stop_completes_cleanly(self) -> None:
        # Arrange
        await self.cache.start_cache_monitor(interval_seconds=3600)

        # Act / Assert — a started monitor can be stopped without error
        await self.cache.stop_cache_monitor()

    async def test_start_can_be_called_again_after_stop(self) -> None:
        # Arrange
        await self.cache.start_cache_monitor(interval_seconds=3600)
        await self.cache.stop_cache_monitor()

        # Act — restarting after a stop establishes a fresh monitor
        await self.cache.start_cache_monitor(interval_seconds=3600)

        # Assert — and it can be torn down again cleanly
        await self.cache.stop_cache_monitor()


if __name__ == "__main__":
    unittest.main()
