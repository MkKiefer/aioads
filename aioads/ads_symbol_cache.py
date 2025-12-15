"""
ADS Symbol Cache for caching symbol information asynchronously.
"""

import asyncio
import logging
import time
from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_symbol_info_by_name_ex import (
    SymbolInfo,
    SymbolInfoByNameEx,
    SymbolInfoByNameExSumRead,
)
from aioads.functions.ads_symbol_table_version import SymbolTableVersion
from aioads.transport import ITransport


class AdsSymbolCache:
    """
    Symbol information loader and caching.
    - Caches symbol information to reduce redundant PLC queries.
    - Monitors symbol table version to invalidate cache when necessary.
    - Provides methods to read symbol info by name, utilizing the cache.
    """

    def __init__(
        self,
        transport: ITransport,
        dst_address: AmsAddress,
        ttl_seconds: int = 21600,
    ) -> None:
        self._logger = logging.getLogger(f"{__name__}.'{dst_address.net_id}'")
        self._transport = transport
        self._dst_address = dst_address
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[int, AdsErrorCode, SymbolInfo]] = {}
        self._cache_monitor_task: asyncio.Task[None] | None = None

    def _get_cached_or_expire(
        self, symbol_name: str
    ) -> tuple[AdsErrorCode, SymbolInfo] | None:
        """
        Retrieves the cached symbol info if it exists and is still valid.
        Otherwise, removes it from the cache and returns None.
        """
        key = symbol_name.casefold()
        cached = self._cache.get(key)
        if cached:
            valid_until, error_code, symbol_info = cached
            if valid_until > time.monotonic():
                return error_code, symbol_info
            del self._cache[key]
        return None

    def _add_to_cache(
        self, symbol_name: str, error_code: AdsErrorCode, symbol_info: SymbolInfo
    ) -> None:
        """
        Add a symbol info to the cache with the current TTL.
        """
        valid_until = time.monotonic() + self._ttl_seconds
        self._cache[symbol_name.casefold()] = (
            int(valid_until),
            error_code,
            symbol_info,
        )

    async def start_cache_monitor(self, interval_seconds: int = 60) -> None:
        """
        Start the cache monitor to detect changes in the symbol table and
        invalidate the cache if necessary.
        """
        if self._cache_monitor_task is None:
            self._cache_monitor_task = asyncio.create_task(
                self._monitor(interval_seconds)
            )

    async def stop_cache_monitor(self) -> None:
        """
        Stop the cache monitor task.
        """
        if self._cache_monitor_task:
            self._cache_monitor_task.cancel()
            try:
                await self._cache_monitor_task
            except asyncio.CancelledError:
                pass
            self._cache_monitor_task = None

    async def _monitor(self, interval_seconds: int) -> None:
        """
        Monitor the symbol table version and clear the cache if it changes.
        """
        symbol_table_version = None
        while True:
            try:
                function = SymbolTableVersion(
                    transport=self._transport,
                    ams_address=self._dst_address,
                )
                current_version = await function.execute()
                if symbol_table_version is None:
                    self._logger.debug(
                        "Initialized symbol table version: %d", current_version
                    )
                    symbol_table_version = current_version
                elif symbol_table_version != current_version:
                    self._logger.info(
                        "Symbol table version changed from %d to %d, clearing cache.",
                        symbol_table_version,
                        current_version,
                    )
                    self._cache.clear()
                    symbol_table_version = current_version
            except asyncio.CancelledError:
                break
            except Exception:  # pylint: disable=broad-except
                self._logger.debug(
                    "Failed to monitor symbol table version", exc_info=True
                )
            finally:
                await asyncio.sleep(interval_seconds)

    async def read_symbol_info_by_name(self, symbol_name: str) -> SymbolInfo:
        """
        Reads symbol info from the cache if it exists and is valid.
        or it gets pulled from the PLC.
        """
        cached = self._get_cached_or_expire(symbol_name)
        if cached:
            error_code, cached_info = cached
            # This is the same error a real call would give use.
            # We simulate it here if the multi call has cached it with an error.
            if error_code != 0:
                raise AdsCommandError(error_code)
            return cached_info

        function = SymbolInfoByNameEx(
            transport=self._transport,
            ams_address=self._dst_address,
            symbol_name=symbol_name,
        )
        symbol_info = await function.execute()
        self._add_to_cache(symbol_name, AdsErrorCode(0), symbol_info)
        return symbol_info

    async def read_symbol_infos_by_names(
        self, symbol_names: set[str]
    ) -> dict[str, tuple[AdsErrorCode, SymbolInfo]]:
        """
        Reads multiple symbol infos from the cache if they exist and are valid,
        or they get pulled from the PLC.
        """
        results: dict[str, tuple[AdsErrorCode, SymbolInfo]] = {}
        request_list: list[str] = []
        # Try to get as many as possible from the cache first.
        for symbol in symbol_names:
            key = symbol.casefold()
            cached = self._cache.get(key)
            if cached:
                valid_until, error_code, symbol_info = cached
                if valid_until > time.monotonic():
                    results[symbol] = (error_code, symbol_info)
                else:
                    del self._cache[key]
                    request_list.append(symbol)
            else:
                request_list.append(symbol)

        # We have symbols that are not yet cached, request them in bulk.
        if request_list:
            function = SymbolInfoByNameExSumRead(
                transport=self._transport,
                ams_address=self._dst_address,
                symbol_names=request_list,
            )
            response = await function.execute()
            for (error, symbol_info), symbol_name in zip(
                response, request_list
            ):
                results[symbol_name] = (AdsErrorCode(error), symbol_info)
                self._add_to_cache(
                    symbol_name, AdsErrorCode(error), symbol_info)
        return results
