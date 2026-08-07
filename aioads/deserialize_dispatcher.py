"""Dispatcher that offloads deserialization to a thread pool for large payloads."""

import asyncio
from collections.abc import Callable
import logging
from aioads.commands.ads_read import AdsReadResponse
from aioads.stream import AdsStream

type DeserializeSingleCallable[T] = Callable[[AdsStream], T]
type DeserializeMultipleCallable[T] = Callable[
    [list[tuple[AdsReadResponse, AdsStream]]], T
]


class DeserializeDispatcher:
    """
    Runs a deserialize callable either in the asyncio loop or in a thread pool
    executor, depending on the size of the response. Payloads larger than the
    configured offload size are dispatched to a thread to keep the event loop
    responsive.
    """

    def __init__(self, offload_size: int) -> None:
        self._logger = logging.getLogger(f"{__name__}.DeserializeDispatcher")
        self.offload_size = offload_size

    async def single[S](self, fn: DeserializeSingleCallable[S], data: AdsStream) -> S:
        """Run fn in a thread if the remaining data exceeds the offload threshold."""
        remaining_length = data.length - data.tell()
        if remaining_length >= self.offload_size:
            self._logger.debug(
                "Offloading deserialization of %d bytes to thread", remaining_length
            )
            return await asyncio.to_thread(fn, data)
        # Parsing of smaller chunks is done in the asyncio loop to avoid the overhead of thread switching
        return fn(data)

    async def multiple[M](
        self,
        fn: DeserializeMultipleCallable[M],
        data: list[tuple[AdsReadResponse, AdsStream]],
    ) -> M:
        """Run fn in a thread if the total remaining data exceeds the offload threshold."""
        total_remaining = sum(stream.length - stream.tell() for _, stream in data)
        if total_remaining >= self.offload_size:
            self._logger.debug(
                "Offloading deserialization of %d bytes to thread", total_remaining
            )
            return await asyncio.to_thread(fn, data)
        # Parsing of smaller batches is done in the asyncio loop to avoid the overhead of thread switching
        return fn(data)
