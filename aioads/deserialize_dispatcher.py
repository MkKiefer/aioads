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

    async def single[T](
        self, callable: DeserializeSingleCallable[T], data: AdsStream
    ) -> T:
        remaining_length = data.length - data.tell()
        if remaining_length >= self.offload_size:
            self._logger.debug(
                f"Offloading deserialization of {remaining_length} bytes to thread"
            )
            return await asyncio.to_thread(callable, data)
        # Parsing of smaller chunks is done in the asyncio loop to avoid the overhead of thread switching
        return callable(data)

    async def multiple[T](
        self,
        callable: DeserializeMultipleCallable[T],
        data: list[tuple[AdsReadResponse, AdsStream]],
    ) -> T:
        total_remaining = sum(stream.length - stream.tell() for _, stream in data)
        if total_remaining >= self.offload_size:
            self._logger.debug(
                f"Offloading deserialization of {total_remaining} bytes to thread"
            )
            return await asyncio.to_thread(callable, data)
        # Parsing of smaller batches is done in the asyncio loop to avoid the overhead of thread switching
        return callable(data)
