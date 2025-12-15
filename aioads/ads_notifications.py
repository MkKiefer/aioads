"""
This module is still just a POC to verify the general functionality of ads notifications. 
We want to add default monitoring of plc cycle usage to automatically disable notifications before
we are the reason of cycle time violations. 

Future Features: 
    - Queueing of messages
    - parallel parsing of notifications (plc tasks are faster as we can parse)
    - QOS for queueing and parsing ? 

"""
import asyncio
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Coroutine

from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import AdsSymbolParser
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.commands.ads_add_notification import (
    AdsAddNotificationCommand, TransmissionMode
)
from aioads.commands.ads_delete_notification import AdsDeleteNotificationCommand
from aioads.functions.ads_symbol_info_by_name_ex import SymbolInfo
from aioads.stream import AdsStream
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class NotificationValue:
    """
    The callback value for parsed notifications
    """
    handle: int
    timestamp: int
    value: Any


TNotificationCallback = Callable[[
    list[NotificationValue]], Coroutine[None, None, None]]


class NotificationManager:
    """
    The notification manager is still in progress,
    the notification manager is responsible to ensure a proper cleanup of all
    not longer used notification subscriptions and distribution of parsed notifications
    """

    MAX_SUBSCRIPTIONS = 100  # Beckhoff PLCs support up to 500 notifications

    def __init__(
        self,
        transport: ITransport,
        dst_address: AmsAddress,
        symbol_cache: AdsSymbolCache,
        parser: AdsSymbolParser,
    ):
        self._logger = logging.getLogger(__name__)
        self.transport = transport
        self.transport.set_notification_callback(self.on_notification_received)
        self.dst_address = dst_address
        self.symbol_cache = symbol_cache
        self.symbol_parser = parser
        self._callback_handles: dict[int,
                                     tuple[TNotificationCallback, SymbolInfo]] = {}
        self._remove_tasks: dict[int, asyncio.Task] = {}

    async def _create_notification(
        self,
        symbol_info: SymbolInfo,
        mode: TransmissionMode,
        cycle_time: int,
        max_delay: int,
    ) -> int:
        """
        Add a notification and return the id of the new created handle
        """

        function = AdsAddNotificationCommand(
            transport=self.transport,
            ams_address=self.dst_address,
            idx_group=symbol_info.idx_group,
            idx_offset=symbol_info.idx_offset,
            length=symbol_info.idx_length,
            transmission_mode=mode,
            max_delay=max_delay,
            cycle_time=cycle_time,
        )
        response = await function.request()
        self._logger.info(
            "Created notification handle %d for symbol %s",
            response.notification_handle,
            symbol_info.symbol_name,
        )
        return response.notification_handle

    async def _remove_notification(self, notification_handle: int) -> None:
        """
        Remove a notification by the handle
        """
        function = AdsDeleteNotificationCommand(
            transport=self.transport,
            ams_address=self.dst_address,
            notification_handle=notification_handle,
        )
        await function.request()
        self._remove_tasks.pop(notification_handle, None)
        self._logger.info(
            "Removed notification handle %d scheduled size %d",
            notification_handle,
            len(self._remove_tasks),
        )

    async def parse_notification_payload(
        self, payload: AdsStream
    ) -> AsyncGenerator[tuple[int, int, Any], None]:
        """
        Handle the parsing of the notifications
        """

        unknown_handles: set[int] = set()

        _length = int.from_bytes(payload.read(4), "little")
        stamp_count = int.from_bytes(payload.read(4), "little")

        for _ in range(stamp_count):
            timestamp = int.from_bytes(payload.read(8), "little")
            samples = int.from_bytes(payload.read(4), "little")
            for _ in range(samples):
                notification_handle = int.from_bytes(payload.read(4), "little")
                sample_size = int.from_bytes(payload.read(4), "little")
                handler = self._callback_handles.get(notification_handle)
                if not handler:
                    unknown_handles.add(notification_handle)
                    # Skip unknown notification
                    payload.seek(payload.tell() + sample_size)
                    continue
                _, symbol_info = handler
                value = self.symbol_parser.parse(
                    symbol_info.data_type, symbol_info.type_name, payload
                )
                yield notification_handle, timestamp, value

        for handle in unknown_handles:
            self._logger.warning(
                "Received notification for unknown handle %d",
                handle,
            )
            await self._remove_notification(handle)

    async def on_notification_received(self, _ams_header: AmsHeader, payload: AdsStream) -> None:
        """
        handler for the raw notification events that need parsing and distribution
        """

        notifications: list[NotificationValue] = []

        async for handle, timestamp, value in self.parse_notification_payload(payload):
            notifications.append(NotificationValue(handle, timestamp, value))

        grouped_by_handle: dict[int,
                                list[NotificationValue]] = defaultdict(list)
        for notification in notifications:
            grouped_by_handle[notification.handle].append(notification)

        async with asyncio.TaskGroup() as tg:
            for handle, notification_group in grouped_by_handle.items():
                callback, _ = self._callback_handles[handle]
                tg.create_task(callback(notification_group))

    @asynccontextmanager
    async def create_notification(
        self,
        symbol_name: str,
        callback: TNotificationCallback,
        mode: TransmissionMode,
        cycle_time: int,
        max_delay: int,
    ) -> AsyncGenerator[int, None]:
        """contextmanager to subscribe to variables that ensures we always remove
        the unstd subscriptions.
        """

        if len(self._callback_handles) >= self.MAX_SUBSCRIPTIONS:
            raise ValueError("Maximum number of notifications reached")

        symbol_info = await self.symbol_cache.read_symbol_info_by_name(symbol_name)
        notification_handle = await self._create_notification(
            symbol_info, mode, cycle_time, max_delay
        )
        try:
            self._callback_handles[notification_handle] = (
                callback, symbol_info)
            yield notification_handle
        finally:
            await self._remove_notification(notification_handle)
