"""ADS Client for communicating with ADS devices asynchronously."""
import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from aioads.ads_error_codes import AdsErrorCode
from aioads.ads_notifications import NotificationManager
from aioads.ads_notifications import TNotificationCallback
from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import AdsSymbolParser
from aioads.ads_symbol_parser import ISymbolParser
from aioads.ams_address import AmsAddress
from aioads.commands.ads_add_notification import TransmissionMode
from aioads.commands.ads_read import AdsReadCommand
from aioads.commands.ads_read_state import ReadStateCommand
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_enable_route import AdsEnableRoute
from aioads.functions.ads_enable_route import RouteSwitch
from aioads.functions.ads_sum_read import AdsSumRead
from aioads.functions.ads_symbol_datatype_by_name import AdsSymbolDataTypeByName
from aioads.functions.ads_symbol_datatype_upload import AdsSymbolDataTypeUpload
from aioads.functions.ads_symbol_info_by_name_ex import SymbolInfo
from aioads.functions.ads_symbol_upload import AdsSymbolUpload
from aioads.functions.ads_symbol_upload_info import AdsSymbolUploadInfo2
from aioads.transport import AdsTcpTransport
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class SymbolReadResult:
    """
    Result for a multi-symbol read operation.
    """

    error_code: AdsErrorCode
    value: Any


class AdsClient:
    """
    ADS Client for communicating with ADS devices asynchronously.
    """

    def __init__(
        self,
        transport: ITransport,
        dst_address: AmsAddress,
        parser: ISymbolParser,
        cache: AdsSymbolCache,
        notification: NotificationManager,
    ) -> None:
        self.logger = logging.getLogger(f"{__name__}.'{dst_address.net_id}'")
        self.transport = transport
        self.dst_address = dst_address
        self.parser = parser
        self._cache = cache
        self._notification = notification
        self.parser_pool = ThreadPoolExecutor(
            max_workers=(os.cpu_count() or 1) * 2
        )

    @classmethod
    def create_tcp(
        cls, src: AmsAddress, dst: AmsAddress, ip: str, port: int = 48898
    ) -> "AdsClient":
        """
        Create a new ADS client with TCP transport.
        :param src: The source AMS address
        :param dst: The destination AMS address
        :param ip: The target IP address
        :param port: The target port
        :return: An instance of AdsClient
        """
        parser = AdsSymbolParser([])
        transport = AdsTcpTransport(src_address=src, ip=ip, port=port)
        cache = AdsSymbolCache(transport=transport, dst_address=dst)
        notification_manager = NotificationManager(
            transport=transport,
            dst_address=dst,
            symbol_cache=cache,
            parser=parser,
        )
        return cls(
            transport=transport,
            parser=parser,
            dst_address=dst,
            cache=cache,
            notification=notification_manager,
        )

    @classmethod
    def create_from_transport(cls, dst: AmsAddress, transport: ITransport) -> "AdsClient":
        """
        Create a new ADS client with an existing transport instance.
        :param src: The source AMS address
        :param dst: The destination AMS address
        :param transport: The existing transport instance
        :return: An instance of AdsClient
        """
        parser = AdsSymbolParser([])
        cache = AdsSymbolCache(transport=transport, dst_address=dst)
        notification_manager = NotificationManager(
            transport=transport,
            dst_address=dst,
            symbol_cache=cache,
            parser=parser,
        )
        return cls(
            transport=transport,
            parser=parser,
            dst_address=dst,
            cache=cache,
            notification=notification_manager,
        )

    async def connect(self) -> None:
        """
        Connect to the ADS device.
        """
        await self.transport.connect()
        await self._cache.start_cache_monitor()
        # ? Should we fail here if symbol upload fails?
        # ? If it fails, we are still able to read basic value types but not complex
        # ? structures.
        try:
            types_ = await self.get_symbol_datatypes()
            self.parser.update_datatypes(types_)
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    async def disconnect(self) -> None:
        """
        Disconnect from the ADS device.
        """
        await self._cache.stop_cache_monitor()
        await self.transport.disconnect()

    async def read_state(self):
        """
        Read the state of the ADS device.
        """
        request = ReadStateCommand(
            transport=self.transport,
            ams_address=self.dst_address,
        )
        return await request.request()

    async def enable_route(self, route_name: str, enabled: bool):
        """
        Enable or disable a ads route. 
        Example route name for `ads over mqtt`: `MQTT:192.168.178.12.1.1:ads` (MQTT:<NetID>:<Topic>)
        Example with defined name: `MQTT:MyBroker`
        """

        request = AdsEnableRoute(
            self.transport,
            self.dst_address,
            route_name,
            RouteSwitch.ROUTE_ENABLE_TMP if enabled else RouteSwitch.ROUTE_DISABLE_TMP
        )
        response = await request.execute()
        if not response.error_code.ok:
            raise AdsCommandError(response.error_code)

    async def get_symbols(self):
        """
        Get all root symbols from the ADS device.
        """
        tree_info_request = AdsSymbolUploadInfo2(
            transport=self.transport,
            ams_address=self.dst_address,
        )
        tree_info = await tree_info_request.execute()

        request = AdsSymbolUpload(
            transport=self.transport,
            ams_address=self.dst_address,
            tree_size=tree_info.symbol_size,
        )
        return await request.execute()

    async def get_symbol_datatypes(self):
        """
        Get all symbol datatypes from the ADS device.
        """
        tree_info_request = AdsSymbolUploadInfo2(
            transport=self.transport,
            ams_address=self.dst_address,
        )
        tree_info = await tree_info_request.execute()
        request = AdsSymbolDataTypeUpload(
            transport=self.transport,
            ams_address=self.dst_address,
            dt_size=tree_info.datatype_size,
        )
        return await request.execute()

    async def read_datatype_by_name(self, datatype_name: str):
        """
        Read a datatype by its name from the ADS device.
        """
        request = AdsSymbolDataTypeByName(
            transport=self.transport,
            ams_address=self.dst_address,
            datatype_name=datatype_name,
        )
        return await request.execute()

    async def read_symbol_info_by_name(self, symbol_name: str):
        """
        Read symbol information by its name from the ADS device.
        """
        return await self._cache.read_symbol_info_by_name(symbol_name)

    async def read_symbol_infos_by_names(self, symbol_names: set[str]):
        """
        Read symbol information for multiple symbols by their names from the ADS device.
        """
        return await self._cache.read_symbol_infos_by_names(symbol_names)

    async def read_symbol_by_name(self, symbol_name: str) -> Any:
        """
        Read a symbol by its name from the ADS device.
        """
        symbol_info = await self._cache.read_symbol_info_by_name(symbol_name)
        read_command = AdsReadCommand(
            transport=self.transport,
            ams_address=self.dst_address,
            idx_group=symbol_info.idx_group,
            idx_offset=symbol_info.idx_offset,
            length=symbol_info.idx_length,
        )
        _, payload = await read_command.request()
        return self.parser.parse(
            data_type=symbol_info.data_type,
            type_name=symbol_info.type_name,
            raw_data=payload,
        )

    def _raise_if_error(
        self, symbol_infos: dict[str, tuple[AdsErrorCode, SymbolInfo]]
    ) -> None:
        """
        Generate a human-readable error message for symbol read errors.
        """
        symbol_errors = [
            (error_code, name)
            for name, (error_code, _) in symbol_infos.items()
            if error_code != 0
        ]
        exceptions: list[AdsCommandError] = []
        for error_code, symbol_name in symbol_errors:
            symbol_path = symbol_name.replace(".", " → ")
            exceptions.append(AdsCommandError(error_code, symbol_path))

        if exceptions:
            raise ExceptionGroup(
                "One or more symbol read errors occurred", exceptions)

    async def read_symbols_by_names(
        self, symbol_names: set[str], raise_errors: bool = True
    ) -> dict[str, SymbolReadResult]:
        """
        Read multiple symbols by their names from the ADS device.
        """
        symbol_infos = await self._cache.read_symbol_infos_by_names(symbol_names)
        if raise_errors:
            self._raise_if_error(symbol_infos)

        read_commands = [
            AdsReadCommand(
                transport=self.transport,
                ams_address=self.dst_address,
                idx_group=symbol_info.idx_group,
                idx_offset=symbol_info.idx_offset,
                length=symbol_info.idx_length,
            )
            for _, symbol_info in symbol_infos.values()
        ]
        function = AdsSumRead(
            transport=self.transport,
            ams_address=self.dst_address,
            commands=read_commands,
        )

        response = await function.execute()
        output: dict[str, SymbolReadResult] = {}

        tasks = []
        for (_, resp_payload), (error_code, symbol_info) in zip(
            response, symbol_infos.values()
        ):
            tasks.append(
                asyncio.get_running_loop().run_in_executor(
                    self.parser_pool,
                    self.parser.parse,
                    symbol_info.data_type,
                    symbol_info.type_name,
                    resp_payload,
                )
            )
        parsed = await asyncio.gather(*tasks)

        for symbol_data, (error_code, symbol_info) in zip(parsed, symbol_infos.values()):
            output[symbol_info.symbol_name] = SymbolReadResult(
                error_code, symbol_data)

        return output

    @asynccontextmanager
    async def subscribe_notification(
        self,
        symbol_name: str,
        callback: TNotificationCallback,
        mode: TransmissionMode = TransmissionMode.CYCLIC,
        cycle_time: int = 50,
        max_delay: int = 100,
    ):
        """
        Subscribe to notifications for a symbol by its name.
        """
        async with self._notification.create_notification(
            symbol_name,
            callback,
            mode,
            cycle_time,
            max_delay,
        ) as notification_handle:
            yield notification_handle
