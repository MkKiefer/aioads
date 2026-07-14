"""ADS Client for communicating with ADS devices asynchronously."""

import asyncio
import logging
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
from aioads.commands.ads_read import AdsReadResponse
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
from aioads.stream import AdsStream
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
        sum_batch_size: int,
    ) -> None:
        self.logger = logging.getLogger(f"{__name__}.'{dst_address.net_id}'")
        self.transport = transport
        self.dst_address = dst_address
        self.parser = parser
        self.sum_batch_size = sum_batch_size
        self._cache = cache
        self._notification = notification

    @classmethod
    def create_tcp(
        cls,
        src: AmsAddress,
        dst: AmsAddress,
        ip: str,
        port: int = 48898,
        sum_batch_size: int = 500,
    ) -> "AdsClient":
        """
        Create a new ADS client with TCP transport.
        :param src: The source AMS address
        :param dst: The destination AMS address
        :param ip: The target IP address
        :param port: The target port
        :param sum_batch_size: Maximum commands per ADS sum command (1..500)
        :return: An instance of AdsClient
        """
        parser = AdsSymbolParser([])
        transport = AdsTcpTransport(src_address=src, ip=ip, port=port)
        cache = AdsSymbolCache(
            transport=transport, dst_address=dst, batch_size=sum_batch_size
        )
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
            sum_batch_size=sum_batch_size,
        )

    @classmethod
    def create_from_transport(
        cls,
        dst: AmsAddress,
        transport: ITransport,
        sum_batch_size: int = 500,
    ) -> "AdsClient":
        """
        Create a new ADS client with an existing transport instance.
        :param src: The source AMS address
        :param dst: The destination AMS address
        :param transport: The existing transport instance
        :param sum_batch_size: Maximum commands per ADS sum command (1..500)
        :return: An instance of AdsClient
        """
        parser = AdsSymbolParser([])
        cache = AdsSymbolCache(
            transport=transport, dst_address=dst, batch_size=sum_batch_size
        )
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
            sum_batch_size=sum_batch_size,
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
            RouteSwitch.ROUTE_ENABLE_TMP if enabled else RouteSwitch.ROUTE_DISABLE_TMP,
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

        The returned dict is keyed by the symbol names exactly as requested,
        preserving their original casing.
        """
        symbol_infos = await self._cache.read_symbol_infos_by_names(symbol_names)
        if raise_errors:
            self._raise_if_error(symbol_infos)

        # Pre-seed the result with lookup errors so the output preserves the
        # requested order; successful entries are overwritten after the read.
        # Symbols with a failed lookup cannot be read: their idx_group/offset
        # would misaligne the bulk response.
        output: dict[str, SymbolReadResult] = {
            name: SymbolReadResult(error_code, None)
            for name, (error_code, _) in symbol_infos.items()
        }
        readable = [
            (name, info)
            for name, (error_code, info) in symbol_infos.items()
            if error_code.ok
        ]
        if not readable:
            return output

        sum_read = AdsSumRead(
            transport=self.transport,
            ams_address=self.dst_address,
            commands=[
                AdsReadCommand(
                    transport=self.transport,
                    ams_address=self.dst_address,
                    idx_group=info.idx_group,
                    idx_offset=info.idx_offset,
                    length=info.idx_length,
                )
                for _, info in readable
            ],
            batch_size=self.sum_batch_size,
        )
        response = await sum_read.execute()

        # Parsing is pure-Python and O(total payload size); run it off the
        # event loop so large bulk reads don't block other tasks.
        parsed = await asyncio.to_thread(
            self._parse_sum_read_response, readable, response)
        output.update(parsed)
        return output

    def _parse_sum_read_response(
        self,
        readable: list[tuple[str, SymbolInfo]],
        response: list[tuple[AdsReadResponse, AdsStream]],
    ) -> dict[str, SymbolReadResult]:
        """
        Parse the payloads of a bulk read into symbol values.
        """
        output: dict[str, SymbolReadResult] = {}
        for (name, info), (read_response, resp_payload) in zip(
            readable, response, strict=True
        ):
            if not read_response.error_code.ok:
                output[name] = SymbolReadResult(read_response.error_code, None)
                continue
            output[name] = SymbolReadResult(
                AdsErrorCode(0),
                self.parser.parse(
                    data_type=info.data_type,
                    type_name=info.type_name,
                    raw_data=resp_payload,
                ),
            )
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
