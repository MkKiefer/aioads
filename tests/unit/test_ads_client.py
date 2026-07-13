"""Unit tests for aioads.ads_client."""

import struct
import unittest
from unittest.mock import AsyncMock

from aioads.ads_client import AdsClient
from aioads.ads_error_codes import AdsErrorCode
from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import AdsSymbolParser
from aioads.ads_notifications import NotificationManager
from aioads.commands.ads_read_state import AdsDeviceState, AdsState, AdsStateResponse
from aioads.commands.ads_write import AdsWriteResponse
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
    SymbolInfo,
)
from aioads.transport import AdsTcpTransport
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_read_payload,
    make_stream,
    make_transport,
)


def make_symbol_info(name: str = "MAIN.VALUE") -> SymbolInfo:
    """A 4-byte INT32 symbol fixture."""
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


class TestAdsClientFactories(unittest.TestCase):

    def test_create_tcp_builds_client_with_tcp_transport(self) -> None:
        # Act
        client = AdsClient.create_tcp(
            src=make_ams_address(net_id="1.1.1.1.1.1"),
            dst=make_ams_address(net_id="2.2.2.2.2.2"),
            ip="127.0.0.1",
        )

        # Assert
        self.assertIsInstance(client.transport, AdsTcpTransport)

    def test_create_from_transport_reuses_given_transport(self) -> None:
        # Arrange
        transport = make_transport()

        # Act
        client = AdsClient.create_from_transport(
            dst=make_ams_address(), transport=transport
        )

        # Assert
        self.assertIs(client.transport, transport)


class TestAdsClient(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.address = make_ams_address()
        self.parser = AdsSymbolParser([])
        self.cache = AdsSymbolCache(self.transport, self.address, batch_size=500)
        self.notification = NotificationManager(
            self.transport, self.address, self.cache, self.parser
        )
        self.client = AdsClient(
            transport=self.transport,
            dst_address=self.address,
            parser=self.parser,
            cache=self.cache,
            notification=self.notification,
            sum_batch_size=500,
        )

    async def test_read_symbol_info_by_name_delegates_to_cache(self) -> None:
        # Arrange
        info = make_symbol_info()
        self.cache.read_symbol_info_by_name = AsyncMock(return_value=info)

        # Act
        result = await self.client.read_symbol_info_by_name("MAIN.VALUE")

        # Assert
        self.assertEqual(result, info)
        self.cache.read_symbol_info_by_name.assert_awaited_once_with(
            "MAIN.VALUE")

    async def test_read_symbol_infos_by_names_delegates_to_cache(self) -> None:
        # Arrange
        expected = {"MAIN.VALUE": (AdsErrorCode(0), make_symbol_info())}
        self.cache.read_symbol_infos_by_names = AsyncMock(
            return_value=expected)

        # Act
        result = await self.client.read_symbol_infos_by_names({"MAIN.VALUE"})

        # Assert
        self.assertEqual(result, expected)

    async def test_read_state_returns_state_response(self) -> None:
        # Arrange
        payload = AdsStateResponse(AdsErrorCode(
            0), AdsState.RUN, AdsDeviceState.OKAY).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        result = await self.client.read_state()

        # Assert
        self.assertEqual(result.ads_state, AdsState.RUN)

    async def test_read_symbol_by_name_returns_parsed_value(self) -> None:
        # Arrange
        self.cache.read_symbol_info_by_name = AsyncMock(
            return_value=make_symbol_info())
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(
                make_read_payload(struct.pack("<i", 99))))
        )

        # Act
        result = await self.client.read_symbol_by_name("MAIN.VALUE")

        # Assert
        self.assertEqual(result, 99)

    async def test_enable_route_success_does_not_raise(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(
                AdsWriteResponse(AdsErrorCode(0)).serialize()))
        )

        # Act
        await self.client.enable_route("MQTT:MyBroker", enabled=True)

        # Assert — a successful write was issued to the system service
        self.transport.request.assert_awaited_once()

    async def test_enable_route_error_raises_command_error(self) -> None:
        # Arrange
        from aioads.commands.errors import AdsCommandError

        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(
                AdsWriteResponse(AdsErrorCode(1793)).serialize()))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.client.enable_route("MQTT:MyBroker", enabled=False)

        self.assertEqual(ctx.exception.error_code, 1793)

    async def test_read_symbols_by_names_with_symbol_error_raises_group(self) -> None:
        # Arrange — cache reports one symbol with a device error
        self.cache.read_symbol_infos_by_names = AsyncMock(
            return_value={"MAIN.BAD": (AdsErrorCode(
                1808), make_symbol_info("MAIN.BAD"))}
        )

        # Act / Assert
        with self.assertRaises(ExceptionGroup) as ctx:
            await self.client.read_symbols_by_names({"MAIN.BAD"}, raise_errors=True)

        self.assertEqual(len(ctx.exception.exceptions), 1)


if __name__ == "__main__":
    unittest.main()
