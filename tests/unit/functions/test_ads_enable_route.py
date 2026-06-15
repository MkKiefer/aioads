"""Unit tests for aioads.functions.ads_enable_route."""

import unittest
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_service_port import AmsServicePort
from aioads.commands.ads_write import AdsWriteResponse
from aioads.functions.ads_enable_route import AdsEnableRoute, RouteSwitch
from tests.builders import (
    make_ams_address,
    make_ams_header,
    make_stream,
    make_transport,
)


class TestAdsEnableRoute(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.function = AdsEnableRoute(
            transport=self.transport,
            ams_address=make_ams_address(net_id="5.6.7.8.1.1", port=851),
            route="MQTT:MyBroker",
            switch=RouteSwitch.ROUTE_ENABLE_TMP,
        )

    def test_init_overrides_port_to_system_service(self) -> None:
        # Assert
        self.assertEqual(
            self.function.modified_address.port, AmsServicePort.SYSTEM_SERVICE
        )

    def test_init_preserves_target_net_id(self) -> None:
        # Assert
        self.assertEqual(self.function.modified_address.net_id, "5.6.7.8.1.1")

    def test_serialize_encodes_route_as_utf8(self) -> None:
        # Act
        result = self.function.serialize()

        # Assert
        self.assertEqual(result, b"MQTT:MyBroker")

    async def test_execute_uses_switch_value_as_index_offset(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(
                make_ams_header(),
                make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()),
            )
        )

        # Act
        await self.function.execute()

        # Assert: write command serializes idx_offset first after group; offset == 4
        sent_payload = self.transport.request.call_args.kwargs["command_payload"]
        index_offset = int.from_bytes(sent_payload[4:8], "little")
        self.assertEqual(index_offset, RouteSwitch.ROUTE_ENABLE_TMP.value)

    async def test_execute_returns_write_response(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(
                make_ams_header(),
                make_stream(AdsWriteResponse(AdsErrorCode(0)).serialize()),
            )
        )

        # Act
        response = await self.function.execute()

        # Assert
        self.assertEqual(response.error_code, 0)


if __name__ == "__main__":
    unittest.main()
