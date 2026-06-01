"""Unit tests for aioads.commands.ads_read_device_info."""

import unittest
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_read_device_info import (
    AdsReadDeviceInfo,
    AdsReadDeviceInfoResponse,
)
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsReadDeviceInfoResponse(unittest.TestCase):

    def setUp(self) -> None:
        self.response = AdsReadDeviceInfoResponse(
            error_code=AdsErrorCode(0),
            major_version=3,
            minor_version=1,
            version_build=4024,
            device_name="Plc30 App",
        )

    def test_version_string_joins_parts_with_dots(self) -> None:
        # Act
        result = self.response.version_string()

        # Assert
        self.assertEqual(result, "3.1.4024")

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Act
        restored = AdsReadDeviceInfoResponse.deserialize(
            make_stream(self.response.serialize())
        )

        # Assert
        self.assertEqual(restored, self.response)

    def test_serialize_pads_device_name_to_twenty_bytes(self) -> None:
        # Act
        result = self.response.serialize()

        # Assert: 4 (error) + 1 + 1 + 2 (version) + 20 (name)
        self.assertEqual(len(result), 28)

    def test_serialize_name_longer_than_twenty_raises_value_error(self) -> None:
        # Arrange
        response = AdsReadDeviceInfoResponse(
            error_code=AdsErrorCode(0),
            major_version=3,
            minor_version=1,
            version_build=1,
            device_name="x" * 21,
        )

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            response.serialize()

        self.assertIn("at most 20 characters", str(ctx.exception))

    def test_deserialize_short_stream_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(b"\x00" * 27)

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            AdsReadDeviceInfoResponse.deserialize(stream)

        self.assertIn("Invalid data length", str(ctx.exception))


class TestAdsReadDeviceInfo(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = AdsReadDeviceInfo(
            transport=self.transport,
            ams_address=make_ams_address(),
        )

    def test_serialize_returns_empty_payload(self) -> None:
        # Act / Assert
        self.assertEqual(self.command.serialize(), b"")

    async def test_request_returns_device_info(self) -> None:
        # Arrange
        payload = AdsReadDeviceInfoResponse(
            AdsErrorCode(0), 3, 1, 4024, "Device"
        ).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        response = await self.command.request()

        # Assert
        self.assertEqual(response.device_name, "Device")

    async def test_request_uses_read_device_info_command_id(self) -> None:
        # Arrange
        payload = AdsReadDeviceInfoResponse(AdsErrorCode(0), 1, 0, 0, "x").serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"],
            AdsCommandId.READ_DEVICE_INFO,
        )

    async def test_request_ams_error_raises_ams_header_error(self) -> None:
        # Arrange
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(error_code=7), make_stream(b""))
        )

        # Act / Assert
        with self.assertRaises(AdsAmsHeaderError):
            await self.command.request()

    async def test_request_command_error_raises_command_error(self) -> None:
        # Arrange
        payload = AdsReadDeviceInfoResponse(AdsErrorCode(1793), 0, 0, 0, "x").serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1793)


if __name__ == "__main__":
    unittest.main()
