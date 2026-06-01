"""Unit tests for aioads.commands.ads_read_state."""

import unittest
from struct import Struct
from unittest.mock import AsyncMock

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_read_state import (
    AdsDeviceState,
    AdsState,
    AdsStateResponse,
    ReadStateCommand,
)
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from tests.builders import make_ams_address, make_ams_header, make_stream, make_transport


class TestAdsStateResponse(unittest.TestCase):

    def test_serialize_packs_error_code_and_states(self) -> None:
        # Arrange
        response = AdsStateResponse(
            error_code=AdsErrorCode(0),
            ads_state=AdsState.RUN,
            device_state=AdsDeviceState.OKAY,
        )

        # Act
        result = response.serialize()

        # Assert
        self.assertEqual(result, Struct("<IHH").pack(0, AdsState.RUN, AdsDeviceState.OKAY))

    def test_serialize_then_deserialize_round_trips(self) -> None:
        # Arrange
        original = AdsStateResponse(
            error_code=AdsErrorCode(0),
            ads_state=AdsState.STOP,
            device_state=AdsDeviceState.OKAY,
        )

        # Act
        restored = AdsStateResponse.deserialize(make_stream(original.serialize()))

        # Assert
        self.assertEqual(restored, original)

    def test_deserialize_reconstructs_state_enum(self) -> None:
        # Arrange
        stream = make_stream(Struct("<IHH").pack(0, AdsState.CONFIG, 0))

        # Act
        response = AdsStateResponse.deserialize(stream)

        # Assert
        self.assertIs(response.ads_state, AdsState.CONFIG)

    def test_deserialize_wrong_length_raises_value_error(self) -> None:
        # Arrange
        stream = make_stream(Struct("<IH").pack(0, 5))

        # Act / Assert
        with self.assertRaises(ValueError) as ctx:
            AdsStateResponse.deserialize(stream)

        self.assertIn("Invalid data length", str(ctx.exception))


class TestReadStateCommand(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = make_transport()
        self.command = ReadStateCommand(
            transport=self.transport,
            ams_address=make_ams_address(),
        )

    def test_serialize_returns_empty_payload(self) -> None:
        # Act / Assert
        self.assertEqual(self.command.serialize(), b"")

    async def test_request_returns_state_response(self) -> None:
        # Arrange
        payload = AdsStateResponse(AdsErrorCode(0), AdsState.RUN, AdsDeviceState.OKAY).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        response = await self.command.request()

        # Assert
        self.assertEqual(response.ads_state, AdsState.RUN)

    async def test_request_uses_read_state_command_id(self) -> None:
        # Arrange
        payload = AdsStateResponse(AdsErrorCode(0), AdsState.IDLE, AdsDeviceState.OKAY).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act
        await self.command.request()

        # Assert
        self.assertEqual(
            self.transport.request.call_args.kwargs["command_id"], AdsCommandId.READ_STATE
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
        payload = AdsStateResponse(AdsErrorCode(1799), AdsState.ERROR, AdsDeviceState.OKAY).serialize()
        self.transport.request = AsyncMock(
            return_value=(make_ams_header(), make_stream(payload))
        )

        # Act / Assert
        with self.assertRaises(AdsCommandError) as ctx:
            await self.command.request()

        self.assertEqual(ctx.exception.error_code, 1799)


if __name__ == "__main__":
    unittest.main()
