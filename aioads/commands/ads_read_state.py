"""
ADS Read State command implementation.
```mermaid
---
title: "ADS Read State Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "error code (4 bytes)"
+2: "ADS state (2 bytes)"
+2: "device state (2 bytes)"
```

"""

from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import ClassVar

from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsState(IntEnum):
    """The state of the Ads system"""

    VALID = 0
    IDLE = 1
    RESET = 2
    INIT = 3
    START = 4
    RUN = 5
    STOP = 6
    SAVECFG = 7
    LOADCFG = 8
    POWERFAILURE = 9
    POWERGOOD = 10
    ERROR = 11
    SHUTDOWN = 12
    SUSPEND = 13
    RESUME = 14
    CONFIG = 15  # system is in config mode
    RECONFIG = 16  # system should restart in config mode


class AdsDeviceState(IntEnum):
    """The state of the device
    This state is supplementary to the AdsState and can very
    based on the device. For this reason there are no real defined values here
    """

    OKAY = 0


@dataclass(frozen=True, slots=True)
class AdsStateResponse:
    """
    The response class for the `ReadStateCommand`
    """

    STRUCT_DEF:  ClassVar[Struct] = Struct("<IHH")

    error_code: AdsErrorCode
    ads_state: AdsState
    device_state: AdsDeviceState

    def serialize(self) -> bytes:
        """
        serialize `AdsStateResponse` to bytes
        """
        return self.STRUCT_DEF.pack(
            self.error_code,
            self.ads_state,
            self.device_state,
        )

    @classmethod
    def deserialize(cls, stream: AdsStream) -> "AdsStateResponse":
        """
        Create `AdsStateResponse` from `AdsStream`
        """

        if stream.length - stream.tell() != 8:
            raise ValueError("Invalid data length for AdsStateResponse")
        parsed: tuple[int, int, int] = stream.read_struct(cls.STRUCT_DEF)
        return cls(
            error_code=AdsErrorCode(parsed[0]),
            ads_state=AdsState(parsed[1]),
            device_state=AdsDeviceState(parsed[2]),
        )


class ReadStateCommand(ICommand[AdsStateResponse]):
    """
    Read the device state from the remote
    """

    def __init__(self, transport: ITransport, ams_address: AmsAddress) -> None:
        self.transport = transport
        self.ams_address = ams_address

    def serialize(self):
        return b""

    async def request(self) -> AdsStateResponse:
        cmd_payload = self.serialize()
        ams_header, ams_payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.READ_STATE,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsStateResponse.deserialize(ams_payload)
        if not response.error_code.ok:
            raise AdsCommandError(response.error_code)
        return response
