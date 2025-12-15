"""
ADS Read Device Info command implementation.

```mermaid
---
title: "ADS Read Device Info Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "error code (4 bytes)"
+1: "major version (1 byte)"
+1: "minor version (1 byte)"
+2: "version build (2 bytes)"
+20: "device name (20 bytes)"
```


"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar
from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.stream import AdsStream
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class AdsReadDeviceInfoResponse:
    """
    Response object for `AdsReadDeviceInfo` command.
    """

    STRUCT_DEF: ClassVar[Struct] = Struct("<IBBH20s")

    error_code: AdsErrorCode
    major_version: int
    minor_version: int
    version_build: int
    device_name: str

    def version_string(self) -> str:
        """Get the runtime version as a combined string"""
        return f"{self.major_version}.{self.minor_version}.{self.version_build}"

    def serialize(self) -> bytes:
        """
        serialize the `DeviceInfo` content 
        """
        if len(self.device_name) > 20:
            raise ValueError("Device name must be at most 20 characters long")

        return self.STRUCT_DEF.pack(
            self.error_code,
            self.major_version,
            self.minor_version,
            self.version_build,
            self.device_name.strip().encode("utf-8").ljust(20, b"\x00"),
        )

    @classmethod
    def deserialize(cls, stream: AdsStream) -> "AdsReadDeviceInfoResponse":
        """
        Create `DeviceInfo` from the ads stream
        """
        if stream.length < 28:
            raise ValueError(
                "Invalid data length for AdsReadDeviceInfoResponse")
        parsed: tuple[
            int, int, int, int, bytes
        ] = stream.read_struct(cls.STRUCT_DEF)

        return cls(
            error_code=AdsErrorCode(parsed[0]),
            major_version=parsed[1],
            minor_version=parsed[2],
            version_build=parsed[3],
            device_name=parsed[4].decode("utf-8").rstrip("\x00"),
        )


class AdsReadDeviceInfo(ICommand[AdsReadDeviceInfoResponse]):
    """
    The ads command to read the device information
    """

    def __init__(self, transport: ITransport, ams_address: AmsAddress) -> None:
        self.transport = transport
        self.ams_address = ams_address

    def serialize(self) -> bytes:
        return b""

    async def request(self) -> AdsReadDeviceInfoResponse:
        cmd_payload = self.serialize()
        ams_header, ams_payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.READ_DEVICE_INFO,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsReadDeviceInfoResponse.deserialize(ams_payload)
        if not response.error_code.ok:
            raise AdsCommandError(response.error_code)
        return response
