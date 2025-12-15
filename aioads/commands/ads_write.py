"""
ADS Write command implementation.

```mermaid
---
title: "ADS Write Request"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "index group (4 bytes)"
+4: "index offset (4 bytes)"
+4: "write length (4 bytes)"
+8: "write data (N bytes)"
```

```mermaid
---
title: "ADS Write Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "error code (4 bytes)"
```

"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar, Final

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.ams_address import AmsAddress
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.stream import AdsStream
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class AdsWriteResponse:
    """
    Write response class for `AdsWriteCommand`
    """
    STRUCT_DEF: ClassVar[Struct] = Struct("<I")

    error_code: AdsErrorCode

    def serialize(self) -> bytes:
        """
        serialize the object to bytes
        """
        data = b""
        data += self.error_code.to_bytes(4, byteorder="little")
        return data

    @classmethod
    def deserialize(cls, stream: AdsStream) -> "AdsWriteResponse":
        """
        Create `AdsWriteResponse` from the `AdsStream`
        """
        if stream.length < 4:
            raise ValueError("Invalid data length for AdsWriteResponse")
        error_code, = stream.read_struct(cls.STRUCT_DEF)
        return AdsWriteResponse(
            error_code=AdsErrorCode(error_code),
        )


class AdsWriteCommand(ICommand[AdsWriteResponse]):
    """
    Command to write to a specific location
    """

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        idx_group: int,
        idx_offset: int,
        payload: bytes,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.idx_group = idx_group
        self.idx_offset = idx_offset
        self.payload = payload

        self.serialize_struct_def: Final[Struct] = Struct(
            f"<III{len(payload)}s")

    def serialize(self) -> bytes:
        return self.serialize_struct_def.pack(
            self.idx_group,
            self.idx_offset,
            len(self.payload),
            self.payload,
        )

    async def request(self) -> AdsWriteResponse:
        cmd_payload = self.serialize()
        ams_header, payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.WRITE,
            ams_address=self.ams_address,
        )
        if ams_header.error_code != 0:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsWriteResponse.deserialize(payload)
        if response.error_code != 0:
            raise AdsCommandError(response.error_code)
        return response
