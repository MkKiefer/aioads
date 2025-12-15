"""
ADS Read command implementation.

```mermaid
---
title: "ADS Read Request"
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
+4: "read length (4 bytes)"
```

```mermaid
---
title: "ADS Read Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "error code (4 bytes)"
+4: "length (4 bytes)"
+8: "data (length bytes)"
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
class AdsReadResponse:
    """
    The response struct for `AdsReadCommand`
    """

    STRUCT_DEF: ClassVar[Struct] = Struct("<II")

    error_code: AdsErrorCode
    length: int

    def serialize(self) -> bytes:
        """
        serialize `AdsReadResponse` to bytes
        """
        return self.STRUCT_DEF.pack(
            self.error_code,
            self.length,
        )

    @staticmethod
    def deserialize(
        stream: AdsStream,
    ) -> "AdsReadResponse":
        """
        Deserialize `AdsReadResponse` from `AdsStream`.
        """
        error_code, length = stream.read_struct(AdsReadResponse.STRUCT_DEF)
        return AdsReadResponse(
            error_code=AdsErrorCode(error_code),
            length=length,
        )


class AdsReadCommand(ICommand[tuple[AdsReadResponse, AdsStream]]):
    """
    Ads command to read from index group, index offset. 
    """

    SERIALIZE_STRUCT_DEF: Final[Struct] = Struct("<III")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        idx_group: int,
        idx_offset: int,
        length: int,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.idx_group = idx_group
        self.idx_offset = idx_offset
        self.length = length

    def serialize(self) -> bytes:
        return self.SERIALIZE_STRUCT_DEF.pack(
            self.idx_group,
            self.idx_offset,
            self.length,
        )

    async def request(self) -> tuple[AdsReadResponse, AdsStream]:
        cmd_payload = self.serialize()
        ams_header, ams_payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.READ,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)

        read_header = AdsReadResponse.deserialize(ams_payload)
        if not read_header.error_code.ok:
            raise AdsCommandError(read_header.error_code)
        return read_header, ams_payload.sub_stream(read_header.length)
