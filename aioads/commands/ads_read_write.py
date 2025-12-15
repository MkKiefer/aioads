"""
ADS Read/Write command implementation.

```mermaid
---
title: "ADS Read/Write Request"
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
+4: "write length (4 bytes)"
+N: "write data (write_length bytes)"
```

"""

from struct import Struct
from typing import Final
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.ads_read import AdsReadResponse
from aioads.ams_address import AmsAddress
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsReadWriteCommand(ICommand[tuple[AdsReadResponse, AdsStream]]):
    """
    Ads command to write and read with a single command. 
    - This is mostly used by ads function calls like `symbol_info_by_name`
    """

    SERIALIZE_HEADER_STRUCT: Final[Struct] = Struct("<IIII")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        idx_group: int,
        idx_offset: int,
        read_length: int,
        write_length: int,
        write_data: bytes,
    ) -> None:

        self.transport = transport
        self.ams_address = ams_address
        self.idx_group = idx_group
        self.idx_offset = idx_offset
        self.read_length = read_length
        self.write_length = write_length
        self.payload = write_data

    def serialize_header(self) -> bytes:
        """
        serialize only the header part of this request. 
        Required for sum read / write command where we re organize the data. 
        First all headers then all payload. 
        """
        return self.SERIALIZE_HEADER_STRUCT.pack(
            self.idx_group,
            self.idx_offset,
            self.read_length,
            self.write_length,
        )

    def serialize(self) -> bytes:
        data = self.serialize_header()
        data += self.payload
        return data

    async def request(self) -> tuple[AdsReadResponse, AdsStream]:
        cmd_payload = self.serialize()
        ams_header, ads_stream = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.READ_WRITE,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)
        read_header = AdsReadResponse.deserialize(ads_stream)
        if not read_header.error_code.ok:
            raise AdsCommandError(read_header.error_code)
        return read_header, ads_stream.sub_stream(read_header.length)
