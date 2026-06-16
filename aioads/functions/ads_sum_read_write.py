"""
ADS Sum Read/Write function implementation.
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adssamples_net/185258507.html&id=
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adsdll2/124830987.html&id=


```mermaid
---
title: "ADS Sum Read/Write Request"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "1 - idx group (4 bytes)"
+4: "1 - idx offset (4 bytes)"
+4: "1 - read length (4 bytes)"
+4: "1 - write length (4 bytes)"
+4: "2 - idx group (4 bytes)"
+4: "2 - idx offset (4 bytes)"
+4: "2 - read length (4 bytes)"
+4: "2 - write length (4 bytes)"
+8: "1 - payload ($write length bytes)"
+8: "2 - payload ($write length bytes)"

```


```mermaid
---
title: "ADS Sum Read/Write Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "1 - error code (4 bytes)"
+4: "1 - length (4 bytes)"
+4: "2 - error code (4 bytes)"
+4: "2 - length (4 bytes)"
+8: "1 - payload ($length bytes)"
+8: "2 - payload ($length bytes)"
```

"""

from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadResponse
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsSumReadWrite(IAdsFunction[list[tuple[AdsReadResponse, AdsStream]]]):
    """
    ADS Sum Read/Write function to send a READ/WRITE Command in batch.
    This function allows multiple read/write commands to be sent in a single request,
    reducing communication overhead.
    """

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        commands: list[AdsReadWriteCommand],
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.commands = commands

    def serialize(self) -> bytes:
        """
        Serialize the ADS Sum Read/Write commands into bytes.
        :return: Serialized bytes of the sum read/write command.
        """
        data = b"".join(cmd.serialize_header() for cmd in self.commands)
        data += b"".join(cmd.payload for cmd in self.commands)
        return data

    async def execute(self) -> list[tuple[AdsReadResponse, AdsStream]]:
        """
        Request execution of the ADS Sum Read/Write function.
        Returns an async generator yielding the responses for each command in the batch.
        :return: An async generator yielding tuples of (AdsReadResponse, AdsStream) for each command.
        """
        if len(self.commands) == 0:
            raise ValueError("At least one command is required for ADS Sum Read/Write")
        if len(self.commands) > 500:
            raise ValueError("Too many commands for ADS Sum Read/Write, maximum is 500")

        payload = self.serialize()
        total_read_length = sum(
            cmd.read_length + 8 for cmd in self.commands
        )  # 8 bytes for each error code
        total_write_length = sum(
            cmd.write_length + (4 * 4) for cmd in self.commands
        )  # 8 Bytes for the fixed header
        assert total_write_length == len(
            payload
        ), "Calculated write length does not match payload length"
        command = AdsReadWriteCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SUM_READ_WRITE,
            idx_offset=len(self.commands),
            read_length=total_read_length,
            write_length=total_write_length,
            write_data=payload,
        )
        header, read_payload = await command.request()
        if not header.error_code.ok:
            raise AdsCommandError(
                header.error_code, "Failed to execute ADS Sum Read/Write"
            )
        read_response_stream = read_payload.sub_stream(
            len(self.commands) * AdsReadResponse.STRUCT_DEF.size
        )

        response: list[tuple[AdsReadResponse, AdsStream]] = []
        for _ in self.commands:
            read_response = AdsReadResponse.deserialize(read_response_stream)
            response.append(
                (read_response, read_payload.sub_stream(read_response.length))
            )
        return response
