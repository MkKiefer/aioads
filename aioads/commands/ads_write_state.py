"""
ADS Write State command implementation.

```mermaid
---
title: "ADS Write State Request"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+2: "ADS state (2 bytes)"
+2: "device state (2 bytes)"
+4: "length of additional data (4 bytes)"
+0: "additional data (0 bytes)"
```

"""

from struct import Struct
from typing import Final

from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.ads_read_state import AdsDeviceState, AdsState
from aioads.commands.ads_write import AdsWriteResponse
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.transport import ITransport


class AdsWriteStateCommand(ICommand[AdsWriteResponse]):
    """
    Command to write / update a remote state (write control)
    """

    SERIALIZE_STRUCT_DEF: Final[Struct] = Struct("<HHI")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        ads_state: AdsState,
        device_state: AdsDeviceState,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.ads_state = ads_state
        self.device_state = device_state

    def serialize(self) -> bytes:
        return self.SERIALIZE_STRUCT_DEF.pack(
            self.ads_state,
            self.device_state,
            0,  # Length of additional data
            # No additional data
        )

    async def request(self) -> AdsWriteResponse:
        cmd_payload = self.serialize()
        ams_header, payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.WRITE_CONTROL,
            ams_address=self.ams_address,
        )
        if ams_header.error_code != 0:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsWriteResponse.deserialize(payload)
        if response.error_code != 0:
            raise AdsCommandError(response.error_code)
        return response
