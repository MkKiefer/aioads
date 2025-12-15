"""
ADS Delete Notification command implementation.

```mermaid
---
title: "ADS Delete Notification Request"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "notification handle (4 bytes)"
```

"""
from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.ads_write import AdsWriteResponse
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.transport import ITransport


class AdsDeleteNotificationCommand(ICommand[AdsWriteResponse]):
    """
    Ads command to remove a registered ads notification by its handle
    """

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        notification_handle: int,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.notification_handle = notification_handle

    def serialize(self) -> bytes:
        return self.notification_handle.to_bytes(4, byteorder="little")

    async def request(self) -> AdsWriteResponse:
        cmd_payload = self.serialize()
        ams_header, ams_payload = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.DELETE_DEVICE_NOTIFICATION,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsWriteResponse.deserialize(ams_payload)
        if not response.error_code.ok:
            raise AdsCommandError(response.error_code)
        return response
