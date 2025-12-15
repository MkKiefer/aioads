"""
AMS Header implementation for ADS protocol communication.
https://infosys.beckhoff.com/english.php?content=../content/1033/tcadscommon/12440276875.html&id=

```mermaid
---
title: "ADS/AMS Packet"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: ture
---
packet
+6: "AMSNetId Target (6 bytes)"
+2: "AMSPort Target  (2 bytes)"
+6: "AMSNetId Source (6 bytes)"
+2: "AMSPort Source  (2 bytes)"
+2: "Command Id (2 bytes)"
+2: "State Flags (2 bytes)"
+4: "Length (4 bytes)"
+4: "Error Code (4 bytes)"
+4: "Invoke Id (4 bytes)"
+16: "Data (...)"
```


"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar

from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, AdsCommandState
from aioads.stream import AdsStream


@dataclass(frozen=True, slots=True)
class AmsHeader:
    """
    The AMS Header Struct for ADS protocol communication.
    This includes all necessary fields for ADS Message Routing and
    errors for connectivity.
    """

    target_ams_address: AmsAddress
    source_ams_address: AmsAddress
    command_id: AdsCommandId
    command_flags: AdsCommandState
    command_length: int
    error_code: AdsErrorCode
    invoke_id: int

    FIXED_SIZE: ClassVar[int] = 32
    STRUCT_DEF: ClassVar[Struct] = Struct("<8s8sHHIII")

    def serialize(self) -> bytes:
        """Serialize the AMS Header into bytes."""
        return self.STRUCT_DEF.pack(
            self.target_ams_address.serialize(),
            self.source_ams_address.serialize(),
            self.command_id,
            self.command_flags,
            self.command_length,
            self.error_code,
            self.invoke_id,
        )

    @staticmethod
    def deserialize(data: AdsStream) -> "AmsHeader":
        """Deserialize bytes into an AMS Header."""
        (
            target_ams_address_bytes,
            source_ams_address_bytes,
            command_id,
            command_flags,
            command_length,
            error_code,
            invoke_id,
        ) = data.read_struct(AmsHeader.STRUCT_DEF)
        target_ams_address = AmsAddress.deserialize(target_ams_address_bytes)
        source_ams_address = AmsAddress.deserialize(source_ams_address_bytes)
        return AmsHeader(
            target_ams_address=target_ams_address,
            source_ams_address=source_ams_address,
            command_id=AdsCommandId(command_id),
            command_flags=AdsCommandState(command_flags),
            command_length=command_length,
            error_code=AdsErrorCode(error_code),
            invoke_id=invoke_id,
        )
