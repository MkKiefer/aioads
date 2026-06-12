"""
The AMS Address Struct for ADS protocol communication.
This includes the Net ID and Port required for addressing.
https://infosys.beckhoff.com/english.php?content=../content/1033/tcadscommon/12440276875.html&id=

```mermaid
---
title: "AMS Address"
config:
    packet:
        rowHeight: 45
        bitWidth: 32
        bitsPerRow: 8
        showBits: true
---
packet
+6: "AMSNetId (6 bytes)"
+2: "AMSPort (2 bytes)"
```
"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class AmsAddress:
    """
    The AMS Address Struct for ADS protocol communication.
    This includes the Net ID and Port required for addressing.
    """

    net_id: str
    port: int

    STRUCT_DEF: ClassVar[Struct] = Struct("<6sH")

    def serialize(self) -> bytes:
        """
        Serialize the AMS Address into bytes.
        """
        net_id_parts = bytes(int(part) for part in self.net_id.split("."))
        return self.STRUCT_DEF.pack(net_id_parts, self.port)

    @staticmethod
    def deserialize(data: bytes) -> "AmsAddress":
        """
        Deserialize bytes into an AMS Address.
        """
        net_id_bytes, port = AmsAddress.STRUCT_DEF.unpack(data)
        net_id = ".".join(str(b) for b in net_id_bytes)
        return AmsAddress(net_id=net_id, port=port)
