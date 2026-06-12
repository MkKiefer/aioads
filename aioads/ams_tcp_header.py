"""
AMS TCP Header implementation for ADS protocol communication.
https://infosys.beckhoff.com/english.php?content=../content/1033/tcadscommon/12440276875.html&id=

```mermaid
---
title: "AMS/TCP Header"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+2: "reserved (2 bytes)"
+4: "length (4 bytes)"
```

"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class AmsTcpHeader:
    """
    ADS TCP Header structure.
    """

    length: int
    PREAMBLE: ClassVar[int] = 0x0000
    FIXED_SIZE: ClassVar[int] = 6
    STRUCT_DEF: ClassVar[Struct] = Struct("<HI")

    def serialize(self) -> bytes:
        """Serialize the AMS TCP Header into bytes."""
        return self.STRUCT_DEF.pack(
            self.PREAMBLE,
            self.length,
        )

    @staticmethod
    def deserialize(data: bytes) -> "AmsTcpHeader":
        """Deserialize bytes into an AMS TCP Header."""
        preamble, length = AmsTcpHeader.STRUCT_DEF.unpack(data)
        if preamble != AmsTcpHeader.PREAMBLE:
            raise ValueError(
                f"Invalid preamble: expected {AmsTcpHeader.PREAMBLE}, got {preamble}"
            )

        return AmsTcpHeader(length=length)
