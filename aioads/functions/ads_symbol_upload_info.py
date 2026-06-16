"""
This module provides the ads function to get basic datatype and symbol information from the plc.
This is required (`AdsSymbolDataTypeUpload`) to get in to more details for example to acquire all datatype definitions.

"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar

from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction

from aioads.stream import AdsStream
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class SymbolUploadInfo2Response:
    """
    Symbol and Datatype basic information's
    """

    symbol_cnt: int
    symbol_size: int
    symbols_max_dynamic_cnt: int
    symbol_used_dynamic_cnt: int
    datatype_cnt: int
    datatype_size: int

    STRUCT_DEF: ClassVar[Struct] = Struct("<IIIIII")

    def serialize(self) -> bytes:
        """serialize `SymbolUploadInfo2Response` payload to bytes"""

        return self.STRUCT_DEF.pack(
            self.symbol_cnt,
            self.symbol_size,
            self.datatype_cnt,
            self.datatype_size,
            self.symbols_max_dynamic_cnt,
            self.symbol_used_dynamic_cnt,
        )

    @classmethod
    def deserialize(cls, data: AdsStream) -> "SymbolUploadInfo2Response":
        """
        Create `SymbolUploadInfo2Response` from the `AdsStream`
        """
        parsed: tuple[int, int, int, int, int, int] = data.read_struct(cls.STRUCT_DEF)
        return cls(
            symbol_cnt=parsed[0],
            symbol_size=parsed[1],
            datatype_cnt=parsed[2],
            datatype_size=parsed[3],
            symbols_max_dynamic_cnt=parsed[4],
            symbol_used_dynamic_cnt=parsed[5],
        )


class AdsSymbolUploadInfo2(IAdsFunction[SymbolUploadInfo2Response]):
    """
    Function to acquire basic symbol information
    """

    def __init__(self, transport: ITransport, ams_address: AmsAddress) -> None:
        self.transport = transport
        self.ams_address = ams_address

    def serialize(self) -> bytes:
        """serialize function payload to bytes"""
        return b""

    async def execute(self):
        command = AdsReadCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SYM_UPLOADINFO_2,
            idx_offset=0,
            # 6 x 4 bytes (We expect a struct with 6x UINT32)
            length=SymbolUploadInfo2Response.STRUCT_DEF.size,
        )
        header, read_payload = await command.request()
        if not header.error_code.ok:
            raise AdsCommandError(
                header.error_code, "Failed to request symbol upload info"
            )
        symbol_upload_info = SymbolUploadInfo2Response.deserialize(read_payload)
        return symbol_upload_info
