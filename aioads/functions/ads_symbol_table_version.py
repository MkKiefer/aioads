"""
This module contains the ads function call for requesting the `SymbolTableVersion`.


This can be used to monitoring, on change the index offsets in symbol metadata can change
and we need to re resolve the new address where we can read this symbol, if we don't do this
we read a random memory array we most likely can't parse.
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adsdll2/124830987.html&id=

"""

from struct import Struct
from typing import Final

from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.transport import ITransport


class SymbolTableVersion(IAdsFunction[int]):
    """
    Ads function to read the symbol table version
    """

    VERSION_STRUCT_DEF: Final[Struct] = Struct("<B")

    def __init__(self, transport: ITransport, ams_address: AmsAddress) -> None:
        self.transport = transport
        self.ams_address = ams_address

    def serialize(self) -> bytes:
        """serialize function payload to bytes"""
        return b""

    async def execute(self) -> int:
        command = AdsReadCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SYM_TABLE_VERSION,
            idx_offset=0,
            length=1,
        )
        header, read_stream = await command.request()
        if not header.error_code.ok:
            raise AdsCommandError(
                header.error_code, "Failed to read symbol table version"
            )
        return read_stream.read_struct(self.VERSION_STRUCT_DEF)[0]
