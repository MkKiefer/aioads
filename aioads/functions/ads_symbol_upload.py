"""
This module contains a ads function to acquire the symbol infos (metadata) of the global / root symbols.  
"""

from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.functions.ads_symbol_info_by_name_ex import SymbolInfo
from aioads.transport import ITransport


class AdsSymbolUpload(IAdsFunction[list[SymbolInfo]]):
    """
    Ads Function to read all global symbols. 
    Hint: 
        Only the top most symbol information are returned, to get the full ads tree
        you need to resolve it from this over the datatypes that can be requested with `AdsSymbolDataTypeUpload`
    """

    def __init__(
        self, transport: ITransport, ams_address: AmsAddress, tree_size: int
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.tree_size = tree_size

    def serialize(self) -> bytes:
        """serialize function payload to bytes"""
        return b""

    async def execute(self):
        command = AdsReadCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SYM_UPLOAD,
            idx_offset=0,
            length=self.tree_size,
        )
        _, read_payload = await command.request()
        symbol_infos = list[SymbolInfo]()

        start = read_payload.tell()
        remaining_length = read_payload.length - start
        if self.tree_size != remaining_length:
            raise ValueError(
                f"Expected tree size {self.tree_size}, but got {remaining_length} bytes"
            )
        while read_payload.tell() < start + self.tree_size:
            symbol_info = SymbolInfo.deserialize(read_payload)
            symbol_infos.append(symbol_info)
        return symbol_infos
