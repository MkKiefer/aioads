"""
ADS Symbol DataType Upload function implementation.
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adsdll2/124833547.html&id=

"""

from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.functions.ads_symbol_datatype_by_name import SymbolDataTypeResponse
from aioads.transport import ITransport


class AdsSymbolDataTypeUpload(IAdsFunction[list[SymbolDataTypeResponse]]):
    """
    Ads function to get all `FB` and `struct` objects that can be used to
    reconstruct the ads symbol tree or just to parse the raw value of a ads symbol read.
    The size of the memory area where the struct is stored
    needs to be requested first with `AdsSymbolUploadInfo2` function
    """

    def __init__(
        self, transport: ITransport, ams_address: AmsAddress, dt_size: int
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.dt_size = dt_size

    def serialize(self) -> bytes:
        """serialize function payload to bytes"""
        return b""

    async def execute(self):
        command = AdsReadCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SYM_DT_UPLOAD,
            idx_offset=0,
            length=self.dt_size,
        )
        _, read_payload = await command.request()

        symbol_datatypes = list[SymbolDataTypeResponse]()
        start = read_payload.tell()

        # Verify we have the expected number of bytes
        remaining = read_payload.length - start
        if self.dt_size != remaining:
            raise ValueError(
                f"Expected datatype size {self.dt_size}, but got {remaining} bytes"
            )

        while read_payload.tell() < start + self.dt_size:
            symbol_dt = SymbolDataTypeResponse.deserialize(read_payload)
            symbol_datatypes.append(symbol_dt)
        return symbol_datatypes
