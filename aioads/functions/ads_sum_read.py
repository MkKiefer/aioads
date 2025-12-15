"""
This module provides a function call that allows
sending multiple `AdsReadCommand` in a single command

https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adsdll2/124830987.html&id=
"""


from struct import Struct
from typing import Final
from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand, AdsReadResponse
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsSumRead(IAdsFunction[list[tuple[AdsReadResponse, AdsStream]]]):
    """
    Ads sum read command that allows batching up to 500 `AdsReadCommand` in a single call    
    """

    ERROR_CODE_STRUCT_DEF: Final[Struct] = Struct("<I")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        commands: list[AdsReadCommand],
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.commands = commands

    def serialize(self) -> bytes:
        """
        serialize all commands to bytes 
        """
        return b"".join(command.serialize() for command in self.commands)

    async def execute(self) -> list[tuple[AdsReadResponse, AdsStream]]:
        if len(self.commands) == 0:
            raise ValueError(
                "At least one command is required for ADS Sum Read")
        if len(self.commands) > 500:
            raise ValueError(
                "Too many commands for ADS Sum Read, maximum is 500")

        payload = self.serialize()
        command = AdsReadWriteCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SUM_READ,
            idx_offset=len(self.commands),
            read_length=sum(
                cmd.length + 4 for cmd in self.commands
            ),  # +4 for the error code per command
            write_length=len(payload),
            write_data=payload,
        )
        _, read_payload = await command.request()
        error_stream = read_payload.sub_stream(
            # 4 bytes per error code
            len(self.commands) * self.ERROR_CODE_STRUCT_DEF.size)

        response: list[tuple[AdsReadResponse, AdsStream]] = []
        for cmd in self.commands:
            error_code = AdsErrorCode(
                error_stream.read_struct(self.ERROR_CODE_STRUCT_DEF)[0])
            response.append((
                AdsReadResponse(
                    error_code=error_code,
                    length=cmd.length,
                ),
                read_payload.sub_stream(cmd.length))
            )
        return response
