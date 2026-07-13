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
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_function import (
    AdsFunctionSymbolGroup,
    IAdsFunction,
)
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsSumRead(IAdsFunction[list[tuple[AdsReadResponse, AdsStream]]]):
    """
    Ads sum read command that batches `AdsReadCommand` calls.
    The PLC accepts at most 500 commands per sum command; larger command lists
    are transparently split into multiple sum commands of at most `batch_size`.
    """

    ERROR_CODE_STRUCT_DEF: Final[Struct] = Struct("<I")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        commands: list[AdsReadCommand],
        batch_size: int,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.commands = commands
        self.batch_size = batch_size

    def serialize(self, commands: list[AdsReadCommand]) -> bytes:
        """
        serialize the given commands to bytes
        """
        return b"".join(command.serialize() for command in commands)

    async def execute(self) -> list[tuple[AdsReadResponse, AdsStream]]:
        if len(self.commands) == 0:
            raise ValueError(
                "At least one command is required for ADS Sum Read")

        if self.batch_size <= 0 or self.batch_size > 500:
            raise ValueError("Batch size must be between 1 and 500")

        response: list[tuple[AdsReadResponse, AdsStream]] = []
        for batch_start in range(0, len(self.commands), self.batch_size):
            batch = self.commands[batch_start: batch_start + self.batch_size]
            response.extend(await self._execute_batch(batch))
        return response

    async def _execute_batch(
        self, commands: list[AdsReadCommand]
    ) -> list[tuple[AdsReadResponse, AdsStream]]:
        payload = self.serialize(commands)
        command = AdsReadWriteCommand(
            transport=self.transport,
            ams_address=self.ams_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SUM_READ,
            idx_offset=len(commands),
            read_length=sum(
                cmd.length + 4 for cmd in commands
            ),  # +4 for the error code per command
            write_length=len(payload),
            write_data=payload,
        )
        header, read_payload = await command.request()
        if not header.error_code.ok:
            raise AdsCommandError(
                header.error_code, "Failed to execute ADS Sum Read")
        error_stream = read_payload.sub_stream(
            # 4 bytes per error code
            len(commands)
            * self.ERROR_CODE_STRUCT_DEF.size
        )

        response: list[tuple[AdsReadResponse, AdsStream]] = []
        for cmd in commands:
            error_code = AdsErrorCode(
                error_stream.read_struct(self.ERROR_CODE_STRUCT_DEF)[0]
            )
            response.append(
                (
                    AdsReadResponse(
                        error_code=error_code,
                        length=cmd.length,
                    ),
                    read_payload.sub_stream(cmd.length),
                )
            )
        return response
