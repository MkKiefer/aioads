"""
This module provides models and ads function to get the symbol info (symbol metadata) by the variable / symbol name. 
"""

from dataclasses import dataclass
from enum import IntEnum, IntFlag
from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.functions.ads_sum_read_write import AdsSumReadWrite
from aioads.stream import AdsStream
from aioads.transport import ITransport


class AdsSymbolDataType(IntEnum):
    """Base ads data types"""

    VOID = 0
    INT8 = 16
    UINT8 = 17
    INT16 = 2
    UINT16 = 18
    INT32 = 3
    UINT32 = 19
    INT64 = 20
    UINT64 = 21
    REAL32 = 4
    REAL64 = 5
    STRING = 30
    WSTRING = 31
    REAL80 = 32
    BIT = 33
    BIGTYPE = 65
    MAXTYPES = 67


class AdsSymbolFlags(IntFlag):
    """The flags that can be set for a symbol
    What the different flags mean can be found in the TwinCAT ADS documentation
    """

    PERSISTENT = 1
    BITVALUE = 2
    REFERENCETO = 4
    TYPEGUID = 8
    TCCOMIFACEPTR = 16
    READONLY = 32
    CONTEXTMASK = 3840


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    """The symbol info class contains all information about a symbol (variable) of the PLC"""

    idx_group: int
    idx_offset: int
    idx_length: int
    data_type: AdsSymbolDataType
    symbol_flags: AdsSymbolFlags
    symbol_name: str
    type_name: str
    comment: str

    def serialize(self) -> bytes:
        """
        Serialize the `SymbolInfo` to bytes
        """
        data = b""
        data += self.idx_group.to_bytes(4, byteorder="little")
        data += self.idx_offset.to_bytes(4, byteorder="little")
        data += self.idx_length.to_bytes(4, byteorder="little")
        data += self.data_type.to_bytes(4, byteorder="little")
        data += self.symbol_flags.to_bytes(4, byteorder="little")

        symbol_name_encoded = self.symbol_name.encode("cp1252") + b"\x00"
        type_name_encoded = self.type_name.encode("cp1252") + b"\x00"
        comment_encoded = self.comment.encode("cp1252") + b"\x00"

        data += (len(symbol_name_encoded) - 1).to_bytes(2, byteorder="little")
        data += (len(type_name_encoded) - 1).to_bytes(2, byteorder="little")
        data += (len(comment_encoded) - 1).to_bytes(2, byteorder="little")
        data += symbol_name_encoded
        data += type_name_encoded
        data += comment_encoded

        entry_length = (len(data) + 4).to_bytes(
            4, byteorder="little"
        )  # Including the entry length field itself

        return entry_length + data

    @staticmethod
    def deserialize(data: AdsStream) -> "SymbolInfo":
        """
        Create `SymbolInfo` from the `AdsStream`
        """
        start_pos = data.tell()
        entry_length = int.from_bytes(data.read_view(4), byteorder="little")
        idx_group = int.from_bytes(data.read_view(4), byteorder="little")
        idx_offset = int.from_bytes(data.read_view(4), byteorder="little")
        idx_length = int.from_bytes(data.read_view(4), byteorder="little")
        data_type = AdsSymbolDataType.from_bytes(
            data.read_view(4), byteorder="little")
        symbol_flags = AdsSymbolFlags.from_bytes(
            data.read_view(4), byteorder="little")
        symbol_name_length = int.from_bytes(
            data.read_view(2), byteorder="little") + 1
        type_name_length = int.from_bytes(
            data.read_view(2), byteorder="little") + 1
        comment_length = int.from_bytes(
            data.read_view(2), byteorder="little") + 1
        symbol_name = data.read(symbol_name_length).rstrip(
            b"\x00").decode("cp1252")
        type_name = data.read(type_name_length).rstrip(
            b"\x00").decode("cp1252")
        comment = data.read(comment_length).rstrip(b"\x00").decode("cp1252")

        # Move the stream position to the end of the entry
        data.seek(start_pos + entry_length)

        return SymbolInfo(
            idx_group=idx_group,
            idx_offset=idx_offset,
            idx_length=idx_length,
            data_type=data_type,
            symbol_flags=symbol_flags,
            symbol_name=symbol_name,
            type_name=type_name,
            comment=comment,
        )


class SymbolInfoByNameEx(IAdsFunction[SymbolInfo]):
    """
    This ads function allows reading of the symbol info (metadata of a variable) from the PLC
    """

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        symbol_name: str,
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.symbol_name = symbol_name

    def serialize(self) -> bytes:
        """serialize function payload to bytes"""
        symbol_path_normalized = self.symbol_name.upper()
        return symbol_path_normalized.encode("cp1252") + b"\x00"

    async def execute(self):
        payload = self.serialize()
        command = AdsReadWriteCommand(
            ams_address=self.ams_address,
            transport=self.transport,
            idx_group=AdsFunctionSymbolGroup.GET_INFO_BY_NAME,
            idx_offset=0,
            write_length=len(payload),
            read_length=0xFFFF,  # Max read length
            write_data=payload,
        )
        _, read_payload = await command.request()
        symbol_info = SymbolInfo.deserialize(read_payload)
        return symbol_info


class SymbolInfoByNameExSumRead(
    IAdsFunction[list[tuple[AdsErrorCode, SymbolInfo]]]
):
    """
    This function utilizes the `AdsSumReadWrite` function to
    send multiple `SymbolInfoByNameEx` function calls in a batch.
    """

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        symbol_names: list[str],
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.symbol_names = symbol_names

    def create_sub_commands(self) -> list[AdsReadWriteCommand]:
        """
        Create multiple function commands like `SymbolInfoByNameEx` for the batch read
        """
        commands = []
        for symbol_name in self.symbol_names:
            symbol_path_normalized = symbol_name.upper()
            payload = symbol_path_normalized.encode("cp1252") + b"\x00"
            command = AdsReadWriteCommand(
                ams_address=self.ams_address,
                transport=self.transport,
                idx_group=AdsFunctionSymbolGroup.GET_INFO_BY_NAME,
                idx_offset=0,
                write_length=len(payload),
                read_length=0xFFFF,  # Max read length
                write_data=payload,
            )
            commands.append(command)
        return commands

    async def execute(self):
        commands = self.create_sub_commands()
        command = AdsSumReadWrite(
            transport=self.transport,
            ams_address=self.ams_address,
            commands=commands,
        )
        response = list[tuple[AdsErrorCode, SymbolInfo]]()
        for read_header, read_payload in await command.execute():
            symbol_info = SymbolInfo.deserialize(read_payload)
            response.append((read_header.error_code, symbol_info))
        return response
