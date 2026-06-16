"""
ADS Symbol DataType By Name function implementation.
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_adsdll2/124833547.html&id=

```mermaid
---
title: "ADS Symbol Datatype Response"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "entry length (4 bytes)"
+4: "version (4 bytes)"
+4: "hash value (4 bytes)"
+4: "type hash value (4 bytes)"
+4: "size (4 bytes)"
+4: "offset (4 bytes)"
+4: "datatype (AdsSymbolDataType) (4 bytes)"
+4: "flags (AdsSymbolFlags) (4 bytes)"
+2: "name length (2 bytes)"
+2: "type name length (2 bytes)"
+2: "comment length (2 bytes)"
+2: "name length (2 bytes)"
+2: "array count (2 bytes)"
+2: "subitems count (2 bytes)"
+12: "name ($name length)"
+8: "type name ($type name length)"
+8: "comment ($comment length)"
+8: "ArrayElements ($array count)"
+8: "SubDataType ($array count)"
```


"""

from dataclasses import dataclass
from struct import Struct
from typing import ClassVar
from aioads.ams_address import AmsAddress
from aioads.commands.ads_read_write import AdsReadWriteCommand
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.functions.ads_symbol_info_by_name_ex import (
    AdsSymbolDataType,
    AdsSymbolFlags,
)
from aioads.stream import AdsStream
from aioads.transport import ITransport


@dataclass(frozen=True, slots=True)
class AdsDatatypeArrayInfo:
    """
    Array bounds struct for ADS Symbol DataType information.
    """

    l_bound: int
    e_elements: int

    STRUCT_DEF: ClassVar[Struct] = Struct("<II")

    def serialize(self) -> bytes:
        """
        serialize the `AdsDatatypeArrayInfo` to bytes
        """
        data = b""
        data += self.l_bound.to_bytes(4, byteorder="little")
        data += self.e_elements.to_bytes(4, byteorder="little")
        return data

    @staticmethod
    def deserialize(data: AdsStream) -> "AdsDatatypeArrayInfo":
        """
        Create `AdsDatatypeArrayInfo` from the `AdsStream`
        """
        l_bound, e_elements = data.read_struct(AdsDatatypeArrayInfo.STRUCT_DEF)
        return AdsDatatypeArrayInfo(
            l_bound=l_bound,
            e_elements=e_elements,
        )


@dataclass(frozen=True, slots=True)
class SymbolDataTypeResponse:
    """
    ADS Datatype / Struct information response.
    """

    version: int
    hash_value: int
    type_hash_value: int
    size: int
    offs: int
    data_type: AdsSymbolDataType
    flags: AdsSymbolFlags
    name: str
    type_name: str
    comment: str
    array: list[AdsDatatypeArrayInfo]
    sub_items: list["SymbolDataTypeResponse"]

    # entry_length, version, hash_value, type_hash_value, size, offs,
    # data_type, flags, name_length, type_name_length, comment_length,
    # array_dim, sub_items_count
    FIXED_STRUCT: ClassVar[Struct] = Struct("<8I5H")

    def serialize(self) -> bytes:
        """
        Serialize the SymbolDataTypeResponse into bytes.
        :return: Serialized bytes of the SymbolDataTypeResponse.
        """
        data = b""
        data += self.version.to_bytes(4, byteorder="little")
        data += self.hash_value.to_bytes(4, byteorder="little")
        data += self.type_hash_value.to_bytes(4, byteorder="little")
        data += self.size.to_bytes(4, byteorder="little")
        data += self.offs.to_bytes(4, byteorder="little")
        data += self.data_type.to_bytes(4, byteorder="little")
        data += self.flags.to_bytes(4, byteorder="little")
        name_encoded = self.name.encode("cp1252") + b"\x00"
        type_encoded = self.type_name.encode("cp1252") + b"\x00"
        comment_encoded = self.comment.encode("cp1252") + b"\x00"

        data += (len(name_encoded) - 1).to_bytes(2, byteorder="little")
        data += (len(type_encoded) - 1).to_bytes(2, byteorder="little")
        data += (len(comment_encoded) - 1).to_bytes(2, byteorder="little")
        data += len(self.array).to_bytes(2, byteorder="little")
        data += len(self.sub_items).to_bytes(2, byteorder="little")

        data += name_encoded
        data += type_encoded
        data += comment_encoded
        for array_info in self.array:
            data += array_info.serialize()
        for sub_item in self.sub_items:
            data += sub_item.serialize()

        entry_length = (len(data) + 4).to_bytes(
            4, byteorder="little"
        )  # Including the entry length field itself
        return entry_length + data

    @staticmethod
    def deserialize(data: AdsStream) -> "SymbolDataTypeResponse":
        """
        Deserialize bytes into a SymbolDataTypeResponse.
        """

        start_pos = data.tell()
        (
            entry_length,
            version,
            hash_value,
            type_hash_value,
            size,
            offs,
            data_type,
            flags,
            name_length,
            type_length,
            comment_length,
            array_dim,
            sub_items_cnt,
        ) = data.read_struct(SymbolDataTypeResponse.FIXED_STRUCT)
        name = data.read(name_length + 1).rstrip(b"\x00").decode("cp1252")
        type_name = data.read(type_length + 1).rstrip(b"\x00").decode("cp1252")
        comment = data.read(comment_length + 1).rstrip(b"\x00").decode("cp1252")
        array_info: list[AdsDatatypeArrayInfo] = []
        for _ in range(array_dim):
            array_info.append(AdsDatatypeArrayInfo.deserialize(data))
        sub_items: list[SymbolDataTypeResponse] = []
        for _ in range(sub_items_cnt):
            sub_items.append(SymbolDataTypeResponse.deserialize(data))

        # Move the stream position to the end of the entry
        data.seek(start_pos + entry_length)

        return SymbolDataTypeResponse(
            version=version,
            hash_value=hash_value,
            type_hash_value=type_hash_value,
            size=size,
            offs=offs,
            data_type=AdsSymbolDataType(data_type),
            flags=AdsSymbolFlags(flags),
            name=name,
            type_name=type_name,
            comment=comment,
            array=array_info,
            sub_items=sub_items,
        )


class AdsSymbolDataTypeByName(IAdsFunction[SymbolDataTypeResponse]):
    """
    ADS Symbol DataType By Name function to read datatype information by its name.
    """

    def __init__(
        self, transport: ITransport, ams_address: AmsAddress, datatype_name: str
    ) -> None:
        self.transport = transport
        self.ams_address = ams_address
        self.datatype_name = datatype_name

    def serialize(self) -> bytes:
        """
        Serialize the request.
        """
        return self.datatype_name.encode("utf-8") + b"\x00"

    async def execute(self) -> SymbolDataTypeResponse:
        """
        Request execution of the ADS Symbol DataType By Name function.
        :return: SymbolDataTypeResponse containing a single datatype information.
        """
        payload = self.serialize()
        command = AdsReadWriteCommand(
            ams_address=self.ams_address,
            transport=self.transport,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_SYM_DT_INFOBYNAMEEX,
            idx_offset=0,
            write_data=payload,
            write_length=len(payload),
            read_length=0xFFFF,  # Max read length
        )
        header, read_payload = await command.request()
        if not header.error_code.ok:
            raise AdsCommandError(
                header.error_code, "Failed to read symbol datatype by name"
            )
        symbol_datatype = SymbolDataTypeResponse.deserialize(read_payload)
        return symbol_datatype
