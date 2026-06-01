"""
https://infosys.beckhoff.com/english.php?content=../content/1033/tc3_ads_intro/index.html&id=
Module provides the command payload and response classes as well as the command itself.
"""

from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import ClassVar, Final
from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.commands.ads_command import AdsCommandId, ICommand
from aioads.commands.errors import AdsCommandError
from aioads.errors import AdsAmsHeaderError
from aioads.stream import AdsStream
from aioads.transport import ITransport


class TransmissionMode(IntEnum):
    """Transmission mode for ADS notifications
    For more information, see the TwinCAT ADS documentation or
    docs/transmission_mode.md in this project.
    """

    NONE = 0
    CLIENT_CYCLE = 1
    CLIENT_ON_CHANGE = 2
    CYCLIC = 3
    ON_CHANGE = 4
    CYCLIC_IN_CONTEXT = 5
    ON_CHANGE_IN_CONTEXT = 6


@dataclass(frozen=True, slots=True)
class AdsAddNotificationResponse:
    """
    Notification create response model

    """

    STRUCT_DEF: ClassVar[Struct] = Struct("<II")

    error_code: AdsErrorCode
    notification_handle: int

    def serialize(self) -> bytes:
        """
        serialize the command payload
        """
        data = self.STRUCT_DEF.pack(
            self.error_code,
            self.notification_handle,
        )
        return data

    @classmethod
    def deserialize(cls, stream: AdsStream) -> "AdsAddNotificationResponse":
        """
        Create `AdsAddNotificationResponse` from the `AdsStream`
        """

        if stream.length < 8:
            raise ValueError(
                "Invalid data length for AdsAddNotificationResponse")
        (
            error_code,
            notification_handle,
        ) = stream.read_struct(cls.STRUCT_DEF)
        return cls(
            error_code=AdsErrorCode(error_code),
            notification_handle=notification_handle,
        )


class AdsAddNotificationCommand(ICommand[AdsAddNotificationResponse]):
    """
    Ads command to create a ads notification (subscription to a remote resource)
    """

    SERIALIZE_STRUCT_DEF: Final[Struct] = Struct("<IIIIII16s")

    def __init__(
        self,
        transport: ITransport,
        ams_address: AmsAddress,
        idx_group: int,
        idx_offset: int,
        length: int,
        transmission_mode: TransmissionMode,
        max_delay: int,
        cycle_time: int,
    ) -> None:
        """
        Create a new AdsAddNotificationCommand instance
        :param transport: The AdsTcpTransport instance
        :param ams_address: The target AmsAddress
        :param idx_group: The index group for the notification
        :param idx_offset: The index offset for the notification
        :param length: Length of data in bytes, which should be sent per notification.
        :param transmission_mode: See description of the structure ADSTRANSMODE at the ADS-DLL.
        :param max_delay: At the latest after this time, the ADS Device Notification is called. The unit is 1ms.
        :param cycle_time: The ADS server checks if the value changes in this time slice. The unit is 1ms

        """
        self.transport = transport
        self.ams_address = ams_address
        self.idx_group = idx_group
        self.idx_offset = idx_offset
        self.length = length
        self.transmission_mode = transmission_mode
        self.max_delay = max_delay
        self.cycle_time = cycle_time

    def serialize(self) -> bytes:
        return self.SERIALIZE_STRUCT_DEF.pack(
            self.idx_group,
            self.idx_offset,
            self.length,
            self.transmission_mode,
            self.max_delay,
            self.cycle_time,
            bytes(16),  # Reserved
        )

    async def request(self) -> AdsAddNotificationResponse:
        cmd_payload = self.serialize()
        ams_header, ads_stream = await self.transport.request(
            command_payload=cmd_payload,
            command_id=AdsCommandId.ADD_DEVICE_NOTIFICATION,
            ams_address=self.ams_address,
        )
        if not ams_header.error_code.ok:
            raise AdsAmsHeaderError(ams_header.error_code)
        response = AdsAddNotificationResponse.deserialize(ads_stream)
        if not response.error_code.ok:
            raise AdsCommandError(response.error_code)
        return response
