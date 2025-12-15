"""
Module for command interface definition and command constant definitions
"""

from abc import ABC, abstractmethod
from enum import IntEnum, IntFlag
from typing import Generic, TypeVar


CmdRespT = TypeVar("CmdRespT")


class ICommand(ABC, Generic[CmdRespT]):
    """
    The interface that every command needs to implement
    """

    @abstractmethod
    async def request(self) -> CmdRespT:
        """Send the request"""
        raise NotImplementedError()

    @abstractmethod
    def serialize(self) -> bytes:
        """Serialize the payload for the command"""
        raise NotImplementedError()


class AdsCommandId(IntEnum):
    """The AdsCommandId class is an enumeration of the ADS command IDs."""

    INVALID = 0
    READ_DEVICE_INFO = 1
    READ = 2
    WRITE = 3
    READ_STATE = 4
    WRITE_CONTROL = 5
    ADD_DEVICE_NOTIFICATION = 6
    DELETE_DEVICE_NOTIFICATION = 7
    DEVICE_NOTIFICATION = 8
    READ_WRITE = 9


class AdsCommandState(IntFlag):
    """Command state enumeration
    Indicates if the command is a request,
    response and or a command.
    A combination of these states is possible.

    :param IntFlag: _description_
    :type IntFlag: _type_
    """

    ADS_REQUEST = 0x0000
    ADS_RESPONSE = 0x0001
    ADS_COMMAND = 0x0004
