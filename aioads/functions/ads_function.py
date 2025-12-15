"""
General interface and enums for ADS functions and function constants.
"""

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Generic, TypeVar

FuncRespT = TypeVar("FuncRespT")


class IAdsFunction(ABC, Generic[FuncRespT]):
    """
    Interface for ADS functions.
    Defines the execute method that must be implemented by all ADS functions.
    """

    @abstractmethod
    async def execute(self) -> FuncRespT:
        """
        Execute the ADS function asynchronously.
        :return: The response of the ADS function.
        """
        raise NotImplementedError()


class AdsFunctionSymbolGroup(IntEnum):
    """Known symbol groups.
    This symbol groups a special meaning in the ADS protocol.
    This groups allows to call special functions on the PLC.
    On of them is the ads sum read command that allows to batch multiple commands
    """

    ADSIGRP_SYM_UPLOADINFO = 0xF00C
    # ? The upload info 2 contains more information about the datastructures
    # ? this is required to fetch all structs with `ADSIGRP_SYM_DT_UPLOAD`
    ADSIGRP_SYM_UPLOADINFO_2 = 0xF00F
    ADSIGRP_SYM_UPLOAD = 0xF00B
    ADSIGRP_SYM_DT_UPLOAD = 0xF00E
    ADSIGRP_SYM_DT_INFOBYNAMEEX = 0xF011
    ADSIGRP_SYM_TABLE_VERSION = 0xF008
    GET_INFO_BY_NAME = 0xF009

    ADSIGRP_SUM_READ = 0xF080
    ADSIGRP_SUM_WRITE = 0xF081
    ADSIGRP_SUM_READ_WRITE = 0xF082

    # ADS System Service
    ADSIGRP_TOGGLE_ROUTE_ENABLE = 0x328
