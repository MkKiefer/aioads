"""
Shared test data builders.

These are plain construction helpers (not tests) used to build the typed
objects and protocol fixtures the unit tests operate on. Keeping them in one
place avoids repeating the verbose AMS/stream construction in every test file
while leaving each test responsible for its own mock placement.
"""

from aioads.ads_error_codes import AdsErrorCode
from aioads.commands.ads_read import AdsReadResponse
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.commands.ads_command import AdsCommandId, AdsCommandState
from aioads.stream import AdsStream
from aioads.transport import AdsTcpTransport


def make_ams_address(net_id: str = "1.2.3.4.5.6", port: int = 851) -> AmsAddress:
    """Build a typed AmsAddress for use as a routing target/source."""
    return AmsAddress(net_id=net_id, port=port)


def make_stream(data: bytes) -> AdsStream:
    """Wrap raw bytes in an AdsStream the way the transport read loop does."""
    return AdsStream(memoryview(data))


def make_ams_header(
    error_code: int = 0,
    command_id: AdsCommandId = AdsCommandId.READ,
    invoke_id: int = 1,
    command_length: int = 0,
) -> AmsHeader:
    """Build a response AMS header, successful unless an error code is given."""
    return AmsHeader(
        target_ams_address=make_ams_address(),
        source_ams_address=make_ams_address(net_id="6.5.4.3.2.1", port=350),
        command_id=command_id,
        command_flags=AdsCommandState.ADS_RESPONSE,
        command_length=command_length,
        error_code=AdsErrorCode(error_code),
        invoke_id=invoke_id,
    )


def make_read_payload(data: bytes, error_code: int = 0) -> bytes:
    """
    Build the body of an ADS read/read-write response: an AdsReadResponse
    header (error code + length) followed by ``data``.
    """
    header = AdsReadResponse(error_code=AdsErrorCode(error_code), length=len(data))
    return header.serialize() + data


def make_transport() -> AdsTcpTransport:
    """
    Build a real, fully typed AdsTcpTransport (Pattern A).

    No connection is opened. Tests replace ``request`` with an AsyncMock
    (Pattern B) when they need to control or assert the transport call.
    """
    return AdsTcpTransport(src_address=make_ams_address(), ip="127.0.0.1")
