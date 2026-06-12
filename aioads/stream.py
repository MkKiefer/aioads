"""
Modules provides AdsStream object comparable to Bytes.Io with
the ability to create substream and parse structs with minimal memory overhead 
"""


from struct import Struct
from typing import TypeVar

T = TypeVar("T", bound=tuple)


class AdsStream:
    """
    The main usage for the stream is to utilize the full potential of 
    the memoryview for zero-copy parsing of the data. 
    The stream will keep track of the current position and allow for 
    reading of bytes and structs without copying the data.
    It will also provide a way to create sub streams for parsing nested structures 
    without affecting the main stream's position.
    with minimal overhead and maximum performance.
    """

    def __init__(self, data: memoryview) -> None:
        self._data = data
        self._length = data.nbytes
        self._pos = 0

    @property
    def length(self) -> int:
        """
        Return the total length of the stream.
        """
        return self._length

    def tell(self) -> int:
        """
        Return the current position in the stream.
        """
        return self._pos

    def seek(self, pos: int) -> None:
        """
        Set the current position in the stream.
        """
        if pos < 0 or pos > self._length:
            raise ValueError("Position out of bounds")
        self._pos = pos

    def _checked_slice(self, size: int) -> memoryview:
        """
        Return a view of the next ``size`` bytes and advance the position.
        Raises instead of silently returning short data on truncated input.
        """
        end = self._pos + size
        if size < 0 or end > self._length:
            raise ValueError(
                f"Read of {size} bytes at position {self._pos} exceeds "
                f"stream length {self._length}"
            )
        chunk = self._data[self._pos:end]
        self._pos = end
        return chunk

    def read(self, size: int) -> bytes:
        """
        Reads a specified number of bytes from the stream and advances the position.
        This will allocate a new bytes object, so it should be used for small reads.
        For larger reads, consider using read_struct or read_struct_as to avoid unnecessary copying.
        """
        return self._checked_slice(size).tobytes()

    def read_view(self, size: int) -> memoryview:
        """
        Reads a specified number of bytes from the stream and advances the position, returning a memoryview.
        This is useful for large reads where you want to avoid copying the data.
        The returned memoryview will be a slice of the original data, so it will not allocate new memory.
        """
        return self._checked_slice(size)

    def read_struct(self, struct: Struct):
        """
        Read a struct from the stream and return the unpacked values as a tuple.
        The struct can be provided as a format string or as a Struct object.
        """
        return struct.unpack(self._checked_slice(struct.size))

    def sub_stream(self, length: int) -> "AdsStream":
        """
        Create a sub stream of the specified length starting from the current position.
        This is useful for parsing nested structures without affecting the main stream's position.
        The sub stream will have its own position and length, but will share the same underlying data.
        """
        if self._pos + length > self._length:
            raise ValueError("Sub stream length exceeds available data")
        sub_data = self._data[self._pos:self._pos + length]
        self._pos += length
        return AdsStream(sub_data)
