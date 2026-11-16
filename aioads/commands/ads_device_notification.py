"""
ADS Device Notification (command id 8) stream codec.

The device notification is a request sent *by the remote* to us, so unlike the
other commands there is nothing to send. This module only provides the pure
(no I/O, no shared state) walk over the notification stream so it can run in
the event loop as well as in a worker thread.

```mermaid
---
title: "ADS Device Notification Stream"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: true
---
packet
+4: "length (4 bytes)"
+4: "stamps (4 bytes)"
+8: "stamp: timestamp (8 bytes)"
+4: "stamp: samples (4 bytes)"
+4: "sample: handle (4 bytes)"
+4: "sample: size (4 bytes)"
+8: "sample: data (size bytes)"
```
"""

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from struct import Struct
from typing import Final

from aioads.stream import AdsStream

STREAM_HEADER: Final[Struct] = Struct("<II")
"""length, stamp count"""
STAMP_HEADER: Final[Struct] = Struct("<QI")
"""timestamp (FILETIME), sample count"""
SAMPLE_HEADER: Final[Struct] = Struct("<II")
"""notification handle, sample size"""

_FILETIME_EPOCH: Final[datetime] = datetime(1601, 1, 1, tzinfo=timezone.utc)


def filetime_to_datetime(filetime: int) -> datetime:
    """
    Convert an ADS notification timestamp (Windows FILETIME: 100 ns ticks
    since 1601-01-01 UTC) to a timezone aware datetime.
    """
    return _FILETIME_EPOCH + timedelta(microseconds=filetime // 10)


def iter_samples(stream: AdsStream) -> Iterator[tuple[int, int, AdsStream]]:
    """
    Walk a device notification stream and yield ``(timestamp, handle, sample)``
    for every sample in wire order.

    Each sample is handed out as its own sub stream bounded to the sample
    size, so a consumer that fails (or does not read) a sample can never
    desynchronise the samples that follow.

    :raises ValueError: if the stream is truncated / malformed.
    """
    _length, stamp_count = stream.read_struct(STREAM_HEADER)
    for _ in range(stamp_count):
        timestamp, sample_count = stream.read_struct(STAMP_HEADER)
        for _ in range(sample_count):
            handle, sample_size = stream.read_struct(SAMPLE_HEADER)
            yield timestamp, handle, stream.sub_stream(sample_size)
