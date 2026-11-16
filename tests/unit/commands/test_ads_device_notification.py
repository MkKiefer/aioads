"""Unit tests for aioads.commands.ads_device_notification."""

import unittest
from datetime import datetime, timezone

from aioads.commands.ads_device_notification import (
    filetime_to_datetime,
    iter_samples,
)
from tests.builders import make_stream


def build_stream(stamps: list[tuple[int, list[tuple[int, bytes]]]]) -> bytes:
    """Build a notification stream from ``[(timestamp, [(handle, data)])]``."""
    body = b""
    for timestamp, samples in stamps:
        body += timestamp.to_bytes(8, "little") + len(samples).to_bytes(4, "little")
        for handle, data in samples:
            body += handle.to_bytes(4, "little") + len(data).to_bytes(4, "little") + data
    return len(body).to_bytes(4, "little") + len(stamps).to_bytes(4, "little") + body


class TestIterSamples(unittest.TestCase):

    def test_iter_samples_yields_every_sample_in_wire_order(self) -> None:
        # Arrange
        stream = make_stream(
            build_stream([(10, [(1, b"\x01"), (2, b"\x02\x03")]), (20, [(1, b"\x04")])])
        )

        # Act
        result = [
            (timestamp, handle, sample.read(sample.length))
            for timestamp, handle, sample in iter_samples(stream)
        ]

        # Assert
        self.assertEqual(
            result, [(10, 1, b"\x01"), (10, 2, b"\x02\x03"), (20, 1, b"\x04")]
        )

    def test_iter_samples_unread_sample_does_not_shift_following_sample(self) -> None:
        # Arrange
        stream = make_stream(build_stream([(10, [(1, b"\xff\xff"), (2, b"\x07")])]))

        # Act — the first sample is skipped without reading it
        samples = [sample for _, _, sample in iter_samples(stream)]

        # Assert
        self.assertEqual(samples[1].read(1), b"\x07")

    def test_iter_samples_without_stamps_yields_nothing(self) -> None:
        # Arrange
        stream = make_stream(build_stream([]))

        # Act
        result = list(iter_samples(stream))

        # Assert
        self.assertEqual(result, [])

    def test_iter_samples_truncated_stream_raises_value_error(self) -> None:
        # Arrange — the sample announces more data than the stream holds
        data = build_stream([(10, [(1, b"\x01\x02\x03\x04")])])[:-2]

        # Act / Assert
        with self.assertRaises(ValueError):
            list(iter_samples(make_stream(data)))


class TestFiletimeToDatetime(unittest.TestCase):

    def test_filetime_to_datetime_converts_unix_epoch(self) -> None:
        # Arrange — 100 ns ticks between 1601-01-01 and 1970-01-01
        filetime = 116444736000000000

        # Act
        result = filetime_to_datetime(filetime)

        # Assert
        self.assertEqual(result, datetime(1970, 1, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
