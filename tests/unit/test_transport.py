"""Unit tests for aioads.transport."""

import asyncio
import logging
import socket
import unittest
from unittest.mock import Mock

from aioads.ams_tcp_header import AmsTcpHeader
from aioads.commands.ads_command import AdsCommandId
from aioads.transport import (
    AdsOverMqttTopics,
    AdsTcpTransport,
    BaseTransport,
    ConnectionState,
    MqttBaseTransport,
)
from tests.builders import make_ams_address


def reader_at_eof(data: bytes = b"") -> asyncio.StreamReader:
    """A StreamReader pre-fed with ``data`` and then closed (EOF)."""
    reader = asyncio.StreamReader()
    if data:
        reader.feed_data(data)
    reader.feed_eof()
    return reader


class TestBaseTransport(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = BaseTransport(logger=logging.getLogger("test"))

    def test_get_next_invoke_id_starts_at_one(self) -> None:
        # Act
        result = self.transport.get_next_invoke_id()

        # Assert
        self.assertEqual(result, 1)

    def test_get_next_invoke_id_increments_on_each_call(self) -> None:
        # Act
        ids = [self.transport.get_next_invoke_id() for _ in range(3)]

        # Assert
        self.assertEqual(ids, [1, 2, 3])

    async def test_cancel_pending_requests_fails_subscribed_future(self) -> None:
        # Arrange
        async with self.transport.subscribe_request(1) as future:
            # Act
            self.transport.cancel_pending_requests(RuntimeError("connection lost"))

            # Assert
            with self.assertRaises(RuntimeError) as ctx:
                await future

        self.assertIn("connection lost", str(ctx.exception))

    async def test_subscribe_request_removes_future_after_exit(self) -> None:
        # Arrange
        async with self.transport.subscribe_request(7):
            pass

        # Act — a second subscription with the same id must be allowed again
        async with self.transport.subscribe_request(7) as future:
            self.transport.cancel_pending_requests(RuntimeError("x"))

            # Assert
            with self.assertRaises(RuntimeError):
                await future

    async def test_cancel_task_none_returns_none(self) -> None:
        # Act
        result = await BaseTransport.cancel_task(None)

        # Assert
        self.assertIsNone(result)

    async def test_cancel_task_completed_task_returns_its_result(self) -> None:
        # Arrange
        async def produce() -> int:
            return 42

        task = asyncio.create_task(produce())
        await asyncio.sleep(0)

        # Act
        result = await BaseTransport.cancel_task(task)

        # Assert
        self.assertEqual(result, 42)


class TestConnectionState(unittest.TestCase):

    def test_distinct_lifecycle_members_exist(self) -> None:
        # Assert
        self.assertNotEqual(ConnectionState.CONNECTED, ConnectionState.CLOSED)


class TestAdsOverMqttTopics(unittest.TestCase):

    def setUp(self) -> None:
        self.topics = AdsOverMqttTopics(
            pub_info="ads/1.1.1.1.1.1/info",
            pub_req=lambda net_id: f"ads/{net_id}/ams",
            sub_response="ads/1.1.1.1.1.1/ams/res",
            sub_info="ads/+/info",
        )

    def test_matches_single_level_wildcard_captures_segment(self) -> None:
        # Act
        match = self.topics.matches("ads/+/info", "ads/123/info")

        # Assert
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "123")

    def test_matches_non_matching_topic_returns_none(self) -> None:
        # Act
        match = self.topics.matches("ads/+/info", "other/123/info")

        # Assert
        self.assertIsNone(match)


class TestMqttBaseTransport(unittest.TestCase):

    def setUp(self) -> None:
        self.transport = MqttBaseTransport(
            src=make_ams_address(net_id="1.2.3.4.5.6"),
            name="TestClient",
            prefix="VirtualAmsNetwork1",
            logger=logging.getLogger("test"),
        )

    def test_pub_info_topic_includes_prefix_and_net_id(self) -> None:
        # Assert
        self.assertEqual(self.transport.topics.pub_info, "VirtualAmsNetwork1/1.2.3.4.5.6/info")

    def test_create_then_parse_info_message_round_trips_online_true(self) -> None:
        # Act
        message = self.transport.create_info_message(online=True)
        result = self.transport.parse_info_message(message)

        # Assert
        self.assertTrue(result)

    def test_create_then_parse_info_message_round_trips_online_false(self) -> None:
        # Act
        message = self.transport.create_info_message(online=False)
        result = self.transport.parse_info_message(message)

        # Assert
        self.assertFalse(result)

    def test_parse_info_message_without_online_element_returns_false(self) -> None:
        # Act
        result = self.transport.parse_info_message(b"<info></info>")

        # Assert
        self.assertFalse(result)


class TestAdsTcpTransport(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.transport = AdsTcpTransport(
            src_address=make_ams_address(), ip="127.0.0.1"
        )

    async def test_request_while_disconnected_raises_connection_error(self) -> None:
        # Act / Assert
        with self.assertRaises(ConnectionError) as ctx:
            await self.transport.request(
                command_payload=b"",
                command_id=AdsCommandId.READ,
                ams_address=make_ams_address(),
            )

        self.assertIn("Not connected", str(ctx.exception))

    async def test_read_loop_orderly_close_raises_connection_error(self) -> None:
        # Arrange — the peer closes without sending anything, so the first
        # readexactly sees a recv() of 0 bytes (EOF on a message boundary).
        self.transport._stream = (reader_at_eof(), Mock())

        # Act / Assert
        with self.assertRaises(ConnectionError) as ctx:
            await self.transport._read_loop()

        self.assertIn("closed by remote", str(ctx.exception))

    async def test_read_loop_eof_mid_message_reports_partial_bytes(self) -> None:
        # Arrange — only half of the AMS/TCP header arrives before EOF.
        partial = b"\x00\x00\x10"
        self.transport._stream = (reader_at_eof(partial), Mock())

        # Act / Assert
        with self.assertRaises(ConnectionError) as ctx:
            await self.transport._read_loop()

        message = str(ctx.exception)
        self.assertIn("mid-message", message)
        self.assertIn(f"{len(partial)} of {AmsTcpHeader.FIXED_SIZE}", message)

    async def test_read_loop_chains_incomplete_read_error_as_cause(self) -> None:
        # Arrange
        self.transport._stream = (reader_at_eof(), Mock())

        # Act / Assert — the original cause is preserved for debugging.
        with self.assertRaises(ConnectionError) as ctx:
            await self.transport._read_loop()

        self.assertIsInstance(ctx.exception.__cause__, asyncio.IncompleteReadError)


class TestAdsTcpTransportKeepalive(unittest.TestCase):
    """TCP keepalive / liveness configuration applied to the active socket."""

    def setUp(self) -> None:
        self.transport = AdsTcpTransport(
            src_address=make_ams_address(), ip="127.0.0.1"
        )
        # A real AF_INET TCP socket whose options we can read back. It does not
        # need to be connected to accept these socket options.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.addCleanup(self.sock.close)
        # A StreamWriter exposes its underlying socket via get_extra_info("socket").
        self.writer = Mock()
        self.writer.get_extra_info.return_value = self.sock

    def test_enable_keepalive_turns_on_so_keepalive(self) -> None:
        # Act
        self.transport._enable_keepalive(self.writer)

        # Assert
        self.assertEqual(
            self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1
        )

    @unittest.skipUnless(
        hasattr(socket, "TCP_KEEPIDLE"), "platform lacks TCP keepalive tuning"
    )
    def test_enable_keepalive_applies_configured_tuning_values(self) -> None:
        # Act
        self.transport._enable_keepalive(self.writer)

        # Assert — each kernel knob reflects the transport's configured constant
        expected = [
            ("TCP_KEEPIDLE", AdsTcpTransport.KEEPALIVE_IDLE),
            ("TCP_KEEPINTVL", AdsTcpTransport.KEEPALIVE_INTERVAL),
            ("TCP_KEEPCNT", AdsTcpTransport.KEEPALIVE_COUNT),
            ("TCP_USER_TIMEOUT", AdsTcpTransport.USER_TIMEOUT_MS),
        ]
        for opt_name, value in expected:
            opt = getattr(socket, opt_name, None)
            if opt is None:
                continue
            with self.subTest(option=opt_name):
                self.assertEqual(
                    self.sock.getsockopt(socket.IPPROTO_TCP, opt), value
                )

    def test_enable_keepalive_without_socket_does_not_raise(self) -> None:
        # Arrange — a StreamWriter without an underlying socket
        self.writer.get_extra_info.return_value = None

        # Act / Assert — keepalive is best-effort, so it is skipped quietly
        try:
            self.transport._enable_keepalive(self.writer)
        except Exception as e:  # pragma: no cover - failure path
            self.fail(f"_enable_keepalive must not raise when no socket: {e}")

    def test_enable_keepalive_on_closed_socket_does_not_raise(self) -> None:
        # Arrange — a closed socket makes setsockopt fail with OSError
        self.sock.close()

        # Act / Assert — keepalive is best-effort, so the error is swallowed
        try:
            self.transport._enable_keepalive(self.writer)
        except OSError:
            self.fail("_enable_keepalive must not propagate socket errors")


if __name__ == "__main__":
    unittest.main()
