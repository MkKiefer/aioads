"""Unit tests for aioads.utils.local_ip."""

import socket
import unittest
from unittest.mock import MagicMock, patch

from aioads.utils.local_ip import get_local_ip


class TestGetLocalIp(unittest.TestCase):

    def test_returns_outgoing_interface_address(self) -> None:
        # Arrange — socket is created internally, so patch.object is required
        with patch.object(socket, socket.socket.__name__) as mock_socket_cls:
            mock_socket_cls.return_value.getsockname.return_value = ("192.168.1.50", 5000)

            # Act
            result = get_local_ip()

        # Assert
        self.assertEqual(result, "192.168.1.50")

    def test_closes_socket_after_use(self) -> None:
        # Arrange
        with patch.object(socket, socket.socket.__name__) as mock_socket_cls:
            mock_sock = mock_socket_cls.return_value
            mock_sock.getsockname.return_value = ("10.0.0.1", 5000)

            # Act
            get_local_ip()

        # Assert
        mock_sock.close.assert_called_once()

    def test_connect_failure_raises_runtime_error(self) -> None:
        # Arrange
        with patch.object(socket, socket.socket.__name__) as mock_socket_cls:
            mock_socket_cls.return_value.connect.side_effect = OSError("unreachable")

            # Act / Assert
            with self.assertRaises(RuntimeError) as ctx:
                get_local_ip()

        self.assertIn("Could not determine local IP", str(ctx.exception))

    def test_passes_target_network_to_connect(self) -> None:
        # Arrange
        with patch.object(socket, socket.socket.__name__) as mock_socket_cls:
            mock_sock = mock_socket_cls.return_value
            mock_sock.getsockname.return_value = ("10.0.0.2", 5000)

            # Act
            get_local_ip(target_network="8.8.8.8")

        # Assert
        mock_sock.connect.assert_called_once_with(("8.8.8.8", 1))


if __name__ == "__main__":
    unittest.main()
