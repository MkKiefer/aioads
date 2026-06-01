"""
TCP transport implementation for ADS protocol.
"""
import asyncio
import contextlib
import enum
import logging
import random
import re
from abc import ABC
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Coroutine
from urllib.parse import urlparse
import uuid
from xml.etree import ElementTree


from aioads.ads_error_codes import AdsErrorCode
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.ams_tcp_header import AmsTcpHeader
from aioads.commands.ads_command import AdsCommandId
from aioads.commands.ads_command import AdsCommandState
from aioads.stream import AdsStream


class ITransport(ABC):
    """
    Interface for the transport implementation
    """

    async def request(self, command_payload: bytes,
                      command_id: AdsCommandId,
                      ams_address: AmsAddress
                      ) -> tuple[AmsHeader, AdsStream]:
        """
        Send a request to the remote PLC and wait for the response.
        :param data: The AmsMessage to send
        :return: A tuple containing the AmsHeader and the payload bytes
        """
        raise NotImplementedError

    async def connect(self):
        """
        Connect the transport to the remote PLC.
        """
        raise NotImplementedError

    async def disconnect(self):
        """
        Disconnect the transport from the remote PLC.
        """
        raise NotImplementedError

    def set_notification_callback(
        self, callback: Callable[[AmsHeader, AdsStream], Coroutine[None, None, None]]
    ) -> None:
        """
        Set a callback for ads notification events.
        This is currently only a experiemntal interface and mybe removed at any time.
        :param callback: The callback to call when a notification is received.
        The callback should be a coroutine that takes an AmsHeader and an AdsStream as parameters.
        """
        raise NotImplementedError


class ConnectionState(enum.Enum):
    """
    Lifecycle state of a transport connection.
    """

    DISCONNECTED = enum.auto()
    """Never connected, or the initial connect failed."""
    CONNECTING = enum.auto()
    """Initial connect attempt in progress."""
    CONNECTED = enum.auto()
    """Connected; requests are allowed."""
    RECONNECTING = enum.auto()
    """Connection dropped; the supervisor is re-establishing it. Requests fail fast."""
    CLOSED = enum.auto()
    """Intentionally closed via disconnect(); the supervisor must not reconnect."""


class BaseTransport:
    """
    Common base class for all transport implementations, providing common functionality for:
        - Managing Invoke IDs and pending requests
        - Handling responses and matching them to requests
        - Handling notifications
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._invoke_id = 1
        self._pending_requests: dict[int, asyncio.Future[tuple[AmsHeader, AdsStream]]] = (
            {}
        )
        self._ads_notification_callback: (
            None | Callable[[AmsHeader, AdsStream],
                            Coroutine[None, None, None]]
        ) = None

    def set_notification_callback(
        self, callback: Callable[[AmsHeader, AdsStream], Coroutine[None, None, None]]
    ) -> None:
        """
        Setts the callback for incoming notifications
        """
        self._ads_notification_callback = callback

    def cancel_pending_requests(self, ex: BaseException):
        """
        Cancel all pending requests with the give exception.
        This is used to notify all pending requests when the connection is lost or an error occurs.
        """
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(ex)

    def get_next_invoke_id(self) -> int:
        """
        Get the next Invoke ID for a request.
        :return: The next Invoke ID
        """
        invoke_id = self._invoke_id
        self._invoke_id += 1
        if self._invoke_id > 0xFFFFFFFF:
            self._invoke_id = 1
        return invoke_id

    @contextlib.asynccontextmanager
    async def subscribe_request(self, invoke_id: int):
        """
        Create a context to subscribe to a invoke ID for incoming responses.
        This uses a context manager to ensure we properly clean up
        the pending request even if an error occurs or the request times out.
        The future is yielded and gets fulfilled when a response with the corresponding invoke ID is received.

        :param invoke_id: The invoke ID to subscribe to
        """
        self._pending_requests[invoke_id] = asyncio.Future()
        try:
            yield self._pending_requests[invoke_id]
        finally:
            del self._pending_requests[invoke_id]

    async def _handle_response(self, ams_header: AmsHeader, ams_command: AdsStream):
        # Fulfill the corresponding future
        future = self._pending_requests.get(ams_header.invoke_id)
        if future is not None and not future.done():
            future.set_result((ams_header, ams_command))
        elif future is None:
            self.logger.warning(
                "Received response with unknown Invoke ID %d",
                ams_header.invoke_id,
            )

    async def _handle_request(self, ams_header: AmsHeader, ams_command: AdsStream):
        if not ams_header.command_id == AdsCommandId.DEVICE_NOTIFICATION:
            self.logger.warning(
                "Received unexpected request with Command ID %d",
                ams_header.command_id,
            )
            return

        if not self._ads_notification_callback:
            self.logger.warning(
                "No notification callback set, dropping notification")
            return

        await self._ads_notification_callback(ams_header, ams_command)

    @staticmethod
    async def cancel_task(task: asyncio.Task[None] | None):
        """
        Cancel a given asyncio task.
        """
        if not task:
            return

        if task.done():
            return await task

        try:
            return await asyncio.wait_for(task, timeout=5)
        except asyncio.TimeoutError:
            with contextlib.suppress(asyncio.CancelledError):
                task.cancel()
                return await task


class AdsTcpTransport(BaseTransport, ITransport):
    """
    Transport implementation for ADS over TCP.
    - Manages TCP connection
    - Parse AMS Header for routing messages
    - Handle Request / Response Matching
    - Handle notifications
    ! Backoff allows only a single connection from a src ip.
    ! The transport can be re-used for multiple client that have the same target ip.
    client 1 ---+
                |
    client 2 ---+--> transport <-> plc 1
    client 3 ------> transport <-> plc 2
    """

    REQUEST_TIMEOUT = 120.0
    CONNECT_TIMEOUT = 30.0
    RECONNECT_INITIAL_BACKOFF = 1.0
    RECONNECT_MAX_BACKOFF = 30.0

    def __init__(
        self,
        src_address: AmsAddress,
        ip: str,
        port: int = 48898,
    ) -> None:
        """
        Create a new AdsTcpTransport instance

        :param ip: The target IP address
        :param port: The target port
        """
        super().__init__(logger=logging.getLogger(f"{__name__}.'{ip}'"))
        self.source_ams_address = src_address
        self.ip = ip
        self.port = port

        self._stream: None | tuple[asyncio.StreamReader,
                                   asyncio.StreamWriter] = None
        self._stream_lock = asyncio.Lock()
        self._supervisor_task: None | asyncio.Task[None] = None
        self._state = ConnectionState.DISCONNECTED

    def set_notification_callback(
        self, callback: Callable[[AmsHeader, AdsStream], Coroutine[None, None, None]]
    ) -> None:
        self._ads_notification_callback = callback

    async def connect(self):
        """
        Connect to the remote PLC via TCP.

        Performs the initial connection synchronously and raises on failure.
        On success a background supervisor task takes over the read loop and
        transparently reconnects (with backoff) if the connection drops.
        """
        if self._supervisor_task is not None and not self._supervisor_task.done():
            return

        self._state = ConnectionState.CONNECTING
        try:
            await self._open()
        except Exception:
            self._state = ConnectionState.DISCONNECTED
            raise
        self._supervisor_task = asyncio.create_task(self._supervise())

    async def disconnect(self):
        """
        Disconnect from the remote PLC and stop the supervisor.

        Sets CLOSED first so the supervisor does not attempt to reconnect, then
        tears down the stream (which unblocks the read loop) and awaits the
        supervisor task.
        """
        self._state = ConnectionState.CLOSED
        await self._teardown_stream(ConnectionError("Transport disconnected"))
        await self.cancel_task(self._supervisor_task)
        self._supervisor_task = None

    async def _open(self):
        """
        Open the TCP connection and publish it as the active stream.
        Raises on failure; callers own the surrounding state transitions.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.ip, self.port), self.CONNECT_TIMEOUT
        )
        async with self._stream_lock:
            self._stream = (reader, writer)
        self._state = ConnectionState.CONNECTED

    async def _teardown_stream(self, ex: BaseException):
        """
        Fail all in-flight requests and close the current stream (if any).

        In-flight requests are intentionally *not* replayed: a write may already
        have been applied on the PLC, so resending could double-apply it.
        """
        self.cancel_pending_requests(ex)
        async with self._stream_lock:
            stream = self._stream
            self._stream = None
        if stream is not None:
            _, writer = stream
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def request(
        self, command_payload: bytes, command_id: AdsCommandId, ams_address: AmsAddress
    ):
        """
        Send a request to the remote PLC and wait for the response.
        :param data: The AmsMessage to send

        :return: A tuple containing the AmsHeader and the payload bytes
        """
        if self._state is not ConnectionState.CONNECTED:
            raise ConnectionError(
                f"Not connected to the remote (state: {self._state.name})"
            )

        invoke_id = self.get_next_invoke_id()

        # Build AmsHeader
        ams_header = AmsHeader(
            target_ams_address=ams_address,
            source_ams_address=self.source_ams_address,
            command_id=command_id,
            command_flags=AdsCommandState.ADS_COMMAND | AdsCommandState.ADS_REQUEST,
            command_length=len(command_payload),
            error_code=AdsErrorCode(0),
            invoke_id=invoke_id,
        )
        ams_header_bytes = ams_header.serialize()

        tcp_header = AmsTcpHeader(length=len(
            ams_header_bytes) + len(command_payload))
        tcp_header_bytes = tcp_header.serialize()

        async with self.subscribe_request(invoke_id) as response_future:
            async with self._stream_lock:
                stream = self._stream
                if stream is None:
                    # A teardown raced this send; surface the failure it already
                    # set on the future rather than a fresh, unretrieved one.
                    if response_future.done():
                        return await response_future
                    raise ConnectionError("Not connected to the remote")
                _, writer = stream
                # Send request
                writer.write(tcp_header_bytes +
                             ams_header_bytes + command_payload)
                await writer.drain()
            return await asyncio.wait_for(
                response_future, timeout=self.REQUEST_TIMEOUT
            )

    async def _supervise(self):
        """
        Own the connection lifecycle: run the read loop, and when the connection
        drops, fail in-flight requests and reconnect with exponential backoff.

        This is the only place that reconnects, so concurrent senders never
        trigger competing reconnect attempts. Senders fail fast instead (see
        :meth:`request`), which also respects the Beckhoff single-connection
        backoff/lockout behavior.
        """
        backoff = self.RECONNECT_INITIAL_BACKOFF
        while self._state is not ConnectionState.CLOSED:
            try:
                await self._read_loop()
            except asyncio.CancelledError: #pylint: disable=try-except-raise
                # Except everything... except a cancellation event
                raise
            except Exception as e:
                self.logger.info("Connection lost: %s", e)

            if self._state is ConnectionState.CLOSED:
                break

            self._state = ConnectionState.RECONNECTING
            await self._teardown_stream(ConnectionError("Connection lost"))

            while self._state is not ConnectionState.CLOSED:
                try:
                    await self._open()
                    self.logger.info("Reconnected to the remote")
                    backoff = self.RECONNECT_INITIAL_BACKOFF
                    break
                except asyncio.CancelledError: #pylint: disable=try-except-raise
                    raise
                except Exception as e:  # pylint: disable=broad-exception-caught
                    self.logger.info(
                        "Reconnect failed: %s, retrying in %.1fs", e, backoff
                    )
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff = min(backoff * 2, self.RECONNECT_MAX_BACKOFF)

    async def _read_loop(self):
        """
        Continuously read and dispatch messages from the active stream.
        Returns/raises when the connection drops; the supervisor handles recovery.
        """
        stream = self._stream
        if stream is None:
            raise ConnectionError("Not connected to the remote PLC")

        reader, _ = stream
        while True:
            try:
                # Read AmsTcpHeader
                tcp_header_bytes = await reader.readexactly(AmsTcpHeader.FIXED_SIZE)
                tcp_header = AmsTcpHeader.deserialize(tcp_header_bytes)
                payload_bytes = await reader.readexactly(tcp_header.length)
            except asyncio.IncompleteReadError as e:
                # readexactly hit EOF: the peer's recv() returned 0 bytes,
                # i.e. the remote closed the connection. An empty partial is an
                # orderly close on a message boundary; a non-empty partial means
                # we were cut off mid-message. Either way the stream is dead, so
                # raise a clear ConnectionError for the supervisor to reconnect.
                if e.partial:
                    raise ConnectionError(
                        f"Connection closed mid-message: received "
                        f"{len(e.partial)} of {e.expected} expected bytes"
                    ) from e
                raise ConnectionError("Connection closed by remote") from e

            ams_message_stream = AdsStream(memoryview(payload_bytes))
            ams_header = AmsHeader.deserialize(ams_message_stream)
            ams_command = ams_message_stream.sub_stream(
                ams_header.command_length)

            if AdsCommandState.ADS_RESPONSE in ams_header.command_flags:
                await self._handle_response(ams_header, ams_command)

            elif AdsCommandState.ADS_REQUEST in ams_header.command_flags:
                await self._handle_request(ams_header, ams_command)


@dataclass
class AdsOverMqttTopics:
    """
    Ads Over MQTT Topics
    """

    pub_info: str
    """
    The topic we publish our state information
    """
    pub_req: Callable[[str], str]
    """
    The topic we publish for requesting others
    """
    sub_response: str
    """
    The response topic we expect responses on
    """

    sub_info: str
    """
    The wildcard topic we subscribe to get the state of the other
    """

    _pattern_cache: dict[str, re.Pattern] = field(
        default_factory=dict, init=False)

    def matches(self, pattern: str, topic: str):
        """
        Matches a MQTT pattern string like `ads/+/info` and returns
        - Null if not matches
        - The matches with th wildcard `+` in capture group 1
        """
        if pattern not in self._pattern_cache:
            regex = re.compile(pattern.replace(
                '+', '([^/]*)').replace('/#', '(|/.*)'))
            self._pattern_cache[pattern] = regex
        return self._pattern_cache[pattern].fullmatch(topic)


class MqttBaseTransport(BaseTransport):
    """Base transport that tunnels AMS/ADS frames over an MQTT broker."""

    REQUEST_TIMEOUT = 120.0

    def __init__(self, src: AmsAddress, name: str, prefix: str, logger: logging.Logger) -> None:
        super().__init__(logger)
        self.name = name
        self.src = src
        self.topics = AdsOverMqttTopics(
            pub_info=f"{prefix}/{src.net_id}/info",
            pub_req=lambda net_id: f"{prefix}/{net_id}/ams",
            sub_info=f"{prefix}/+/info",
            sub_response=f"{prefix}/{src.net_id}/ams/res"
        )

    def create_info_message(self, online: bool) -> bytes:
        """
        Creates a XML info message containing the required infromations about this
        system / transport. The info message is used as a sort of network discovery.
        Example of a plc payload:
        ```
        <info>
        <online name="RX40_000005" osVersion="10.0.14393" osPlatform="2" tcVersion="3.1.4024">true</online>
        </info>
        ```
        """
        element_info = ElementTree.Element("info")
        element_online = ElementTree.SubElement(element_info, "online")
        element_online.set("name", self.name.strip())
        # ? Not sure if we need to set the below
        element_online.set("osVersion", "10.0.14393")
        element_online.set("osPlatform", "2")
        element_online.set("tcVersion", "3.1.4024")
        element_online.text = str(online).lower()

        return ElementTree.tostring(element_info)

    def parse_info_message(self, xml_bytes: bytes) -> bool:
        """
        Parses the <info> XML message and returns True/False
        based on the <online> element's text content.
        """
        root = ElementTree.fromstring(xml_bytes)

        online = root.find("online")
        if online is None or online.text is None:
            return False
        return online.text.strip().lower() == "true"


try:
    import aiomqtt
except ImportError:
    pass


class AdsAioMqttTransport(MqttBaseTransport, ITransport):
    """
    Transport implementation for ADS over MQTT using the aiomqtt client library.
    As we can only have a single connection to the MQTT Broker, this transport
    is designed to be shared between multiple client.
    """

    def __init__(self, src: AmsAddress, name: str, url: str, prefix: str) -> None:
        """
        Create a new AdsMqttTransport instance.
        :param src: The source AMS address for this transport, used for routing messages.
        !This has to be unique for each transport instance.
        !Overall if not other required use only a single MQTT Transport instance

        client 1 ---+                 +--- PLC 1
                    |                 |
        client 2 ---+--> transport -- +
                    |                 |
        client 3 ---+                 +--- PLC 2

        :param name: The name of this transport / client, used in the discovery identifier and for logging.
        :param url: The MQTT broker URL, e.g. "mqtt://192.168.178.1:1883"
        :param prefix: The prefix for MQTT topics, e.g. "VirtualAmsNetwork1"
        """
        super().__init__(
            src=src,
            name=name,
            prefix=prefix,
            logger=logging.getLogger(f"{__name__}.'{name}'")
        )
        parsed_url = urlparse(url)

        self.remotes: dict[str, bool] = {}
        self.remotes_initialized = asyncio.Future[None]()
        will_message = aiomqtt.Will(
            topic=self.topics.pub_info,
            payload=self.create_info_message(online=False),
            qos=aiomqtt.QoS.EXACTLY_ONCE,
            retain=True,
        )
        self.client = aiomqtt.Client(
            hostname=parsed_url.hostname,
            port=parsed_url.port or 1883,
            username=parsed_url.username,
            password=parsed_url.password.encode() if parsed_url.password else None,
            will=will_message,
            identifier=name,
            clean_start=True,
            reconnect=True
        )
        self.exitstack = contextlib.AsyncExitStack()
        self._reader_task: None | asyncio.Task[None] = None

    async def connect(self):
        """
        Connect to the MQTT broker.
        """
        if self._reader_task is not None:
            return

        await self.exitstack.enter_async_context(self.client)
        await self.client.publish(
            self.topics.pub_info,
            self.create_info_message(online=True),
            qos=aiomqtt.QoS.AT_MOST_ONCE,
            retain=True,
            packet_id=next(self.client.packet_ids)
        )
        await self.client.subscribe(
            self.topics.sub_info,
        )
        self._reader_task = asyncio.create_task(self._reader())
        # ? Not sure if this really works, as this is set on the first message and
        # ? other connection states maybe come to late and we should use a
        # ? sleep / connect wait time or something like this.
        await self.remotes_initialized

    async def disconnect(self):
        """
        Disconnect from the MQTT broker.
        """
        await self.client.publish(
            self.topics.pub_info,
            self.create_info_message(online=False),
            qos=aiomqtt.QoS.AT_MOST_ONCE,
            retain=True,
            packet_id=next(self.client.packet_ids)
        )
        await self.exitstack.aclose()
        await self.cancel_task(self._reader_task)
        self._reader_task = None

    async def request(self, command_payload: bytes,
                      command_id: AdsCommandId,
                      ams_address: AmsAddress
                      ) -> tuple[AmsHeader, AdsStream]:
        """
        Send a request to the remote PLC and wait for the response.
        :param data: The AmsMessage to send
        :return: A tuple containing the AmsHeader and the payload bytes
        """
        invoke_id = self.get_next_invoke_id()

        if not self.remotes.get(ams_address.net_id, False):
            raise ConnectionError(
                f"The remote {ams_address.net_id} is not online / connected to the broker")

        # Build AmsHeader
        ams_header = AmsHeader(
            target_ams_address=ams_address,
            source_ams_address=self.src,
            command_id=command_id,
            command_flags=AdsCommandState.ADS_COMMAND | AdsCommandState.ADS_REQUEST,
            command_length=len(command_payload),
            error_code=AdsErrorCode(0),
            invoke_id=invoke_id,
        )
        ams_header_bytes = ams_header.serialize()

        async with self.subscribe_request(invoke_id) as response_future:
            await self.client.publish(
                self.topics.pub_req(ams_address.net_id),
                ams_header_bytes + command_payload,
                qos=aiomqtt.QoS.AT_MOST_ONCE,
                packet_id=next(self.client.packet_ids)
            )
            return await asyncio.wait_for(
                response_future, timeout=self.REQUEST_TIMEOUT
            )

    async def _reader(self):
        try:
            await self.client.subscribe(
                self.topics.sub_response,
                max_qos=aiomqtt.QoS.AT_MOST_ONCE,
            )
            async for message in self.client.messages():
                if not isinstance(message, aiomqtt.PublishPacket):
                    return

                if self.topics.matches(self.topics.sub_response, message.topic):
                    ams_message_stream = AdsStream(memoryview(message.payload))
                    ams_header = AmsHeader.deserialize(ams_message_stream)
                    ams_command = ams_message_stream.sub_stream(
                        ams_header.command_length)

                    if AdsCommandState.ADS_RESPONSE in ams_header.command_flags:
                        await self._handle_response(ams_header, ams_command)
                    elif AdsCommandState.ADS_REQUEST in ams_header.command_flags:
                        await self._handle_request(ams_header, ams_command)
                if (matches := self.topics.matches(self.topics.sub_info, message.topic)):
                    # The initial connect waits till we have the first message received
                    if not self.remotes_initialized.done():
                        self.remotes_initialized.set_result(None)
                    online = self.parse_info_message(message.payload)
                    self.remotes[matches.group(1)] = online

        except aiomqtt.ConnectError as ex:
            # We can't say if this is expected on shutdown / disconnect or has another reason ?
            # more mqtt lib testing required to check the re-connect behaviors
            self.cancel_pending_requests(ex)
        except Exception as ex:
            self.logger.error("Reader task encountered an error", exc_info=ex)
            self.cancel_pending_requests(ex)


try:
    import gmqtt
except ImportError:
    pass


class AdsGMqttTransport(MqttBaseTransport, ITransport):
    """
    Transport implementation for ADS over MQTT using the gmqtt client library.
    As we can only have a single connection to the MQTT Broker, this transport
    is designed to be shared between multiple client.
    """

    def __init__(self, src: AmsAddress, name: str, url: str, prefix: str) -> None:
        """
        Create a new AdsMqttTransport instance.
        :param src: The source AMS address for this transport, used for routing messages.
        !This has to be unique for each transport instance.
        !Overall if not other required use only a single MQTT Transport instance

        client 1 ---+                 +--- PLC 1
                    |                 |
        client 2 ---+--> transport -- +
                    |                 |
        client 3 ---+                 +--- PLC 2

        :param name: The name of this transport / client, used in the discovery identifier and for logging.
        :param url: The MQTT broker URL, e.g. "mqtt://192.168.178.1:1883"
        :param prefix: The prefix for MQTT topics, e.g. "VirtualAmsNetwork1"
        """
        super().__init__(src=src, name=name, prefix=prefix,
                         logger=logging.getLogger(f"{__name__}.'{name}'"))

        self.url = urlparse(url)
        self.client = gmqtt.Client(
            client_id=uuid.uuid4().hex,
            will_message=gmqtt.Message(
                topic=self.topics.pub_info,
                payload=self.create_info_message(online=False),
                retain=True
            ),
            clean_session=True
        )
        if self.url.username and self.url.password:
            self.client.set_auth_credentials(
                username=self.url.username,
                password=self.url.password
            )
        self.client.on_message = self._on_response

        self.remotes: dict[str, bool] = {}
        self.remotes_initialized = asyncio.Future[None]()

    async def request(self, command_payload: bytes,
                      command_id: AdsCommandId,
                      ams_address: AmsAddress
                      ) -> tuple[AmsHeader, AdsStream]:
        """
        Send a request to the remote PLC and wait for the response.
        :param data: The AmsMessage to send
        :return: A tuple containing the AmsHeader and the payload bytes
        """
        invoke_id = self.get_next_invoke_id()

        if not self.remotes.get(ams_address.net_id, False):
            raise ConnectionError(
                f"The remote {ams_address.net_id} is not online / connected to the broker")

        # Build AmsHeader
        ams_header = AmsHeader(
            target_ams_address=ams_address,
            source_ams_address=self.src,
            command_id=command_id,
            command_flags=AdsCommandState.ADS_COMMAND | AdsCommandState.ADS_REQUEST,
            command_length=len(command_payload),
            error_code=AdsErrorCode(0),
            invoke_id=invoke_id,
        )
        ams_header_bytes = ams_header.serialize()

        async with self.subscribe_request(invoke_id) as response_future:
            self.client.publish(
                message_or_topic=gmqtt.Message(
                    topic=self.topics.pub_req(ams_address.net_id),
                    payload=ams_header_bytes + command_payload,
                )
            )
            return await asyncio.wait_for(
                response_future, timeout=self.REQUEST_TIMEOUT
            )

    async def _on_response(self, _client: gmqtt.Client, topic: str, payload: bytes, _qos: int, _properties: dict):
        if self.topics.matches(self.topics.sub_response, topic):
            ams_message_stream = AdsStream(memoryview(payload))
            ams_header = AmsHeader.deserialize(ams_message_stream)
            ams_command = ams_message_stream.sub_stream(
                ams_header.command_length)

            if AdsCommandState.ADS_RESPONSE in ams_header.command_flags:
                await self._handle_response(ams_header, ams_command)
            elif AdsCommandState.ADS_REQUEST in ams_header.command_flags:
                await self._handle_request(ams_header, ams_command)
        if (matches := self.topics.matches(self.topics.sub_info, topic)):
            # The initial connect waits till we have the first message received
            if not self.remotes_initialized.done():
                self.remotes_initialized.set_result(None)
            online = self.parse_info_message(payload)
            self.remotes[matches.group(1)] = online

    async def connect(self):
        """
        Connect the transport to the remote PLC.
        """
        if self.client.is_connected:
            return

        await self.client.connect(
            host=self.url.hostname,
            port=self.url.port or 1883,
            ssl=self.url.scheme == "mqtts"
        )
        self.client.publish(
            message_or_topic=gmqtt.Message(
                topic=self.topics.pub_info,
                payload=self.create_info_message(online=True),
                retain=True,
            )
        )
        self.client.subscribe(
            subscription_or_topic=gmqtt.Subscription(
                topic=self.topics.sub_info,
                no_local=False
            )
        )
        await self.remotes_initialized
        self.client.subscribe(
            subscription_or_topic=gmqtt.Subscription(
                topic=self.topics.sub_response
            )
        )

    async def disconnect(self):
        """
        Disconnect the transport from the remote PLC.
        """
        self.client.publish(
            message_or_topic=gmqtt.Message(
                topic=self.topics.pub_info,
                payload=self.create_info_message(False),
                retain=True
            )
        )
        await self.client.disconnect()
