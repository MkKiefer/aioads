"""
ADS notifications (subscriptions to remote symbols).

Layers, from the wire up::

    transport read loop --feed()--> ingress queue --> pump --> subscription buffer --> callback(batch)
       (I/O only, sync)             bounded           decode    bounded, OverflowPolicy   1 task / subscription

- :class:`NotificationRegistry` exists once per transport and is attached to
  it as its :class:`aioads.transport.INotificationSink`. The transport is the
  only place that sees every notification frame, so the registry is the single
  authority over which ``(remote address, handle)`` pairs are in use. It owns
  the lifecycle of every handle and guarantees that handles nobody uses
  (unknown, orphaned or failed-to-delete ones) get removed from the remote.
- :class:`Subscription` is one registered notification including its delivery
  buffer and consumer task. A slow callback only ever delays its own
  subscription; values that arrive meanwhile are handed over as one batch.
- :class:`NotificationManager` is the thin per client facade used by
  :class:`aioads.ads_client.AdsClient`.

Known limitations:
    - ADS has no command to list the notifications of a client. An orphaned
      handle is only detected (and removed) when it fires the next time.
    - Samples that arrive in the short window between the remote creating a
      handle and us learning its id are dropped.
    - Monitoring of the plc cycle usage (to disable notifications before we
      are the reason of cycle time violations) is not implemented.
"""
import asyncio
import contextlib
import enum
import logging
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from struct import Struct
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Coroutine
from typing import Final

from aioads.ads_symbol_cache import AdsSymbolCache
from aioads.ads_symbol_parser import ISymbolParser
from aioads.ams_address import AmsAddress
from aioads.ams_header import AmsHeader
from aioads.commands.ads_add_notification import AdsAddNotificationCommand
from aioads.commands.ads_add_notification import TransmissionMode
from aioads.commands.ads_delete_notification import AdsDeleteNotificationCommand
from aioads.commands.ads_device_notification import filetime_to_datetime
from aioads.commands.ads_device_notification import iter_samples
from aioads.commands.errors import AdsCommandError
from aioads.functions.ads_symbol_info_by_name_ex import SymbolInfo
from aioads.stream import AdsStream
from aioads.transport import INotificationSink
from aioads.transport import ITransport

ADS_ERROR_NOTIFICATION_HANDLE_INVALID: Final[int] = 1812
"""The remote does not know the handle (anymore), there is nothing left to delete."""

DEFAULT_QUEUE_SIZE: Final[int] = 1000

class OverflowPolicy(enum.Enum):
    """
    What happens to the values of a subscription when its callback cannot
    keep up with the remote. Delivery never blocks the connection, so in every
    case values are discarded; the policy decides which ones.
    """

    DROP_OLDEST = enum.auto()
    """
    Keep the most recent ``queue_size`` values. The callback stays as close to
    the present as possible but loses history (monitoring, dashboards).
    """
    DROP_NEWEST = enum.auto()
    """
    Keep the ``queue_size`` values that arrived first and discard new ones
    until the callback caught up. Preserves an unbroken sequence up to the
    gap (recording, edge detection).
    """
    LATEST_ONLY = enum.auto()
    """
    Only the most recent value is of interest; older ones are replaced, not
    queued (``queue_size`` is ignored). This is the expected mode of operation
    and not reported as a warning (state mirroring).
    """


RouteKey = tuple[AmsAddress, int]
"""Notification handles are only unique per remote, so routes are keyed by (remote address, handle)."""


@dataclass(frozen=True, slots=True)
class NotificationValue:
    """
    The callback value for parsed notifications
    """
    handle: int
    """
    The handle returned when the subscription was created. It identifies the
    subscription for its whole lifetime, even if the handle on the remote had
    to be re-created after a reconnect.
    """
    timestamp: int
    """Raw remote timestamp (Windows FILETIME, 100 ns ticks since 1601-01-01 UTC)."""
    value: Any

    @property
    def datetime(self) -> datetime:
        """The timestamp as timezone aware (UTC) datetime."""
        return filetime_to_datetime(self.timestamp)


TNotificationCallback = Callable[[
    list[NotificationValue]], Coroutine[None, None, None]]


class Subscription:
    """
    A single registered notification: the decode plan used by the pump and the
    bounded delivery buffer drained by its own consumer task.

    Instances are created and owned by the :class:`NotificationRegistry`.
    """

    def __init__(
        self,
        owner: object,
        address: AmsAddress,
        symbol_info: SymbolInfo,
        parser: ISymbolParser,
        callback: TNotificationCallback,
        mode: TransmissionMode,
        cycle_time: int,
        max_delay: int,
        queue_size: int,
        overflow: OverflowPolicy,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        if overflow is OverflowPolicy.LATEST_ONLY:
            queue_size = 1
        self.overflow = overflow
        self.owner = owner
        self.address = address
        self.symbol_info = symbol_info
        self.parser = parser
        self.callback = callback
        self.mode = mode
        self.cycle_time = cycle_time
        self.max_delay = max_delay
        self.handle = 0
        """Stable id reported to the user (the first handle the remote assigned)."""
        self.remote_handle = 0
        """The handle currently registered on the remote."""
        self.closed = False
        self.dropped = 0
        """Number of values discarded (or replaced) because the callback could not keep up."""
        # Primitive symbols skip the generic parser: one precompiled unpack per sample.
        self._primitive: Struct | None = parser.primitive_struct(
            symbol_info.data_type, symbol_info.type_name
        )
        self._buffer: deque[NotificationValue] = deque(maxlen=queue_size)
        self._wakeup = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger(__name__)

    def decode(self, sample: AdsStream) -> Any:
        """
        Decode a single sample. Pure with respect to the subscription, so the
        pump may call it from a worker thread.
        """
        if self._primitive is not None and sample.length == self._primitive.size:
            return sample.read_struct(self._primitive)[0]
        return self.parser.parse(
            self.symbol_info.data_type, self.symbol_info.type_name, sample
        )

    def start(self) -> None:
        """Start the consumer task that feeds the callback."""
        if self._task is None:
            self._task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        """Stop the consumer task, values still buffered are discarded."""
        task, self._task = self._task, None
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    def push(self, values: list[NotificationValue]) -> None:
        """
        Buffer decoded values for the callback. Never blocks: if the buffer is
        full, values are discarded according to the overflow policy (and
        counted in :attr:`dropped`).
        """
        maxlen = self._buffer.maxlen or 0
        excess = len(self._buffer) + len(values) - maxlen
        if excess > 0:
            if self.dropped == 0 and self.overflow is not OverflowPolicy.LATEST_ONLY:
                self._logger.warning(
                    "Notification callback for %s (handle %d) cannot keep up, "
                    "dropping values (%s)",
                    self.symbol_info.symbol_name,
                    self.handle,
                    self.overflow.name,
                )
            self.dropped += excess
            if self.overflow is OverflowPolicy.DROP_NEWEST:
                values = values[: maxlen - len(self._buffer)]
        # A full deque discards from the opposite end, i.e. the oldest values.
        self._buffer.extend(values)
        self._wakeup.set()

    async def _consume(self) -> None:
        # ``closed`` ends the loop when the subscription is removed from
        # within its own callback (a task cannot await its own cancellation).
        while not self.closed:
            await self._wakeup.wait()
            self._wakeup.clear()
            if not self._buffer:
                continue
            # Everything that piled up while the last callback was running is
            # delivered as one batch.
            batch = list(self._buffer)
            self._buffer.clear()
            try:
                await self.callback(batch)
            except Exception:  # pylint: disable=broad-exception-caught
                self._logger.exception(
                    "Notification callback for %s (handle %d) failed",
                    self.symbol_info.symbol_name,
                    self.handle,
                )


@dataclass(slots=True)
class DecodeResult:
    """Outcome of decoding a batch of notification frames."""

    values: dict[Subscription, list[NotificationValue]] = field(default_factory=dict)
    unknown: set[RouteKey] = field(default_factory=set)
    errors: int = 0


def decode_frames(
    frames: list[tuple[AmsAddress, AdsStream]],
    routes: dict[RouteKey, Subscription],
) -> DecodeResult:
    """
    Decode a batch of notification frames into values grouped by subscription
    (wire order is preserved per subscription).

    Pure function: ``routes`` is a snapshot that is never mutated, so this is
    safe to run in a worker thread. Samples of handles without a route are not
    decoded, only reported in ``unknown``.
    """
    result = DecodeResult()
    values = result.values
    for address, stream in frames:
        try:
            for timestamp, handle, sample in iter_samples(stream):
                subscription = routes.get((address, handle))
                if subscription is None:
                    result.unknown.add((address, handle))
                    continue
                try:
                    value = subscription.decode(sample)
                except Exception:  # pylint: disable=broad-exception-caught
                    result.errors += 1
                    continue
                item = NotificationValue(subscription.handle, timestamp, value)
                batch = values.get(subscription)
                if batch is None:
                    values[subscription] = [item]
                else:
                    batch.append(item)
        except ValueError:
            # Truncated / malformed frame, keep what was decoded so far.
            result.errors += 1
    return result


class NotificationRegistry(INotificationSink):
    """
    Per transport owner of all notification handles. Attached to the transport
    as its notification sink, use :meth:`attach` to get the shared instance.

    Responsibilities:
        - routing of incoming notification frames to subscriptions without
          ever blocking the transport read loop
        - batched decoding (offloaded to a thread for big batches)
        - handle lifecycle: every handle that is not (or no longer) used is
          removed from the remote, including handles we never heard of
        - re-registration of all subscriptions after a reconnect
    """

    MAX_SUBSCRIPTIONS = 500
    """Limit per remote address. TwinCAT allows 1024 registrations per port for all clients together."""
    MAX_PENDING_FRAMES = 4096
    """Ingress bound; if the pump falls behind further the oldest frames are dropped."""
    MAX_BATCH_FRAMES = 256
    """Maximum number of frames decoded in one go."""
    OFFLOAD_SIZE = 16 * 1024
    """Batches with at least this many payload bytes are decoded in a worker thread."""
    REAP_RETRY_DELAY = 5.0
    """Seconds before a failed removal of an unknown handle is attempted again."""
    SHUTDOWN_TIMEOUT = 5.0
    """Maximum time spent on removing remote handles during shutdown."""

    def __init__(self, transport: ITransport) -> None:
        self._logger = logging.getLogger(__name__)
        self._transport = transport
        # Copy-on-write: the dict is replaced, never mutated, so the decode
        # thread can use it as a consistent snapshot without locking.
        self._routes: dict[RouteKey, Subscription] = {}
        self._removing: set[RouteKey] = set()
        """Deletion in flight; late samples of these handles are expected."""
        self._reaping: set[RouteKey] = set()
        """Unknown handles whose removal is in flight or cooling down."""
        self._pending_deletes: set[RouteKey] = set()
        """Deletions that failed and are retried after the next reconnect."""
        self._adds_in_flight: dict[AmsAddress, int] = {}
        self._parked: dict[AmsAddress, set[int]] = {}
        """Unknown handles seen while an add was in flight (may be the new handle)."""
        self._ingress: deque[tuple[AmsAddress, AdsStream]] = deque()
        self._ingress_event = asyncio.Event()
        self._pump_task: asyncio.Task[None] | None = None
        self._resync_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self.dropped_frames = 0
        """Frames discarded because the pump could not keep up with the remote."""
        self.decode_errors = 0
        """Samples / frames that could not be decoded."""

    @classmethod
    def attach(cls, transport: ITransport) -> "NotificationRegistry":
        """
        Return the registry of the transport, creating and attaching it on
        first use. All clients of a transport share the same registry, because
        only together they know every handle that is in use.
        """
        sink = transport.notification_sink
        if isinstance(sink, cls):
            return sink
        if sink is not None:
            raise RuntimeError(
                "The transport already has a different notification sink attached"
            )
        registry = cls(transport)
        transport.set_notification_sink(registry)
        return registry

    # ------------------------------------------------------------------ ingress

    def feed(self, ams_header: AmsHeader, payload: AdsStream) -> None:
        """
        Hand over a raw device notification frame. Called by the transport
        from its read loop: synchronous and O(1), all work happens in the pump.
        """
        if len(self._ingress) >= self.MAX_PENDING_FRAMES:
            self._ingress.popleft()
            if self.dropped_frames == 0:
                self._logger.warning(
                    "Notifications arrive faster than they can be processed, "
                    "dropping oldest frames"
                )
            self.dropped_frames += 1
        self._ingress.append((ams_header.source_ams_address, payload))
        self._ingress_event.set()
        if self._pump_task is None:
            self._pump_task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        while True:
            await self._ingress_event.wait()
            self._ingress_event.clear()
            while self._ingress:
                frames: list[tuple[AmsAddress, AdsStream]] = []
                size = 0
                while self._ingress and len(frames) < self.MAX_BATCH_FRAMES:
                    frame = self._ingress.popleft()
                    size += frame[1].length
                    frames.append(frame)
                try:
                    if size >= self.OFFLOAD_SIZE:
                        result = await asyncio.to_thread(
                            decode_frames, frames, self._routes
                        )
                    else:
                        result = decode_frames(frames, self._routes)
                    self._dispatch(result)
                except Exception:  # pylint: disable=broad-exception-caught
                    self._logger.exception("Failed to process notifications")
                # Let responses and callbacks run between batches.
                await asyncio.sleep(0)

    def _dispatch(self, result: DecodeResult) -> None:
        if result.errors:
            if self.decode_errors == 0:
                self._logger.warning("Failed to decode notification samples")
            self.decode_errors += result.errors
        for subscription, values in result.values.items():
            if not subscription.closed:
                subscription.push(values)
        for key in result.unknown:
            self._on_unknown(key)

    # ---------------------------------------------------------- unknown handles

    def _on_unknown(self, key: RouteKey) -> None:
        if key in self._routes or key in self._removing or key in self._reaping:
            return
        address, handle = key
        if self._adds_in_flight.get(address, 0) > 0:
            # Could be the handle of the add we have not seen the response for.
            self._parked.setdefault(address, set()).add(handle)
            return
        self._reap(key)

    def _reap(self, key: RouteKey) -> None:
        self._logger.warning(
            "Received notification for unknown handle %d from %s, removing it",
            key[1], key[0],
        )
        self._reaping.add(key)
        self._spawn(self._reap_remote(key))

    async def _reap_remote(self, key: RouteKey) -> None:
        try:
            await self._delete_remote(key)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self._logger.warning(
                "Failed to remove unknown notification handle %d: %r", key[1], ex
            )
            # Keep it marked for a while so a firing handle does not trigger a
            # delete request per frame.
            await asyncio.sleep(self.REAP_RETRY_DELAY)
        finally:
            self._reaping.discard(key)

    @contextlib.contextmanager
    def _adding(self, address: AmsAddress):
        """
        Marks an add request as in flight. The first samples of a new handle
        can be processed before the add response reached us, so unknown
        handles of that remote are parked instead of removed meanwhile.
        """
        self._adds_in_flight[address] = self._adds_in_flight.get(address, 0) + 1
        try:
            yield
        finally:
            self._adds_in_flight[address] -= 1
            if self._adds_in_flight[address] == 0:
                del self._adds_in_flight[address]
                for handle in self._parked.pop(address, ()):
                    self._on_unknown((address, handle))

    # ------------------------------------------------------------ remote access

    async def _add_remote(self, subscription: Subscription) -> int:
        command = AdsAddNotificationCommand(
            transport=self._transport,
            ams_address=subscription.address,
            idx_group=subscription.symbol_info.idx_group,
            idx_offset=subscription.symbol_info.idx_offset,
            length=subscription.symbol_info.idx_length,
            transmission_mode=subscription.mode,
            max_delay=subscription.max_delay,
            cycle_time=subscription.cycle_time,
        )
        response = await command.request()
        return response.notification_handle

    async def _delete_remote(self, key: RouteKey) -> None:
        address, handle = key
        command = AdsDeleteNotificationCommand(
            transport=self._transport,
            ams_address=address,
            notification_handle=handle,
        )
        try:
            await command.request()
        except AdsCommandError as ex:
            if ex.error_code != ADS_ERROR_NOTIFICATION_HANDLE_INVALID:
                raise

    async def _release(self, key: RouteKey) -> None:
        """
        Delete a handle we used to own. Expects ``key`` to be in ``_removing``.
        A failed deletion is remembered and retried after the next reconnect;
        if the handle fires before that it is removed as unknown handle.
        """
        try:
            await self._delete_remote(key)
            self._pending_deletes.discard(key)
            self._logger.info("Removed notification handle %d", key[1])
        except Exception as ex:  # pylint: disable=broad-exception-caught
            self._pending_deletes.add(key)
            self._logger.warning(
                "Failed to remove notification handle %d, will retry: %r", key[1], ex
            )
        finally:
            self._removing.discard(key)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _set_route(self, key: RouteKey, subscription: Subscription) -> None:
        self._routes = {**self._routes, key: subscription}

    def _drop_route(self, key: RouteKey) -> None:
        self._routes = {k: v for k, v in self._routes.items() if k != key}

    # --------------------------------------------------------------- public api

    @property
    def subscriptions(self) -> list[Subscription]:
        """All currently active subscriptions of this transport."""
        return list(self._routes.values())

    async def subscribe(
        self,
        owner: object,
        address: AmsAddress,
        symbol_info: SymbolInfo,
        parser: ISymbolParser,
        callback: TNotificationCallback,
        mode: TransmissionMode,
        cycle_time: int,
        max_delay: int,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        overflow: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ) -> Subscription:
        """
        Register a notification on the remote and start delivering its values.

        :param owner: Opaque token to release all subscriptions of one user
            via :meth:`unsubscribe_all`.
        :param queue_size: Maximum number of values buffered for the callback.
        :param overflow: Which values are discarded once the buffer is full.
        """
        active = sum(1 for sub in self._routes.values() if sub.address == address)
        if active >= self.MAX_SUBSCRIPTIONS:
            raise ValueError("Maximum number of notifications reached")

        subscription = Subscription(
            owner, address, symbol_info, parser, callback,
            mode, cycle_time, max_delay, queue_size, overflow,
        )
        with self._adding(address):
            handle = await self._add_remote(subscription)
            subscription.handle = handle
            subscription.remote_handle = handle
            self._set_route((address, handle), subscription)
        subscription.start()
        self._logger.info(
            "Created notification handle %d for symbol %s",
            handle, symbol_info.symbol_name,
        )
        return subscription

    async def unsubscribe(self, subscription: Subscription) -> None:
        """
        Stop delivery and remove the handle from the remote.

        Never raises for a failed removal: the handle stays tracked and is
        removed later (after a reconnect or as soon as it fires again). The
        removal also completes if the caller gets cancelled.
        """
        if subscription.closed:
            return
        subscription.closed = True
        key = (subscription.address, subscription.remote_handle)
        self._drop_route(key)
        self._removing.add(key)
        release = self._spawn(self._release(key))
        await subscription.stop()
        await asyncio.shield(release)

    async def unsubscribe_all(self, owner: object) -> None:
        """Remove all subscriptions that were created for ``owner``."""
        owned = [sub for sub in self._routes.values() if sub.owner is owner]
        if owned:
            await asyncio.gather(*(self.unsubscribe(sub) for sub in owned))

    def on_reconnected(self) -> None:
        """
        Called by the transport after the connection was re-established.

        The remote may or may not have kept our handles (it does not after a
        restart), so every subscription is registered again and its previous
        handle is deleted afterwards.
        """
        if self._resync_task is not None:
            self._resync_task.cancel()
        self._resync_task = self._spawn(self._resync())

    async def _resync(self) -> None:
        for key in list(self._pending_deletes):
            if key not in self._removing:
                self._removing.add(key)
                self._spawn(self._release(key))

        for subscription in list(self._routes.values()):
            if subscription.closed:
                continue
            address = subscription.address
            try:
                with self._adding(address):
                    new_handle = await self._add_remote(subscription)
                    old_key = (address, subscription.remote_handle)
                    if subscription.closed:
                        # Unsubscribed while we were waiting; nobody owns the new handle.
                        stale_key = (address, new_handle)
                    else:
                        subscription.remote_handle = new_handle
                        routes = {k: v for k, v in self._routes.items() if k != old_key}
                        routes[(address, new_handle)] = subscription
                        self._routes = routes
                        stale_key = old_key
                    self._removing.add(stale_key)
                    self._spawn(self._release(stale_key))
            except Exception as ex:  # pylint: disable=broad-exception-caught
                self._logger.error(
                    "Failed to re-register notification for %s after reconnect: %r",
                    subscription.symbol_info.symbol_name, ex,
                )

    async def shutdown(self) -> None:
        """
        Remove every remaining handle from the remote (best effort, bounded by
        SHUTDOWN_TIMEOUT) and stop all background work. Called by the transport
        before it closes the connection; the registry is reusable afterwards.
        """
        if self._resync_task is not None:
            self._resync_task.cancel()
            self._resync_task = None

        subscriptions = list(self._routes.values())
        if subscriptions:
            _, pending = await asyncio.wait(
                [asyncio.ensure_future(self.unsubscribe(sub)) for sub in subscriptions],
                timeout=self.SHUTDOWN_TIMEOUT,
            )
            for task in pending:
                task.cancel()
            if pending:
                self._logger.warning(
                    "%d notification handles could not be removed before shutdown",
                    len(pending),
                )
            for subscription in subscriptions:
                await subscription.stop()

        tasks = list(self._tasks)
        if self._pump_task is not None:
            tasks.append(self._pump_task)
            self._pump_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._routes = {}
        self._ingress.clear()
        self._removing.clear()
        self._reaping.clear()
        self._pending_deletes.clear()
        self._parked.clear()
        self._adds_in_flight.clear()


class NotificationManager:
    """
    Per client access to the notifications of a transport. Resolves symbols
    and ensures the subscriptions of this client are always removed again.
    """

    def __init__(
        self,
        transport: ITransport,
        dst_address: AmsAddress,
        symbol_cache: AdsSymbolCache,
        parser: ISymbolParser,
    ):
        self.transport = transport
        self.dst_address = dst_address
        self.symbol_cache = symbol_cache
        self.symbol_parser = parser
        self.registry = NotificationRegistry.attach(transport)

    @asynccontextmanager
    async def create_notification(
        self,
        symbol_name: str,
        callback: TNotificationCallback,
        mode: TransmissionMode,
        cycle_time: int,
        max_delay: int,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        overflow: OverflowPolicy = OverflowPolicy.DROP_OLDEST,
    ) -> AsyncGenerator[int, None]:
        """
        Context manager to subscribe to a symbol; the subscription is always
        removed again when the context is left.

        The callback receives the values in batches: everything that arrived
        while the previous call was still running is delivered at once.
        """
        symbol_info = await self.symbol_cache.read_symbol_info_by_name(symbol_name)
        subscription = await self.registry.subscribe(
            owner=self,
            address=self.dst_address,
            symbol_info=symbol_info,
            parser=self.symbol_parser,
            callback=callback,
            mode=mode,
            cycle_time=cycle_time,
            max_delay=max_delay,
            queue_size=queue_size,
            overflow=overflow,
        )
        try:
            yield subscription.handle
        finally:
            await self.registry.unsubscribe(subscription)

    async def close(self) -> None:
        """Remove all notifications created through this manager."""
        await self.registry.unsubscribe_all(self)
