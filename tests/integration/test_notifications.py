"""
Integration tests for device notifications on the configured endpoint.
"""
import asyncio

from aioads.ads_notifications import NotificationRegistry
from aioads.ads_notifications import NotificationValue
from aioads.commands.ads_add_notification import TransmissionMode
from tests.integration.base import IntegrationTestCase


class TestNotifications(IntegrationTestCase):
    """Subscribe to the symbol defined as `symbols.notification` in the config."""

    async def test_subscribe_notification_delivers_values(self) -> None:
        symbol_name = self.config["symbols"]["notification"]
        received: list[NotificationValue] = []
        first_batch = asyncio.Event()

        async def callback(values: list[NotificationValue]) -> None:
            received.extend(values)
            first_batch.set()

        async with self.client.subscribe_notification(
            symbol_name, callback, TransmissionMode.CYCLIC, cycle_time=10, max_delay=50
        ) as handle:
            await asyncio.wait_for(first_batch.wait(), timeout=5)

        self.assertTrue(all(value.handle == handle for value in received))

    async def test_subscribe_notification_removes_handle_on_exit(self) -> None:
        symbol_name = self.config["symbols"]["notification"]

        async def callback(_values: list[NotificationValue]) -> None:
            return None

        async with self.client.subscribe_notification(symbol_name, callback):
            pass

        registry = NotificationRegistry.attach(self.client.transport)
        self.assertEqual(registry.subscriptions, [])
