"""
Integration tests for the basic connection lifecycle.
"""
from aioads.commands.ads_read_state import AdsState

from tests.integration.base import IntegrationTestCase


class TestConnection(IntegrationTestCase):
    """Connect to the configured endpoint and check the device responds."""

    async def test_read_state(self) -> None:
        state = await self.client.read_state()
        self.assertIsInstance(state.ads_state, AdsState)

    async def test_reconnect(self) -> None:
        await self.client.disconnect()
        await self.client.connect()
        state = await self.client.read_state()
        self.assertIsInstance(state.ads_state, AdsState)
