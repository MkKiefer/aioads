"""
Shared base class for all integration tests.
"""
import unittest

from tests.integration.config import CONFIG_PATH
from tests.integration.config import create_client
from tests.integration.config import load_config


@unittest.skipUnless(
    CONFIG_PATH.exists(),
    "Integration config missing: copy config.example.toml to config.toml",
)
class IntegrationTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Connects the configured client before each test and disconnects afterwards.
    Tests access the endpoint via `self.client` and the raw config via `self.config`.
    """

    async def asyncSetUp(self) -> None:
        self.config = load_config()
        self.client = create_client(self.config)
        await self.client.connect()

    async def asyncTearDown(self) -> None:
        await self.client.disconnect()
