"""
Load the integration test configuration and build a client from it.

The configuration lives in `config.toml` next to this file, see
`config.example.toml` for a documented template.
"""
import tomllib
from pathlib import Path
from typing import Any

from aioads.ads_client import AdsClient
from aioads.ams_address import AmsAddress

CONFIG_PATH = Path(__file__).parent / "config.toml"


def load_config() -> dict[str, Any]:
    """
    Load the integration test configuration from `config.toml`.
    """
    with CONFIG_PATH.open("rb") as file:
        return tomllib.load(file)


def create_client(config: dict[str, Any]) -> AdsClient:
    """
    Create an `AdsClient` for the transport selected in the configuration.
    """
    connection = config["connection"]
    src = AmsAddress(
        net_id=connection["src_net_id"], port=connection["src_port"])
    dst = AmsAddress(
        net_id=connection["dst_net_id"], port=connection["dst_port"])
    transport_name = connection["transport"]
    options = config["transport"][transport_name]

    if transport_name == "tcp":
        return AdsClient.create_tcp(
            src=src, dst=dst, ip=options["ip"], port=options["port"]
        )

    if transport_name == "aiomqtt":
        from aioads.transport import AdsAioMqttTransport  # pylint: disable=import-outside-toplevel

        transport_aiomqtt = AdsAioMqttTransport(
            src=src, name=options["name"], url=options["url"], prefix=options["prefix"]
        )
        return AdsClient.create_from_transport(dst=dst, transport=transport_aiomqtt)

    if transport_name == "gmqtt":
        from aioads.transport import AdsGMqttTransport  # pylint: disable=import-outside-toplevel

        transport_gmqtt = AdsGMqttTransport(
            src=src, name=options["name"], url=options["url"], prefix=options["prefix"]
        )
        return AdsClient.create_from_transport(dst=dst, transport=transport_gmqtt)

    raise ValueError(f"Unknown transport '{transport_name}' in {CONFIG_PATH}")
