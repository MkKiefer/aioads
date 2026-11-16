import asyncio
import logging

from aioads.ads_client import AdsClient
from aioads.ads_notifications import NotificationValue
from aioads.ads_notifications import OverflowPolicy
from aioads.ams_address import AmsAddress
from aioads.ams_service_port import AmsServicePort
from aioads.commands.ads_add_notification import TransmissionMode

logging.basicConfig(level=logging.DEBUG)


async def on_cycle_count(values: list[NotificationValue]) -> None:
    # Called with a batch: everything that arrived while the previous call
    # was still running is delivered at once.
    for item in values:
        print(f"{item.datetime.isoformat()} | CycleCount: {item.value}")


async def main():
    client = AdsClient.create_tcp(
        src=AmsAddress(net_id="192.168.178.12.1.1", port=1234),
        dst=AmsAddress(net_id="192.168.178.11.1.1", port=AmsServicePort.TC3_RUNTIME_1),
        ip="192.168.178.11",
        port=48898,
    )
    try:

        await client.connect()
        cycle_count = "TwinCAT_SystemInfoVarList._TaskInfo[1].CycleCount"
        # The PLC pushes the value instead of us polling it.
        async with client.subscribe_notification(
            cycle_count,
            on_cycle_count,
            mode=TransmissionMode.CYCLIC,
            cycle_time=100,  # ms, how often the PLC samples the value
            max_delay=500,  # ms, the PLC may bundle values up to this long
            queue_size=1000,  # values buffered for the callback
            overflow=OverflowPolicy.DROP_OLDEST,
        ) as handle:
            print(f"Subscribed to '{cycle_count}' with handle {handle}")
            await asyncio.sleep(10)
        # Leaving the context removes the notification from the PLC.

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
