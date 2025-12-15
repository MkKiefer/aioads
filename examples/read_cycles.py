import asyncio
import logging

from aioads.ads_client import AdsClient
from aioads.ams_address import AmsAddress

logging.basicConfig(level=logging.DEBUG)


async def main():
    client = AdsClient.create_tcp(
        src=AmsAddress(net_id="192.168.178.12.1.1", port=1234),
        dst=AmsAddress(net_id="192.168.178.11.1.1", port=851),
        ip="192.168.178.11",
        port=48898,
    )
    try:

        await client.connect()
        task_info = "TwinCAT_SystemInfoVarList._TaskInfo[1]"
        last_cycle_cnt = 0
        for _ in range(500):
            symbol_value = await client.read_symbol_by_name(task_info)
            cycle_cnt = symbol_value["CycleCount"]
            # Set task cycle time in multiples of 100 ns
            cycle_time = symbol_value["CycleTime"]
            cycle_cnt_dif = cycle_cnt - last_cycle_cnt
            plc_time = cycle_cnt_dif * cycle_time / 10000  # Convert to ms
            print(
                f"Cycles changed: {cycle_cnt_dif} | Time changed: {plc_time}ms"
            )
            last_cycle_cnt = cycle_cnt

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
