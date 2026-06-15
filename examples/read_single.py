import asyncio
import logging

from aioads.ads_client import AdsClient
from aioads.ams_address import AmsAddress
from aioads.ams_service_port import AmsServicePort

logging.basicConfig(level=logging.DEBUG)


async def main():
    client = AdsClient.create_tcp(
        src=AmsAddress(net_id="192.168.178.12.1.1", port=1234),
        dst=AmsAddress(net_id="192.168.178.11.1.1", port=AmsServicePort.TC3_RUNTIME_1),
        ip="192.168.178.11",
        port=48898,
    )
    try:

        await client.connect()
        state = await client.read_state()
        print(
            f"ADS State: {state.ads_state.name} | Device State: {state.device_state.name}"
        )

        symbols = await client.get_symbols()
        print(f"Read all root symbols: {len(symbols)} symbols found")

        data_types = await client.get_symbol_datatypes()
        print(f"Read all root data types: {len(data_types)} data types found")

        task_info = "TwinCAT_SystemInfoVarList._TaskInfo[1]"
        symbol_value = await client.read_symbol_by_name(task_info)
        cycle_time = symbol_value["CycleTime"]
        print(f"Read symbol '{task_info}' with CycleTime: {cycle_time}µs")

        last_cycle_cnt = 0
        for _ in range(99):
            symbol_value = await client.read_symbol_by_name(task_info)
            cycle_cnt = symbol_value["CycleCount"]
            diff = cycle_cnt - last_cycle_cnt
            last_cycle_cnt = cycle_cnt
            print(
                f"CycleCount: {cycle_cnt} | CycleTime: {symbol_value['CycleTime']}µs | Diff: {diff}"
            )

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
