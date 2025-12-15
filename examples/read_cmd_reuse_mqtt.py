"""
This is a example for how we can use the system in a more complex way with the possibility
to improve performance by for example passing the stream to a external Thread for parsing and re-using
the command. Re using the command comes with the possibility that a plc restart can change the address space of the variables
"""

import asyncio
import logging

from aioads.ads_client import AdsClient
from aioads.ams_address import AmsAddress
from aioads.commands.ads_read import AdsReadCommand
from aioads.transport import AdsMqttTransport

logging.basicConfig(level=logging.DEBUG)


async def main():
    transport = AdsMqttTransport(
        src=AmsAddress(net_id="192.168.178.12.1.1", port=1234),
        name="AdsClient",
        url="mqtt://127.0.0.1:1883",
        prefix="ads"
    )
    client = AdsClient.create_from_transport(
        dst=AmsAddress(net_id="192.168.178.11.1.1", port=851),
        transport=transport
    )
    try:

        await client.connect()
        task_info = "TwinCAT_SystemInfoVarList._TaskInfo[1]"
        symbol_info = await client.read_symbol_info_by_name(task_info)
        read_command = AdsReadCommand(
            transport=transport,
            ams_address=client.dst_address,
            idx_group=symbol_info.idx_group,
            idx_offset=symbol_info.idx_offset,
            length=symbol_info.idx_length,
        )
        last_cycle_cnt = 0
        for _ in range(500):

            _, stream = await read_command.request()
            symbol_value = client.parser.parse(
                symbol_info.data_type,
                type_name=symbol_info.type_name,
                raw_data=stream
            )

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
