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
        variables = {
            "TwinCAT_SystemInfoVarList._AppInfo.TaskCnt",
            "TwinCAT_SystemInfoVarList._AppInfo.OnlineChangeCnt",
            "TwinCAT_SystemInfoVarList._AppInfo.AdsPort",
            "TwinCAT_SystemInfoVarList._AppInfo.BootDataLoaded",
            "TwinCAT_SystemInfoVarList._AppInfo.OldBootData",
            "TwinCAT_SystemInfoVarList._AppInfo.AppTimestamp",
            "TwinCAT_SystemInfoVarList._AppInfo.KeepOutputsOnBP",
            "TwinCAT_SystemInfoVarList._AppInfo.ShutdownInProgress",
            "TwinCAT_SystemInfoVarList._AppInfo.LicensesPending",
            "TwinCAT_SystemInfoVarList._AppInfo.BSODOccured",
            "TwinCAT_SystemInfoVarList._AppInfo.LoggedIn",
            "TwinCAT_SystemInfoVarList._AppInfo.PersistentStatus",
            "TwinCAT_SystemInfoVarList._AppInfo.AppName",
            "TwinCAT_SystemInfoVarList._AppInfo.ProjectName",
            # "TwinCAT_SystemInfoVarList._AppInfo.ActivePlcForces",
            # "TwinCAT_SystemInfoVarList._AppInfo.ActiveIoForces",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].ObjId",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].CycleTime",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].Priority",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].AdsPort",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].CycleCount",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].DcTaskTime",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].LastExecTime",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].FirstCycle",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].CycleTimeExceeded",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].InCallAfterOutputUpdate",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].RTViolation",
            "TwinCAT_SystemInfoVarList._TaskInfo[1].TaskName",
        }

        last_cycle_cnt = 0
        for _ in range(500):
            symbol_value = await client.read_symbols_by_names(variables)
            cycle_cnt = symbol_value["TwinCAT_SystemInfoVarList._TaskInfo[1].CycleCount"].value
            # Set task cycle time in multiples of 100 ns
            cycle_time = symbol_value["TwinCAT_SystemInfoVarList._TaskInfo[1].CycleTime"].value
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
