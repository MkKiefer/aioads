"""
Ads function implementation to switch a MQTT router on / off

```mermaid
---
title: "ADS Sum Read/Write Request"
config:
    packet:
        rowHeight: 40
        bitWidth: 80
        bitsPerRow: 8
        showBits: ture
---
packet
+4: "IndexGroup = 808 (4 bytes)"
+4: "IndexOffset (1-4) (4 bytes) defines enable or disable"
+4: "Data length (4 bytes)"
+8: "Route identifier string (MQTT:<Address>:<Topic> or MQTT:<Name>)"
```

IndexOffset values:
1 = ADS_ROUTE_DISABLE       (permanent)
2 = ADS_ROUTE_ENABLE        (permanent)
3 = ADS_ROUTE_DISABLE_TMP   (temporary)
4 = ADS_ROUTE_ENABLE_TMP    (temporary)


Example of a route config `/3.1/Target/Routes/<route.xml>` 
```
<!-- Route configuration example -->
<Mqtt Disabled="true">
    <Name>MyBroker</Name>
    <Address Port="1883">myMessageBrokerAddress</Address>
    <Topic>VirtualAmsNetwork1</Topic>
</Mqtt>
```

Expected route name string: `MQTT:MyBroker` 
"""

from enum import IntEnum

from aioads.ams_address import AmsAddress
from aioads.commands.ads_write import AdsWriteCommand, AdsWriteResponse
from aioads.functions.ads_function import AdsFunctionSymbolGroup, IAdsFunction
from aioads.transport import ITransport


class RouteSwitch(IntEnum):
    """
    Enum (index offset) to control the state of a route. 
    A route can temporarily enabled / disabled and gets reset after a restart of the runtime. 
    """
    ROUTE_DISABLE = 1
    ROUTE_ENABLE = 2
    ROUTE_DISABLE_TMP = 3
    ROUTE_ENABLE_TMP = 4


class AdsEnableRoute(IAdsFunction[AdsWriteResponse]):
    """
    This is a function that can be called against the System Service (ADS Port 10000)
    to enable or disable a ads route. This is documented in use for service cases where
    we can enable ads over mqtt and disable it again.

    As for now this is only tested for mqtt routes enable / disable
    The route names are build from the following.
    `MQTT:<NetID>:<Topic>`
    or if a name in the mqtt route config section is defined
    `MQTT:<RouteName>`

    I think that this interface allows the same for the default router but i have not tested it yet. 
    """

    SYSTEM_SERVICE_PORT = 10000

    def __init__(self, transport: ITransport, ams_address: AmsAddress, route: str, switch: RouteSwitch) -> None:
        # We modify the port here as this function only works on the system service
        self.modified_address = AmsAddress(
            net_id=ams_address.net_id,
            port=self.SYSTEM_SERVICE_PORT
        )
        self.transport = transport
        self.route = route
        self.switch = switch

    def serialize(self) -> bytes:
        """serialize the function payload to bytes"""
        return self.route.encode("utf-8")

    async def execute(self):
        command = AdsWriteCommand(
            transport=self.transport,
            ams_address=self.modified_address,
            idx_group=AdsFunctionSymbolGroup.ADSIGRP_TOGGLE_ROUTE_ENABLE,
            idx_offset=self.switch.value,
            payload=self.serialize()
        )
        return await command.request()
