# AIOADS - Asynchronous ADS Library for Python

An asynchronous Python library for communicating with Beckhoff TwinCAT PLCs using the ADS (Automation Device Specification) protocol. This library provides high-performance, async/await-based communication with PLCs for reading symbols, data types, notifications, and more.

## Features

- ✨ **Fully asynchronous** - Built from the ground up with asyncio
- 🚀 **High performance** - Efficient batch operations and smart caching
- 📡 **ADS over TCP** - Direct communication with TwinCAT systems
- 🔍 **Symbol resolution** - Automatic symbol table parsing and caching
- 📊 **Data type support** - Complete TwinCAT data type parsing
- 🔔 **Notifications** - Event-driven data change monitoring
- 🛠️ **Type safety** - Full typing support for better development experience
- ⚡ **Smart caching** - Intelligent symbol and data type caching for performance

## Installation

```bash
# Ads TCP
pdm add aioads
pip install aioads

# ADS over MQTT (gmqtt)
pdm add aioads[gmqtt]
pip install aioads[gmqtt]

# Ads over MQTT (aiomqtt)
pdm add aioads[aiomqtt]
pip install aioads[aiomqtt]

```

## Quick Start

```python
import asyncio
from aioads import AdsClient, AmsAddress
from aioads.ams_service_port import AmsServicePort

async def main():
    # Create client
    client = AdsClient.create_tcp(
        src=AmsAddress(net_id="192.168.1.100.1.1", port=1234),
        dst=AmsAddress(net_id="192.168.1.200.1.1", port=AmsServicePort.TC3_RUNTIME_1),
        ip="192.168.1.200",
        port=48898,
    )

    try:
        # Connect to PLC
        await client.connect()

        # Read device state
        state = await client.read_state()
        print(f"PLC State: {state.ads_state.name}")

        # Read single symbol
        value = await client.read_symbol_by_name("MAIN.MyVariable")
        print(f"Value: {value}")

        # Read multiple symbols efficiently
        symbols = ["MAIN.Var1", "MAIN.Var2", "MAIN.Var3"]
        values = await client.read_symbols_by_names(symbols)
        print(f"Values: {values}")

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

## Core Components

### AdsClient

The main interface for ADS communication:

```python
from aioads import AdsClient, AmsAddress

# Create TCP client
client = AdsClient.create_tcp(
    src=AmsAddress(net_id="source.net.id.1.1", port=1234),
    dst=AmsAddress(net_id="target.net.id.1.1", port=851),
    ip="192.168.1.100"
)

# Connect
await client.connect()

# Read operations
state = await client.read_state()
symbols = await client.get_symbols()
data_types = await client.get_symbol_datatypes()
value = await client.read_symbol_by_name("MAIN.Variable")
values = await client.read_symbols_by_names(["Var1", "Var2"])

# Disconnect
await client.disconnect()
```

### Batch Operations

Efficiently read multiple symbols in a single operation:

```python
# Prepare symbol list
symbols_to_read = {
    "MAIN.Temperature",
    "MAIN.Pressure",
    "MAIN.FlowRate",
    "MAIN.Status.Running",
    "MAIN.Recipe.CurrentStep"
}

# Read all symbols in one batch operation
values = await client.read_symbols_by_names(symbols_to_read)

for symbol_name, value in values.items():
    print(f"{symbol_name}: {value}")
```

## Advanced Usage

### Custom Transport

You can provide your own transport implementation:

```python
from aioads.tcp_transport import AdsTcpTransport
from aioads.ads_symbol_cache import AdsSymbolCache

transport = AdsTcpTransport(
    src_address=src_address,
    ip="192.168.1.100",
    port=48898
)

cache = AdsSymbolCache(transport=transport, dst_address=dst_address)
client = AdsClient(transport=transport, dst_address=dst_address, cache=cache)
```

### Performance Tuning

For high-performance applications:

```python
# Pre-load symbol information to cache
await client.read_symbol_infos_by_names(large_symbol_list)

# Use batch reads for efficiency
start = time.perf_counter()
values = await client.read_symbols_by_names(symbols)
duration = (time.perf_counter() - start) * 1000
print(f"Read {len(values)} symbols in {duration:.2f}ms")
```

### Concurrent Operations

Handle multiple tasks concurrently:

```python
import asyncio

async def read_process_data(client, process_symbols):
    while True:
        data = await client.read_symbols_by_names(process_symbols)
        # Process data...
        await asyncio.sleep(0.1)

async def read_diagnostics(client, diag_symbols):
    while True:
        data = await client.read_symbols_by_names(diag_symbols)
        # Process diagnostics...
        await asyncio.sleep(1.0)

async def main():
    client = AdsClient.create_tcp(...)
    await client.connect()

    try:
        # Run multiple concurrent tasks
        async with asyncio.TaskGroup() as tg:
            tg.create_task(read_process_data(client, process_symbols))
            tg.create_task(read_diagnostics(client, diagnostic_symbols))
    finally:
        await client.disconnect()
```

## Protocol Support

### ADS Commands

The library supports all major ADS commands:

- **Read/Write Operations**: Single and batch
- **Device Information**: State, version, and device info
- **Symbol Management**: Symbol table upload and parsing
- **Data Types**: Complete data type information and parsing
- **Notifications**: Change and cyclic notifications (Still in preview)
- **Sum Commands**: Efficient bulk operations

### Data Types

Full support for TwinCAT data types:

- **Basic Types**: BOOL, BYTE, WORD, DWORD, INT, REAL, STRING, etc.
- **Structured Types**: STRUCT, ARRAY, UNION
- **Complex Types**: Custom user-defined types
- **Arrays**: Multi-dimensional arrays with proper indexing

## Requirements

- Python 3.11+
- asyncio
- aiomqtt (optional)

## Disclaimer

This project is an independent, open‑source implementation of the ADS (Automation Device Specification) protocol. It is not affiliated with, endorsed by, or supported by Beckhoff Automation GmbH & Co. KG, the developer of TwinCAT and the ADS protocol.
All trademarks and product names are the property of their respective owners.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Troubleshooting

### Connection Issues

1. **Firewall**: Ensure port 48898 (ADS) is open
2. **AMS Routes**: Verify AMS routes are configured on target system
3. **Network**: Check IP connectivity between systems

### Symbol Access

1. **PLC State**: Ensure PLC is in RUN mode for symbol access

## Contributing guidelines

AIOADS is a personal hobby project, developed and maintained in my spare time.
While I aim to keep it functional and improve it over time, it may not always receive frequent updates, and features or bug fixes might take a while.

If you rely on this library in production environments, consider reviewing the code, contributing improvements, or opening issues so the community can help keep it healthy.
