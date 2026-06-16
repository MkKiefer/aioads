# Integration tests

These tests run against a real ADS endpoint (e.g. a TwinCAT PLC).
They are **skipped automatically** if no `config.toml` is present, so they
never interfere with the unit test suite or CI.

## Setup

1. Copy the template:

   ```bash
   cp tests/integration/config.example.toml tests/integration/config.toml
   ```

2. Edit `config.toml`:
   - Pick the transport (`tcp`, `aiomqtt` or `gmqtt`) in `[connection]` and
     adjust the matching `[transport.*]` section.
   - Set the source/destination AMS addresses.
   - Adjust the `[symbols]` section if the default TwinCAT system symbols are
     not available on your target.

   `config.toml` is gitignored, so your local endpoint settings stay local.

3. Run the tests:

   ```bash
   pdm run pytest tests/integration -v
   ```

## Structure

- `config.py` — loads `config.toml` and builds the `AdsClient` for the configured transport
- `base.py` — base test case that connects before and disconnects after each test
- `test_connection.py` — connection lifecycle and device state
- `test_read_symbols.py` — single / multiple symbol reads and symbol info
- `test_notifications.py` — device notification subscription
