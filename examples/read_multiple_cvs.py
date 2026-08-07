import argparse
import asyncio
import csv
import logging
from pathlib import Path

from aioads.ads_client import AdsClient
from aioads.ams_address import AmsAddress
from aioads.ams_service_port import AmsServicePort

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def load_symbols_from_csv(csv_file: Path) -> set[str]:
    symbols: set[str] = set()
    with csv_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol_name = (row.get("name") or "").strip()
            if symbol_name:
                symbols.add(symbol_name)
    return symbols


async def worker(client: AdsClient, csv_file: Path, interval: float) -> None:
    symbols = load_symbols_from_csv(csv_file)
    if not symbols:
        LOGGER.warning("Skipping %s: no symbols found", csv_file.name)
        return

    LOGGER.info(
        "Worker for %s started with %d symbols (interval %.2fs)",
        csv_file.name,
        len(symbols),
        interval,
    )

    while True:
        values = await client.read_symbols_by_names(symbols)
        LOGGER.info("%s read %d symbols", csv_file.name, len(values))
        await asyncio.sleep(interval)


async def main(csv_folder: Path, interval: float) -> None:
    csv_files = sorted(csv_folder.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in folder: {csv_folder}")

    client = AdsClient.create_tcp(
        src=AmsAddress(net_id="192.168.73.9.1.1", port=1234),
        dst=AmsAddress(net_id="192.168.73.1.1.1", port=AmsServicePort.TC3_RUNTIME_1),
        ip="192.168.73.1",
        port=48898,
    )

    try:
        await client.connect()
        async with asyncio.TaskGroup() as task_group:
            for csv_file in csv_files:
                task_group.create_task(
                    worker(client=client, csv_file=csv_file, interval=interval)
                )
    finally:
        await client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Continuously read symbols from all CSV files in a folder. "
            "Each CSV runs in its own worker task."
        )
    )
    parser.add_argument(
        "csv_folder",
        type=Path,
        nargs="?",
        default=Path(__file__).parent / "csv",
        help="Folder containing CSV files with 'name' column (default: examples/csv)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    asyncio.run(main(csv_folder=cli_args.csv_folder, interval=cli_args.interval))
