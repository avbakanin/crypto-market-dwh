"""Extract hourly open interest of the whitelisted perpetuals into a new batch directory.

Run from repo root: python -m ingestion.extract_open_interest
Contracts come from config/perpetuals.csv (see map_perpetuals). Writes
data/raw/open_interest_1h/<batch_id>/<CONTRACT>.json and, last, _manifest.json.
The algorithm (served window, last-`limit` paging, increment) is shared with the other
/futures/data/ series: see futures_stats.py.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Sequence

from ingestion.clients.binance import BinanceFuturesClient
from ingestion.extract_cli import exit_code, parse_extract_args
from ingestion.extract_funding import load_contracts
from ingestion.futures_stats import PERIOD, StatsSeries, extract_batch, ms_to_utc

OPEN_INTEREST = StatsSeries(name=f"open_interest_{PERIOD}", endpoint="openInterestHist")
OUTPUT_DIR = OPEN_INTEREST.output_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_extract_args("Extract new 1h USD-M open interest.", argv)
    contracts = load_contracts()
    now_ms = int(time.time() * 1000)  # single as-of moment for the whole run
    print(
        f"Extracting {PERIOD} open interest of {len(contracts)} contracts "
        f"as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )
    with BinanceFuturesClient() as client:
        batch_dir = extract_batch(client, OPEN_INTEREST, contracts, now_ms)
    return exit_code(batch_dir, args.nothing_new_exit_code)


if __name__ == "__main__":
    sys.exit(main())
