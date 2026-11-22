"""Extract hourly long/short ratios of the whitelisted perpetuals: three series, three batches.

Run from repo root: python -m ingestion.extract_long_short
Contracts come from config/perpetuals.csv. Each series has its own landing zone and
watermark, data/raw/<series>/<batch_id>/<CONTRACT>.json, written by the algorithm shared
with open interest (futures_stats.py): Binance serves these only for about 30 days too.

Series (live shape: symbol, longAccount, shortAccount, longShortRatio, timestamp; the two
shares are rounded to 4 decimals, the ratio comes from the unrounded shares):
  top_account_ratio_1h   top 20% traders by margin balance: share of accounts long/short
  top_position_ratio_1h  the same traders: share of their position size long/short
  global_account_ratio_1h  all traders: share of accounts long/short
The fourth long/short endpoint, takerlongshortRatio, is left out: it is the taker buy/sell
volume of the hour starting at its timestamp, which futures klines keep for years (and the
endpoint reads slightly below them), so nothing is lost by not extracting it now.

One as-of moment for all three: a run gives the three batches the same batch id.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Sequence

from ingestion.clients.binance import BinanceFuturesClient
from ingestion.extract_cli import exit_code, parse_extract_args
from ingestion.extract_funding import load_contracts
from ingestion.futures_stats import PERIOD, StatsSeries, extract_batch, ms_to_utc

TOP_ACCOUNT_RATIO = StatsSeries(
    name=f"top_account_ratio_{PERIOD}", endpoint="topLongShortAccountRatio"
)
TOP_POSITION_RATIO = StatsSeries(
    name=f"top_position_ratio_{PERIOD}", endpoint="topLongShortPositionRatio"
)
GLOBAL_ACCOUNT_RATIO = StatsSeries(
    name=f"global_account_ratio_{PERIOD}", endpoint="globalLongShortAccountRatio"
)
SERIES = (TOP_ACCOUNT_RATIO, TOP_POSITION_RATIO, GLOBAL_ACCOUNT_RATIO)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_extract_args("Extract new 1h USD-M long/short ratios (three series).", argv)
    contracts = load_contracts()
    now_ms = int(time.time() * 1000)  # single as-of moment for every series
    print(
        f"Extracting {PERIOD} long/short ratios ({len(SERIES)} series) of {len(contracts)} "
        f"contracts as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )
    written = []
    with BinanceFuturesClient() as client:
        for series in SERIES:
            print(f"\n== {series.name} ==")
            batch_dir = extract_batch(client, series, contracts, now_ms)
            if batch_dir is not None:
                written.append(batch_dir)
    # Any batch is worth loading; 'nothing new' only when no series wrote one.
    return exit_code(written[0] if written else None, args.nothing_new_exit_code)


if __name__ == "__main__":
    sys.exit(main())
