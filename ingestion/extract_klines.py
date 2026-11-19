"""Extract hourly klines for whitelisted symbols from Binance and save them to JSON.

Run from repo root: python -m ingestion.extract_klines
"""

from __future__ import annotations

import csv
import json
import time
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

from ingestion.clients.binance import BinanceClient

Kline = list[Any]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS_PATH = PROJECT_ROOT / "config" / "symbols.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"

HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS
# Fixed-length Binance intervals only: "1M" (calendar month) has no constant length.
INTERVAL_MS_BY_NAME = {"1h": HOUR_MS, "1d": DAY_MS}

INTERVAL = "1h"
INTERVAL_MS = INTERVAL_MS_BY_NAME[INTERVAL]  # unsupported INTERVAL fails at import, not mid-run
PAGE_LIMIT = 1000
HISTORY_DAYS = 365
PAUSE_S = 0.2  # politeness pause between pages


class KlinesSource(Protocol):
    """Anything that serves klines like BinanceClient.klines: the real client or a test fake."""

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]: ...


def load_symbols(path: Path = SYMBOLS_PATH) -> list[str]:
    """Read whitelisted symbols from config/symbols.csv."""
    with path.open(encoding="utf-8", newline="") as f:
        return [row["symbol"] for row in csv.DictReader(f)]


def fetch_history(client: KlinesSource, symbol: str, start_ms: int, now_ms: int) -> list[Kline]:
    """Page through klines from start_ms until now_ms."""
    all_klines: list[Kline] = []
    while start_ms < now_ms:
        batch = client.klines(symbol, INTERVAL, start_time=start_ms, limit=PAGE_LIMIT)
        if not batch:
            break
        all_klines.extend(batch)
        start_ms = batch[-1][0] + INTERVAL_MS  # next candle after the last one received
        if len(batch) < PAGE_LIMIT:
            break  # reached the most recent candle
        time.sleep(PAUSE_S)
    return all_klines


def drop_unclosed(klines: list[Kline], now_ms: int) -> list[Kline]:
    """Keep only candles closed before the run's as-of moment (DQ note #2)."""
    return [k for k in klines if k[6] < now_ms]


def check_klines(klines: list[Kline]) -> dict[str, int]:
    """DQ checks: row count, duplicate open_time, gaps in the interval grid."""
    open_times = [k[0] for k in klines]
    unique = sorted(set(open_times))
    gaps = sum(1 for prev, curr in pairwise(unique) if curr - prev != INTERVAL_MS)
    return {
        "rows": len(klines),
        "duplicates": len(open_times) - len(unique),
        "gaps": gaps,
    }


def ms_to_utc(ms: int) -> datetime:
    """Convert Unix milliseconds to timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def main() -> None:
    symbols = load_symbols()
    now_ms = int(time.time() * 1000)  # single as-of moment for the whole run
    start_ms = now_ms - HISTORY_DAYS * DAY_MS
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(
        f"Extracting {len(symbols)} symbols, {INTERVAL}, "
        f"as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )

    results: dict[str, dict[str, int]] = {}
    with BinanceClient() as client:
        for i, symbol in enumerate(symbols, start=1):
            klines = drop_unclosed(fetch_history(client, symbol, start_ms, now_ms), now_ms)
            stats = check_klines(klines)
            results[symbol] = stats
            (OUTPUT_DIR / f"{symbol}.json").write_text(json.dumps(klines), encoding="utf-8")
            print(
                f"[{i:>2}/{len(symbols)}] {symbol:<10} rows={stats['rows']:>5} "
                f"dup={stats['duplicates']} gaps={stats['gaps']}"
            )

    with_issues = [s for s, st in results.items() if st["duplicates"] or st["gaps"]]
    print(f"\nDone: {len(results)} symbols, with DQ issues: {len(with_issues)} {with_issues or ''}")


if __name__ == "__main__":
    main()
