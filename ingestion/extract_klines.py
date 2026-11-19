"""Extract hourly klines from Binance public API and save them to JSON."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"
OUTPUT_DIR = Path("data/raw")
HOUR_MS = 60 * 60 * 1000
PAGE_LIMIT = 1000


def fetch_klines(symbol: str, start_ms: int, interval: str = "1h") -> list[list]:
    """Return one page of klines starting from start_ms (inclusive)."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "limit": PAGE_LIMIT,
    }
    response = requests.get(BASE_URL + KLINES_ENDPOINT, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_history(symbol: str, days: int) -> list[list]:
    """Page through the API from (now - days) until now."""
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 24 * HOUR_MS
    all_klines: list[list] = []

    while start_ms < now_ms:
        batch = fetch_klines(symbol, start_ms)
        if not batch:
            break
        all_klines.extend(batch)
        print(f"  {symbol}: +{len(batch)} candles, up to {ms_to_utc(batch[-1][0])}")

        start_ms = batch[-1][0] + HOUR_MS  # next candle after the last one received
        if len(batch) < PAGE_LIMIT:
            break  # reached the most recent candle
        time.sleep(0.2)  # be gentle with API rate limits

    return all_klines


def drop_unclosed(klines: list[list]) -> list[list]:
    """Remove the current candle that is not closed yet."""
    now_ms = int(time.time() * 1000)
    return [k for k in klines if k[6] < now_ms]


def check_klines(klines: list[list]) -> None:
    """Basic data quality checks: row count, duplicates, gaps in the hourly grid."""
    open_times = [k[0] for k in klines]
    duplicates = len(open_times) - len(set(open_times))
    gaps = [
        (ms_to_utc(prev), ms_to_utc(curr))
        for prev, curr in zip(open_times, open_times[1:])
        if curr - prev != HOUR_MS
    ]
    print(f"Rows: {len(klines)}")
    print(f"Duplicate open_time: {duplicates}")
    print(f"Gaps in hourly grid: {len(gaps)}")
    for prev, curr in gaps[:10]:
        print(f"  gap: {prev} -> {curr}")


def ms_to_utc(ms: int) -> datetime:
    """Convert Unix milliseconds to timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def main() -> None:
    symbol = "BTCUSDT"
    klines = drop_unclosed(fetch_history(symbol, days=365))
    check_klines(klines)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / f"{symbol}.json"
    out_file.write_text(json.dumps(klines), encoding="utf-8")
    print(f"Saved to {out_file}")


if __name__ == "__main__":
    main()