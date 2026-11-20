"""Test data factories: synthetic Binance klines and landing-zone batches, no network needed."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.batches import write_manifest
from ingestion.extract_klines import DAY_MS, INTERVAL_MS, Kline

START_MS = 1_735_689_600_000  # 2025-01-01 00:00:00 UTC, aligned to the hour


def make_kline(open_ms: int) -> Kline:
    """Build one kline in Binance's 12-field format; prices are strings like the real API."""
    close_ms = open_ms + INTERVAL_MS - 1
    return [
        open_ms,  # 0 open_time
        "100.00000000",  # 1 open
        "101.00000000",  # 2 high
        "99.00000000",  # 3 low
        "100.50000000",  # 4 close
        "10.00000000",  # 5 volume
        close_ms,  # 6 close_time
        "1005.00000000",  # 7 quote_asset_volume
        42,  # 8 number_of_trades
        "5.00000000",  # 9 taker_buy_base_volume
        "502.50000000",  # 10 taker_buy_quote_volume
        "0",  # 11 ignore
    ]


def make_klines(count: int, start_ms: int = START_MS) -> list[Kline]:
    """Build `count` consecutive hourly klines with no duplicates and no gaps."""
    return [make_kline(start_ms + i * INTERVAL_MS) for i in range(count)]


def make_symbol_info(
    base: str, quote: str = "USDT", status: str = "TRADING", spot: bool = True
) -> dict[str, Any]:
    """Build one entry of exchangeInfo["symbols"] with the fields select_symbols reads."""
    return {
        "symbol": base + quote,
        "status": status,
        "baseAsset": base,
        "quoteAsset": quote,
        "isSpotTradingAllowed": spot,
    }


def make_daily_klines(
    first_open_ms: int,
    last_open_ms: int,
    close: str = "100.00000000",
    quote_volume: str = "20000000.00000000",
) -> list[Kline]:
    """Build consecutive daily klines from first_open_ms to last_open_ms inclusive.

    Candles are flat (open = high = low = close): select_symbols reads only close and quote volume.
    """
    return [
        [
            open_ms,
            close,
            close,
            close,
            close,
            "1.00000000",
            open_ms + DAY_MS - 1,
            quote_volume,
            42,
            "0.50000000",
            "0",
            "0",
        ]
        for open_ms in range(first_open_ms, last_open_ms + 1, DAY_MS)
    ]


def make_marked_batch(root: Path, batch_id: str, last_open: dict[str, int]) -> Path:
    """A complete batch whose manifest records last_open_ms per symbol; no data files.

    Enough for high_water_marks, which reads the manifest: seeds what earlier runs extracted.
    """
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    write_manifest(
        batch_dir,
        {
            "batch_id": batch_id,
            "as_of_utc": "2025-01-01T00:00:00+00:00",
            "interval": "1h",
            "symbols": {
                s: {"rows": 1, "duplicates": 0, "gaps": 0, "last_open_ms": ms}
                for s, ms in last_open.items()
            },
        },
    )
    return batch_dir
