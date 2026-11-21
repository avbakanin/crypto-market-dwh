"""Test data factories: synthetic Binance klines and landing-zone batches, no network needed."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.batches import write_manifest
from ingestion.extract_klines import DAY_MS, HOUR_MS, INTERVAL_MS, Kline

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


def make_contract(
    base: str,
    symbol: str | None = None,
    contract_type: str = "PERPETUAL",
    status: str = "TRADING",
    quote: str = "USDT",
) -> dict[str, Any]:
    """Build one entry of the futures exchangeInfo["symbols"] (live field names and values)."""
    return {
        "symbol": symbol or base + quote,
        "pair": base + quote,
        "contractType": contract_type,
        "status": status,
        "baseAsset": base,
        "quoteAsset": quote,
        "marginAsset": quote,
    }


def make_premium_index(symbol: str, interest_rate: str = "0.00010000") -> dict[str, Any]:
    """One entry of /fapi/v1/premiumIndex (no symbol: a list) in the live shape.

    lastFundingRate differs from interestRate on purpose: copying the wrong field fails.
    """
    return {
        "symbol": symbol,
        "markPrice": "100.00000000",
        "indexPrice": "100.01000000",
        "estimatedSettlePrice": "100.00500000",
        "lastFundingRate": "0.00003810",
        "interestRate": interest_rate,
        "nextFundingTime": 1_735_718_400_000,
        "time": 1_735_689_600_000,
    }


def make_funding(symbol: str, funding_ms: int, rate: str = "0.00010000") -> dict[str, Any]:
    """One record of /fapi/v1/fundingRate in the live shape: numbers as strings."""
    return {
        "symbol": symbol,
        "fundingTime": funding_ms,
        "fundingRate": rate,
        "markPrice": "100.00000000",
        "rateType": "Regular",
    }


def make_fundings(
    symbol: str, first_ms: int, count: int, interval_hours: int = 8
) -> list[dict[str, Any]]:
    """`count` consecutive settlements from first_ms; each lands 0-26 ms after its hour, as live."""
    step = interval_hours * HOUR_MS
    return [make_funding(symbol, first_ms + i * step + i * 7 % 27) for i in range(count)]


def make_open_interest(symbol: str, timestamp_ms: int) -> dict[str, Any]:
    """One point of /futures/data/openInterestHist in the live shape: numbers as strings."""
    return {
        "symbol": symbol,
        "sumOpenInterest": "96000.00000000",
        "sumOpenInterestValue": "5600000000.00000000",
        "CMCCirculatingSupply": "20114434.00000000",
        "timestamp": timestamp_ms,
    }


def make_open_interests(symbol: str, first_ms: int, count: int) -> list[dict[str, Any]]:
    """`count` consecutive hourly points from first_ms (on the hour, as live)."""
    return [make_open_interest(symbol, first_ms + i * HOUR_MS) for i in range(count)]


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


def make_marked_batch(
    root: Path, batch_id: str, last_open: dict[str, int], key: str = "last_open_ms"
) -> Path:
    """A complete batch whose manifest records the newest record time per symbol under `key`
    (klines: last_open_ms, other datasets: last_ms); no data files.

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
                s: {"rows": 1, "duplicates": 0, "gaps": 0, key: ms} for s, ms in last_open.items()
            },
        },
    )
    return batch_dir
