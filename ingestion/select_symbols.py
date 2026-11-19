"""Select a whitelist of liquid Binance spot USDT pairs for the DWH.

Output: config/symbols.csv — future source for dim_symbol (dbt seed).
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://api.binance.com"
TIMEOUT_S = 10

QUOTE_ASSET = "USDT"
TOP_N = 25
MIN_SELECTED = 20
MIN_24H_VOLUME_PREFILTER_USDT = 5_000_000   # cheap prefilter to limit API calls
MIN_AVG_QUOTE_VOLUME_USDT = 10_000_000      # liquidity rule: 30-day average daily turnover
VOLUME_WINDOW_DAYS = 30
MIN_HISTORY_DAYS = 365
PEG_LOW = 0.98    # 30d close range inside [PEG_LOW, PEG_HIGH] => USD-pegged asset
PEG_HIGH = 1.02
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "config" / "symbols.csv"

EXCLUDED_BASE_ASSETS = {
    # Stablecoins / fiat-like (price vs USDT ~ 1)
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "PYUSD", "USDE",
    "EUR", "EURI", "AEUR", "XUSD", "BFUSD", "RLUSD", "USD1", "RWUSD",
    # Commodity-backed tokens (track gold, not crypto market)
    "PAXG", "XAUT",
    # Wrapped duplicates
    "WBTC", "WBETH",
}


def get_json(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET a Binance endpoint; on error show the response body (code/msg)."""
    response = requests.get(f"{BASE_URL}{path}", params=params, timeout=TIMEOUT_S)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code} on {path}: {response.text}")
    return response.json()


def get_trading_symbols() -> dict[str, dict[str, Any]]:
    """Spot USDT pairs that are currently trading, excluding stables/wrapped."""
    data = get_json("/api/v3/exchangeInfo")
    return {
        s["symbol"]: s
        for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["quoteAsset"] == QUOTE_ASSET
        and s.get("isSpotTradingAllowed", False)
        and s["baseAsset"] not in EXCLUDED_BASE_ASSETS
    }


def get_quote_volumes() -> dict[str, float]:
    """24h quote volume per symbol. float is OK here: used only for ranking, never stored as a fact."""
    tickers = get_json("/api/v3/ticker/24hr")
    return {t["symbol"]: float(t["quoteVolume"]) for t in tickers}


def get_first_open_time(symbol: str) -> datetime:
    """Open time of the very first daily candle = listing date (approx.)."""
    klines = get_json(
        "/api/v3/klines",
        {"symbol": symbol, "interval": "1d", "startTime": 0, "limit": 1},
    )
    return datetime.fromtimestamp(klines[0][0] / 1000, tz=timezone.utc)


def get_daily_stats(symbol: str, days: int = VOLUME_WINDOW_DAYS) -> tuple[float, float, float]:
    """Return (avg daily quote volume, min close, max close) over the last `days` closed daily candles."""
    klines = get_json(
        "/api/v3/klines",
        {"symbol": symbol, "interval": "1d", "limit": days + 1},
    )
    closed = klines[:-1]  # today's daily candle is not closed yet (DQ note #2)
    avg_volume = sum(float(k[7]) for k in closed) / len(closed)
    closes = [float(k[4]) for k in closed]
    return avg_volume, min(closes), max(closes)


def select_symbols() -> list[dict[str, Any]]:
    """Apply whitelist rules: history >= 1y, not USD-pegged, 30d avg volume >= threshold.

    Trade-off: the 24h prefilter may drop a symbol with a quiet day but high 30d average;
    accepted to keep API calls low.
    """
    symbols = get_trading_symbols()
    volumes_24h = get_quote_volumes()

    candidates = [s for s in symbols if volumes_24h.get(s, 0.0) >= MIN_24H_VOLUME_PREFILTER_USDT]
    print(
        f"TRADING {QUOTE_ASSET} pairs: {len(symbols)}, "
        f"passed 24h prefilter (${MIN_24H_VOLUME_PREFILTER_USDT:,}): {len(candidates)}"
    )

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MIN_HISTORY_DAYS)
    passed: list[dict[str, Any]] = []

    for symbol in candidates:
        first_open = get_first_open_time(symbol)
        if first_open > cutoff:
            print(f"  skip {symbol}: listed {first_open:%Y-%m-%d}, history < {MIN_HISTORY_DAYS} days")
            continue

        avg_volume, min_close, max_close = get_daily_stats(symbol)
        if PEG_LOW <= min_close and max_close <= PEG_HIGH:
            print(f"  skip {symbol}: USD-pegged, {VOLUME_WINDOW_DAYS}d close range {min_close}-{max_close}")
            continue
        if avg_volume < MIN_AVG_QUOTE_VOLUME_USDT:
            print(f"  skip {symbol}: {VOLUME_WINDOW_DAYS}d avg volume {avg_volume:,.0f} < {MIN_AVG_QUOTE_VOLUME_USDT:,}")
            continue

        passed.append(
            {
                "symbol": symbol,
                "base_asset": symbols[symbol]["baseAsset"],
                "quote_asset": QUOTE_ASSET,
                "avg_quote_volume_30d_usdt": round(avg_volume),
                "quote_volume_24h_usdt": round(volumes_24h[symbol]),
                "first_candle_date": first_open.date().isoformat(),
                "selected_at_utc": now.isoformat(timespec="seconds"),
            }
        )

    passed.sort(key=lambda r: r["avg_quote_volume_30d_usdt"], reverse=True)
    selected = passed[:TOP_N]

    if len(selected) < MIN_SELECTED:
        raise RuntimeError(f"Only {len(selected)} symbols passed filters, expected >= {MIN_SELECTED}")
    return selected


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = select_symbols()
    write_csv(rows, OUTPUT_PATH)
    print(f"\nSaved {len(rows)} symbols to {OUTPUT_PATH}:")
    for r in rows:
        print(f"  {r['symbol']:<12} {r['avg_quote_volume_30d_usdt']:>16,}  since {r['first_candle_date']}")


if __name__ == "__main__":
    main()