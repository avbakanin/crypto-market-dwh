"""Select a whitelist of liquid Binance spot USDT pairs for the DWH.

Output: config/symbols.csv — future source for dim_symbol (dbt seed).
Run from repo root: python -m ingestion.select_symbols
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from ingestion.clients.binance import BinanceClient

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


class SymbolsSource(Protocol):
    """What select_symbols needs from an exchange: the real BinanceClient or a test fake."""

    def exchange_info(self) -> dict[str, Any]: ...

    def ticker_24h(self) -> list[dict[str, Any]]: ...

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[list[Any]]: ...


def get_trading_symbols(client: SymbolsSource) -> dict[str, dict[str, Any]]:
    """Spot USDT pairs that are currently trading, excluding stables/wrapped."""
    data = client.exchange_info()
    return {
        s["symbol"]: s
        for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["quoteAsset"] == QUOTE_ASSET
        and s.get("isSpotTradingAllowed", False)
        and s["baseAsset"] not in EXCLUDED_BASE_ASSETS
    }


def get_quote_volumes(client: SymbolsSource) -> dict[str, float]:
    """24h quote volume per symbol.

    float is OK here: used only for ranking, never stored as a fact.
    """
    return {t["symbol"]: float(t["quoteVolume"]) for t in client.ticker_24h()}


def get_first_open_time(client: SymbolsSource, symbol: str) -> datetime:
    """Open time of the very first daily candle = listing date (approx.)."""
    klines = client.klines(symbol, "1d", start_time=0, limit=1)
    return datetime.fromtimestamp(klines[0][0] / 1000, tz=timezone.utc)


def get_daily_stats(
    client: SymbolsSource, symbol: str, days: int = VOLUME_WINDOW_DAYS
) -> tuple[float, float, float]:
    """Return (avg daily quote volume, min close, max close) over the last `days` closed days."""
    klines = client.klines(symbol, "1d", limit=days + 1)  # no start_time => most recent candles
    closed = klines[:-1]  # the most recent one is today's, still open (DQ note #14)
    avg_volume = sum(float(k[7]) for k in closed) / len(closed)
    closes = [float(k[4]) for k in closed]
    return avg_volume, min(closes), max(closes)


def select_symbols(client: SymbolsSource, now: datetime) -> list[dict[str, Any]]:
    """Apply whitelist rules: history >= 1y, not USD-pegged, 30d avg volume >= threshold.

    `now` is the run's as-of moment: the history cutoff and selected_at_utc derive from it.

    Trade-off: the 24h prefilter may drop a symbol with a quiet day but high 30d average;
    accepted to keep API calls low.
    """
    symbols = get_trading_symbols(client)
    volumes_24h = get_quote_volumes(client)

    candidates = [s for s in symbols if volumes_24h.get(s, 0.0) >= MIN_24H_VOLUME_PREFILTER_USDT]
    print(
        f"TRADING {QUOTE_ASSET} pairs: {len(symbols)}, "
        f"passed 24h prefilter (${MIN_24H_VOLUME_PREFILTER_USDT:,}): {len(candidates)}"
    )

    cutoff = now - timedelta(days=MIN_HISTORY_DAYS)
    passed: list[dict[str, Any]] = []

    for symbol in candidates:
        first_open = get_first_open_time(client, symbol)
        if first_open > cutoff:
            print(
                f"  skip {symbol}: listed {first_open:%Y-%m-%d}, "
                f"history < {MIN_HISTORY_DAYS} days"
            )
            continue

        avg_volume, min_close, max_close = get_daily_stats(client, symbol)
        if PEG_LOW <= min_close and max_close <= PEG_HIGH:
            print(
                f"  skip {symbol}: USD-pegged, "
                f"{VOLUME_WINDOW_DAYS}d close range {min_close}-{max_close}"
            )
            continue
        if avg_volume < MIN_AVG_QUOTE_VOLUME_USDT:
            print(
                f"  skip {symbol}: {VOLUME_WINDOW_DAYS}d avg volume "
                f"{avg_volume:,.0f} < {MIN_AVG_QUOTE_VOLUME_USDT:,}"
            )
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
        raise RuntimeError(
            f"Only {len(selected)} symbols passed filters, expected >= {MIN_SELECTED}"
        )
    return selected


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    with BinanceClient() as client:
        rows = select_symbols(client, now=datetime.now(timezone.utc))
    write_csv(rows, OUTPUT_PATH)
    print(f"\nSaved {len(rows)} symbols to {OUTPUT_PATH}:")
    for r in rows:
        print(
            f"  {r['symbol']:<12} {r['avg_quote_volume_30d_usdt']:>16,}  "
            f"since {r['first_candle_date']}"
        )


if __name__ == "__main__":
    main()
