"""Map whitelisted spot symbols to their Binance USD-M perpetual contracts.

Output: config/perpetuals.csv — which contract carries each coin's funding rate and open
interest. Rebuild it after config/symbols.csv changes, deliberately, and review the diff.
Run from repo root: python -m ingestion.map_perpetuals

A contract is matched on its baseAsset, never on a substring of its symbol: BTC must not
match BTCDOMUSDT (a dominance index) nor PUMP match PUMPBTCUSDT. Low-priced coins are quoted
per 1000 or per million coins (PEPE -> 1000PEPEUSDT): the multiplier is kept in the file.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.clients.binance import BinanceFuturesClient
from ingestion.select_symbols import write_csv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS_PATH = PROJECT_ROOT / "config" / "symbols.csv"
OUTPUT_PATH = PROJECT_ROOT / "config" / "perpetuals.csv"

QUOTE_ASSET = "USDT"
# baseAsset prefix of a contract quoted per N coins -> N
MULTIPLIER_PREFIXES = {"": 1, "1000": 1_000, "1000000": 1_000_000, "1M": 1_000_000}


def load_spot_symbols(path: Path = SYMBOLS_PATH) -> list[tuple[str, str]]:
    """(symbol, base_asset) of every whitelisted spot pair, in whitelist order."""
    with path.open(encoding="utf-8", newline="") as f:
        return [(row["symbol"], row["base_asset"]) for row in csv.DictReader(f)]


def is_tradable_perpetual(contract: dict[str, Any]) -> bool:
    """A live USDT perpetual: not a quarterly, not a TRADIFI_PERPETUAL (tokenized stock),
    not settling (being delisted)."""
    return (
        contract["contractType"] == "PERPETUAL"
        and contract["status"] == "TRADING"
        and contract["quoteAsset"] == QUOTE_ASSET
    )


def perpetuals_for(base: str, contracts: list[dict[str, Any]]) -> list[tuple[dict[str, Any], int]]:
    """Contracts on `base` itself or on N units of it, with N."""
    return [
        (contract, multiplier)
        for contract in contracts
        for prefix, multiplier in MULTIPLIER_PREFIXES.items()
        if contract["baseAsset"] == prefix + base
    ]


def map_perpetuals(
    spot: list[tuple[str, str]], exchange_info: dict[str, Any], now: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    """Rows of perpetuals.csv for the spot symbols that have a contract, and those that do not.

    Two contracts for one coin is ambiguous: raise, a human maps it. No contract at all is
    a coin without funding and open interest data: returned so the caller can warn.
    """
    if now.utcoffset() is None:
        raise ValueError("now must be timezone-aware (UTC)")
    now = now.astimezone(UTC)

    contracts = [c for c in exchange_info["symbols"] if is_tradable_perpetual(c)]
    rows: list[dict[str, Any]] = []
    unmapped: list[str] = []
    for symbol, base in spot:
        found = perpetuals_for(base, contracts)
        if not found:
            unmapped.append(symbol)
            continue
        if len(found) > 1:
            names = sorted(contract["symbol"] for contract, _ in found)
            raise ValueError(f"{symbol}: several perpetuals {names}, map it by hand")
        contract, multiplier = found[0]
        rows.append(
            {
                "spot_symbol": symbol,
                "futures_symbol": contract["symbol"],
                "contract_multiplier": multiplier,
                "mapped_at_utc": now.isoformat(timespec="seconds"),
            }
        )
    if not rows:
        raise RuntimeError("No whitelisted symbol has a USD-M perpetual: wrong exchangeInfo?")
    return rows, unmapped


def main() -> None:
    spot = load_spot_symbols()
    with BinanceFuturesClient() as client:
        rows, unmapped = map_perpetuals(spot, client.exchange_info(), now=datetime.now(UTC))
    write_csv(rows, OUTPUT_PATH)
    print(f"Saved {len(rows)} of {len(spot)} symbols to {OUTPUT_PATH}:")
    for r in rows:
        note = f"  (x{r['contract_multiplier']})" if r["contract_multiplier"] != 1 else ""
        print(f"  {r['spot_symbol']:<12} -> {r['futures_symbol']}{note}")
    if unmapped:
        print(f"WARNING: no tradable USD-M perpetual, no funding/OI data: {unmapped}")


if __name__ == "__main__":
    main()
