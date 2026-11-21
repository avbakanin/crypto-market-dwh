"""Extract hourly open interest of the whitelisted perpetuals into a new batch directory.

Run from repo root: python -m ingestion.extract_open_interest
Contracts come from config/perpetuals.csv (see map_perpetuals). Incremental like klines:
each contract starts from its high-water mark, re-reading that point; a contract with no
mark gets the WINDOW_DAYS the API still serves. Writes
data/raw/open_interest_1h/<batch_id>/<CONTRACT>.json and, last, _manifest.json (batches.py).

Source traps (verified live): only about the latest month is served, and an older startTime
or endTime is HTTP 400, so history not extracted in time is lost for good (RAW is the only
copy); within [startTime, endTime] the API returns the LAST `limit` points, unlike klines and
funding, so pages are explicit windows no wider than `limit`; point T is published about a
minute after T and does not change afterwards.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol

from ingestion.batches import BatchWriter, high_water_marks
from ingestion.clients.binance import BinanceFuturesClient
from ingestion.extract_funding import load_contracts
from ingestion.extract_klines import DAY_MS, HOUR_MS

OpenInterest = dict[str, Any]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PERIOD = "1h"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / f"open_interest_{PERIOD}"

WINDOW_DAYS = 30  # served: about 31 days back; the margin keeps startTime clear of HTTP 400
PAGE_LIMIT = 500
# Point T is expected only this long after T: published 61-76 s after the hour (measured).
PUBLISH_LAG_MS = 5 * 60 * 1000
PAUSE_S = 0.2  # politeness pause between pages


class OpenInterestSource(Protocol):
    """What extract needs from the futures API: the real client or a test fake."""

    def open_interest_hist(
        self,
        symbol: str,
        period: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]: ...


def last_ready_ms(now_ms: int) -> int:
    """Newest hourly point that should be published by now."""
    return (now_ms - PUBLISH_LAG_MS) // HOUR_MS * HOUR_MS


def window_start_ms(now_ms: int) -> int:
    """Oldest point to ask for: the first full hour inside the WINDOW_DAYS window."""
    return -(-(now_ms - WINDOW_DAYS * DAY_MS) // HOUR_MS) * HOUR_MS


def start_ms_for(mark: int | None, now_ms: int) -> int:
    """First point to request: the window for a new contract, else the mark itself (re-read:
    proves the seam); never earlier than the window, which the API would refuse."""
    window_start = window_start_ms(now_ms)
    return window_start if mark is None else max(window_start, mark)


def lost_points(mark: int | None, now_ms: int) -> int:
    """Points after the mark that fell out of the served window before this run: gone."""
    if mark is None:
        return 0
    return max(0, (window_start_ms(now_ms) - mark) // HOUR_MS - 1)


def expected_points(start_ms: int, now_ms: int) -> int:
    """Hourly points from start_ms (on the hour) to the newest published one."""
    return max(0, (last_ready_ms(now_ms) - start_ms) // HOUR_MS + 1)


def fetch_open_interest(
    client: OpenInterestSource, symbol: str, start_ms: int, last_ms: int
) -> list[OpenInterest]:
    """Page forward through windows of PAGE_LIMIT hours with an explicit endTime: a window no
    wider than `limit` comes back whole, although the API returns the last `limit` points."""
    points: list[OpenInterest] = []
    while start_ms <= last_ms:
        end_ms = min(start_ms + (PAGE_LIMIT - 1) * HOUR_MS, last_ms)
        points.extend(
            client.open_interest_hist(
                symbol, PERIOD, start_time=start_ms, end_time=end_ms, limit=PAGE_LIMIT
            )
        )
        start_ms = end_ms + HOUR_MS  # an empty window (no trades) does not end the paging
        if start_ms <= last_ms:
            time.sleep(PAUSE_S)
    return points


def check_open_interest(points: list[OpenInterest]) -> dict[str, int]:
    """DQ checks: row count, duplicate timestamps, steps other than one hour."""
    stamps = [p["timestamp"] for p in points]
    unique = sorted(set(stamps))
    return {
        "rows": len(points),
        "duplicates": len(stamps) - len(unique),
        "gaps": sum(1 for prev, curr in pairwise(unique) if curr - prev != HOUR_MS),
    }


def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def extract_batch(
    client: OpenInterestSource, contracts: list[str], now_ms: int, output_dir: Path = OUTPUT_DIR
) -> Path | None:
    """Extract points new since each contract's mark into a new batch directory.

    Returns the batch directory, or None when no contract has a new point.
    """
    writer = BatchWriter(output_dir, now_ms, PERIOD)  # refuses an existing batch up front
    marks = high_water_marks(output_dir)
    newest = last_ready_ms(now_ms)

    no_new: list[str] = []
    for i, symbol in enumerate(contracts, start=1):
        prefix = f"[{i:>2}/{len(contracts)}] {symbol:<13}"
        mark = marks.get(symbol)
        if mark is not None and mark >= newest:
            print(f"{prefix} up to date")  # no point published since: no request
            continue
        lost = lost_points(mark, now_ms)
        if lost:
            print(
                f"{prefix} WARNING: {lost} points after {ms_to_utc(mark or 0):%Y-%m-%d %H:%M} "
                "are no longer served: history lost"
            )
        start_ms = start_ms_for(mark, now_ms)
        points = fetch_open_interest(client, symbol, start_ms, newest)
        new_from = start_ms if mark is None else max(start_ms, mark + HOUR_MS)
        if not any(p["timestamp"] >= new_from for p in points):
            # No trades, contract halted or delisted? It stays out of the batch.
            no_new.append(symbol)
            expected_new = expected_points(new_from, now_ms)
            print(f"{prefix} WARNING: no new points, expected {expected_new}")
            continue

        stats = check_open_interest(points)
        stats["expected"] = expected_points(start_ms, now_ms)
        stats["lost"] = lost
        stats["last_ms"] = max(p["timestamp"] for p in points)  # the next run's mark
        writer.add(symbol, points, stats)
        print(
            f"{prefix} from {ms_to_utc(start_ms):%Y-%m-%d %H:%M} rows={stats['rows']:>4} "
            f"expected={stats['expected']:>4} dup={stats['duplicates']} gaps={stats['gaps']}"
        )

    batch_dir = writer.finish()  # the manifest, last: a batch without one is incomplete
    if batch_dir is None:
        print(f"\nNothing new as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC: no batch written")
        return None

    with_issues = [
        s
        for s, st in writer.symbols.items()
        if st["duplicates"]
        or st["gaps"]
        or st["lost"]
        or st["rows"] - st["duplicates"] != st["expected"]
    ]
    print(
        f"\nDone: {len(writer.symbols)} contracts, with DQ issues: {len(with_issues)} "
        f"{with_issues or ''}"
    )
    if no_new:
        print(f"No new points (left out of the batch): {no_new}")
    print(f"Batch: {batch_dir}")
    return batch_dir


def main() -> None:
    contracts = load_contracts()
    now_ms = int(time.time() * 1000)  # single as-of moment for the whole run
    print(
        f"Extracting {PERIOD} open interest of {len(contracts)} contracts "
        f"as of {ms_to_utc(now_ms):%Y-%m-%d %H:%M} UTC"
    )
    with BinanceFuturesClient() as client:
        extract_batch(client, contracts, now_ms)


if __name__ == "__main__":
    main()
