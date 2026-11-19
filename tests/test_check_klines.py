"""Tests for check_klines: prove the DQ checks actually catch duplicates and gaps."""

from __future__ import annotations

import pytest

from ingestion.extract_klines import INTERVAL_MS, Kline, check_klines

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


def test_clean_grid_has_no_issues() -> None:
    klines = make_klines(24)

    stats = check_klines(klines)

    assert stats == {"rows": 24, "duplicates": 0, "gaps": 0}


def test_duplicate_open_time_is_detected() -> None:
    klines = make_klines(24)
    klines.append(make_kline(klines[5][0]))  # same open_time as the 6th candle

    stats = check_klines(klines)

    assert stats == {"rows": 25, "duplicates": 1, "gaps": 0}


@pytest.mark.parametrize("missing", [1, 5])
def test_hole_in_grid_is_detected(missing: int) -> None:
    klines = make_klines(24)
    del klines[10 : 10 + missing]

    stats = check_klines(klines)

    assert stats["rows"] == 24 - missing
    assert stats["gaps"] == 1  # one hole = one gap, regardless of its length
    assert stats["duplicates"] == 0


def test_unsorted_input_is_not_a_gap() -> None:
    klines = make_klines(24)
    klines.reverse()

    stats = check_klines(klines)

    assert stats == {"rows": 24, "duplicates": 0, "gaps": 0}


def test_empty_input() -> None:
    assert check_klines([]) == {"rows": 0, "duplicates": 0, "gaps": 0}
