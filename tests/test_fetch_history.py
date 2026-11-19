"""Tests for fetch_history: pagination over a fake client, no network."""

from __future__ import annotations

import pytest

from ingestion import extract_klines
from ingestion.extract_klines import (
    DAY_MS,
    INTERVAL_MS,
    PAGE_LIMIT,
    check_klines,
    drop_unclosed,
    fetch_history,
)
from tests.factories import START_MS, make_klines
from tests.fakes import FakeKlinesSource

MINUTE_MS = 60 * 1000
SYMBOL = "BTCUSDT"


@pytest.fixture(autouse=True)
def no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip the politeness pause between pages: tests talk to a fake, not to Binance."""
    monkeypatch.setattr(extract_klines, "PAUSE_S", 0)


def test_pages_through_full_and_partial_pages() -> None:
    source = FakeKlinesSource(make_klines(2500))  # 1000 + 1000 + 500
    now_ms = START_MS + 2500 * INTERVAL_MS

    klines = fetch_history(source, SYMBOL, START_MS, now_ms)

    assert len(source.calls) == 3
    assert check_klines(klines) == {"rows": 2500, "duplicates": 0, "gaps": 0}


def test_next_page_starts_right_after_last_candle() -> None:
    source = FakeKlinesSource(make_klines(2500))
    now_ms = START_MS + 2500 * INTERVAL_MS

    fetch_history(source, SYMBOL, START_MS, now_ms)

    assert source.calls == [
        START_MS,
        START_MS + PAGE_LIMIT * INTERVAL_MS,
        START_MS + 2 * PAGE_LIMIT * INTERVAL_MS,
    ]


def test_stops_on_empty_response() -> None:
    source = FakeKlinesSource(make_klines(2000))  # exactly two full pages
    now_ms = START_MS + 3000 * INTERVAL_MS  # as-of moment is later than the last candle

    klines = fetch_history(source, SYMBOL, START_MS, now_ms)

    assert len(klines) == 2000
    assert len(source.calls) == 3  # the third request returns [] and ends the loop


def test_partial_page_ends_paging_without_extra_request() -> None:
    """Data ends before now_ms (e.g. a trading halt): a short page must stop paging by itself."""
    source = FakeKlinesSource(make_klines(1500))  # 1000 + 500
    now_ms = START_MS + 3000 * INTERVAL_MS

    fetch_history(source, SYMBOL, START_MS, now_ms)

    assert len(source.calls) == 2


def test_no_request_when_start_is_not_before_now() -> None:
    source = FakeKlinesSource(make_klines(10))

    assert fetch_history(source, SYMBOL, START_MS, now_ms=START_MS) == []
    assert source.calls == []


def test_full_year_with_unaligned_start_gives_8759_closed_candles() -> None:
    """DQ note #13: a 365-day window yields 8759 closed candles, not 8760.

    start_ms is 20 minutes past the hour, so the first candle returned is the next full hour;
    the candle of the current hour is still open and drop_unclosed removes it.
    """
    now_ms = START_MS + 365 * DAY_MS + 20 * MINUTE_MS
    start_ms = now_ms - 365 * DAY_MS
    source = FakeKlinesSource(make_klines(365 * 24 + 1))  # includes the current open candle

    klines = drop_unclosed(fetch_history(source, SYMBOL, start_ms, now_ms), now_ms)

    assert check_klines(klines) == {"rows": 8759, "duplicates": 0, "gaps": 0}
