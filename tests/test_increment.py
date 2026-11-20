"""Tests for the increment arithmetic: newest closed candle, where to start, how many to expect."""

from __future__ import annotations

import pytest

from ingestion import extract_klines
from ingestion.extract_klines import (
    DAY_MS,
    HISTORY_DAYS,
    INTERVAL_MS,
    drop_unclosed,
    expected_candles,
    last_closed_open_ms,
    start_ms_for,
)
from tests.factories import START_MS, make_klines

NOW_MS = START_MS + 24 * INTERVAL_MS  # 2025-01-02 00:00 UTC
WINDOW_START = NOW_MS - HISTORY_DAYS * DAY_MS


# --- newest closed candle (same rule as drop_unclosed) --------------------------------------


def test_candle_counts_as_closed_once_its_close_time_has_passed() -> None:
    open_ms = START_MS
    assert last_closed_open_ms(open_ms + INTERVAL_MS) == open_ms  # now = close_time + 1


def test_candle_is_not_closed_at_its_last_millisecond() -> None:
    open_ms = START_MS
    assert last_closed_open_ms(open_ms + INTERVAL_MS - 1) == open_ms - INTERVAL_MS


def test_agrees_with_drop_unclosed_mid_hour() -> None:
    now_ms = START_MS + 5 * INTERVAL_MS + 20 * 60 * 1000  # 05:20
    kept = drop_unclosed(make_klines(6), now_ms)  # 00:00 .. 05:00, 05:00 still open
    assert last_closed_open_ms(now_ms) == kept[-1][0] == START_MS + 4 * INTERVAL_MS


# --- where to start -------------------------------------------------------------------------


def test_new_symbol_starts_at_the_history_window() -> None:
    assert start_ms_for(None, NOW_MS) == WINDOW_START


def test_known_symbol_rereads_its_newest_candle() -> None:
    mark = NOW_MS - 5 * INTERVAL_MS
    assert start_ms_for(mark, NOW_MS) == mark  # OVERLAP_CANDLES = 1


def test_overlap_rereads_that_many_newest_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract_klines, "OVERLAP_CANDLES", 3)
    mark = NOW_MS - 5 * INTERVAL_MS
    assert start_ms_for(mark, NOW_MS) == mark - 2 * INTERVAL_MS  # mark and the two before it


def test_mark_older_than_the_window_restarts_at_the_window() -> None:
    assert start_ms_for(WINDOW_START - 10 * INTERVAL_MS, NOW_MS) == WINDOW_START


def test_mark_exactly_at_the_window_start_is_kept() -> None:
    assert start_ms_for(WINDOW_START, NOW_MS) == WINDOW_START


# --- how many closed candles to expect ------------------------------------------------------


def test_expected_counts_both_ends_of_an_aligned_range() -> None:
    assert expected_candles(NOW_MS - 5 * INTERVAL_MS, NOW_MS) == 5  # 19:00 .. 23:00


def test_expected_for_a_full_year_with_unaligned_start_is_8759() -> None:
    now_ms = NOW_MS + 20 * 60 * 1000  # 00:20: the window start is 20 minutes past the hour
    assert expected_candles(now_ms - HISTORY_DAYS * DAY_MS, now_ms) == 8759  # DQ note #13


def test_expected_is_one_when_starting_at_the_newest_closed_candle() -> None:
    assert expected_candles(NOW_MS - INTERVAL_MS, NOW_MS) == 1


def test_expected_is_zero_when_starting_after_the_newest_closed_candle() -> None:
    assert expected_candles(NOW_MS, NOW_MS) == 0
    assert expected_candles(NOW_MS + 5 * INTERVAL_MS, NOW_MS) == 0  # never negative
