"""Tests for drop_unclosed: keep only candles closed strictly before the run's as-of moment."""

from __future__ import annotations

from ingestion.extract_klines import INTERVAL_MS, drop_unclosed
from tests.factories import START_MS, make_klines

MINUTE_MS = 60 * 1000


def test_drops_candle_still_in_progress() -> None:
    klines = make_klines(3)  # 00:00, 01:00, 02:00
    now_ms = START_MS + 2 * INTERVAL_MS + 30 * MINUTE_MS  # 02:30 -> 02:00 candle still open

    kept = drop_unclosed(klines, now_ms)

    assert [k[0] for k in kept] == [START_MS, START_MS + INTERVAL_MS]


def test_keeps_all_when_every_candle_is_closed() -> None:
    """A full historical page has no open candle; a blind klines[:-1] would lose one here."""
    klines = make_klines(1000)
    now_ms = START_MS + 1000 * INTERVAL_MS + 5 * MINUTE_MS  # well after the page ends

    kept = drop_unclosed(klines, now_ms)

    assert kept == klines


def test_candle_is_kept_once_its_close_time_has_passed() -> None:
    klines = make_klines(1)
    close_ms = klines[0][6]

    assert drop_unclosed(klines, now_ms=close_ms + 1) == klines


def test_candle_is_dropped_at_its_last_millisecond() -> None:
    klines = make_klines(1)
    close_ms = klines[0][6]

    assert drop_unclosed(klines, now_ms=close_ms) == []
