"""Tests for check_klines: prove the DQ checks actually catch duplicates and gaps."""

from __future__ import annotations

import pytest

from ingestion.extract_klines import check_klines
from tests.factories import make_kline, make_klines


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
