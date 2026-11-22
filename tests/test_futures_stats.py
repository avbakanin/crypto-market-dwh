"""Tests for futures_stats (open interest as the series): served window, last-`limit` paging,
increment, batches."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion import futures_stats as fs
from ingestion.batches import read_manifest
from ingestion.extract_klines import DAY_MS, HOUR_MS
from ingestion.extract_open_interest import OPEN_INTEREST as OI
from tests.factories import START_MS, make_marked_batch, make_open_interests
from tests.fakes import FakeFutures

LAG = fs.PUBLISH_LAG_MS
NOW_MS = START_MS + 40 * DAY_MS + 30 * 60 * 1000  # 00:30: the 00:00 point is published
LAST = START_MS + 40 * DAY_MS
WINDOW_START = START_MS + 10 * DAY_MS + HOUR_MS  # first full hour inside 30 days
SERVED_FROM = NOW_MS - 31 * DAY_MS  # older start/end times are HTTP 400, as live
EARLIER_BATCH = "20250101T000000Z"
BTC = make_open_interests("BTCUSDT", START_MS, 40 * 24 + 1)  # every point through LAST


def fake(points: list[dict] | None = None) -> FakeFutures:
    return FakeFutures(
        open_interest={"BTCUSDT": BTC if points is None else points}, served_from=SERVED_FROM
    )


def at(hours: int) -> int:
    return START_MS + hours * HOUR_MS


@pytest.fixture(autouse=True)
def no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fs, "PAUSE_S", 0)


def stamps(batch_dir: Path, symbol: str) -> list[int]:
    points = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
    return [p["timestamp"] for p in points]


# --- window arithmetic ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("now_ms", "ready"),
    [(at(5) + LAG, at(5)), (at(5) + LAG - 1, at(4))],
    ids=["lag-passed", "still-in-lag"],
)
def test_point_counts_once_its_publication_lag_has_passed(now_ms: int, ready: int) -> None:
    assert fs.last_ready_ms(now_ms) == ready


def test_window_starts_at_the_first_full_hour_inside_30_days() -> None:
    assert fs.window_start_ms(NOW_MS) == WINDOW_START
    assert fs.window_start_ms(LAST) == START_MS + 10 * DAY_MS  # on the hour: that hour


def test_start_is_the_mark_itself_or_the_window() -> None:
    assert fs.start_ms_for(None, NOW_MS) == WINDOW_START
    assert fs.start_ms_for(LAST - HOUR_MS, NOW_MS) == LAST - HOUR_MS
    assert fs.start_ms_for(START_MS, NOW_MS) == WINDOW_START  # older than the API serves


@pytest.mark.parametrize(
    ("mark", "lost"),
    [(None, 0), (LAST - HOUR_MS, 0), (WINDOW_START - HOUR_MS, 0), (WINDOW_START - 3 * HOUR_MS, 2)],
    ids=["new-contract", "recent-mark", "next-point-is-the-window-start", "two-points-gone"],
)
def test_lost_points_lie_between_the_mark_and_the_window(mark: int | None, lost: int) -> None:
    assert fs.lost_points(mark, NOW_MS) == lost


def test_full_window_expects_one_point_per_hour() -> None:
    assert fs.expected_points(WINDOW_START, NOW_MS) == 30 * 24


# --- fetch_points -----------------------------------------------------------------------------


def test_pages_are_explicit_windows_no_wider_than_the_limit() -> None:
    source = fake()

    got = fs.fetch_points(source, OI, "BTCUSDT", WINDOW_START, LAST)

    assert [p["timestamp"] for p in got] == list(range(WINDOW_START, LAST + 1, HOUR_MS))
    assert source.stats_calls == [
        ("openInterestHist", "BTCUSDT", WINDOW_START, WINDOW_START + 499 * HOUR_MS),
        ("openInterestHist", "BTCUSDT", WINDOW_START + 500 * HOUR_MS, LAST),
    ]


def test_empty_window_does_not_end_the_paging() -> None:
    later = [p for p in BTC if p["timestamp"] >= WINDOW_START + 500 * HOUR_MS]  # no trades before

    got = fs.fetch_points(fake(later), OI, "BTCUSDT", WINDOW_START, LAST)

    assert len(got) == 220


# --- check_open_interest ----------------------------------------------------------------------


def test_missing_hour_is_a_gap_and_a_repeated_one_a_duplicate() -> None:
    points = BTC[:10] + BTC[11:20] + [BTC[5]]
    assert fs.check_points(points) == {"rows": 20, "duplicates": 1, "gaps": 1}


def test_unsorted_points_are_not_gaps() -> None:
    assert fs.check_points(BTC[:6][::-1])["gaps"] == 0


# --- extract_batch: first run -----------------------------------------------------------------


def test_first_run_takes_the_whole_served_window(tmp_path: Path) -> None:
    source = fake()

    batch_dir = fs.extract_batch(source, OI, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    manifest = read_manifest(batch_dir)
    assert manifest["interval"] == "1h"
    assert manifest["symbols"] == {
        "BTCUSDT": {
            "rows": 720,
            "duplicates": 0,
            "gaps": 0,
            "expected": 720,
            "lost": 0,
            "last_ms": LAST,
        }
    }
    assert stamps(batch_dir, "BTCUSDT") == list(range(WINDOW_START, LAST + 1, HOUR_MS))


def test_hour_without_trades_is_a_dq_issue(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    no_trades = [p for p in BTC if p["timestamp"] != LAST - 100 * HOUR_MS]

    batch_dir = fs.extract_batch(fake(no_trades), OI, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["expected"], stats["gaps"]) == (719, 720, 1)
    assert "with DQ issues: 1 ['BTCUSDT']" in capsys.readouterr().out


def test_missing_newest_point_is_a_dq_issue_against_expected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # No neighbour after it, so no gap: only rows vs expected can see it.
    late = [p for p in BTC if p["timestamp"] < LAST]

    batch_dir = fs.extract_batch(fake(late), OI, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["expected"], stats["gaps"]) == (719, 720, 0)
    assert "with DQ issues: 1 ['BTCUSDT']" in capsys.readouterr().out


def test_existing_batch_is_refused_before_any_request(tmp_path: Path) -> None:
    (tmp_path / "20250210T003000Z").mkdir()  # batch id of NOW_MS
    source = fake()
    with pytest.raises(FileExistsError):
        fs.extract_batch(source, OI, ["BTCUSDT"], NOW_MS, tmp_path)
    assert source.stats_calls == []


# --- extract_batch: increments ----------------------------------------------------------------


def test_increment_starts_at_the_mark_and_re_reads_it(tmp_path: Path) -> None:
    mark = LAST - 3 * HOUR_MS
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": mark}, key="last_ms")
    source = fake()

    batch_dir = fs.extract_batch(source, OI, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.stats_calls == [("openInterestHist", "BTCUSDT", mark, LAST)]
    assert stamps(batch_dir, "BTCUSDT") == [mark, mark + HOUR_MS, mark + 2 * HOUR_MS, LAST]
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["expected"], stats["lost"]) == (4, 4, 0)


def test_up_to_date_contract_costs_no_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": LAST}, key="last_ms")
    source = fake()

    assert fs.extract_batch(source, OI, ["BTCUSDT"], NOW_MS, tmp_path) is None
    assert source.stats_calls == []
    assert "Nothing new" in capsys.readouterr().out


def test_point_within_its_publication_lag_is_not_requested(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": LAST - HOUR_MS}, key="last_ms")
    source = fake()

    assert fs.extract_batch(source, OI, ["BTCUSDT"], LAST + LAG - 1, tmp_path) is None
    assert source.stats_calls == []


def test_mark_older_than_the_served_window_reports_lost_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    mark = START_MS + 5 * DAY_MS  # the last run was 35 days ago
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": mark}, key="last_ms")
    source = fake()

    batch_dir = fs.extract_batch(source, OI, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.stats_calls[0][2] == WINDOW_START  # not the mark: that would be HTTP 400
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["lost"], stats["rows"], stats["expected"]) == (120, 720, 720)
    out = capsys.readouterr().out
    assert "WARNING: 120 points after 2025-01-06 00:00 are no longer served" in out
    assert "with DQ issues: 1 ['BTCUSDT']" in out


def test_contract_without_new_points_is_left_out_and_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    halted = [p for p in BTC if p["timestamp"] <= LAST - 3 * HOUR_MS]
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": LAST - 3 * HOUR_MS}, key="last_ms")

    assert fs.extract_batch(fake(halted), OI, ["BTCUSDT"], NOW_MS, tmp_path) is None
    assert "WARNING: no new points, expected 3" in capsys.readouterr().out
