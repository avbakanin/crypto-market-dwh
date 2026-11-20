"""Tests for extract_batch: incremental from the high-water mark, one immutable batch per run."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion import extract_klines
from ingestion.batches import MANIFEST_NAME, batch_id_for, latest_complete_batch, read_manifest
from ingestion.extract_klines import DAY_MS, HISTORY_DAYS, INTERVAL_MS, Kline, extract_batch
from tests.factories import START_MS, make_klines, make_marked_batch
from tests.fakes import FakeKlinesSource

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
NOW_MS = START_MS + 24 * INTERVAL_MS  # 2025-01-02 00:00 UTC, on the hour
WINDOW_START = NOW_MS - HISTORY_DAYS * DAY_MS
HISTORY = make_klines(HISTORY_DAYS * 24, start_ms=WINDOW_START)  # every closed candle in the window
EARLIER_BATCH = "20250101T200000Z"  # an earlier run's batch id, before NOW_MS


def hours_ago(n: int) -> int:
    """open_time of the candle opened n hours before NOW_MS; hours_ago(1) is the newest closed."""
    return NOW_MS - n * INTERVAL_MS


def opens(batch_dir: Path, symbol: str) -> list[int]:
    klines = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
    return [k[0] for k in klines]


class FailingSource(FakeKlinesSource):
    """Fails for one symbol, like a network error in the middle of a run."""

    def __init__(self, klines: list[Kline], fail_symbol: str) -> None:
        super().__init__(klines)
        self.fail_symbol = fail_symbol

    def klines(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[Kline]:
        if symbol == self.fail_symbol:
            raise RuntimeError("connection reset")
        return super().klines(symbol, interval, start_time, end_time, limit)


@pytest.fixture(autouse=True)
def no_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract_klines, "PAUSE_S", 0)


# --- first run: no marks, full history window -----------------------------------------------


def test_first_run_writes_one_file_per_symbol_and_a_manifest(tmp_path: Path) -> None:
    batch_dir = extract_batch(FakeKlinesSource(HISTORY), SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir is not None
    assert batch_dir == tmp_path / batch_id_for(NOW_MS)
    assert {p.name for p in batch_dir.iterdir()} == {"BTCUSDT.json", "ETHUSDT.json", MANIFEST_NAME}
    assert json.loads((batch_dir / "BTCUSDT.json").read_text(encoding="utf-8")) == HISTORY


def test_manifest_records_as_of_and_stats_of_each_file(tmp_path: Path) -> None:
    batch_dir = extract_batch(FakeKlinesSource(HISTORY), SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir is not None
    assert read_manifest(batch_dir) == {
        "batch_id": "20250102T000000Z",
        "as_of_utc": "2025-01-02T00:00:00+00:00",
        "interval": "1h",
        "symbols": {
            s: {
                "rows": 8760,
                "duplicates": 0,
                "gaps": 0,
                "expected": 8760,  # aligned as-of: the window holds exactly 365 * 24 candles
                "last_open_ms": hours_ago(1),
            }
            for s in SYMBOLS
        },
    }


# --- incremental runs -----------------------------------------------------------------------


def test_increment_starts_at_the_last_extracted_candle(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {s: hours_ago(5) for s in SYMBOLS})
    source = FakeKlinesSource(HISTORY)

    batch_dir = extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.calls == [hours_ago(5), hours_ago(5)]  # one request per symbol
    assert opens(batch_dir, "BTCUSDT") == [hours_ago(n) for n in (5, 4, 3, 2, 1)]  # overlap + 4
    assert read_manifest(batch_dir)["symbols"]["BTCUSDT"] == {
        "rows": 5,
        "duplicates": 0,
        "gaps": 0,
        "expected": 5,
        "last_open_ms": hours_ago(1),
    }


def test_second_run_continues_from_where_the_first_ended(tmp_path: Path) -> None:
    source = FakeKlinesSource(HISTORY)
    extract_batch(source, ["BTCUSDT"], hours_ago(3), tmp_path)  # newest closed: hours_ago(4)
    source.calls.clear()

    batch_dir = extract_batch(source, ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.calls == [hours_ago(4)]
    assert opens(batch_dir, "BTCUSDT") == [hours_ago(n) for n in (4, 3, 2, 1)]


def test_up_to_date_symbol_costs_no_request(tmp_path: Path) -> None:
    marks = {"BTCUSDT": hours_ago(1), "ETHUSDT": hours_ago(2)}  # ETH has exactly one new candle
    make_marked_batch(tmp_path, EARLIER_BATCH, marks)
    source = FakeKlinesSource(HISTORY)

    batch_dir = extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.calls == [hours_ago(2)]  # nothing asked for BTCUSDT
    assert {p.name for p in batch_dir.iterdir()} == {"ETHUSDT.json", MANIFEST_NAME}
    assert opens(batch_dir, "ETHUSDT") == [hours_ago(2), hours_ago(1)]


def test_nothing_new_writes_no_batch_and_makes_no_request(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {s: hours_ago(1) for s in SYMBOLS})
    source = FakeKlinesSource(HISTORY)

    assert extract_batch(source, SYMBOLS, NOW_MS, tmp_path) is None

    assert source.calls == []
    assert [p.name for p in tmp_path.iterdir()] == [EARLIER_BATCH]


def test_symbol_without_new_candles_is_left_out_and_reported(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {s: hours_ago(5) for s in SYMBOLS})
    halted = [k for k in HISTORY if k[0] <= hours_ago(5)]  # trading stopped after the mark
    source = FakeKlinesSource(halted)

    assert extract_batch(source, SYMBOLS, NOW_MS, tmp_path) is None

    assert source.calls == [hours_ago(5), hours_ago(5)]  # asked, got only the overlap candle
    assert [p.name for p in tmp_path.iterdir()] == [EARLIER_BATCH]
    assert "no new candles, expected 4" in capsys.readouterr().out


def test_new_symbol_gets_the_full_history_window(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": hours_ago(5)})  # ETHUSDT is new
    source = FakeKlinesSource(HISTORY)

    batch_dir = extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir is not None
    assert source.calls[0] == hours_ago(5)
    assert source.calls[1] == WINDOW_START  # then paginates through the year
    assert read_manifest(batch_dir)["symbols"]["ETHUSDT"]["rows"] == 8760


def test_missing_newest_candle_is_a_dq_issue_against_expected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    make_marked_batch(tmp_path, EARLIER_BATCH, {"BTCUSDT": hours_ago(5)})
    lagging = [k for k in HISTORY if k[0] != hours_ago(1)]  # source has not published it yet

    batch_dir = extract_batch(FakeKlinesSource(lagging), ["BTCUSDT"], NOW_MS, tmp_path)

    assert batch_dir is not None
    stats = read_manifest(batch_dir)["symbols"]["BTCUSDT"]
    assert (stats["rows"], stats["gaps"], stats["expected"]) == (4, 0, 5)  # no gap inside
    assert "with DQ issues: 1 ['BTCUSDT']" in capsys.readouterr().out


# --- immutability and completeness ----------------------------------------------------------


def test_existing_batch_is_never_overwritten(tmp_path: Path) -> None:
    batch_dir = tmp_path / batch_id_for(NOW_MS)
    batch_dir.mkdir()
    (batch_dir / "BTCUSDT.json").write_text("[]", encoding="utf-8")
    source = FakeKlinesSource(HISTORY)

    with pytest.raises(FileExistsError):
        extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    assert source.calls == []  # refused before any API request
    assert (batch_dir / "BTCUSDT.json").read_text(encoding="utf-8") == "[]"


def test_failed_run_leaves_no_manifest_so_the_batch_is_not_loadable(tmp_path: Path) -> None:
    source = FailingSource(HISTORY, fail_symbol="ETHUSDT")

    with pytest.raises(RuntimeError):
        extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    batch_dir = tmp_path / batch_id_for(NOW_MS)
    assert (batch_dir / "BTCUSDT.json").is_file()  # partial data is on disk...
    assert not (batch_dir / MANIFEST_NAME).exists()  # ...but the batch is not marked complete
    with pytest.raises(FileNotFoundError):
        latest_complete_batch(tmp_path)
