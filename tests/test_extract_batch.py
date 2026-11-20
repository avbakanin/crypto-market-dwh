"""Tests for extract_batch: one immutable batch directory per run, manifest written last."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion import extract_klines
from ingestion.batches import MANIFEST_NAME, batch_id_for, latest_complete_batch, read_manifest
from ingestion.extract_klines import INTERVAL_MS, Kline, extract_batch
from tests.factories import START_MS, make_klines
from tests.fakes import FakeKlinesSource

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
NOW_MS = START_MS + 24 * INTERVAL_MS  # 24 closed candles from START_MS (history start is earlier)


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


def test_writes_one_file_per_symbol_and_a_manifest(tmp_path: Path) -> None:
    batch_dir = extract_batch(FakeKlinesSource(make_klines(24)), SYMBOLS, NOW_MS, tmp_path)

    assert batch_dir == tmp_path / batch_id_for(NOW_MS)
    assert {p.name for p in batch_dir.iterdir()} == {"BTCUSDT.json", "ETHUSDT.json", MANIFEST_NAME}
    assert json.loads((batch_dir / "BTCUSDT.json").read_text(encoding="utf-8")) == make_klines(24)


def test_manifest_records_as_of_and_stats_of_each_file(tmp_path: Path) -> None:
    batch_dir = extract_batch(FakeKlinesSource(make_klines(24)), SYMBOLS, NOW_MS, tmp_path)

    assert read_manifest(batch_dir) == {
        "batch_id": "20250102T000000Z",
        "as_of_utc": "2025-01-02T00:00:00+00:00",
        "interval": "1h",
        "symbols": {
            s: {"rows": 24, "duplicates": 0, "gaps": 0, "last_open_ms": START_MS + 23 * INTERVAL_MS}
            for s in SYMBOLS
        },
    }


def test_existing_batch_is_never_overwritten(tmp_path: Path) -> None:
    batch_dir = tmp_path / batch_id_for(NOW_MS)
    batch_dir.mkdir()
    (batch_dir / "BTCUSDT.json").write_text("[]", encoding="utf-8")
    source = FakeKlinesSource(make_klines(24))

    with pytest.raises(FileExistsError):
        extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    assert source.calls == []  # refused before any API request
    assert (batch_dir / "BTCUSDT.json").read_text(encoding="utf-8") == "[]"


def test_failed_run_leaves_no_manifest_so_the_batch_is_not_loadable(tmp_path: Path) -> None:
    source = FailingSource(make_klines(24), fail_symbol="ETHUSDT")

    with pytest.raises(RuntimeError):
        extract_batch(source, SYMBOLS, NOW_MS, tmp_path)

    batch_dir = tmp_path / batch_id_for(NOW_MS)
    assert (batch_dir / "BTCUSDT.json").is_file()  # partial data is on disk...
    assert not (batch_dir / MANIFEST_NAME).exists()  # ...but the batch is not marked complete
    with pytest.raises(FileNotFoundError):
        latest_complete_batch(tmp_path)
