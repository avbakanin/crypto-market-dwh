"""Tests for the landing-zone contract: batch ids, manifests, complete batches, watermarks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.batches import (
    MANIFEST_NAME,
    Manifest,
    batch_id_for,
    complete_batches,
    high_water_marks,
    is_batch_id,
    latest_complete_batch,
    read_manifest,
    write_manifest,
)
from ingestion.extract_klines import INTERVAL_MS
from tests.factories import START_MS, make_klines, make_marked_batch


def make_manifest(batch_id: str) -> Manifest:
    return {
        "batch_id": batch_id,
        "as_of_utc": "2025-01-01T00:00:00+00:00",
        "interval": "1h",
        "symbols": {"BTCUSDT": {"rows": 24, "duplicates": 0, "gaps": 0}},
    }


def make_batch(root: Path, batch_id: str, complete: bool = True) -> Path:
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True)
    if complete:
        write_manifest(batch_dir, make_manifest(batch_id))
    return batch_dir


def test_batch_id_is_utc_as_of_moment() -> None:
    assert batch_id_for(START_MS) == "20250101T000000Z"


def test_batch_id_truncates_milliseconds_instead_of_rounding() -> None:
    assert batch_id_for(START_MS + 999) == "20250101T000000Z"


def test_batch_ids_sort_chronologically_as_strings() -> None:
    earlier = batch_id_for(START_MS + 9 * 1000)  # ...T000009Z
    later = batch_id_for(START_MS + 10 * 1000)  # ...T000010Z
    assert earlier < later


@pytest.mark.parametrize(
    "value",
    ["20250101T000000z", "20250101T000000", "latest", "../20250101T000000Z", "20250101T000000Z/x"],
)
def test_is_batch_id_rejects_anything_but_the_exact_format(value: str) -> None:
    assert not is_batch_id(value)


def test_manifest_round_trip(tmp_path: Path) -> None:
    batch_dir = make_batch(tmp_path, "20250101T000000Z")
    assert read_manifest(batch_dir) == make_manifest("20250101T000000Z")


def test_manifest_of_another_batch_is_refused(tmp_path: Path) -> None:
    batch_dir = tmp_path / "20250102T000000Z"
    batch_dir.mkdir()
    write_manifest(batch_dir, make_manifest("20250101T000000Z"))  # copied from another batch
    with pytest.raises(ValueError, match="describes batch"):
        read_manifest(batch_dir)


def test_latest_complete_batch_is_the_newest_with_a_manifest(tmp_path: Path) -> None:
    make_batch(tmp_path, "20250101T000000Z")
    expected = make_batch(tmp_path, "20250102T000000Z")
    make_batch(tmp_path, "20250103T000000Z", complete=False)  # extract crashed mid-batch
    backup = tmp_path / "backup_20250101"  # a manual copy, manifest included; sorts last
    backup.mkdir()
    write_manifest(backup, make_manifest("20250101T000000Z"))

    assert latest_complete_batch(tmp_path) == expected


def test_no_complete_batch_raises(tmp_path: Path) -> None:
    make_batch(tmp_path, "20250101T000000Z", complete=False)
    with pytest.raises(FileNotFoundError):
        latest_complete_batch(tmp_path)


def test_missing_root_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        latest_complete_batch(tmp_path / "klines_1h")


def test_complete_batches_are_listed_oldest_first(tmp_path: Path) -> None:
    newer = make_batch(tmp_path, "20250102T000000Z")
    older = make_batch(tmp_path, "20250101T000000Z")
    make_batch(tmp_path, "20250103T000000Z", complete=False)

    assert complete_batches(tmp_path) == [older, newer]


def test_missing_root_has_no_complete_batches(tmp_path: Path) -> None:
    assert complete_batches(tmp_path / "klines_1h") == []


# --- high-water marks (extract watermark) ---------------------------------------------------


def hour(n: int) -> int:
    return START_MS + n * INTERVAL_MS


def test_mark_is_the_newest_open_time_per_symbol_over_all_batches(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": hour(10), "ETHUSDT": hour(10)})
    make_marked_batch(tmp_path, "20250102T000000Z", {"BTCUSDT": hour(34)})  # ETH not extracted

    assert high_water_marks(tmp_path) == {"BTCUSDT": hour(34), "ETHUSDT": hour(10)}


def test_mark_never_moves_back_whatever_the_batch_order(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": hour(34)})
    make_marked_batch(tmp_path, "20250102T000000Z", {"BTCUSDT": hour(10)})

    assert high_water_marks(tmp_path) == {"BTCUSDT": hour(34)}


def test_incomplete_batch_does_not_move_the_mark(tmp_path: Path) -> None:
    make_marked_batch(tmp_path, "20250101T000000Z", {"BTCUSDT": hour(10)})
    crashed = make_marked_batch(tmp_path, "20250102T000000Z", {"BTCUSDT": hour(34)})
    (crashed / MANIFEST_NAME).unlink()  # extract crashed before writing the manifest

    assert high_water_marks(tmp_path) == {"BTCUSDT": hour(10)}


def test_no_batches_means_no_marks(tmp_path: Path) -> None:
    assert high_water_marks(tmp_path / "klines_1h") == {}


def test_manifest_without_last_open_falls_back_to_the_file(tmp_path: Path) -> None:
    # Manifests written before Week 2 have only check_klines stats; the batch is never rewritten.
    batch_dir = make_batch(tmp_path, "20250101T000000Z")  # BTCUSDT: rows 24, no last_open_ms
    (batch_dir / "BTCUSDT.json").write_text(json.dumps(make_klines(24)), encoding="utf-8")

    assert high_water_marks(tmp_path) == {"BTCUSDT": hour(23)}


def test_empty_file_without_last_open_gives_no_mark(tmp_path: Path) -> None:
    batch_dir = make_batch(tmp_path, "20250101T000000Z")
    (batch_dir / "BTCUSDT.json").write_text("[]", encoding="utf-8")

    assert high_water_marks(tmp_path) == {}
