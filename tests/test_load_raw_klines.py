"""Tests for the RAW loader: statements it builds, how it checks results, idempotency guard.

No Snowflake: statements go to a scripted fake cursor. What Snowflake itself does with
them (PUT/COPY semantics) is verified on the live account, not here.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ingestion.batches import Manifest, write_manifest
from ingestion.load_raw_klines import (
    BATCH_STATS_SQL,
    LoadError,
    check_copy_results,
    check_local_files,
    check_put_results,
    copy_sql,
    load_batch,
    put_sql,
    reconcile,
)
from tests.factories import make_klines
from tests.fakes import FakeCursor

BATCH_ID = "20250102T000000Z"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
ROWS = 24


def make_manifest(rows: int = ROWS, duplicates: int = 0) -> Manifest:
    return {
        "batch_id": BATCH_ID,
        "as_of_utc": "2025-01-02T00:00:00+00:00",
        "interval": "1h",
        "symbols": {s: {"rows": rows, "duplicates": duplicates, "gaps": 0} for s in SYMBOLS},
    }


@pytest.fixture
def batch_dir(tmp_path: Path) -> Path:
    path = tmp_path / BATCH_ID
    path.mkdir()
    for symbol in SYMBOLS:
        (path / f"{symbol}.json").write_text(json.dumps(make_klines(ROWS)), encoding="utf-8")
    write_manifest(path, make_manifest())
    return path


def put_rows(status: str = "UPLOADED") -> list[dict[str, Any]]:
    return [
        {"source": f"{s}.json", "target": f"{s}.json.gz", "status": status, "message": ""}
        for s in SYMBOLS
    ]


def copy_rows(rows_loaded: int = ROWS, status: str = "LOADED") -> list[dict[str, Any]]:
    return [
        {
            "file": f"klines_stage/{BATCH_ID}/{s}.json.gz",
            "status": status,
            "rows_loaded": rows_loaded,
        }
        for s in SYMBOLS
    ]


def stats_rows(row_count: int = ROWS, duplicate_count: int = 0) -> list[dict[str, Any]]:
    # Upper-case keys, as Snowflake returns unquoted column aliases.
    return [
        {"SYMBOL": s, "ROW_COUNT": row_count, "DUPLICATE_COUNT": duplicate_count} for s in SYMBOLS
    ]


# --- statements -----------------------------------------------------------------------------


def test_put_uploads_the_batch_directory_and_never_overwrites(batch_dir: Path) -> None:
    assert put_sql(batch_dir) == (
        f"PUT 'file://{batch_dir.as_posix()}/*' @RAW.KLINES_STAGE/{BATCH_ID}/ "
        "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
    )


def test_put_refuses_a_directory_that_is_not_a_batch(tmp_path: Path) -> None:
    with pytest.raises(LoadError, match="Not a batch directory"):
        put_sql(tmp_path / "klines_1h")


def test_copy_reads_only_the_batch_directory_and_the_manifest_files() -> None:
    sql = copy_sql(BATCH_ID, ["ETHUSDT", "BTCUSDT"])

    assert f"FROM @RAW.KLINES_STAGE/{BATCH_ID}/\n" in sql
    assert "FILES = ('BTCUSDT.json.gz', 'ETHUSDT.json.gz')" in sql
    assert "ON_ERROR = ABORT_STATEMENT" in sql


@pytest.mark.parametrize("batch_id", ["latest", "20250102T000000Z/../", "x' OR '1'='1"])
def test_copy_refuses_an_invalid_batch_id(batch_id: str) -> None:
    with pytest.raises(LoadError, match="Invalid batch id"):
        copy_sql(batch_id, ["BTCUSDT"])


@pytest.mark.parametrize("symbol", ["btcusdt", "BTC.json.gz', 'X", "../BTCUSDT", ""])
def test_copy_refuses_a_symbol_outside_the_expected_alphabet(symbol: str) -> None:
    with pytest.raises(LoadError, match="Unexpected symbol name"):
        copy_sql(BATCH_ID, [symbol])


# --- local pre-flight -----------------------------------------------------------------------


def test_local_files_matching_the_manifest_pass(batch_dir: Path) -> None:
    check_local_files(batch_dir, make_manifest())


def test_file_changed_after_extract_is_refused(batch_dir: Path) -> None:
    (batch_dir / "ETHUSDT.json").write_text(json.dumps(make_klines(ROWS - 1)), encoding="utf-8")
    with pytest.raises(LoadError, match="ETHUSDT.json has 23 rows, manifest says 24"):
        check_local_files(batch_dir, make_manifest())


# --- PUT and COPY results -------------------------------------------------------------------


def test_put_results_uploaded_or_already_staged_pass() -> None:
    rows = put_rows()
    rows[1]["status"] = "SKIPPED"  # a re-run after a failed COPY: the file is already staged
    check_put_results(rows, make_manifest())


def test_put_result_with_an_error_status_stops_the_load() -> None:
    with pytest.raises(LoadError, match="PUT failed"):
        check_put_results(put_rows(status="ERROR"), make_manifest())


def test_manifest_file_missing_from_put_results_stops_the_load() -> None:
    with pytest.raises(LoadError, match=r"Not staged: \['ETHUSDT.json.gz'\]"):
        check_put_results(put_rows()[:1], make_manifest())


def test_copy_results_with_every_file_fully_loaded_pass() -> None:
    check_copy_results(copy_rows(), make_manifest())


def test_copy_that_processed_no_files_stops_the_load() -> None:
    rows = [{"status": "Copy executed with 0 files processed."}]
    with pytest.raises(LoadError, match="COPY loaded nothing"):
        check_copy_results(rows, make_manifest())


def test_copy_with_fewer_rows_than_the_manifest_stops_the_load() -> None:
    with pytest.raises(LoadError, match="rows_loaded 23, expected 24"):
        check_copy_results(copy_rows(rows_loaded=ROWS - 1), make_manifest())


def test_copy_with_a_failed_file_stops_the_load() -> None:
    with pytest.raises(LoadError, match="status LOAD_FAILED"):
        check_copy_results(copy_rows(status="LOAD_FAILED"), make_manifest())


def test_copy_that_skipped_a_manifest_file_stops_the_load() -> None:
    with pytest.raises(LoadError, match="did not process ETHUSDT.json.gz"):
        check_copy_results(copy_rows()[:1], make_manifest())


# --- reconciliation with the manifest ------------------------------------------------------


def lower(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k.lower(): v for k, v in r.items()} for r in rows]


def test_batch_matching_the_manifest_reconciles() -> None:
    assert reconcile(lower(stats_rows()), make_manifest()) == []


def test_symbol_missing_from_the_table_is_reported() -> None:
    assert reconcile(lower(stats_rows())[:1], make_manifest()) == ["ETHUSDT: not in table"]


def test_symbol_not_in_the_manifest_is_reported() -> None:
    stats = lower(stats_rows()) + [{"symbol": "SOLUSDT", "row_count": 1, "duplicate_count": 0}]
    assert reconcile(stats, make_manifest()) == ["SOLUSDT: not in manifest"]


def test_row_count_mismatch_is_reported() -> None:
    problems = reconcile(lower(stats_rows(row_count=2 * ROWS)), make_manifest())
    assert problems == ["BTCUSDT: 48 rows, manifest 24", "ETHUSDT: 48 rows, manifest 24"]


def test_duplicates_introduced_by_the_load_are_reported() -> None:
    problems = reconcile(lower(stats_rows(duplicate_count=1)), make_manifest())
    assert problems == [
        "BTCUSDT: 1 duplicate open_time, manifest 0",
        "ETHUSDT: 1 duplicate open_time, manifest 0",
    ]


def test_duplicates_already_in_the_source_file_reconcile() -> None:
    stats = lower(stats_rows(duplicate_count=1))
    assert reconcile(stats, make_manifest(duplicates=1)) == []


# --- orchestration: idempotency guard --------------------------------------------------------


def test_new_batch_is_put_copied_and_reconciled(batch_dir: Path) -> None:
    cur = FakeCursor([[], put_rows(), copy_rows(), stats_rows()])

    problems = load_batch(cur, batch_dir, make_manifest())

    assert problems == []
    assert cur.executed == [
        BATCH_STATS_SQL,
        put_sql(batch_dir),
        copy_sql(BATCH_ID, list(SYMBOLS)),
        BATCH_STATS_SQL,
    ]
    assert cur.params[0] == (BATCH_ID,)


def test_batch_already_in_the_table_is_not_loaded_again(batch_dir: Path) -> None:
    cur = FakeCursor([stats_rows()])

    problems = load_batch(cur, batch_dir, make_manifest())

    assert problems == []
    assert cur.executed == [BATCH_STATS_SQL]  # no PUT, no COPY


def test_local_mismatch_stops_before_any_statement(batch_dir: Path) -> None:
    (batch_dir / "BTCUSDT.json").write_text("[]", encoding="utf-8")
    cur = FakeCursor([])

    with pytest.raises(LoadError):
        load_batch(cur, batch_dir, make_manifest())

    assert cur.executed == []
