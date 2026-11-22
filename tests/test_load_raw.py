"""Tests for the RAW loader: statements, result checks, guard, pending batches, datasets, main.

No Snowflake: statements go to a scripted fake cursor. What Snowflake itself does with
them (PUT/COPY semantics) is verified on the live account, not here.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from ingestion import load_raw
from ingestion.batches import Manifest, write_manifest
from ingestion.load_raw import (
    DATASETS,
    FUNDING_RATE,
    KLINES,
    OPEN_INTEREST,
    LoadError,
    RawDataset,
    batch_stats_sql,
    check_copy_results,
    check_local_files,
    check_put_results,
    copy_sql,
    ensure_raw_objects,
    load_batch,
    load_pending,
    loaded_batches_sql,
    main,
    pending_batches,
    put_sql,
    reconcile,
)
from tests.factories import make_klines
from tests.fakes import FakeConnection, FakeCursor

BATCH_ID = "20250102T000000Z"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
ROWS = 24


def make_manifest(rows: int = ROWS, duplicates: int = 0, batch_id: str = BATCH_ID) -> Manifest:
    return {
        "batch_id": batch_id,
        "as_of_utc": "2025-01-02T00:00:00+00:00",
        "interval": "1h",
        "symbols": {s: {"rows": rows, "duplicates": duplicates, "gaps": 0} for s in SYMBOLS},
    }


def write_batch(root: Path, batch_id: str = BATCH_ID) -> Path:
    path = root / batch_id
    path.mkdir(parents=True)
    for symbol in SYMBOLS:
        (path / f"{symbol}.json").write_text(json.dumps(make_klines(ROWS)), encoding="utf-8")
    write_manifest(path, make_manifest(batch_id=batch_id))
    return path


@pytest.fixture
def batch_dir(tmp_path: Path) -> Path:
    return write_batch(tmp_path)


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
    assert put_sql(KLINES, batch_dir) == (
        f"PUT 'file://{batch_dir.as_posix()}/*' @RAW.KLINES_STAGE/{BATCH_ID}/ "
        "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
    )


def test_put_refuses_a_directory_that_is_not_a_batch(tmp_path: Path) -> None:
    with pytest.raises(LoadError, match="Not a batch directory"):
        put_sql(KLINES, tmp_path / "klines_1h")


def test_copy_reads_only_the_batch_directory_and_the_manifest_files() -> None:
    sql = copy_sql(KLINES, BATCH_ID, ["ETHUSDT", "BTCUSDT"])

    assert f"FROM @RAW.KLINES_STAGE/{BATCH_ID}/\n" in sql
    assert "FILES = ('BTCUSDT.json.gz', 'ETHUSDT.json.gz')" in sql
    assert "ON_ERROR = ABORT_STATEMENT" in sql


@pytest.mark.parametrize("batch_id", ["latest", "20250102T000000Z/../", "x' OR '1'='1"])
def test_copy_refuses_an_invalid_batch_id(batch_id: str) -> None:
    with pytest.raises(LoadError, match="Invalid batch id"):
        copy_sql(KLINES, batch_id, ["BTCUSDT"])


@pytest.mark.parametrize("symbol", ["btcusdt", "BTC.json.gz', 'X", "../BTCUSDT", ""])
def test_copy_refuses_a_symbol_outside_the_expected_alphabet(symbol: str) -> None:
    with pytest.raises(LoadError, match="Unexpected symbol name"):
        copy_sql(KLINES, BATCH_ID, [symbol])


def test_futures_contract_names_are_valid_symbols() -> None:
    assert "'1000PEPEUSDT.json.gz'" in copy_sql(FUNDING_RATE, BATCH_ID, ["1000PEPEUSDT"])


# --- datasets: same loading, own stage, table, format and key -------------------------------


@pytest.mark.parametrize(
    ("dataset", "stage", "table", "file_format", "key"),
    [
        (KLINES, "KLINES_STAGE", "BINANCE_SPOT_KLINES_1H", "KLINES_JSON_FORMAT", "payload[0]"),
        (
            FUNDING_RATE,
            "FUNDING_RATE_STAGE",
            "BINANCE_FUTURES_FUNDING_RATE",
            "JSON_ARRAY_FORMAT",
            "FLOOR(payload:fundingTime::NUMBER(38, 0) / 3600000)",  # the hour, like the extract
        ),
        (
            OPEN_INTEREST,
            "OPEN_INTEREST_STAGE",
            "BINANCE_FUTURES_OPEN_INTEREST_1H",
            "JSON_ARRAY_FORMAT",
            "payload:timestamp",
        ),
    ],
    ids=["klines", "funding_rate", "open_interest"],
)
def test_each_dataset_has_its_own_stage_table_format_and_key(
    batch_dir: Path, dataset: RawDataset, stage: str, table: str, file_format: str, key: str
) -> None:
    copy = copy_sql(dataset, BATCH_ID, ["BTCUSDT"])
    assert f"@RAW.{stage}/{BATCH_ID}/" in put_sql(dataset, batch_dir)
    assert f"COPY INTO RAW.{table}\n" in copy
    assert f"FROM @RAW.{stage}/{BATCH_ID}/\n" in copy
    assert f"FORMAT_NAME = 'RAW.{file_format}'" in copy
    assert f"COUNT(DISTINCT {key}" in batch_stats_sql(dataset)
    assert f"FROM RAW.{table}\n" in batch_stats_sql(dataset)
    assert loaded_batches_sql(dataset) == f"SELECT DISTINCT _batch_id FROM RAW.{table}"


def test_datasets_are_loaded_klines_first() -> None:
    assert [d.name for d in DATASETS] == ["klines", "funding_rate", "open_interest"]


def test_landing_zones_keep_their_directories() -> None:
    # The landing zone is the extract's watermark: a renamed directory would start the
    # history over (a year of klines, 30 days of open interest) and orphan the old batches.
    root = Path(load_raw.PROJECT_ROOT) / "data" / "raw"
    assert {d.name: d.landing_dir.relative_to(root).as_posix() for d in DATASETS} == {
        "klines": "klines_1h",
        "funding_rate": "funding_rate",
        "open_interest": "open_interest_1h",
    }


def test_raw_objects_of_every_dataset_are_created() -> None:
    class RecordingConnection:
        def __init__(self) -> None:
            self.scripts: list[str] = []

        def execute_string(self, sql: str, return_cursors: bool = True) -> list[Any]:
            self.scripts.append(sql)
            return []

    conn = RecordingConnection()
    ensure_raw_objects(conn)  # pyright: ignore[reportArgumentType]

    assert len(conn.scripts) == 2
    assert "RAW.BINANCE_SPOT_KLINES_1H" in conn.scripts[0]
    assert "RAW.BINANCE_FUTURES_FUNDING_RATE" in conn.scripts[1]
    assert "RAW.BINANCE_FUTURES_OPEN_INTEREST_1H" in conn.scripts[1]


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


def test_copy_that_skipped_files_loaded_before_stops_the_load() -> None:
    rows = copy_rows(rows_loaded=0, status="LOAD_SKIPPED")
    for row in rows:
        row["first_error"] = "File was loaded before."  # live answer to COPY ... FILES = (...)
    with pytest.raises(LoadError, match="skipped it as loaded before"):
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
        "BTCUSDT: 1 duplicate keys, manifest 0",
        "ETHUSDT: 1 duplicate keys, manifest 0",
    ]


def test_duplicates_already_in_the_source_file_reconcile() -> None:
    stats = lower(stats_rows(duplicate_count=1))
    assert reconcile(stats, make_manifest(duplicates=1)) == []


# --- orchestration: idempotency guard --------------------------------------------------------


def load_statements(dataset: RawDataset, batch_dir: Path) -> list[str]:
    """What load_batch runs for a batch that is not in the table yet."""
    return [
        batch_stats_sql(dataset),
        put_sql(dataset, batch_dir),
        copy_sql(dataset, batch_dir.name, list(SYMBOLS)),
        batch_stats_sql(dataset),
    ]


def test_new_batch_is_put_copied_and_reconciled(batch_dir: Path) -> None:
    cur = FakeCursor([[], put_rows(), copy_rows(), stats_rows()])

    problems = load_batch(cur, KLINES, batch_dir, make_manifest())

    assert problems == []
    assert cur.executed == load_statements(KLINES, batch_dir)
    assert cur.params[0] == (BATCH_ID,)


def test_batch_already_in_the_table_is_not_loaded_again(batch_dir: Path) -> None:
    cur = FakeCursor([stats_rows()])

    problems = load_batch(cur, KLINES, batch_dir, make_manifest())

    assert problems == []
    assert cur.executed == [batch_stats_sql(KLINES)]  # no PUT, no COPY


def test_local_mismatch_stops_before_any_statement(batch_dir: Path) -> None:
    (batch_dir / "BTCUSDT.json").write_text("[]", encoding="utf-8")
    cur = FakeCursor([])

    with pytest.raises(LoadError):
        load_batch(cur, KLINES, batch_dir, make_manifest())

    assert cur.executed == []


# --- pending batches: load watermark = _batch_id values in RAW -------------------------------

OLD, MID, NEW = "20250101T000000Z", "20250102T000000Z", "20250103T000000Z"


def test_pending_batches_are_complete_ones_not_in_raw_oldest_first(tmp_path: Path) -> None:
    new, old = write_batch(tmp_path, NEW), write_batch(tmp_path, OLD)
    write_batch(tmp_path, MID)
    (tmp_path / "20250104T000000Z").mkdir()  # extract crashed: no manifest

    assert pending_batches(tmp_path, loaded={MID}) == [old, new]


def test_every_pending_batch_is_loaded_oldest_first(tmp_path: Path) -> None:
    write_batch(tmp_path, OLD)
    mid, new = write_batch(tmp_path, MID), write_batch(tmp_path, NEW)
    dataset = replace(KLINES, landing_dir=tmp_path)
    cur = FakeCursor(
        [[{"_BATCH_ID": OLD}]]  # upper-case key, as Snowflake returns the column
        + [[], put_rows(), copy_rows(), stats_rows()] * 2
    )

    assert load_pending(cur, dataset) == ([MID, NEW], [])

    assert cur.executed == [
        loaded_batches_sql(dataset),
        *load_statements(dataset, mid),
        *load_statements(dataset, new),
    ]
    assert [p for p in cur.params if p] == [(MID,), (MID,), (NEW,), (NEW,)]


def test_nothing_pending_runs_only_the_lookup(tmp_path: Path) -> None:
    write_batch(tmp_path, OLD)
    dataset = replace(KLINES, landing_dir=tmp_path)
    cur = FakeCursor([[{"_BATCH_ID": OLD}]])

    assert load_pending(cur, dataset) == ([], [])

    assert cur.executed == [loaded_batches_sql(dataset)]  # no PUT, no COPY


def test_load_stops_at_the_first_batch_that_does_not_reconcile(tmp_path: Path) -> None:
    mid = write_batch(tmp_path, MID)
    write_batch(tmp_path, NEW)
    dataset = replace(KLINES, landing_dir=tmp_path)
    cur = FakeCursor([[], [], put_rows(), copy_rows(), stats_rows(row_count=ROWS + 1)])

    loaded, problems = load_pending(cur, dataset)

    assert loaded == []
    assert problems == [f"{MID}: {s}: {ROWS + 1} rows, manifest {ROWS}" for s in SYMBOLS]
    assert cur.executed == [loaded_batches_sql(dataset), *load_statements(dataset, mid)]


def test_batches_loaded_before_a_failure_are_reported_as_loaded(tmp_path: Path) -> None:
    write_batch(tmp_path, MID)
    write_batch(tmp_path, NEW)
    cur = FakeCursor(
        [[], [], put_rows(), copy_rows(), stats_rows()]
        + [[], put_rows(), copy_rows(), stats_rows(row_count=ROWS + 1)]
    )

    loaded, problems = load_pending(cur, replace(KLINES, landing_dir=tmp_path))

    assert loaded == [MID]
    assert len(problems) == len(SYMBOLS)


# --- main: datasets, one session, exit code and the final line ------------------------------


def run_main(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    results: list[list[dict[str, Any]]],
    argv: list[str] | None = None,
) -> tuple[int, FakeCursor, list[int]]:
    """Run main() on tmp_path/<dataset> landing dirs with a scripted cursor; no Snowflake."""
    cur = FakeCursor(results)
    connects: list[int] = []

    def fake_connect() -> FakeConnection:
        connects.append(1)
        return FakeConnection(cur)

    datasets = tuple(replace(d, landing_dir=tmp_path / d.name) for d in DATASETS)
    monkeypatch.setattr(load_raw, "DATASETS", datasets)
    monkeypatch.setattr(load_raw, "connect", fake_connect)
    monkeypatch.setattr(load_raw, "ensure_raw_objects", lambda conn: None)
    return main(argv or []), cur, connects


def last_line(capsys: pytest.CaptureFixture[str]) -> str:
    return capsys.readouterr().out.strip().splitlines()[-1]


def new_batch_results() -> list[list[dict[str, Any]]]:
    """Cursor answers for one dataset with one pending batch that loads and reconciles."""
    return [[], [], put_rows(), copy_rows(), stats_rows()]


def test_main_with_nothing_pending_says_nothing_to_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "klines", OLD)

    exit_code, _, _ = run_main(monkeypatch, tmp_path, [[{"_BATCH_ID": OLD}]])

    assert exit_code == 0
    assert last_line(capsys) == "OK: klines nothing to load"


def test_main_names_the_batches_it_loaded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "klines", OLD)
    write_batch(tmp_path / "klines", MID)
    results = [[{"_BATCH_ID": OLD}], [], put_rows(), copy_rows(), stats_rows()]

    exit_code, _, _ = run_main(monkeypatch, tmp_path, results)

    assert exit_code == 0
    assert last_line(capsys) == f"OK: klines loaded and reconciled {MID}"


def test_main_loads_every_dataset_in_one_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in ("klines", "funding_rate", "open_interest"):
        write_batch(tmp_path / name, MID)

    exit_code, cur, connects = run_main(monkeypatch, tmp_path, new_batch_results() * 3)

    assert exit_code == 0
    assert connects == [1]  # one connection, so the warehouse resumes once per run
    lookups = [sql for sql in cur.executed if sql.startswith("SELECT DISTINCT")]
    assert lookups == [loaded_batches_sql(d) for d in (KLINES, FUNDING_RATE, OPEN_INTEREST)]
    assert last_line(capsys) == (
        f"OK: klines loaded and reconciled {MID}; funding_rate loaded and reconciled {MID}; "
        f"open_interest loaded and reconciled {MID}"
    )


def test_dataset_without_batches_is_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "funding_rate", MID)

    exit_code, cur, _ = run_main(monkeypatch, tmp_path, new_batch_results())

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "klines: no complete batch" in out
    assert "open_interest: no complete batch" in out
    assert [sql for sql in cur.executed if sql.startswith("SELECT DISTINCT")] == [
        loaded_batches_sql(FUNDING_RATE)
    ]


def test_failure_in_one_dataset_does_not_stop_the_others(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "klines", MID)
    write_batch(tmp_path / "funding_rate", MID)
    failing = [[], [], put_rows(), copy_rows(), stats_rows(row_count=ROWS + 1)]

    exit_code, cur, _ = run_main(monkeypatch, tmp_path, failing + new_batch_results())

    out = capsys.readouterr().out
    assert exit_code == 1
    assert copy_sql(FUNDING_RATE, MID, list(SYMBOLS)) in cur.executed  # funding still loaded
    assert f"klines: {MID}: BTCUSDT: {ROWS + 1} rows, manifest {ROWS}" in out
    assert "funding_rate:" not in out.split("FAIL")[1]


def test_main_with_one_batch_loads_only_that_batch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "open_interest", OLD)
    write_batch(tmp_path / "open_interest", MID)
    results = [[], put_rows(), copy_rows(), stats_rows()]  # no lookup of loaded batches

    argv = ["--dataset", "open_interest", "--batch", MID]
    exit_code, cur, _ = run_main(monkeypatch, tmp_path, results, argv)

    assert exit_code == 0
    assert cur.executed[2] == copy_sql(OPEN_INTEREST, MID, list(SYMBOLS))
    assert last_line(capsys) == f"OK: open_interest batch {MID} reconciles with its manifest"


def test_batch_without_dataset_is_a_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit) as exc:
        run_main(monkeypatch, tmp_path, [], ["--batch", MID])
    assert exc.value.code == 2


def test_main_fails_when_a_batch_does_not_reconcile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_batch(tmp_path / "klines", MID)
    results = [[], [], put_rows(), copy_rows(), stats_rows(row_count=ROWS + 1)]

    exit_code, _, _ = run_main(monkeypatch, tmp_path, results)

    assert exit_code == 1
    assert "FAIL: a batch does not reconcile" in capsys.readouterr().out


def test_main_without_any_complete_batch_fails_before_connecting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code, _, connects = run_main(monkeypatch, tmp_path, [])

    assert exit_code == 1
    assert connects == []
    assert "No complete batch to load" in capsys.readouterr().out
