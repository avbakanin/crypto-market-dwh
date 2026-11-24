"""Tests for the export of the marts to Parquet (fake Snowflake cursor, temporary directories)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion import export_marts
from ingestion.batches import MANIFEST_NAME, batch_id_for
from ingestion.clients.snowflake_conn import reporter_connection_params
from ingestion.export_marts import (
    BOOLEAN,
    DATE,
    EXPORTS,
    FIXED,
    REAL,
    TEXT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    arrow_type,
    conform,
    select_sql,
    session_problem,
    stale_exports,
    target_schema,
    write_export,
)
from tests.factories import START_MS
from tests.fakes import Column, FakeArrowConnection, FakeArrowCursor

AS_OF = datetime(2026, 11, 24, 9, 10, tzinfo=UTC)
METADATA = [
    Column("ID", TEXT),
    Column("N", FIXED, 38, 0),
    Column("X", FIXED, 38, 8),
    Column("TS", TIMESTAMP_TZ, 0, 9),
    Column("FLAG", BOOLEAN),
]


def connector_table(rows: int, int_type: pa.DataType | None = None) -> pa.Table:
    """A result as the connector hands it over: upper-case names, the narrowest int type."""
    return pa.table(
        {
            "ID": pa.array([f"id-{i}" for i in range(rows)], pa.string()),
            "N": pa.array(list(range(rows)), int_type or pa.int8()),
            "X": pa.array([Decimal("1.12345678")] * rows, pa.decimal128(38, 8)),
            "TS": pa.array([START_MS * 1000] * rows, pa.timestamp("us", tz="UTC")),
            "FLAG": pa.array([True] * rows, pa.bool_()),
        }
    )


def mart_results(rows: int = 3) -> dict[str, tuple[list[Column], pa.Table]]:
    return {export.relation: (METADATA, connector_table(rows)) for export in EXPORTS}


def stable_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.string()),
            ("n", pa.int64()),
            ("x", pa.decimal128(38, 8)),
            ("ts", pa.timestamp("us", tz="UTC")),
            ("flag", pa.bool_()),
        ]
    )


# --- types from the metadata ------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        (Column("C", FIXED, 38, 0), pa.int64()),
        (Column("C", FIXED, 18, 0), pa.int64()),
        (Column("C", FIXED, 38, 8), pa.decimal128(38, 8)),
        (Column("C", FIXED, 38, 15), pa.decimal128(38, 15)),
        (Column("C", FIXED, 10, 2), pa.decimal128(10, 2)),
        (Column("C", REAL), pa.float64()),
        (Column("C", TEXT), pa.string()),
        (Column("C", BOOLEAN), pa.bool_()),
        (Column("C", DATE), pa.date32()),
        (Column("C", TIMESTAMP_TZ, 0, 9), pa.timestamp("us", tz="UTC")),
        (Column("C", TIMESTAMP_LTZ, 0, 9), pa.timestamp("us", tz="UTC")),
        (Column("C", TIMESTAMP_NTZ, 0, 9), pa.timestamp("us")),
    ],
)
def test_arrow_type_follows_the_metadata(column: Column, expected: pa.DataType) -> None:
    assert arrow_type(column) == expected


def test_a_column_type_the_export_does_not_know_is_refused() -> None:
    with pytest.raises(ValueError, match="PAYLOAD"):
        arrow_type(Column("PAYLOAD", 5))  # VARIANT


def test_integer_width_chosen_by_the_values_gives_one_file_schema() -> None:
    """Live: the connector sends int8 ... int64 by the largest value of the result."""
    schema = target_schema(METADATA)
    small = conform(connector_table(3, pa.int8()), schema)
    large = conform(connector_table(3, pa.int32()), schema)
    assert small.schema == large.schema == stable_schema()


def test_a_lossy_cast_fails_instead_of_rounding() -> None:
    table = connector_table(1).set_column(
        2, "X", pa.array([Decimal("1.123456789012")], pa.decimal128(38, 12))
    )
    with pytest.raises(pa.ArrowInvalid):
        conform(table, target_schema(METADATA))


# --- queries and the session ------------------------------------------------------------


def test_exported_files_are_the_ones_the_readers_expect() -> None:
    """Independent literals: renaming a file breaks the dashboard or the analysis."""
    assert [export.name for export in EXPORTS] == [
        "dim_symbol",
        "fct_pump_events",
        "fct_pump_event_window",
        "mart_pump_profile",
        "fct_pump_event_excess",
        "mart_pump_signal_check",
        "dq_check_results",
        "dq_data_coverage",
    ]
    assert {export.relation.split(".")[1] for export in EXPORTS} == {"MARTS", "DQ"}


def test_every_query_has_a_stable_row_order() -> None:
    assert select_sql(EXPORTS[0]) == "SELECT * FROM CRYPTO_DWH.MARTS.DIM_SYMBOL ORDER BY symbol"
    assert all(" ORDER BY " in select_sql(export) for export in EXPORTS)


@pytest.mark.parametrize(
    ("role", "available", "problem"),
    [
        ("CRYPTO_REPORTER", ["CRYPTO_REPORTER", "PUBLIC"], None),
        ("CRYPTO_REPORTER", ["CRYPTO_REPORTER"], None),
        ("CRYPTO_TRANSFORMER", ["CRYPTO_TRANSFORMER", "PUBLIC"], "current role"),
        ("CRYPTO_REPORTER", ["CRYPTO_REPORTER", "PUBLIC", "SYSADMIN"], "SYSADMIN"),
    ],
)
def test_session_must_be_the_reporter_and_nothing_more(
    role: str, available: list[str], problem: str | None
) -> None:
    found = session_problem(role, json.dumps(available))
    if problem is None:
        assert found is None
    else:
        assert found is not None and problem in found


# --- one export -------------------------------------------------------------------------


def test_export_writes_every_table_and_the_manifest(tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    cur = FakeArrowCursor(mart_results(3))
    manifest = write_export(cur, export_dir, AS_OF)

    assert sorted(p.name for p in export_dir.iterdir()) == sorted(
        [f"{export.name}.parquet" for export in EXPORTS] + [MANIFEST_NAME]
    )
    for export in EXPORTS:
        table = pq.read_table(export_dir / f"{export.name}.parquet")
        assert table.schema == stable_schema()
        assert table.num_rows == 3
    assert json.loads((export_dir / MANIFEST_NAME).read_text(encoding="utf-8")) == manifest
    assert manifest["export_id"] == "20261124T091000Z"
    assert manifest["exported_at_utc"] == "2026-11-24T09:10:00+00:00"
    entry = manifest["tables"]["fct_pump_events"]
    assert entry["relation"] == "CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS"
    assert entry["rows"] == 3
    assert entry["columns"]["n"] == "int64"
    assert entry["query_id"] == "query-2"


def test_timestamps_are_fetched_in_microseconds_and_empty_results_as_tables(
    tmp_path: Path,
) -> None:
    cur = FakeArrowCursor(mart_results())
    write_export(cur, tmp_path / "20261124T091000Z", AS_OF)
    assert cur.fetch_kwargs == [
        {"force_return_table": True, "force_microsecond_precision": True}
    ] * len(EXPORTS)


def test_an_empty_mart_is_exported_with_its_columns(tmp_path: Path) -> None:
    results = mart_results()
    results["CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS"] = (METADATA, connector_table(0))
    export_dir = tmp_path / "20261124T091000Z"
    manifest = write_export(FakeArrowCursor(results), export_dir, AS_OF)
    table = pq.read_table(export_dir / "fct_pump_events.parquet")
    assert table.num_rows == 0
    assert table.schema == stable_schema()
    assert manifest["tables"]["fct_pump_events"]["rows"] == 0


def test_a_failed_export_leaves_no_manifest(tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    cur = FakeArrowCursor(mart_results(), fail_on=EXPORTS[2].relation)
    with pytest.raises(RuntimeError, match="connection lost"):
        write_export(cur, export_dir, AS_OF)
    assert sorted(p.name for p in export_dir.iterdir()) == [
        "dim_symbol.parquet",
        "fct_pump_events.parquet",
    ]


def test_an_existing_export_directory_is_never_overwritten(tmp_path: Path) -> None:
    export_dir = tmp_path / "20261124T091000Z"
    export_dir.mkdir()
    cur = FakeArrowCursor(mart_results())
    with pytest.raises(FileExistsError):
        write_export(cur, export_dir, AS_OF)
    assert cur.executed == []


# --- retention --------------------------------------------------------------------------


def make_export(root: Path, export_id: str, complete: bool = True) -> Path:
    directory = root / export_id
    directory.mkdir(parents=True)
    (directory / "dim_symbol.parquet").write_bytes(b"PAR1")
    if complete:
        (directory / MANIFEST_NAME).write_text("{}", encoding="utf-8")
    return directory


def test_old_complete_exports_beyond_the_newest_ones_are_stale(tmp_path: Path) -> None:
    ids = ["20261124T000700Z", "20261124T030700Z", "20261124T060700Z", "20261124T090700Z"]
    for export_id in ids:
        make_export(tmp_path, export_id)
    assert [d.name for d in stale_exports(tmp_path, keep=2)] == ids[:2]


def test_incomplete_exports_are_stale_only_when_older_than_the_newest_complete(
    tmp_path: Path,
) -> None:
    make_export(tmp_path, "20261124T030700Z", complete=False)  # failed export
    make_export(tmp_path, "20261124T060700Z")
    make_export(tmp_path, "20261124T090700Z", complete=False)  # maybe still being written
    (tmp_path / "2026-11-24-backup").mkdir()  # a manual copy, sorts first: never touched
    (tmp_path / "20261124T010700Z.txt").write_text("x", encoding="utf-8")
    assert [d.name for d in stale_exports(tmp_path, keep=8)] == ["20261124T030700Z"]


def test_nothing_is_stale_without_a_complete_export(tmp_path: Path) -> None:
    make_export(tmp_path, "20261124T030700Z", complete=False)
    assert stale_exports(tmp_path, keep=8) == []
    assert stale_exports(tmp_path / "missing", keep=8) == []


# --- main -------------------------------------------------------------------------------

NOW_MS = int(AS_OF.timestamp() * 1000)


@pytest.fixture
def output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "marts"
    monkeypatch.setattr(export_marts, "OUTPUT_DIR", root)
    return root


def patch_connect(monkeypatch: pytest.MonkeyPatch, cursor: FakeArrowCursor) -> list[Any]:
    calls: list[Any] = []

    def fake_connect(env: object = None, params: object = None) -> FakeArrowConnection:
        calls.append(params)
        return FakeArrowConnection(cursor)

    monkeypatch.setattr(export_marts, "connect", fake_connect)
    return calls


def test_main_exports_as_the_reporter_and_prunes_old_exports(
    output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for hour in range(8):
        make_export(output_dir, f"20261123T{hour:02d}0700Z")
    calls = patch_connect(monkeypatch, FakeArrowCursor(mart_results()))

    assert export_marts.main(now_ms=NOW_MS) == 0
    assert calls == [reporter_connection_params]
    exports = sorted(d.name for d in output_dir.iterdir())
    assert len(exports) == export_marts.KEEP_EXPORTS
    assert exports[0] == "20261123T010700Z"  # the oldest one is gone
    assert exports[-1] == batch_id_for(NOW_MS) == "20261124T091000Z"


def test_main_refuses_a_session_with_other_roles_and_writes_nothing(
    output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cursor = FakeArrowCursor(
        mart_results(), available_roles=["CRYPTO_REPORTER", "PUBLIC", "CRYPTO_TRANSFORMER"]
    )
    patch_connect(monkeypatch, cursor)
    assert export_marts.main(now_ms=NOW_MS) == 1
    assert not output_dir.exists()
    assert len(cursor.executed) == 1  # the context query only


def test_main_refuses_an_existing_export_before_connecting(
    output_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_export(output_dir, batch_id_for(NOW_MS))
    calls = patch_connect(monkeypatch, FakeArrowCursor(mart_results()))
    with pytest.raises(FileExistsError):
        export_marts.main(now_ms=NOW_MS)
    assert calls == []
