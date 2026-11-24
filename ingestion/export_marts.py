"""Export the marts the dashboard reads from Snowflake to local Parquet files.

Run from repo root: python -m ingestion.export_marts
In Airflow it is the last task of the pipeline, after dbt has built and checked the marts.

    data/marts/<export_id>/<table>.parquet   one file per exported table
    data/marts/<export_id>/_manifest.json    written last: marks the export complete

The dashboard reads the newest complete export and never Snowflake: it keeps working after
the trial ends, and the files are the same whatever engine builds the marts. Each export
is a new directory (named like a landing-zone batch: its UTC as-of moment), so a reader
never sees half a set; the newest KEEP_EXPORTS complete exports are kept.

Connects as the read-only service user CRYPTO_REPORTER and refuses to export if the
session could use any other role. Column types come from the result's metadata, not from
its values (see arrow_type), so a file's schema does not change between exports.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.batches import MANIFEST_NAME, batch_id_for, complete_batches, is_batch_id
from ingestion.clients.snowflake_conn import (
    PROJECT_ROOT,
    REPORTER_IDENTITY,
    connect,
    reporter_connection_params,
)

OUTPUT_DIR = PROJECT_ROOT / "data" / "marts"
KEEP_EXPORTS = 8  # a day of exports at the pipeline's 3-hour cadence
EXPECTED_ROLES = frozenset({REPORTER_IDENTITY["role"], "PUBLIC"})  # PUBLIC: every user has it


@dataclass(frozen=True)
class MartExport:
    """One exported table. Fixed names, never user input: they go into SQL as they are."""

    name: str  # file name, without .parquet
    relation: str
    order_by: str  # a stable row order: the same data gives the same file


EXPORTS = (
    MartExport("dim_symbol", "CRYPTO_DWH.MARTS.DIM_SYMBOL", "symbol"),
    MartExport("fct_pump_events", "CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS", "event_open_time_ms, symbol"),
    MartExport(
        "fct_pump_event_window",
        "CRYPTO_DWH.MARTS.FCT_PUMP_EVENT_WINDOW",
        "pump_event_id, offset_hours",
    ),
    MartExport(
        "mart_pump_profile",
        "CRYPTO_DWH.MARTS.MART_PUMP_PROFILE",
        "event_scope, metric, offset_hours",
    ),
    MartExport(
        "dq_check_results",
        "CRYPTO_DWH.DQ.DQ_CHECK_RESULTS",
        "run_started_at, invocation_id, check_unique_id",
    ),
)

# Type codes of snowflake.connector.constants.FIELD_ID_TO_NAME
FIXED = 0
REAL = 1
TEXT = 2
DATE = 3
TIMESTAMP_LTZ = 6
TIMESTAMP_TZ = 7
TIMESTAMP_NTZ = 8
BOOLEAN = 13


class ColumnMeta(Protocol):
    """The fields of the connector's ResultMetadata that the export reads."""

    @property
    def name(self) -> str: ...
    @property
    def type_code(self) -> int: ...
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...


class ArrowCursor(Protocol):
    @property
    def description(self) -> Sequence[ColumnMeta] | None: ...
    @property
    def sfqid(self) -> str | None: ...

    def execute(self, command: str, /) -> Any: ...
    def fetch_arrow_all(
        self, force_return_table: bool = False, force_microsecond_precision: bool | None = None
    ) -> pa.Table | None: ...


def arrow_type(column: ColumnMeta) -> pa.DataType:
    """The Parquet type of a Snowflake column, from the result metadata.

    The connector picks the Arrow type from the values: int8 ... int64 by the largest
    integer in the result, and decimal or double for fixed-point numbers. One schema per
    column type keeps the files stable. An unexpected type fails: a new column type in a
    mart must be a decision, not a surprise in the dashboard.
    """
    code = column.type_code
    if code == FIXED:
        if not column.scale:
            return pa.int64()
        return pa.decimal128(column.precision or 38, column.scale)
    if code == REAL:
        return pa.float64()
    if code == TEXT:
        return pa.string()
    if code == BOOLEAN:
        return pa.bool_()
    if code == DATE:
        return pa.date32()
    if code in (TIMESTAMP_TZ, TIMESTAMP_LTZ):
        return pa.timestamp("us", tz="UTC")
    if code == TIMESTAMP_NTZ:
        return pa.timestamp("us")
    raise ValueError(f"Column {column.name}: Snowflake type code {code} is not exported")


def target_schema(description: Sequence[ColumnMeta]) -> pa.Schema:
    """Lower-case column names (as in the dbt models) with their stable types."""
    return pa.schema([pa.field(c.name.lower(), arrow_type(c)) for c in description])


def conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Rename the result's columns and cast them to the schema; a lossy cast fails."""
    return table.rename_columns(schema.names).cast(schema, safe=True)


def select_sql(export: MartExport) -> str:
    return f"SELECT * FROM {export.relation} ORDER BY {export.order_by}"


def session_problem(current_role: str, available_roles_json: str) -> str | None:
    """Why this session must not export, or None: the reporter role and nothing more."""
    if current_role != REPORTER_IDENTITY["role"]:
        return f"current role is {current_role}, not {REPORTER_IDENTITY['role']}"
    extra = set(json.loads(available_roles_json)) - EXPECTED_ROLES
    if extra:
        return f"session can use roles beyond {sorted(EXPECTED_ROLES)}: {sorted(extra)}"
    return None


def fetch_table(cur: ArrowCursor, export: MartExport) -> tuple[pa.Table, str | None]:
    cur.execute(select_sql(export))
    if cur.description is None:
        raise RuntimeError(f"{export.relation}: the query returned no result metadata")
    schema = target_schema(cur.description)
    # force_return_table: an empty result is a table with its columns, not None.
    table = cur.fetch_arrow_all(force_return_table=True, force_microsecond_precision=True)
    if table is None:
        raise RuntimeError(f"{export.relation}: no Arrow result")
    return conform(table, schema), cur.sfqid


def write_export(cur: ArrowCursor, export_dir: Path, as_of: datetime) -> dict[str, Any]:
    """Write every table of EXPORTS into export_dir, the manifest last; return the manifest.

    The directory must not exist yet. A failure leaves it without a manifest: incomplete,
    invisible to readers, removed by a later prune.
    """
    export_dir.mkdir(parents=True)
    tables: dict[str, Any] = {}
    for export in EXPORTS:
        table, query_id = fetch_table(cur, export)
        path = export_dir / f"{export.name}.parquet"
        pq.write_table(table, path)
        tables[export.name] = {
            "relation": export.relation,
            "rows": table.num_rows,
            "columns": {field.name: str(field.type) for field in table.schema},
            "query_id": query_id,  # lineage: QUERY_HISTORY of the reporter
        }
        print(f"  {export.name}: {table.num_rows} rows, {path.stat().st_size} bytes")
    manifest = {
        "export_id": export_dir.name,
        "exported_at_utc": as_of.isoformat(),
        "source": "snowflake",
        "tables": tables,
    }
    (export_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def stale_exports(root: Path, keep: int) -> list[Path]:
    """Export directories to remove: complete ones beyond the newest `keep`, and incomplete
    ones older than the newest complete export (an incomplete one newer than that may be
    an export in progress). Anything not named like an export is left alone."""
    complete = complete_batches(root)
    if not complete:
        return []
    kept = {d.name for d in complete[-keep:]} if keep > 0 else set()
    newest = complete[-1].name
    return sorted(
        d
        for d in root.iterdir()
        if d.is_dir() and is_batch_id(d.name) and d.name not in kept and d.name < newest
    )


def prune_exports(root: Path, keep: int = KEEP_EXPORTS) -> list[Path]:
    stale = stale_exports(root, keep)
    for directory in stale:
        shutil.rmtree(directory)
    return stale


def main(now_ms: int | None = None) -> int:
    root = OUTPUT_DIR
    if now_ms is None:
        now_ms = time.time_ns() // 1_000_000
    as_of = datetime.fromtimestamp(now_ms / 1000, tz=UTC)
    export_dir = root / batch_id_for(now_ms)
    if export_dir.exists():  # before connecting, as for a landing-zone batch
        raise FileExistsError(f"Export already exists: {export_dir}")

    with connect(params=reporter_connection_params) as conn, conn.cursor() as cur:
        # Context functions only: answered without the warehouse.
        cur.execute("SELECT CURRENT_ROLE(), CURRENT_AVAILABLE_ROLES()")
        row = cur.fetchone()
        if not isinstance(row, tuple):
            raise RuntimeError("Context query returned no rows")
        current_role, available_roles = row
        problem = session_problem(current_role, available_roles)
        if problem:
            print(f"FAIL: {problem}; nothing exported")
            return 1
        print(f"Export {export_dir.name} as {current_role} -> {export_dir}")
        manifest = write_export(cur, export_dir, as_of)

    removed = prune_exports(root)
    total = sum(t["rows"] for t in manifest["tables"].values())
    print(
        f"OK: exported {len(manifest['tables'])} tables, {total} rows; "
        f"removed {len(removed)} old exports"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
