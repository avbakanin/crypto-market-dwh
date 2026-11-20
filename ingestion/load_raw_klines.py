"""Load one extracted klines batch into Snowflake RAW: PUT to the stage, COPY INTO the table.

Run from repo root: python -m ingestion.load_raw_klines [--batch BATCH_ID]
Default batch: the latest complete one in data/raw/klines_1h (see batches.py).

Idempotent: a batch already in the table is not loaded again; COPY's load metadata
skips files it has loaded before. Exit code 1 if the loaded batch does not reconcile
with its manifest (row counts per symbol, duplicates).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

from snowflake.connector import DictCursor, SnowflakeConnection

from ingestion.batches import Manifest, is_batch_id, latest_complete_batch, read_manifest
from ingestion.clients.snowflake_conn import connect
from ingestion.extract_klines import OUTPUT_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = PROJECT_ROOT / "snowflake" / "raw_klines.sql"

STAGE = "RAW.KLINES_STAGE"
TABLE = "RAW.BINANCE_SPOT_KLINES_1H"
FILE_FORMAT = "RAW.KLINES_JSON_FORMAT"
SYMBOL_PATTERN = re.compile(r"[A-Z0-9]+")

PUT_OK_STATUSES = frozenset({"UPLOADED", "SKIPPED"})  # SKIPPED = same file already staged


class LoadError(RuntimeError):
    """The batch could not be loaded as expected; nothing is reconciled."""


class Cursor(Protocol):
    """The part of a Snowflake DictCursor the loader uses; tests pass a fake."""

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> object: ...

    def fetchall(self) -> list[Any]: ...


def query(cur: Cursor, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    """Run a statement and return its rows as dicts with lower-case column names."""
    cur.execute(sql, params)
    return [{key.lower(): value for key, value in row.items()} for row in cur.fetchall()]


def check_symbol(symbol: str) -> str:
    """Symbols end up in file paths and SQL text (PUT/COPY take no binds): allow [A-Z0-9]+ only."""
    if SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise LoadError(f"Unexpected symbol name {symbol!r}")
    return symbol


def check_local_files(batch_dir: Path, manifest: Manifest) -> None:
    """Every manifest file exists and still has the row count the manifest recorded."""
    for symbol, stats in manifest["symbols"].items():
        rows = len(
            json.loads((batch_dir / f"{check_symbol(symbol)}.json").read_text(encoding="utf-8"))
        )
        if rows != stats["rows"]:
            raise LoadError(f"{symbol}.json has {rows} rows, manifest says {stats['rows']}")


def put_sql(batch_dir: Path) -> str:
    """Upload every file of the batch (manifest included, for audit) to its stage directory."""
    if not is_batch_id(batch_dir.name):
        raise LoadError(f"Not a batch directory: {batch_dir}")
    # PUT accepts no bind variables; quoted file URI with forward slashes, as the connector does.
    uri = batch_dir.as_posix().replace("\\", "\\\\").replace("'", "\\'")
    return (
        f"PUT 'file://{uri}/*' @{STAGE}/{batch_dir.name}/ "
        "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"  # a staged file is never replaced
    )


def copy_sql(batch_id: str, symbols: Sequence[str]) -> str:
    """COPY exactly the manifest's files of one batch, with lineage columns from file metadata."""
    if not is_batch_id(batch_id):
        raise LoadError(f"Invalid batch id {batch_id!r}")
    files = ", ".join(f"'{check_symbol(symbol)}.json.gz'" for symbol in sorted(symbols))
    return f"""
COPY INTO {TABLE}
    (symbol, payload, _batch_id, _source_file, _file_row_number, _file_content_key, _loaded_at)
FROM (
    SELECT
        SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '.', 1),
        $1,
        SPLIT_PART(METADATA$FILENAME, '/', -2),
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        CONVERT_TIMEZONE('UTC', METADATA$START_SCAN_TIME)
    FROM @{STAGE}/{batch_id}/
)
FILES = ({files})
FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
ON_ERROR = ABORT_STATEMENT
"""


BATCH_STATS_SQL = f"""
SELECT
    symbol,
    COUNT(*) AS row_count,
    COUNT(*) - COUNT(DISTINCT payload[0]::NUMBER(38, 0)) AS duplicate_count
FROM {TABLE}
WHERE _batch_id = %s
GROUP BY symbol
"""


def check_put_results(rows: list[dict[str, Any]], manifest: Manifest) -> None:
    """Every manifest file reached the stage (uploaded now or already there)."""
    bad = [r for r in rows if r["status"] not in PUT_OK_STATUSES]
    if bad:
        raise LoadError(f"PUT failed: {[(r['source'], r['status'], r['message']) for r in bad]}")
    staged = {r["target"] for r in rows}
    missing = {f"{symbol}.json.gz" for symbol in manifest["symbols"]} - staged
    if missing:
        raise LoadError(f"Not staged: {sorted(missing)}")


def check_copy_results(rows: list[dict[str, Any]], manifest: Manifest) -> None:
    """COPY loaded every manifest file completely; anything else stops the run."""
    if "file" not in (rows[0] if rows else {}):
        # e.g. "Copy executed with 0 files processed." (COPY without FILES; not seen live:
        # with FILES, COPY answers per file, see LOAD_SKIPPED below).
        raise LoadError(f"COPY loaded nothing: {rows}")
    loaded = {r["file"].rsplit("/", 1)[-1]: r for r in rows}
    for symbol, stats in manifest["symbols"].items():
        row = loaded.get(f"{symbol}.json.gz")
        if row is None:
            raise LoadError(f"COPY did not process {symbol}.json.gz")
        if row["status"] == "LOAD_SKIPPED":
            # Live answer: "File was loaded before." The load metadata says the file is in the
            # table, yet the batch is not (rows deleted?): a human decides between TRUNCATE
            # (clears the load metadata) and FORCE.
            raise LoadError(f"{symbol}: COPY skipped it as loaded before ({row['first_error']})")
        if row["status"] != "LOADED" or row["rows_loaded"] != stats["rows"]:
            raise LoadError(
                f"{symbol}: status {row['status']}, rows_loaded {row['rows_loaded']}, "
                f"expected {stats['rows']}"
            )


def reconcile(stats: list[dict[str, Any]], manifest: Manifest) -> list[str]:
    """Compare the batch in the table with its manifest; return problems (empty = OK)."""
    in_table = {r["symbol"]: r for r in stats}
    problems = [f"{s}: not in table" for s in sorted(manifest["symbols"].keys() - in_table.keys())]
    problems += [f"{s}: not in manifest" for s in sorted(in_table.keys() - manifest["symbols"])]
    for symbol in sorted(manifest["symbols"].keys() & in_table.keys()):
        expected, actual = manifest["symbols"][symbol], in_table[symbol]
        if actual["row_count"] != expected["rows"]:
            problems.append(f"{symbol}: {actual['row_count']} rows, manifest {expected['rows']}")
        if actual["duplicate_count"] != expected["duplicates"]:
            problems.append(
                f"{symbol}: {actual['duplicate_count']} duplicate open_time, "
                f"manifest {expected['duplicates']}"
            )
    return problems


def load_batch(cur: Cursor, batch_dir: Path, manifest: Manifest) -> list[str]:
    """Load the batch unless it is already in the table; return reconciliation problems."""
    batch_id = manifest["batch_id"]
    check_local_files(batch_dir, manifest)

    stats = query(cur, BATCH_STATS_SQL, (batch_id,))
    if stats:
        print(f"Batch {batch_id} is already in {TABLE}: skipping PUT and COPY")
    else:
        check_put_results(query(cur, put_sql(batch_dir)), manifest)
        check_copy_results(query(cur, copy_sql(batch_id, list(manifest["symbols"]))), manifest)
        print(f"Batch {batch_id}: {len(manifest['symbols'])} files loaded into {TABLE}")
        stats = query(cur, BATCH_STATS_SQL, (batch_id,))
    return reconcile(stats, manifest)


def ensure_raw_objects(conn: SnowflakeConnection) -> None:
    """Create the stage, file format and table if missing (snowflake/raw_klines.sql)."""
    conn.execute_string(DDL_PATH.read_text(encoding="utf-8"), return_cursors=False)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load a klines batch into Snowflake RAW.")
    parser.add_argument("--batch", help="batch id (default: the latest complete batch)")
    args = parser.parse_args(argv)

    batch_dir = OUTPUT_DIR / args.batch if args.batch else latest_complete_batch(OUTPUT_DIR)
    manifest = read_manifest(batch_dir)
    with connect() as conn:
        ensure_raw_objects(conn)
        with conn.cursor(DictCursor) as cur:
            problems = load_batch(cur, batch_dir, manifest)

    expected_rows = sum(s["rows"] for s in manifest["symbols"].values())
    if problems:
        print(f"\nFAIL: batch {manifest['batch_id']} does not reconcile with its manifest:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    print(
        f"\nOK: batch {manifest['batch_id']} reconciles: {len(manifest['symbols'])} symbols, "
        f"{expected_rows} rows, duplicates as in the manifest"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
