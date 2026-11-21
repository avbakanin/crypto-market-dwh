"""Load extracted batches into Snowflake RAW: PUT to the dataset's stage, COPY INTO its table.

Run from repo root: python -m ingestion.load_raw [--dataset NAME] [--batch BATCH_ID]
Default: every dataset (klines, funding_rate, open_interest) in one session, so one run
resumes the warehouse once; per dataset, every complete batch that is not in RAW yet, oldest
first (the load watermark is the set of _batch_id values in its table). --batch loads one
batch of one --dataset.

Idempotent: a batch already in the table is not loaded again; COPY's load metadata skips
files it has loaded before. Exit code 1 if a loaded batch does not reconcile with its
manifest (row counts per symbol, duplicate keys); later batches of that dataset wait for a
human, the other datasets still load.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from snowflake.connector import DictCursor, SnowflakeConnection

from ingestion import extract_funding, extract_klines, extract_open_interest
from ingestion.batches import Manifest, complete_batches, is_batch_id, read_manifest
from ingestion.clients.snowflake_conn import connect

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATHS = (
    PROJECT_ROOT / "snowflake" / "raw_klines.sql",
    PROJECT_ROOT / "snowflake" / "raw_futures.sql",
)
SYMBOL_PATTERN = re.compile(r"[A-Z0-9]+")

PUT_OK_STATUSES = frozenset({"UPLOADED", "SKIPPED"})  # SKIPPED = same file already staged


@dataclass(frozen=True)
class RawDataset:
    """What the loader needs to know about a dataset: the loading itself is the same for all."""

    name: str
    landing_dir: Path
    stage: str
    table: str
    file_format: str
    # Grid key of a row, for duplicate counts: must match the extract's check, which the
    # manifest's `duplicates` comes from.
    key_sql: str


KLINES = RawDataset(
    name="klines",
    landing_dir=extract_klines.OUTPUT_DIR,
    stage="RAW.KLINES_STAGE",
    table="RAW.BINANCE_SPOT_KLINES_1H",
    file_format="RAW.KLINES_JSON_FORMAT",
    key_sql="payload[0]::NUMBER(38, 0)",  # open_time
)
FUNDING_RATE = RawDataset(
    name="funding_rate",
    landing_dir=extract_funding.OUTPUT_DIR,
    stage="RAW.FUNDING_RATE_STAGE",
    table="RAW.BINANCE_FUTURES_FUNDING_RATE",
    file_format="RAW.JSON_ARRAY_FORMAT",
    key_sql="FLOOR(payload:fundingTime::NUMBER(38, 0) / 3600000)",  # the hour: ms jitter
)
OPEN_INTEREST = RawDataset(
    name="open_interest",
    landing_dir=extract_open_interest.OUTPUT_DIR,
    stage="RAW.OPEN_INTEREST_STAGE",
    table="RAW.BINANCE_FUTURES_OPEN_INTEREST_1H",
    file_format="RAW.JSON_ARRAY_FORMAT",
    key_sql="payload:timestamp::NUMBER(38, 0)",
)
DATASETS = (KLINES, FUNDING_RATE, OPEN_INTEREST)  # loaded in this order, in one session


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


def put_sql(dataset: RawDataset, batch_dir: Path) -> str:
    """Upload every file of the batch (manifest included, for audit) to its stage directory."""
    if not is_batch_id(batch_dir.name):
        raise LoadError(f"Not a batch directory: {batch_dir}")
    # PUT accepts no bind variables; quoted file URI with forward slashes, as the connector does.
    uri = batch_dir.as_posix().replace("\\", "\\\\").replace("'", "\\'")
    return (
        f"PUT 'file://{uri}/*' @{dataset.stage}/{batch_dir.name}/ "
        "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"  # a staged file is never replaced
    )


def copy_sql(dataset: RawDataset, batch_id: str, symbols: Sequence[str]) -> str:
    """COPY exactly the manifest's files of one batch, with lineage columns from file metadata."""
    if not is_batch_id(batch_id):
        raise LoadError(f"Invalid batch id {batch_id!r}")
    files = ", ".join(f"'{check_symbol(symbol)}.json.gz'" for symbol in sorted(symbols))
    return f"""
COPY INTO {dataset.table}
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
    FROM @{dataset.stage}/{batch_id}/
)
FILES = ({files})
FILE_FORMAT = (FORMAT_NAME = '{dataset.file_format}')
ON_ERROR = ABORT_STATEMENT
"""


def batch_stats_sql(dataset: RawDataset) -> str:
    """Rows and duplicate keys per symbol of one batch (bound: the batch id)."""
    return f"""
SELECT
    symbol,
    COUNT(*) AS row_count,
    COUNT(*) - COUNT(DISTINCT {dataset.key_sql}) AS duplicate_count
FROM {dataset.table}
WHERE _batch_id = %s
GROUP BY symbol
"""


def loaded_batches_sql(dataset: RawDataset) -> str:
    return f"SELECT DISTINCT _batch_id FROM {dataset.table}"


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
                f"{symbol}: {actual['duplicate_count']} duplicate keys, "
                f"manifest {expected['duplicates']}"
            )
    return problems


def load_batch(cur: Cursor, dataset: RawDataset, batch_dir: Path, manifest: Manifest) -> list[str]:
    """Load the batch unless it is already in the table; return reconciliation problems."""
    batch_id = manifest["batch_id"]
    check_local_files(batch_dir, manifest)

    stats = query(cur, batch_stats_sql(dataset), (batch_id,))
    if stats:
        print(f"Batch {batch_id} is already in {dataset.table}: skipping PUT and COPY")
    else:
        check_put_results(query(cur, put_sql(dataset, batch_dir)), manifest)
        check_copy_results(
            query(cur, copy_sql(dataset, batch_id, list(manifest["symbols"]))), manifest
        )
        print(f"Batch {batch_id}: {len(manifest['symbols'])} files loaded into {dataset.table}")
        stats = query(cur, batch_stats_sql(dataset), (batch_id,))
    return reconcile(stats, manifest)


def pending_batches(root: Path, loaded: set[str]) -> list[Path]:
    """Complete batches that are not in RAW yet, oldest first: the order they were extracted in."""
    return [d for d in complete_batches(root) if d.name not in loaded]


def report_reconciled(manifest: Manifest) -> None:
    rows = sum(s["rows"] for s in manifest["symbols"].values())
    print(
        f"Batch {manifest['batch_id']} reconciles: {len(manifest['symbols'])} symbols, "
        f"{rows} rows, duplicates as in the manifest"
    )


def load_pending(cur: Cursor, dataset: RawDataset) -> tuple[list[str], list[str]]:
    """Load every pending batch of a dataset, oldest first; stop at the first one that does
    not reconcile.

    Returns the ids of the batches loaded and reconciled by this call, and the problems of
    the batch it stopped at, each prefixed with the batch id (empty = all reconciled).
    """
    in_raw = {r["_batch_id"] for r in query(cur, loaded_batches_sql(dataset))}
    pending = pending_batches(dataset.landing_dir, in_raw)
    if not pending:
        print(f"Every complete batch in {dataset.landing_dir} is in {dataset.table}")
        return [], []
    print(
        f"Batches not in {dataset.table} yet: {len(pending)} ({', '.join(d.name for d in pending)})"
    )
    loaded: list[str] = []
    for batch_dir in pending:
        manifest = read_manifest(batch_dir)
        problems = load_batch(cur, dataset, batch_dir, manifest)
        if problems:
            return loaded, [f"{batch_dir.name}: {problem}" for problem in problems]
        report_reconciled(manifest)
        loaded.append(batch_dir.name)
    return loaded, []


def ensure_raw_objects(conn: SnowflakeConnection) -> None:
    """Create the stages, file formats and tables if missing (snowflake/raw_*.sql)."""
    for path in DDL_PATHS:
        conn.execute_string(path.read_text(encoding="utf-8"), return_cursors=False)


def main(argv: Sequence[str] | None = None) -> int:
    by_name = {d.name: d for d in DATASETS}
    parser = argparse.ArgumentParser(description="Load extracted batches into Snowflake RAW.")
    parser.add_argument("--dataset", choices=sorted(by_name), help="default: every dataset")
    parser.add_argument("--batch", help="load only this batch id of --dataset")
    args = parser.parse_args(argv)
    if args.batch and not args.dataset:
        parser.error("--batch needs --dataset")

    datasets = [by_name[args.dataset]] if args.dataset else list(DATASETS)
    # Fail before connecting: a bad --batch, or nothing extracted at all.
    one = read_manifest(datasets[0].landing_dir / args.batch) if args.batch else None
    with_batches = [d for d in datasets if one is not None or complete_batches(d.landing_dir)]
    for dataset in datasets:
        if dataset not in with_batches:
            print(f"{dataset.name}: no complete batch in {dataset.landing_dir}, skipped")
    if not with_batches:
        print("No complete batch to load: run the extracts first (python -m ingestion.extract_*)")
        return 1

    outcomes: list[str] = []
    problems: list[str] = []
    with connect() as conn:  # one session for every dataset: one warehouse resume
        ensure_raw_objects(conn)
        with conn.cursor(DictCursor) as cur:
            for dataset in with_batches:
                print(f"\n== {dataset.name} ==")
                if one is None:
                    loaded, failed = load_pending(cur, dataset)
                    outcome = (
                        f"loaded and reconciled {', '.join(loaded)}"
                        if loaded
                        else "nothing to load"
                    )
                else:
                    batch_dir = dataset.landing_dir / one["batch_id"]
                    failed = [
                        f"{one['batch_id']}: {p}" for p in load_batch(cur, dataset, batch_dir, one)
                    ]
                    if not failed:
                        report_reconciled(one)
                    outcome = f"batch {one['batch_id']} reconciles with its manifest"
                if failed:
                    problems += [f"{dataset.name}: {p}" for p in failed]
                else:
                    outcomes.append(f"{dataset.name} {outcome}")

    if problems:
        print("\nFAIL: a batch does not reconcile with its manifest; later batches of its")
        print("dataset are not loaded:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    print(f"\nOK: {'; '.join(outcomes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
