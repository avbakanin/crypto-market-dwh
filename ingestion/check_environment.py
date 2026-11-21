"""Check that this process has what the pipeline needs, without contacting Snowflake or Binance.

Run from repo root: python -m ingestion.check_environment
Exit code 1 on any problem. All checks are local and read nothing secret aloud:

- the Snowflake variables of the loader and of dbt are set (names are printed, values never);
- each private key file exists and, on POSIX, only its owner can access it (the Snowflake
  connector refuses a key file that group or others can write; see docker/with-private-keys.sh);
- the landing zone of every dataset exists, is writable and has complete batches.

In Airflow it runs first: a broken environment fails here, before a task wakes the warehouse.
"""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from dotenv import load_dotenv

from ingestion.batches import high_water_marks
from ingestion.clients import snowflake_conn
from ingestion.extract_klines import ms_to_utc
from ingestion.load_raw import DATASETS

DBT_VARS = (
    "DBT_PROJECT_DIR",
    "DBT_PROFILES_DIR",
    "SNOWFLAKE_DBT_PRIVATE_KEY_PATH",
    "DBT_ENV_SECRET_SNOWFLAKE_DBT_KEY_PASSPHRASE",
)
# The passphrases are required here: this project's keys are encrypted.
REQUIRED_VARS = (*snowflake_conn.REQUIRED_VARS, snowflake_conn.PASSPHRASE_VAR, *DBT_VARS)
KEY_PATH_VARS = ("SNOWFLAKE_PRIVATE_KEY_PATH", "SNOWFLAKE_DBT_PRIVATE_KEY_PATH")


def missing_variables(env: Mapping[str, str], names: Sequence[str]) -> list[str]:
    """Names that are unset or empty, in the given order: an empty value counts as missing."""
    return [name for name in names if not env.get(name)]


def key_permission_problem(mode: int, owner: int, uid: int) -> str | None:
    """Why a key file with this st_mode and owner is not private to process `uid`, or None."""
    if owner != uid:
        return f"owned by uid {owner}, not by this process (uid {uid})"
    if mode & 0o077:
        return f"mode {stat.S_IMODE(mode):04o}: group or others have access"
    return None


def key_file_problem(path: Path) -> str | None:
    """Why the key file cannot be used, or None. Permissions are POSIX-only (Windows has ACLs)."""
    if not path.is_file():
        return "file not found"
    if os.name != "posix":
        return None
    info = path.stat()
    return key_permission_problem(info.st_mode, info.st_uid, os.getuid())


def landing_zone_problem(root: Path) -> str | None:
    """Why a dataset's landing zone cannot serve as the extract's watermark, or None."""
    if not root.is_dir():
        return "directory not found"
    if not os.access(root, os.W_OK):
        return "directory not writable"
    if not high_water_marks(root):
        return "no complete batch"
    return None


def main(env: Mapping[str, str] | None = None) -> int:
    if env is None:
        load_dotenv(snowflake_conn.ENV_PATH)  # as connect() does; never overrides the environment
        env = os.environ
    problems: list[str] = []
    print(f"Python {sys.version.split()[0]}: {sys.executable}")

    missing = missing_variables(env, REQUIRED_VARS)
    print(f"Variables: {len(REQUIRED_VARS) - len(missing)} of {len(REQUIRED_VARS)} set")
    problems += [f"variable {name} is not set" for name in missing]

    for var in KEY_PATH_VARS:
        if var in missing:
            continue
        problem = key_file_problem(Path(env[var]).expanduser())
        print(f"Key {var}: {problem or 'ok'}")
        if problem:
            problems.append(f"key {var}: {problem}")

    for dataset in DATASETS:
        problem = landing_zone_problem(dataset.landing_dir)
        if problem:
            print(f"Landing zone {dataset.name}: {problem}")
            problems.append(f"landing zone {dataset.name} ({dataset.landing_dir}): {problem}")
            continue
        marks = high_water_marks(dataset.landing_dir)
        print(
            f"Landing zone {dataset.name}: {len(marks)} symbols, "
            f"newest {ms_to_utc(max(marks.values())):%Y-%m-%d %H:%M} UTC"
        )

    if problems:
        print("\nFAIL:\n" + "\n".join(f"  {p}" for p in problems))
        return 1
    print("\nOK: environment is ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
