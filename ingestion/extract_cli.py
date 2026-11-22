"""Command line shared by the extracts: how a run that writes no batch ends.

Nothing new -> no batch (see batches.py). By default that is a success (exit 0) for a
person at a terminal. An orchestrator can ask for its own exit code instead: the Airflow
DAG passes 99, which BashOperator reports as a skipped task, so the load does not run
and the warehouse stays asleep.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

NOTHING_NEW_OPTION = "--nothing-new-exit-code"


def parse_extract_args(description: str, argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        NOTHING_NEW_OPTION,
        type=int,
        default=0,
        metavar="N",
        help="exit code when nothing is new and no batch is written (default 0)",
    )
    args = parser.parse_args(argv)
    if not 0 <= args.nothing_new_exit_code <= 255:  # the OS keeps only the low byte
        parser.error(f"{NOTHING_NEW_OPTION} must be between 0 and 255")
    return args


def exit_code(batch_dir: Path | None, nothing_new_exit_code: int) -> int:
    """0 when the run wrote a batch, otherwise the code asked for 'nothing new'."""
    return 0 if batch_dir is not None else nothing_new_exit_code
