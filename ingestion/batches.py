"""Landing-zone contract between extract and load: one immutable directory per extract run.

    data/raw/klines_1h/<batch_id>/<SYMBOL>.json   klines exactly as received
    data/raw/klines_1h/<batch_id>/_manifest.json  written last: marks the batch complete

A batch is written once and never changed. Snowflake's COPY INTO skips files it has
already loaded only while they stay unchanged, so overwriting <SYMBOL>.json in place
would make the next load re-insert every overlapping candle.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

BATCH_ID_FORMAT = "%Y%m%dT%H%M%SZ"  # UTC as-of moment: sorts chronologically, safe in paths
BATCH_ID_PATTERN = re.compile(r"\d{8}T\d{6}Z")
MANIFEST_NAME = "_manifest.json"


class Manifest(TypedDict):
    batch_id: str
    as_of_utc: str
    interval: str
    symbols: dict[str, dict[str, int]]  # symbol -> check_klines stats of its file


def batch_id_for(as_of_ms: int) -> str:
    """Batch id for an extract run: its as-of moment in UTC, truncated to the second."""
    return datetime.fromtimestamp(as_of_ms // 1000, tz=UTC).strftime(BATCH_ID_FORMAT)


def is_batch_id(value: str) -> bool:
    return BATCH_ID_PATTERN.fullmatch(value) is not None


def write_manifest(batch_dir: Path, manifest: Manifest) -> None:
    (batch_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def read_manifest(batch_dir: Path) -> Manifest:
    """Read a batch's manifest; refuse one that describes a different batch."""
    manifest = cast(Manifest, json.loads((batch_dir / MANIFEST_NAME).read_text(encoding="utf-8")))
    if manifest["batch_id"] != batch_dir.name:
        raise ValueError(f"Manifest in {batch_dir.name} describes batch {manifest['batch_id']!r}")
    return manifest


def latest_complete_batch(root: Path) -> Path:
    """Newest batch directory that has a manifest; batches without one are incomplete."""
    if not root.is_dir():
        raise FileNotFoundError(f"No batch directory {root}")
    complete = [d for d in root.iterdir() if is_batch_id(d.name) and (d / MANIFEST_NAME).is_file()]
    if not complete:
        raise FileNotFoundError(f"No complete batch in {root}")
    return max(complete, key=lambda d: d.name)
