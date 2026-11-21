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
    # symbol -> stats of its file: DQ counts (rows, duplicates, gaps, expected) plus the
    # newest record time in the file: last_ms (klines: last_open_ms; absent before Week 2)
    symbols: dict[str, dict[str, int]]


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


def complete_batches(root: Path) -> list[Path]:
    """Batch directories that have a manifest, oldest first; batches without one are incomplete."""
    if not root.is_dir():
        return []
    complete = [d for d in root.iterdir() if is_batch_id(d.name) and (d / MANIFEST_NAME).is_file()]
    return sorted(complete, key=lambda d: d.name)


def latest_complete_batch(root: Path) -> Path:
    """Newest batch directory that has a manifest."""
    if not root.is_dir():
        raise FileNotFoundError(f"No batch directory {root}")
    complete = complete_batches(root)
    if not complete:
        raise FileNotFoundError(f"No complete batch in {root}")
    return complete[-1]


LAST_TIME_KEYS = ("last_ms", "last_open_ms")  # any dataset; klines manifests of Week 2


def last_time_ms(batch_dir: Path, symbol: str, stats: dict[str, int]) -> int | None:
    """Newest record time in a batch file: from the manifest, else (klines manifests written
    before Week 2) the newest open_time in the file."""
    for key in LAST_TIME_KEYS:
        if key in stats:
            return stats[key]
    # The batch is immutable, so an old manifest is never rewritten: read its file instead.
    klines = json.loads((batch_dir / f"{symbol}.json").read_text(encoding="utf-8"))
    return max((k[0] for k in klines), default=None)


def high_water_marks(root: Path) -> dict[str, int]:
    """Extract watermark: newest record time already extracted, per symbol, over complete batches.

    It answers "what has been extracted", not "what has been loaded": the loader closes that
    gap by loading every complete batch that is not in RAW yet. No batches -> no marks.
    """
    marks: dict[str, int] = {}
    for batch_dir in complete_batches(root):
        for symbol, stats in read_manifest(batch_dir)["symbols"].items():
            last = last_time_ms(batch_dir, symbol, stats)
            if last is not None:
                marks[symbol] = max(marks.get(symbol, last), last)
    return marks
