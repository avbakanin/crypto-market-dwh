"""Airflow puts its DAGs folder on sys.path; do the same so that the DAGs (and the tests)
can import the shared code in dags/alerting."""

from __future__ import annotations

import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
