"""DAG integrity tests. They need Airflow, so they run with Airflow's interpreter in the image:

    docker run --rm --entrypoint python crypto-market-dwh:test -m pytest -q tests_airflow

(the project venv has no Airflow; Airflow's has no project dependencies).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
PROJECT_BIN = "/opt/project-venv/bin/"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR)


def test_every_dag_file_imports(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}
    assert dagbag.dag_ids, "no DAGs found"


def test_every_dag_is_paused_upon_creation_and_tagged(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.is_paused_upon_creation is True, dag_id
        assert dag.tags, dag_id


def test_every_bash_task_runs_from_the_project_venv(dagbag: DagBag) -> None:
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            command = getattr(task, "bash_command", None)
            if command is not None:
                assert command.startswith(PROJECT_BIN), f"{dag.dag_id}.{task.task_id}"
                assert task.cwd == "/opt/project", f"{dag.dag_id}.{task.task_id}"


def test_smoke_environment_has_no_schedule_and_checks_before_dbt(dagbag: DagBag) -> None:
    dag = dagbag.dags["smoke_environment"]
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.task_dict["dbt_parse"].upstream_task_ids == {"check_environment"}
