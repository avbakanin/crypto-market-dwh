"""DAG integrity tests. They need Airflow, so they run with Airflow's interpreter in the image:

    docker run --rm --entrypoint python crypto-market-dwh:test -m pytest -q tests_airflow

(the project venv has no Airflow; Airflow's has no project dependencies).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.sdk import DAG, CronTriggerTimetable, TriggerRule

from alerting.callbacks import heartbeat, run_failure_alert, task_failure_alert, warning_digest

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


PIPELINE_EXTRACTS = {"extract_klines", "extract_funding", "extract_open_interest"}


@pytest.fixture(scope="module")
def pipeline(dagbag: DagBag) -> DAG:
    return dagbag.dags["crypto_pipeline"]


def test_pipeline_runs_every_three_hours_one_run_at_a_time(pipeline: DAG) -> None:
    assert isinstance(pipeline.timetable, CronTriggerTimetable)
    assert pipeline.schedule == "7 */3 * * *"
    assert pipeline.catchup is False
    assert pipeline.max_active_runs == 1


def test_pipeline_order(pipeline: DAG) -> None:
    upstream = {task_id: task.upstream_task_ids for task_id, task in pipeline.task_dict.items()}
    assert upstream == {
        "check_environment": set(),
        **{extract: {"check_environment"} for extract in PIPELINE_EXTRACTS},
        "load_raw": PIPELINE_EXTRACTS,
        "source_freshness": {"load_raw"},
        "dbt_build": {"source_freshness"},
        "dq_log": {"dbt_build"},
    }


def test_only_extracts_skip_and_only_on_nothing_new(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        if task_id in PIPELINE_EXTRACTS:
            assert task.bash_command.endswith("--nothing-new-exit-code 99"), task_id
            assert list(task.skip_on_exit_code) == [99], task_id
        else:
            assert list(task.skip_on_exit_code) == [], task_id


def test_load_runs_when_one_extract_wrote_a_batch_and_none_failed(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        expected = (
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            if task_id == "load_raw"
            else TriggerRule.ALL_SUCCESS
        )
        assert task.trigger_rule == expected, task_id


def test_retries_only_where_a_retry_can_help(pipeline: DAG) -> None:
    retries = {task_id: task.retries for task_id, task in pipeline.task_dict.items()}
    assert retries == {
        "check_environment": 0,
        **dict.fromkeys(PIPELINE_EXTRACTS, 2),
        "load_raw": 1,
        "source_freshness": 0,
        "dbt_build": 0,
        "dq_log": 0,
    }


def test_pipeline_tasks_have_a_timeout_and_push_no_xcom(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        assert task.execution_timeout is not None, task_id
        assert task.do_xcom_push is False, task_id


def test_dbt_commands(pipeline: DAG) -> None:
    """The build uses the default selector; freshness must select its sources itself."""
    commands = {task_id: task.bash_command for task_id, task in pipeline.task_dict.items()}
    freshness = "dbt source freshness --select source:binance"  # the selector has no source
    assert commands["source_freshness"] == f"{PROJECT_BIN}{freshness}"
    assert commands["dbt_build"] == f"{PROJECT_BIN}dbt build"
    assert commands["dq_log"] == f"{PROJECT_BIN}dbt test --select tag:dq_log"


def _callbacks(value: object) -> list[object]:
    if value is None:
        return []
    return list(value) if isinstance(value, list | tuple) else [value]


def test_every_pipeline_task_alerts_on_its_final_failure_only(pipeline: DAG) -> None:
    for task_id, task in pipeline.task_dict.items():
        assert _callbacks(task.on_failure_callback) == [task_failure_alert], task_id
        assert _callbacks(task.on_retry_callback) == [], task_id  # a retry is not news yet


def test_the_build_sends_the_warning_digest_and_no_other_task_reports_success(
    pipeline: DAG,
) -> None:
    for task_id, task in pipeline.task_dict.items():
        expected = [warning_digest] if task_id == "dbt_build" else []
        assert _callbacks(task.on_success_callback) == expected, task_id


def test_the_run_reports_timeouts_and_pings_the_watchdog_on_success(pipeline: DAG) -> None:
    assert _callbacks(pipeline.on_failure_callback) == [run_failure_alert]
    assert _callbacks(pipeline.on_success_callback) == [heartbeat]
