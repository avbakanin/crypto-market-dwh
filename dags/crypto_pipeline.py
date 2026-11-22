"""The pipeline: Binance -> landing zone -> Snowflake RAW -> dbt, with its DQ checks.

check_environment -> extract x3 (in parallel) -> load_raw -> source_freshness -> dbt_build -> dq_log

* The extracts work from their high-water marks, not from the run's logical date: a run
  after any downtime catches up by itself (open interest within its 30-day window), so
  there is no catchup and no backfill.
* An extract with nothing new writes no batch and exits 99, which BashOperator reports as
  skipped. load_raw runs when at least one extract wrote a batch and none failed; when all
  three are skipped, everything downstream is skipped too and the warehouse stays asleep.
* Failures of dbt are findings about the data, not transient errors: no retries there.
  The DQ log is reconciled in a second dbt call, after the hook of the build has written it.

Trigger it by hand with "Drain" (the run goes, the DAG stays paused) before unpausing it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule

PROJECT_HOME = "/opt/project"
# The project's own venv, not Airflow's: see the Dockerfile.
PROJECT_BIN = "/opt/project-venv/bin"
# At :07 the last hour's candle is closed and the open interest point and funding
# settlement of the hour are published (lags of about a minute; 5 minutes assumed).
# Every 3 hours: completeness does not depend on the cadence, the warehouse bill does.
SCHEDULE = "7 */3 * * *"
SKIPPED = 99  # BashOperator's default skip_on_exit_code
EXTRACTS = ("klines", "funding", "open_interest")


def project_task(task_id: str, command: str, **kwargs: Any) -> BashOperator:
    """A command of the project venv, run from the project root."""
    kwargs.setdefault("skip_on_exit_code", None)  # only the extracts may skip
    return BashOperator(
        task_id=task_id,
        bash_command=f"{PROJECT_BIN}/{command}",
        cwd=PROJECT_HOME,
        **kwargs,
    )


with DAG(
    dag_id="crypto_pipeline",
    description="Extract, load, transform and check the crypto market data",
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,  # idempotent steps still must not race each other
    dagrun_timeout=timedelta(hours=2),
    is_paused_upon_creation=True,
    tags=["pipeline"],
    doc_md=__doc__,
    default_args={
        "retries": 0,
        "do_xcom_push": False,  # no task needs its output in the metadata database
        "execution_timeout": timedelta(minutes=20),
    },
):
    check_environment = project_task(
        "check_environment",
        "python -m ingestion.check_environment",
        execution_timeout=timedelta(minutes=2),
    )
    # Retries on top of the client's own (5 attempts, seconds apart): outages of minutes.
    extracts = [
        project_task(
            f"extract_{name}",
            f"python -m ingestion.extract_{name} --nothing-new-exit-code {SKIPPED}",
            skip_on_exit_code=SKIPPED,
            retries=2,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
        )
        for name in EXTRACTS
    ]
    # Idempotent (its own guard + COPY load metadata): one retry for a lost connection.
    load_raw = project_task(
        "load_raw",
        "python -m ingestion.load_raw",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    # The default selector (dbt/selectors.yml) matches no source: without --select,
    # "dbt source freshness" checks nothing and still exits 0.
    source_freshness = project_task(
        "source_freshness", "dbt source freshness --select source:binance"
    )
    dbt_build = project_task("dbt_build", "dbt build")
    dq_log = project_task("dq_log", "dbt test --select tag:dq_log")

    check_environment >> extracts >> load_raw >> source_freshness >> dbt_build >> dq_log
