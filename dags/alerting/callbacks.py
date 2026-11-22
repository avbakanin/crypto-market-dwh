"""The notifiers of the pipeline DAG.

Where they run (Airflow 3.4, LocalExecutor):
* A task's callback runs in the task's own process, in the scheduler container, right
  after the task: dbt/target and the task log are there.
* The DAG's callbacks, and the callback of a task that ended from outside (heartbeat
  timeout, killed process), run in the DAG processor: no dbt artifacts there, so every
  message must make sense without them.
A callback that raises is only logged by Airflow: a lost alert is silent.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk import BaseNotifier, Context, Variable

from alerting import dbt_artifacts
from alerting.channels import send_telegram

LOG_ROOT = Path("/opt/airflow/logs")
LOG_TAIL_LINES = 8
# A task's output reaches its log file through the supervisor process; give it a moment.
LOG_WAIT_ATTEMPTS = 5
LOG_WAIT_S = 0.2
WARNING_STATE_VARIABLE = "crypto_pipeline_dq_warnings"

_OUTPUT_LOGGER = "SubprocessHook"
_COMMAND_EXITED = "Command exited with return code"
_NOT_OUTPUT = ("Tmp dir root location", "Running command", "Output:", _COMMAND_EXITED)


def _attempt(ti: Any) -> str:
    max_tries = getattr(ti, "max_tries", None)
    tries = f"/{max_tries + 1}" if isinstance(max_tries, int) else ""
    return f"try {getattr(ti, 'try_number', '?')}{tries}"


def log_path(ti: Any, root: Path | None = None) -> Path:
    """The task log of this attempt (Airflow's default log_filename_template)."""
    return (
        (root or LOG_ROOT)
        / f"dag_id={ti.dag_id}"
        / f"run_id={ti.run_id}"
        / f"task_id={ti.task_id}"
        / f"attempt={ti.try_number}.log"
    )


def output_tail(path: Path, lines: int = LOG_TAIL_LINES) -> list[str]:
    """The last lines the command printed, read from the JSON lines of the task log.

    Waits briefly for the "Command exited" line: the log file is written by the
    supervisor process, a moment behind the task process that runs this callback.
    """
    output: list[str] = []
    for attempt in range(LOG_WAIT_ATTEMPTS):
        output, exited = [], False
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            raw = []
        for line in raw:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not str(record.get("logger", "")).endswith(_OUTPUT_LOGGER):
                continue
            event = str(record.get("event", ""))
            if event.startswith(_COMMAND_EXITED):
                exited = True
            if not event.startswith(_NOT_OUTPUT):
                output.append(event)
        if exited or attempt == LOG_WAIT_ATTEMPTS - 1:
            break
        time.sleep(LOG_WAIT_S)
    return [dbt_artifacts.first_line(line) for line in output[-lines:]]


def task_failure_text(context: Context) -> str:
    """Which task failed, why, what dbt found (if it ran) and where the log is."""
    ti = context["ti"]
    lines = [f"❌ {ti.dag_id}: {ti.task_id} failed", f"run {ti.run_id}, {_attempt(ti)}"]
    exception = context.get("exception")
    if exception is not None:
        lines.append(dbt_artifacts.first_line(str(exception)) or type(exception).__name__)
    start = getattr(ti, "start_date", None)
    artifacts = (
        dbt_artifacts.read_fresh_artifacts(dbt_artifacts.DBT_TARGET, start)
        if isinstance(start, datetime) and start.utcoffset() is not None
        else {}
    )
    for name, artifact in artifacts.items():
        lines.append("")
        lines.append(f"{dbt_artifacts.command_label(name, artifact)}:")
        lines.extend(dbt_artifacts.summarize(artifact) or ["(no failed node)"])
    if not artifacts:
        tail = output_tail(log_path(ti))
        if tail:
            lines.append("")
            lines.append("last output:")
            lines.extend(tail)
    log_url = getattr(ti, "log_url", None)
    if log_url:
        lines.append("")
        lines.append(f"log: {log_url}")
    return "\n".join(lines)


def run_failure_text(context: Context) -> str | None:
    """A run that failed without a failed task of its own: timed out, deadlocked.

    A task failure is reported by the task's own callback, with its details; the run's
    callback would only repeat it.
    """
    reason = context.get("reason")
    if reason == "task_failure":
        return None
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", "?")
    lines = [f"❌ {dag_id}: run {reason}", f"run {context.get('run_id')}"]
    ti = context.get("ti")
    if ti is not None:
        lines.append(f"last task: {ti.task_id}")
    return "\n".join(lines)


class TelegramNotifier(BaseNotifier):
    """Sends the text that `compose(context)` builds; nothing when it returns None."""

    def __init__(self, compose: Callable[[Context], str | None]):
        super().__init__()
        self.compose = compose

    def notify(self, context: Context) -> None:
        text = self.compose(context)
        if text is not None:
            send_telegram(text)


class WarningDigestNotifier(BaseNotifier):
    """After a successful build: a message when a warn-level check finds more rows than
    in the previous build (new checks included), so steady warnings do not repeat.

    The counts of the previous build live in an Airflow Variable (the metadata database)
    and move on only after the message is sent: a lost message is retried next build.
    """

    def notify(self, context: Context) -> None:
        ti = context["ti"]
        artifacts = dbt_artifacts.read_fresh_artifacts(dbt_artifacts.DBT_TARGET, ti.start_date)
        run_results = artifacts.get(dbt_artifacts.RUN_RESULTS)
        if run_results is None:
            self.log.warning("No run_results.json from this task: no warning digest")
            return
        current = dbt_artifacts.warning_counts(run_results)
        counts = {name: failures for name, (failures, _) in current.items()}
        previous = Variable.get(WARNING_STATE_VARIABLE, default=None, deserialize_json=True)
        text = warning_digest_text(ti, current, previous)
        if text is not None:
            send_telegram(text)
        if counts != previous:
            Variable.set(WARNING_STATE_VARIABLE, counts, serialize_json=True)


def warning_digest_text(
    ti: Any,
    current: dict[str, tuple[int, str | None]],
    previous: dict[str, int] | None,
) -> str | None:
    counts = {name: failures for name, (failures, _) in current.items()}
    if previous is None:
        if not counts:
            return None
        header = f"⚠️ {ti.dag_id}: DQ warnings now (first digest, alerts on growth from here)"
        grown, resolved = sorted(counts), []
    else:
        grown, resolved = dbt_artifacts.warning_changes(previous, counts)
        if not grown:
            return None
        header = f"⚠️ {ti.dag_id}: DQ warnings grew"
    lines = [header, f"run {ti.run_id}, {ti.task_id}"]
    for name in grown:
        failures, relation = current[name]
        was = (previous or {}).get(name)
        change = f"{was} -> {failures}" if was is not None else f"{failures}"
        lines.append(f"WARN {name}: {change} rows")
        if relation:
            lines.append(f"  select * from {relation}")
    for name in resolved:
        lines.append(f"resolved: {name} (was {(previous or {})[name]})")
    return "\n".join(lines)


task_failure_alert = TelegramNotifier(task_failure_text)
run_failure_alert = TelegramNotifier(run_failure_text)
warning_digest = WarningDigestNotifier()
