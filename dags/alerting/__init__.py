"""Alerts of the pipeline DAG: Telegram messages when a task fails for good or the run
times out, and when a warn-level DQ check finds more rows than in the previous build.

* dbt_artifacts: what a dbt call reports in its own artifacts (pure, no Airflow).
* channels: sending (Telegram Bot API) with the standard library only.
* callbacks: the Airflow notifiers the DAG attaches to its tasks and to itself.

This package lives in the DAGs folder, which Airflow puts on sys.path; .airflowignore
keeps the DAG processor from parsing it as DAG files.
"""
