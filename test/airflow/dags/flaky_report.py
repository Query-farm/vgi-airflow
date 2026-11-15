"""Fixture DAG: a report whose ``render`` task always fails after one retry."""

from __future__ import annotations

import datetime as dt

try:  # Airflow 3
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG  # type: ignore[no-redef]
    from airflow.decorators import task  # type: ignore[no-redef]

with DAG(
    dag_id="flaky_report",
    description="Weekly report with a task that always fails",
    schedule=None,
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    catchup=False,
    tags=["reports"],
    default_args={"retries": 1, "retry_delay": dt.timedelta(seconds=1)},
) as dag:

    @task
    def gather() -> int:
        print("gathered 42 rows")
        return 42

    @task
    def render(rows: int) -> None:
        print(f"rendering {rows} rows")
        raise ValueError("template 'weekly.html' not found")

    @task
    def publish() -> None:
        print("never runs")

    render(gather()) >> publish()
