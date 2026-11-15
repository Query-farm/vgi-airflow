"""Fixture DAG: scheduled on the ``s3://warehouse/orders`` asset/dataset."""

from __future__ import annotations

import datetime as dt

try:  # Airflow 3
    from airflow.sdk import DAG, task
    from airflow.sdk import Asset as _Asset
except ImportError:  # Airflow 2
    from airflow import DAG  # type: ignore[no-redef]
    from airflow import Dataset as _Asset
    from airflow.decorators import task  # type: ignore[no-redef]

with DAG(
    dag_id="orders_consumer",
    description="Refreshes the orders dashboard whenever new orders land",
    schedule=[_Asset("s3://warehouse/orders")],
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    catchup=False,
    tags=["dashboards"],
) as dag:

    @task
    def refresh_dashboard() -> str:
        print("dashboard refreshed")
        return "ok"

    refresh_dashboard()
