"""Fixture DAG: a small ETL that publishes the ``warehouse_orders`` asset/dataset."""

from __future__ import annotations

import datetime as dt

try:  # Airflow 3
    from airflow.sdk import DAG, task
    from airflow.sdk import Asset as _Asset
except ImportError:  # Airflow 2
    from airflow import DAG  # type: ignore[no-redef]
    from airflow import Dataset as _Asset
    from airflow.decorators import task  # type: ignore[no-redef]

ORDERS = _Asset("s3://warehouse/orders")

with DAG(
    dag_id="etl_orders",
    description="Extract, transform and load the daily orders file",
    schedule="@daily",
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    catchup=False,
    tags=["etl", "orders"],
    max_active_runs=1,
) as dag:

    @task
    def extract() -> list[int]:
        print("extracting 3 orders")
        return [101, 102, 103]

    @task
    def transform(order_ids: list[int]) -> dict[str, int]:
        print(f"transforming {len(order_ids)} orders")
        return {"count": len(order_ids), "total_cents": 12345}

    @task(outlets=[ORDERS])
    def load(summary: dict[str, int]) -> None:
        print(f"loaded {summary['count']} orders")

    load(transform(extract()))
