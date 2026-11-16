"""Discovery metadata: catalog, schemas, tables and functions.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every table and function carries ``object_tags(...)``: a title, a
category from its schema's category list, keywords, and LLM- and human-facing
docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "airflow"

SOURCE_URL = "https://github.com/Query-farm/vgi-airflow"
AIRFLOW_API_DOCS = "https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html"

MAIN_CATEGORIES = [
    {"name": "dags", "title": "DAGs and runs", "description": "DAG definitions and their runs."},
    {"name": "tasks", "title": "Tasks", "description": "Task instances, their attempts, logs and XComs."},
    {
        "name": "assets",
        "title": "Airflow Assets and Datasets",
        "description": "Assets (Airflow 2: datasets) and their update events.",
    },
    {"name": "config", "title": "Configuration", "description": "Pools, variables and connections (secrets masked)."},
    {"name": "audit", "title": "Audit and health", "description": "The audit log and DAG import errors."},
]
UNIFIED_CATEGORIES = [
    {"name": "runs", "title": "Cross-orchestrator runs", "description": "Runs and task runs in a shared shape."},
]
_ALL_CATEGORIES = {c["name"] for c in MAIN_CATEGORIES + UNIFIED_CATEGORIES}


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in _ALL_CATEGORIES:
        raise ValueError(f"unknown category {category!r}")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CREDENTIALS_MD = (
    "Credentials come from a DuckDB secret of type `airflow`, created **after** `ATTACH`:\n\n"
    "```sql\n"
    "-- Airflow 3 (JWT exchanged via /auth/token) or Airflow 2 (basic auth)\n"
    "CREATE SECRET af (TYPE airflow, BASE_URL 'http://localhost:8080', USERNAME 'admin', PASSWORD '...');\n"
    "-- A bearer token: Airflow 3 JWT, Astronomer deployment API token, Cloud Composer access token\n"
    "CREATE SECRET af (TYPE airflow, BASE_URL 'https://<deployment>.astronomer.run/<id>', TOKEN '...',\n"
    "                  PLATFORM 'astronomer');\n"
    "```"
)

CATALOG_DESCRIPTION = (
    "Apache Airflow's stable REST API as SQL tables: DAGs, runs, task instances and their tries, task logs, "
    "XComs, assets, pools, variables, connections, import errors and the audit log, from Airflow 2 or 3."
)

CATALOG_DOC_LLM = (
    "Query one Apache Airflow deployment (Airflow 2.x /api/v1 or 3.x /api/v2, detected automatically) "
    "through its stable REST API, self-managed or on Astronomer / Cloud Composer. Requires a DuckDB secret: "
    "CREATE SECRET (TYPE airflow, BASE_URL '...', TOKEN '...') or with USERNAME/PASSWORD, created after ATTACH. "
    "Schema `main` exposes Airflow's own model, one table per REST collection with the same columns on "
    "Airflow 2 and 3: a DAG has runs, a run has task instances, a task instance has one or more tries, each "
    "with its log; plus assets, pools, the audit log and configuration. Secrets stay in Airflow: variable "
    "values are masked unless non-secret ones are requested, and connection passwords are never returned. "
    "Schema `unified` reshapes runs and task runs into columns shared with other orchestrator workers for "
    "UNION ALL. Filters on dag_id, run_id, task_id, state and date ranges are sent to the API; always filter "
    "by dag_id and a date range on large deployments. Timestamps are `TIMESTAMPTZ` in UTC."
)

CATALOG_DOC_MD = (
    "# Apache Airflow\n\n"
    "Query an Airflow deployment's DAGs, runs, task instances, logs, assets and configuration with SQL. "
    "Works with Airflow 2.x (`/api/v1`) and 3.x (`/api/v2`), self-managed or on Astronomer / Cloud Composer.\n\n"
    f"{CREDENTIALS_MD}\n\n"
    "Filters on `dag_id`, `run_id`, `task_id`, `state` and date ranges are pushed into the API's own "
    "query parameters, so a query for one DAG's failed runs last week reads only those runs."
)

MAIN_DOC_LLM = (
    "Native Airflow resources, one table per REST collection, normalized across Airflow 2 and 3. Join keys: "
    "dag_runs(dag_id, run_id) ↔ task_instances(dag_id, run_id) ↔ task_instance_tries(dag_id, run_id, task_id, "
    "map_index). State values are Airflow's own (success, failed, upstream_failed, skipped, running, queued, ...). "
    "Pushed down to the API: dag_id (= or IN), run_id, task_id, state (= or IN), logical_date / start_date / "
    "end_date ranges, run_type, pool, asset uri, event names and audit-log time ranges. task_logs(dag_id, run_id, "
    "task_id) returns log lines; variable_values(reveal := true) shows non-secret variable values."
)

MAIN_DOC_MD = (
    "## main\n\n"
    "Airflow's REST resources as tables, with the same columns on Airflow 2 and 3. The keys follow "
    "Airflow's own model: a DAG has runs, a run has task instances, and a task instance has one or more "
    "tries, each with its log.\n\n"
    "Filter by DAG, run and date range wherever you can: those predicates become API parameters, so the "
    "worker reads only the matching runs and tasks instead of the whole history.\n\n"
    "Secrets stay in Airflow. Variable values are masked unless you ask for non-secret ones, and "
    "connection passwords are never returned."
)

UNIFIED_DOC_LLM = (
    "Cross-orchestrator tables with columns identical across the Airflow, Dagster, Prefect, Temporal, "
    "Databricks, dbt and GitHub workers, for UNION ALL. runs(orchestrator, run_id, pipeline, status, raw_status, "
    "trigger, queued_at, started_at, ended_at, duration_seconds, url) and task_runs(orchestrator, run_id, "
    "task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url). status is "
    "one of queued, running, success, failed, cancelled, skipped; Airflow's upstream_failed maps to skipped. "
    "run_id is '<dag_id>/<dag_run_id>'; pipeline is the DAG id. Filters on pipeline, run_id, status and "
    "started_at/ended_at ranges are pushed to the API."
)

UNIFIED_DOC_MD = (
    "## unified\n\n"
    "Runs and task runs in the shape every Query.Farm orchestrator worker shares, so you can "
    "`UNION ALL` Airflow with Dagster, Prefect, Temporal and others.\n\n"
    "`run_id` is `'<dag_id>/<dag_run_id>'` because Airflow run ids are only unique within a DAG, and "
    "Airflow's `upstream_failed` counts as `skipped` so each failure is counted once."
)

TABLE_DOCS: dict[str, dict[str, str]] = {
    "dags": {
        "comment": "Airflow DAGs: schedule, paused/active flags, owners, tags and next run.",
        "title": "Airflow DAG Definitions",
        "category": "dags",
        "keywords": "dag, dags, workflow, pipeline, schedule, paused, tags, owners",
        "doc_llm": (
            "One row per DAG (including inactive/stale ones; filter is_active). Pushed down: dag_id = (as an "
            "API pattern), is_paused, is_active. Columns: dag_id, is_paused, is_active, has_import_errors, schedule, "
            "owners, tags (`VARCHAR[]`), next_dagrun_logical_date, last_parsed_time and more."
        ),
        "doc_md": "Every DAG the deployment knows, with its schedule, owners, tags and next scheduled run.",
    },
    "dag_runs": {
        "comment": "Airflow DAG runs with state, run type, logical date and timings.",
        "title": "Airflow DAG Run History",
        "category": "dags",
        "keywords": "dag run, runs, execution, state, failed runs, run history, logical date, execution date",
        "doc_llm": (
            "One row per DAG run, keyed by (dag_id, run_id). state is queued, running, success or failed. Pushed "
            "down: dag_id (= or IN, as the URL path), state (= or IN), run_type and run_id (Airflow 3), and "
            "ranges on logical_date, start_date, end_date. duration_seconds = end_date - start_date. conf is JSON. "
            "Join to dags ON dag_runs.dag_id = dags.dag_id; task_instances join back ON (dag_id, run_id)."
        ),
        "doc_md": "Run history for every DAG. Filter by `dag_id`, `state` and a date range to keep reads small.",
    },
    "task_instances": {
        "comment": "Airflow task instances (latest try) with state, operator, timings and pool.",
        "title": "Airflow Task Instance States",
        "category": "tasks",
        "keywords": "task instance, tasks, task state, failed tasks, operator, pool, duration, retries",
        "doc_llm": (
            "One row per task instance (its latest try), keyed by (dag_id, run_id, task_id, map_index). Pushed "
            "down: dag_id and run_id (URL path), task_id (Airflow 3), state (= or IN), pool, and logical_date / "
            "start_date / end_date ranges. try_number is the latest attempt; see task_instance_tries for all. "
            "Join to dag_runs ON (task_instances.dag_id, task_instances.run_id) = (dag_runs.dag_id, dag_runs.run_id)."
        ),
        "doc_md": "Every task instance's current state, operator, host, pool and duration.",
    },
    "task_instance_tries": {
        "comment": "Every attempt of every Airflow task instance (retries included).",
        "title": "Airflow Task Attempts",
        "category": "tasks",
        "keywords": "task tries, attempts, retries, retry history, flaky tasks",
        "doc_llm": (
            "One row per attempt, keyed by (dag_id, run_id, task_id, map_index, try_number). Reads task instances "
            "(dag_id, run_id, task_id pushed down) and then one tries request per instance, so filter by dag_id "
            "and run_id. Use it to count retries or find flaky tasks. Join to task_instances ON "
            "(dag_id, run_id, task_id, map_index)."
        ),
        "doc_md": "Attempt-by-attempt history: what failed before a retry succeeded.",
    },
    "xcom_entries": {
        "comment": "XCom entries (keys, not values) pushed by Airflow tasks.",
        "title": "Airflow XCom Keys",
        "category": "tasks",
        "keywords": "xcom, return value, task output, cross communication",
        "doc_llm": (
            "One row per XCom entry: dag_id, run_id, task_id, map_index, key, timestamp. Values are not "
            "fetched. Pushed down: dag_id, run_id, task_id (URL path), key, map_index. On Airflow 2 run_id is "
            "resolved from the logical date. Join to task_instances ON (dag_id, run_id, task_id, map_index)."
        ),
        "doc_md": "Which tasks pushed which XCom keys, and when.",
    },
    "assets": {
        "comment": "Airflow assets (Airflow 2 datasets) with producing tasks and consuming DAGs.",
        "title": "Airflow Assets and Datasets",
        "category": "assets",
        "keywords": "asset, assets, dataset, datasets, data-aware scheduling, lineage, outlets",
        "doc_llm": (
            "One row per asset (Airflow 2: dataset): asset_id, name, uri, asset_group, extra (JSON), "
            "consuming_dags (`VARCHAR[]` of DAG ids scheduled on it), producing_tasks (`STRUCT(dag_id, task_id)[]`). "
            "Pushed down: uri =."
        ),
        "doc_md": "Assets / datasets and the DAGs and tasks around them: lightweight lineage.",
    },
    "asset_events": {
        "comment": "Asset (dataset) update events and the DAG runs they triggered.",
        "title": "Airflow Asset Update Events",
        "category": "assets",
        "keywords": "asset event, dataset event, asset update, triggered runs, lineage",
        "doc_llm": (
            "One row per asset update: event_id, asset_id, uri, timestamp, source_dag_id/task_id/run_id, extra "
            "(JSON), created_runs (`STRUCT(dag_id, run_id)[]`). Pushed down: asset_id, source_dag_id, "
            "source_task_id, source_run_id, and timestamp ranges (Airflow 3). Join to assets ON "
            "asset_events.asset_id = assets.asset_id; to the emitting run ON (source_dag_id, source_run_id) = "
            "dag_runs(dag_id, run_id)."
        ),
        "doc_md": "When each asset was updated, by which task, and which runs that triggered.",
    },
    "event_logs": {
        "comment": "The Airflow audit log: who did what to which DAG, run or task, and when.",
        "title": "Airflow Audit Log",
        "category": "audit",
        "keywords": "audit log, event log, who triggered, user actions, cli, history",
        "doc_llm": (
            "One row per audit-log entry: event_log_id, event_time, event, owner, dag_id, task_id, run_id, "
            "try_number, extra. Pushed down: dag_id, task_id, run_id, owner, event (= or IN) and event_time ranges."
        ),
        "doc_md": "The audit log: triggers, pauses, state changes and CLI commands.",
    },
    "import_errors": {
        "comment": "DAG files that currently fail to import, with their stack traces.",
        "title": "Airflow DAG Import Errors",
        "category": "audit",
        "keywords": "import error, broken dag, parse error, traceback, dag file",
        "doc_llm": "One row per DAG file that fails to import: import_error_id, timestamp, filename, stack_trace.",
        "doc_md": "Broken DAG files and why they fail to import.",
    },
    "pools": {
        "comment": "Airflow pools and their slot usage.",
        "title": "Airflow Pool Slot Usage",
        "category": "config",
        "keywords": "pool, pools, slots, concurrency, capacity",
        "doc_llm": (
            "One row per pool: name, slots (total; -1 unlimited), occupied/running/queued/scheduled/deferred/open "
            "slots. Pushed down: name = (Airflow 3)."
        ),
        "doc_md": "Pool capacity and current slot usage.",
    },
    "variables": {
        "comment": "Airflow variables with every value masked ('***').",
        "title": "Airflow Variables",
        "category": "config",
        "keywords": "variable, variables, configuration, settings",
        "doc_llm": (
            "One row per Airflow variable: key, value (always '***' when set), is_masked, description. Values are "
            "never returned here; use variable_values(reveal := true) to see non-secret values."
        ),
        "doc_md": "Variable keys and descriptions. Values are always masked; see `variable_values`.",
    },
    "connections": {
        "comment": "Airflow connections without passwords; secret-looking extra fields masked.",
        "title": "Airflow Connections",
        "category": "config",
        "keywords": "connection, connections, hooks, credentials, hosts, conn_type",
        "doc_llm": (
            "One row per connection: connection_id, conn_type, host, port, login, schema, has_password, extra "
            "(JSON with secret-looking keys replaced by '***'). Passwords are never returned. Pushed down: "
            "connection_id = (Airflow 3)."
        ),
        "doc_md": "Connection inventory: types, hosts and logins. Passwords never leave the worker.",
    },
    "runs": {
        "comment": "Airflow DAG runs in the cross-orchestrator unified.runs shape.",
        "title": "Cross-Orchestrator Runs",
        "category": "runs",
        "keywords": "unified runs, cross orchestrator, pipeline runs, run status, union all",
        "doc_llm": (
            "Airflow DAG runs as unified.runs: orchestrator='airflow', run_id='<dag_id>/<dag_run_id>', "
            "pipeline=dag_id, status (queued/running/success/failed), raw_status, trigger (schedule, manual, api, "
            "event, other), queued_at, started_at, ended_at, duration_seconds, url. Pushed down: pipeline, run_id, "
            "status, started_at/ended_at ranges."
        ),
        "doc_md": "DAG runs in the shared orchestrator shape, ready to `UNION ALL` with other workers.",
    },
    "task_runs": {
        "comment": "Airflow task instances in the cross-orchestrator unified.task_runs shape.",
        "title": "Cross-Orchestrator Task Runs",
        "category": "runs",
        "keywords": "unified task runs, cross orchestrator, task status, union all",
        "doc_llm": (
            "Airflow task instances (latest try) as unified.task_runs: run_id='<dag_id>/<dag_run_id>' (joins "
            "unified.runs), task_run_id='<dag_id>/<dag_run_id>/<task_id>[/<map_index>]', task_name=task_id, status "
            "(upstream_failed→skipped, removed→cancelled, up_for_retry→queued), raw_status, attempt=try_number. "
            "Pushed down: run_id, task_name, status, started_at/ended_at ranges."
        ),
        "doc_md": "Task instances in the shared orchestrator shape.",
    },
}

AGENT_TEST_TASKS = [
    {"name": "failed_runs", "prompt": "Using the airflow catalog, which DAG runs failed? Give the dag id and run id."},
    {
        "name": "retried_task",
        "prompt": "Using the airflow catalog, which task needed more than one attempt, and how many attempts?",
    },
    {
        "name": "runs_per_status",
        "prompt": "Using the airflow catalog's unified schema, how many runs are there per status?",
    },
    {"name": "broken_files", "prompt": "Using the airflow catalog, which DAG files fail to import?"},
    {"name": "etl_tagged_dags", "prompt": "Using the airflow catalog, which DAGs are tagged 'etl'?"},
    {
        "name": "skipped_tasks",
        "prompt": (
            "Using the airflow catalog, list the task instances that never ran because an upstream task failed "
            "(dag id, run id, task id)."
        ),
    },
    {
        "name": "render_error",
        "prompt": (
            "Using the airflow catalog, what error lines did the 'render' task log on its second attempt in run "
            "'manual_report_1' of the 'flaky_report' DAG?"
        ),
    },
    {
        "name": "xcom_keys",
        "prompt": "Using the airflow catalog, which distinct XCom keys did the 'transform' task of 'etl_orders' push?",
    },
    {
        "name": "asset_consumers",
        "prompt": (
            "Using the airflow catalog, for the asset s3://warehouse/orders: which DAG consumes it, and how many "
            "update events were recorded?"
        ),
    },
    {
        "name": "audit_events",
        "prompt": "Using the airflow catalog's audit log, how many entries mention the DAG 'flaky_report'?",
    },
    {"name": "pool_capacity", "prompt": "Using the airflow catalog, how many slots does each pool have in total?"},
    {
        "name": "config_inventory",
        "prompt": (
            "Using the airflow catalog, list the variable keys whose values are safe to reveal, with their values, "
            "and the connection ids that have a password set."
        ),
    },
    {
        "name": "unified_task_status",
        "prompt": (
            "Using the airflow catalog's unified schema, how many task runs are there per unified status for the "
            "pipeline 'flaky_report'?"
        ),
    },
    {
        "name": "normalize_state",
        "prompt": "What unified status does the airflow worker assign to the Airflow task state 'up_for_reschedule'?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Apache Airflow",
    "vgi.subtitle": "Airflow 2 and 3 DAGs, runs, tasks, logs and assets as SQL tables",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "airflow, apache airflow, dag, orchestration, workflow, task instance, astronomer, cloud composer, rest api"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
