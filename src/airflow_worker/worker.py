"""Assemble the ``airflow`` catalog and run the worker.

Run it:

    vgi-airflow-worker                      # stdio: what DuckDB launches by default
    vgi-airflow-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB, then give it credentials:

    ATTACH 'airflow' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-airflow-worker');
    CREATE SECRET (TYPE airflow, BASE_URL 'http://localhost:8080', USERNAME 'admin', PASSWORD '...');
"""

from __future__ import annotations

import json
from typing import Any, cast

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import ReferencedKey, constraint_record
from vgi.metadata import FunctionExample

from .client import AIRFLOW_SECRET
from .functions import TaskLogs, VariableValues
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_CATEGORIES,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    SOURCE_URL,
    TABLE_DOCS,
    UNIFIED_CATEGORIES,
    UNIFIED_DOC_LLM,
    UNIFIED_DOC_MD,
    keywords_array,
    object_tags,
)
from .status import UnifiedStatus
from .tables import (
    AssetEvents,
    Assets,
    Connections,
    DagRuns,
    Dags,
    EventLogs,
    ImportErrors,
    Pools,
    TaskInstances,
    TaskInstanceTries,
    Variables,
    XcomEntries,
)
from .unified import UnifiedRuns, UnifiedTaskRuns

Q = f"{CATALOG_NAME}.main"
U = f"{CATALOG_NAME}.unified"

EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "dags": [
        (
            "Active DAGs that are paused",
            f"SELECT dag_id, schedule, owners FROM {Q}.dags WHERE is_paused AND is_active ORDER BY dag_id",
        ),
        (
            "DAG count per tag",
            f"SELECT tag, count(*) AS dags FROM (SELECT unnest(tags) AS tag FROM {Q}.dags) "
            "GROUP BY tag ORDER BY dags DESC",
        ),
    ],
    "dag_runs": [
        (
            "Failed runs of one DAG in November 2026 (dag_id, state and the date range are sent to the API)",
            f"SELECT run_id, logical_date, duration_seconds FROM {Q}.dag_runs WHERE dag_id = 'etl_orders' "
            "AND state = 'failed' AND start_date >= TIMESTAMPTZ '2026-11-01' ORDER BY logical_date",
        ),
        (
            "Run outcomes and median duration per DAG",
            f"SELECT dag_id, state, count(*) AS runs, median(duration_seconds) AS median_s FROM {Q}.dag_runs "
            "GROUP BY ALL ORDER BY dag_id, state",
        ),
    ],
    "task_instances": [
        (
            "Failed tasks of one DAG run",
            f"SELECT task_id, state, try_number, end_date FROM {Q}.task_instances WHERE dag_id = 'flaky_report' "
            "AND run_id = 'manual_report_1' AND state IN ('failed', 'upstream_failed') ORDER BY task_id",
        ),
        (
            "Slowest tasks across all runs",
            f"SELECT dag_id, task_id, max(duration_seconds) AS worst_s FROM {Q}.task_instances "
            "WHERE state = 'success' GROUP BY ALL ORDER BY worst_s DESC LIMIT 10",
        ),
    ],
    "task_instance_tries": [
        (
            "Tasks that needed retries",
            f"SELECT dag_id, task_id, run_id, count(*) AS attempts FROM {Q}.task_instance_tries "
            "WHERE dag_id = 'flaky_report' GROUP BY ALL HAVING count(*) > 1 ORDER BY attempts DESC",
        )
    ],
    "xcom_entries": [
        (
            "XCom keys pushed in one DAG",
            f"SELECT task_id, key, count(*) AS entries FROM {Q}.xcom_entries WHERE dag_id = 'etl_orders' "
            "GROUP BY ALL ORDER BY task_id",
        )
    ],
    "assets": [
        (
            "Which tasks produce each asset and which DAGs consume it",
            f"SELECT uri, producing_tasks, consuming_dags FROM {Q}.assets ORDER BY uri",
        )
    ],
    "asset_events": [
        (
            "Asset updates per producing DAG and the runs they triggered",
            f"SELECT source_dag_id, count(*) AS updates, sum(len(created_runs)) AS triggered_runs "
            f"FROM {Q}.asset_events GROUP BY ALL ORDER BY updates DESC",
        )
    ],
    "event_logs": [
        (
            "Who triggered or paused DAGs",
            f"SELECT event_time, owner, event, dag_id FROM {Q}.event_logs "
            "WHERE event IN ('trigger_dag_run', 'paused', 'patch_dag') ORDER BY event_time DESC LIMIT 20",
        )
    ],
    "import_errors": [
        (
            "Broken DAG files",
            f"SELECT filename, timestamp, split_part(stack_trace, chr(10), -2) AS error "
            f"FROM {Q}.import_errors ORDER BY filename",
        )
    ],
    "pools": [
        (
            "Pools that are nearly full",
            f"SELECT name, slots, open_slots FROM {Q}.pools WHERE slots > 0 AND open_slots < slots * 0.1 ORDER BY name",
        )
    ],
    "variables": [
        (
            "Variable keys and descriptions (values masked)",
            f"SELECT key, is_masked, description FROM {Q}.variables ORDER BY key",
        )
    ],
    "connections": [
        (
            "Connections per type",
            f"SELECT conn_type, count(*) AS n, list(connection_id ORDER BY connection_id) AS ids "
            f"FROM {Q}.connections GROUP BY conn_type ORDER BY n DESC",
        )
    ],
    "runs": [
        ("Runs per status", f"SELECT status, count(*) AS runs FROM {U}.runs GROUP BY status ORDER BY status"),
        (
            "Failure rate per pipeline since November",
            f"SELECT pipeline, avg((status = 'failed')::INT) AS failure_rate FROM {U}.runs "
            "WHERE started_at >= TIMESTAMPTZ '2026-11-01' GROUP BY pipeline ORDER BY failure_rate DESC",
        ),
    ],
    "task_runs": [
        (
            "Task outcomes joined to their runs",
            f"SELECT r.pipeline, t.status, count(*) AS tasks FROM {U}.task_runs t JOIN {U}.runs r USING (run_id) "
            "GROUP BY ALL ORDER BY r.pipeline, t.status",
        )
    ],
}


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB names it (for ``vgi.result_columns_schema``)."""
    if getattr(t, "extension_name", None) == "arrow.json":
        return "JSON"
    if pa.types.is_timestamp(t):
        return "TIMESTAMPTZ" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return "STRUCT(" + ", ".join(f"{f.name} {duckdb_type(f.type)}" for f in t) + ")"
    simple = {pa.string(): "VARCHAR", pa.int64(): "BIGINT", pa.int32(): "INTEGER", pa.bool_(): "BOOLEAN"}
    return simple.get(t, "DOUBLE" if pa.types.is_floating(t) else str(t).upper())


def _result_columns(schema: pa.Schema) -> str:
    return json.dumps(
        [
            {"name": f.name, "type": duckdb_type(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def _examples_json(name: str) -> str:
    return json.dumps([{"description": d, "sql": s} for d, s in EXAMPLES[name]])


def _table(
    name: str,
    scan: type[Any],
    schema: list[str],
    key: list[str],
    fks: tuple[tuple[list[str], str, list[str]], ...] = (),
) -> Table:
    """Build a catalog table over ``scan`` with NOT NULL, primary key and foreign key records.

    Each foreign key is ``(columns, referenced table, referenced primary key)`` in the
    same schema. They mirror the foreign keys of Airflow's metadata database, which is
    what the API serves, so every row satisfies them.
    """
    doc = TABLE_DOCS[name]
    not_null = [f.name for f in scan.FIXED_SCHEMA if not f.nullable]
    constraints = [
        constraint_record(
            schema, name, f"{name}_{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in not_null
    ]
    if key:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_pk",
                "PRIMARY_KEY",
                [[c] for c in key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    for cols, ref_table, ref_cols in fks:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_{ref_table}_fk",
                "FOREIGN_KEY",
                [[c] for c in cols],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
                references=ReferencedKey(
                    schema_path=schema,
                    table=ref_table,
                    columns=[[c] for c in ref_cols],
                    constraint_id=f"{ref_table}_pk",
                ),
            )
        )
    return Table(
        name=name,
        function=scan,
        comment=doc["comment"],
        constraints=tuple(constraints),
        column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in scan.FIXED_SCHEMA},
        tags={
            **object_tags(
                title=doc["title"],
                category=doc["category"],
                keywords=doc["keywords"],
                doc_llm=doc["doc_llm"],
                doc_md=doc["doc_md"],
            ),
            "domain": "orchestration",
            "vgi.example_queries": _examples_json(name),
        },
    )


RUN_KEY = ["dag_id", "run_id"]
TI_KEY = ["dag_id", "run_id", "task_id", "map_index"]
MAIN_TABLES = [
    _table("dags", Dags, ["main"], ["dag_id"]),
    _table("dag_runs", DagRuns, ["main"], RUN_KEY, ((["dag_id"], "dags", ["dag_id"]),)),
    _table("task_instances", TaskInstances, ["main"], TI_KEY, ((RUN_KEY, "dag_runs", RUN_KEY),)),
    _table(
        "task_instance_tries",
        TaskInstanceTries,
        ["main"],
        [*TI_KEY, "try_number"],
        ((TI_KEY, "task_instances", TI_KEY),),
    ),
    _table("xcom_entries", XcomEntries, ["main"], [*TI_KEY, "key"], ((TI_KEY, "task_instances", TI_KEY),)),
    _table("assets", Assets, ["main"], ["asset_id"]),
    _table("asset_events", AssetEvents, ["main"], ["event_id"], ((["asset_id"], "assets", ["asset_id"]),)),
    _table("event_logs", EventLogs, ["main"], ["event_log_id"]),
    _table("import_errors", ImportErrors, ["main"], ["import_error_id"]),
    _table("pools", Pools, ["main"], ["name"]),
    _table("variables", Variables, ["main"], ["key"]),
    _table("connections", Connections, ["main"], ["connection_id"]),
]
UNIFIED_TABLES = [
    _table("runs", UnifiedRuns, ["unified"], ["run_id"]),
    _table("task_runs", UnifiedTaskRuns, ["unified"], ["task_run_id"], ((["run_id"], "runs", ["run_id"]),)),
]

TaskLogs.Meta.tags = {
    **object_tags(
        title="Task Logs",
        category="tasks",
        keywords="task log, logs, log lines, error message, traceback, debugging, attempt",
        doc_llm=(
            "task_logs(dag_id, run_id, task_id, try_number := NULL, map_index := -1) returns the task "
            "instance's log, one row per line: dag_id, run_id, task_id, map_index, try_number, line_number, "
            "timestamp, level (lower case), logger, message, details (JSON extras such as error_detail on "
            "Airflow 3). try_number NULL reads every attempt. Filter level = 'error' to find the failure."
        ),
        doc_md=(
            "## task_logs(dag_id, run_id, task_id, try_number := NULL, map_index := -1)\n\n"
            "A task instance's log lines, parsed into timestamp, level, logger and message. Reads every attempt "
            "unless `try_number` is given."
        ),
    ),
    "domain": "orchestration",
    "vgi.result_columns_schema": _result_columns(TaskLogs.FIXED_SCHEMA),
}
TaskLogs.Meta.examples = [
    FunctionExample(
        sql=(
            f"SELECT try_number, timestamp, message FROM {Q}.task_logs('flaky_report', 'manual_report_1', 'render') "
            "WHERE level = 'error' ORDER BY try_number, line_number"
        ),
        description="Error lines from every attempt of a failing task",
    ),
    FunctionExample(
        sql=(
            f"SELECT level, count(*) AS lines FROM {Q}.task_logs('etl_orders', 'manual_orders_1', 'load', "
            "try_number := 1) GROUP BY level ORDER BY lines DESC"
        ),
        description="Log volume per level for one attempt",
    ),
]

VariableValues.Meta.tags = {
    **object_tags(
        title="Variable Values",
        category="config",
        keywords="variable values, reveal, configuration values, settings",
        doc_llm=(
            "variable_values(reveal := false) lists Airflow variables: key, value, is_masked, description, "
            "is_encrypted. By default every value is '***'. With reveal := true a value is shown only when its key "
            "does not look secret (password, token, key, secret, auth, ...) and the value does not look like a "
            "credential (JWT, API-key prefixes, private keys, URLs with passwords)."
        ),
        doc_md=(
            "## variable_values(reveal := false)\n\n"
            "Variables with opt-in revealing of values that do not look secret. Secret-looking keys stay masked."
        ),
    ),
    "domain": "orchestration",
    "vgi.result_columns_schema": _result_columns(VariableValues.FIXED_SCHEMA),
}
VariableValues.Meta.examples = [
    FunctionExample(
        sql=f"SELECT key, value FROM {Q}.variable_values(reveal := true) WHERE NOT is_masked ORDER BY key",
        description="Non-secret variable values",
    ),
]

FUNCTIONS: list[type[Any]] = [TaskLogs, VariableValues]
for fn in [*FUNCTIONS, cast("type[Any]", UnifiedStatus)]:
    fn.Meta.tags = {
        **fn.Meta.tags,
        "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in fn.Meta.examples]),
    }


def _schema_examples(names: list[str], extra: list[type[Any]]) -> str:
    out = [{"description": d, "sql": s} for n in names for d, s in EXAMPLES[n][:1]]
    out += [{"description": e.description, "sql": e.sql} for fn in extra for e in fn.Meta.examples[:1]]
    return json.dumps(out)


class AirflowWorker(Worker):
    """The worker process: the ``airflow`` catalog with ``main`` and ``unified`` schemas."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Airflow REST resources as tables: DAGs, runs, tasks, logs, assets, config and audit log.",
                tables=MAIN_TABLES,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Airflow Resources",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array("dags, dag runs, task instances, logs, assets, pools, variables"),
                    "vgi.doc_llm": MAIN_DOC_LLM,
                    "vgi.doc_md": MAIN_DOC_MD,
                    "vgi.categories": json.dumps(MAIN_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in MAIN_TABLES], FUNCTIONS),
                },
            ),
            Schema(
                path=["unified"],
                comment="Runs and task runs in the cross-orchestrator shape shared by Query.Farm workers.",
                tables=UNIFIED_TABLES,
                functions=[UnifiedStatus],
                tags={
                    "vgi.title": "Unified Orchestrator Runs",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array("unified, runs, task runs, cross orchestrator, union all"),
                    "vgi.doc_llm": UNIFIED_DOC_LLM,
                    "vgi.doc_md": UNIFIED_DOC_MD,
                    "vgi.categories": json.dumps(UNIFIED_CATEGORIES),
                    "vgi.example_queries": _schema_examples([t.name for t in UNIFIED_TABLES], [UnifiedStatus]),
                },
            ),
        ],
    )

    secret_types = [AIRFLOW_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    AirflowWorker.main()


if __name__ == "__main__":
    main()
