"""The ``main`` schema: Airflow REST resources as tables, with filter pushdown.

Each class below is the scan behind one catalog table. ``requests`` documents,
in code, exactly which SQL predicates reach the API; everything else is
filtered by DuckDB (``auto_apply_filters`` re-checks every pushed predicate).
Airflow 2 (``/api/v1``) and Airflow 3 (``/api/v2``) are normalized to the same
columns; a field one version lacks is NULL there.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, ClassVar

import pyarrow as pa

from .client import AirflowClient, NotFoundError, quote
from .masking import mask_extra, mask_variable
from .meta import column
from .scan import ApiScan, Listing, Pushdown, date_range_params, duration, json_text, to_int, ts

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())


def _names(items: Any, key: str = "name") -> list[str]:
    """Pull ``key`` out of a list of objects (tags, owners, consuming DAGs)."""
    out: list[str] = []
    for item in items or []:
        if isinstance(item, dict):
            if item.get(key) is not None:
                out.append(str(item[key]))
        elif item is not None:
            out.append(str(item))
    return out


def _json_or_string(value: Any) -> str | None:
    """Keep JSON text as is; wrap anything else as a JSON string."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except ValueError:
            return json.dumps(value)
    return json.dumps(value, sort_keys=True)


def _fan_out(pd: Pushdown, column_name: str) -> list[str]:
    """Path segments for a column: its ``=``/``IN`` values, or ``~`` (all)."""
    vals = pd.values(column_name)
    return [str(v) for v in vals] if vals else ["~"]


def run_url(client: AirflowClient, dag_id: str, run_id: str) -> str:
    """Web-UI link to a DAG run (grid view)."""
    if client.is_v2:
        return client.ui_url(f"/dags/{quote(dag_id)}/runs/{quote(run_id)}")
    return client.ui_url(f"/dags/{quote(dag_id)}/grid?dag_run_id={quote(run_id)}")


def task_url(client: AirflowClient, dag_id: str, run_id: str, task_id: str, map_index: int | None) -> str:
    """Web-UI link to one task instance."""
    mapped = map_index is not None and map_index >= 0
    if client.is_v2:
        tail = f"/mapped/{map_index}" if mapped else ""
        return client.ui_url(f"/dags/{quote(dag_id)}/runs/{quote(run_id)}/tasks/{quote(task_id)}{tail}")
    extra = f"&map_index={map_index}" if mapped else ""
    return client.ui_url(f"/dags/{quote(dag_id)}/grid?dag_run_id={quote(run_id)}&task_id={quote(task_id)}{extra}")


# ---------------------------------------------------------------------------- dags


class Dags(ApiScan):
    """Rows for ``airflow.main.dags``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("dag_id", pa.string(), "DAG identifier.", nullable=False),
            column("dag_display_name", pa.string(), "Display name shown in the UI."),
            column("description", pa.string(), "DAG description."),
            column("is_paused", pa.bool_(), "True when the DAG is paused (no new scheduled runs)."),
            column("is_active", pa.bool_(), "True when the DAG file still defines the DAG (Airflow 3: not stale)."),
            column("has_import_errors", pa.bool_(), "True when the DAG's file currently fails to import."),
            column("schedule", pa.string(), "Schedule expression, e.g. '0 0 * * *', '@daily' or an asset condition."),
            column("timetable_description", pa.string(), "Human-readable schedule description."),
            column("owners", STR_LIST, "DAG owners."),
            column("tags", STR_LIST, "DAG tags."),
            column("fileloc", pa.string(), "Absolute path of the file that defines the DAG."),
            column("bundle_name", pa.string(), "DAG bundle the file comes from (Airflow 3; NULL on 2.x)."),
            column("max_active_runs", pa.int64(), "Maximum number of concurrent runs (count)."),
            column("max_active_tasks", pa.int64(), "Maximum number of concurrent task instances (count)."),
            column("next_dagrun_logical_date", TSTZ, "Logical date of the next scheduled run."),
            column("next_dagrun_run_after", TSTZ, "Earliest time the next scheduled run is created."),
            column("last_parsed_time", TSTZ, "When the DAG file was last parsed."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "dags_scan"
        description = "Scan behind the dags table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """Push ``dag_id = x`` (as a pattern), ``is_paused`` and ``is_active``."""
        q: list[tuple[str, Any]] = []
        dag = pd.one("dag_id")
        if dag is not None:
            q.append(("dag_id_pattern", str(dag)))
        paused = pd.one("is_paused")
        if paused is not None:
            q.append(("paused", "true" if paused else "false"))
        active = pd.one("is_active")
        if client.is_v2:
            q.append(("exclude_stale", "true" if active is True else "false"))
        else:
            q.append(("only_active", "true" if active is True else "false"))
        return [Listing("/dags", "dags", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Normalize v1/v2 DAG objects."""
        out = []
        for d in items:
            active: Any
            if client.is_v2:
                schedule = d.get("timetable_summary")
                active = not d.get("is_stale", False)
                next_logical, next_after = d.get("next_dagrun_logical_date"), d.get("next_dagrun_run_after")
            else:
                si = d.get("schedule_interval")
                schedule = si.get("value") if isinstance(si, dict) else si
                active = d.get("is_active")
                next_logical, next_after = d.get("next_dagrun"), d.get("next_dagrun_create_after")
            out.append(
                {
                    "dag_id": d["dag_id"],
                    "dag_display_name": d.get("dag_display_name"),
                    "description": d.get("description"),
                    "is_paused": d.get("is_paused"),
                    "is_active": active,
                    "has_import_errors": d.get("has_import_errors"),
                    "schedule": None if schedule is None else str(schedule),
                    "timetable_description": d.get("timetable_description"),
                    "owners": _names(d.get("owners")),
                    "tags": _names(d.get("tags")),
                    "fileloc": d.get("fileloc"),
                    "bundle_name": d.get("bundle_name"),
                    "max_active_runs": to_int(d.get("max_active_runs")),
                    "max_active_tasks": to_int(d.get("max_active_tasks")),
                    "next_dagrun_logical_date": ts(next_logical),
                    "next_dagrun_run_after": ts(next_after),
                    "last_parsed_time": ts(d.get("last_parsed_time")),
                }
            )
        return out


# ------------------------------------------------------------------------ dag_runs

DAG_RUN_FIELDS = [
    column("dag_id", pa.string(), "DAG the run belongs to.", nullable=False),
    column("run_id", pa.string(), "Run identifier, unique within the DAG.", nullable=False),
    column("state", pa.string(), "Run state: queued, running, success or failed."),
    column("run_type", pa.string(), "manual, scheduled, backfill, asset_triggered (3.x) or dataset_triggered (2.x)."),
    column("triggered_by", pa.string(), "What triggered the run, e.g. rest_api, ui, cli, timetable, asset (3.x)."),
    column("logical_date", TSTZ, "Logical date (Airflow 2 execution_date); NULL for some 3.x asset/manual runs."),
    column("queued_at", TSTZ, "When the run was queued."),
    column("start_date", TSTZ, "When the run started."),
    column("end_date", TSTZ, "When the run finished."),
    column("duration_seconds", pa.float64(), "end_date - start_date in seconds."),
    column("data_interval_start", TSTZ, "Start of the data interval."),
    column("data_interval_end", TSTZ, "End of the data interval."),
    column("run_after", TSTZ, "Earliest time the run could start (Airflow 3)."),
    column("last_scheduling_decision", TSTZ, "Last time the scheduler evaluated the run."),
    column("conf", pa.json_(), "Run configuration (JSON)."),
    column("note", pa.string(), "User note on the run."),
    column("triggering_user_name", pa.string(), "User who triggered the run (Airflow 3)."),
]


def dag_run_listings(client: AirflowClient, pd: Pushdown, *, dag_col: str = "dag_id") -> list[Listing]:
    """Build dag-run listings: dag_id fans out into paths; state/run_type/date ranges become parameters."""
    v2 = client.is_v2
    q: list[tuple[str, Any]] = []
    for state in pd.values("state") or []:
        q.append(("state", str(state)))
    if v2:
        for run_type in pd.values("run_type") or []:
            q.append(("run_type", str(run_type)))
        run = pd.one("run_id")
        if run is not None:
            q.append(("run_id_pattern", str(run)))
    q += date_range_params(pd, "logical_date", "logical_date" if v2 else "execution_date", strict_ok=v2)
    q += date_range_params(pd, "start_date", "start_date", strict_ok=v2)
    q += date_range_params(pd, "end_date", "end_date", strict_ok=v2)
    if v2:
        q.append(("order_by", "id"))
    dags = _fan_out(pd, dag_col)
    return [Listing(f"/dags/{quote(d) if d != '~' else '~'}/dagRuns", "dag_runs", tuple(q), d != "~") for d in dags]


def dag_run_row(client: AirflowClient, r: dict[str, Any]) -> dict[str, Any]:
    """Normalize one v1/v2 DAG run object."""
    return {
        "dag_id": r["dag_id"],
        "run_id": r["dag_run_id"],
        "state": r.get("state"),
        "run_type": r.get("run_type"),
        "triggered_by": r.get("triggered_by"),
        "logical_date": ts(r.get("logical_date") or r.get("execution_date")),
        "queued_at": ts(r.get("queued_at")),
        "start_date": ts(r.get("start_date")),
        "end_date": ts(r.get("end_date")),
        "duration_seconds": duration(r),
        "data_interval_start": ts(r.get("data_interval_start")),
        "data_interval_end": ts(r.get("data_interval_end")),
        "run_after": ts(r.get("run_after")),
        "last_scheduling_decision": ts(r.get("last_scheduling_decision")),
        "conf": json_text(r.get("conf")),
        "note": r.get("note"),
        "triggering_user_name": r.get("triggering_user_name"),
    }


class DagRuns(ApiScan):
    """Rows for ``airflow.main.dag_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(DAG_RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "dag_runs_scan"
        description = "Scan behind the dag_runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """See ``dag_run_listings``."""
        return dag_run_listings(client, pd)

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per run."""
        return [dag_run_row(client, r) for r in items]


# ------------------------------------------------------------------ task instances

TI_FIELDS = [
    column("dag_id", pa.string(), "DAG the task belongs to.", nullable=False),
    column("run_id", pa.string(), "DAG run the task instance belongs to.", nullable=False),
    column("task_id", pa.string(), "Task identifier within the DAG.", nullable=False),
    column("map_index", pa.int64(), "Index of a mapped task instance; -1 when not mapped.", nullable=False),
    column("try_number", pa.int64(), "Current (latest) try number; 0 when it never ran."),
    column("max_tries", pa.int64(), "Number of retries allowed (count)."),
    column(
        "state",
        pa.string(),
        "success, failed, upstream_failed, skipped, running, queued, scheduled, up_for_retry, deferred, removed, NULL.",
    ),
    column("operator", pa.string(), "Operator class, e.g. PythonOperator or _PythonDecoratedOperator."),
    column("logical_date", TSTZ, "Logical date of the DAG run."),
    column("start_date", TSTZ, "When the current try started."),
    column("end_date", TSTZ, "When the current try finished."),
    column("duration_seconds", pa.float64(), "Duration of the current try in seconds."),
    column("queued_when", TSTZ, "When the task instance was queued."),
    column("hostname", pa.string(), "Worker host that ran the try."),
    column("pool", pa.string(), "Pool the task runs in."),
    column("pool_slots", pa.int64(), "Number of pool slots the task occupies (count)."),
    column("queue", pa.string(), "Executor queue."),
    column("priority_weight", pa.int64(), "Scheduling priority weight (higher runs first; unitless score)."),
    column("executor", pa.string(), "Executor name, when set."),
    column("pid", pa.int64(), "Operating-system process id (PID) of the try."),
    column("note", pa.string(), "User note on the task instance."),
    column("uuid", pa.string(), "Airflow 3 UUID of this task instance try (NULL on 2.x)."),
]


def ti_listings(client: AirflowClient, pd: Pushdown, *, state: bool = True) -> list[Listing]:
    """Build task-instance listings.

    ``dag_id`` and (with a single ``dag_id``) ``run_id`` become path segments;
    Airflow 3 also takes ``task_id`` and a ``run_id`` pattern. ``state``,
    ``pool`` and the date ranges become parameters.
    """
    v2 = client.is_v2
    q: list[tuple[str, Any]] = []
    if state:
        for s in pd.values("state") or []:
            q.append(("state", str(s)))
    for pool in pd.values("pool") or []:
        q.append(("pool", str(pool)))
    q += date_range_params(pd, "logical_date", "logical_date" if v2 else "execution_date", strict_ok=v2)
    if state:
        q += date_range_params(pd, "start_date", "start_date", strict_ok=v2)
        q += date_range_params(pd, "end_date", "end_date", strict_ok=v2)
    task = pd.one("task_id")
    if v2 and task is not None:
        q.append(("task_id", str(task)))
    dags = _fan_out(pd, "dag_id")
    runs = _fan_out(pd, "run_id")
    out: list[Listing] = []
    for d in dags:
        if d == "~" or runs == ["~"]:
            rq = list(q)
            if v2 and len(runs) == 1 and runs[0] != "~":
                rq.append(("run_id_pattern", runs[0]))
            path = f"/dags/{quote(d) if d != '~' else '~'}/dagRuns/~/taskInstances"
            out.append(Listing(path, "task_instances", tuple(rq), d != "~"))
        else:
            for r in runs:
                out.append(
                    Listing(f"/dags/{quote(d)}/dagRuns/{quote(r)}/taskInstances", "task_instances", tuple(q), True)
                )
    return out


def ti_row(client: AirflowClient, t: dict[str, Any]) -> dict[str, Any]:
    """Normalize one v1/v2 task-instance (or try) object."""
    return {
        "dag_id": t["dag_id"],
        "run_id": t["dag_run_id"],
        "task_id": t["task_id"],
        "map_index": to_int(t.get("map_index")) if t.get("map_index") is not None else -1,
        "try_number": to_int(t.get("try_number")),
        "max_tries": to_int(t.get("max_tries")),
        "state": t.get("state"),
        "operator": t.get("operator"),
        "logical_date": ts(t.get("logical_date") or t.get("execution_date")),
        "start_date": ts(t.get("start_date")),
        "end_date": ts(t.get("end_date")),
        "duration_seconds": duration(t),
        "queued_when": ts(t.get("queued_when")),
        "hostname": t.get("hostname") or None,
        "pool": t.get("pool"),
        "pool_slots": to_int(t.get("pool_slots")),
        "queue": t.get("queue"),
        "priority_weight": to_int(t.get("priority_weight")),
        "executor": t.get("executor"),
        "pid": to_int(t.get("pid")),
        "note": t.get("note"),
        "uuid": t.get("id"),
    }


class TaskInstances(ApiScan):
    """Rows for ``airflow.main.task_instances``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(TI_FIELDS)

    class Meta:
        """Table metadata."""

        name = "task_instances_scan"
        description = "Scan behind the task_instances table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """See ``ti_listings``."""
        return ti_listings(client, pd)

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per task instance (latest try)."""
        return [ti_row(client, t) for t in items]


class TaskInstanceTries(ApiScan):
    """Rows for ``airflow.main.task_instance_tries``: every attempt that actually started."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [f for f in TI_FIELDS if f.name not in ("note", "logical_date", "max_tries")]
        + [column("max_tries", pa.int64(), "Number of retries allowed at the time of the try (count).")]
    )

    class Meta:
        """Table metadata."""

        name = "task_instance_tries_scan"
        description = "Scan behind the task_instance_tries table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """Page the task instances (dag_id/run_id/task_id pushed down), then fetch each one's tries."""
        return ti_listings(client, pd, state=False)

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per try: ``GET .../taskInstances/{task_id}[/{map_index}]/tries`` for each instance."""
        out = []
        for t in items:
            base = f"/dags/{quote(t['dag_id'])}/dagRuns/{quote(t['dag_run_id'])}/taskInstances/{quote(t['task_id'])}"
            mi = t.get("map_index")
            if mi is not None and mi >= 0:
                base += f"/{mi}"
            tries: list[dict[str, Any]] = []
            offset = 0
            try:
                while True:
                    # Airflow 3 returns every try at once; Airflow 2 pages them.
                    paging = None if client.is_v2 else {"limit": 100, "offset": offset}
                    body = client.get(f"{base}/tries", paging)
                    page = list(body.get("task_instances") or [])
                    tries += page
                    offset += len(page)
                    if client.is_v2 or not page or offset >= int(body.get("total_entries") or 0):
                        break
            except NotFoundError:
                # Airflow < 2.10 has no tries endpoint: the instance is its only known try.
                tries = [t] if (t.get("try_number") or 0) > 0 else []
            # try_number 0 is a placeholder for an instance that never ran (e.g. upstream_failed).
            out += [ti_row(client, x) for x in tries if (x.get("try_number") or 0) > 0]
        return out


# ------------------------------------------------------------------- xcom entries


class XcomEntries(ApiScan):
    """Rows for ``airflow.main.xcom_entries``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("dag_id", pa.string(), "DAG of the task that pushed the XCom.", nullable=False),
            column(
                "run_id",
                pa.string(),
                "DAG run (Airflow 2 omits it; the worker resolves it from the logical date).",
                nullable=False,
            ),
            column("task_id", pa.string(), "Task that pushed the XCom.", nullable=False),
            column("map_index", pa.int64(), "Mapped task index; -1 when not mapped.", nullable=False),
            column("key", pa.string(), "XCom key, e.g. return_value.", nullable=False),
            column("timestamp", TSTZ, "When the XCom was written."),
            column("logical_date", TSTZ, "Logical date of the run."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "xcom_entries_scan"
        description = "Scan behind the xcom_entries table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """dag_id, run_id and task_id become path segments (fanning out over IN lists); key → xcom_key."""
        q: list[tuple[str, Any]] = []
        key = pd.one("key")
        if key is not None:
            q.append(("xcom_key", str(key)))
        mi = pd.one("map_index")
        if mi is not None:
            q.append(("map_index", int(mi)))
        out = []
        for d in _fan_out(pd, "dag_id"):
            for r in _fan_out(pd, "run_id"):
                for t in _fan_out(pd, "task_id"):
                    seg = [x if x == "~" else quote(x) for x in (d, r, t)]
                    path = f"/dags/{seg[0]}/dagRuns/{seg[1]}/taskInstances/{seg[2]}/xcomEntries"
                    ctx = (("run_id", r),) if r != "~" else ()
                    out.append(Listing(path, "xcom_entries", tuple(q), d != "~" or r != "~" or t != "~", ctx))
        return out

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per XCom entry (values are not fetched).

        Airflow 2 lists XComs without their run id; it is taken from the path when
        run_id was filtered, else resolved once per (dag_id, execution_date).
        """
        ctx = dict(listing.context)
        resolved: dict[tuple[str, str], str | None] = {}

        def run_of(x: dict[str, Any]) -> str | None:
            run = x.get("run_id") or x.get("dag_run_id") or ctx.get("run_id")
            if run or not x.get("execution_date"):
                return None if run is None else str(run)
            key = (str(x["dag_id"]), str(x["execution_date"]))
            if key not in resolved:
                body = client.get(
                    f"/dags/{quote(key[0])}/dagRuns",
                    {"execution_date_gte": key[1], "execution_date_lte": key[1], "limit": 1},
                )
                runs = body.get("dag_runs") or []
                resolved[key] = str(runs[0]["dag_run_id"]) if runs else None
            return resolved[key]

        rows = [
            {
                "dag_id": x["dag_id"],
                "run_id": run_of(x),
                "task_id": x["task_id"],
                "map_index": to_int(x.get("map_index")) if x.get("map_index") is not None else -1,
                "key": x["key"],
                "timestamp": ts(x.get("timestamp")),
                "logical_date": ts(x.get("logical_date") or x.get("execution_date")),
            }
            for x in items
        ]
        # A run deleted between the two calls leaves nothing to attach the entry to.
        return [r for r in rows if r["run_id"] is not None]


# -------------------------------------------------------------- assets / datasets

PRODUCER = pa.struct([pa.field("dag_id", pa.string()), pa.field("task_id", pa.string())])
CREATED_RUN = pa.struct([pa.field("dag_id", pa.string()), pa.field("run_id", pa.string())])


class Assets(ApiScan):
    """Rows for ``airflow.main.assets`` (Airflow 3 assets, Airflow 2 datasets)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "asset_id", pa.int64(), "Asset (2.x: dataset) identifier (integer assigned by Airflow).", nullable=False
            ),
            column("name", pa.string(), "Asset name (Airflow 3; the URI on 2.x)."),
            column("uri", pa.string(), "Asset URI, e.g. s3://bucket/key."),
            column("asset_group", pa.string(), "Asset group (Airflow 3)."),
            column("extra", pa.json_(), "Extra metadata attached to the asset (JSON)."),
            column("created_at", TSTZ, "When the asset was first seen."),
            column("updated_at", TSTZ, "When the asset was last updated."),
            column("consuming_dags", STR_LIST, "DAGs scheduled on this asset."),
            column("producing_tasks", pa.list_(PRODUCER), "Tasks that list this asset as an outlet."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "assets_scan"
        description = "Scan behind the assets table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """``uri = x`` → ``uri_pattern``."""
        q: list[tuple[str, Any]] = []
        uri = pd.one("uri")
        if uri is not None:
            q.append(("uri_pattern", str(uri)))
        if client.is_v2:
            # Include inactive assets too, so every asset event's asset_id resolves (the FK holds).
            q.append(("only_active", "false"))
            return [Listing("/assets", "assets", tuple(q))]
        return [Listing("/datasets", "datasets", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Normalize asset/dataset objects."""
        return [
            {
                "asset_id": int(a["id"]),
                "name": a.get("name") or a.get("uri"),
                "uri": a.get("uri"),
                "asset_group": a.get("group"),
                "extra": json_text(a.get("extra")),
                "created_at": ts(a.get("created_at")),
                "updated_at": ts(a.get("updated_at")),
                "consuming_dags": _names(a.get("scheduled_dags") or a.get("consuming_dags"), "dag_id"),
                "producing_tasks": [
                    {"dag_id": p.get("dag_id"), "task_id": p.get("task_id")} for p in a.get("producing_tasks") or []
                ],
            }
            for a in items
        ]


class AssetEvents(ApiScan):
    """Rows for ``airflow.main.asset_events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("event_id", pa.int64(), "Asset event identifier (integer assigned by Airflow).", nullable=False),
            column(
                "asset_id",
                pa.int64(),
                "Identifier (integer) of the asset (dataset) that was updated; joins assets.asset_id.",
            ),
            column("uri", pa.string(), "Asset URI."),
            column("name", pa.string(), "Asset name (Airflow 3; the URI on 2.x)."),
            column("timestamp", TSTZ, "When the event was emitted."),
            column("source_dag_id", pa.string(), "DAG whose task emitted the event."),
            column("source_task_id", pa.string(), "Task that emitted the event."),
            column("source_run_id", pa.string(), "Run that emitted the event."),
            column("source_map_index", pa.int64(), "Map index of the emitting task (-1 when not mapped)."),
            column("extra", pa.json_(), "Event extra (JSON)."),
            column("created_runs", pa.list_(CREATED_RUN), "DAG runs the event triggered."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "asset_events_scan"
        description = "Scan behind the asset_events table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """asset_id and source_* equality become parameters; timestamp ranges too on Airflow 3."""
        v2 = client.is_v2
        q: list[tuple[str, Any]] = []
        aid = pd.one("asset_id")
        if aid is not None:
            q.append(("asset_id" if v2 else "dataset_id", int(aid)))
        for col in ("source_dag_id", "source_task_id", "source_run_id"):
            val = pd.one(col)
            if val is not None:
                q.append((col, str(val)))
        if v2:
            q += date_range_params(pd, "timestamp", "timestamp", strict_ok=True)
            return [Listing("/assets/events", "asset_events", tuple(q))]
        return [Listing("/datasets/events", "dataset_events", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Normalize asset/dataset events."""
        return [
            {
                "event_id": int(e["id"]),
                "asset_id": to_int(e.get("asset_id", e.get("dataset_id"))),
                "uri": e.get("uri") or e.get("dataset_uri"),
                "name": e.get("name") or e.get("uri") or e.get("dataset_uri"),
                "timestamp": ts(e.get("timestamp")),
                "source_dag_id": e.get("source_dag_id"),
                "source_task_id": e.get("source_task_id"),
                "source_run_id": e.get("source_run_id"),
                "source_map_index": to_int(e.get("source_map_index")),
                "extra": json_text(e.get("extra")),
                "created_runs": [
                    {"dag_id": r.get("dag_id"), "run_id": r.get("run_id") or r.get("dag_run_id")}
                    for r in e.get("created_dagruns") or []
                ],
            }
            for e in items
        ]


# --------------------------------------------------------------------- event logs


class EventLogs(ApiScan):
    """Rows for ``airflow.main.event_logs`` (the audit log)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "event_log_id", pa.int64(), "Audit log entry identifier (integer, increasing in time).", nullable=False
            ),
            column("event_time", TSTZ, "When the event happened (Airflow's `when`)."),
            column("event", pa.string(), "Event name, e.g. cli_scheduler, trigger_dag_run, failed, running."),
            column("owner", pa.string(), "User or system that caused the event."),
            column("dag_id", pa.string(), "DAG involved, when any."),
            column("task_id", pa.string(), "Task involved, when any."),
            column("run_id", pa.string(), "Run involved, when any."),
            column("map_index", pa.int64(), "Map index of the task involved (-1 when not mapped), when any."),
            column("try_number", pa.int64(), "Attempt number (count from 1) of the task involved, when any."),
            column("logical_date", TSTZ, "Logical date of the run, when any."),
            column("extra", pa.json_(), "Detail recorded with the event (JSON; free text is stored as a JSON string)."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "event_logs_scan"
        description = "Scan behind the event_logs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """dag_id/task_id/run_id/event/owner equality, IN on event, and ``event_time`` ranges (after/before)."""
        q: list[tuple[str, Any]] = []
        for col in ("dag_id", "task_id", "run_id", "owner"):
            val = pd.one(col)
            if val is not None:
                q.append((col, str(val)))
        events = pd.values("event")
        if events and len(events) == 1:
            q.append(("event", str(events[0])))
        elif events:
            q.append(("included_events", ",".join(str(e) for e in events)))
        b = pd.bounds("event_time")
        if b is not None:
            lo, _, hi, _ = b
            # Airflow compares strictly; widen by a second and let DuckDB trim the edge.
            if isinstance(lo, dt.datetime):
                q.append(("after", (lo - dt.timedelta(seconds=1)).astimezone(dt.UTC).isoformat()))
            if isinstance(hi, dt.datetime):
                q.append(("before", (hi + dt.timedelta(seconds=1)).astimezone(dt.UTC).isoformat()))
        return [Listing("/eventLogs", "event_logs", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Normalize audit log entries."""
        return [
            {
                "event_log_id": int(e["event_log_id"]),
                "event_time": ts(e.get("when")),
                "event": e.get("event"),
                "owner": e.get("owner"),
                "dag_id": e.get("dag_id"),
                "task_id": e.get("task_id"),
                "run_id": e.get("run_id"),
                "map_index": to_int(e.get("map_index")),
                "try_number": to_int(e.get("try_number")),
                "logical_date": ts(e.get("logical_date") or e.get("execution_date")),
                "extra": _json_or_string(e.get("extra")),
            }
            for e in items
        ]


# ------------------------------------------------------------------ small tables


class ImportErrors(ApiScan):
    """Rows for ``airflow.main.import_errors``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "import_error_id",
                pa.int64(),
                "Identifier (integer) Airflow assigns to the failing file's error record.",
                nullable=False,
            ),
            column("timestamp", TSTZ, "When the file last failed to import."),
            column("filename", pa.string(), "DAG file that failed (relative to its bundle on Airflow 3)."),
            column("bundle_name", pa.string(), "DAG bundle (Airflow 3)."),
            column("stack_trace", pa.string(), "Python traceback of the failure."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "import_errors_scan"
        description = "Scan behind the import_errors table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """No server-side filters are pushed."""
        return [Listing("/importErrors", "import_errors")]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per failing file."""
        return [
            {
                "import_error_id": int(e["import_error_id"]),
                "timestamp": ts(e.get("timestamp")),
                "filename": e.get("filename"),
                "bundle_name": e.get("bundle_name"),
                "stack_trace": e.get("stack_trace"),
            }
            for e in items
        ]


class Pools(ApiScan):
    """Rows for ``airflow.main.pools``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("name", pa.string(), "Pool name.", nullable=False),
            column("slots", pa.int64(), "Total number of slots (count; -1 means unlimited)."),
            column(
                "occupied_slots",
                pa.int64(),
                "Number of slots in use (count: running + queued, plus deferred when counted).",
            ),
            column("running_slots", pa.int64(), "Number of slots used by running tasks (count)."),
            column("queued_slots", pa.int64(), "Number of slots used by queued tasks (count)."),
            column("scheduled_slots", pa.int64(), "Number of slots used by scheduled tasks (count)."),
            column("deferred_slots", pa.int64(), "Number of slots used by deferred tasks (count)."),
            column("open_slots", pa.int64(), "Number of free slots (count)."),
            column("include_deferred", pa.bool_(), "Whether deferred tasks count against the pool."),
            column("description", pa.string(), "Pool description."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "pools_scan"
        description = "Scan behind the pools table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """``name = x`` → ``pool_name_pattern`` on Airflow 3."""
        q: list[tuple[str, Any]] = []
        name = pd.one("name")
        if client.is_v2 and name is not None:
            q.append(("pool_name_pattern", str(name)))
        return [Listing("/pools", "pools", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per pool."""
        keys = ("slots", "occupied_slots", "running_slots", "queued_slots", "scheduled_slots", "deferred_slots")
        return [
            {
                "name": p["name"],
                **{k: to_int(p.get(k)) for k in (*keys, "open_slots")},
                "include_deferred": p.get("include_deferred"),
                "description": p.get("description"),
            }
            for p in items
        ]


VARIABLE_FIELDS = [
    column("key", pa.string(), "Variable key.", nullable=False),
    column("value", pa.string(), "Value, or '***' when masked."),
    column("is_masked", pa.bool_(), "True when value is withheld."),
    column("description", pa.string(), "Variable description."),
    column("is_encrypted", pa.bool_(), "Whether Airflow stores the value encrypted (Airflow 3)."),
]


def variable_rows(items: list[dict[str, Any]], *, reveal: bool) -> list[dict[str, Any]]:
    """Mask variable values (see ``masking.mask_variable``)."""
    out = []
    for v in items:
        value, masked = mask_variable(str(v["key"]), v.get("value"), reveal=reveal)
        out.append(
            {
                "key": v["key"],
                "value": value,
                "is_masked": masked,
                "description": v.get("description"),
                "is_encrypted": v.get("is_encrypted"),
            }
        )
    return out


def variable_listing(client: AirflowClient, pd: Pushdown) -> list[Listing]:
    """``key = x`` → ``variable_key_pattern`` on Airflow 3."""
    q: list[tuple[str, Any]] = []
    key = pd.one("key")
    if client.is_v2 and key is not None:
        q.append(("variable_key_pattern", str(key)))
    return [Listing("/variables", "variables", tuple(q))]


class Variables(ApiScan):
    """Rows for ``airflow.main.variables``: every value masked."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(VARIABLE_FIELDS)

    class Meta:
        """Table metadata."""

        name = "variables_scan"
        description = "Scan behind the variables table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """See ``variable_listing``."""
        return variable_listing(client, pd)

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Mask every value."""
        return variable_rows(items, reveal=False)


class Connections(ApiScan):
    """Rows for ``airflow.main.connections``: never a password; ``extra`` secrets masked."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column(
                "connection_id",
                pa.string(),
                "Name hooks and operators use to look the connection up, e.g. 'warehouse_pg'.",
                nullable=False,
            ),
            column("conn_type", pa.string(), "Connection type, e.g. postgres, http, aws."),
            column("description", pa.string(), "Connection description."),
            column("host", pa.string(), "Server host name or base URL the connection points at."),
            column("port", pa.int64(), "TCP port number (index 1-65535), when set."),
            column("login", pa.string(), "Login / user name."),
            column("schema", pa.string(), "Schema or database."),
            column("has_password", pa.bool_(), "True when a password is set (Airflow 3; NULL on 2.x)."),
            column("extra", pa.json_(), "Extra JSON with secret-looking keys replaced by '***'."),
        ]
    )

    class Meta:
        """Table metadata."""

        name = "connections_scan"
        description = "Scan behind the connections table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """``connection_id = x`` → ``connection_id_pattern`` on Airflow 3."""
        q: list[tuple[str, Any]] = []
        cid = pd.one("connection_id")
        if client.is_v2 and cid is not None:
            q.append(("connection_id_pattern", str(cid)))
        return [Listing("/connections", "connections", tuple(q))]

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per connection; the password itself is never read into a row."""
        return [
            {
                "connection_id": c["connection_id"],
                "conn_type": c.get("conn_type"),
                "description": c.get("description"),
                "host": c.get("host"),
                "port": to_int(c.get("port")),
                "login": c.get("login"),
                "schema": c.get("schema"),
                "has_password": (c.get("password") not in (None, "")) if "password" in c else None,
                "extra": mask_extra(c.get("extra")),
            }
            for c in items
        ]
