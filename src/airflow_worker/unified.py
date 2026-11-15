"""The ``unified`` schema: runs and task runs in the cross-orchestrator shape.

The same two tables (same columns, types and status vocabulary) are served by
the Airflow, Dagster, Prefect, Temporal, Databricks, dbt and GitHub Actions
workers, so ``UNION ALL`` across them just works.

Airflow specifics:

* ``run_id`` is ``'<dag_id>/<dag_run_id>'``: Airflow run ids are only unique
  within a DAG (every daily DAG has a ``scheduled__2026-11-15T00:00:00+00:00``),
  and ``task_runs`` has no pipeline column to disambiguate a join.
* ``task_run_id`` is ``'<dag_id>/<dag_run_id>/<task_id>'``, plus
  ``'/<map_index>'`` for mapped task instances. One row per task instance (its
  latest try); ``attempt`` is the try number. Every try is in
  ``airflow.main.task_instance_tries``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .client import AirflowClient
from .meta import column
from .scan import ApiScan, Listing, Pushdown, duration, ts
from .tables import dag_run_listings, run_url, task_url, ti_listings

TSTZ = pa.timestamp("us", tz="UTC")
ORCHESTRATOR = "airflow"
STATUSES = ("queued", "running", "success", "failed", "cancelled", "skipped")

RUN_STATUS = {"queued": "queued", "running": "running", "success": "success", "failed": "failed"}

TASK_STATUS = {
    "success": "success",
    "failed": "failed",
    # The task never ran because something upstream failed: the failure is counted once, upstream.
    "upstream_failed": "skipped",
    "skipped": "skipped",
    "removed": "cancelled",
    "shutdown": "cancelled",
    "running": "running",
    "deferred": "running",
    "restarting": "running",
    "up_for_retry": "queued",
    "up_for_reschedule": "queued",
    "scheduled": "queued",
    "queued": "queued",
    "none": "queued",
}


def run_status(raw: str | None) -> str:
    """Map an Airflow DAG-run state onto the unified vocabulary."""
    return RUN_STATUS.get(raw or "", "queued")


def task_status(raw: str | None) -> str:
    """Map an Airflow task-instance state onto the unified vocabulary (NULL state = not yet scheduled)."""
    return TASK_STATUS.get(raw or "none", "queued")


def trigger(run_type: str | None, triggered_by: str | None) -> str | None:
    """Map run_type / triggered_by onto schedule, manual, sensor, api, event or other."""
    if run_type == "scheduled":
        return "schedule"
    if run_type in ("asset_triggered", "dataset_triggered", "asset_materialization"):
        return "event"
    if run_type == "manual":
        return "api" if triggered_by == "rest_api" else "manual"
    if run_type is None:
        return None
    return "other"


def _inverse(mapping: dict[str, str], statuses: list[Any]) -> list[str]:
    wanted = {str(s) for s in statuses}
    return sorted(raw for raw, status in mapping.items() if status in wanted and raw != "none")


class Remapped(Pushdown):
    """A ``Pushdown`` view whose columns were renamed / values translated for reuse."""

    def __init__(self, values: dict[str, list[Any]], bounds: dict[str, tuple[Any, bool, Any, bool]]) -> None:
        """Hold precomputed constraints."""
        super().__init__(None)
        self._values = values
        self._bounds = bounds

    def values(self, column: str) -> list[Any] | None:
        """Return translated exact values for a native column."""
        return self._values.get(column)

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """Return the translated range for a native column."""
        return self._bounds.get(column)


def _split_runs(values: list[Any] | None) -> tuple[list[str] | None, list[str] | None]:
    """Split composite ``dag/run`` ids into (dag ids, run ids)."""
    if not values:
        return None, None
    dags, runs = [], []
    for v in values:
        dag, sep, run = str(v).partition("/")
        if not sep:
            return None, None
        dags.append(dag)
        runs.append(run)
    return sorted(set(dags)), sorted(set(runs))


RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'airflow'.", nullable=False),
    column("run_id", pa.string(), "'<dag_id>/<dag_run_id>' (Airflow run ids are unique only per DAG).", nullable=False),
    column("pipeline", pa.string(), "DAG id."),
    column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
    column("raw_status", pa.string(), "Airflow's own DAG-run state."),
    column("trigger", pa.string(), "schedule, manual, api, event or other."),
    column("queued_at", TSTZ, "When the run was queued."),
    column("started_at", TSTZ, "When the run started."),
    column("ended_at", TSTZ, "When the run finished."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("url", pa.string(), "Link to the run in the Airflow UI."),
]

TASK_RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'airflow'.", nullable=False),
    column("run_id", pa.string(), "'<dag_id>/<dag_run_id>', joins unified.runs.run_id.", nullable=False),
    column(
        "task_run_id",
        pa.string(),
        "'<dag_id>/<dag_run_id>/<task_id>', plus '/<map_index>' for mapped tasks.",
        nullable=False,
    ),
    column("task_name", pa.string(), "Task id."),
    column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
    column("raw_status", pa.string(), "Airflow's own task-instance state."),
    column("attempt", pa.int32(), "Try number of the latest attempt (0 = never ran)."),
    column("started_at", TSTZ, "When the latest attempt started."),
    column("ended_at", TSTZ, "When the latest attempt finished."),
    column("duration_seconds", pa.float64(), "Duration of the latest attempt in seconds."),
    column("url", pa.string(), "Link to the task instance in the Airflow UI."),
]


class UnifiedRuns(ApiScan):
    """Rows for ``airflow.unified.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_runs_scan"
        description = "Scan behind the unified.runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """Pipeline → DAG path, status → state, run_id → DAG path + run pattern, started/ended ranges."""
        values: dict[str, list[Any]] = {}
        dags, runs = _split_runs(pd.values("run_id"))
        pipelines = pd.values("pipeline")
        if dags is not None:
            values["dag_id"] = dags if pipelines is None else sorted(set(dags) & {str(p) for p in pipelines})
            if len(runs or []) == 1:
                values["run_id"] = list(runs or [])
            if not values["dag_id"]:
                return []
        elif pipelines:
            values["dag_id"] = [str(p) for p in pipelines]
        statuses = pd.values("status")
        if statuses:
            raw = _inverse(RUN_STATUS, statuses)
            if not raw:
                return []
            values["state"] = raw
        bounds = {}
        for src, dst in (("started_at", "start_date"), ("ended_at", "end_date")):
            b = pd.bounds(src)
            if b is not None:
                bounds[dst] = b
        return dag_run_listings(client, Remapped(values, bounds))

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map DAG runs to unified rows."""
        out = []
        for r in items:
            dag, run = r["dag_id"], r["dag_run_id"]
            out.append(
                {
                    "orchestrator": ORCHESTRATOR,
                    "run_id": f"{dag}/{run}",
                    "pipeline": dag,
                    "status": run_status(r.get("state")),
                    "raw_status": r.get("state"),
                    "trigger": trigger(r.get("run_type"), r.get("triggered_by")),
                    "queued_at": ts(r.get("queued_at")),
                    "started_at": ts(r.get("start_date")),
                    "ended_at": ts(r.get("end_date")),
                    "duration_seconds": duration(r),
                    "url": run_url(client, dag, run) if "url" in wanted else None,
                }
            )
        return out


class UnifiedTaskRuns(ApiScan):
    """Rows for ``airflow.unified.task_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(TASK_RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_task_runs_scan"
        description = "Scan behind the unified.task_runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """run_id → DAG/run path, task_name → task_id, status → state, started/ended ranges."""
        values: dict[str, list[Any]] = {}
        dags, runs = _split_runs(pd.values("run_id"))
        if dags is not None and runs is not None:
            values["dag_id"] = dags
            if len(dags) == 1:
                values["run_id"] = runs
        task = pd.one("task_name")
        if task is not None:
            values["task_id"] = [task]
        statuses = pd.values("status")
        if statuses:
            raw = _inverse(TASK_STATUS, statuses)
            if not raw:
                return []
            values["state"] = raw
        bounds = {}
        for src, dst in (("started_at", "start_date"), ("ended_at", "end_date")):
            b = pd.bounds(src)
            if b is not None:
                bounds[dst] = b
        return ti_listings(client, Remapped(values, bounds))

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map task instances to unified rows."""
        out = []
        for t in items:
            dag, run, task = t["dag_id"], t["dag_run_id"], t["task_id"]
            mi = t.get("map_index")
            suffix = f"/{mi}" if isinstance(mi, int) and mi >= 0 else ""
            out.append(
                {
                    "orchestrator": ORCHESTRATOR,
                    "run_id": f"{dag}/{run}",
                    "task_run_id": f"{dag}/{run}/{task}{suffix}",
                    "task_name": task,
                    "status": task_status(t.get("state")),
                    "raw_status": t.get("state"),
                    "attempt": t.get("try_number"),
                    "started_at": ts(t.get("start_date")),
                    "ended_at": ts(t.get("end_date")),
                    "duration_seconds": duration(t),
                    "url": task_url(client, dag, run, task, mi) if "url" in wanted else None,
                }
            )
        return out
