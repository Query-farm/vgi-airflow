"""Scalar ``unified_status(state)``: Airflow state → cross-orchestrator status.

The same mapping the ``unified`` tables use, exposed so you can normalize states
you already have (an exported ``dag_run`` table, a metadata-DB replica, an
OpenLineage feed) without calling the API.
"""

from __future__ import annotations

import json
from typing import Annotated, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .unified import task_status

_STATE_DOC = (
    "An Airflow DAG-run or task-instance state string as the Airflow API reports it. "
    "NULL means the task was not yet scheduled."
)


class UnifiedStatus(ScalarFunction):
    """``unified_status(state)`` for every row."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "unified_status"
        description = "Map an Airflow DAG-run or task state onto queued/running/success/failed/cancelled/skipped"
        tags: ClassVar[dict[str, str]] = {
            **object_tags(
                title="Unified Status From Airflow State",
                category="runs",
                keywords="status mapping, normalize state, upstream_failed, cross orchestrator, unified status",
                doc_llm=(
                    "unified_status(state) maps an Airflow DAG-run or task-instance state to the shared "
                    "orchestrator vocabulary used by the unified schema: success→success, failed→failed, "
                    "upstream_failed→skipped, skipped→skipped, removed/shutdown→cancelled, running/deferred/"
                    "restarting→running, queued/scheduled/up_for_retry/up_for_reschedule/NULL→queued. Unknown "
                    "states map to queued. Needs no credentials and makes no API calls."
                ),
                doc_md=(
                    "## unified_status(state)\n\n"
                    "Normalize an Airflow state to `queued`, `running`, `success`, `failed`, `cancelled` or "
                    "`skipped`, exactly as `unified.runs` and `unified.task_runs` do.\n\n"
                    "`upstream_failed` becomes `skipped`: the failure is counted once, on the upstream task."
                ),
            ),
            "domain": "orchestration",
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize every Airflow task state",
                        "sql": (
                            "SELECT s AS state, airflow.unified.unified_status(s) AS status FROM (VALUES "
                            "('success'), ('failed'), ('upstream_failed'), ('up_for_retry'), ('removed')) t(s) "
                            "ORDER BY s"
                        ),
                        "expected_result": [
                            {"state": "failed", "status": "failed"},
                            {"state": "removed", "status": "cancelled"},
                            {"state": "success", "status": "success"},
                            {"state": "up_for_retry", "status": "queued"},
                            {"state": "upstream_failed", "status": "skipped"},
                        ],
                    },
                    {
                        "description": "A task that was never scheduled (NULL state) counts as queued",
                        "sql": "SELECT airflow.unified.unified_status(NULL::VARCHAR) AS status",
                        "expected_result": [{"status": "queued"}],
                    },
                ]
            ),
        }
        examples: ClassVar[list[FunctionExample]] = [
            FunctionExample(
                sql=(
                    "SELECT airflow.unified.unified_status(state) AS status, count(*) AS tasks "
                    "FROM airflow.main.task_instances GROUP BY ALL ORDER BY status"
                ),
                description="Task instances per unified status",
            ),
        ]

    @classmethod
    def compute(
        cls,
        state: Annotated[pa.StringArray, Param(doc=_STATE_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each state; NULL means 'not yet scheduled' and maps to queued."""
        # Map the handful of distinct states once, then gather: no per-row Python.
        distinct = pc.unique(state)
        mapped = pa.array([task_status(s) for s in distinct.to_pylist()], type=pa.string())
        return pc.take(mapped, pc.index_in(state, value_set=distinct, skip_nulls=False))
