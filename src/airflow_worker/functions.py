"""Table functions that take arguments: ``task_logs`` and ``variable_values``."""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import AIRFLOW_SECRET_TYPE, AirflowClient, quote
from .meta import column
from .scan import client_from, ts
from .tables import VARIABLE_FIELDS, variable_rows

TSTZ = pa.timestamp("us", tz="UTC")

# ------------------------------------------------------------------------ task_logs

LOG_SCHEMA = pa.schema(
    [
        column("dag_id", pa.string(), "DAG id.", nullable=False),
        column("run_id", pa.string(), "DAG run id.", nullable=False),
        column("task_id", pa.string(), "Task id.", nullable=False),
        column("map_index", pa.int64(), "Mapped task index; -1 when not mapped.", nullable=False),
        column("try_number", pa.int64(), "Attempt the line belongs to.", nullable=False),
        column("line_number", pa.int64(), "1-based position of the line within the attempt's log.", nullable=False),
        column("timestamp", TSTZ, "Timestamp of the log record, when the line carries one."),
        column("level", pa.string(), "Log level in lower case (info, warning, error, ...), when known."),
        column("logger", pa.string(), "Logger (Airflow 3) or source file:line (Airflow 2), when known."),
        column("message", pa.string(), "The log message (Airflow 3 event text; one raw text line on 2.x)."),
        column("details", pa.json_(), "Remaining structured fields of an Airflow 3 record, e.g. error_detail (JSON)."),
    ]
)

# "[2026-11-15T05:53:55.577+0000] {logging_mixin.py:190} INFO - rendering 42 rows"
_V1_LINE = re.compile(r"^\[(?P<ts>[^\]]+)\] \{(?P<src>[^}]*)\} (?P<level>[A-Z]+) - (?P<msg>.*)$")
_STD_KEYS = {"timestamp", "event", "level", "logger", "dag_id", "task_id", "run_id", "map_index", "try_number", "ti_id"}


def _v1_ts(raw: str) -> Any:
    # Airflow 2 writes "+0000"; fromisoformat wants "+00:00".
    fixed = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", raw.strip())
    return ts(fixed)


def parse_log(content: Any) -> list[dict[str, Any]]:
    """Split an Airflow log payload into line records.

    Airflow 3 returns a list of structured records (``event``, ``timestamp``,
    ``level``, ``logger`` and extras); Airflow 2 returns text, sometimes wrapped
    as the repr of ``[(hostname, text), ...]``.

    Args:
        content: The ``content`` field of the logs response.

    Returns:
        One dict per line with timestamp, level, logger, message, details.

    """
    lines: list[dict[str, Any]] = []
    if isinstance(content, list) and all(isinstance(x, dict) for x in content):
        for rec in content:
            extra = {k: v for k, v in rec.items() if k not in _STD_KEYS}
            lines.append(
                {
                    "timestamp": ts(rec.get("timestamp")),
                    "level": str(rec["level"]).lower() if rec.get("level") else None,
                    "logger": rec.get("logger"),
                    "message": None if rec.get("event") is None else str(rec.get("event")),
                    "details": json.dumps(extra, sort_keys=True, default=str) if extra else None,
                }
            )
        return lines
    text = content if isinstance(content, str) else json.dumps(content)
    if text.startswith("[(") and text.endswith(")]"):
        try:
            chunks = ast.literal_eval(text)
            text = "\n".join(str(chunk[1]) for chunk in chunks)
        except (ValueError, SyntaxError, IndexError, TypeError):
            pass
    for raw in text.splitlines():
        m = _V1_LINE.match(raw)
        if m:
            lines.append(
                {
                    "timestamp": _v1_ts(m["ts"]),
                    "level": m["level"].lower(),
                    "logger": m["src"],
                    "message": m["msg"],
                    "details": None,
                }
            )
        else:
            level = None
            stripped = raw.strip()
            for name in ("INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"):
                if stripped.startswith(f"{name} - "):
                    level, stripped = name.lower(), stripped[len(name) + 3 :]
                    break
            lines.append(
                {
                    "timestamp": None,
                    "level": level,
                    "logger": None,
                    "message": stripped if level else raw,
                    "details": None,
                }
            )
    return lines


@dataclass(slots=True, frozen=True, kw_only=True)
class LogArgs:
    """Arguments for ``task_logs(dag_id, run_id, task_id, try_number := NULL, map_index := -1)``."""

    dag_id: Annotated[str, Arg(0, doc="DAG id, e.g. 'etl_orders'.")]
    run_id: Annotated[str, Arg(1, doc="DAG run id, e.g. 'manual__2026-11-15T00:00:00+00:00'.")]
    task_id: Annotated[str, Arg(2, doc="Task id within the DAG, e.g. 'load'.")]
    try_number: Annotated[
        int | None,
        Arg("try_number", default=None, doc="Attempt to read (1-based); NULL (default) reads every attempt.", ge=1),
    ]
    map_index: Annotated[
        int, Arg("map_index", default=-1, doc="Mapped task index; -1 (default) for an unmapped task.", ge=-1)
    ]


@dataclass(kw_only=True)
class LogState(ArrowSerializableDataclass):
    """Cursor: attempts still to read (filled on the first call)."""

    started: bool = False
    tries: list[int] = field(default_factory=list)


def _ti_path(args: LogArgs) -> str:
    path = f"/dags/{quote(args.dag_id)}/dagRuns/{quote(args.run_id)}/taskInstances/{quote(args.task_id)}"
    return path + (f"/{args.map_index}" if args.map_index >= 0 else "")


def fetch_log(client: AirflowClient, args: LogArgs, attempt: int) -> Any:
    """GET one attempt's full log and return its ``content``."""
    path = f"/dags/{quote(args.dag_id)}/dagRuns/{quote(args.run_id)}/taskInstances/{quote(args.task_id)}"
    query: dict[str, Any] = {"full_content": "true"}
    if args.map_index >= 0:
        query["map_index"] = args.map_index
    body = client.get(f"{path}/logs/{attempt}", query)
    return body.get("content") if isinstance(body, dict) else body


@init_single_worker
class TaskLogs(TableFunctionGenerator[LogArgs, LogState]):
    """Every line of a task instance's log, one attempt per batch."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LOG_SCHEMA

    class Meta:
        """Function metadata; tags are filled in by ``meta.py``."""

        name = "task_logs"
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[FunctionExample]] = []
        description = "Log lines of one Airflow task instance, every attempt or one try_number"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[LogArgs],
        *,
        airflow: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(AIRFLOW_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Return the fixed schema and ask DuckDB for the ``airflow`` secret."""
        return BindResponse(output_schema=LOG_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[LogArgs]) -> LogState:
        """Start empty; attempts are resolved on the first ``process`` call."""
        return LogState()

    @classmethod
    def process(cls, params: ProcessParams[LogArgs], state: LogState, out: OutputCollector) -> None:
        """Emit one attempt's log lines per call."""
        args = params.args
        client = client_from(params)
        if not state.started:
            state.started = True
            if args.try_number is not None:
                state.tries = [args.try_number]
            else:
                ti = client.get(_ti_path(args))
                state.tries = list(range(1, int(ti.get("try_number") or 0) + 1))
        if not state.tries:
            out.finish()
            return
        attempt = state.tries.pop(0)
        lines = parse_log(fetch_log(client, args, attempt))
        n = len(lines)
        cols: dict[str, list[Any]] = {
            "dag_id": [args.dag_id] * n,
            "run_id": [args.run_id] * n,
            "task_id": [args.task_id] * n,
            "map_index": [args.map_index] * n,
            "try_number": [attempt] * n,
            "line_number": list(range(1, n + 1)),
        }
        for key in ("timestamp", "level", "logger", "message", "details"):
            cols[key] = [line[key] for line in lines]
        schema = params.output_schema
        out.emit(pa.RecordBatch.from_pydict({f.name: cols[f.name] for f in schema}, schema=schema))


# ------------------------------------------------------------------ variable_values


@dataclass(slots=True, frozen=True, kw_only=True)
class VariableArgs:
    """Arguments for ``variable_values(reveal := false)``."""

    reveal: Annotated[
        bool,
        Arg(
            "reveal",
            default=False,
            doc="Show values whose key and value do not look secret. Default false: every value is '***'.",
        ),
    ]


@dataclass(kw_only=True)
class OffsetState(ArrowSerializableDataclass):
    """Cursor: next offset, or -1 when done."""

    offset: int = 0


@init_single_worker
class VariableValues(TableFunctionGenerator[VariableArgs, OffsetState]):
    """Airflow variables with opt-in revealing of non-secret values."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(VARIABLE_FIELDS)

    class Meta:
        """Function metadata; tags are filled in by ``meta.py``."""

        name = "variable_values"
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[FunctionExample]] = []
        description = "Airflow variables; reveal := true shows values whose key and value do not look secret"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[VariableArgs],
        *,
        airflow: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(AIRFLOW_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Return the fixed schema and ask DuckDB for the ``airflow`` secret."""
        return BindResponse(output_schema=pa.schema(VARIABLE_FIELDS))

    @classmethod
    def initial_state(cls, params: ProcessParams[VariableArgs]) -> OffsetState:
        """Start at offset 0."""
        return OffsetState()

    @classmethod
    def process(cls, params: ProcessParams[VariableArgs], state: OffsetState, out: OutputCollector) -> None:
        """Emit one page of variables per call."""
        if state.offset < 0:
            out.finish()
            return
        client = client_from(params)
        limit = client.config.page_size
        body = client.get("/variables", {"limit": limit, "offset": state.offset})
        items = list(body.get("variables") or [])
        total = int(body.get("total_entries") or 0)
        state.offset = (
            -1 if not items or len(items) < limit or state.offset + len(items) >= total else state.offset + len(items)
        )
        rows = variable_rows(items, reveal=params.args.reveal)
        schema = params.output_schema
        out.emit(pa.RecordBatch.from_pydict({f.name: [r[f.name] for r in rows] for f in schema}, schema=schema))
