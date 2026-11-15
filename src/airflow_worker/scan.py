"""Shared machinery for the API-backed tables: pushdown, paging, row conversion.

Every table is a ``TableFunctionGenerator`` subclass of ``ApiScan``. A subclass
declares its ``FIXED_SCHEMA`` and implements two hooks:

* ``requests(client, pushdown)`` turns the pushed-down SQL predicates into one or
  more API listings (path + query parameters). Equality on ``dag_id`` becomes a
  path segment, ``IN`` lists fan out into one listing per value, ranges become
  ``*_gte`` / ``*_lte`` parameters, and so on.
* ``rows(client, items)`` maps one page of API items to column values.

The cursor (``ScanState``) is just ``(listing index, offset)``. It travels with
each batch over the HTTP transport, holds no credentials, and lets a scan resume
on any worker process. ``auto_apply_filters`` re-applies every predicate to each
batch, so translating a predicate only ever has to *narrow* what the API
returns (a superset is fine), never be exact.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.invocation import BindResponse
from vgi.table_filter_pushdown import PushdownFilters
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import AIRFLOW_SECRET_TYPE, AirflowClient, AirflowConfig, NotFoundError

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor: which listing, and how far into it."""

    listing: int = 0
    offset: int = 0


@dataclass(frozen=True, slots=True)
class Listing:
    """One paginated API collection to read: path, items key, and filters."""

    path: str
    key: str
    query: tuple[tuple[str, Any], ...] = ()
    # Ignore a 404 (an equality filter named a DAG / run that does not exist).
    missing_ok: bool = False
    # Column values implied by the path (e.g. run_id on Airflow 2 XCom listings).
    context: tuple[tuple[str, Any], ...] = ()


def secret_from(params: ProcessParams[Any]) -> dict[str, Any] | None:
    """Return the first resolved ``airflow`` secret, if any."""
    for secret in params.secrets.of_type(AIRFLOW_SECRET_TYPE):
        return dict(secret)
    return None


def client_from(params: ProcessParams[Any]) -> AirflowClient:
    """Build a client from the secret DuckDB resolved for this call."""
    return AirflowClient(AirflowConfig.from_secret(secret_from(params)))


# --------------------------------------------------------------------- conversions


def ts(value: Any) -> dt.datetime | None:
    """Parse an Airflow ISO-8601 timestamp (``Z`` or offset) into an aware datetime."""
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def duration(item: dict[str, Any], start: str = "start_date", end: str = "end_date") -> float | None:
    """Seconds between two timestamps of an item, or the item's ``duration`` field."""
    if isinstance(item.get("duration"), int | float):
        return float(item["duration"])
    started, ended = ts(item.get(start)), ts(item.get(end))
    if started and ended:
        return (ended - started).total_seconds()
    return None


def json_text(value: Any) -> str | None:
    """Serialize a JSON-able value for a JSON column (``None`` stays NULL)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def to_int(value: Any) -> int | None:
    """Coerce to int, keeping NULL."""
    return None if value is None or value == "" else int(value)


# ------------------------------------------------------------------------ pushdown


class Pushdown:
    """Read-only helpers over the predicates DuckDB pushed into a scan."""

    def __init__(self, filters: PushdownFilters | None) -> None:
        """Wrap the (possibly absent) pushdown filters."""
        self.filters = filters

    def values(self, column: str) -> list[Any] | None:
        """Exact values a column is constrained to (``=`` or ``IN``), else None."""
        if not self.filters:
            return None
        arr = self.filters.get_column_values(column)
        if arr is None:
            return None
        vals = [v for v in arr.to_pylist() if v is not None]
        return vals

    def one(self, column: str) -> Any | None:
        """Return the single value a column must equal, else None."""
        vals = self.values(column)
        return vals[0] if vals and len(vals) == 1 else None

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """``(low, low_inclusive, high, high_inclusive)`` for a column, else None."""
        if not self.filters:
            return None
        b = self.filters.get_column_bounds(column)
        if b is None:
            return None
        lo = b.min_value.as_py() if b.min_value is not None else None
        hi = b.max_value.as_py() if b.max_value is not None else None
        if lo is None and hi is None:
            return None
        return lo, b.min_inclusive, hi, b.max_inclusive


def iso(value: Any) -> str:
    """Format a timestamp/date filter constant the way Airflow's API parses it (UTC)."""
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return aware.astimezone(dt.UTC).isoformat()
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day, tzinfo=dt.UTC).isoformat()
    return str(value)


def date_range_params(pd: Pushdown, column: str, param: str, *, strict_ok: bool) -> list[tuple[str, Any]]:
    """Translate a timestamp range on ``column`` into ``<param>_gte``/``_lte`` (or ``_gt``/``_lt``).

    Args:
        pd: The pushed-down filters.
        column: SQL column name.
        param: Airflow parameter stem, e.g. ``start_date``.
        strict_ok: The API accepts ``_gt``/``_lt`` (Airflow 3); otherwise strict
            bounds widen to inclusive ones and DuckDB's re-check trims the edge.

    Returns:
        Query parameters to add.

    """
    b = pd.bounds(column)
    if b is None:
        return []
    lo, lo_inc, hi, hi_inc = b
    out: list[tuple[str, Any]] = []
    if isinstance(lo, dt.date):
        out.append((f"{param}_gte" if lo_inc or not strict_ok else f"{param}_gt", iso(lo)))
    if isinstance(hi, dt.date):
        out.append((f"{param}_lte" if hi_inc or not strict_ok else f"{param}_lt", iso(hi)))
    return out


# --------------------------------------------------------------------------- base


@init_single_worker
class ApiScan(TableFunctionGenerator[NoArgs, ScanState]):
    """Base class for tables that page through one or more Airflow listings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    class Meta:
        """Overridden by every concrete table."""

        name = "api_scan"
        description = "Base class for Airflow API scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[NoArgs],
        *,
        airflow: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(AIRFLOW_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``airflow`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> ScanState:
        """Start at the first listing's first page."""
        return ScanState()

    @classmethod
    def requests(cls, client: AirflowClient, pd: Pushdown) -> list[Listing]:
        """Return the listings to read for these pushed-down predicates."""
        raise NotImplementedError

    @classmethod
    def rows(
        cls, client: AirflowClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map one page of API items to rows (dicts keyed by column name)."""
        raise NotImplementedError

    @classmethod
    def emit(cls, params: ProcessParams[Any], out: OutputCollector, rows: list[dict[str, Any]]) -> None:
        """Emit rows as one batch in the bound (projected) schema."""
        schema = params.output_schema
        columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: ScanState, out: OutputCollector) -> None:
        """Read one page of the current listing, emit it, and advance the cursor."""
        client = client_from(params)
        listings = cls.requests(client, Pushdown(params.current_pushdown_filters))
        wanted = set(params.output_schema.names)
        while state.listing < len(listings):
            listing = listings[state.listing]
            limit = client.config.page_size
            query = [*listing.query, ("limit", limit), ("offset", state.offset)]
            grouped: dict[str, Any] = {}
            for k, v in query:
                if k in grouped:
                    prev = grouped[k]
                    grouped[k] = [*prev, v] if isinstance(prev, list) else [prev, v]
                else:
                    grouped[k] = v
            try:
                body = client.get(listing.path, grouped)
            except NotFoundError:
                if not listing.missing_ok:
                    raise
                state.listing, state.offset = state.listing + 1, 0
                continue
            items = list(body.get(listing.key) or [])
            total = body.get("total_entries")
            if not items or len(items) < limit or (isinstance(total, int) and state.offset + len(items) >= total):
                state.listing, state.offset = state.listing + 1, 0
            else:
                state.offset += len(items)
            if items:
                cls.emit(params, out, cls.rows(client, items, wanted, listing))
                return
        out.finish()
