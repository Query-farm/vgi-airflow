"""Unit tests for the client, pushdown translation, masking, log parsing and status mapping.

The SQL-level tests (through DuckDB and the real vgi extension) live in test/sql.
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.request
from typing import Any

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from airflow_worker.client import AirflowClient, AirflowConfig, AirflowError, _retry_delay
from airflow_worker.functions import parse_log
from airflow_worker.masking import mask_extra, mask_variable
from airflow_worker.scan import Pushdown
from airflow_worker.tables import DagRuns, Dags, TaskInstances, XcomEntries, dag_run_listings
from airflow_worker.unified import UnifiedRuns, UnifiedTaskRuns, run_status, task_status, trigger

UTC = dt.UTC


def _client(base: str, **fields: Any) -> AirflowClient:
    return AirflowClient(AirflowConfig.from_secret({"base_url": base, **fields}))


def _offline(version: str) -> AirflowClient:
    return _client("http://airflow.invalid", token="t", api_version=version)


def _pd(*filters: Any) -> Pushdown:
    return Pushdown(PushdownFilters(filters=tuple(filters)))


def _eq(col: str, value: Any, typ: pa.DataType | None = None) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=ComparisonOp.EQ, value=pa.scalar(value, typ))


def _cmp(col: str, op: ComparisonOp, value: Any) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value, pa.timestamp("us", tz="UTC")))


def _in(col: str, values: list[Any]) -> InFilter:
    return InFilter(column_name=col, column_index=0, values=pa.array(values))


# ------------------------------------------------------------------ configuration


def test_config_requires_secret() -> None:
    with pytest.raises(AirflowError, match="no Airflow credentials"):
        AirflowConfig.from_secret(None)


def test_config_strips_api_suffix_and_validates() -> None:
    cfg = AirflowConfig.from_secret({"base_url": pa.scalar("http://h:8080/api/v1/"), "token": pa.scalar("x")})
    assert cfg.base_url == "http://h:8080"
    with pytest.raises(AirflowError, match="PLATFORM 'astronomer'"):
        AirflowConfig.from_secret({"base_url": "http://h", "username": "u", "password": "p", "platform": "astronomer"})
    with pytest.raises(AirflowError, match="mwaa"):
        AirflowConfig.from_secret({"base_url": "http://h", "token": "t", "platform": "mwaa"})
    with pytest.raises(AirflowError, match="PAGE_SIZE"):
        AirflowConfig.from_secret({"base_url": "http://h", "token": "t", "page_size": 5000})


def test_config_repr_hides_secrets() -> None:
    cfg = AirflowConfig.from_secret({"base_url": "http://h", "username": "u", "password": "hunter2", "token": "tok123"})
    assert "hunter2" not in repr(cfg) and "tok123" not in repr(cfg)


def test_retry_delay_honours_retry_after() -> None:
    assert _retry_delay("3", 1) == 3.0
    assert _retry_delay("9999", 1) == 60.0
    assert _retry_delay(None, 2) == 2.0


# ------------------------------------------------------------- client vs replays


def test_v3_version_detection_and_token_exchange(mock_v3: str) -> None:
    client = _client(mock_v3, username="admin", password="admin-fixture-pw")
    assert client.version == "v2"
    body = client.get("/dags")
    assert body["total_entries"] == 3


def test_v3_bad_password(mock_v3: str) -> None:
    client = _client(mock_v3, username="admin", password="nope")
    with pytest.raises(AirflowError, match="auth/token failed with HTTP 401"):
        client.get("/dags")


def test_v3_static_token(mock_v3: str) -> None:
    assert _client(mock_v3, token="astro-fixture-token", platform="astronomer").get("/pools")["total_entries"] == 2
    with pytest.raises(AirflowError, match="HTTP 401"):
        _client(mock_v3, token="bad").get("/pools")


def test_v2_falls_back_to_v1_basic_auth(mock_v2: str) -> None:
    client = _client(mock_v2, username="admin", password="admin-fixture-pw")
    assert client.version == "v1"
    assert client.get("/version")["version"] == "2.11.2"


def test_not_airflow(mock_v3: str) -> None:
    client = _client(f"{mock_v3}/nothing-here", token="x")
    with pytest.raises(AirflowError, match="does not look like an Airflow API server"):
        _ = client.version


def test_429_is_retried(mock_v3: str) -> None:
    # MOCK_FLAKY answers 429 to the first request on each path; the client still succeeds.
    client = _client(mock_v3, token="astro-fixture-token")
    assert client.get("/importErrors")["total_entries"] == 1
    with urllib.request.urlopen(f"{mock_v3}/__requests") as resp:
        log = json.load(resp)
    assert [r["path"] for r in log].count("/api/v2/importErrors") == 2


def test_unknown_query_parameter_is_rejected_by_replay(mock_v2: str) -> None:
    client = _client(mock_v2, username="admin", password="admin-fixture-pw")
    with pytest.raises(AirflowError, match="HTTP 400"):
        client.get("/dags/~/dagRuns", {"run_type": "manual"})


# ------------------------------------------------------------- pushdown → params


def test_dag_runs_v2_pushdown() -> None:
    pd = _pd(
        _in("dag_id", ["a", "b"]),
        _in("state", ["failed", "success"]),
        _cmp("start_date", ComparisonOp.GT, dt.datetime(2026, 11, 1, tzinfo=UTC)),
        _cmp("start_date", ComparisonOp.LE, dt.datetime(2026, 11, 30, tzinfo=UTC)),
    )
    listings = DagRuns.requests(_offline("v2"), pd)
    assert [x.path for x in listings] == ["/dags/a/dagRuns", "/dags/b/dagRuns"]
    q = listings[0].query
    assert ("state", "failed") in q and ("state", "success") in q
    assert ("start_date_gt", "2026-11-01T00:00:00+00:00") in q
    assert ("start_date_lte", "2026-11-30T00:00:00+00:00") in q
    assert all(x.missing_ok for x in listings)


def test_dag_runs_v1_widens_strict_bounds_and_renames_logical_date() -> None:
    pd = _pd(
        _cmp("logical_date", ComparisonOp.GT, dt.datetime(2026, 11, 1, tzinfo=UTC)),
        _eq("run_type", "manual"),
        _eq("run_id", "r1"),
    )
    (listing,) = dag_run_listings(_offline("v1"), pd)
    assert listing.path == "/dags/~/dagRuns"
    assert listing.query == (("execution_date_gte", "2026-11-01T00:00:00+00:00"),)


def test_naive_and_offset_timestamps_are_sent_in_utc() -> None:
    est = dt.timezone(dt.timedelta(hours=-5))
    pd = _pd(_cmp("end_date", ComparisonOp.GE, dt.datetime(2026, 11, 1, 19, 0, tzinfo=est)))
    (listing,) = DagRuns.requests(_offline("v2"), pd)
    assert ("end_date_gte", "2026-11-02T00:00:00+00:00") in listing.query


def test_task_instances_paths() -> None:
    client = _offline("v2")
    one = TaskInstances.requests(client, _pd(_eq("dag_id", "d"), _eq("run_id", "r:1+x"), _eq("task_id", "t")))
    assert [x.path for x in one] == ["/dags/d/dagRuns/r%3A1%2Bx/taskInstances"]
    assert ("task_id", "t") in one[0].query
    any_dag = TaskInstances.requests(client, _pd(_eq("run_id", "r1")))
    assert any_dag[0].path == "/dags/~/dagRuns/~/taskInstances"
    assert ("run_id_pattern", "r1") in any_dag[0].query
    v1 = TaskInstances.requests(_offline("v1"), _pd(_eq("task_id", "t"), _eq("run_id", "r1")))
    assert v1[0].query == ()


def test_dags_pushdown() -> None:
    (v2,) = Dags.requests(_offline("v2"), _pd(_eq("is_paused", True), _eq("is_active", True)))
    assert ("paused", "true") in v2.query and ("exclude_stale", "true") in v2.query
    (v1,) = Dags.requests(_offline("v1"), _pd())
    assert ("only_active", "false") in v1.query


def test_xcom_fan_out_and_context() -> None:
    listings = XcomEntries.requests(
        _offline("v1"), _pd(_eq("dag_id", "d"), _in("run_id", ["r1", "r2"]), _eq("key", "k"))
    )
    assert [x.path for x in listings] == [
        "/dags/d/dagRuns/r1/taskInstances/~/xcomEntries",
        "/dags/d/dagRuns/r2/taskInstances/~/xcomEntries",
    ]
    assert listings[1].context == (("run_id", "r2"),)
    assert ("xcom_key", "k") in listings[0].query


def test_unified_pushdown() -> None:
    client = _offline("v2")
    (listing,) = UnifiedRuns.requests(client, _pd(_eq("run_id", "etl/run_1"), _eq("status", "failed")))
    assert listing.path == "/dags/etl/dagRuns"
    assert ("run_id_pattern", "run_1") in listing.query and ("state", "failed") in listing.query
    assert UnifiedRuns.requests(client, _pd(_eq("status", "cancelled"))) == []
    assert UnifiedRuns.requests(client, _pd(_eq("run_id", "a/x"), _eq("pipeline", "b"))) == []
    (task_listing,) = UnifiedTaskRuns.requests(client, _pd(_eq("run_id", "etl/run_1"), _eq("status", "skipped")))
    assert task_listing.path == "/dags/etl/dagRuns/run_1/taskInstances"
    assert ("state", "skipped") in task_listing.query and ("state", "upstream_failed") in task_listing.query


# ------------------------------------------------------------------------- masking


def test_variable_masking() -> None:
    assert mask_variable("warehouse_region", "us-east-1", reveal=False) == ("***", True)
    assert mask_variable("warehouse_region", "us-east-1", reveal=True) == ("us-east-1", False)
    assert mask_variable("slack_api_token", "xoxb-1", reveal=True) == ("***", True)
    assert mask_variable("db_password", "hunter2", reveal=True) == ("***", True)
    # Value-based detection catches credentials under innocent keys.
    assert mask_variable("webhook", "https://u:pw@example.com/x", reveal=True) == ("***", True)
    assert mask_variable("bot", "xoxb-123-abc", reveal=True) == ("***", True)
    assert mask_variable("id_token_like", "eyJhbGciOi.eyJzdWIi.sig", reveal=True) == ("***", True)
    assert mask_variable("empty", None, reveal=True) == (None, False)


def test_extra_masking() -> None:
    extra = json.dumps({"sslmode": "require", "api_key": "k", "nested": {"client_secret": "s", "region": "x"}})
    assert json.loads(mask_extra(extra) or "") == {
        "api_key": "***",
        "nested": {"client_secret": "***", "region": "x"},
        "sslmode": "require",
    }
    assert mask_extra("not json") is None
    assert mask_extra('"a string"') is None
    assert mask_extra(None) is None


# ----------------------------------------------------------------------- log parsing


def test_parse_v1_text_log() -> None:
    text = "[('host', \" INFO - ::group::x\\n[2026-11-15T05:53:55.577+0000] {mixin.py:190} ERROR - boom\\nTraceback\")]"
    lines = parse_log(text)
    assert [(x["level"], x["message"]) for x in lines] == [
        ("info", "::group::x"),
        ("error", "boom"),
        (None, "Traceback"),
    ]
    assert lines[1]["timestamp"] == dt.datetime(2026, 11, 15, 5, 53, 55, 577000, tzinfo=UTC)
    assert lines[1]["logger"] == "mixin.py:190"


def test_parse_v2_structured_log() -> None:
    lines = parse_log(
        [
            {"event": "hello", "timestamp": "2026-11-15T05:50:39.263535Z", "level": "info", "logger": "task.stdout"},
            {"event": "failed", "level": "error", "error_detail": [{"exc_type": "ValueError"}], "dag_id": "d"},
            {"event": "::endgroup::"},
        ]
    )
    assert lines[0]["message"] == "hello" and lines[0]["logger"] == "task.stdout"
    assert json.loads(lines[1]["details"]) == {"error_detail": [{"exc_type": "ValueError"}]}
    assert lines[2]["level"] is None and lines[2]["details"] is None


# -------------------------------------------------------------------- unified maps


def test_status_and_trigger_maps() -> None:
    assert [run_status(s) for s in ("queued", "running", "success", "failed", None)] == [
        "queued",
        "running",
        "success",
        "failed",
        "queued",
    ]
    assert task_status("upstream_failed") == "skipped"
    assert task_status("removed") == "cancelled"
    assert task_status("up_for_retry") == "queued"
    assert task_status(None) == "queued"
    assert trigger("scheduled", "timetable") == "schedule"
    assert trigger("manual", "rest_api") == "api"
    assert trigger("manual", "ui") == "manual"
    assert trigger("asset_triggered", "asset") == "event"
    assert trigger("dataset_triggered", None) == "event"
    assert trigger("backfill", None) == "other"
