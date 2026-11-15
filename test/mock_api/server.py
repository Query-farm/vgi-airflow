#!/usr/bin/env python3
"""Offline replay of Airflow's stable REST API, seeded from recorded snapshots.

``test/fixtures/airflow3.json`` (Airflow 3.4.0, ``/api/v2``) and
``test/fixtures/airflow2.json`` (Airflow 2.11.2, ``/api/v1``) were recorded by
``test/airflow/record.py`` from real ``airflow standalone`` containers running
the DAGs in ``test/airflow/dags``. This server replays those objects verbatim
and re-implements the parts of Airflow the worker relies on, so the tests need
neither Docker nor network:

* the version's URL prefix only (``/api/v1`` *or* ``/api/v2``; the other 404s),
* authentication: HTTP basic (v1) or bearer tokens from ``POST /auth/token`` /
  the static ``MOCK_STATIC_TOKEN`` (v2); anything else is 401,
* ``~`` wildcards in paths, ``limit``/``offset`` paging with ``total_entries``,
* the filters the worker pushes down (state, date ranges, patterns, ...). Any
  query parameter the real endpoint does not accept is a 400, so pushing a
  v2-only filter to v1 fails loudly.

Switches (environment): ``MOCK_FIXTURE`` (path, default airflow3.json),
``MOCK_FLAKY=1`` (first request to every path answers 429 + ``Retry-After: 0``),
``MOCK_STATIC_TOKEN`` (accepted bearer token, default ``astro-fixture-token``).
``GET /__requests`` returns the request log; ``POST /__reset`` clears it.

Usage::

    python3 test/mock_api/server.py --fixture test/fixtures/airflow2.json   # prints PORT:<n>
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
USER, PASSWORD = "admin", "admin-fixture-pw"
PAGING = {"limit", "offset", "order_by"}
DATES = ("logical_date", "execution_date", "start_date", "end_date", "run_after", "updated_at")


def _range_params(stems: tuple[str, ...], ops: tuple[str, ...]) -> set[str]:
    return {f"{s}_{o}" for s in stems for o in ops}


# Accepted query parameters per (version, endpoint), from the servers' OpenAPI documents.
ALLOWED: dict[tuple[str, str], set[str]] = {
    ("v1", "dags"): PAGING | {"tags", "only_active", "paused", "fields", "dag_id_pattern"},
    ("v2", "dags"): PAGING | {"tags", "dag_id_pattern", "paused", "exclude_stale", "last_dag_run_state"},
    ("v1", "dag_runs"): PAGING | {"state", "fields"} | _range_params(DATES, ("gte", "lte")),
    ("v2", "dag_runs"): PAGING
    | {"state", "run_type", "run_id_pattern", "dag_id_pattern", "cursor"}
    | _range_params(DATES, ("gte", "gt", "lte", "lt")),
    ("v1", "task_instances"): PAGING
    | {"state", "pool", "queue", "executor"}
    | _range_params((*DATES, "duration"), ("gte", "lte")),
    ("v2", "task_instances"): PAGING
    | {"state", "pool", "queue", "executor", "task_id", "run_id_pattern", "dag_id_pattern", "try_number", "cursor"}
    | _range_params((*DATES, "duration"), ("gte", "gt", "lte", "lt")),
    ("v1", "tries"): PAGING,
    ("v2", "tries"): {"map_index"},
    ("v1", "ti"): set(),
    ("v2", "ti"): set(),
    ("v1", "logs"): {"full_content", "map_index", "token"},
    ("v2", "logs"): {"full_content", "map_index", "token"},
    ("v1", "xcom_entries"): {"limit", "offset", "map_index", "xcom_key"},
    ("v2", "xcom_entries"): PAGING | {"map_index", "xcom_key", "xcom_key_pattern"},
    ("v1", "assets"): PAGING | {"uri_pattern", "dag_ids"},
    ("v2", "assets"): PAGING | {"uri_pattern", "name_pattern", "dag_ids", "only_active"},
    ("v1", "asset_events"): PAGING
    | {"dataset_id", "source_dag_id", "source_task_id", "source_run_id", "source_map_index"},
    ("v2", "asset_events"): PAGING
    | {"asset_id", "source_dag_id", "source_task_id", "source_run_id", "source_map_index"}
    | _range_params(("timestamp",), ("gte", "gt", "lte", "lt")),
    ("v1", "event_logs"): PAGING
    | {"dag_id", "task_id", "run_id", "map_index", "try_number", "event", "owner", "before", "after"}
    | {"included_events", "excluded_events"},
    ("v2", "event_logs"): PAGING
    | {"dag_id", "task_id", "run_id", "map_index", "try_number", "event", "owner", "before", "after"}
    | {"included_events", "excluded_events"},
    ("v1", "import_errors"): PAGING,
    ("v2", "import_errors"): PAGING | {"filename_pattern"},
    ("v1", "pools"): PAGING,
    ("v2", "pools"): PAGING | {"pool_name_pattern"},
    ("v1", "variables"): PAGING,
    ("v2", "variables"): PAGING | {"variable_key_pattern"},
    ("v1", "connections"): PAGING,
    ("v2", "connections"): PAGING | {"connection_id_pattern"},
    ("v1", "version"): set(),
    ("v2", "version"): set(),
}


class HttpError(Exception):
    """An HTTP error response."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def _ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _pattern(value: str | None, pattern: str) -> bool:
    """Airflow ``*_pattern`` filters: case-insensitive SQL LIKE '%pattern%'."""
    import re

    rx = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern.lower())
    return value is not None and re.search(rx, value.lower()) is not None


class Replay:
    """Fixture data plus Airflow-ish filtering."""

    def __init__(self, fixture: Path) -> None:
        self.snap = json.loads(fixture.read_text())
        self.version = self.snap["api_prefix"].rsplit("/", 1)[-1]
        self.tokens: set[str] = {os.environ.get("MOCK_STATIC_TOKEN", "astro-fixture-token")}
        self.flaky = os.environ.get("MOCK_FLAKY") == "1"
        self.seen_paths: set[str] = set()
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    # ---------------------------------------------------------------- filtering

    @staticmethod
    def _dates(items: list[dict[str, Any]], q: dict[str, list[str]], stems: dict[str, str]) -> list[dict[str, Any]]:
        for stem, field in stems.items():
            for op in ("gte", "gt", "lte", "lt"):
                raw = q.get(f"{stem}_{op}")
                if not raw:
                    continue
                bound = _ts(raw[0])
                assert bound is not None

                def ok(item: dict[str, Any], op: str = op, bound: dt.datetime = bound, field: str = field) -> bool:
                    v = _ts(item.get(field))
                    if v is None:
                        return False
                    return {"gte": v >= bound, "gt": v > bound, "lte": v <= bound, "lt": v < bound}[op]

                items = [i for i in items if ok(i)]
        return items

    def _page(self, key: str, items: list[Any], q: dict[str, list[str]]) -> dict[str, Any]:
        limit = int(q.get("limit", ["100"])[0])
        offset = int(q.get("offset", ["0"])[0])
        return {key: items[offset : offset + limit], "total_entries": len(items)}

    def route(self, path: str, q: dict[str, list[str]]) -> tuple[str, Any]:
        """Return (endpoint name, body) for an API path below the prefix."""
        v2 = self.version == "v2"
        s = self.snap
        parts = [urllib.parse.unquote(p) for p in path.strip("/").split("/")]

        def eq(value: Any, want: str) -> bool:
            return want == "~" or str(value) == want

        if parts == ["version"]:
            return "version", s["version"]
        if parts == ["dags"]:
            items = list(s["dags"])
            if "dag_id_pattern" in q:
                items = [d for d in items if _pattern(d["dag_id"], q["dag_id_pattern"][0])]
            if "paused" in q:
                items = [d for d in items if d["is_paused"] == (q["paused"][0] == "true")]
            if q.get("exclude_stale", ["true"])[0] == "true" and v2:
                items = [d for d in items if not d.get("is_stale")]
            if q.get("only_active", ["true"])[0] == "true" and not v2:
                items = [d for d in items if d.get("is_active")]
            return "dags", self._page("dags", items, q)
        if len(parts) == 3 and parts[0] == "dags" and parts[2] == "dagRuns":
            if parts[1] != "~" and not any(d["dag_id"] == parts[1] for d in s["dags"]):
                raise HttpError(404, f"The DAG with dag_id: `{parts[1]}` was not found")
            items = [r for r in s["dag_runs"] if eq(r["dag_id"], parts[1])]
            if "state" in q:
                items = [r for r in items if r["state"] in q["state"]]
            if "run_type" in q:
                items = [r for r in items if r["run_type"] in q["run_type"]]
            if "run_id_pattern" in q:
                items = [r for r in items if _pattern(r["dag_run_id"], q["run_id_pattern"][0])]
            items = self._dates(
                items,
                q,
                {
                    "logical_date": "logical_date",
                    "execution_date": "execution_date",
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
            )
            return "dag_runs", self._page("dag_runs", items, q)
        if len(parts) == 5 and parts[0] == "dags" and parts[2] == "dagRuns" and parts[4] == "taskInstances":
            if parts[3] != "~" and not any(
                eq(r["dag_id"], parts[1]) and r["dag_run_id"] == parts[3] for r in s["dag_runs"]
            ):
                raise HttpError(404, "DagRun not found")
            items = [t for t in s["task_instances"] if eq(t["dag_id"], parts[1]) and eq(t["dag_run_id"], parts[3])]
            for name in ("state", "pool", "task_id"):
                if name in q:
                    items = [t for t in items if str(t.get(name)) in q[name]]
            if "run_id_pattern" in q:
                items = [t for t in items if _pattern(t["dag_run_id"], q["run_id_pattern"][0])]
            items = self._dates(
                items,
                q,
                {
                    "logical_date": "logical_date",
                    "execution_date": "execution_date",
                    "start_date": "start_date",
                    "end_date": "end_date",
                },
            )
            return "task_instances", self._page("task_instances", items, q)
        if len(parts) >= 6 and parts[0] == "dags" and parts[2] == "dagRuns" and parts[4] == "taskInstances":
            dag, run, task, rest = parts[1], parts[3], parts[5], parts[6:]
            if task != "~" and rest and rest[0].isdigit():
                rest = rest[1:]
            if rest == ["xcomEntries"]:
                items = []
                runs_by_date = {(r["dag_id"], r.get("execution_date")): r["dag_run_id"] for r in s["dag_runs"]}
                for x in s["xcom_entries"]:
                    run_id = x.get("run_id") or runs_by_date.get((x["dag_id"], x.get("execution_date")))
                    if eq(x["dag_id"], dag) and eq(run_id, run) and eq(x["task_id"], task):
                        items.append(x)
                if "xcom_key" in q:
                    items = [x for x in items if x["key"] == q["xcom_key"][0]]
                if "map_index" in q:
                    items = [x for x in items if str(x["map_index"]) == q["map_index"][0]]
                return "xcom_entries", self._page("xcom_entries", items, q)
            key = f"{dag}|{run}|{task}"
            if not rest:
                for t in s["task_instances"]:
                    if (t["dag_id"], t["dag_run_id"], t["task_id"]) == (dag, run, task):
                        return "ti", t
                raise HttpError(404, "Task instance not found")
            if rest == ["tries"]:
                if key not in s["tries"]:
                    raise HttpError(404, "Task instance not found")
                return "tries", self._page("task_instances", s["tries"][key], q)
            if len(rest) == 2 and rest[0] == "logs":
                log = s["logs"].get(f"{key}|{rest[1]}")
                if log is None:
                    raise HttpError(404, "TaskInstance not found")
                return "logs", log
        if (v2 and parts[:1] == ["assets"]) or (not v2 and parts[:1] == ["datasets"]):
            if parts[1:] == ["events"]:
                key = "asset_events" if v2 else "dataset_events"
                items = list(s["asset_events"])
                id_param = "asset_id" if v2 else "dataset_id"
                if id_param in q:
                    items = [e for e in items if str(e.get(id_param)) == q[id_param][0]]
                for name in ("source_dag_id", "source_task_id", "source_run_id"):
                    if name in q:
                        items = [e for e in items if e.get(name) == q[name][0]]
                items = self._dates(items, q, {"timestamp": "timestamp"})
                return "asset_events", self._page(key, items, q)
            if len(parts) == 1:
                items = list(s["assets"])
                if "uri_pattern" in q:
                    items = [a for a in items if _pattern(a["uri"], q["uri_pattern"][0])]
                return "assets", self._page("assets" if v2 else "datasets", items, q)
        if parts == ["eventLogs"]:
            items = list(s["event_logs"])
            for name in ("dag_id", "task_id", "run_id", "owner", "event"):
                if name in q:
                    items = [e for e in items if e.get(name) == q[name][0]]
            if "included_events" in q:
                wanted = set(q["included_events"][0].split(","))
                items = [e for e in items if e.get("event") in wanted]
            if "after" in q:
                bound = _ts(q["after"][0])
                items = [e for e in items if (w := _ts(e.get("when"))) is not None and bound and w > bound]
            if "before" in q:
                bound = _ts(q["before"][0])
                items = [e for e in items if (w := _ts(e.get("when"))) is not None and bound and w < bound]
            return "event_logs", self._page("event_logs", items, q)
        simple = {
            "importErrors": ("import_errors", "filename", "filename_pattern"),
            "pools": ("pools", "name", "pool_name_pattern"),
            "variables": ("variables", "key", "variable_key_pattern"),
            "connections": ("connections", "connection_id", "connection_id_pattern"),
        }
        if len(parts) == 1 and parts[0] in simple:
            name, field, pattern = simple[parts[0]]
            items = list(s[name])
            if pattern in q:
                items = [i for i in items if _pattern(i.get(field), q[pattern][0])]
            return name, self._page(name, items, q)
        raise HttpError(404, f"no such endpoint: {path}")


def make_handler(replay: Replay) -> type[BaseHTTPRequestHandler]:
    """Bind a request handler to one replay."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def _authorized(self) -> bool:
            header = self.headers.get("Authorization", "")
            if replay.version == "v1":
                want = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
                return header == f"Basic {want}"
            return header.startswith("Bearer ") and header[7:] in replay.tokens

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/__reset":
                with replay.lock:
                    replay.requests.clear()
                self._send(200, {"ok": True})
                return
            if parsed.path == "/auth/token" and replay.version == "v2":
                length = int(self.headers.get("Content-Length") or 0)
                try:
                    creds = json.loads(self.rfile.read(length) or b"{}")
                except ValueError:
                    creds = {}
                with replay.lock:
                    replay.requests.append({"method": "POST", "path": parsed.path, "query": {}})
                if creds.get("username") == USER and creds.get("password") == PASSWORD:
                    exp = int(dt.datetime.now(dt.UTC).timestamp()) + 3600
                    claims = base64.urlsafe_b64encode(json.dumps({"sub": USER, "exp": exp}).encode()).decode()
                    token = f"eyJhbGciOiJub25lIn0.{claims.rstrip('=')}.mock{len(replay.tokens)}"
                    replay.tokens.add(token)
                    self._send(201, {"access_token": token})
                else:
                    self._send(401, {"detail": "Invalid credentials"})
                return
            self._send(405, {"detail": "Method Not Allowed"})

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(parsed.query)
            if parsed.path == "/__requests":
                with replay.lock:
                    self._send(200, list(replay.requests))
                return
            if parsed.path == "/health":
                self._send(200, {"status": "ok"})
                return
            with replay.lock:
                replay.requests.append({"method": "GET", "path": parsed.path, "query": q})
            prefix = f"/api/{replay.version}"
            if not parsed.path.startswith(prefix + "/"):
                self._send(404, {"detail": "Not Found"})
                return
            if replay.flaky:
                with replay.lock:
                    first = parsed.path not in replay.seen_paths
                    replay.seen_paths.add(parsed.path)
                if first:
                    self._send(429, {"detail": "Too Many Requests"}, {"Retry-After": "0"})
                    return
            if not self._authorized():
                self._send(401 if replay.version == "v2" else 403, {"detail": "Not authenticated"})
                return
            try:
                endpoint, body = replay.route(parsed.path[len(prefix) :], q)
                unknown = set(q) - ALLOWED[(replay.version, endpoint)]
                if unknown:
                    raise HttpError(400, f"unsupported query parameter(s) for {endpoint}: {sorted(unknown)}")
            except HttpError as err:
                self._send(err.status, {"detail": err.detail})
                return
            self._send(200, body)

    return Handler


def main() -> None:
    """Serve until killed; print ``PORT:<n>`` once listening."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=0)
    default = os.environ.get("MOCK_FIXTURE", str(HERE.parent / "fixtures" / "airflow3.json"))
    ap.add_argument("--fixture", type=Path, default=Path(default))
    args = ap.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(Replay(args.fixture)))
    print(f"PORT:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
