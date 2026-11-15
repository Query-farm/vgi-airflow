"""Record a fixture snapshot from a live Airflow (2.x ``/api/v1`` or 3.x ``/api/v2``).

Run once against a disposable ``airflow standalone`` container (see the README's
Testing section); the output feeds ``test/mock_api/server.py``, which
replays it (with Airflow's own filtering and paging rules) so tests never need
Docker::

    python test/airflow/record.py --base-url http://localhost:18080 \
        --username admin --password admin-fixture-pw --out test/fixtures/airflow3.json

Only the fixture container's data is recorded. Secrets the fixture planted
(variable/connection passwords) are already masked by Airflow itself.
"""

from __future__ import annotations

import argparse
import base64
import json
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class Api:
    """Tiny authenticated GET client for one Airflow API version."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        """Detect the API version and authenticate."""
        self.base = base_url.rstrip("/")
        self.headers = {"Accept": "application/json"}
        try:
            body = json.dumps({"username": username, "password": password}).encode()
            req = urllib.request.Request(
                f"{self.base}/auth/token", data=body, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                self.headers["Authorization"] = "Bearer " + json.load(resp)["access_token"]
            self.prefix = "/api/v2"
        except urllib.error.HTTPError:
            raw = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.headers["Authorization"] = f"Basic {raw}"
            self.prefix = "/api/v1"

    def get(self, path: str, **query: Any) -> Any:
        """GET one path under the API prefix."""
        qs = urllib.parse.urlencode(query, doseq=True)
        url = f"{self.base}{self.prefix}{path}" + (f"?{qs}" if qs else "")
        with urllib.request.urlopen(urllib.request.Request(url, headers=self.headers), timeout=60) as resp:
            return json.load(resp)

    def collect(self, path: str, key: str, **query: Any) -> list[Any]:
        """Page through a collection endpoint and return every item."""
        items: list[Any] = []
        while True:
            page = self.get(path, limit=100, offset=len(items), **query)
            items.extend(page[key])
            if not page[key] or len(items) >= page["total_entries"]:
                return items


def record(api: Api) -> dict[str, Any]:
    """Snapshot every endpoint the worker reads."""
    v2 = api.prefix == "/api/v2"
    q = urllib.parse.quote
    snap: dict[str, Any] = {"api_prefix": api.prefix, "version": api.get("/version")}
    snap["dags"] = api.collect("/dags", "dags")
    snap["dag_runs"] = api.collect("/dags/~/dagRuns", "dag_runs", order_by="id")
    snap["task_instances"] = api.collect("/dags/~/dagRuns/~/taskInstances", "task_instances")
    snap["tries"] = {}
    snap["logs"] = {}
    for ti in snap["task_instances"]:
        base = f"/dags/{q(ti['dag_id'])}/dagRuns/{q(ti['dag_run_id'], safe='')}/taskInstances/{q(ti['task_id'])}"
        ti_key = f"{ti['dag_id']}|{ti['dag_run_id']}|{ti['task_id']}"
        try:
            snap["tries"][ti_key] = api.collect(f"{base}/tries", "task_instances")
        except urllib.error.HTTPError as err:
            print(f"tries {ti_key}: HTTP {err.code}")
        for attempt in range(1, (ti.get("try_number") or 0) + 1):
            try:
                snap["logs"][f"{ti_key}|{attempt}"] = api.get(f"{base}/logs/{attempt}", full_content="true")
            except urllib.error.HTTPError as err:
                print(f"log {ti_key}|{attempt}: HTTP {err.code}")
    snap["xcom_entries"] = api.collect("/dags/~/dagRuns/~/taskInstances/~/xcomEntries", "xcom_entries")
    assets = "/assets" if v2 else "/datasets"
    snap["assets"] = api.collect(assets, "assets" if v2 else "datasets")
    snap["asset_events"] = api.collect(f"{assets}/events", "asset_events" if v2 else "dataset_events")
    snap["event_logs"] = api.collect("/eventLogs", "event_logs")
    snap["import_errors"] = api.collect("/importErrors", "import_errors")
    snap["pools"] = api.collect("/pools", "pools")
    snap["variables"] = api.collect("/variables", "variables")
    snap["connections"] = api.collect("/connections", "connections")
    return snap


def main() -> None:
    """Parse arguments, record, write JSON."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()
    snap = record(Api(args.base_url, args.username, args.password))
    args.out.write_text(json.dumps(snap, indent=1, sort_keys=True) + "\n")
    print(f"wrote {args.out} ({snap['api_prefix']}, Airflow {snap['version']['version']})")


if __name__ == "__main__":
    main()
