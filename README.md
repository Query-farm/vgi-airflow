<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Apache Airflow DAGs, Runs and Task Logs in DuckDB

> **vgi-airflow** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-airflow/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Apache Airflow's stable REST API as SQL tables. Query DAGs, DAG runs, task instances and
every retry, task logs, XComs, assets and their events, pools, variables, connections,
import errors and the audit log. It works with **Airflow 2.x** (`/api/v1`) and **Airflow 3.x**
(`/api/v2`), self-managed or on Astronomer or Cloud Composer. The worker detects the version and
returns the same columns from both.

```sql
-- Which DAGs failed most often this month, and how long did those runs take?
SELECT dag_id, count(*) AS failures, round(avg(duration_seconds)) AS avg_s
FROM airflow.main.dag_runs
WHERE state = 'failed' AND start_date >= TIMESTAMPTZ '2026-11-01'
GROUP BY dag_id ORDER BY failures DESC;
```

The `state` filter and the date range go to the API as query parameters, so the worker fetches
only the failed November runs.

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'airflow' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-airflow@v0.1.2 vgi-airflow-worker');

-- Credentials go in a DuckDB secret, created after ATTACH (which registers the secret type).
CREATE SECRET af (TYPE airflow, BASE_URL 'http://localhost:8080',
                  USERNAME 'admin', PASSWORD '...');

SELECT dag_id, is_paused, schedule, tags FROM airflow.main.dags ORDER BY dag_id;
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same
`ATTACH`.

**From a checkout:** `make build`, then use
`LOCATION '/abs/path/to/vgi-airflow/.venv/bin/vgi-airflow-worker'`.

## Credentials

Every table reads the one `airflow` secret in the session:

| Field | Meaning |
|---|---|
| `BASE_URL` (required) | Root URL of the webserver / API server, without `/api/v1` or `/api/v2` |
| `TOKEN` | Bearer token: an Airflow 3 JWT, an Astronomer deployment API token, or a Cloud Composer OAuth access token. Takes precedence over username/password |
| `USERNAME`, `PASSWORD` | Basic auth on Airflow 2. On Airflow 3 the worker exchanges them at `POST /auth/token` for a JWT, caches it per process and refreshes it on expiry or a 401 |
| `AUTH` | `auto` (default), `token`, `basic` or `password` (forces the `/auth/token` exchange) |
| `API_VERSION` | `auto` (default: probe `/api/v2/version`, fall back to `/api/v1`), `v1` or `v2` |
| `PLATFORM` | `airflow` (default), `astronomer` or `composer` (both require `TOKEN`). `mwaa` is reserved and not supported yet |
| `PAGE_SIZE` | Items per API page, 1–1000 (default 100) |
| `TIMEOUT` | Per-request timeout in seconds (default 30) |

```sql
-- Astronomer
CREATE SECRET astro (TYPE airflow, BASE_URL 'https://<org>.astronomer.run/<deployment>',
                     TOKEN '<deployment API token>', PLATFORM 'astronomer');
-- Cloud Composer: the Airflow web server URL plus `gcloud auth print-access-token`
CREATE SECRET composer (TYPE airflow, BASE_URL 'https://<id>.composer.googleusercontent.com',
                        TOKEN '<access token>', PLATFORM 'composer');
```

Amazon MWAA has not been added yet. It needs AWS SigV4 signing (boto3), and the worker avoids
heavy cloud SDK dependencies. Cloud Composer works with a token you obtain yourself; the worker
does not mint one (no `google-auth` dependency). The secret field is named `PLATFORM` because
DuckDB reserves `provider` on every secret.

## Tables and functions

**`airflow.main`**: one table per REST collection.

| Object | What it holds | Pushed to the API |
|---|---|---|
| `dags` | DAG definitions: schedule, paused/active, owners, tags, next run | `dag_id =` (pattern), `is_paused`, `is_active` |
| `dag_runs` | Run history, key `(dag_id, run_id)` | `dag_id` (= / IN → URL path), `state` (= / IN), `logical_date` / `start_date` / `end_date` ranges; on 3.x also `run_type`, `run_id` |
| `task_instances` | Latest try of each task instance | `dag_id`, `run_id` (path), `state`, `pool`, date ranges; on 3.x also `task_id` |
| `task_instance_tries` | Every attempt that started (retries included) | `dag_id`, `run_id`, `task_id` on the instance listing, then one tries request per instance |
| `xcom_entries` | XCom keys pushed by tasks (no values) | `dag_id`, `run_id`, `task_id` (path, IN fans out), `key`, `map_index` |
| `assets` | Assets (2.x datasets) with producing tasks and consuming DAGs | `uri =` (pattern) |
| `asset_events` | Asset updates and the runs they triggered | `asset_id`, `source_dag_id/task_id/run_id`; on 3.x also `timestamp` ranges |
| `event_logs` | The audit log | `dag_id`, `task_id`, `run_id`, `owner`, `event` (= / IN), `event_time` ranges |
| `import_errors` | DAG files that fail to import, with tracebacks | none |
| `pools` | Pool slot usage | 3.x: `name =` |
| `variables` | Variable keys; **values always `***`** | 3.x: `key =` |
| `connections` | Connections; **never a password**, secret-looking `extra` keys masked | 3.x: `connection_id =` |
| `task_logs(dag_id, run_id, task_id, try_number := NULL, map_index := -1)` | Log lines parsed into timestamp, level, logger, message, details | each attempt is one request |
| `variable_values(reveal := false)` | Variables; with `reveal := true`, values whose key and value don't look secret | none |

**`airflow.unified`**: the cross-orchestrator shape that the Dagster, Prefect, Temporal,
Databricks, dbt and GitHub workers also serve, for `UNION ALL`:

- `runs(orchestrator, run_id, pipeline, status, raw_status, trigger, queued_at, started_at, ended_at, duration_seconds, url)`
- `task_runs(orchestrator, run_id, task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url)`
- `unified_status(state)`: the same state mapping as a scalar, with no API calls

`run_id` is `'<dag_id>/<dag_run_id>'`, because Airflow run ids are only unique within a DAG.
`task_run_id` adds `/<task_id>` (and `/<map_index>` for mapped tasks). `status` is one of `queued`,
`running`, `success`, `failed`, `cancelled` or `skipped`. `upstream_failed` maps to `skipped` (the
failure is counted once, on the upstream task), `removed`/`shutdown` to `cancelled`, and
`up_for_retry`/`scheduled` to `queued`. `trigger` is `schedule`, `manual`, `api` (3.x `rest_api`),
`event` (asset/dataset triggered) or `other`. Filters on `pipeline`, `run_id`, `status` and the
`started_at`/`ended_at` ranges are pushed down too.

```sql
-- Task outcomes per DAG, joined through the unified keys
SELECT r.pipeline, t.status, count(*) AS tasks
FROM airflow.unified.task_runs t JOIN airflow.unified.runs r USING (run_id)
GROUP BY ALL ORDER BY ALL;

-- The error lines of every attempt of a failing task
SELECT try_number, timestamp, message
FROM airflow.main.task_logs('flaky_report', 'manual_report_1', 'render')
WHERE level = 'error' ORDER BY try_number, line_number;
```

### How pushdown and paging work

DuckDB hands the worker each query's simple predicates. Equality and `IN` on `dag_id` (and
`run_id` / `task_id`) become URL path segments, with one listing per value. `state` becomes the API's
repeated `state` parameter, and timestamp ranges become `*_gte` / `*_lte` (`*_gt` / `*_lt` on
Airflow 3, which accepts strict bounds). DuckDB re-applies every predicate to the returned rows.
Translation therefore only has to narrow the API request, never match it exactly. A strict bound
that Airflow 2 can't express is widened to an inclusive one and trimmed afterwards. A filter
naming a DAG or run that doesn't exist gives an empty result, not an error.

Listings page with `limit`/`offset`. The cursor (listing index + offset) is the table function's
state and contains no credentials. It goes back to DuckDB with every batch, so a scan over the
HTTP transport resumes on any worker process. 429 and 502/503/504 responses are retried up to 5
times, honouring `Retry-After` (seconds or HTTP date, capped at 60 s). Other errors fail the query
with the HTTP status in the message.

### Secrets never leak

Airflow 2's `GET /variables` returns every value verbatim, secrets included (we saw this on the
2.11 fixture). The worker masks values itself. `variables` always shows `***`.
`variable_values(reveal := true)` shows a value only when the key has no secret-like word in it
(password, token, secret, key, auth, conn, …) and the value doesn't look like a credential (JWT,
`xox*-`/`ghp_`/`AKIA`/`sk-` prefixes, private keys, URLs with a password). `connections` has no
password column, only `has_password` (Airflow 3). Secret-looking keys in `extra` become `***`, and
an `extra` that isn't a JSON object is withheld. The secret's `TOKEN` and `PASSWORD` are marked
`redact`, so DuckDB hides them in `duckdb_secrets()`.

## Testing

```bash
make check      # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
```

The SQL suite (`test/sql/*.test`) runs through the real `vgi` extension in Haybarn 2.0. It runs
against `test/mock_api/server.py`, which replays real API responses recorded from two disposable
containers:

- **Airflow 3.4.0** (`apache/airflow:3.4.0`, `airflow standalone`, about 0.8 GB RSS):
  `test/fixtures/airflow3.json`
- **Airflow 2.11.2** (`apache/airflow:2.11.2`, `airflow standalone`, about 1.6 GB RSS):
  `test/fixtures/airflow2.json`

Both ran the DAGs in `test/airflow/dags`: an ETL that publishes the `s3://warehouse/orders`
asset/dataset, a consumer scheduled on it, a report whose `render` task fails after one retry
(so `publish` is `upstream_failed`), and a file that fails to import. The replay implements
Airflow's `~` wildcards, paging, auth (basic on v1; `/auth/token` and static bearer tokens on v2)
and the filters the worker sends. It answers **400 to any query parameter the real endpoint does
not accept**, so sending a 3.x-only filter to 2.x fails the tests. The v2 replay also answers 429
to the first request on every path, which exercises the retry path in every run. CI needs no
Docker.

To re-record (Docker, ≤2.5 GB):

```bash
docker run -d --name vgi-airflow-af3 --memory 2.5g -p 18080:8080 \
  -v $PWD/test/airflow/dags:/opt/airflow/dags:ro \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False -e AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin \
  -e AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/tmp/pw.json -e AIRFLOW__API__WORKERS=1 \
  apache/airflow:3.4.0 bash -c 'echo "{\"admin\": \"admin-fixture-pw\"}" > /tmp/pw.json; exec airflow standalone'
# unpause the DAGs, trigger manual_orders_1 / manual_report_1, add the variables/pool/connections, then:
python test/airflow/record.py --base-url http://localhost:18080 --username admin \
  --password admin-fixture-pw --out test/fixtures/airflow3.json
docker rm -f vgi-airflow-af3
```

For 2.x, use `apache/airflow:2.11.2` with `AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth`
and `airflow users create -u admin -p admin-fixture-pw -r Admin ...` before `airflow standalone`.

The metadata check (`make lint-explain`) scores 100/100 with zero findings at `fail_on = "info"`.
The tables need a live Airflow and credentials, so CI runs the lint with `execute: "false"`. The
SQL suite covers the tables against the replays instead.

## Deploying

The same entry point serves stdio (DuckDB starts it) or HTTP (`vgi-airflow-worker --http --port 8000`,
or the Dockerfile). With HTTP, credentials still come from the caller's DuckDB secret, never from
the server's environment.

## Support

Open an issue on [Query-farm/vgi-airflow](https://github.com/Query-farm/vgi-airflow/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the [BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

Apache Airflow, Airflow and the Airflow logo are trademarks of the Apache Software Foundation.
This project is not affiliated with or endorsed by the ASF, Astronomer or Google.
