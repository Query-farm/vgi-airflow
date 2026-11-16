# Changelog

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.

## 0.1.1

- Catalog and schema docs are overviews instead of object lists, and schemas have their own one-line comments (vgi-lint 2.8: VGI173, VGI102).

## 0.1.0

Initial release: Airflow 2.x (`/api/v1`) and 3.x (`/api/v2`) stable REST API as SQL.

- `airflow.main`: dags, dag_runs, task_instances, task_instance_tries, xcom_entries, assets,
  asset_events, event_logs, import_errors, pools, variables (masked), connections (no passwords);
  `task_logs(...)`, `variable_values(reveal := false)`.
- `airflow.unified`: runs, task_runs (shared orchestrator shape), `unified_status(state)`.
- `airflow` secret: base_url + token / basic auth / Airflow 3 `/auth/token` exchange; Astronomer
  and Cloud Composer bearer tokens. MWAA not yet supported.
- Filter pushdown into API paths and parameters; offset paging with a resumable cursor; 429 retry.
- Tests replay responses recorded from Airflow 3.4.0 and 2.11.2 `airflow standalone` containers.

Built from vgi-starter-python v1.3.0. Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11
