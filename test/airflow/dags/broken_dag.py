"""Fixture DAG file that fails to import, so the API reports an import error."""

import this_module_does_not_exist  # noqa: F401
from airflow.sdk import DAG  # noqa: F401
