"""Shared fixtures: replays of the recorded Airflow 2 and 3 APIs."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from airflow_worker.client import clear_caches

ROOT = Path(__file__).resolve().parent.parent


def _serve(fixture: str, **env: str) -> Iterator[str]:
    proc = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "test" / "mock_api" / "server.py"),
            "--fixture",
            str(ROOT / "test" / "fixtures" / fixture),
        ],
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, **env},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture(scope="session")
def mock_v3() -> Iterator[str]:
    """Airflow 3.4 replay (/api/v2), rate-limiting the first request to every path."""
    yield from _serve("airflow3.json", MOCK_FLAKY="1")


@pytest.fixture(scope="session")
def mock_v2() -> Iterator[str]:
    """Airflow 2.11 replay (/api/v1)."""
    yield from _serve("airflow2.json")


@pytest.fixture(autouse=True)
def _fresh_caches() -> None:
    clear_caches()
