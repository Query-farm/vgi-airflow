"""HTTP client for Airflow's stable REST API (2.x ``/api/v1`` and 3.x ``/api/v2``).

Everything that touches the network lives here:

* **Configuration** comes from a DuckDB secret of type ``airflow`` (see
  ``AIRFLOW_SECRET_TYPE``). Nothing is read from the environment.
* **Version detection**: ``GET /api/v2/version`` answers (200, 401 or 403) on
  Airflow 3; a 404 there means Airflow 2, which serves ``/api/v1``. The result is
  cached per base URL for the life of the process. ``api_version`` in the secret
  skips the probe.
* **Authentication**: a bearer ``token`` (Airflow 3 JWT, Astronomer deployment
  API token, Cloud Composer OAuth access token), HTTP basic
  ``username``/``password`` (Airflow 2), or an Airflow 3 ``POST /auth/token``
  exchange of ``username``/``password`` for a JWT (cached per process, refreshed
  on 401 or expiry).
* **Politeness**: every request has a timeout; 429 and 502/503/504 are retried
  with backoff that honours ``Retry-After``.

Credentials are never written to table-function state or logs: each
``process`` call rebuilds the client from the secret the SDK hands it.
"""

from __future__ import annotations

import base64
import datetime as dt
import email.utils
import hashlib
import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

AIRFLOW_SECRET_TYPE = "airflow"
USER_AGENT = "vgi-airflow-worker/0.1"
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
MAX_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 502, 503, 504})
DOC_URL = "https://github.com/Query-farm/vgi-airflow#credentials"

# "provider" is taken: DuckDB reserves it (with name, type and scope) on every secret.
PLATFORMS = ("airflow", "astronomer", "composer", "mwaa")
AUTH_MODES = ("auto", "token", "basic", "password")


def _field(
    name: str, doc: str, type_: pa.DataType | None = None, *, redact: bool = False, required: bool = False
) -> Any:
    meta = {"vgi_doc": doc}
    if redact:
        meta["redact"] = "true"
    if required:
        meta["vgi_required"] = "true"
    return pa.field(name, type_ or pa.string(), metadata=meta)


AIRFLOW_SECRET = SecretTypeSpec(
    name=AIRFLOW_SECRET_TYPE,
    description=(
        "Connection to one Airflow deployment's stable REST API: its base URL plus a bearer token, "
        "or a username and password (basic auth on Airflow 2, a /auth/token exchange on Airflow 3)."
    ),
    schema=pa.schema(
        [
            _field(
                "base_url",
                "Root URL of the Airflow webserver / API server, without /api/v1 or /api/v2, e.g. "
                "'http://localhost:8080' or an Astronomer deployment URL.",
                required=True,
            ),
            _field(
                "token",
                "Bearer token: an Airflow 3 JWT, an Astronomer deployment API token, or a Cloud Composer "
                "OAuth access token. Takes precedence over username/password.",
                redact=True,
            ),
            _field("username", "Airflow username for basic auth (2.x) or the /auth/token exchange (3.x)."),
            _field("password", "Password for `username`.", redact=True),
            _field(
                "auth",
                "How to authenticate: 'auto' (default; token if set, else basic on 2.x and /auth/token on 3.x), "
                "'token', 'basic' or 'password' (force the Airflow 3 /auth/token exchange).",
            ),
            _field(
                "api_version",
                "'auto' (default; probe /api/v2/version), 'v1' (Airflow 2.x) or 'v2' (Airflow 3.x).",
            ),
            _field(
                "platform",
                "Hosting flavour: 'airflow' (default, self-managed), 'astronomer' or 'composer' (both bearer "
                "token). 'mwaa' is reserved and not yet supported.",
            ),
            _field("page_size", "Items requested per API page, 1 to 1000 (default 100).", pa.int64()),
            _field("timeout", "Per-request timeout in seconds (default 30).", pa.float64()),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


class AirflowError(RuntimeError):
    """The Airflow API returned an error, or the secret is unusable."""


class NotFoundError(AirflowError):
    """HTTP 404 from the API."""


@dataclass(frozen=True, slots=True)
class AirflowConfig:
    """Connection settings parsed from an ``airflow`` secret."""

    base_url: str
    token: str | None = field(default=None, repr=False)
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    auth: str = "auto"
    api_version: str = "auto"
    platform: str = "airflow"
    page_size: int = DEFAULT_PAGE_SIZE
    timeout: float = 30.0

    @classmethod
    def from_secret(cls, secret: Mapping[str, Any] | None) -> AirflowConfig:
        """Build and validate a config from a resolved secret's key/value pairs.

        Args:
            secret: Secret fields; values may be ``pa.Scalar`` or plain Python.

        Returns:
            The validated configuration.

        Raises:
            AirflowError: No secret, or an invalid field value.

        """
        if not secret:
            raise AirflowError(
                "no Airflow credentials: run CREATE SECRET (TYPE airflow, BASE_URL 'http://host:8080', "
                "TOKEN '...') or (TYPE airflow, BASE_URL '...', USERNAME '...', PASSWORD '...') "
                f"after ATTACH. See {DOC_URL}"
            )

        def get(name: str) -> Any:
            value = secret.get(name)
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return None if value == "" else value

        base_url = get("base_url")
        if not base_url or not str(base_url).startswith(("http://", "https://")):
            raise AirflowError("airflow secret: BASE_URL must be set to an http:// or https:// URL")
        base = str(base_url).rstrip("/")
        for suffix in ("/api/v1", "/api/v2"):
            base = base.removesuffix(suffix)
        auth = str(get("auth") or "auto").lower()
        if auth not in AUTH_MODES:
            raise AirflowError(f"airflow secret: AUTH must be one of {', '.join(AUTH_MODES)}, not {auth!r}")
        api_version = str(get("api_version") or "auto").lower()
        if api_version not in ("auto", "v1", "v2"):
            raise AirflowError(f"airflow secret: API_VERSION must be 'auto', 'v1' or 'v2', not {api_version!r}")
        platform = str(get("platform") or "airflow").lower()
        if platform not in PLATFORMS:
            raise AirflowError(f"airflow secret: PLATFORM must be one of {', '.join(PLATFORMS)}, not {platform!r}")
        if platform == "mwaa":
            raise AirflowError(
                "airflow secret: PLATFORM 'mwaa' (Amazon MWAA) is not supported yet; it needs AWS SigV4 "
                "request signing. Use a self-managed or Astronomer/Composer deployment, or a bearer TOKEN."
            )
        token = get("token")
        username, password = get("username"), get("password")
        if platform in ("astronomer", "composer") and not token:
            raise AirflowError(f"airflow secret: PLATFORM '{platform}' authenticates with a bearer TOKEN")
        if auth == "token" and not token:
            raise AirflowError("airflow secret: AUTH 'token' needs TOKEN")
        if auth in ("basic", "password") and not (username and password):
            raise AirflowError(f"airflow secret: AUTH '{auth}' needs USERNAME and PASSWORD")
        page_size = int(get("page_size") or DEFAULT_PAGE_SIZE)
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise AirflowError(f"airflow secret: PAGE_SIZE must be between 1 and {MAX_PAGE_SIZE}")
        timeout = float(get("timeout") or 30.0)
        return cls(
            base_url=base,
            token=str(token) if token else None,
            username=str(username) if username else None,
            password=str(password) if password else None,
            auth=auth,
            api_version=api_version,
            platform=platform,
            page_size=page_size,
            timeout=timeout,
        )


# Process-wide caches. Keys never include a raw password.
_LOCK = threading.Lock()
_VERSION_CACHE: dict[str, str] = {}
_TOKEN_CACHE: dict[tuple[str, str, str], tuple[str, float]] = {}


def clear_caches() -> None:
    """Forget detected API versions and exchanged tokens (used by tests)."""
    with _LOCK:
        _VERSION_CACHE.clear()
        _TOKEN_CACHE.clear()


def _retry_delay(header: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` given a ``Retry-After`` header value."""
    if header:
        header = header.strip()
        if header.replace(".", "", 1).isdigit():
            return min(float(header), MAX_BACKOFF_SECONDS)
        try:
            when = email.utils.parsedate_to_datetime(header)
            return max(0.0, min((when - dt.datetime.now(dt.UTC)).total_seconds(), MAX_BACKOFF_SECONDS))
        except (TypeError, ValueError):
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


def _jwt_expiry(token: str) -> float:
    """Return the ``exp`` claim of a JWT, or one hour from now when unreadable."""
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return float(claims["exp"])
    except (IndexError, KeyError, ValueError, TypeError):
        return time.time() + 3600


class AirflowClient:
    """Authenticated JSON GETs against one Airflow deployment."""

    def __init__(self, config: AirflowConfig) -> None:
        """Bind to a configuration; version detection and auth happen lazily."""
        self.config = config
        self._version: str | None = None if config.api_version == "auto" else config.api_version

    # ------------------------------------------------------------------ plumbing

    def _open(self, request: urllib.request.Request) -> tuple[int, Any, Any]:
        """Send one request with retries; return (status, decoded JSON body, headers)."""
        url = request.full_url
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.config.timeout) as resp:
                    raw = resp.read()
                    return resp.status, json.loads(raw) if raw else None, resp.headers
            except urllib.error.HTTPError as err:
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    delay = _retry_delay(err.headers.get("Retry-After"), attempt)
                    log.info("airflow: HTTP %s from %s; retrying in %.1fs", err.code, url, delay)
                    time.sleep(delay)
                    continue
                body = err.read(800).decode(errors="replace")
                try:
                    detail = json.loads(body)
                    if isinstance(detail, dict):
                        body = str(detail.get("detail") or detail.get("title") or body)
                except ValueError:
                    pass
                return err.code, body, err.headers
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise AirflowError(f"GET {url} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
        raise AirflowError(f"GET {url}: gave up after {MAX_ATTEMPTS} attempts")

    def _request(self, url: str, headers: Mapping[str, str], data: bytes | None = None) -> urllib.request.Request:
        base = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if data is not None:
            base["Content-Type"] = "application/json"
        return urllib.request.Request(url, data=data, headers={**base, **headers}, method="POST" if data else "GET")

    # ------------------------------------------------------------------ version + auth

    @property
    def version(self) -> str:
        """``'v1'`` (Airflow 2) or ``'v2'`` (Airflow 3), probing once per base URL."""
        if self._version is None:
            with _LOCK:
                cached = _VERSION_CACHE.get(self.config.base_url)
            if cached is None:
                cached = self._probe_version()
                with _LOCK:
                    _VERSION_CACHE[self.config.base_url] = cached
            self._version = cached
        return self._version

    def _probe_version(self) -> str:
        status, _, _ = self._open(self._request(f"{self.config.base_url}/api/v2/version", {}))
        if status in (200, 401, 403):
            return "v2"
        status, _, _ = self._open(self._request(f"{self.config.base_url}/api/v1/version", {}))
        if status in (200, 401, 403):
            return "v1"
        raise AirflowError(
            f"{self.config.base_url} does not look like an Airflow API server "
            f"(/api/v2/version and /api/v1/version both failed; last HTTP {status})"
        )

    @property
    def is_v2(self) -> bool:
        """True for Airflow 3's ``/api/v2``."""
        return self.version == "v2"

    def _auth_headers(self, *, refresh: bool = False) -> dict[str, str]:
        cfg = self.config
        mode = cfg.auth
        if mode == "auto":
            if cfg.token:
                mode = "token"
            elif cfg.username and cfg.password:
                mode = "password" if self.is_v2 else "basic"
            else:
                raise AirflowError("airflow secret: set TOKEN, or USERNAME and PASSWORD")
        if mode == "token":
            return {"Authorization": f"Bearer {cfg.token}"}
        assert cfg.username is not None and cfg.password is not None
        if mode == "basic":
            raw = base64.b64encode(f"{cfg.username}:{cfg.password}".encode()).decode()
            return {"Authorization": f"Basic {raw}"}
        return {"Authorization": f"Bearer {self._exchanged_token(refresh=refresh)}"}

    def _exchanged_token(self, *, refresh: bool) -> str:
        cfg = self.config
        assert cfg.username is not None and cfg.password is not None
        key = (cfg.base_url, cfg.username, hashlib.sha256(cfg.password.encode()).hexdigest())
        with _LOCK:
            hit = _TOKEN_CACHE.get(key)
        if hit and not refresh and hit[1] - 60 > time.time():
            return hit[0]
        body = json.dumps({"username": cfg.username, "password": cfg.password}).encode()
        status, payload, _ = self._open(self._request(f"{cfg.base_url}/auth/token", {}, body))
        if status not in (200, 201) or not isinstance(payload, dict) or "access_token" not in payload:
            raise AirflowError(f"POST {cfg.base_url}/auth/token failed with HTTP {status}: {payload}")
        token = str(payload["access_token"])
        with _LOCK:
            _TOKEN_CACHE[key] = (token, _jwt_expiry(token))
        return token

    # ------------------------------------------------------------------ public API

    def get(self, path: str, query: Mapping[str, Any] | None = None) -> Any:
        """GET ``/api/<version><path>`` and return the decoded JSON body.

        Args:
            path: Path below the API prefix, already URL-quoted, e.g. ``/dags/~/dagRuns``.
            query: Query parameters; list values repeat the parameter, ``None`` values are dropped.

        Returns:
            The decoded JSON response.

        Raises:
            NotFoundError: HTTP 404.
            AirflowError: Any other non-2xx status.

        """
        clean = {k: v for k, v in (query or {}).items() if v is not None}
        qs = urllib.parse.urlencode(clean, doseq=True)
        url = f"{self.config.base_url}/api/{self.version}{path}" + (f"?{qs}" if qs else "")
        status, body, _ = self._open(self._request(url, self._auth_headers()))
        if status == 401 and self.config.auth in ("auto", "password") and not self.config.token and self.is_v2:
            status, body, _ = self._open(self._request(url, self._auth_headers(refresh=True)))
        if 200 <= status < 300:
            return body
        if status == 404:
            raise NotFoundError(f"GET {url} failed with HTTP 404: {body}")
        if status in (401, 403):
            raise AirflowError(f"GET {url} failed with HTTP {status} (check the airflow secret's credentials): {body}")
        raise AirflowError(f"GET {url} failed with HTTP {status}: {body}")

    def ui_url(self, path: str) -> str:
        """Absolute URL of a page in the Airflow web UI."""
        return f"{self.config.base_url}{path}"


def quote(value: str) -> str:
    """Quote one path segment (run ids contain ``:`` and ``+``)."""
    return urllib.parse.quote(value, safe="")
