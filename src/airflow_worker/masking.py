"""Keep secrets out of query results.

Airflow 3 masks sensitive variable values and connection passwords itself, but
Airflow 2's ``GET /variables`` returns every value verbatim, and both versions
return connection ``extra`` JSON. The worker therefore masks independently of
the server:

* ``connections`` never has a password column, only ``has_password``; secret
  looking keys inside ``extra`` are replaced by ``***`` (recursively), and an
  ``extra`` that is not a JSON object is withheld entirely.
* ``variables`` always masks values. ``variable_values(reveal := true)`` shows a
  value only when its key does not look secret *and* the value does not look
  like a credential.
"""

from __future__ import annotations

import json
import re
from typing import Any

MASK = "***"

# Airflow's DEFAULT_SENSITIVE_FIELDS plus common credential words. Deliberately broad:
# a false positive hides a harmless value, a false negative leaks a secret.
_SENSITIVE_WORDS = (
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "bearer",
    "cert",
    "conn",
    "cookie",
    "credential",
    "dsn",
    "jwt",
    "key",
    "pass",
    "private",
    "sas",
    "secret",
    "session",
    "signature",
    "token",
    "service_account",
    "keyfile",
)

# Values that look like credentials regardless of their key.
_SECRET_VALUE = re.compile(
    r"^(?:xox[abposr]-|gh[pousr]_|github_pat_|AKIA|ASIA|sk-|sk_live_|rk_live_|glpat-|ya29\.|AIza)"
    r"|^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\."  # JWT
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r"|://[^/\s:@]+:[^/\s@]+@"  # URI with embedded password
)


def is_sensitive_key(key: str) -> bool:
    """Return True when a variable/extra key name suggests a secret."""
    lowered = key.lower().replace("-", "_")
    return any(word in lowered for word in _SENSITIVE_WORDS)


def looks_secret(value: str) -> bool:
    """Return True when a value looks like a credential (token prefix, JWT, private key, URI password)."""
    return bool(_SECRET_VALUE.search(value.strip())) or value.strip() == MASK


def mask_variable(key: str, value: Any, *, reveal: bool) -> tuple[str | None, bool]:
    """Return ``(value_to_show, masked)`` for one Airflow variable."""
    if value is None:
        return None, False
    text = value if isinstance(value, str) else json.dumps(value)
    if reveal and not is_sensitive_key(key) and not looks_secret(text):
        return text, False
    return MASK, True


def _mask_tree(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: MASK if is_sensitive_key(str(k)) and v not in (None, "") else _mask_tree(v) for k, v in value.items()
        }
    if isinstance(value, list):
        return [_mask_tree(v) for v in value]
    if isinstance(value, str) and looks_secret(value) and value != MASK:
        return MASK
    return value


def mask_extra(extra: Any) -> str | None:
    """Mask a connection's ``extra``; return JSON text, or None when absent or unparseable."""
    if extra in (None, ""):
        return None
    parsed = extra
    if isinstance(extra, str):
        try:
            parsed = json.loads(extra)
        except ValueError:
            return None
    if not isinstance(parsed, dict):
        return None
    return json.dumps(_mask_tree(parsed), sort_keys=True)
