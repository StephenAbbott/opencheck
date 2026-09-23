"""Keep credentials out of anything a reader, a cache or a saved report can hold.

Two adapters authenticate with a key in the query string — Datafordeler CVR
(``?apiKey=``) and OpenCorporates (``?api_token=``). httpx puts the full
request URL, query string included, into the message of every
``HTTPStatusError``, and the lookup and search routers used to report a
failed source as ``f"{type(exc).__name__}: {exc}"``. So a 401, 429 or 5xx
from either service carried the key into:

- the ``source_error`` / ``deepen_error`` SSE events and ``/lookup`` errors,
- ``/search`` errors, ``/lookup-source`` and the batch row reason,
- the 15-minute replay cache, and from there
- a saved report — hashed bytes kept for 90 days, which cannot be redacted
  after the fact without breaking the hash.

:func:`describe_exception` is the one way a caught exception becomes text a
client may see. Three layers, each sufficient for the case it covers:

1. **An httpx status error is described, never stringified**: type, status
   code, reason phrase and host. No path, no query string. A reader learns
   what they need ("401 from graphql.datafordeler.dk") and nothing else.
2. **Every other message is scrubbed** by :func:`scrub` — which catches a
   status error some adapter re-wrapped as ``RuntimeError(f"... {exc}")``.
3. :func:`scrub` removes **the configured secret values themselves**, taken
   from :class:`~opencheck.config.Settings`, as well as credential-named
   query parameters and URL userinfo. Matching the value, not the parameter
   name, is what makes this hold for a key that turns up somewhere nobody
   anticipated.

Server logs are out of scope: they are not served to anyone, and a traceback
is what makes a log useful. This module governs what leaves the process in a
response, an event or a stored record.
"""

from __future__ import annotations

import re

import httpx

REDACTED = "[redacted]"

# Query-parameter names that carry a credential. Matched as a whole name,
# case-insensitively, so ``apiKey``, ``api_key``, ``API-KEY`` and ``api_token``
# all match while ``keyword`` or ``tokens_used`` do not.
_SECRET_PARAM = re.compile(
    r"(?P<lead>[?&;](?:api[_-]?key|apikey|api[_-]?token|access[_-]?token|"
    r"auth[_-]?token|token|key|secret|client[_-]?secret|password|passwd|pwd|"
    r"signature|sig)=)(?P<value>[^&#\s'\"<>]*)",
    re.IGNORECASE,
)

# ``scheme://user:password@host`` — drop the userinfo entirely.
_USERINFO = re.compile(r"(?P<scheme>\b[a-z][a-z0-9+.-]*://)[^/\s@'\"<>]+@", re.IGNORECASE)

# Settings fields whose value is a credential. A username is not a secret on
# its own, but it is half of one and has no business in an error message.
_SECRET_FIELD = re.compile(r"(key|token|secret|password|username)$", re.IGNORECASE)

# A configured value shorter than this is not replaced by value: a two- or
# three-character setting would match ordinary words in every message.
_MIN_SECRET_LEN = 6


def _configured_secrets() -> list[str]:
    """Every credential value in the live settings, longest first."""
    try:
        from .config import get_settings

        settings = get_settings()
    except Exception:  # noqa: BLE001 — scrubbing must never raise
        return []
    values: set[str] = set()
    for name in type(settings).model_fields:
        if not _SECRET_FIELD.search(name):
            continue
        value = getattr(settings, name, None)
        if isinstance(value, str):
            value = value.strip()
            if len(value) >= _MIN_SECRET_LEN:
                values.add(value)
    # Longest first, so a key that contains another is replaced whole.
    return sorted(values, key=len, reverse=True)


def scrub(text: str) -> str:
    """``text`` with every credential it could carry replaced by ``[redacted]``."""
    if not text:
        return text
    for secret in _configured_secrets():
        if secret in text:
            text = text.replace(secret, REDACTED)
    text = _SECRET_PARAM.sub(lambda m: m.group("lead") + REDACTED, text)
    text = _USERINFO.sub(lambda m: m.group("scheme") + REDACTED + "@", text)
    return text


def _host(request: httpx.Request | None) -> str | None:
    try:
        return request.url.host if request is not None else None
    except Exception:  # noqa: BLE001
        return None


def describe_http_status_error(exc: httpx.HTTPStatusError) -> str:
    """``HTTPStatusError: HTTP 401 Unauthorized from graphql.datafordeler.dk``."""
    parts = ["HTTP"]
    try:
        parts.append(str(exc.response.status_code))
        reason = (exc.response.reason_phrase or "").strip()
        if reason:
            parts.append(reason)
    except Exception:  # noqa: BLE001 — a status error without a response
        parts.append("error")
    text = " ".join(parts)
    try:
        host = _host(exc.request)
    except RuntimeError:  # httpx raises when .request was never set
        host = None
    if host:
        text += f" from {host}"
    return f"{type(exc).__name__}: {text}"


def safe_message(exc: BaseException) -> str:
    """The client-safe message alone, without the type-name prefix."""
    if isinstance(exc, httpx.HTTPStatusError):
        return describe_http_status_error(exc).split(": ", 1)[1]
    return scrub(str(exc))


def describe_exception(exc: BaseException) -> str:
    """The client-safe ``"<Type>: <message>"`` for a caught exception."""
    if isinstance(exc, httpx.HTTPStatusError):
        return describe_http_status_error(exc)
    return f"{type(exc).__name__}: {scrub(str(exc))}"
