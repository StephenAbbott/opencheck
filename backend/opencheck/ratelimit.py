"""Per-IP rate limiting for the public API (abuse protection).

The public deployment has no auth, so the only thing standing between a
scripted client and the key-gated upstream quotas (Companies House, NZBN,
Corporations Canada, CVR, OpenSanctions, …) — or the CPU/Anthropic-token cost
of /export/pdf and /narrative — is a per-IP budget. This module provides it
with `slowapi <https://slowapi.readthedocs.io/>`_ backed by **in-memory**
storage: OpenCheck runs as a single Render instance, so shared counters need
no external store (if the API ever scales out, point slowapi at a Redis
``storage_uri`` — the endpoint code doesn't change).

Design notes:

* **Explicit decorators, no middleware.** Every public route carries its own
  ``@limiter.limit(<tier>)`` decorator. slowapi's ``SlowAPIMiddleware`` is
  deliberately not used: it is a ``BaseHTTPMiddleware``, which interferes with
  SSE streaming (``/lookup-stream``, ``/stream``) and crashes on the mounted
  MCP routes (their endpoints aren't plain functions). ``/health`` and
  ``/sources`` are exempt by simply not being decorated.
* **Tiers are callables**, resolved through :func:`get_settings` on every
  request, so budgets can be tuned per-deploy via env vars
  (``OPENCHECK_RATE_LIMIT_LOOKUP`` etc.) and tests can shrink them via
  ``monkeypatch`` + ``get_settings.cache_clear()``.
* **Client IP behind Render.** Render fronts services with Cloudflare and its
  own proxy, so ``request.client.host`` is always a proxy address in
  production. The edge sets ``True-Client-IP`` (Cloudflare) and prepends the
  real client to ``X-Forwarded-For``; we trust them in that order and fall
  back to the socket peer for local dev / tests. A client talking straight
  to a deployment with no proxy could spoof these headers to rotate buckets —
  that degrades rate limiting for the spoofer only (equivalent to rotating
  IPs), it never blocks other users, so it is an accepted trade-off for a
  free public demo.
* Errors count. A request that 400s/500s inside a handler still spends
  budget — hammering the API with garbage is exactly the abuse this guards.
"""

from __future__ import annotations

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import get_settings

__all__ = [
    "client_ip",
    "default_tier",
    "heavy_tier",
    "limiter",
    "lookup_tier",
    "rate_limit_exceeded_handler",
]


def client_ip(request: Request) -> str:
    """Best available client IP behind Render's proxy chain (see module doc)."""
    true_client_ip = request.headers.get("true-client-ip", "").strip()
    if true_client_ip:
        return true_client_ip
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


# Tier callables — evaluated per request so env overrides and test
# monkeypatching (via get_settings.cache_clear()) take effect immediately.


def lookup_tier() -> str:
    """Budget for full fan-out endpoints (every source adapter dispatched)."""
    return get_settings().rate_limit_lookup


def heavy_tier() -> str:
    """Budget for CPU / Anthropic-token heavy synthesis endpoints."""
    return get_settings().rate_limit_heavy


def default_tier() -> str:
    """Budget for all other public endpoints."""
    return get_settings().rate_limit_default


limiter = Limiter(
    key_func=client_ip,
    headers_enabled=True,  # X-RateLimit-* + Retry-After on limited routes
    enabled=get_settings().rate_limit_enabled,
)


async def rate_limit_exceeded_handler(request: Request, exc: Exception) -> JSONResponse:
    """429 handler — FastAPI-conventional ``detail`` body + Retry-After header.

    slowapi ships ``_rate_limit_exceeded_handler`` but its body uses an
    ``error`` key; the rest of this API (and the frontend's error handling)
    speaks ``detail``, so we roll our own and reuse slowapi's header injection.
    """
    assert isinstance(exc, RateLimitExceeded)  # registered only for this type
    response = JSONResponse(
        status_code=429,
        content={
            "detail": (
                f"Rate limit exceeded ({exc.detail}). OpenCheck is a free, shared "
                "service — please slow down and retry shortly."
            )
        },
    )
    view_rate_limit = getattr(request.state, "view_rate_limit", None)
    if view_rate_limit is not None:
        limiter._inject_headers(response, view_rate_limit)  # mutates in place
    return response
