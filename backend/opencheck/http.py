"""Shared httpx.AsyncClient with sensible defaults for OpenCheck adapters.

All live adapters go through ``get_client()``. This keeps timeouts, retries,
and the User-Agent consistent, and makes it easy to add observability or
a circuit breaker later.
"""

from __future__ import annotations

import httpx

from . import __version__

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
_DEFAULT_LIMITS = httpx.Limits(max_connections=20, max_keepalive_connections=10)
_USER_AGENT = f"OpenCheck/{__version__} (+https://github.com/StephenAbbott/opencheck)"


def build_client() -> httpx.AsyncClient:
    """Build a new async client. Callers own the lifecycle (use ``async with``)."""
    transport = httpx.AsyncHTTPTransport(retries=2)
    return httpx.AsyncClient(
        timeout=_DEFAULT_TIMEOUT,
        limits=_DEFAULT_LIMITS,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        transport=transport,
        follow_redirects=True,
    )
