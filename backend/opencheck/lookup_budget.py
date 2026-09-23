"""One per-client budget of full lookups, whoever asks for them (Phase 234).

Before this phase the per-IP rate limit was a property of *routes*: ``/lookup``
allowed 10 requests a minute, but ``/expand-layer`` sat on the 60/min default
tier and ran up to 25 full pipelines per request, ``POST /watch/items`` and
``/expand`` ran one each on the same default tier, a batch ran twenty on the
heavy tier, and the MCP tools ran as many as a client asked for with no limit
at all. Roughly 1,500 full lookups a minute were available to one IP through
``/expand-layer`` alone.

The fix is to charge the thing that costs, not the door it came through:

* :class:`ClientScopeMiddleware` (pure ASGI, so SSE and the mounted MCP app are
  untouched) records the caller's IP in a context variable for every HTTP
  request. Work started by the server itself — the watchlist worker, cache
  warm-up — has no client and is never charged.
* :func:`charge` is called by ``routers.lookup._lookup_pipeline_cached`` at the
  one moment a **fresh** pipeline is about to start. A replayed run and a run
  joined while in flight cost nothing, so a reader re-opening a report, or
  FullCheck revisiting a node, spends no budget.
* The budget's size is ``OPENCHECK_RATE_LIMIT_LOOKUP`` — the same string that
  sizes ``/lookup`` — so no route and no MCP tool can run more full lookups a
  minute than ``/lookup`` allows. They all draw on the one budget.
* Interactive callers are refused at once with the seconds until a slot frees
  (``429`` + ``Retry-After``); a batch row (:func:`waiting`) waits for it.

:class:`Quota` is the second, smaller half: per-IP counters for things that
fill a process-wide cap (new watchlists, saved reports), and for the per-tool
limits on the MCP mount. All counters are in memory, like slowapi's: one
Render instance, reset on deploy.
"""

from __future__ import annotations

import asyncio
import contextvars
import math
import time
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from limits import parse_many
from limits.storage import MemoryStorage
from limits.strategies import MovingWindowRateLimiter
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from .config import get_settings

__all__ = [
    "BudgetExceededError",
    "ClientScopeMiddleware",
    "Quota",
    "charge",
    "client_scope",
    "current_client",
    "refund",
    "reset_for_tests",
    "waiting",
]

_client: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "opencheck_client", default=None
)
_wait_s: contextvars.ContextVar[float] = contextvars.ContextVar(
    "opencheck_budget_wait_s", default=0.0
)


def current_client() -> str | None:
    """The IP of the HTTP client this work is being done for, or ``None``
    for work the server started itself."""
    return _client.get()


@contextmanager
def client_scope(client: str | None) -> Iterator[None]:
    """Charge work inside the block to ``client`` (tests, and callers that
    know the client some other way)."""
    token = _client.set(client)
    try:
        yield
    finally:
        _client.reset(token)


@contextmanager
def waiting(max_s: float) -> Iterator[None]:
    """Inside the block a fresh run waits up to ``max_s`` for the client's
    budget instead of being refused at once. Batch rows use this: twenty
    companies already take a couple of minutes, and a row that waits is
    better than a row that fails."""
    token = _wait_s.set(max(0.0, float(max_s)))
    try:
        yield
    finally:
        _wait_s.reset(token)


class ClientScopeMiddleware:
    """Pure ASGI: sets :func:`current_client` for the life of each request.

    Not ``BaseHTTPMiddleware`` — that wraps the response in a way that breaks
    SSE streaming and the mounted MCP routes (see ``ratelimit.py``). The IP
    is resolved exactly as the rate limiter resolves it.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        from .ratelimit import client_ip

        token = _client.set(client_ip(Request(scope)))
        try:
            await self.app(scope, receive, send)
        finally:
            _client.reset(token)


class BudgetExceededError(Exception):
    """The client has used its lookup budget; ``retry_after_s`` until the
    oldest charge ages out."""

    def __init__(self, retry_after_s: float):
        self.retry_after_s = max(1, math.ceil(retry_after_s))
        super().__init__(
            "You have run as many full checks as OpenCheck allows in a minute "
            f"(this counts every check you start, including FullCheck layers, "
            f"watchlist additions and MCP tools). Try again in {self.retry_after_s}s."
        )


def _enabled() -> bool:
    return bool(get_settings().rate_limit_enabled)


def _windows() -> list[tuple[int, float]]:
    """``[(amount, window_seconds), …]`` from ``OPENCHECK_RATE_LIMIT_LOOKUP``."""
    return [
        (int(item.amount), float(item.get_expiry()))
        for item in parse_many(get_settings().rate_limit_lookup)
    ]


class _Budget:
    """Sliding windows of charge timestamps, one deque per client."""

    def __init__(self) -> None:
        self._charges: dict[str, deque[float]] = {}

    def _prune(self, q: deque[float], now: float, horizon: float) -> None:
        while q and q[0] <= now - horizon:
            q.popleft()

    def try_charge(self, client: str, now: float | None = None) -> float | None:
        """Charge one run. ``None`` when charged, else seconds until a slot frees."""
        now = time.monotonic() if now is None else now
        windows = _windows()
        if not windows:
            return None
        horizon = max(w for _, w in windows)
        q = self._charges.setdefault(client, deque())
        self._prune(q, now, horizon)
        wait = 0.0
        for amount, window in windows:
            recent = [t for t in q if t > now - window]
            if len(recent) >= amount:
                # The slot frees when the charge that put us at the limit ages out.
                wait = max(wait, recent[len(recent) - amount] + window - now)
        if wait > 0:
            return wait
        q.append(now)
        if len(self._charges) > 10_000:
            self._sweep(now, horizon)
        return None

    def refund(self, client: str) -> None:
        q = self._charges.get(client)
        if q:
            q.pop()

    def _sweep(self, now: float, horizon: float) -> None:
        for key in [k for k, q in self._charges.items() if not q or q[-1] <= now - horizon]:
            self._charges.pop(key, None)

    def used(self, client: str) -> int:
        q = self._charges.get(client)
        return len(q) if q else 0


_budget = _Budget()


async def charge() -> str | None:
    """Charge one fresh pipeline run to the current client.

    Returns the client charged (``None`` when nothing was charged — no client,
    or limiting disabled) so the caller can :func:`refund` it if the run turns
    out not to have cost anything. Raises :class:`BudgetExceededError` when the
    budget is spent and the caller is not :func:`waiting`, or waited too long.
    """
    client = current_client()
    if client is None or not _enabled():
        return None
    deadline = time.monotonic() + _wait_s.get()
    while True:
        wait = _budget.try_charge(client)
        if wait is None:
            return client
        if time.monotonic() + wait > deadline:
            raise BudgetExceededError(wait)
        await asyncio.sleep(min(wait, 5.0))


def refund(client: str | None) -> None:
    """Give back a charge for a run that spent nothing upstream (a malformed
    LEI refused before any call, or a run refused for want of a slot)."""
    if client is not None:
        _budget.refund(client)


class Quota:
    """A named per-client counter backed by ``limits`` (moving window).

    ``check`` asks without spending, ``hit`` spends; callers check before the
    work and hit after it succeeds, so a refused or failed save costs nothing.
    Disabled with the rate limiter, and never applied to server-started work.
    """

    _storage = MemoryStorage()
    _limiter = MovingWindowRateLimiter(_storage)

    def __init__(self, name: str, limit: Any):
        self.name = name
        self._limit = limit  # a string, or a callable returning one

    def _items(self) -> list[Any]:
        spec = self._limit() if callable(self._limit) else self._limit
        return list(parse_many(spec)) if spec else []

    def retry_after(self, client: str | None = None) -> float | None:
        """``None`` when the client may proceed, else seconds until it may."""
        client = current_client() if client is None else client
        if client is None or not _enabled():
            return None
        worst: float | None = None
        for item in self._items():
            if not self._limiter.test(item, self.name, client):
                reset, _remaining = self._limiter.get_window_stats(item, self.name, client)
                wait = max(1.0, reset - time.time())
                worst = wait if worst is None else max(worst, wait)
        return worst

    def hit(self, client: str | None = None) -> bool:
        client = current_client() if client is None else client
        if client is None or not _enabled():
            return True
        ok = True
        for item in self._items():
            ok = self._limiter.hit(item, self.name, client) and ok
        return ok

    def describe(self) -> str:
        spec = self._limit() if callable(self._limit) else self._limit
        return str(spec)

    @classmethod
    def reset_all(cls) -> None:
        cls._storage.reset()


def reset_for_tests() -> None:
    global _budget
    _budget = _Budget()
    Quota.reset_all()
