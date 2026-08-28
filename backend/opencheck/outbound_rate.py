"""Outbound rate limiting for upstreams that publish a hard request budget.

Distinct from :mod:`opencheck.ratelimit`, which is *inbound* per-IP abuse
protection for OpenCheck's own public API. This module limits how fast
OpenCheck calls *someone else's* API, for the handful of sources whose
published quota is low enough that an unthrottled fan-out would trip it.

The first such source is the Greek ΓΕΜΗ Open Data API, whose documented
budget is **8 requests per minute** — one every 7.5 seconds. A FullCheck that
traverses several Greek related parties issues one call each; without a
throttle the third or fourth lands on HTTP 429 and the lookup loses data it
could have had by waiting.

Two mechanisms, deliberately separate:

* :class:`TokenBucket` — paces requests so we stay under the published rate.
  Refills continuously, so a source idle for a minute can burst up to
  ``capacity`` and then settles to the steady rate.
* :class:`CallBudget` — caps how many calls a *single lookup* may spend on one
  source, so one deep ownership chain cannot monopolise the minute's budget
  and stall the request past its timeout. Scoped with a ContextVar, in the
  same style as :mod:`opencheck.provenance` and :mod:`opencheck.degradation`,
  so adapters need no plumbing.

Both are process-local. OpenCheck runs as a single Render instance, the same
assumption :mod:`opencheck.ratelimit` already makes; if it ever scales out,
these become per-instance and the effective rate multiplies by the instance
count, which is the moment to move the counter into Redis.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

_LOG = logging.getLogger(__name__)

__all__ = ["TokenBucket", "CallBudget", "budget_scope", "current_budget"]


class TokenBucket:
    """An asyncio token bucket that paces calls to one upstream host.

    ``rate_per_minute`` is the sustained rate; ``capacity`` is how many calls
    may be made back-to-back after an idle period (default: one, i.e. no
    burst, which is the safe reading of a "N per minute" quota that may be
    enforced as a sliding window).

    :meth:`acquire` waits until a token is available. It is safe to call from
    many coroutines at once — an :class:`asyncio.Lock` serialises the refill
    arithmetic so two callers cannot both spend the last token.
    """

    def __init__(
        self,
        rate_per_minute: float,
        *,
        capacity: int = 1,
        name: str = "upstream",
    ) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be positive")
        self._interval = 60.0 / rate_per_minute
        self._capacity = max(1, capacity)
        self._name = name
        self._tokens = float(self._capacity)
        self._updated = time.monotonic()
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        # Created lazily: the adapter is instantiated at import time, when
        # there may be no running event loop to bind a Lock to.
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(self) -> float:
        """Wait for a token. Returns how many seconds the caller was delayed."""
        async with self._get_lock():
            now = time.monotonic()
            self._tokens = min(
                float(self._capacity),
                self._tokens + (now - self._updated) / self._interval,
            )
            self._updated = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait = (1.0 - self._tokens) * self._interval
            # Hold the lock across the sleep so waiters queue in arrival order
            # and each gets its own slot rather than racing for one token.
            await asyncio.sleep(wait)
            self._updated = time.monotonic()
            self._tokens = 0.0
            _LOG.debug("%s rate limiter delayed a call by %.1fs", self._name, wait)
            return wait


@dataclass
class CallBudget:
    """How many upstream calls one lookup may spend on a single source."""

    limit: int
    spent: int = 0

    def take(self) -> bool:
        """Claim one call. False when the budget is exhausted."""
        if self.spent >= self.limit:
            return False
        self.spent += 1
        return True

    @property
    def exhausted(self) -> bool:
        return self.spent >= self.limit


_CURRENT: contextvars.ContextVar[dict[str, CallBudget] | None] = contextvars.ContextVar(
    "opencheck_outbound_budgets", default=None
)


@contextmanager
def budget_scope() -> Iterator[dict[str, CallBudget]]:
    """Open a per-lookup budget scope. Budgets are created on first use."""
    budgets: dict[str, CallBudget] = {}
    token = _CURRENT.set(budgets)
    try:
        yield budgets
    finally:
        _CURRENT.reset(token)


def current_budget(source_id: str, limit: int) -> CallBudget | None:
    """The budget for *source_id*, or None when no scope is open.

    No scope means no cap — a standalone ``fetch()`` in a script or a test is
    not part of a user-facing lookup and should not be silently truncated.
    """
    budgets = _CURRENT.get()
    if budgets is None:
        return None
    return budgets.setdefault(source_id, CallBudget(limit=limit))
