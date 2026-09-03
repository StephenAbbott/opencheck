"""Outbound rate limiting for upstreams that publish a hard request budget.

Distinct from :mod:`opencheck.ratelimit`, which is *inbound* per-IP abuse
protection for OpenCheck's own public API. This module limits how fast
OpenCheck calls *someone else's* API, for the handful of sources whose
published quota is low enough that an unthrottled fan-out would trip it.

The first such source is the Greek ΓΕΜΗ Open Data API, whose documented
budget is **20 requests per minute** — one every three seconds, raised from
the original 8 on 2026-09-03. A FullCheck that traverses several Greek related
parties issues one call each; without a throttle the third or fourth lands on
HTTP 429 and the lookup loses data it could have had by waiting.

Two mechanisms, deliberately separate:

* :class:`TokenBucket` — paces requests so we stay under the published rate.
  Refills continuously, so a source idle for a minute can burst up to
  ``capacity`` and then settles to the steady rate.
* :class:`CallBudget` — caps how many calls a *single lookup* may spend on one
  source, so one deep ownership chain cannot monopolise the minute's budget
  and stall the request past its timeout. Scoped with a ContextVar, in the
  same style as :mod:`opencheck.provenance` and :mod:`opencheck.degradation`,
  so adapters need no plumbing.

A budget only exists while a scope is open, and **something has to open it**.
Phase 137 shipped the mechanism and both call sites in the ΓΕΜΗ adapter but
never opened a scope on the request path — only inside a test, which opened
its own — so :func:`current_budget` returned ``None`` in production and the
cap was inert for every real lookup from Phase 137 to Phase 165. Nothing
looked wrong: the adapter's ``if budget is not None`` guard is exactly what a
script or probe needs, so the dead path was indistinguishable from the
intended one. What it cost was the honest degradation the cap exists to
produce — a deep Greek chain queued on the bucket instead and hit its 30s
source timeout, reporting a timeout rather than "GEMI was rate-limited".

The lesson generalises: a ContextVar mechanism is only as live as its
outermost ``begin()``, and a test that opens the scope itself cannot tell you
whether production does. :func:`begin` is called once per lookup in
``routers/lookup.py``, beside ``degradation.begin()``, and a test asserts that
the real pipeline opens it.

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

__all__ = [
    "TokenBucket",
    "CallBudget",
    "begin",
    "end",
    "budget_scope",
    "current_budget",
]


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


def begin() -> dict[str, CallBudget]:
    """Open a per-lookup budget scope, and return the live budget map.

    The counterpart to :func:`budget_scope` for callers that cannot wrap their
    body in a ``with`` — deliberately, and for the same reason
    :func:`opencheck.degradation.begin` exists: ``_lookup_pipeline`` is an
    async generator several hundred lines long, and re-indenting it under a
    context manager would be a large diff for no behavioural gain. It is also
    the *safe* shape here: a ``with`` block spanning ``yield`` statements in an
    async generator resets the ContextVar in whichever task happens to resume
    the generator, which is not necessarily the one that set it.

    Setting the var in the consuming task's context is what makes the budget
    shared: :func:`asyncio.create_task` copies the context at creation, and
    the value is a mutable dict of mutable :class:`CallBudget`s, so every
    concurrent source fetch spends from the same counter — the same mechanism
    ``degradation`` relies on to collect from concurrent fetches.
    """
    budgets: dict[str, CallBudget] = {}
    _CURRENT.set(budgets)
    return budgets


def end() -> None:
    """Close the scope, so a later standalone fetch is uncapped again."""
    _CURRENT.set(None)


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
