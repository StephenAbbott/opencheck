"""Process-wide rate throttle for the GLEIF API (Phase 143).

GLEIF rate-limits by IP: "Rate limiting is currently set at 60 requests, per
minute, per user, for all users." Every deployment of OpenCheck shares one
egress IP across all of its GLEIF traffic — the anchor lookup (4–6 requests),
the securities ISIN call, the Time Machine field-modifications feed and the
subsidiary-network reveal — so under crawler-scale lookup volume the server's
own aggregate demand can exceed GLEIF's budget. Diagnosed live 2026-08-29:
~12 lookups/min × 4–6 parallel GLEIF calls ≈ 50–70 req/min, every request
answered 429, and — because rejected requests still count against GLEIF's
sliding window — the saturation was self-sustaining even though no lookup
succeeded.

The fix is to stop asking. This module keeps a process-wide sliding-window
budget (default 50/min, headroom under GLEIF's 60) and a shared penalty box
fed by observed 429s. It is enforced at a single choke point: the transport
wrapper below, installed by :func:`opencheck.http.build_client`, applies to
every request whose host is ``api.gleif.org`` and to nothing else. No caller
edits, and no future call site can forget to opt in.

Waiting is bounded. A caller that cannot get a slot within
``OPENCHECK_GLEIF_THROTTLE_MAX_WAIT_S`` gets :class:`GleifRateLimitedError`
*without a request being sent* — a request we know will 429 only feeds the
window we are trying to drain. The GLEIF adapter treats that error exactly
like an observed 429 and falls back (stale cache → Golden Copy snapshot →
friendly 503) instead of aborting the lookup with a 502.

Concurrency notes: the budget state is plain data mutated only between
awaits on the single event loop, so it needs no lock; the check-then-append
in :meth:`GleifThrottle.acquire` is atomic within one loop iteration. The
singleton is process-global (one uvicorn worker = one budget). Tests disable
the throttle wholesale via ``OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE=0`` in
``conftest.py`` and re-enable it per-fixture.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

import httpx

from .config import get_settings

log = logging.getLogger(__name__)

#: The one host this throttle governs.
GLEIF_HOST = "api.gleif.org"

#: Ceiling for honouring a server-sent Retry-After — a hostile or confused
#: header must not park the whole process for minutes.
_MAX_RETRY_AFTER_S = 15.0

#: Backoff applied on a 429 that carries no usable Retry-After header.
_DEFAULT_RETRY_AFTER_S = 2.0

_WINDOW_S = 60.0


class GleifRateLimitedError(Exception):
    """GLEIF's rate limit blocks this request and no budget slot freed in time.

    Raised *instead of sending* when the shared budget is exhausted beyond the
    max wait, and by callers that observed a 429 and have no fallback left.
    """


class GleifThrottle:
    """Sliding-window request budget plus a 429-fed penalty box."""

    def __init__(self) -> None:
        self._sent: deque[float] = deque()
        self._penalty_until: float = 0.0

    # -- state inspection (tests) ---------------------------------------

    @property
    def in_flight_window(self) -> int:
        self._prune(time.monotonic())
        return len(self._sent)

    # -- core ------------------------------------------------------------

    def _prune(self, now: float) -> None:
        cutoff = now - _WINDOW_S
        while self._sent and self._sent[0] <= cutoff:
            self._sent.popleft()

    def penalise(self, seconds: float) -> None:
        """Push the shared resume time out after an observed 429.

        Every concurrent caller waits it out, so one rejected request quiets
        the whole process instead of letting the other five calls of the same
        lookup burn the window too.
        """
        seconds = min(max(seconds, 0.0), _MAX_RETRY_AFTER_S)
        self._penalty_until = max(self._penalty_until, time.monotonic() + seconds)

    async def acquire(self) -> None:
        """Take one budget slot, waiting up to the configured max wait.

        Raises :class:`GleifRateLimitedError` when no slot can free within
        the deadline — deliberately without sending anything.
        """
        settings = get_settings()
        limit = settings.gleif_rate_limit_per_minute
        if limit <= 0:  # throttle disabled (tests, or operator override)
            return
        deadline = time.monotonic() + max(settings.gleif_throttle_max_wait_s, 0.0)
        while True:
            now = time.monotonic()
            self._prune(now)
            if now >= self._penalty_until and len(self._sent) < limit:
                self._sent.append(now)
                return
            wait_until = max(
                self._penalty_until,
                (self._sent[0] + _WINDOW_S) if len(self._sent) >= limit else 0.0,
            )
            if wait_until > deadline:
                raise GleifRateLimitedError(
                    "GLEIF request budget exhausted "
                    f"({limit}/min shared across this process) and no slot "
                    f"frees within {settings.gleif_throttle_max_wait_s:.0f}s"
                )
            # Sleep in short steps so an early penalty release or freed slot
            # is picked up promptly.
            await asyncio.sleep(min(max(wait_until - now, 0.05), 1.0))


_throttle = GleifThrottle()


def get_throttle() -> GleifThrottle:
    return _throttle


def reset_throttle_for_tests() -> None:
    global _throttle
    _throttle = GleifThrottle()


def _retry_after_seconds(response: httpx.Response) -> float:
    """Parse Retry-After (delta-seconds form only), clamped to sane bounds."""
    raw = response.headers.get("Retry-After", "")
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_RETRY_AFTER_S
    return min(max(value, 0.0), _MAX_RETRY_AFTER_S)


class GleifThrottledTransport(httpx.AsyncBaseTransport):
    """Transport wrapper: budget + one Retry-After-honouring retry for GLEIF.

    Requests to any other host pass straight through untouched — this wraps
    the client every adapter builds, not just the GLEIF adapter's.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if (request.url.host or "").lower() != GLEIF_HOST:
            return await self._inner.handle_async_request(request)

        throttle = get_throttle()
        await throttle.acquire()
        response = await self._inner.handle_async_request(request)
        if response.status_code != 429:
            return response

        # One retry, honouring Retry-After. The penalty is shared so the
        # sibling calls of the same lookup back off with us.
        delay = _retry_after_seconds(response)
        throttle.penalise(delay)
        log.warning(
            "GLEIF 429 on %s — backing off %.1fs and retrying once",
            request.url.path,
            delay,
        )
        await response.aclose()
        await asyncio.sleep(delay)
        await throttle.acquire()
        response = await self._inner.handle_async_request(request)
        if response.status_code == 429:
            # Still saturated: penalise and hand the 429 back — the caller's
            # fallback chain (stale cache → snapshot → 503) takes it from here.
            throttle.penalise(_retry_after_seconds(response))
        return response

    async def aclose(self) -> None:
        await self._inner.aclose()
