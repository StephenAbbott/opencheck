"""Per-host request pacing for upstreams with a tight per-IP quota.

The GLEIF throttle (:mod:`opencheck.gleif_throttle`, Phase 143) paces one host
and carries GLEIF-only machinery (a lookup reserve, discretionary calls, call
statistics). This module is the general form for any other host whose quota is
per caller IP, configured from settings and installed at the same choke point —
:func:`opencheck.http.build_client` — so no adapter has to opt in and no future
call site can forget to.

**Why it exists — KvK open data (27 Oct 2026).** ``opendata.kvk.nl`` allows one
request per minute per IP address (plus 200 per five minutes across all
callers combined) and answers a spent quota with a bare 429: no
``Retry-After``, no ``RateLimit-*`` headers. Nothing paced it, and the
adapter's retry loop waited 2 + 4 + 8 ≈ 14 s against a 60 s window, so once the
minute was spent every retry was certain to fail and was itself another refused
request. Two of three Dutch company cards checked in production that morning
showed placeholder KvK data.

What a governed host gets:

* **A sliding-window budget** of ``limit`` requests per ``window_s``. A caller
  waits for a slot for at most ``max_wait_s``; if none frees in time it gets
  :class:`HostRateLimitedError` *without anything being sent* — a request we
  know will be refused only spends the window we are waiting on.
* **A penalty box fed by observed 429s.** The server's ``Retry-After`` when it
  sends a number, else one full window — the honest guess for a quota that
  says nothing about when it resets.
* **Retries inside the same deadline.** After a 429 the request is re-sent once
  the penalty has passed, as long as that is still within ``max_wait_s`` of the
  caller's first attempt. An interactive lookup (short wait) therefore gives up
  at once and degrades honestly; a batch job (the sweep sets a long wait) queues
  through the window instead.

The budget is process-global (one uvicorn worker = one budget, which is how
production runs). Its state is plain data mutated only between awaits on the
single event loop, so it needs no lock. ``limit <= 0`` disables a host's
throttle; the test suite does that in ``conftest.py`` and re-enables it where a
test needs it.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

from .config import Settings, get_settings

log = logging.getLogger(__name__)

#: The KvK open-data host (High-Value Datasets API).
KVK_OPENDATA_HOST = "opendata.kvk.nl"

#: The Taiwan GCIS open-data host (MOEA company register API).
GCIS_TAIWAN_HOST = "data.gcis.nat.gov.tw"

#: The Guernsey Registry's public search API (60 requests a minute).
GUERNSEY_REGISTRY_HOST = "portal.guernseyregistry.com"

#: Ceiling for honouring a server-sent Retry-After — a hostile or confused
#: header must not park the process for longer than this.
_MAX_RETRY_AFTER_S = 300.0


@dataclass(frozen=True)
class HostLimits:
    """One governed host's quota, as read from settings."""

    host: str
    limit: int
    window_s: float
    max_wait_s: float


def _kvk_limits(settings: Settings) -> HostLimits:
    return HostLimits(
        host=KVK_OPENDATA_HOST,
        limit=settings.kvk_rate_limit_per_minute,
        window_s=60.0,
        max_wait_s=settings.kvk_throttle_max_wait_s,
    )


def _gcis_limits(settings: Settings) -> HostLimits:
    return HostLimits(
        host=GCIS_TAIWAN_HOST,
        limit=settings.gcis_rate_limit_per_minute,
        window_s=60.0,
        max_wait_s=settings.gcis_throttle_max_wait_s,
    )


def _guernsey_limits(settings: Settings) -> HostLimits:
    return HostLimits(
        host=GUERNSEY_REGISTRY_HOST,
        limit=settings.guernsey_rate_limit_per_minute,
        window_s=60.0,
        max_wait_s=settings.guernsey_throttle_max_wait_s,
    )


#: host → how to read its limits from settings. Add a host here, give it two
#: settings, and every adapter that reaches it through ``build_client`` is
#: paced. GLEIF is deliberately not here: it keeps its own throttle.
_LIMIT_READERS: dict[str, Callable[[Settings], HostLimits]] = {
    KVK_OPENDATA_HOST: _kvk_limits,
    GCIS_TAIWAN_HOST: _gcis_limits,
    GUERNSEY_REGISTRY_HOST: _guernsey_limits,
}


class HostRateLimitedError(Exception):
    """No request slot for ``host`` frees within the caller's max wait.

    Raised *instead of sending*: nothing reached the upstream. Callers treat it
    exactly like an observed 429 that outlasted its retries.
    """

    def __init__(self, host: str, message: str) -> None:
        super().__init__(message)
        self.host = host


class HostThrottle:
    """Sliding-window request budget plus a 429-fed penalty box for one host.

    ``clock`` and ``sleep`` are injectable so tests can run a minute-long
    window in no time.
    """

    def __init__(
        self,
        host: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.host = host
        self._clock = clock
        self._sleep = sleep
        self._sent: deque[float] = deque()
        self._penalty_until: float = 0.0

    # -- configuration -----------------------------------------------------

    def limits(self) -> HostLimits:
        return _LIMIT_READERS[self.host](get_settings())

    @property
    def enabled(self) -> bool:
        return self.limits().limit > 0

    # -- state -------------------------------------------------------------

    def _prune(self, now: float, window_s: float) -> None:
        cutoff = now - window_s
        while self._sent and self._sent[0] <= cutoff:
            self._sent.popleft()

    def next_slot_at(self) -> float:
        """The earliest clock time at which a request could be sent."""
        lim = self.limits()
        now = self._clock()
        self._prune(now, lim.window_s)
        window_free = (
            self._sent[0] + lim.window_s if len(self._sent) >= lim.limit else 0.0
        )
        return max(now, self._penalty_until, window_free)

    def penalise(self, seconds: float) -> None:
        """Hold every caller back for ``seconds`` after an observed 429."""
        seconds = min(max(seconds, 0.0), _MAX_RETRY_AFTER_S)
        self._penalty_until = max(self._penalty_until, self._clock() + seconds)

    def deadline(self) -> float:
        """The latest clock time a caller starting now may wait until."""
        return self._clock() + max(self.limits().max_wait_s, 0.0)

    async def acquire(self, deadline: float) -> None:
        """Take one slot, waiting until at most ``deadline``.

        Raises :class:`HostRateLimitedError` — deliberately without sending —
        when no slot frees in time.
        """
        lim = self.limits()
        if lim.limit <= 0:
            return
        while True:
            at = self.next_slot_at()
            now = self._clock()
            if at <= now:
                self._sent.append(now)
                return
            if at > deadline:
                raise HostRateLimitedError(
                    self.host,
                    f"{self.host} allows {lim.limit} request(s) per "
                    f"{lim.window_s:.0f}s from this server and the next slot is "
                    f"{at - now:.0f}s away, beyond the {lim.max_wait_s:.0f}s wait; "
                    "not sent",
                )
            # Short steps, so a slot that frees early is taken promptly.
            await self._sleep(min(max(at - now, 0.05), 1.0))


_throttles: dict[str, HostThrottle] = {}


def get_host_throttle(host: str) -> HostThrottle | None:
    """The process-wide throttle for ``host``, or ``None`` if it is not governed."""
    host = host.lower()
    if host not in _LIMIT_READERS:
        return None
    if host not in _throttles:
        _throttles[host] = HostThrottle(host)
    return _throttles[host]


def reset_host_throttles_for_tests() -> None:
    _throttles.clear()


def _penalty_for(response: httpx.Response, window_s: float) -> float:
    """Retry-After (delta-seconds) when sent, else one full window."""
    raw = response.headers.get("Retry-After", "")
    try:
        return float(raw)
    except ValueError:
        return window_s


class HostThrottledTransport(httpx.AsyncBaseTransport):
    """Transport wrapper that paces every governed host and passes the rest.

    A 429 from a governed host penalises the whole process and the request is
    re-sent once the penalty has passed — as often as the caller's deadline
    allows. When the deadline does not allow another try, the last 429 is
    returned as-is and the caller decides what it means (for KvK: an honest
    ``rate_limited`` degradation, never a cached "no record").
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        throttle = get_host_throttle(request.url.host or "")
        if throttle is None or not throttle.enabled:
            return await self._inner.handle_async_request(request)

        deadline = throttle.deadline()
        await throttle.acquire(deadline)
        while True:
            response = await self._inner.handle_async_request(request)
            if response.status_code != 429:
                return response
            throttle.penalise(_penalty_for(response, throttle.limits().window_s))
            if throttle.next_slot_at() > deadline:
                log.warning(
                    "%s answered 429 on %s and the next slot is past this "
                    "request's wait; handing the 429 back",
                    throttle.host,
                    request.url.path,
                )
                return response
            log.warning(
                "%s answered 429 on %s — waiting for the next slot and retrying",
                throttle.host,
                request.url.path,
            )
            await response.aclose()
            await throttle.acquire(deadline)

    async def aclose(self) -> None:
        await self._inner.aclose()
