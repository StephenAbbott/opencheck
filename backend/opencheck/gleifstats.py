"""gleifstats — where OpenCheck's GLEIF budget goes (Phase 258).

Every OpenCheck process shares one GLEIF window (``gleif_throttle``: 50
requests a minute, under GLEIF's 60 per IP). Until this module nothing said
*who* spent it: the Golden Copy ticket's measurement gate — "is ``/isins`` a
material share of the remaining GLEIF calls?" — could not be read, and a
Phase 234 discretionary refusal (the Quantexa report of 28 Sept 2026) left no
trace at all.

Counted, per **route** (which OpenCheck surface asked) and per **endpoint**
(which GLEIF endpoint was asked):

* ``sent`` — a request that left for ``api.gleif.org`` (a Retry-After retry
  counts again: it spends a slot);
* ``http_429`` — GLEIF answered 429;
* ``refused_held_for_lookups`` / ``refused_rate_limited`` — the throttle
  refused before sending, with the reason ``GleifRateLimitedError`` carries.

Plus how ``/securities`` answered (``securities``): from GLEIF's ISIN file,
from the one-day cache, live, from a stale stand-in, or not at all (by reason).

Same contract as ``/signalstats``, where it is served as the ``gleif``
section: public, aggregate only, in-process, reset on deploy. Every key is a
closed vocabulary — a route, an endpoint, a reason — so no LEI, name or path
can reach a counter; ``test_gleifstats.py`` pins that.
"""

from __future__ import annotations

import contextvars
import logging
import re
import threading
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

log = logging.getLogger(__name__)

#: The OpenCheck surface a GLEIF call was made for. Pipeline runs keep the
#: Phase 238 caller kinds; the panel routes that call GLEIF directly get their
#: own names. ``server`` = no request (warm-ups, the watchlist worker).
ROUTES = frozenset(
    {
        "stream", "api", "batch", "expand", "export", "narrative", "watch", "mcp",
        "securities", "subsidiaries", "share", "national_id", "search", "history",
        "entity", "other", "server",
    }
)

#: Checked before the pipeline kinds: the first match wins.
_PATH_ROUTES = (
    ("/securities", "securities"),
    ("/subsidiaries", "subsidiaries"),
    ("/og", "share"),
    ("/share", "share"),
    ("/resolve-national-id", "national_id"),
    ("/search", "search"),
    ("/history", "history"),
    ("/entity", "entity"),
)

#: GLEIF endpoints, from the request path. Order matters: the most specific
#: pattern first.
ENDPOINTS = (
    ("isins", re.compile(r"/lei-records/[^/]+/isins$")),
    ("parent_exception", re.compile(r"/lei-records/[^/]+/(direct|ultimate)-parent-reporting-exception$")),
    ("parent_relationship", re.compile(r"/lei-records/[^/]+/(direct|ultimate)-parent-relationship$")),
    ("parent", re.compile(r"/lei-records/[^/]+/(direct|ultimate)-parent$")),
    ("child_relationships", re.compile(r"/lei-records/[^/]+/(direct|ultimate)-child-relationships$")),
    ("children", re.compile(r"/lei-records/[^/]+/(direct|ultimate)-children$")),
    ("field_modifications", re.compile(r"/lei-records/[^/]+/field-modifications$")),
    ("lei_record", re.compile(r"/lei-records/[^/]+$")),
    ("search", re.compile(r"/(lei-records|autocompletions|fuzzycompletions)$")),
)
ENDPOINT_NAMES = frozenset({name for name, _ in ENDPOINTS} | {"other"})

OUTCOMES = frozenset(
    {"sent", "http_429", "refused_held_for_lookups", "refused_rate_limited"}
)

#: How /securities answered.
SECURITIES_SERVED = frozenset(
    {
        "file",  # GLEIF's ISIN-to-LEI file (Phase 258)
        "cache",  # the Phase 253 one-day cache
        "live",  # a live GLEIF call
        "stale",  # a Phase 253 stand-in up to 30 days old
        "unavailable_held_for_lookups",
        "unavailable_rate_limited",
        "unavailable_unreachable",
    }
)

_route: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "opencheck_gleif_route", default=None
)


def route_for_path(path: str | None) -> str:
    """The route name for an HTTP request path — always one of :data:`ROUTES`."""
    if not path:
        return "other"
    for prefix, name in _PATH_ROUTES:
        if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "?"):
            return name
    from .pipelinestats import caller_kind_for_path

    kind = caller_kind_for_path(path)
    return kind if kind in ROUTES else "other"


def current_route() -> str:
    return _route.get() or "server"


def set_route(path: str | None) -> contextvars.Token:
    """Set by ``ClientScopeMiddleware`` for the life of a request."""
    return _route.set(route_for_path(path))


def reset_route(token: contextvars.Token) -> None:
    _route.reset(token)


@contextmanager
def route_scope(name: str | None) -> Iterator[None]:
    """Attribute GLEIF calls inside the block to ``name`` (tests)."""
    token = _route.set(name)
    try:
        yield
    finally:
        _route.reset(token)


def endpoint_for(path: str) -> str:
    """The endpoint name for a GLEIF request path (``/api/v1/...``)."""
    trimmed = re.sub(r"^/api/v\d+", "", path or "").rstrip("/")
    for name, pattern in ENDPOINTS:
        if pattern.search(trimmed):
            return name
    return "other"


class _State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.calls: Counter[tuple[str, str, str]] = Counter()
        self.securities: Counter[str] = Counter()


_state = _State()


def record_call(endpoint: str, outcome: str, route: str | None = None) -> None:
    """Count one GLEIF call outcome. Never raises."""
    try:
        r = route if route in ROUTES else current_route()
        if r not in ROUTES:
            r = "other"
        e = endpoint if endpoint in ENDPOINT_NAMES else "other"
        if outcome not in OUTCOMES:
            return
        with _state.lock:
            _state.calls[(r, e, outcome)] += 1
    except Exception as exc:  # noqa: BLE001 — instrumentation never breaks a call
        log.debug("gleifstats.record_call failed: %s", exc)


def record_securities(served: str) -> None:
    """Count how one /securities answer was served. Never raises."""
    try:
        if served in SECURITIES_SERVED:
            with _state.lock:
                _state.securities[served] += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("gleifstats.record_securities failed: %s", exc)


def _blank() -> dict[str, int]:
    return {k: 0 for k in sorted(OUTCOMES)}


def stats() -> dict[str, Any]:
    """The ``gleif`` section of ``/signalstats``."""
    with _state.lock:
        calls = Counter(_state.calls)
        securities = Counter(_state.securities)
        started = _state.started
    by_route: dict[str, dict[str, int]] = {}
    by_endpoint: dict[str, dict[str, int]] = {}
    sent_by: dict[str, int] = {}
    for (route, endpoint, outcome), n in calls.items():
        by_route.setdefault(route, _blank())[outcome] += n
        by_endpoint.setdefault(endpoint, _blank())[outcome] += n
        if outcome == "sent":
            sent_by[f"{route}|{endpoint}"] = sent_by.get(f"{route}|{endpoint}", 0) + n
    sent_total = sum(v["sent"] for v in by_route.values())
    isins_sent = (by_endpoint.get("isins") or {}).get("sent", 0)
    try:
        from . import isin_index

        table = isin_index.status()
    except Exception as exc:  # noqa: BLE001 — never fail /signalstats
        log.debug("isin_index.status failed: %s", exc)
        table = {}
    return {
        "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "sent_total": sent_total,
        # The Golden Copy ticket's gate, as a number: the share of GLEIF
        # requests that were the /isins endpoint.
        "isins_share": round(isins_sent / sent_total, 4) if sent_total else None,
        "refused_total": sum(
            v["refused_held_for_lookups"] + v["refused_rate_limited"] for v in by_route.values()
        ),
        "http_429_total": sum(v["http_429"] for v in by_route.values()),
        "by_route": dict(sorted(by_route.items())),
        "by_endpoint": dict(sorted(by_endpoint.items())),
        "sent_by_route_endpoint": dict(sorted(sent_by.items())),
        "securities_served": {k: securities.get(k, 0) for k in sorted(SECURITIES_SERVED)},
        "isin_index": table,
    }


def reset_for_tests() -> None:
    global _state
    _state = _State()


__all__ = [
    "ENDPOINT_NAMES",
    "ROUTES",
    "SECURITIES_SERVED",
    "current_route",
    "endpoint_for",
    "record_call",
    "record_securities",
    "reset_for_tests",
    "reset_route",
    "route_for_path",
    "route_scope",
    "set_route",
    "stats",
]
