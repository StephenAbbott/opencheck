"""signalstats — which sources actually contribute which risk signals.

Motivation (2026-08-16): "is OpenAleph screening contributing anything in
production?" was unanswerable without running lookups by hand and counting.
The only available method was a client-side sweep, and a sweep is both
expensive and misleading: every ``/lookup`` is a live fan-out across ~38
upstreams, so even a few hundred LEIs would put real load on a free-tier
instance, risk tripping upstream rate limits — which produces **degraded**
results that read as "signal absent", the exact trap that caused two
retracted findings during Phase 98 testing — pull CC BY-NC data at volume
for what is effectively analytics, and yield a sample biased toward
whichever LEIs happened to be chosen.

Server-side counting has none of those problems. It observes traffic that
was going to happen anyway, is exact rather than sampled, and keeps
working without anyone re-running anything.

What is counted, and where:

* **Signals per ``(code, source_id)``** — recorded inside
  ``_merge_signals`` in ``routers/lookup.py``, which is where the
  deduplication rules live. Counting there rather than at the call site
  makes "count after dedup" true by construction rather than by
  discipline, which matters because related-party paths now emit multiple
  signals per hit: pre-dedup numbers would overstate.
* **``degraded_sources`` per ``(source_id, check, reason)``** — a signal
  count is meaningless without the denominator of screens that actually
  ran. An empty signal list next to a non-empty degraded list is not a
  clean screen, and the same holds in aggregate.
* **Lookups** — so counts can be read as "per lookup" rather than as
  absolutes.
* **Companies House walks** (Phase 184) — recorded by the adapter at the
  end of every corporate-PSC walk, split by *origin* (a subject lookup or
  a FullCheck register hop, set through ``walk_origin`` by the caller):
  how many related companies the walk pulled in, the depth it reached,
  why it stopped (the closed ``unfollowed`` reason vocabulary), how many
  register calls it made and how many were answered from the cache, and
  the wall time. This is the measurement the "Live-fed UK PSC graph"
  ticket asks for before a local graph is built: whether the live walk —
  four serial register calls per company, a 600-per-five-minutes key —
  is what limits UK chains in production, and how deep those chains go.

**Privacy.** Counts are aggregate only. The recorders read *only* closed-
vocabulary fields — a signal's ``code`` and ``source_id``, a degradation's
``source_id`` / ``check`` / ``reason`` — and never ``summary``, ``hit_id``,
``evidence`` or a degradation's free-text ``detail``. Entity names, LEIs
and related-party names are structurally unable to reach these counters,
which is the same contract ``degraded_sources`` already establishes.
``test_signalstats.py`` enforces it.

**Durability.** These are in-process counters, so they reset on every
deploy and whenever Render spins the instance down — exactly like
``memwatch``. That is acceptable for the rough picture this is for; making
it durable means scraping ``/signalstats`` periodically, which is a
separate decision and deliberately not made here.

Everything fails soft: instrumentation must never take down or slow a
lookup, so every public entry point swallows its own errors.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from contextvars import ContextVar
from typing import Any, Iterable, Mapping

log = logging.getLogger("opencheck.signalstats")

#: Codes assessed against the MERGED bundle by the cross-source screens,
#: rather than produced by a single source's own record. They answer a
#: different question from subject-level codes — screening *reach* versus
#: subject *risk* — so ``stats()`` reports them split as well as combined.
_RELATED_PREFIX = "RELATED_"

#: Upper bound on distinct keys per counter. The real matrix is ~25 signal
#: codes x ~38 sources, so this is nowhere near a working limit — it exists
#: so that a defect upstream (an id built from an unbounded value, say)
#: becomes a visible ``truncated`` flag instead of unbounded memory growth
#: on a small instance.
_MAX_KEYS = 2_000


#: Where a Companies House walk was started from. ``"lookup"`` is the
#: subject's own dispatch in ``_lookup_pipeline``; ``"hop"`` is a FullCheck
#: register hop (``_register_one_layer`` sets it for the duration of the
#: fetch). A ContextVar because the adapter is the one place that knows a
#: walk's shape, and it has no other way to know who asked.
walk_origin: ContextVar[str] = ContextVar("opencheck_ch_walk_origin", default="lookup")

_WALK_ORIGINS = ("lookup", "hop")

#: Related-company histogram buckets. The cap is 25 per walk, so the top
#: bucket is closed.
_RELATED_BUCKETS = ((0, "0"), (1, "1"), (3, "2-3"), (6, "4-6"), (12, "7-12"), (25, "13-25"))


def _related_bucket(n: int) -> str:
    for upper, label in _RELATED_BUCKETS:
        if n <= upper:
            return label
    return "26+"


class _Walks:
    """Companies House walk counters, all aggregate."""

    def __init__(self) -> None:
        self.walks: Counter[str] = Counter()  # origin
        self.related: Counter[tuple[str, str]] = Counter()  # origin, bucket
        self.depth: Counter[tuple[str, str]] = Counter()  # origin, depth reached
        self.unfollowed: Counter[tuple[str, str]] = Counter()  # origin, reason
        self.calls_live: Counter[str] = Counter()
        self.calls_cached: Counter[str] = Counter()
        self.seconds: dict[str, float] = {}
        self.max_related: Counter[str] = Counter()
        self.max_depth: Counter[str] = Counter()


class _Counters:
    """Mutable module state, guarded by a lock.

    The lock is not strictly required for the current call paths, but the
    pipeline touches ``asyncio.to_thread`` in places and a dropped
    increment would be an annoying thing to debug for no gain.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.lookups = 0
        self.signals: Counter[tuple[str, str]] = Counter()
        self.degraded: Counter[tuple[str, str, str]] = Counter()
        self.walks = _Walks()
        self.truncated = False


totals = _Counters()


def reset() -> None:
    """Clear all counters. Tests only."""
    global totals
    totals = _Counters()


def _bounded_increment(counter: Counter, key: tuple[str, ...]) -> None:
    """Increment, refusing to create a new key past the cardinality cap."""
    if key not in counter and len(counter) >= _MAX_KEYS:
        totals.truncated = True
        return
    counter[key] += 1


def record_signals(signals: Iterable[Mapping[str, Any]]) -> None:
    """Count each finalised signal under ``(code, source_id)``.

    Reads nothing but those two fields — see the privacy note above.
    Called from ``_merge_signals`` with the post-deduplication list, so a
    signal a user sees once is counted once.
    """
    try:
        with totals.lock:
            for sig in signals:
                code = sig.get("code")
                source_id = sig.get("source_id")
                if not isinstance(code, str) or not isinstance(source_id, str):
                    continue  # malformed — skip it, never fail the lookup
                _bounded_increment(totals.signals, (code, source_id))
    except Exception as exc:  # noqa: BLE001
        log.debug("signalstats.record_signals failed, ignoring: %s", exc)


def record_degraded(degraded: Iterable[Mapping[str, Any]]) -> None:
    """Count each degradation under ``(source_id, check, reason)``.

    ``detail`` is deliberately not read: it is the only free-text field on
    a ``DegradedSource``, and while its own contract already forbids names,
    a counter keyed on it would have unbounded cardinality anyway.
    """
    try:
        with totals.lock:
            for rec in degraded:
                key = (rec.get("source_id"), rec.get("check"), rec.get("reason"))
                if not all(isinstance(part, str) for part in key):
                    continue
                _bounded_increment(totals.degraded, key)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        log.debug("signalstats.record_degraded failed, ignoring: %s", exc)


def record_lookup() -> None:
    """Count one completed lookup pipeline run — the denominator.

    NOTE: this counts *pipeline executions*, not user-visible lookups. A
    page refresh, a shared URL or an SSE reconnect is answered from the
    replay cache without re-running the pipeline. Pipeline runs are the
    right denominator for "does this source contribute signals when it is
    actually queried", but they are not sessions.
    """
    try:
        with totals.lock:
            totals.lookups += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("signalstats.record_lookup failed, ignoring: %s", exc)


def record_ch_walk(
    *,
    related: int,
    depth: int,
    calls_live: int,
    calls_cached: int,
    seconds: float,
    unfollowed: Iterable[str],
) -> None:
    """Count one Companies House corporate-PSC walk under its origin.

    *related* is how many companies beyond the subject the walk pulled in
    (0–25), *depth* the deepest hop it fetched (0 = the subject only),
    *unfollowed* the reasons (closed vocabulary — the adapter's ``_SKIP_*``
    strings) it stopped following corporate PSCs, one entry per PSC not
    followed. Reads no names or numbers: the adapter passes counts and
    reason codes only, and the ``unfollowed`` entries' particulars stay on
    the bundle.
    """
    try:
        origin = walk_origin.get()
        if origin not in _WALK_ORIGINS:
            origin = "lookup"
        # Coerce everything before touching a counter, so a malformed call
        # counts nothing rather than half a walk.
        n_related = max(0, int(related))
        n_depth = max(0, int(depth))
        n_live = max(0, int(calls_live))
        n_cached = max(0, int(calls_cached))
        secs = max(0.0, float(seconds))
        reasons = [r for r in (unfollowed or ()) if isinstance(r, str)]
        with totals.lock:
            w = totals.walks
            w.walks[origin] += 1
            w.related[(origin, _related_bucket(n_related))] += 1
            w.depth[(origin, str(n_depth))] += 1
            for reason in reasons:
                _bounded_increment(w.unfollowed, (origin, reason))
            w.calls_live[origin] += n_live
            w.calls_cached[origin] += n_cached
            w.seconds[origin] = w.seconds.get(origin, 0.0) + secs
            w.max_related[origin] = max(w.max_related[origin], n_related)
            w.max_depth[origin] = max(w.max_depth[origin], n_depth)
    except Exception as exc:  # noqa: BLE001
        log.debug("signalstats.record_ch_walk failed, ignoring: %s", exc)


def _walk_stats(w: _Walks) -> dict[str, Any]:
    """The ``companies_house_walks`` section: per origin, then histograms.

    ``calls_live`` / ``calls_cached`` / ``seconds`` are totals; divide by
    ``walks`` for the per-walk figure. ``depth`` and ``related`` are
    histograms keyed ``origin|value`` so the shape of UK chains — not only
    their average — can be read off.
    """
    per_origin: dict[str, dict[str, Any]] = {}
    for origin in _WALK_ORIGINS:
        n = w.walks[origin]
        if not n:
            continue
        per_origin[origin] = {
            "walks": n,
            "calls_live": w.calls_live[origin],
            "calls_cached": w.calls_cached[origin],
            "seconds": round(w.seconds.get(origin, 0.0), 3),
            "max_related": w.max_related[origin],
            "max_depth": w.max_depth[origin],
        }
    return {
        "by_origin": per_origin,
        "related": {f"{o}|{b}": n for (o, b), n in sorted(w.related.items())},
        "depth": {f"{o}|{d}": n for (o, d), n in sorted(w.depth.items())},
        "unfollowed": {f"{o}|{r}": n for (o, r), n in sorted(w.unfollowed.items())},
    }


def _split_related(
    signals: Counter[tuple[str, str]],
) -> tuple[dict[str, int], dict[str, int]]:
    """Partition the matrix into subject-level and related-party halves."""
    subject: dict[str, int] = {}
    related: dict[str, int] = {}
    for (code, source_id), n in signals.items():
        bucket = related if code.startswith(_RELATED_PREFIX) else subject
        bucket[f"{code}|{source_id}"] = n
    return subject, related


def stats() -> dict[str, Any]:
    """Aggregate-only snapshot for ``/signalstats``.

    Flat ``"a|b"`` string keys so the payload is JSON-safe without a
    client-side tuple convention. Contains no entity names, LEIs, related-
    party names, hit ids or evidence — only closed-vocabulary codes, source
    ids, check names and degradation reasons, plus counts.
    """
    with totals.lock:
        signals = Counter(totals.signals)
        degraded = Counter(totals.degraded)
        lookups = totals.lookups
        started = totals.started
        truncated = totals.truncated
        walks = _walk_stats(totals.walks)

    subject, related = _split_related(signals)
    return {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "uptime_s": int(time.time() - started),
        # Completed pipeline runs, NOT user-visible lookups — replayed runs
        # are served from cache and never reach the pipeline.
        "lookups": lookups,
        "signals_total": sum(signals.values()),
        "signals": {f"{code}|{sid}": n for (code, sid), n in signals.items()},
        "signals_subject": subject,
        "signals_related": related,
        "degraded_total": sum(degraded.values()),
        "degraded": {"|".join(key): n for key, n in degraded.items()},
        # Phase 184: the Companies House corporate-PSC walk, by origin.
        "companies_house_walks": walks,
        # True only if the cardinality cap was hit — i.e. something is
        # generating keys it should not be, and these numbers are partial.
        "truncated": truncated,
    }
