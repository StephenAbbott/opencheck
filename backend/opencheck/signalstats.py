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
        # True only if the cardinality cap was hit — i.e. something is
        # generating keys it should not be, and these numbers are partial.
        "truncated": truncated,
    }
