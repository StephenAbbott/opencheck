"""Per-lookup collection of source degradations recorded by adapters.

``risk.DegradedSource`` already carries "this screen did not fully run" for the
*derived* checks — cross-source name screening, ICIJ, OpenAleph. Those receive
a ``degraded`` list as a parameter, because the pipeline calls them directly.
A **source adapter** has no such channel: it is called deep inside a dispatch
task, and its only way to say "the register refused me" was to return
something that looked like an answer.

That is how Lithuania read as healthy. ``jar_lithuania`` got HTTP 403 from the
JAR public interface, logged a warning, and returned a bundle carrying the
GLEIF legal name with every register field null and ``is_stub: False``. To
every consumer — the UI, the export, the weekly health sweep — that is a
successful lookup of a company about which the register happens to say little.
The distinction that matters, *did the register actually answer*, was visible
only in the server log.

This module gives adapters the same recorder shape ``provenance`` uses: a
``ContextVar`` opened once per lookup, written to from anywhere beneath it. An
asyncio task copies the context on creation and the value is a list, so
concurrent source fetches append to the same list without stepping on each
other. Outside a scope ``record()`` is a no-op, so adapters, scripts and tests
can call it freely.

The privacy contract is ``DegradedSource``'s: ``detail`` carries counts and
source/check names only, never entity or person names.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .risk import DegradedSource

#: The check name adapters use when the source itself did not answer, as
#: distinct from a derived screen that could not run.
CHECK_SOURCE_FETCH = "source_fetch"
#: Phase 241: the source returned a record and then failed while it was being
#: read in full (a deepen error) — a partial answer, not a missing one.
CHECK_SOURCE_READ = "source_read"

# ``risk`` imports ``sources``, so an adapter importing ``risk`` at module
# level is a circular import. These mirror the DEGRADED_* constants there and
# ``test_source_probes.py`` asserts they stay identical; ``DegradedSource``
# itself is imported lazily, inside record().
REASON_UPSTREAM_ERROR = "upstream_error"
REASON_TIMEOUT = "timeout"
REASON_NOT_CONFIGURED = "not_configured"
REASON_RATE_LIMITED = "rate_limited"


def reason_for_failure(failure: str) -> str:
    """Map a short failure label ("HTTP 403", "ConnectTimeout") onto the closed
    reason vocabulary.

    ``risk.classify_degradation_reason`` does this for a live exception; an
    adapter that has already swallowed the error has only the label left.
    """
    # ``in``, not ``startswith``: a pipeline error string carries the
    # exception type first ("HTTPStatusError: HTTP 429 Too Many Requests …").
    if "HTTP 429" in failure:
        return REASON_RATE_LIMITED
    if "Timeout" in failure:
        return REASON_TIMEOUT
    return REASON_UPSTREAM_ERROR

_CURRENT: contextvars.ContextVar[list[DegradedSource] | None] = contextvars.ContextVar(
    "opencheck_degradations", default=None
)


def begin() -> None:
    """Open a collection scope for one lookup.

    Deliberately not a context manager at the call sites in the lookup
    pipeline: those functions are hundreds of lines long and re-indenting them
    under a ``with`` would be a large diff for no behavioural gain. Call
    ``begin()`` at the top and ``collect()`` where the degraded list is built.
    """
    _CURRENT.set([])


def collect() -> list[DegradedSource]:
    """Everything recorded since ``begin()``, and close the scope.

    Returns a fresh list, so the caller owns it and can keep appending — which
    is exactly what the pipeline does when the derived checks run afterwards.
    """
    recorded = _CURRENT.get() or []
    _CURRENT.set(None)
    return list(recorded)


def record(
    source_id: str,
    detail: str,
    *,
    check: str = CHECK_SOURCE_FETCH,
    reason: str = REASON_UPSTREAM_ERROR,
    affected_signals: Sequence[str] = (),
) -> None:
    """Note that a source did not answer from its own data. No-op outside a scope.

    ``detail`` must carry counts and identifiers of *sources*, never the names
    of the entities or people being looked up — same rule as every other
    ``DegradedSource``, enforced by ``test_degraded_sources.py``.
    """
    current = _CURRENT.get()
    if current is None:
        return
    from .risk import DegradedSource  # lazy: risk imports sources

    # Identical observations collapse. An adapter that loops — EITI fetches
    # revenue per matched organisation — would otherwise record the same
    # sentence once per iteration, and a degraded_sources list with four
    # identical rows reads as four problems. The detail text is deliberately
    # count-free, so a repeat carries no information the first does not.
    if any(
        d.source_id == source_id
        and d.check == check
        and d.reason == reason
        and d.detail == detail
        for d in current
    ):
        return

    current.append(
        DegradedSource(
            source_id=source_id,
            check=check,
            affected_signals=list(affected_signals),
            detail=detail,
            reason=reason,
        )
    )


#: The ``detail`` of a degradation built from a pipeline error. Fixed
#: sentences, so no entity name can reach them; each reads after the source's
#: name ("OpenAleph — did not answer …").
SOURCE_ERROR_DETAIL = "did not answer, so its records were not consulted"
SOURCE_READ_DETAIL = (
    "returned a result, then failed before it could be read in full — "
    "its records are incomplete in this report"
)


def add_source_errors(
    degraded: list[DegradedSource],
    errors: dict[str, str],
    *,
    with_data: set[str],
) -> None:
    """Phase 241: every source error is a degradation.

    A source that errored used to appear only on its own card. Its absence
    from ``degraded_sources`` meant the verdict, the MCP summary's CAUTION
    and the batch row's ``degraded`` flag all read the lookup as complete —
    and a source that returned a result *and* then failed (OpenAleph's
    ``ReadTimeout`` while its record was read, on Scottish Mortgage) read as
    a clean answer everywhere. Appends one record per errored source that an
    adapter has not already recorded: ``CHECK_SOURCE_FETCH`` when it returned
    nothing, ``CHECK_SOURCE_READ`` when it returned a record (``with_data``).
    """
    from .risk import DegradedSource  # lazy: risk imports sources

    covered = {
        d.source_id
        for d in degraded
        if d.check in (CHECK_SOURCE_FETCH, CHECK_SOURCE_READ)
    }
    for source_id in sorted(errors):
        if source_id in covered:
            continue
        partial = source_id in with_data
        degraded.append(
            DegradedSource(
                source_id=source_id,
                check=CHECK_SOURCE_READ if partial else CHECK_SOURCE_FETCH,
                affected_signals=[],
                detail=SOURCE_READ_DETAIL if partial else SOURCE_ERROR_DETAIL,
                reason=reason_for_failure(errors[source_id] or ""),
            )
        )


@contextmanager
def recording() -> Iterator[list[DegradedSource]]:
    """Scoped form, for tests and scripts."""
    begin()
    collected: list[DegradedSource] = []
    try:
        yield collected
    finally:
        collected.extend(collect())


__all__ = [
    "CHECK_SOURCE_FETCH",
    "CHECK_SOURCE_READ",
    "REASON_NOT_CONFIGURED",
    "REASON_RATE_LIMITED",
    "REASON_TIMEOUT",
    "REASON_UPSTREAM_ERROR",
    "add_source_errors",
    "begin",
    "collect",
    "reason_for_failure",
    "record",
    "recording",
]
