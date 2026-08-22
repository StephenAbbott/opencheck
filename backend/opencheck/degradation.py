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
    if failure.startswith("HTTP 429"):
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

    current.append(
        DegradedSource(
            source_id=source_id,
            check=check,
            affected_signals=list(affected_signals),
            detail=detail,
            reason=reason,
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
    "REASON_NOT_CONFIGURED",
    "REASON_RATE_LIMITED",
    "REASON_TIMEOUT",
    "REASON_UPSTREAM_ERROR",
    "begin",
    "collect",
    "reason_for_failure",
    "record",
    "recording",
]
