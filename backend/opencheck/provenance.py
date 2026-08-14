"""Where a payload came from, and when we actually obtained it.

OpenCheck republishes other people's data. The BODS dates guidance is firm
about what that obliges us to say: ``source.retrievedAt`` is "only applicable
where data is being republished", and when republishing, publishers "must
provide information on when they downloaded the data from the government
registry".

Before this module every statement claimed ``retrievedAt`` = the moment the
BODS mapper ran, regardless of whether the payload came from a live API call, a
filesystem cache written last week, a bulk parquet snapshot built last month, or
a curated fixture committed to the repo. Four very different provenance claims,
one timestamp, and the timestamp was wrong for three of them.

Rather than edit every adapter, provenance is recorded at the two chokepoints
every adapter already passes through:

* ``opencheck.cache.Cache`` — reads record ``curated`` (demo fixtures) or
  ``cached`` (runtime cache) along with the entry's real age.
* ``opencheck.http.build_client`` — constructing an HTTP client records a
  ``live`` fetch at the current time.

Adapters that read neither (bulk parquet, committed JSON) call ``record()``
directly. Anything that records nothing at all stays ``stub``, so a source that
forgets to declare itself under-claims rather than over-claims.

Resolution is deliberately pessimistic. A Companies House lookup issues several
requests, some served from cache and some live; the resulting bundle is only as
fresh as its stalest component, so the worst liveness wins and the *oldest*
retrieval time is reported.

The recorder lives in a ``ContextVar``, so concurrent source dispatch (each
source runs in its own ``asyncio`` task, which copies the context on creation)
cannot bleed one source's provenance into another's.
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Literal

Liveness = Literal["live", "cached", "snapshot", "curated", "stub"]

# Ordered best (most current) to worst. "Worst" is not a judgement about data
# quality — a curated Nigerian BO set is more useful than a live call that
# returns nothing. It orders how confidently we can say the payload reflects
# the register *right now*, which is the only question retrievedAt answers.
_SEVERITY: dict[str, int] = {
    "live": 0,
    "cached": 1,
    "snapshot": 2,
    "curated": 3,
    "stub": 4,
}

# Human-readable labels, reused by the API and the frontend.
LIVENESS_LABELS: dict[str, str] = {
    "live": "Live",
    "cached": "Cached",
    "snapshot": "Snapshot",
    "curated": "Curated",
    "stub": "Stub",
}

LIVENESS_DESCRIPTIONS: dict[str, str] = {
    "live": "Fetched from the source at lookup time.",
    "cached": "Served from OpenCheck's response cache; retrieved earlier.",
    "snapshot": "Read from a bulk dataset built on the date shown.",
    "curated": "A fixture committed to the repository, not fetched live.",
    "stub": "Placeholder data — no live source was contacted.",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Provenance:
    """Resolved provenance for one source's contribution to a lookup."""

    liveness: Liveness = "stub"
    retrieved_at: datetime | None = None
    detail: str | None = None

    @property
    def is_live(self) -> bool:
        return self.liveness == "live"

    @property
    def label(self) -> str:
        return LIVENESS_LABELS.get(self.liveness, self.liveness)

    def retrieved_at_iso(self) -> str | None:
        """UTC ISO-8601 with a trailing Z, or None when we never fetched."""
        if self.retrieved_at is None:
            return None
        moment = self.retrieved_at
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return (
            moment.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "liveness": self.liveness,
            "label": self.label,
            "retrieved_at": self.retrieved_at_iso(),
            "detail": self.detail,
        }


STUB_PROVENANCE = Provenance()


@dataclass
class _Entry:
    liveness: Liveness
    retrieved_at: datetime | None
    detail: str | None


class Recorder:
    """Collects provenance observations made during a single source fetch."""

    def __init__(self) -> None:
        self._entries: list[_Entry] = []

    def record(
        self,
        liveness: Liveness,
        retrieved_at: datetime | None = None,
        detail: str | None = None,
    ) -> None:
        self._entries.append(_Entry(liveness, retrieved_at, detail))

    def resolve(self, *, is_stub: bool = False) -> Provenance:
        """Collapse the observations into one claim.

        Stub payloads short-circuit: a placeholder must never carry a retrieval
        time, whatever happened to be recorded while producing it.
        """
        if is_stub or not self._entries:
            return STUB_PROVENANCE

        worst = max(self._entries, key=lambda e: _SEVERITY.get(e.liveness, 4))
        moments = [e.retrieved_at for e in self._entries if e.retrieved_at is not None]
        # The bundle is only as fresh as its stalest component.
        oldest = min(moments) if moments else None

        detail = worst.detail
        if detail is None:
            for entry in self._entries:
                if entry.detail:
                    detail = entry.detail
                    break

        return Provenance(
            liveness=worst.liveness, retrieved_at=oldest, detail=detail
        )


_CURRENT: contextvars.ContextVar[Recorder | None] = contextvars.ContextVar(
    "opencheck_provenance_recorder", default=None
)


@contextmanager
def recording() -> Iterator[Recorder]:
    """Open a provenance scope for one source fetch."""
    recorder = Recorder()
    token = _CURRENT.set(recorder)
    try:
        yield recorder
    finally:
        _CURRENT.reset(token)


def record(
    liveness: Liveness,
    retrieved_at: datetime | None = None,
    detail: str | None = None,
) -> None:
    """Record an observation. A no-op outside a scope, so adapters, scripts and
    tests can call cache/HTTP helpers without having to open one."""
    recorder = _CURRENT.get()
    if recorder is not None:
        recorder.record(liveness, retrieved_at, detail)


def record_live(detail: str | None = None) -> None:
    record("live", _utcnow(), detail)


def record_cached(retrieved_at: datetime | None, detail: str | None = None) -> None:
    record("cached", retrieved_at, detail)


def record_curated(
    detail: str | None = None, harvested_at: datetime | None = None
) -> None:
    """A fixture committed to the repository.

    ``harvested_at`` is only for curated sets that record when they were
    actually harvested from the register (``cac_nigeria``'s index declares a
    ``meta.harvested`` date, for instance). Otherwise no retrieval time is
    claimed: a checked-out file's mtime records when git wrote it locally,
    which says nothing about when the data left the register, and asserting it
    would be exactly the invented-precision problem this module exists to
    remove.
    """
    record("curated", harvested_at, detail)


def record_snapshot(
    built_at: datetime | None, detail: str | None = None
) -> None:
    """A bulk dataset. ``built_at`` should be the upstream extract date where it
    is known, not merely when the local artifact file was written."""
    record("snapshot", built_at, detail)


# ----------------------------------------------------------------------
# Mapping-time context
# ----------------------------------------------------------------------
# The BODS mappers are called as ``map_<source>(raw)`` from the lookup pipeline
# and have no access to the SourceHit that carries the resolved provenance.
# Rather than thread a parameter through ~55 mapper functions and every
# make_*_statement call site, the pipeline sets the active provenance around
# each (synchronous, non-awaiting) mapper invocation and _source_block reads it.

_MAPPING: contextvars.ContextVar[Provenance] = contextvars.ContextVar(
    "opencheck_provenance_mapping", default=STUB_PROVENANCE
)


@contextmanager
def mapping_provenance(provenance: Provenance | None) -> Iterator[None]:
    token = _MAPPING.set(provenance or STUB_PROVENANCE)
    try:
        yield
    finally:
        _MAPPING.reset(token)


def current_mapping_provenance() -> Provenance:
    return _MAPPING.get()
