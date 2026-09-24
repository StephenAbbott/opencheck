"""Which sources applied to a lookup, and what each one said (Phase 241).

One definition, read by every surface that counts sources — the MCP summary
line, the batch row (and so the watchlist baseline and the batch CSV), and the
PDF / Markdown report. The web report computes the same figures from the
stream (``frontend/src/lib/lookupProgress.ts`` ``settledCount``); the two are
kept in step by the tests on each side, not by a shared file.

Before this, three surfaces used three denominators. The MCP summary divided
the sources *with a result* by the sources *with a result or an error*, so a
company to which thirteen sources applied read "11 of 11 sources returned
data" — the two that answered with no record were in neither figure. The
batch field ``answered`` counted only sources with a result, so a source that
ran and found nothing read as silent. And a source that returned a result and
then failed (OpenAleph's ``ReadTimeout`` on Scottish Mortgage) counted as a
clean answer everywhere.

The vocabulary, applied to each applicable source:

* **with data** — it returned at least one real record;
* **no record** — it answered, and the answer is that it holds nothing on
  this company (a completed search with no result, or a coverage-note card
  such as INPI's "not in the RNE");
* **answered** — with data or no record: the source was asked and replied;
* **did not answer** — it errored and returned nothing;
* **partial** — it returned a record *and* errored (the read of the record
  failed). Counted as answered with data, because it did reply, and recorded
  as a degradation (``degradation.CHECK_SOURCE_READ``) by the pipeline.

The GLEIF anchor is always one of the applicable sources when the lookup
resolved: ``sources_applicable`` never lists it (it has answered before that
event fires — Phase 126), but it is one of the registry's sources and it did
answer, which is how ``coverageCopy()`` has counted it since Phase 156.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

ANCHOR = "gleif"


def _field(hit: Any, name: str) -> Any:
    if isinstance(hit, Mapping):
        return hit.get(name)
    return getattr(hit, name, None)


def _has_record(hit: Any) -> bool:
    """A hit that is a real record — not a stub, not a coverage-note card."""
    if _field(hit, "is_stub"):
        return False
    raw = _field(hit, "raw")
    if isinstance(raw, Mapping) and raw.get("not_found"):
        return False
    return True


def source_coverage(
    hits: Iterable[Any],
    errors: Mapping[str, str] | None,
    sources_applicable: Iterable[str] | None,
    *,
    anchored: bool = True,
) -> dict[str, Any]:
    """Counts and ids for every applicable source, in one shape.

    ``hits`` may be ``SourceHit`` models or their dicts (a report payload);
    ``errors`` is the lookup's ``errors`` map, which holds fetch errors and —
    folded in by ``fold_lookup_events`` — read (deepen) errors. A source that
    produced a hit or an error without being announced in
    ``sources_applicable`` is added to the applicable list, so every source the
    report shows is also counted and named.
    """
    hits = list(hits or [])
    errors = dict(errors or {})

    with_data: set[str] = set()
    seen: list[str] = []
    for h in hits:
        sid = _field(h, "source_id")
        if not sid:
            continue
        seen.append(sid)
        if _has_record(h):
            with_data.add(sid)

    applicable = list(
        dict.fromkeys(
            [
                *([ANCHOR] if anchored else []),
                *(sources_applicable or []),
                *seen,
                *errors.keys(),
            ]
        )
    )

    with_data_ids = [s for s in applicable if s in with_data]
    failed_ids = [s for s in applicable if s in errors and s not in with_data]
    partial_ids = [s for s in applicable if s in errors and s in with_data]
    no_record_ids = [
        s for s in applicable if s not in with_data and s not in errors
    ]
    answered_ids = [s for s in applicable if s not in failed_ids]

    return {
        "applicable": len(applicable),
        "answered": len(answered_ids),
        "with_data": len(with_data_ids),
        "no_record": len(no_record_ids),
        "failed": len(failed_ids),
        "applicable_ids": applicable,
        "answered_ids": answered_ids,
        "with_data_ids": with_data_ids,
        "no_record_ids": no_record_ids,
        "failed_ids": failed_ids,
        "partial_ids": partial_ids,
    }


def coverage_sentence(cov: Mapping[str, Any]) -> str:
    """"12 of 13 sources answered (9 with records, 3 with no record); 1 did not answer."

    The same numerator and denominator the web report's Coverage column
    uses. Never "returned data" as the headline: a source that answered
    "nothing on this company" answered.
    """
    applicable = int(cov.get("applicable") or 0)
    answered = int(cov.get("answered") or 0)
    with_data = int(cov.get("with_data") or 0)
    no_record = int(cov.get("no_record") or 0)
    failed = int(cov.get("failed") or 0)
    noun = "source" if applicable == 1 else "sources"
    parts = [f"{answered} of {applicable} {noun} answered"]
    split = []
    if with_data:
        split.append(f"{with_data} with {'a record' if with_data == 1 else 'records'}")
    if no_record:
        split.append(f"{no_record} with no record")
    if split and (with_data and no_record):
        parts[0] += f" ({', '.join(split)})"
    elif no_record and not with_data:
        parts[0] += ", none with a record"
    sentence = parts[0]
    if failed:
        sentence += f"; {failed} did not answer"
    return sentence


def report_coverage(report: Mapping[str, Any]) -> dict[str, Any]:
    """``source_coverage`` for a report payload (a ``LookupResponse`` dump,
    live or read back from a saved report). Anchored when GLEIF resolved the
    LEI — there is a legal name, or a GLEIF hit."""
    hits = list(report.get("hits") or [])
    anchored = bool(report.get("legal_name")) or any(
        _field(h, "source_id") == ANCHOR for h in hits
    )
    return source_coverage(
        hits,
        report.get("errors") or {},
        report.get("sources_applicable") or [],
        anchored=anchored,
    )
