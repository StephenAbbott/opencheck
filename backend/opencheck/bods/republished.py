"""Fold a republished copy of a source into the source's own read.

OpenSanctions republishes Global Energy Monitor's Global Energy Ownership
Tracker (dataset ``gem_energy_ownership``), and OpenCheck reads the tracker
directly as the ``climatetrace`` source. On Rosneft's lookup of 11 Nov 2026
the same GEM rows reached the graph twice: once from GEM (BP Russian
Investments, 19.75%) and once through OpenSanctions (a second BP node, and
the treasury holdings integrated ownership turned into "at least 21.9%").
Two sources appeared to agree; they were one.

Stephen's decision on the undated-relationships ticket (11 Nov 2026): fold
the republished edges into GEM's own read, and keep them only as a labelled
fallback. So, for one lookup:

* An OpenSanctions relationship carried by GEM's tracker alone is marked in
  the FtM mapper (``source.assertedBy`` Global Energy Monitor —
  ``bods.mappers.ftm.is_gem_republished``).
* When GEM's own read answered for the subject — the ``climatetrace``
  bundle names at least one owner or parent of a subject statement — every
  such relationship *about the subject* is dropped, with any OpenSanctions
  party statement that only it referenced.
* Otherwise (GEM has no record of the subject, or the edge is about another
  company in the OpenSanctions record) the edge stays, labelled as GEM's.

The pipeline computes the ids once (``fold``) and yields them on the
``republished_folded`` event; ``fold_lookup_events`` removes them from the
response with ``apply``, so a saved report folds as its run did. Per-source
views (``/deepen``, the Data drawer, the FullCheck hop) keep the labelled
edge: they show what OpenSanctions says.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .refs import party_ref, resolver
from .source_ids import source_id_of

#: The publisher whose republished copy is folded, and the source that reads
#: it directly.
GEM_PUBLISHER = "Global Energy Monitor"
REPUBLISHER = "opensanctions"
DIRECT_SOURCE = "climatetrace"


def _asserted_by_gem(stmt: Mapping[str, Any]) -> bool:
    source = stmt.get("source") or {}
    return any(
        isinstance(a, Mapping) and a.get("name") == GEM_PUBLISHER
        for a in source.get("assertedBy") or []
    )


def fold(
    statements: list[dict[str, Any]], subject_ids: Iterable[str]
) -> dict[str, Any]:
    """The statements to drop, as the ``republished_folded`` event payload.

    ``subject_ids`` are the subject's statementIds (``subject_identity``).
    Returns ``{"statement_ids": [], ...}`` when nothing is folded.
    """
    subjects = set(subject_ids)
    resolve = resolver(statements)
    rels = [s for s in statements if s.get("recordType") == "relationship"]

    def subject_of(rel: Mapping[str, Any]) -> str | None:
        ref = party_ref((rel.get("recordDetails") or {}).get("subject"))
        return resolve(ref) if ref else None

    gem_answered = any(
        source_id_of(r) == DIRECT_SOURCE and subject_of(r) in subjects for r in rels
    )
    empty: dict[str, Any] = {"statement_ids": [], "folded_into": DIRECT_SOURCE, "relationships": 0}
    if not gem_answered:
        return empty
    dropped = [
        r
        for r in rels
        if source_id_of(r) == REPUBLISHER
        and _asserted_by_gem(r)
        and subject_of(r) in subjects
    ]
    if not dropped:
        return empty
    dropped_ids = {str(r.get("statementId")) for r in dropped}

    def parties(rel: Mapping[str, Any]) -> set[str]:
        details = rel.get("recordDetails") or {}
        out = set()
        for key in ("subject", "interestedParty"):
            ref = party_ref(details.get(key))
            if ref:
                out.add(resolve(ref))
        return out

    orphaned: set[str] = set()
    for rel in dropped:
        orphaned |= parties(rel)
    for rel in rels:
        if str(rel.get("statementId")) not in dropped_ids:
            orphaned -= parties(rel)
    orphaned -= subjects
    party_ids = {
        str(s.get("statementId"))
        for s in statements
        if s.get("recordType") in ("entity", "person")
        and str(s.get("statementId")) in orphaned
        and source_id_of(s) == REPUBLISHER
    }
    return {
        "statement_ids": sorted(dropped_ids | party_ids),
        "folded_into": DIRECT_SOURCE,
        "relationships": len(dropped_ids),
    }


def apply(
    statements: list[dict[str, Any]], payload: Mapping[str, Any] | None
) -> list[dict[str, Any]]:
    """``statements`` without the ones the event names (a new list)."""
    drop = set((payload or {}).get("statement_ids") or [])
    if not drop:
        return statements
    return [s for s in statements if str(s.get("statementId")) not in drop]
