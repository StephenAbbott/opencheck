"""Source lineage — which source republishes which.

Why this exists
---------------

OpenCheck fans out across ~40 sources and, wherever several of them describe
the same company, reads their agreement as corroboration: the results page
counts "sources that independently publish the subject's LEI", the FullCheck
network marks nodes "corroborated by ≥2 sources", and the cross-source
reconciler treats two results sharing an identifier as a bridge.

Several of those sources are copies of each other. OpenCorporates republishes
the national registers; OpenAleph's ``gb-coh-psc-*`` and ``lei-*``
collections are Companies House and GLEIF; OpenSanctions carries the GLEIF
and UK PSC datasets; EveryPolitician reads the OpenSanctions database. On a
Shell plc lookup (2 Sept 2026) all 22 officer records from Companies House and
OpenCorporates agreed on every name and birth month — which corroborates
nothing, because one is the other. Counting them as two overstates what the
check found, in exactly the surface that exists to say how sure we are.

This module is the single declaration of that lineage. Adapters carry a
``derived_from`` class attribute naming the sources they republish; the
handful of source ids that appear in BODS bundles without being adapters
(the bulk-dataset paths) are declared here. Everything else is independent
by default, so an adapter that says nothing under-claims its dependence
rather than over-claiming corroboration.

Two operations matter downstream:

* :func:`independent` — are two sources independent of each other?
* :func:`independent_count` — how many *independent* origins does a set of
  sources represent? A source is discounted when one of its upstreams is
  also present, and two derivatives of the same absent upstream (OpenCorporates
  and OpenAleph both mirroring Companies House) count once between them.

Lineage is about *records*, not *findings*. An OpenSanctions sanctions
listing is original data even though OpenSanctions' entity record for the
company may be a GLEIF mirror; this module only informs whether two records
about an entity are one observation or two, and is never consulted when
deciding whether a risk signal fires.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Iterable, Mapping

#: Marker an adapter may put in ``derived_from`` to say "every national
#: register OpenCheck knows about" — expanded against ``SourceInfo.
#: is_national_register`` at query time, so a new register adapter is
#: covered without touching the aggregator that mirrors it.
NATIONAL_REGISTERS = "national-registers"

#: Source ids that appear as BODS ``source.description`` publishers without
#: being adapters in ``REGISTRY`` (bulk-dataset and derived paths), and what
#: they republish. Keyed the same way as adapter ids so one table serves the
#: backend, the ``/sources`` endpoint and the generated frontend copy.
EXTRA_DERIVED: Mapping[str, frozenset[str]] = {
    # GLEIF Level 1/2 concatenated files, mapped to BODS in bulk.
    "bods_gleif": frozenset({"gleif"}),
    # Companies House PSC bulk snapshot.
    "bods_uk_psc": frozenset({"companies_house"}),
}


def _registry():
    # Imported lazily: ``sources/__init__`` imports every adapter, and adapters
    # import this module for the ``NATIONAL_REGISTERS`` marker.
    from . import REGISTRY

    return REGISTRY


@lru_cache(maxsize=1)
def national_register_ids() -> frozenset[str]:
    """Adapter ids whose ``SourceInfo.is_national_register`` is true."""
    out: set[str] = set()
    for source_id, adapter in _registry().items():
        try:
            if adapter.info.is_national_register:
                out.add(source_id)
        except Exception:  # pragma: no cover — an adapter whose info needs config
            continue
    return frozenset(out)


@lru_cache(maxsize=1)
def lineage_table() -> dict[str, frozenset[str]]:
    """``source_id → direct upstreams`` for every source that has any.

    Adapters' ``derived_from`` (with :data:`NATIONAL_REGISTERS` expanded) plus
    :data:`EXTRA_DERIVED`. Sources with no upstream are absent from the table.
    """
    table: dict[str, frozenset[str]] = {}
    registers = national_register_ids()
    for source_id, adapter in _registry().items():
        declared = frozenset(getattr(adapter, "derived_from", frozenset()))
        if not declared:
            continue
        upstream: set[str] = set()
        for item in declared:
            if item == NATIONAL_REGISTERS:
                upstream |= registers
            else:
                upstream.add(item)
        upstream.discard(source_id)
        if upstream:
            table[source_id] = frozenset(upstream)
    for source_id, upstream in EXTRA_DERIVED.items():
        table[source_id] = frozenset(upstream) | table.get(source_id, frozenset())
    return table


def derived_from(source_id: str) -> frozenset[str]:
    """Direct upstreams of ``source_id`` (empty when it is original)."""
    return lineage_table().get(source_id, frozenset())


def ancestors(source_id: str) -> frozenset[str]:
    """Every source ``source_id`` transitively republishes.

    Walks the table with a visited set, so a declaration cycle (which the
    tests forbid, but a future edit could introduce) terminates rather than
    recursing forever.
    """
    table = lineage_table()
    seen: set[str] = set()
    frontier = list(table.get(source_id, ()))
    while frontier:
        current = frontier.pop()
        if current in seen or current == source_id:
            continue
        seen.add(current)
        frontier.extend(table.get(current, ()))
    return frozenset(seen)


def independent(a: str, b: str) -> bool:
    """True when neither source republishes the other.

    The same id twice is not independent of itself. Two derivatives of a
    shared upstream (OpenCorporates and OpenAleph, both mirroring Companies
    House) are *not* independent either: an agreement between them may be one
    register read twice. That is the conservative reading and the one
    corroboration copy should rest on.
    """
    if a == b:
        return False
    if a in ancestors(b) or b in ancestors(a):
        return False
    return not (ancestors(a) & ancestors(b))


def independent_sources(source_ids: Iterable[str]) -> list[str]:
    """Collapse a set of sources to its independent origins.

    A source whose upstream is present is dropped in favour of the upstream.
    Of the survivors, any two that share an ancestor are merged and the first
    (in sorted order, for determinism) represents the pair. The result is
    the list of representatives — ``len()`` of it is the corroboration count.

    >>> independent_sources(["opencorporates", "companies_house", "gleif"])
    ['companies_house', 'gleif']
    """
    ids = sorted({s for s in source_ids if s})
    present = set(ids)
    survivors = [s for s in ids if not (ancestors(s) & present)]
    # Union-find over shared ancestry among the survivors.
    parent = {s: s for s in survivors}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i, a in enumerate(survivors):
        for b in survivors[i + 1 :]:
            if ancestors(a) & ancestors(b):
                ra, rb = find(a), find(b)
                if ra != rb:
                    # Keep the lexically smaller root so output is stable.
                    parent[max(ra, rb)] = min(ra, rb)
    return sorted({find(s) for s in survivors})


def independent_count(source_ids: Iterable[str]) -> int:
    """How many independent origins ``source_ids`` represent."""
    return len(independent_sources(source_ids))


def export_table(descriptions: Mapping[str, str] | None = None) -> dict:
    """The lineage as plain JSON for the frontend (see ``scripts/gen_lineage.py``).

    ``descriptions`` maps source id → the ``source.description`` string the
    BODS mapper stamps on statements, because the FullCheck network only
    holds those labels, not adapter ids. Sorted throughout so the committed
    file is byte-stable across runs.
    """
    table = lineage_table()
    all_ids = sorted(set(_registry()) | set(EXTRA_DERIVED) | set(table))
    return {
        "derived_from": {k: sorted(v) for k, v in sorted(table.items())},
        "ancestors": {
            k: sorted(ancestors(k)) for k in all_ids if ancestors(k)
        },
        "descriptions": {
            k: v for k, v in sorted((descriptions or {}).items()) if k in all_ids
        },
    }


def export_json(descriptions: Mapping[str, str] | None = None) -> str:
    return json.dumps(export_table(descriptions), indent=2, ensure_ascii=False) + "\n"
