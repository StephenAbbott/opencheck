"""How big a lookup's ownership-and-control graph is — the ``graph_shape`` event.

Split out of ``routers/lookup.py`` in Phase 246, unchanged: pure functions over
the merged BODS bundle, with no fetch and no clock. ``_count_parties``
collapses statements that share a published identifier, so one company
described by three sources counts once; ``_graph_shape`` adds the relationship
count and the longest chain the risk layer measured. ``routers/lookup.py``
re-exports all three names.
"""

from __future__ import annotations

from typing import Any


def _identifier_keys(statement: dict[str, Any]) -> list[str]:
    """The published identifiers on a statement, normalised for comparison."""
    details = statement.get("recordDetails") or {}
    keys: list[str] = []
    for ident in details.get("identifiers") or []:
        if not isinstance(ident, dict):
            continue
        scheme = ident.get("scheme") or ident.get("schemeName") or ""
        value = ident.get("id")
        if isinstance(value, str) and value.strip():
            keys.append(f"{scheme}:{value.strip()}".lower())
    return keys


def _count_parties(statements: list[dict[str, Any]]) -> int:
    """How many distinct parties a list of same-kind statements describes.

    Every mapper derives its ids as ``_stable_id(source_id, kind, local_id)``,
    so GLEIF's copy of a company and Companies House's copy have **different**
    ``statementId``s by construction. Counting statements and calling the total
    "companies" therefore overstated every graph where two sources describe the
    subject — which is nearly all of them.

    Records are joined into one party when they share a published identifier
    (``recordDetails.identifiers[]``), which is the evidence ``reconcile.py``
    already requires before it will assert cross-source corroboration. It is a
    transitive join, not a "pick one key" rule: GLEIF may publish only the LEI
    while Companies House publishes a company number *and* the LEI, so the two
    records agree on one identifier out of three and must still be one party.

    Names are deliberately **not** used. A name match is not an identity claim
    anywhere else in OpenCheck — ``possibly_same_entities`` exists precisely to
    hold name-only pairs out of the graph and hand them to a human — and it
    must not become one here just because it would make a number smaller.

    The consequence is that the figure is an **upper bound**: two records of
    one company sharing no identifier stay two. That is the safe direction for
    an invitation into FullCheck, whose job is to go and resolve exactly those.
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    roots: list[str] = []
    for index, statement in enumerate(statements):
        sid = statement.get("statementId")
        node = f"stmt:{sid}" if isinstance(sid, str) else f"stmt:#{index}"
        find(node)
        roots.append(node)
        for key in _identifier_keys(statement):
            union(node, f"id:{key}")

    return len({find(node) for node in roots})


def _graph_shape(
    bods: list[dict[str, Any]], signals: list[dict[str, Any]]
) -> dict[str, int | None]:
    """How big the ownership-and-control graph on this page actually is.

    The report's third verdict column invites the reader into FullCheck, and an
    invitation with no numbers on it is a button. These are the numbers the
    check has *already earned*: the parties in the merged BODS bundle — the
    same bundle ``/export`` ships and the risk engine assessed — collapsed by
    ``_count_parties`` so one company described by three sources counts once.

    It deliberately does **not** reach for the GLEIF subsidiary total or
    anything FullCheck would go on to discover. Those are a different scope,
    and a sentence that mixes "what we have" with "what we might find" is the
    same overclaim as a progress bar that runs ahead of its stream.

    ``depth`` is the longest ownership chain the risk layer actually measured
    (``COMPLEX_OWNERSHIP_LAYERS`` carries it as ``evidence.longest_path``), or
    ``None`` when the signal did not fire — never a guess, and never 0, which
    would render as a flat graph.
    """
    entities = [s for s in bods if s.get("recordType") == "entity"]
    persons = [s for s in bods if s.get("recordType") == "person"]
    relationships = {
        s.get("statementId")
        for s in bods
        if s.get("recordType") == "relationship" and isinstance(s.get("statementId"), str)
    }

    depth: int | None = None
    for signal in signals:
        if signal.get("code") != "COMPLEX_OWNERSHIP_LAYERS":
            continue
        path = (signal.get("evidence") or {}).get("longest_path")
        if isinstance(path, list) and path:
            depth = max(depth or 0, len(path))
    return {
        "companies": _count_parties(entities),
        "people": _count_parties(persons),
        "relationships": len(relationships),
        "depth": depth,
    }
