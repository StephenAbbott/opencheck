"""BODS v0.4 → Cypher (Neo4j) — a lightweight projection for quick exploration.

Emits idempotent ``MERGE`` statements you can paste straight into the Neo4j
Browser (or run with ``cypher-shell``) to build the ownership network: one node
per entity/person and an ``OWNS_OR_CONTROLS`` edge per disclosed relationship
(owner → owned, matching BODS).

A party BODS cannot name — an ``UnspecifiedRecord`` (``{reason, description}``)
in a relationship's ``subject`` or ``interestedParty`` — becomes a placeholder
``:UnspecifiedParty`` node, one per relationship statement and side, so a chain
that ends in an undisclosed owner still ends visibly (Phase 274; until then the
whole edge was dropped, and Cypher and RDF disagreed about the same bundle).
The label is distinct from ``:Entity`` / ``:Person`` so a traversal over real
parties can exclude it, and "which chains end in an unspecified party?" is one
``MATCH``.

This is a **convenience projection** — entity-centric, lossy on provenance — for
eyeballing a FullCheck network. The full-fidelity, bidirectional path is the
external `bods-neo4j` tool, which consumes the same BODS this export produces.
Pure, side-effect-free.
"""

from __future__ import annotations

from typing import Any

from .refs import resolver


def _esc(value: str | None) -> str:
    """Escape a value for a single-quoted Cypher string literal."""
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _entity_lei(rd: dict[str, Any]) -> str | None:
    for ident in rd.get("identifiers") or []:
        val = (ident.get("id") or "").strip()
        scheme = f"{ident.get('scheme', '')} {ident.get('schemeName', '')}".upper()
        if val and "LEI" in scheme:
            return val
    return None


#: The two relationship fields that name a party.
_PARTY_SIDES: tuple[str, ...] = ("subject", "interestedParty")


def _unspecified_node(rel_key: str, side: str, party: dict[str, Any]) -> tuple[str, str]:
    """A placeholder node for an unspecified party: ``(node id, MERGE line)``.

    Keyed on the relationship statement and the side, never on the reason — a
    node shared per reason would join every chain that ends in, say,
    ``interestedPartyHasNotProvidedInformation`` through one hub, inventing
    paths between unrelated companies.
    """
    node_id = f"{rel_key}#{side}"
    sets = [f"u.side = '{_esc(side)}'", f"u.relationship = '{_esc(rel_key)}'"]
    reason = party.get("reason")
    if isinstance(reason, str) and reason:
        sets.append(f"u.reason = '{_esc(reason)}'")
    description = party.get("description")
    if isinstance(description, str) and description:
        sets.append(f"u.description = '{_esc(description)}'")
    line = f"MERGE (u:UnspecifiedParty {{id: '{_esc(node_id)}'}}) SET " + ", ".join(sets) + ";"
    return node_id, line


def to_cypher(bods: list[dict[str, Any]]) -> str:
    """Render a BODS bundle as a Cypher script (nodes then edges)."""
    lines: list[str] = [
        "// OpenCheck FullCheck network — Neo4j / Cypher projection.",
        "// Paste into Neo4j Browser or run with cypher-shell. MERGE = idempotent.",
        "// :UnspecifiedParty = a party the source did not disclose (BODS unspecified",
        "// record); exclude it with WHERE NOT n:UnspecifiedParty.",
        "",
    ]

    for s in bods or []:
        rt = s.get("recordType")
        sid = s.get("statementId")
        if rt not in ("entity", "person") or not sid:
            continue
        rd = s.get("recordDetails") or {}
        if rt == "person":
            label = "Person"
            names = rd.get("names") or []
            name = (names[0].get("fullName") if names else "") or ""
        else:
            label = "Entity"
            name = rd.get("name") or ""

        sets = [f"n.name = '{_esc(name)}'"]
        lei = _entity_lei(rd) if rt == "entity" else None
        if lei:
            sets.append(f"n.lei = '{_esc(lei)}'")
        jur = (rd.get("jurisdiction") or {}).get("code")
        if jur:
            sets.append(f"n.jurisdiction = '{_esc(jur)}'")
        lines.append(
            f"MERGE (n:{label} {{id: '{_esc(sid)}'}}) SET " + ", ".join(sets) + ";"
        )

    lines.append("")
    resolve = resolver(bods or [])
    for s in bods or []:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        # A reference resolves to a node: nodes are MERGEd on statementId, and
        # a v0.4 reference is a recordId, so resolve it. An unspecified party
        # object becomes a placeholder node (Phase 274) instead of dropping the
        # edge. Anything else (absent, malformed) still has nothing to draw.
        rel_key = s.get("statementId") or s.get("recordId")
        ends: dict[str, str] = {}
        placeholders: list[str] = []
        for side in _PARTY_SIDES:
            ref = rd.get(side)
            if isinstance(ref, str) and ref:
                ends[side] = resolve(ref)
            elif isinstance(ref, dict) and rel_key:
                ends[side], line = _unspecified_node(str(rel_key), side, ref)
                placeholders.append(line)
        if len(ends) != len(_PARTY_SIDES):
            continue
        lines.extend(placeholders)
        subject = ends["subject"]
        party = ends["interestedParty"]
        interests = rd.get("interests") or []
        kinds = "; ".join(i.get("type", "") for i in interests if i.get("type")) or "interest"
        lines.append(
            f"MATCH (a {{id: '{_esc(party)}'}}), (b {{id: '{_esc(subject)}'}}) "
            f"MERGE (a)-[r:OWNS_OR_CONTROLS]->(b) SET r.interest = '{_esc(kinds)}';"
        )

    return "\n".join(lines) + "\n"
