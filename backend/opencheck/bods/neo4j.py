"""BODS v0.4 → Cypher (Neo4j) — a lightweight projection for quick exploration.

Emits idempotent ``MERGE`` statements you can paste straight into the Neo4j
Browser (or run with ``cypher-shell``) to build the ownership network: one node
per entity/person and an ``OWNS_OR_CONTROLS`` edge per disclosed relationship
(owner → owned, matching BODS).

This is a **convenience projection** — entity-centric, lossy on provenance — for
eyeballing a FullCheck network. The full-fidelity, bidirectional path is the
external `bods-neo4j` tool, which consumes the same BODS this export produces.
Pure, side-effect-free.
"""

from __future__ import annotations

from typing import Any


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


def to_cypher(bods: list[dict[str, Any]]) -> str:
    """Render a BODS bundle as a Cypher script (nodes then edges)."""
    lines: list[str] = [
        "// OpenCheck FullCheck network — Neo4j / Cypher projection.",
        "// Paste into Neo4j Browser or run with cypher-shell. MERGE = idempotent.",
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
    for s in bods or []:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        subject = rd.get("subject")
        party = rd.get("interestedParty")
        # Only a plain statementId reference resolves to a node; an "unspecified"
        # (unknown) party object cannot be linked.
        if not isinstance(subject, str) or not isinstance(party, str):
            continue
        interests = rd.get("interests") or []
        kinds = "; ".join(i.get("type", "") for i in interests if i.get("type")) or "interest"
        lines.append(
            f"MATCH (a {{id: '{_esc(party)}'}}), (b {{id: '{_esc(subject)}'}}) "
            f"MERGE (a)-[r:OWNS_OR_CONTROLS]->(b) SET r.interest = '{_esc(kinds)}';"
        )

    return "\n".join(lines) + "\n"
