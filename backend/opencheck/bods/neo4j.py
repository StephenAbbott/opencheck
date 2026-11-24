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

**One edge per relationship statement** (the Relate review, 24 Nov 2026).
Until then edges were ``MERGE``d per (owner, owned) pair, so two sources'
relationships for one pair collapsed into one edge and the last ``SET`` won —
a cross-source merge OpenCheck makes nowhere else — and an edge carried no
source, no lifecycle and no share. An agent querying the graph read it as
fact. Now each relationship statement is its own edge, ``MERGE``d on its
``statementId``, carrying what a query needs to follow OpenCheck's rules
(``EDGE_PROPERTIES``): the source and its licence, the category the canvas
draws, the interest types, the share of the holding interest, the BODS
beneficial-ownership flag, an officer position, whether it has ended and
when, whether no date was given, and ``notOwnership`` for the sources whose
edges are not ownership (GLEIF Level 2 consolidation, MEIP group membership —
``integrated_ownership.EXCLUDED_SOURCES``). Nodes carry their source and
licence too. "Current ownership, one source at a time" is then a ``WHERE``
clause. Edge ``MATCH``es are labelled, so each one is an index seek.

This stays a projection — one node per entity/person **statement**, no
addresses or identifiers beyond the LEI. The full-fidelity, bidirectional
path is the external ``bods-neo4j`` tool (``bods-neo4j to-cypher`` writes an
APOC-free script of it). The query rules for an agent are in
``skills/opencheck-due-diligence/references/graph-queries.md``.
Pure, side-effect-free.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .lifecycle import _iso_day, statement_lifecycle, today_iso
from .officer_roles import officer_position
from .refs import resolver
from .source_ids import source_id_of
from .undated import is_undated

#: Sources whose relationships are not ownership: GLEIF Level 2 is accounting
#: consolidation, MEIP is statistical group membership. The same set
#: integrated ownership leaves out (``integrated_ownership.EXCLUDED_SOURCES``,
#: pinned by a test).
NOT_OWNERSHIP_BASIS: dict[str, str] = {
    "gleif": "accounting consolidation (GLEIF Level 2), not ownership",
    "bods_gleif": "accounting consolidation (GLEIF Level 2), not ownership",
    "meip": "group membership (OECD-UNSD MEIP), not ownership",
}

#: Every property an ``OWNS_OR_CONTROLS`` edge can carry, with its meaning.
#: The data dictionary and the graph-queries skill reference describe them.
EDGE_PROPERTIES: dict[str, str] = {
    "statementId": "The relationship statement's id. One edge per statement.",
    "recordId": "The relationship record's id (stable across versions).",
    "sourceId": "The OpenCheck source that published the statement.",
    "source": "The source's display name.",
    "license": "The source's licence (URL, else identifier).",
    "category": "ownership | control | role | unknown, as the canvas draws it.",
    "interest": "The BODS interest types, joined with '; ' (kept for older queries).",
    "interests": "The BODS interest types, as a list.",
    "shareInterest": "The interest type the share figures below belong to.",
    "shareExact": "Its exact percentage, where the source gives one.",
    "shareMin": "Its lower bound (inclusive), where the source gives a band.",
    "shareMax": "Its upper bound (inclusive).",
    "shareExclusiveMin": "Its lower bound (exclusive).",
    "shareExclusiveMax": "Its upper bound (exclusive).",
    "beneficialOwnershipOrControl": "True when any interest is flagged as beneficial "
                                     "ownership or control by the source.",
    "directOrIndirect": "direct | indirect | unknown, when every interest says the same.",
    "officerPosition": "An officer appointment's position (secretary, agent…): a role, "
                       "never control.",
    "startDate": "The earliest interest start date.",
    "ended": "True when the record is closed or every interest has ended.",
    "endDate": "The latest interest end date, on an ended relationship.",
    "endedAsOf": "The day 'ended' was judged on.",
    "undated": "True for a current ownership or control relationship the source "
               "gives no date for.",
    "notOwnership": "True for GLEIF consolidation and MEIP group membership edges.",
    "basis": "What a notOwnership edge records instead of ownership.",
}


def _esc(value: str | None) -> str:
    """Escape a value for a single-quoted Cypher string literal."""
    return (
        (value or "")
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _lit(value: Any) -> str:
    """A Cypher literal for a string, number, boolean or list of strings."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(float(value)) if isinstance(value, float) else str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_lit(v) for v in value) + "]"
    return f"'{_esc(str(value))}'"


def _sets(var: str, props: dict[str, Any]) -> str:
    return ", ".join(f"{var}.{k} = {_lit(v)}" for k, v in props.items() if v is not None)


def _entity_lei(rd: dict[str, Any]) -> str | None:
    for ident in rd.get("identifiers") or []:
        val = (ident.get("id") or "").strip()
        scheme = f"{ident.get('scheme', '')} {ident.get('schemeName', '')}".upper()
        if val and "LEI" in scheme:
            return val
    return None


def _source_props(stmt: Mapping[str, Any]) -> dict[str, Any]:
    """``sourceId``, ``source`` and ``license`` of a statement."""
    from ..licensing import most_restrictive

    src = stmt.get("source") if isinstance(stmt.get("source"), Mapping) else {}
    sid = source_id_of(stmt)
    lic = most_restrictive([sid]) if sid else None
    return {
        "sourceId": sid,
        "source": (src or {}).get("description") or None,
        "license": (lic.terms.url or lic.terms.license) if lic else None,
    }


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


#: Share keys, BODS → edge property.
_SHARE_KEYS = (
    ("exact", "shareExact"),
    ("minimum", "shareMin"),
    ("maximum", "shareMax"),
    ("exclusiveMinimum", "shareExclusiveMin"),
    ("exclusiveMaximum", "shareExclusiveMax"),
)


def _share_props(interests: list[dict[str, Any]]) -> dict[str, Any]:
    """The share of one interest — a shareholding first, else the first with
    a share — never a sum or a mix of two interests' figures."""
    def has_share(i: dict[str, Any]) -> bool:
        share = i.get("share")
        return isinstance(share, dict) and any(
            isinstance(share.get(k), (int, float)) and not isinstance(share.get(k), bool)
            for k, _ in _SHARE_KEYS
        )

    with_share = [i for i in interests if has_share(i)]
    if not with_share:
        return {}
    chosen = next((i for i in with_share if i.get("type") == "shareholding"), with_share[0])
    share = chosen["share"]
    out: dict[str, Any] = {"shareInterest": chosen.get("type") or None}
    for key, prop in _SHARE_KEYS:
        value = share.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out[prop] = float(value)
    return out


def edge_properties(stmt: dict[str, Any], as_of: str | None = None) -> dict[str, Any]:
    """The properties of one relationship statement's edge (``EDGE_PROPERTIES``)."""
    from ..reporting.diagram import categorise

    as_of = as_of or today_iso()
    rd = stmt.get("recordDetails") or {}
    interests = [i for i in rd.get("interests") or [] if isinstance(i, dict)]
    types = [i["type"] for i in interests if isinstance(i.get("type"), str) and i["type"]]
    props: dict[str, Any] = {
        "statementId": stmt.get("statementId"),
        "recordId": stmt.get("recordId"),
        **_source_props(stmt),
        "category": categorise(interests),
        "interest": "; ".join(types) or "interest",
        "interests": types,
        **_share_props(interests),
    }
    if any(i.get("beneficialOwnershipOrControl") is True for i in interests):
        props["beneficialOwnershipOrControl"] = True
    elif interests and all(i.get("beneficialOwnershipOrControl") is False for i in interests):
        props["beneficialOwnershipOrControl"] = False
    directions = {i.get("directOrIndirect") for i in interests if i.get("directOrIndirect")}
    if len(directions) == 1:
        props["directOrIndirect"] = directions.pop()
    position = next((officer_position(i) for i in interests if officer_position(i)), None)
    if position:
        props["officerPosition"] = position
    starts = sorted(d for d in (_iso_day(i.get("startDate")) for i in interests) if d)
    if starts:
        props["startDate"] = starts[0]
    life = statement_lifecycle(stmt, as_of)
    props["ended"] = life.ended
    if life.ended_on:
        props["endDate"] = life.ended_on
    props["endedAsOf"] = as_of
    props["undated"] = is_undated(stmt)
    basis = NOT_OWNERSHIP_BASIS.get(props.get("sourceId") or "")
    if basis:
        props["notOwnership"] = True
        props["basis"] = basis
    return props


def to_cypher(bods: list[dict[str, Any]], as_of: str | None = None) -> str:
    """Render a BODS bundle as a Cypher script (nodes then edges).

    ``as_of`` is the day an edge's ``ended`` is judged on — a saved report's
    run date, else today.
    """
    lines: list[str] = [
        "// OpenCheck network — Neo4j / Cypher projection. MERGE = idempotent.",
        "// Core Cypher only: no APOC, no LOAD CSV. The three CREATE INDEX lines",
        "// are Neo4j 5 syntax; on another engine drop them (they only speed up",
        "// the MATCHes).",
        "// One OWNS_OR_CONTROLS edge per BODS relationship statement, never merged",
        "// across sources. Before you query, read the rules:",
        "//   * r.notOwnership: GLEIF consolidation / MEIP membership, not ownership.",
        "//   * WHERE NOT r.ended for current relationships (judged on r.endedAsOf).",
        "//   * Never sum() shares across r.sourceId; compare sources side by side.",
        "//   * r.category = 'role' (officers) is never ownership or control.",
        "//   * :UnspecifiedParty = a party the source did not disclose (BODS",
        "//     unspecified record); exclude it with WHERE NOT n:UnspecifiedParty.",
        "//   * Two nodes with one name are two records, not one party.",
        "",
        "CREATE INDEX opencheck_entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id);",
        "CREATE INDEX opencheck_person_id IF NOT EXISTS FOR (n:Person) ON (n.id);",
        "CREATE INDEX opencheck_unspecified_id IF NOT EXISTS FOR (n:UnspecifiedParty) ON (n.id);",
        "",
    ]

    labels: dict[str, str] = {}
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
        labels[sid] = label

        props: dict[str, Any] = {"name": name, "recordId": s.get("recordId")}
        if rt == "entity":
            props["lei"] = _entity_lei(rd)
        props["jurisdiction"] = (rd.get("jurisdiction") or {}).get("code")
        props.update(_source_props(s))
        lines.append(f"MERGE (n:{label} {{id: '{_esc(sid)}'}}) SET " + _sets("n", props) + ";")

    lines.append("")
    resolve = resolver(bods or [])
    for s in bods or []:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        # A reference resolves to a node: nodes are MERGEd on statementId, and
        # a v0.4 reference is a recordId, so resolve it. An unspecified party
        # object becomes a placeholder node (Phase 274) instead of dropping the
        # edge. Anything else (absent, malformed, a party not in the bundle)
        # still has nothing to draw.
        rel_key = s.get("statementId") or s.get("recordId")
        ends: dict[str, tuple[str, str]] = {}
        placeholders: list[str] = []
        for side in _PARTY_SIDES:
            ref = rd.get(side)
            if isinstance(ref, str) and ref:
                node = resolve(ref)
                if node in labels:
                    ends[side] = (labels[node], node)
            elif isinstance(ref, dict) and rel_key:
                node, line = _unspecified_node(str(rel_key), side, ref)
                ends[side] = ("UnspecifiedParty", node)
                placeholders.append(line)
        if len(ends) != len(_PARTY_SIDES) or not rel_key:
            continue
        lines.extend(placeholders)
        s_label, subject = ends["subject"]
        p_label, party = ends["interestedParty"]
        props = edge_properties(s, as_of)
        props.pop("statementId", None)
        lines.append(
            f"MATCH (a:{p_label} {{id: '{_esc(party)}'}}) "
            f"MATCH (b:{s_label} {{id: '{_esc(subject)}'}}) "
            f"MERGE (a)-[r:OWNS_OR_CONTROLS {{statementId: '{_esc(str(rel_key))}'}}]->(b) "
            "SET " + _sets("r", props) + ";"
        )

    return "\n".join(lines) + "\n"
