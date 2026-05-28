"""Shared BODS validation helpers for use across the test suite.

Used by:
  - tests/test_bods_graph_integrity.py  (cross-adapter connectivity matrix)
  - tests/test_bods_multi_source.py     (multi-source assembly audit)
  - tests/test_bods_inpi.py             (per-mapper connectivity check)
  - tests/test_bods_firmenbuch.py
  - tests/test_bods_gleif_ftm.py
  - tests/test_bods_ch_directors.py
  - tests/test_bods_mapper.py

Public functions:

check_graph_connectivity(stmts)
    Returns a list of human-readable issue strings for every relationship
    statement whose ``subject`` or ``interestedParty`` does not reference a
    ``statementId`` / ``recordId`` present in the same bundle.  An empty list
    means the bundle is fully connected — no dangling references.

check_interest_types(stmts)
    Returns a list of issue strings for every interest ``type`` that is not a
    member of the BODS v0.4 codelist.

check_unreferenced_entities(stmts)
    Returns a list of (statementId, name) tuples for every entity statement
    that is not referenced as ``subject`` or ``interestedParty`` in any
    relationship statement in the same bundle.  Used in multi-source assembly
    tests to detect orphaned entity nodes that will appear floating in
    bods-dagre.

check_duplicate_entity_names(stmts)
    Returns a list of (name, [statementId, ...]) tuples for every entity
    legal name that appears in more than one entity statement.  In a
    well-assembled multi-source bundle a company should appear as exactly one
    entity node; duplicates indicate that two sources both produced entity
    statements for the same company without ID normalisation.

connected_components(stmts)
    Returns a list of frozensets, each containing the statementIds of one
    connected component in the BODS graph.  A fully connected bundle has
    exactly one component (possibly plus isolated nodes that are genuinely
    standalone).  Multiple components indicate disconnected subgraphs — which
    means bods-dagre will render separate clusters with no edges between them.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# BODS v0.4 valid interest type codelist
#
# Single source of truth: imported from the production validator so the test
# helpers and the validator never drift apart.  The validator's set is derived
# from the official BODS CSV codelist published by Open Ownership.
# ---------------------------------------------------------------------------

from opencheck.bods.validator import _VALID_INTEREST_TYPES as VALID_INTEREST_TYPES  # noqa: E402


def to_stmts(result: Any) -> list[dict]:
    """Accept a BODSBundle (.statements) or a plain iterable of statements."""
    if hasattr(result, "statements"):
        return result.statements
    return list(result)


def check_graph_connectivity(stmts: list[dict]) -> list[str]:
    """Return one issue string per dangling or malformed reference.

    A dangling reference occurs when a relationship statement's ``subject`` or
    ``interestedParty`` value is a string that does not match any
    ``statementId`` or ``recordId`` present in the bundle.

    A malformed reference occurs when the value is still in v0.3 object format
    (``{"describedByEntityStatement": "..."}``), which bods-dagre will not
    connect to a node correctly.

    An empty list means the bundle is fully connected.
    """
    all_ids: set[str] = set()
    for s in stmts:
        sid = s.get("statementId") or ""
        rid = s.get("recordId") or ""
        if sid:
            all_ids.add(sid)
        if rid:
            all_ids.add(rid)

    issues: list[str] = []
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        rel_id = s.get("statementId", "<unknown>")

        for field, val in [
            ("subject", rd.get("subject")),
            ("interestedParty", rd.get("interestedParty")),
        ]:
            if val is None:
                issues.append(f"{rel_id}: MISSING {field}")
            elif isinstance(val, str):
                if val not in all_ids:
                    issues.append(
                        f"{rel_id}: DANGLING {field} '{val}' — not in bundle"
                    )
            elif isinstance(val, dict):
                ref = val.get("describedByEntityStatement") or val.get(
                    "describedByPersonStatement"
                )
                issues.append(
                    f"{rel_id}: v0.3 object-format {field} (ref={ref!r})"
                    " — must be bare string in BODS v0.4"
                )
                if ref and ref not in all_ids:
                    issues.append(
                        f"  └─ {rel_id}: DANGLING {field} ref '{ref}' — not in bundle"
                    )

    return issues


def check_interest_types(stmts: list[dict]) -> list[str]:
    """Return one issue string per interest type that is not a v0.4 codelist member."""
    issues: list[str] = []
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rel_id = s.get("statementId", "<unknown>")
        for interest in (s.get("recordDetails") or {}).get("interests") or []:
            t = interest.get("type")
            if t and t not in VALID_INTEREST_TYPES:
                issues.append(
                    f"{rel_id}: invalid interest type '{t}' "
                    f"(not in BODS v0.4 codelist)"
                )
    return issues


# ---------------------------------------------------------------------------
# Multi-source assembly helpers
# ---------------------------------------------------------------------------


def check_unreferenced_entities(stmts: list[dict]) -> list[tuple[str, str]]:
    """Return (statementId, name) for every entity not referenced by any relationship.

    An entity statement is "unreferenced" when its ``statementId`` does not
    appear as the ``subject`` or ``interestedParty`` of any relationship
    statement in the bundle.

    In a single-source bundle this is normal (a company with no disclosed
    owners is genuinely standalone).  In a *multi-source* bundle it reveals
    orphaned entity nodes — most commonly caused by two sources each producing
    their own entity statement for the same company with different IDs, where
    only one of the duplicates gets connected to a relationship.
    """
    referenced: set[str] = set()
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        for field in ("subject", "interestedParty"):
            val = rd.get(field)
            if isinstance(val, str):
                referenced.add(val)

    result: list[tuple[str, str]] = []
    for s in stmts:
        if s.get("recordType") != "entity":
            continue
        sid = s.get("statementId", "")
        if sid and sid not in referenced:
            name = (s.get("recordDetails") or {}).get("name", "")
            result.append((sid, name))
    return result


def check_duplicate_entity_names(stmts: list[dict]) -> list[tuple[str, list[str]]]:
    """Return (name, [statementId, ...]) for entity names appearing more than once.

    In a correctly assembled multi-source bundle a legal entity should appear
    as exactly one entity statement.  Duplicate names indicate that two
    adapters both emitted entity statements for the same company without
    cross-source ID normalisation.  This is the direct cause of duplicate
    nodes in bods-dagre.
    """
    from collections import defaultdict

    name_to_ids: dict[str, list[str]] = defaultdict(list)
    for s in stmts:
        if s.get("recordType") != "entity":
            continue
        name = (s.get("recordDetails") or {}).get("name", "").strip()
        sid = s.get("statementId", "")
        if name and sid:
            name_to_ids[name].append(sid)

    return [(name, ids) for name, ids in sorted(name_to_ids.items()) if len(ids) > 1]


def connected_components(stmts: list[dict]) -> list[frozenset[str]]:
    """Return one frozenset per connected component in the BODS graph.

    Nodes are ``statementId`` values.  An edge exists between two nodes when
    a relationship statement references one as ``subject`` and the other as
    ``interestedParty``.  Entity and person nodes that are not mentioned by
    any relationship are returned as singleton components.

    A single-component result means bods-dagre will render one connected
    graph.  Multiple components mean disconnected subgraphs — which is the
    signature of the cross-source entity ID mismatch problem.
    """
    # Build adjacency list
    adj: dict[str, set[str]] = {}
    for s in stmts:
        sid = s.get("statementId")
        if sid:
            adj.setdefault(sid, set())

    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        subj = rd.get("subject")
        ip = rd.get("interestedParty")
        rel_id = s.get("statementId")
        if rel_id:
            adj.setdefault(rel_id, set())
        for node in (subj, ip):
            if isinstance(node, str) and node:
                adj.setdefault(node, set())
                if rel_id:
                    adj[node].add(rel_id)
                    adj[rel_id].add(node)

    # Union-Find
    parent: dict[str, str] = {n: n for n in adj}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        parent[find(x)] = find(y)

    for node, neighbours in adj.items():
        for nb in neighbours:
            union(node, nb)

    groups: dict[str, list[str]] = {}
    for node in adj:
        root = find(node)
        groups.setdefault(root, []).append(node)

    return [frozenset(g) for g in groups.values()]
