"""Resolving a relationship's parties to the statements they name (Phase 210).

BODS v0.4 says a relationship's ``subject`` and ``interestedParty`` hold the
**recordId** of the party's entity or person statement. Every OpenCheck
mapper set ``statementId == recordId`` for entities and people (the record-ID
ticket's phase B ends that), so for ten months nothing distinguished the two
and every consumer — the graph, the risk layer walk, the reconciler, the PDF
diagram, the Senzing / Neo4j / FtM exports — keyed its lookups on
``statementId`` and happened to work. ``tests/test_record_id_references.py``
now scrambles the two apart and holds every consumer to resolving.

Phase 208 brought the first statements OpenCheck did not write: the OECD's
MEIP release, where ``statementId`` is a hash and ``recordId`` is
``meip-entity-N`` and the relationships reference the latter, as the standard
says. The edge then resolved to nothing and A/S Norske Shell and SHELL PLC
drew as two unlinked nodes.

This module is the one place that knows both spellings. ``party_ref`` reads a
reference out of either the v0.4 bare string or the legacy wrapped object;
``resolver`` returns a function mapping any reference — recordId or
statementId — to the **statementId** of the statement it names, which is
what every index in the codebase is keyed on. An unknown reference comes
back unchanged so a dangling edge still reads as dangling.

``frontend/src/lib/bodsRefs.ts`` is the mirror; ``tests/test_frontend_parity.py``
(Phase 266) parses it and fails when its party types, legacy wrapper keys, id
fallbacks or tier order differ from this module's.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

_PARTY_TYPES = frozenset({"entity", "person", "entityStatement", "personStatement"})

_WRAPPED_KEYS = (
    "describedByEntityStatement",
    "describedByPersonStatement",
    "describedByAnonymousEntityStatement",
)


def party_ref(raw: Any) -> str | None:
    """The reference a ``subject`` / ``interestedParty`` value carries, or
    ``None`` for an unspecified record (``{reason: …}``) and for nothing."""
    if isinstance(raw, str):
        return raw or None
    if isinstance(raw, dict):
        for key in _WRAPPED_KEYS:
            value = raw.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def unspecified_party(raw: Any) -> dict[str, str] | None:
    """``{reason, description}`` when a ``subject`` / ``interestedParty`` value
    is a BODS ``UnspecifiedRecord``; ``None`` for a reference (bare or legacy
    wrapped) and for nothing at all.

    Reads the v0.4 shape (``{reason, description}``) and the older wrapped one
    (``{"unspecified": {reason, description}}``); either key may be missing.
    The complement of ``party_ref``: a dict it cannot read a reference out of
    is the unspecified record. Used by the Senzing and FtM exports (Phase 276)
    to draw a placeholder instead of dropping the relationship.
    """
    if not isinstance(raw, dict) or party_ref(raw) is not None:
        return None
    wrapped = raw.get("unspecified")
    body: dict[str, Any] = wrapped if isinstance(wrapped, dict) else raw
    out: dict[str, str] = {}
    for key in ("reason", "description"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            out[key] = value.strip()
    return out


def statement_index(statements: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Every entity and person statement, keyed by statementId **and**
    recordId, so a lookup by whichever a relationship used finds the
    statement. A statementId key is never overwritten by a recordId collision
    from another statement.

    ``declarationSubject`` is **not** a key. It names the subject of the
    network a statement belongs to (the OECD stamps the group head on every
    MEIP statement; since the declaration-subject phase OpenCheck stamps the
    looked-up company on every statement of a source bundle), so it names
    *another* statement, never this one. Phase 210 indexed it as a third tier
    below recordId, which was harmless only while the subject's own statement
    was in the bundle — in any bundle without it the subject's id resolved to
    whichever party came first. A relationship references a party by
    recordId (v0.4) or statementId (OpenCheck's own, equal for now); nothing
    references a party by declarationSubject."""
    # Two tiers, in strict order: a statementId is never shadowed by another
    # statement's recordId.
    by_sid: dict[str, dict[str, Any]] = {}
    by_rid: dict[str, dict[str, Any]] = {}
    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        kind = stmt.get("recordType") or stmt.get("statementType")
        if kind not in _PARTY_TYPES:
            continue
        sid = stmt.get("statementId") or stmt.get("statementID")
        if isinstance(sid, str) and sid:
            by_sid.setdefault(sid, stmt)
        rid = stmt.get("recordId")
        if isinstance(rid, str) and rid:
            by_rid.setdefault(rid, stmt)
    index = dict(by_rid)
    index.update(by_sid)
    return index


def resolver(statements: Iterable[dict[str, Any]]) -> Callable[[Any], str]:
    """A function from a party reference (any spelling, bare or wrapped) to
    the **statementId** of the statement it names — or, when nothing in the
    bundle carries that id, the reference itself, so callers can still detect
    a dangling edge by ``ref not in <their statementId set>``."""
    index = statement_index(statements)

    def resolve(raw: Any) -> str:
        ref = party_ref(raw)
        if ref is None:
            return ""
        stmt = index.get(ref)
        if stmt is None:
            return ref
        sid = stmt.get("statementId") or stmt.get("statementID")
        return sid if isinstance(sid, str) and sid else ref

    return resolve


# ---------------------------------------------------------------------------
# Risk-signal evidence names statements too
# ---------------------------------------------------------------------------

#: The evidence fields that name a statement, and the shape each one has.
#: ``signalScope.ts`` reads the same fields on the frontend.
SIGNAL_REFERENCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("statement_id", "one"),
    ("subject_statement_id", "one"),
    ("subject_statement_ids", "list"),
    ("longest_path", "list"),
    ("matches", "list_of_dicts"),
    ("jurisdictions", "list_of_dicts"),
    ("ended_relationship_statement_ids", "list"),
)


def resolve_signal_references(
    signals: Iterable[dict[str, Any]], statements: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """The signals with every evidence reference that names a statement
    resolved to that statement's **statementId**.

    The risk engine predicts the id of the statement a finding is about
    from the source and the hit id — a prediction that is the party's
    *recordId* now that a statementId is content-addressed and cannot be
    predicted. A consumer (the graph badges, ``signalScope``, the
    narrative) keys on statementId, so the reference is resolved here, in
    one place, through the same tiers a relationship reference takes. An
    id that names nothing in ``statements`` is left as it is, so a
    dangling reference still reads as dangling. Signals are copied where
    a field changes; untouched ones are returned as they are.
    """
    resolve = resolver(statements)

    def one(value: Any) -> Any:
        if not isinstance(value, str) or not value:
            return value
        resolved = resolve(value)
        return resolved or value

    out: list[dict[str, Any]] = []
    for sig in signals:
        evidence = sig.get("evidence")
        if not isinstance(evidence, dict):
            out.append(sig)
            continue
        new_evidence: dict[str, Any] | None = None
        for field, shape in SIGNAL_REFERENCE_FIELDS:
            value = evidence.get(field)
            if value is None:
                continue
            if shape == "one":
                replacement: Any = one(value)
            elif shape == "list" and isinstance(value, list):
                replacement = [one(v) for v in value]
            elif shape == "list_of_dicts" and isinstance(value, list):
                replacement = [
                    {**m, "statement_id": one(m.get("statement_id"))}
                    if isinstance(m, dict) and m.get("statement_id") else m
                    for m in value
                ]
            else:
                continue
            if replacement != value:
                if new_evidence is None:
                    new_evidence = dict(evidence)
                new_evidence[field] = replacement
        if new_evidence is None:
            out.append(sig)
        else:
            out.append({**sig, "evidence": new_evidence})
    return out
