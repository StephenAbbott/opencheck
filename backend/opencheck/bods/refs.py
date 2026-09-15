"""Resolving a relationship's parties to the statements they name (Phase 210).

BODS v0.4 says a relationship's ``subject`` and ``interestedParty`` hold the
**recordId** of the party's entity or person statement. Every OpenCheck
mapper sets ``statementId == recordId`` for entities and people, so for ten
months nothing distinguished the two and every consumer — the graph, the risk
layer walk, the reconciler, the PDF diagram, the Senzing / Neo4j / FtM
exports — keyed its lookups on ``statementId`` and happened to work.

Phase 208 brought the first statements OpenCheck did not write: the OECD's
MEIP release, where ``statementId`` is a hash and ``recordId`` is
``meip-entity-N`` and the relationships reference the latter, as the standard
says. The edge then resolved to nothing and A/S Norske Shell and SHELL PLC
drew as two unlinked nodes.

This module is the one place that knows both spellings. ``party_ref`` reads a
reference out of either the v0.4 bare string or the legacy wrapped object;
``resolver`` returns a function mapping any reference — recordId, statementId
or ``declarationSubject`` alias — to the **statementId** of the statement it
names, which is what every index in the codebase is keyed on. An unknown
reference comes back unchanged so a dangling edge still reads as dangling.
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


def statement_index(statements: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Every entity and person statement, keyed by statementId **and**
    recordId (and ``declarationSubject`` where it differs), so a lookup by
    whichever a relationship used finds the statement. A statementId key is
    never overwritten by a recordId collision from another statement."""
    # Three tiers, in strict order: a statementId is never shadowed by another
    # statement's recordId, and a recordId never by a declarationSubject. The
    # last matters in the wild — the OECD stamps every statement's
    # declarationSubject with the *group head's* recordId, so as a peer of
    # recordId it pointed the head's id at the first subsidiary in the file.
    by_sid: dict[str, dict[str, Any]] = {}
    by_rid: dict[str, dict[str, Any]] = {}
    by_decl: dict[str, dict[str, Any]] = {}
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
        decl = stmt.get("declarationSubject")
        if isinstance(decl, str) and decl:
            by_decl.setdefault(decl, stmt)
    index = dict(by_decl)
    index.update(by_rid)
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
