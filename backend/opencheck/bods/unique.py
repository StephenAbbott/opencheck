"""One statement per statementId (Phase 235).

BODS requires statementIds to be unique within a publication, and OpenCheck's
bundles were not: on Rosneft the OpenSanctions mapper emitted the same
subsidiary once per FtM edge that named it (13 repeated ids in one export),
and GLEIF emitted John Swire & Sons twice for Swire Pacific — once as the
direct parent, once as the ultimate. Every repeat was byte-identical: the
factories derive the id from the source and the record, so the same record
mapped twice is the same statement twice.

``unique_statements`` keeps the first occurrence of each statementId, in
order. A later statement under an id already seen is dropped whether or not it
is identical — two different bodies under one id cannot both be published, and
keeping the first keeps the mapper's own order. A non-identical collision is
logged (counts only, never content) because it means a mapper gave two
different records the same id, which is a mapper bug worth finding.
Statements with no statementId are kept as they are; ``validate_shape``
reports them.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable

_LOG = logging.getLogger(__name__)


def _canonical(stmt: dict[str, Any]) -> str:
    return json.dumps(stmt, sort_keys=True, ensure_ascii=False, default=str)


_PARTY_TYPES = frozenset({"entity", "person"})


def _dedupe_key(stmt: dict[str, Any]) -> str | None:
    """One statement per *record* for a party, per statementId otherwise.

    Since statementIds became content-addressed (the record-ID ticket's phase
    B) a party mapped twice with slightly different details — a person on two
    of a register's lists, an officer who is also a PSC — carries two
    statementIds under one recordId. Deduping on the recordId keeps exactly
    what deduping on the statementId kept while the two were equal: the first
    mapping of each party, in the mapper's order. A relationship keeps its
    statementId key: a closed record beside the open one it replaced is two
    statements of one record, and both are published (Phase 317).
    """
    if stmt.get("recordType") in _PARTY_TYPES:
        rid = stmt.get("recordId")
        if isinstance(rid, str) and rid:
            return rid
    sid = stmt.get("statementId")
    return sid if isinstance(sid, str) and sid else None


def unique_statements(statements: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """``statements`` with every repeated party (by recordId) or repeated
    statementId after the first dropped."""
    out: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    differing = 0
    for stmt in statements:
        key = _dedupe_key(stmt) if isinstance(stmt, dict) else None
        if not key:
            out.append(stmt)
            continue
        first = seen.get(key)
        if first is None:
            seen[key] = stmt
            out.append(stmt)
            continue
        if first is not stmt and _canonical(first) != _canonical(stmt):
            differing += 1
    if differing:
        _LOG.warning(
            "unique_statements: %d statement(s) repeated a record with different "
            "content; the first was kept.",
            differing,
        )
    return out


def duplicate_statement_ids(statements: Iterable[dict[str, Any]]) -> list[str]:
    """statementIds that occur more than once, in first-repeat order."""
    seen: set[str] = set()
    dupes: list[str] = []
    for stmt in statements:
        sid = stmt.get("statementId") if isinstance(stmt, dict) else None
        if not sid:
            continue
        if sid in seen and sid not in dupes:
            dupes.append(sid)
        seen.add(sid)
    return dupes


__all__ = ["duplicate_statement_ids", "unique_statements"]
