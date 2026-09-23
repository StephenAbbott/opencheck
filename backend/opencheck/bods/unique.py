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


def unique_statements(statements: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """``statements`` with every repeated statementId after the first dropped."""
    out: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    differing = 0
    for stmt in statements:
        sid = stmt.get("statementId") if isinstance(stmt, dict) else None
        if not sid:
            out.append(stmt)
            continue
        first = seen.get(sid)
        if first is None:
            seen[sid] = stmt
            out.append(stmt)
            continue
        if first is not stmt and _canonical(first) != _canonical(stmt):
            differing += 1
    if differing:
        _LOG.warning(
            "unique_statements: %d statement(s) reused an existing statementId "
            "with different content; the first was kept.",
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
