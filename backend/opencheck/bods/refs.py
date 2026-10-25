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
    # RECORD_CONSISTENCY names the subject's statements here (Phase 337); the
    # key matches neither the ``statement_id`` nor the ``…statement_ids`` rule,
    # so FullCheck's anchor collapse left it stale until it was named (found
    # in production, 25 Oct 2026).
    ("subject_statements", "list"),
    # GLEIF_REPORTING_EXCEPTION / OPAQUE_OWNERSHIP carry each exception's
    # bridge statement here as well as in ``matches[]``; only ``matches[]``
    # was resolved, so the two named different ids for one node.
    ("exceptions", "list_of_dicts"),
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


# ---------------------------------------------------------------------------
# Rewriting references (the structured anchor remap)
# ---------------------------------------------------------------------------

#: The top-level and ``recordDetails`` fields of a BODS statement that hold a
#: party or record reference. FullCheck's anchor collapse (``routers/expand.py``)
#: rewrites exactly these — never a name, an annotation sentence, ``details``
#: or any other free text. ``subject`` / ``interestedParty`` may be a bare
#: string or a legacy ``describedBy*`` wrapper (``_WRAPPED_KEYS``), and an
#: ``UnspecifiedRecord`` there is left alone; ``componentRecords`` holds
#: relationship recordIds.
STATEMENT_REFERENCE_FIELDS: tuple[str, ...] = (
    "statementId",
    "recordId",
    "declarationSubject",
    "recordDetails/subject",
    "recordDetails/interestedParty",
    "recordDetails/componentRecords",
)

#: An evidence key holding one statement id, wherever it sits in ``evidence``.
EVIDENCE_ID_KEY = "statement_id"
#: An evidence key holding a list of statement ids, by suffix: ``statement_ids``,
#: ``subject_statement_ids``, ``party_statement_ids``,
#: ``relationship_statement_ids``, ``ended_relationship_statement_ids`` …
EVIDENCE_ID_LIST_SUFFIX = "statement_ids"


def _remap_party_value(raw: Any, repl: dict[str, str]) -> Any:
    """A ``subject`` / ``interestedParty`` value with its reference rewritten,
    in the shape it came in — a bare string stays bare, a legacy wrapper keeps
    its key — or the value itself when it is an ``UnspecifiedRecord``, empty,
    or names nothing in ``repl``."""
    if isinstance(raw, str):
        return repl.get(raw, raw)
    if isinstance(raw, dict):
        for key in _WRAPPED_KEYS:
            value = raw.get(key)
            if isinstance(value, str) and value in repl:
                return {**raw, key: repl[value]}
    return raw


def remap_statement_references(
    statements: Iterable[dict[str, Any]], repl: dict[str, str]
) -> list[dict[str, Any]]:
    """The statements with every reference in ``STATEMENT_REFERENCE_FIELDS``
    that names a key of ``repl`` replaced by its value — ``statementId``,
    ``recordId`` and ``declarationSubject`` on the statement, the party
    references and ``componentRecords`` under ``recordDetails``. Nothing else
    is read: a statement id quoted in an annotation sentence, a name or an
    interest's ``details`` is left as written.

    Statements are copied where a field changes; untouched ones are returned
    as they are (the ``resolve_signal_references`` rule). The old
    ``json.dumps().replace()`` remap rewrote every occurrence of an id,
    including inside free text, and knew nothing about which fields are
    references.
    """
    if not repl:
        return list(statements)
    out: list[dict[str, Any]] = []
    for stmt in statements:
        if not isinstance(stmt, dict):
            out.append(stmt)
            continue
        new_stmt: dict[str, Any] | None = None
        for field in ("statementId", "recordId", "declarationSubject"):
            value = stmt.get(field)
            if isinstance(value, str) and value in repl:
                if new_stmt is None:
                    new_stmt = dict(stmt)
                new_stmt[field] = repl[value]
        rd = stmt.get("recordDetails")
        if isinstance(rd, dict):
            new_rd: dict[str, Any] | None = None
            for field in ("subject", "interestedParty"):
                if field not in rd:
                    continue
                replacement = _remap_party_value(rd[field], repl)
                if replacement is not rd[field]:
                    if new_rd is None:
                        new_rd = dict(rd)
                    new_rd[field] = replacement
            components = rd.get("componentRecords")
            if isinstance(components, list) and any(
                isinstance(c, str) and c in repl for c in components
            ):
                if new_rd is None:
                    new_rd = dict(rd)
                new_rd["componentRecords"] = [
                    repl.get(c, c) if isinstance(c, str) else c for c in components
                ]
            if new_rd is not None:
                if new_stmt is None:
                    new_stmt = dict(stmt)
                new_stmt["recordDetails"] = new_rd
        out.append(stmt if new_stmt is None else new_stmt)
    return out


_NAMED_ONE_FIELDS = frozenset(f for f, shape in SIGNAL_REFERENCE_FIELDS if shape == "one")
_NAMED_LIST_FIELDS = frozenset(f for f, shape in SIGNAL_REFERENCE_FIELDS if shape == "list")


def _remap_evidence(value: Any, repl: dict[str, str], key: str | None = None) -> Any:
    """``value`` with every statement id it holds in a reference position
    rewritten through ``repl``: a ``statement_id`` key, a key ending
    ``statement_ids``, and the named list fields in ``SIGNAL_REFERENCE_FIELDS``
    (``longest_path``, whose name does not say it holds ids). Walks dicts and
    lists to any depth, so a nested ``state_holdings[].parties[]`` or
    ``elements[].statement_ids`` is reached, and reads no other string — a
    ``summary`` or a ``detail`` that quotes an id keeps it. Returns the same
    object when nothing changed, so callers can copy on change."""
    if isinstance(value, dict):
        new: dict[str, Any] | None = None
        for k, v in value.items():
            if k == EVIDENCE_ID_KEY or k in _NAMED_ONE_FIELDS:
                r: Any = repl.get(v, v) if isinstance(v, str) else v
            elif k.endswith(EVIDENCE_ID_LIST_SUFFIX) or k in _NAMED_LIST_FIELDS:
                r = (
                    [repl.get(x, x) if isinstance(x, str) else x for x in v]
                    if isinstance(v, list) else _remap_evidence(v, repl, k)
                )
            else:
                r = _remap_evidence(v, repl, k)
            if r is not v and r != v:
                if new is None:
                    new = dict(value)
                new[k] = r
        return value if new is None else new
    if isinstance(value, list):
        items = [_remap_evidence(x, repl) for x in value]
        return value if all(a is b for a, b in zip(items, value)) else items
    return value



def remap_signal_references(
    signals: Iterable[dict[str, Any]], repl: dict[str, str]
) -> list[dict[str, Any]]:
    """The signals with every statement id in their ``evidence`` that names a
    key of ``repl`` replaced by its value: the fields
    ``resolve_signal_references`` names (``SIGNAL_REFERENCE_FIELDS``) **and**,
    because the risk engine nests ids the named list does not reach —
    ``STATE_CONTROLLED``'s ``state_holdings[].parties[].statement_id`` and
    ``party_statement_ids``, ``TRUST_OR_ARRANGEMENT``'s
    ``elements[].statement_ids``, ``relationship_statement_ids`` — any
    ``statement_id`` key or key ending ``statement_ids`` at any depth of
    ``evidence`` (Stephen, 24 Oct 2026: both). ``summary``, ``hit_id`` and
    every other free text are not read. Signals are copied where a field
    changes; untouched ones are returned as they are.
    """
    if not repl:
        return list(signals)
    out: list[dict[str, Any]] = []
    for sig in signals:
        evidence = sig.get("evidence") if isinstance(sig, dict) else None
        if not isinstance(evidence, dict):
            out.append(sig)
            continue
        new_evidence = _remap_evidence(evidence, repl)
        out.append(sig if new_evidence is evidence else {**sig, "evidence": new_evidence})
    return out
