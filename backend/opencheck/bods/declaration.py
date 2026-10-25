"""``declarationSubject`` names the network's subject (the declaration-subject phase).

BODS v0.4: ``declarationSubject`` is "a ``recordId`` value for the subject of
a beneficial ownership network (always an entity or person)", and every
statement about a declared network "MUST contain the subject's ``recordId``
as its ``declarationSubject``" (modelling requirements, *Information
updates*). The standard's own example (*Record identifiers*) has a
self-disclosed company whose ``recordId`` and ``declarationSubject`` are the
same value, and the same company inside another company's network with a
different ``recordId`` and that company's ``recordId`` as its
``declarationSubject``.

Until this phase every entity and person statement OpenCheck wrote carried
**its own** ``recordId`` as ``declarationSubject`` (``make_entity_statement``
/ ``make_person_statement``), and a relationship carried its *immediate*
subject's — so a Wikidata board member, an owner or a subsidiary each
claimed to be the subject of its own network, and in a chain deeper than one
layer an upper relationship named the company in the middle. Only the
looked-up company's own statement was right.

The decision (Stephen, 23 Oct 2026): **per source** — each source bundle's
statements carry *that source's own* subject record, so a bundle read alone
(``/deepen``, the Data drawer, a per-source export) is a declared network in
its own right, and the same bundle inside a lookup says the same thing.

Two passes, one rule:

* ``stamp_bundle`` runs where one source's bundle is mapped (``/deepen``,
  ``_safe_deepen``, the FullCheck register hop, the subsidiary network).
  The subject is found by ``find_subject``: the lookup's subject reference
  when the caller has one (an LEI, or Phase 290's ``SCHEME:id``) → an entity
  statement carrying the adapter's ``hit_id`` as an identifier (a register
  bundle never carries the LEI; the Data drawer knows only the hit id) → the
  only entity statement in the bundle → nothing. Nothing is invented: a
  bundle whose subject cannot be found keeps the factory default and the
  miss is counted on ``/signalstats`` (``declaration_subject``), the Phase
  267 rule that a miss is loud.
* ``stamp_merged`` runs over the merged lookup bundle, where the reconciler's
  referent group names every source's subject statement at once
  (``subject_profile.subject_statements``), and restamps each source's
  statements to that source's member of the group. Idempotent, so a bundle
  the first pass got right is unchanged, and a source the first pass could
  not resolve alone (no LEI on the statement, a hit id the mapper does not
  carry as an identifier) is resolved here.

Both work on copies and never mutate a statement (the MEIP rule), and both
leave alone any statement OpenCheck did not publish — the OECD's MEIP
statements (``VERBATIM_SOURCES``) and Open Ownership's stored bundles — whose
``declarationSubject`` is the publisher's to set.

The resolver (``bods/refs.py``) no longer treats ``declarationSubject`` as
an alias for the statement that carries it: once every statement names the
subject, that alias would point the subject's id at whichever party came
first in a bundle lacking the subject's own statement.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Iterable
from typing import Any

from ..matching import canonical_identifier
from .state_bodies import VERBATIM_SOURCES

log = logging.getLogger(__name__)

_PARTY_TYPES = frozenset({"entity", "person"})

#: How the subject of a single bundle was found — the closed vocabulary
#: ``/signalstats`` counts under ``declaration_subject``. ``ref``: the
#: lookup's subject reference; ``hit_id``: an entity statement carrying the
#: adapter's hit id; ``single``: the bundle's only entity statement;
#: ``unresolved``: none of those, the factory default left in place.
OUTCOMES: frozenset[str] = frozenset({"ref", "hit_id", "single", "unresolved"})


def _record_id(stmt: dict[str, Any]) -> str | None:
    rid = stmt.get("recordId")
    return rid if isinstance(rid, str) and rid else None


def _published_by_opencheck(stmt: dict[str, Any]) -> bool:
    publisher = (stmt.get("publicationDetails") or {}).get("publisher") or {}
    return str(publisher.get("name") or "").startswith("OpenCheck")


def _source_key(stmt: dict[str, Any]) -> str | None:
    """The source a statement names — the stamped id (Phase 267), else the
    display description, so Open Ownership's republished rows (an
    unregistered id) still group with their own subject."""
    source = stmt.get("source")
    if not isinstance(source, dict):
        return None
    sid = source.get("opencheckSourceId")
    if isinstance(sid, str) and sid:
        return sid
    desc = source.get("description")
    return desc if isinstance(desc, str) and desc else None


def _stampable(stmt: dict[str, Any], *, source_id: str | None) -> bool:
    if not isinstance(stmt, dict):
        return False
    if (source_id or _source_key(stmt)) in VERBATIM_SOURCES:
        return False
    return _published_by_opencheck(stmt)


def _entity_statements(statements: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        s for s in statements
        if isinstance(s, dict) and s.get("recordType") == "entity" and _record_id(s)
    ]


def _identifier_values(stmt: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for ident in (stmt.get("recordDetails") or {}).get("identifiers") or []:
        if not isinstance(ident, dict):
            continue
        value = canonical_identifier(ident.get("id"), min_len=0)
        if value:
            out.add(value)
    return out


def find_subject(
    statements: list[dict[str, Any]],
    *,
    ref: str | None = None,
    hit_id: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """The entity statement that is the subject of one source's bundle, and
    how it was found (one of ``OUTCOMES``). ``None, "unresolved"`` when none
    of the three rules names one."""
    entities = _entity_statements(statements)
    if not entities:
        return None, "unresolved"
    if ref:
        from ..subject_profile import subject_statements  # lazy: avoids a cycle

        for stmt in subject_statements(ref, statements):
            if stmt.get("recordType") == "entity" and _record_id(stmt):
                return stmt, "ref"
    if hit_id:
        wanted = {canonical_identifier(hit_id, min_len=0)}
        # OpenCorporates-style ``jurisdiction/number`` hit ids carry the
        # number after the last slash.
        if "/" in hit_id:
            wanted.add(canonical_identifier(hit_id.rsplit("/", 1)[-1], min_len=0))
        wanted.discard(None)
        for stmt in entities:
            if _identifier_values(stmt) & wanted:
                return stmt, "hit_id"
    if len(entities) == 1:
        return entities[0], "single"
    return None, "unresolved"


def _stamp(
    statements: list[dict[str, Any]],
    subject_record_id: str,
    *,
    source_id: str | None,
    only_source: str | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for stmt in statements:
        if (
            not _stampable(stmt, source_id=source_id)
            or (only_source is not None and _source_key(stmt) != only_source)
            or stmt.get("declarationSubject") == subject_record_id
        ):
            out.append(stmt)
            continue
        changed = copy.copy(stmt)
        changed["declarationSubject"] = subject_record_id
        out.append(changed)
    return out


def stamp_bundle(
    statements: list[dict[str, Any]],
    *,
    source_id: str | None = None,
    ref: str | None = None,
    hit_id: str | None = None,
    record: bool = True,
) -> list[dict[str, Any]]:
    """One source's bundle with every OpenCheck-published statement's
    ``declarationSubject`` set to the bundle subject's ``recordId``.

    Returns the list unchanged (same objects) when the subject cannot be
    found or the source is a verbatim publisher; otherwise a new list in
    which only the statements that changed are copies. ``record`` counts the
    outcome on ``/signalstats`` by ``source_id``.
    """
    if not statements or (source_id and source_id in VERBATIM_SOURCES):
        return statements
    if not any(_stampable(s, source_id=source_id) for s in statements):
        # Another publisher's statements (a stored Open Ownership bundle):
        # nothing to stamp, and no miss to count.
        return statements
    subject, outcome = find_subject(statements, ref=ref, hit_id=hit_id)
    if record and source_id:
        _record(source_id, outcome)
    rid = _record_id(subject) if subject else None
    if rid is None:
        return statements
    return _stamp(statements, rid, source_id=source_id)


def stamp_merged(
    statements: list[dict[str, Any]],
    ref: str,
) -> list[dict[str, Any]]:
    """The merged lookup bundle with each source's statements stamped to
    that source's own subject statement — the member of the subject's
    referent group that the source published (per source, by decision).

    A source with no member in the group is left as the first pass left it.
    """
    if not statements or not ref:
        return statements
    out = statements
    for key, rid in network_subjects(statements, ref).items():
        out = _stamp(out, rid, source_id=None, only_source=key)
    return out


def network_subjects(statements: list[dict[str, Any]], ref: str) -> dict[str, str]:
    """Each source's subject record in a merged bundle about ``ref``: source
    key → the ``recordId`` of that source's member of the subject's referent
    group. The networks ``stamp_merged`` stamps, read without stamping — a
    release build uses it to know which records are network subjects."""
    if not statements or not ref:
        return {}
    from ..subject_profile import subject_statements  # lazy: avoids a cycle

    by_source: dict[str, str] = {}
    for stmt in subject_statements(ref, statements):
        if stmt.get("recordType") != "entity":
            continue
        key = _source_key(stmt)
        rid = _record_id(stmt)
        if key and rid and key not in by_source:
            by_source[key] = rid
    return by_source


# ---------------------------------------------------------------------------
# Release files: many networks, one record per party
# ---------------------------------------------------------------------------
#
# A release file (the Estonia dataset, the laundromat dataset) holds many
# networks — one per company mapped — and one record per party: a person on
# two boards, or a company that is its own network's subject and a shareholder
# of three others, is one statement. BODS's *Record identifiers* page asks for
# a record per set of information, so a party shared by two networks would be
# two records; Stephen's decision (25 Oct 2026, the bulk-script stamping
# ticket) keeps one record, so the party stays one node across companies, and
# stamps it with one network:
#
# * a relationship keeps the subject of the bundle it was mapped in;
# * a network's subject names itself;
# * any other party names the first, by ``recordId``, of the network subjects
#   whose relationships reference it — a rule that does not depend on build
#   order — or, referenced by none, keeps the network its bundle gave it.
#
# The network subjects are passed in by the build, never read back from the
# file: in the pre-phase shape every party names itself, which any rule
# derived from the file's own values would accept.


def _party_refs(stmt: dict[str, Any]) -> list[str]:
    from .refs import party_ref

    rd = stmt.get("recordDetails") or {}
    out = []
    for key in ("subject", "interestedParty"):
        ref = party_ref(rd.get(key))
        if ref:
            out.append(ref)
    return out


def relationship_networks(
    statements: Iterable[dict[str, Any]], subjects: Iterable[str] | None = None
) -> dict[str, set[str]]:
    """Party ``recordId`` → the networks (``declarationSubject`` values) whose
    relationships reference it. With ``subjects``, only those networks count.
    One pass over the relationships; parties and other statements are
    skipped, so a build can stream a file through it."""
    subject_set = set(subjects) if subjects is not None else None
    networks: dict[str, set[str]] = {}
    for stmt in statements:
        if not isinstance(stmt, dict) or stmt.get("recordType") != "relationship":
            continue
        decl = stmt.get("declarationSubject")
        if not isinstance(decl, str) or not decl:
            continue
        if subject_set is not None and decl not in subject_set:
            continue
        for ref in _party_refs(stmt):
            networks.setdefault(ref, set()).add(decl)
    return networks


def settle_party(
    stmt: dict[str, Any], subjects: set[str], networks: dict[str, set[str]]
) -> dict[str, Any]:
    """One statement stamped by the one-record-one-network rule;
    ``networks`` is ``relationship_networks(..., subjects)``. A relationship,
    another publisher's statement or a party referenced by no network is
    returned as it is; a changed party is a copy."""
    rid = _record_id(stmt) if isinstance(stmt, dict) else None
    if (
        rid is None
        or stmt.get("recordType") not in _PARTY_TYPES
        or not _stampable(stmt, source_id=None)
    ):
        return stmt
    if rid in subjects:
        wanted: str | None = rid
    elif rid in networks:
        wanted = min(networks[rid])
    else:
        wanted = None
    if wanted is None or stmt.get("declarationSubject") == wanted:
        return stmt
    changed = copy.copy(stmt)
    changed["declarationSubject"] = wanted
    return changed


def settle_shared_parties(
    statements: list[dict[str, Any]], subjects: Iterable[str]
) -> list[dict[str, Any]]:
    """A release file with every OpenCheck-published party stamped by the
    one-record-one-network rule above (``settle_party`` over the list)."""
    subject_set = set(subjects)
    networks = relationship_networks(statements, subject_set)
    return [settle_party(s, subject_set, networks) for s in statements]


def declaration_problem(
    stmt: dict[str, Any], subjects: set[str], networks: dict[str, set[str]]
) -> str | None:
    """The rule one statement of a release file breaks, or ``None``;
    ``networks`` is ``relationship_networks(...)`` over the whole file,
    unfiltered. Another publisher's statements are not checked."""
    if not isinstance(stmt, dict) or not _stampable(stmt, source_id=None):
        return None
    sid = stmt.get("statementId")
    rid = _record_id(stmt)
    decl = stmt.get("declarationSubject")
    if not isinstance(decl, str) or decl not in subjects:
        return f"{sid}: declarationSubject {decl} is not a network subject"
    if stmt.get("recordType") not in _PARTY_TYPES or rid is None:
        return None
    if rid in subjects:
        if decl != rid:
            return f"{sid}: a network subject's record names {decl}, not itself"
    elif rid in networks and decl not in networks[rid]:
        return (
            f"{sid}: names network {decl}, which none of the relationships"
            " referencing it belong to"
        )
    return None


def release_declaration_problems(
    statements: Iterable[dict[str, Any]], subjects: Iterable[str]
) -> list[str]:
    """What the one-record-one-network rule promises, checked over a release
    file against the network subjects its build recorded. Empty means the
    file can ship; each entry names a statement and the rule it breaks:

    * its ``declarationSubject`` is not a network subject (the pre-phase
      shape: a non-subject party that names itself);
    * a network subject's own record names another network;
    * a party referenced by a network's relationships names a network none of
      its relationships belong to.
    """
    stmts = [s for s in statements if isinstance(s, dict)]
    subject_set = set(subjects)
    networks = relationship_networks(stmts)
    return [p for s in stmts if (p := declaration_problem(s, subject_set, networks))]


def _record(source_id: str, outcome: str) -> None:
    try:
        from .. import signalstats

        signalstats.record_declaration_subject(source_id, outcome)
    except Exception as exc:  # noqa: BLE001 — instrumentation never fails a lookup
        log.debug("record_declaration_subject failed, ignoring: %s", exc)
