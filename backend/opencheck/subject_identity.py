"""The subject identity set — which statements in a bundle ARE the looked-up
company (Phase 235).

The related-party screens (``cross_check``, ``icij_check``,
``openaleph_check``) are handed the merged bundle, and until Phase 235 they
screened every entity statement in it. That included other sources'
statements *of the subject itself*: OpenSanctions' record for Rosneft carries
Rosneft's own LEI, and screening it produced RELATED_SANCTIONED,
RELATED_DEBARMENT, RELATED_EXPORT_CONTROLLED and RELATED_EXPORT_RISK — the
subject's own listing, counted a second time and presented as third-party
exposure. CLP HOLDINGS LIMITED read "Related entity 'CLP HOLDINGS LIMITED'".

The rule, in one place so the three screens cannot drift:

* **Seed** — ``subject_profile.subject_statements``: the referent group that
  holds a statement carrying the LEI as ``XI-LEI`` (the FullCheck merge rule),
  or the statements carrying the LEI when no group formed.
* **Identity set** — the LEI plus every identifier merge key those statements
  carry (``reconcile._identifier_keys``: LEI, scheme-scoped values and the
  jurisdiction-scoped register number). An entity statement sharing any key
  is the subject too. **Identifiers only, never a name**: a name match is
  never a referent, here as in ``subject_profile``.
* **A company cannot own itself.** When a relationship joins two statements
  the identifier rule put in the set, a shared identifier has pulled a
  counterparty in (a register number filed against the wrong company, say).
  The end that does not itself carry the looked-up LEI is dropped from the
  set, so it stays a screened related party. When both ends carry the LEI —
  the OECD's MEIP heads carry another group member's LEI in ~4% of rows,
  carried through as published by decision — the bundle gives no way to tell
  them apart and neither is dropped.

What leaves the related-party screens is the *subject*, not the finding: the
subject is screened at subject level by the LEI-keyed OpenSanctions and
OpenAleph adapters, and ``icij_check`` — which has no subject-level
adapter — screens the subject's names once, worded as the company's own.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .bods.refs import resolver
from .reconcile import _identifier_keys
from .subject_profile import subject_statements


def _carries_lei(stmt: dict[str, Any], lei: str) -> bool:
    return f"LEI:{lei}" in _identifier_keys(stmt)


@dataclass(frozen=True)
class SubjectIdentity:
    """The looked-up company's statements in a bundle."""

    lei: str = ""
    #: statementIds of the entity statements that describe the subject.
    statement_ids: frozenset[str] = field(default_factory=frozenset)
    #: The identifier merge keys the subject's statements carry.
    keys: frozenset[str] = field(default_factory=frozenset)
    #: The subject's statements, in bundle order.
    statements: tuple[dict[str, Any], ...] = ()

    def __contains__(self, statement_id: object) -> bool:
        return statement_id in self.statement_ids

    def __bool__(self) -> bool:
        return bool(self.statement_ids)

    def anchor_statement_id(self) -> str:
        """The statement a subject-level finding is attributed to: GLEIF's
        statement of the LEI when there is one (every lookup has it and the
        graph draws the subject from it), else the first statement carrying
        the LEI, else the first in the set."""
        gleif = [
            s for s in self.statements
            if _carries_lei(s, self.lei)
            and str((s.get("source") or {}).get("description") or "").strip() == "GLEIF"
        ]
        with_lei = [s for s in self.statements if _carries_lei(s, self.lei)]
        for pool in (gleif, with_lei, list(self.statements)):
            if pool:
                return str(pool[0].get("statementId") or "")
        return ""


EMPTY = SubjectIdentity()


def subject_identity(lei: str | None, bods: list[dict[str, Any]]) -> SubjectIdentity:
    """The subject identity set for ``lei`` in ``bods``.

    Empty when there is no LEI or no statement carries it — the screens then
    behave exactly as before, screening everything.
    """
    norm = (lei or "").strip().upper()
    if not norm or not bods:
        return EMPTY
    seed = subject_statements(norm, bods)
    if not seed:
        return EMPTY

    keys: set[str] = {f"LEI:{norm}"}
    for stmt in seed:
        keys |= _identifier_keys(stmt)

    members: dict[str, dict[str, Any]] = {}
    for stmt in bods:
        if stmt.get("recordType") != "entity":
            continue
        sid = stmt.get("statementId")
        if not sid:
            continue
        if any(s is stmt for s in seed) or (_identifier_keys(stmt) & keys):
            members[sid] = stmt

    # A company cannot own itself: a relationship joining two members means
    # one of them was put in the set by an identifier it should not carry.
    resolve = resolver(bods)
    dropped: set[str] = set()
    for stmt in bods:
        if stmt.get("recordType") != "relationship":
            continue
        rd = stmt.get("recordDetails") or {}
        ends = [
            resolve(rd.get("subject") if rd.get("subject") is not None else stmt.get("subject")),
            resolve(
                rd.get("interestedParty")
                if rd.get("interestedParty") is not None
                else stmt.get("interestedParty")
            ),
        ]
        if not all(ends) or ends[0] == ends[1] or not all(e in members for e in ends):
            continue
        carriers = [e for e in ends if _carries_lei(members[e], norm)]
        if len(carriers) == 1:
            dropped.update(e for e in ends if e not in carriers)

    kept = {sid: s for sid, s in members.items() if sid not in dropped}
    return SubjectIdentity(
        lei=norm,
        statement_ids=frozenset(kept),
        keys=frozenset(keys),
        statements=tuple(kept.values()),
    )


__all__ = ["EMPTY", "SubjectIdentity", "subject_identity"]
