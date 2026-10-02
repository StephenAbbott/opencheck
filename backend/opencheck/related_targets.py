"""Which related parties a capped screen reads, and in what order (Phase 279).

The related-party screens — ``cross_check`` (OpenSanctions + EveryPolitician)
and ``icij_check`` (Offshore Leaks) — read at most a fixed number of parties
per lookup, to keep upstream request volume sane. Until Phase 279 they took the
first N targets in **bundle order**, i.e. the order the sources answered and
were merged, and said nothing about the rest. One more source answering pushed
real parties past the cap: Eli Lilly's ``RELATED_PEP`` came and went between
runs because OpenCorporates' officers landed ahead of Katherine Baicker, and
Rosneft screened 25 of its 106 parties with Igor Sechin at position 104.

This module replaces "the first N" with a deliberate choice, in three steps:

1. **Dedupe across sources.** The same person often arrives two or three times
   (Wikidata and OpenCorporates both describe a director). Targets of the same
   kind merge when they share an identifier, or when their normalised names
   are identical and their birth years (founding years, for entities) do not
   conflict — a year missing on one side is not a conflict (Stephen, 2 Oct
   2026). An entity additionally must not have conflicting jurisdictions. A
   cluster never holds two different years, so a year-less record cannot
   chain two dated namesakes together.
2. **Rank.** Current before former; within each, owners and controllers, then
   board members and senior managing officials, then everyone else; within
   each tier, a party linked directly to the looked-up company before one
   linked further out. Ties keep bundle order, so the result is deterministic.
3. **Cap**, and report what the cap left out — counts only, never names.

A screen then reads one representative per cluster and hands any signal back
to **every** statement in the cluster (``fan_out``), so the per-source cards
keep their badges exactly as when each copy was screened on its own. A cluster
held together by an identifier can carry genuinely different spellings —
GLEIF's Cyrillic legal name and OpenSanctions' English one — and a name
screen matches on the spelling, so each distinct normalised name is read once
(at most ``MAX_NAMES_PER_PARTY``); the cluster still counts as one party
against the cap.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field
from typing import Any

from . import names
from .bods.refs import resolver
from .reconcile import _identifier_keys
from .risk import (
    _ended_relationship_ids,
    _interests,
    _relationship_endpoints,
    _stmt_kind,
    _statement_id,
)

#: Interest types that make the interested party an owner or controller.
#: The v0.4 ``interestType`` codelist, minus the board/officer roles below
#: and the two "we don't know" values.
_OWNER_CONTROL_INTERESTS = frozenset(
    {
        "shareholding",
        "votingRights",
        "appointmentOfBoard",
        "otherInfluenceOrControl",
        "controlViaCompanyRulesOrArticles",
        "controlByLegalFramework",
        "rightsToSurplusAssetsOnDissolution",
        "rightsToProfitOrIncome",
        "rightsGrantedByContract",
        "conditionalRightsGrantedByContract",
        "nominee",
        "nominator",
        "trustee",
        "settlor",
        "protector",
        "beneficiaryOfLegalArrangement",
        "enjoymentAndUseOfAssets",
        "rightToProfitOrIncomeFromAssets",
    }
)
#: Interest types that make the interested party a board member or officer.
_ROLE_INTERESTS = frozenset({"boardMember", "boardChair", "seniorManagingOfficial"})

#: Distinct spellings read per party — the same bound ``icij_check`` puts on
#: the subject's own names.
MAX_NAMES_PER_PARTY = 3

#: Role tiers, best first.
TIER_OWNER_CONTROL = 0
TIER_ROLE = 1
TIER_OTHER = 2


@dataclass
class Selection:
    """What a capped screen will read, and what it will not."""

    #: The targets to read, best-ranked party first: one per distinct
    #: spelling of each of at most ``limit`` parties. Each carries
    #: ``statement_ids`` (every member of its cluster).
    screened: list[dict[str, Any]] = field(default_factory=list)
    #: Parties (clusters) those targets cover — what the cap counts.
    parties_screened: int = 0
    #: Distinct related parties after dedupe (screened + not screened).
    total: int = 0
    #: Raw targets before dedupe — so a caller can say "49 statements, 31
    #: distinct parties" if it ever wants to.
    statements: int = 0
    #: Breakdown of the parties the cap left out, by why they ranked low.
    not_screened_former: int = 0
    not_screened_current_other: int = 0
    not_screened_current_role_or_owner: int = 0

    @property
    def not_screened(self) -> int:
        return self.total - self.parties_screened

    @property
    def truncated(self) -> bool:
        return self.not_screened > 0

    def detail(self, screen: str) -> str:
        """The counts-only sentence for a ``truncated`` ``DegradedSource``.

        ``screen`` names the check ("Related-party sanctions and PEP
        screening"). Never a party's name — the privacy contract of
        ``DegradedSource``.
        """
        n = self.not_screened
        parts: list[str] = []
        if self.not_screened_former:
            parts.append(f"{self.not_screened_former} former")
        if self.not_screened_current_other:
            parts.append(
                f"{self.not_screened_current_other} current with no ownership "
                "or board role recorded"
            )
        if self.not_screened_current_role_or_owner:
            parts.append(
                f"{self.not_screened_current_role_or_owner} current owners, "
                "controllers or officers beyond the limit"
            )
        why = f" ({'; '.join(parts)})" if parts else ""
        return (
            f"{screen} covered the {self.parties_screened} highest-ranked of "
            f"{self.total} related parties; {n} "
            f"{'was' if n == 1 else 'were'} not screened{why}."
        )


def select(
    targets: list[dict[str, Any]],
    bods: list[dict[str, Any]],
    *,
    limit: int,
    subject_ids: frozenset[str] | set[str] = frozenset(),
) -> Selection:
    """Dedupe, rank and cap ``targets`` (as built by a screen's collector).

    ``targets`` must carry ``kind``, ``statement_id``, ``name`` and ``former``;
    any of ``birth_year``/``founded`` and ``nationalities``/``countries`` they
    carry are pooled across a cluster onto its representative. ``subject_ids``
    is the looked-up company's identity set (statementIds), used only to tell
    a direct link from an indirect one — empty means "no anchor", and every
    party then ranks as indirect.
    """
    stmts = {_statement_id(s): s for s in bods if _statement_id(s)}
    clusters = _cluster(targets, stmts)
    facts = _party_facts(bods, subject_ids)
    position = {id(t): i for i, t in enumerate(targets)}

    ranked: list[tuple[tuple[int, int, int, int], list[dict[str, Any]]]] = []
    for members in clusters:
        reps = _representatives(members)
        rep = reps[0]
        tier, direct = min(
            (facts.get(m["statement_id"], (TIER_OTHER, False)) for m in members),
            key=lambda f: (f[0], not f[1]),
        )
        first_seen = min(position[id(m)] for m in members)
        key = (1 if rep["former"] else 0, tier, 0 if direct else 1, first_seen)
        ranked.append((key, reps))
    ranked.sort(key=lambda kr: kr[0])

    limit = max(0, limit)
    out = Selection(total=len(ranked), statements=len(targets))
    for i, (key, reps) in enumerate(ranked):
        rep, tier = reps[0], key[1]
        if i < limit:
            out.screened.extend(reps)
            out.parties_screened += 1
        elif rep["former"]:
            out.not_screened_former += 1
        elif tier == TIER_OTHER:
            out.not_screened_current_other += 1
        else:
            out.not_screened_current_role_or_owner += 1
    return out


def fan_out(signals: list[Any], screened: list[dict[str, Any]]) -> list[Any]:
    """Give every member of a cluster the signals its representative earned.

    A signal is attributed to the representative's statement through
    ``evidence.subject_statement_id``; each other member gets a copy carrying
    its own statementId (Stephen, 2 Oct 2026: attach to both). Copies are
    ``dataclasses.replace`` of the original with a fresh evidence dict, so
    nothing is shared between them. Signals of any other shape — a subject
    signal keyed on ``statement_id`` — pass through untouched.
    """
    extra: dict[str, list[str]] = {}
    for rep in screened:
        sids = [s for s in rep.get("statement_ids") or [] if s != rep["statement_id"]]
        if sids:
            extra[rep["statement_id"]] = sids
    # Two spellings of one party can match the same record; their copies then
    # coincide on (code, source, record, statement), which each screen's own
    # ``_dedupe`` collapses.
    if not extra:
        return list(signals)
    out: list[Any] = []
    for sig in signals:
        out.append(sig)
        sub = str((getattr(sig, "evidence", None) or {}).get("subject_statement_id") or "")
        for sid in extra.get(sub, ()):
            evidence = dict(sig.evidence)
            evidence["subject_statement_id"] = sid
            out.append(dataclasses.replace(sig, evidence=evidence))
    return out


def member_ids(screened: list[dict[str, Any]], statement_id: str) -> list[str]:
    """Every statementId of the cluster ``statement_id`` represents."""
    for rep in screened:
        if rep["statement_id"] == statement_id:
            return list(rep.get("statement_ids") or [statement_id])
    return [statement_id]


# ---------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------


@dataclass
class _Cluster:
    kind: str
    members: list[dict[str, Any]] = field(default_factory=list)
    names: set[str] = field(default_factory=set)
    idents: set[str] = field(default_factory=set)
    years: set[int] = field(default_factory=set)
    jurisdictions: set[str] = field(default_factory=set)


def _cluster(
    targets: list[dict[str, Any]], stmts: dict[str, dict[str, Any]]
) -> list[list[dict[str, Any]]]:
    clusters: list[_Cluster] = []
    for t in targets:
        stmt = stmts.get(t["statement_id"]) or {}
        rd = stmt.get("recordDetails") or {}
        name = names.normalise_name(t.get("name") or "")
        idents = _identifier_keys(stmt) if stmt else set()
        year = _year(rd, t["kind"])
        juris = _jurisdiction(rd) if t["kind"] == "entity" else None

        home = None
        for c in clusters:
            if c.kind != t["kind"]:
                continue
            if idents & c.idents:
                home = c
                break
            if not name or name not in c.names:
                continue
            if year is not None and c.years and year not in c.years:
                continue
            if juris is not None and c.jurisdictions and juris not in c.jurisdictions:
                continue
            home = c
            break
        if home is None:
            home = _Cluster(kind=t["kind"])
            clusters.append(home)
        home.members.append(t)
        if name:
            home.names.add(name)
        home.idents |= idents
        if year is not None:
            home.years.add(year)
        if juris is not None:
            home.jurisdictions.add(juris)
    return [c.members for c in clusters]


def _year(rd: dict[str, Any], kind: str) -> int | None:
    raw = rd.get("birthDate") if kind == "person" else rd.get("foundingDate")
    m = re.match(r"(\d{4})", str(raw or ""))
    return int(m.group(1)) if m else None


def _jurisdiction(rd: dict[str, Any]) -> str | None:
    juris = rd.get("jurisdiction")
    if isinstance(juris, dict):
        code = str(juris.get("code") or "").strip().upper()
        if re.match(r"^[A-Z]{2}", code):
            return code[:2]
    return None


def _representatives(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One target per distinct spelling in the cluster, all carrying its facts.

    Name-merged members share a spelling by construction, so most clusters
    yield one target; an identifier-merged cluster may yield a few. Each keeps
    the statementId of the member it was spelled from.
    """
    base = _representative(members)
    out = [base]
    seen = {names.normalise_name(base.get("name") or "")}
    for m in members[1:]:
        key = names.normalise_name(m.get("name") or "")
        if not key or key in seen or len(out) >= MAX_NAMES_PER_PARTY:
            continue
        seen.add(key)
        out.append({**base, "name": m["name"], "statement_id": m["statement_id"]})
    return out


def _representative(members: list[dict[str, Any]]) -> dict[str, Any]:
    """The first member in bundle order, with the cluster's facts pooled.

    A year one source omits is taken from another (the same person, by the
    merge rule); nationalities and countries are unioned; a cluster is former
    only when **every** member is — one current role is a current party.
    """
    rep = dict(members[0])
    for key in ("birth_year", "founded"):
        if key in rep and not rep[key]:
            rep[key] = next((m.get(key) for m in members if m.get(key)), rep[key])
    if "founded" in rep:
        dated = sorted(str(m["founded"]) for m in members if m.get("founded"))
        if dated:
            rep["founded"] = dated[0]
    for key in ("nationalities", "countries"):
        if key in rep:
            pooled: list[Any] = []
            for m in members:
                for v in m.get(key) or ():
                    if v not in pooled:
                        pooled.append(v)
            rep[key] = type(rep[key])(pooled) if isinstance(rep[key], tuple) else (
                sorted(pooled) if key == "countries" else pooled
            )
    rep["former"] = all(bool(m.get("former")) for m in members)
    rep["statement_ids"] = [m["statement_id"] for m in members]
    return rep


# ---------------------------------------------------------------------
# Ranking facts
# ---------------------------------------------------------------------


def _party_facts(
    bods: list[dict[str, Any]], subject_ids: frozenset[str] | set[str]
) -> dict[str, tuple[int, bool]]:
    """statementId → (best role tier, linked directly to the subject?).

    Read from **current** relationships only: an ended directorship says why a
    party is former, not that it is an officer now. A party that is only ever
    the *subject* of a relationship (a subsidiary, an owned entity) gets the
    "other" tier, but is still direct when the looked-up company owns it.
    """
    ended = _ended_relationship_ids(bods)
    resolve = resolver(bods)
    out: dict[str, tuple[int, bool]] = {}

    def _note(sid: str, tier: int, direct: bool) -> None:
        if not sid:
            return
        cur = out.get(sid)
        if cur is None:
            out[sid] = (tier, direct)
        else:
            out[sid] = (min(cur[0], tier), cur[1] or direct)

    for stmt in bods:
        if _stmt_kind(stmt) != "relationship" or _statement_id(stmt) in ended:
            continue
        subj, ip, _ = _relationship_endpoints(stmt, resolve)
        tier = TIER_OTHER
        for interest in _interests(stmt):
            if not isinstance(interest, dict):
                continue
            itype = interest.get("type")
            if interest.get("beneficialOwnershipOrControl") is True or (
                itype in _OWNER_CONTROL_INTERESTS
            ):
                tier = TIER_OWNER_CONTROL
                break
            if itype in _ROLE_INTERESTS:
                tier = TIER_ROLE
        _note(ip, tier, subj in subject_ids)
        _note(subj, TIER_OTHER, ip in subject_ids)
    return out
