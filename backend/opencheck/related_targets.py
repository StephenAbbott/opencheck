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
   kind merge when they share an identifier, or when their name keys are
   identical and their birth years (founding years, for entities) do not
   conflict — a year missing on one side is not a conflict (Stephen, 2 Oct
   2026). An entity additionally must not have conflicting jurisdictions. A
   cluster never holds two different years, so a year-less record cannot
   chain two dated namesakes together. A person's name key ignores word
   order and honorifics (Phase 282): "FIORE, Norman Benito" and "NORMAN
   BENITO FIORE" are one director, not two.
2. **Rank.** Current before former; within each, owners and controllers, then
   officers, then everyone else; within each tier, a party linked directly
   to the looked-up company before one linked further out; and among
   officers equally close, board chair, then board members, then senior
   managing officials (Phase 282).
3. **A party carrying an LEI first** among parties still tied (Phase 284):
   a GLEIF-registered group member before an officer's other appointments.
   When a whole large group shares one tier, the name order below would
   otherwise pick an arbitrary slice — Taqa Bratani's 93 parties all tie in
   the bottom tier, and alphabetical order pushed Taweelah Asia Power
   Company, the sanctions-controlled group member, past the limit.
4. **Break remaining ties without bundle order** (Phase 282). Bundle
   order is the order the sources answered, so it changed between runs,
   and with it which parties at the limit were screened (Eli Lilly, 2 Oct
   2026). Ties go to the party more sources name, then by name key,
   birth/founding year and identifiers. statementIds are never used: they
   change between runs.
5. **Cap**, and report what the cap left out — counts only, never names.

A screen then reads one representative per cluster, and a signal it earns is
attached to the whole party (``attach_to_party``): one signal per party per
record, carrying **every** statement of the cluster in
``evidence.subject_statement_ids`` so each per-source card keeps its badge
(Phase 282; until then each statement got its own copy, and one party
counted once per source that named it). A cluster
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
from .bods.source_ids import source_id_of
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

#: Order of officer roles inside ``TIER_ROLE`` (Phase 282), best first.
_ROLE_RANK = {"boardChair": 0, "boardMember": 1, "seniorManagingOfficial": 2}
#: Rank of a party with no officer role (any tier other than ``TIER_ROLE``).
_NO_ROLE_RANK = len(_ROLE_RANK)


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

    ranked: list[tuple[tuple[Any, ...], list[dict[str, Any]]]] = []
    for cluster in clusters:
        members = sorted(cluster.members, key=lambda m: _member_order(m, stmts))
        reps = _representatives(members)
        rep = reps[0]
        tier, role, direct = min(
            (
                facts.get(m["statement_id"], (TIER_OTHER, _NO_ROLE_RANK, False))
                for m in members
            ),
            key=lambda f: (f[0], not f[2], f[1]),
        )
        key = (
            1 if rep["former"] else 0,
            tier,
            0 if direct else 1,
            role,
            # Phase 284: a party carrying an LEI before one without — a
            # GLEIF-registered group member before an officer's other
            # appointments. Taqa Bratani's 93 parties all tied in the bottom
            # tier, and the name order below pushed Taweelah Asia Power
            # Company (sanctions-controlled, export-control-linked; one of
            # its 8 LEI-bearing group members) past the limit.
            0 if any(k.startswith("LEI:") for k in cluster.idents) else 1,
            # Phase 282: ties without bundle order. A party more registers
            # name first, then fields that do not change between runs.
            -len({_source_key(stmts.get(m["statement_id"]) or {}) for m in members}),
            min(cluster.names) if cluster.names else "",
            min(cluster.years) if cluster.years else 0,
            tuple(sorted(cluster.idents)),
            tuple(sorted(cluster.jurisdictions)),
            tuple(sorted(names.normalise_name(m.get("name") or "") for m in members)),
        )
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


def officer_only_entity_ids(bods: list[dict[str, Any]]) -> set[str]:
    """statementIds of **corporate officers**: entities that are the interested
    party of at least one relationship, and whose every relationship as
    interested party carries only officer interests (board member, chair,
    senior managing official) — none of ownership or control.

    Phase 293. A company sitting on a board — the Belize IBCs that are the
    designated members of an Azerbaijani Laundromat LLP — is named by the
    register for its role, not for anything that would distinguish it, and its
    name is usually generic ("CORPORATE SOLUTIONS LIMITED"). The screens use
    this to require a corroborating country before such a match counts for
    more than ``low`` (Stephen, 6 Oct 2026). Ended relationships count too: a
    former corporate director is still only an officer.
    """
    resolve = resolver(bods)
    entities = {_statement_id(s) for s in bods if _stmt_kind(s) == "entity"}
    officer: set[str] = set()
    other: set[str] = set()
    for stmt in bods:
        if _stmt_kind(stmt) != "relationship":
            continue
        _subj, ip, _ = _relationship_endpoints(stmt, resolve)
        if not ip or ip not in entities:
            continue
        types = {
            i.get("type") for i in _interests(stmt) if isinstance(i, dict)
        }
        if types and types <= _ROLE_INTERESTS:
            officer.add(ip)
        else:
            other.add(ip)
    return officer - other


def attach_to_party(signals: list[Any], screened: list[dict[str, Any]]) -> list[Any]:
    """Attach each signal a representative earned to its whole party.

    A signal names the representative it was screened as through
    ``evidence.subject_statement_id``. When that representative's cluster
    holds several statements, the signal is re-anchored on the party's first
    representative and lists every member in
    ``evidence.subject_statement_ids`` — the shape ``signalScope`` already
    reads (Phase 247), so the card for each source that named the party
    keeps its badge (Stephen, 2 Oct 2026: attach to both).

    Phase 282 replaced one copy per statement with this. The copies made one
    party count once per source that named it — Moody's Corporation was two
    ``RELATED_EXPORT_RISK`` signals on Risk First Limited — in the API, the
    exports, batch screening and ``/signalstats``. Two spellings of one party
    that match the same record now share an anchor, so each screen's own
    ``_dedupe`` collapses them to one. Signals of any other shape — a subject
    signal keyed on ``statement_id`` — pass through untouched. Each changed
    signal is a ``dataclasses.replace`` with a fresh evidence dict.
    """
    party: dict[str, tuple[str, list[str]]] = {}
    for rep in screened:
        sids = list(rep.get("statement_ids") or [rep["statement_id"]])
        anchor = rep.get("party_anchor") or rep["statement_id"]
        party[rep["statement_id"]] = (anchor, sids)
    out: list[Any] = []
    for sig in signals:
        evidence = getattr(sig, "evidence", None) or {}
        sub = str(evidence.get("subject_statement_id") or "")
        hit = party.get(sub)
        if hit is None or (len(hit[1]) < 2 and hit[0] == sub):
            out.append(sig)
            continue
        anchor, sids = hit
        new_evidence = dict(evidence)
        new_evidence["subject_statement_id"] = anchor
        if len(sids) > 1:
            new_evidence["subject_statement_ids"] = sorted(set(sids))
        out.append(dataclasses.replace(sig, evidence=new_evidence))
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


def _name_key(name: str | None, kind: str) -> str:
    """What two targets must share to merge on name: a person's name key is
    order- and honorific-insensitive (``names.person_name_key``, Phase 282);
    an entity's is its normalised name, where word order is meaningful."""
    if kind == "person":
        return names.person_name_key(name)
    return names.normalise_name(name or "")


def _cluster(
    targets: list[dict[str, Any]], stmts: dict[str, dict[str, Any]]
) -> list[_Cluster]:
    clusters: list[_Cluster] = []
    for t in targets:
        stmt = stmts.get(t["statement_id"]) or {}
        rd = stmt.get("recordDetails") or {}
        name = _name_key(t.get("name"), t["kind"])
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
    return clusters


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


def _source_key(stmt: dict[str, Any]) -> str:
    """Which source a statement came from, for counting distinct sources:
    the registered adapter id, else the source description as published."""
    sid = source_id_of(stmt) if stmt else None
    if sid:
        return sid
    return str((stmt.get("source") or {}).get("description") or "")


def _member_order(m: dict[str, Any], stmts: dict[str, dict[str, Any]]) -> tuple[str, ...]:
    """A cluster's members in an order that does not depend on bundle order
    (Phase 282): by spelling, then source. The representative — whose
    spelling is screened first and whose facts lead — is the first of these.
    """
    return (
        names.normalise_name(m.get("name") or ""),
        _source_key(stmts.get(m["statement_id"]) or {}),
        str(m.get("name") or ""),
    )


def _representatives(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One target per distinct spelling in the cluster, all carrying its facts.

    Name-merged members share a spelling by construction, so most clusters
    yield one target; an identifier-merged cluster may yield a few. Each keeps
    the statementId of the member it was spelled from.
    """
    base = _representative(members)
    base["party_anchor"] = base["statement_id"]
    out = [base]
    kind = base.get("kind") or ""
    # Spellings that differ only in word order or honorifics are one spelling
    # to a name screen (Phase 282), so a person is not read twice for them.
    seen = {_name_key(base.get("name"), kind)}
    for m in members[1:]:
        key = _name_key(m.get("name"), kind)
        if not key or key in seen or len(out) >= MAX_NAMES_PER_PARTY:
            continue
        seen.add(key)
        out.append({**base, "name": m["name"], "statement_id": m["statement_id"]})
    return out


def _representative(members: list[dict[str, Any]]) -> dict[str, Any]:
    """The first member (``_member_order``), with the cluster's facts pooled.

    A year one source omits is taken from another (the same person, by the
    merge rule); nationalities and countries are unioned; a cluster is former
    only when **every** member is — one current role is a current party.
    """
    rep = dict(members[0])
    if "birth_year" in rep:
        years = sorted(m["birth_year"] for m in members if m.get("birth_year"))
        if years:
            rep["birth_year"] = years[0]
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
    # Phase 293: a party is a corporate officer only when every statement of
    # it is — one source that records it as an owner makes it an owner.
    if "officer_entity" in rep:
        rep["officer_entity"] = all(bool(m.get("officer_entity")) for m in members)
    rep["statement_ids"] = [m["statement_id"] for m in members]
    return rep


# ---------------------------------------------------------------------
# Ranking facts
# ---------------------------------------------------------------------


def _party_facts(
    bods: list[dict[str, Any]], subject_ids: frozenset[str] | set[str]
) -> dict[str, tuple[int, int, bool]]:
    """statementId → (best role tier, officer rank, linked directly?).

    The officer rank orders chair, board member and senior managing official
    inside ``TIER_ROLE`` (Phase 282); it is ``_NO_ROLE_RANK`` in every other
    tier, so it never reorders owners or "other" parties.

    Read from **current** relationships only: an ended directorship says why a
    party is former, not that it is an officer now. A party that is only ever
    the *subject* of a relationship (a subsidiary, an owned entity) gets the
    "other" tier, but is still direct when the looked-up company owns it.
    """
    ended = _ended_relationship_ids(bods)
    resolve = resolver(bods)
    out: dict[str, tuple[int, int, bool]] = {}

    def _note(sid: str, tier: int, role: int, direct: bool) -> None:
        if not sid:
            return
        cur = out.get(sid)
        if cur is None:
            out[sid] = (tier, role, direct)
        else:
            best = min((cur[0], cur[1]), (tier, role))
            out[sid] = (best[0], best[1], cur[2] or direct)

    for stmt in bods:
        if _stmt_kind(stmt) != "relationship" or _statement_id(stmt) in ended:
            continue
        subj, ip, _ = _relationship_endpoints(stmt, resolve)
        tier = TIER_OTHER
        role = _NO_ROLE_RANK
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
                role = min(role, _ROLE_RANK[itype])
        if tier != TIER_ROLE:
            role = _NO_ROLE_RANK
        _note(ip, tier, role, subj in subject_ids)
        _note(subj, TIER_OTHER, _NO_ROLE_RANK, ip in subject_ids)
    return out
