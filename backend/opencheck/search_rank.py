"""Cross-source ranking for ``/search`` and ``opencheck_search`` (Phase 292).

Why this exists
---------------
Until Phase 292 ``/search`` returned hits grouped by source in registry
order, with no ranking across sources. ``opencheck_search("Metastar Invest")``
on 5 Oct 2026 returned 44 candidates with the exact match — ``METASTAR INVEST
LLP`` (Companies House OC346224) — at position 21, behind twenty fuzzy
``abr_australia`` and ``brreg`` rows such as ":-) INVEST AS" and "M GROW
INVEST". An agent reading the top ten saw nothing relevant.

The ladder
----------
Each hit gets a *match tier* measured from the query's side — what matters is
whether the candidate is what was asked for, not whether the two names are
symmetrically similar:

0. ``exact`` — equal under :func:`names.org_comparable_name` (legal forms
   collapsed to a cross-language class, so "Unilever Public Limited Company"
   ≡ "Unilever PLC"), or under its despaced form ("Ørsted A/S" ≡ "Orsted AS").
1. ``same_name`` — the names are equal once legal forms are removed
   entirely (:func:`names.org_name_residue`): the query "Metastar Invest"
   against "METASTAR INVEST LLP". The query named no form, so any form is
   the same name.
2. ``all_tokens`` — every token of the query appears verbatim in the
   candidate ("Bentcard Import" in "BENTCARD IMPORT AND EXPORT LTD").
3. ``distinctive_tokens`` — every *distinctive* token of the query (filler
   such as "holdings" or "international" dropped, the same list the Phase 120
   gate uses) agrees with a candidate token, exactly or within a single edit;
   numeric and single-letter discriminators must be identical.
4. ``fuzzy`` — anything else the source returned.

The direction is deliberate and differs from
:func:`names.distinctive_token_agreement`, which is symmetric (the shorter
residue must be covered by the longer): that rule is right for "are these
two records the same company?" but wrong for ranking, where ":-) INVEST AS"
would pass against "Metastar Invest" because {invest} ⊆ {metastar, invest}.

Ties within a tier are broken, in order, by register status as the source's
own summary line states it (live or unstated → in a terminal process →
dissolved), by whether the hit carries an LEI, by whole-name similarity, and
finally by the original order (registry order, then the source's own order),
so the sort is stable and deterministic.

Status is read from the hit's ``summary`` segments only, against a short
vocabulary of the labels adapters actually print there (Companies House
``company_status`` codes, Brreg's "in liquidation"/"bankrupt", GLEIF
``entity.status``). An unrecognised or absent status ranks with live — the
:mod:`opencheck.bods.liveness` rule that absence of a status is not a
finding, and an unclassified status must not be guessed either way.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from . import names
from .names import _distinctive_tokens, _token_agrees

#: Match-tier labels, in rank order. Exposed on MCP candidates as ``match``.
MATCH_TIERS: tuple[str, ...] = (
    "exact",
    "same_name",
    "all_tokens",
    "distinctive_tokens",
    "fuzzy",
)

#: Summary segments that mean the register no longer treats the entity as a
#: legal person. Whole-segment, case-insensitive matches only — "inactive"
#: must never be found inside "active", nor vice versa.
_TERMINAL_STATUS = frozenset(
    {
        "dissolved",
        "struck off",
        "removed",
        "closed",
        "converted-closed",
        "converted closed",
        "cancelled",
        "deregistered",
        "inactive",
        "ceased",
        "liquidated",
        "retired",
        "merged",
    }
)

#: Summary segments for a terminal process that is under way but not complete.
_PENDING_STATUS = frozenset(
    {
        "liquidation",
        "in liquidation",
        "bankrupt",
        "receivership",
        "administration",
        "insolvency-proceedings",
        "insolvency proceedings",
        "voluntary-arrangement",
        "voluntary arrangement",
    }
)


def status_rank(summary: str | None) -> int:
    """0 = live or unstated, 1 = in a terminal process, 2 = dissolved.

    Reads the `` · ``-separated segments of a hit's summary. A terminal
    segment wins over a pending one when both appear."""
    segments = {
        " ".join(seg.split()).lower()
        for seg in str(summary or "").split("·")
    }
    if segments & _TERMINAL_STATUS:
        return 2
    if segments & _PENDING_STATUS:
        return 1
    return 0


def _query_tokens(query: str) -> list[str]:
    """The query's own tokens with legal forms removed — the query "BP plc"
    asks for BP, and must not require "plc" in the candidate. Falls back to
    the plain normalised tokens when the query is all legal form."""
    residue = names.org_name_residue(query)
    return (residue or names.normalise_name(query)).split()


def match_tier(query: str, name: str, *, person: bool = False) -> int:
    """Index into :data:`MATCH_TIERS` for one candidate name."""
    if person:
        qa, qb = names.person_name_key(query), names.person_name_key(name)
        if not qa or not qb:
            return len(MATCH_TIERS) - 1
        if qa == qb or sorted(qa.split()) == sorted(qb.split()):
            return 0
        if set(qa.split()) <= set(qb.split()):
            return 2
        return len(MATCH_TIERS) - 1

    qc, nc = names.org_comparable_name(query), names.org_comparable_name(name)
    if not qc or not nc:
        return len(MATCH_TIERS) - 1
    if qc == nc or names.despace(qc) == names.despace(nc):
        return 0

    qr, nr = names.org_name_residue(query), names.org_name_residue(name)
    if qr and nr and (qr == nr or names.despace(qr) == names.despace(nr)):
        return 1

    cand_tokens = set(names.normalise_name(name).split()) | set(nr.split())
    q_tokens = _query_tokens(query)
    if q_tokens and all(t in cand_tokens for t in q_tokens):
        return 2

    q_distinct = _distinctive_tokens(qr) if qr else []
    if q_distinct:
        cand_list = sorted(cand_tokens | set(_distinctive_tokens(nr)))
        discriminators_ok = all(
            t in cand_tokens for t in q_distinct if len(t) == 1 or t.isdigit()
        )
        if discriminators_ok and all(_token_agrees(t, cand_list) for t in q_distinct):
            return 3

    return len(MATCH_TIERS) - 1


@dataclass(frozen=True)
class RankKey:
    """Sort key for one hit — exposed so tests can pin each component."""

    stub: int
    tier: int
    status: int
    no_lei: int
    neg_similarity: float
    position: int


def _has_lei(hit: Any) -> bool:
    return bool(hit.identifiers.get("lei") or hit.source_id == "gleif")


def former_names_of_hit(hit: Any) -> list[str]:
    """The former legal names a search hit's own payload carries (Phase 309):
    GLEIF's ``otherNames`` of type ``PREVIOUS_LEGAL_NAME`` (the hit's ``raw``
    is the ``lei-records`` item), or a register's ``previous_company_names``
    / ``former_names`` list when its raw payload has one. Trading names and
    translations are never former. ``[]`` for a person hit or a raw payload
    with no such list."""
    raw = getattr(hit, "raw", None)
    if not isinstance(raw, dict):
        return []
    out: list[str] = []
    entity = ((raw.get("attributes") or {}).get("entity") or {}) if "attributes" in raw else raw.get("entity") or {}
    for other in (entity.get("otherNames") or []) if isinstance(entity, dict) else []:
        if isinstance(other, dict) and str(other.get("type") or "").upper() == "PREVIOUS_LEGAL_NAME":
            name = " ".join(str(other.get("name") or "").split())
            if name:
                out.append(name)
    for key in ("previous_company_names", "former_names"):
        for prev in raw.get(key) or []:
            value = prev.get("name") if isinstance(prev, dict) else prev
            name = " ".join(str(value or "").split())
            if name:
                out.append(name)
    seen: set[str] = set()
    unique: list[str] = []
    for name in out:
        key = names.display_name_key(name)
        if key and key not in seen:
            seen.add(key)
            unique.append(name)
    return unique


def best_match(query: str, hit: Any, *, person: bool = False) -> tuple[int, str | None]:
    """The best tier across the hit's name and its former names (Phase 309),
    with the former name that won — ``None`` when the current name matched
    at least as well. The Barrick case: "Barrick Gold Corporation" reaches
    BARRICK MINING CORPORATION only through its PREVIOUS_LEGAL_NAME, and the
    row must say so rather than show a fuzzy match."""
    tier = match_tier(query, hit.name, person=person)
    matched: str | None = None
    if not person:
        for former in former_names_of_hit(hit):
            candidate = match_tier(query, former, person=False)
            if candidate < tier:
                tier, matched = candidate, former
    return tier, matched


def rank_key(query: str, hit: Any, position: int, *, person: bool = False) -> RankKey:
    tier, matched = best_match(query, hit, person=person)
    compared = matched or hit.name
    return RankKey(
        stub=1 if getattr(hit, "is_stub", False) else 0,
        tier=tier,
        status=status_rank(hit.summary),
        no_lei=0 if _has_lei(hit) else 1,
        neg_similarity=-round(names.name_similarity(query, compared), 4),
        position=position,
    )


def rank_hits(query: str, hits: Iterable[Any], *, person: bool = False) -> list[Any]:
    """Return ``hits`` ordered best-first across sources. Stubs (placeholder
    rows from an adapter without live access) always sink to the bottom."""
    indexed = list(enumerate(hits))
    keyed = [
        (rank_key(query, h, i, person=person), h) for i, h in indexed
    ]
    keyed.sort(
        key=lambda kh: (
            kh[0].stub,
            kh[0].tier,
            kh[0].status,
            kh[0].no_lei,
            kh[0].neg_similarity,
            kh[0].position,
        )
    )
    return [h for _, h in keyed]


def match_label(query: str, name: str, *, person: bool = False) -> str:
    return MATCH_TIERS[match_tier(query, name, person=person)]
