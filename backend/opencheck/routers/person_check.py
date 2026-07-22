"""BackgroundCheck — on-demand person screening endpoint.

SPIKE (feat/background-check): first slice of the BackgroundCheck
feature — surfacing risk checks on the *people* connected to an entity
(officers, PSCs, beneficial owners) rather than the entity itself.

``GET /person-check?name=<name>&birth_year=<yyyy>`` fans a person query
out across every person-capable adapter in the registry (Companies
House officers, OpenSanctions, EveryPolitician, Wikidata, OpenAleph, …)
and returns:

* every hit, scored against the queried name (and birth year where both
  sides carry one) so the UI can distinguish strong matches from
  same-name noise;
* deterministic risk signals (PEP / SANCTIONED / …) derived **only from
  strong matches** — a weak fuzzy match must never put a risk chip next
  to a real person's name;
* per-source attribution, licence and error records, so "no hit" can be
  presented honestly ("checked, nothing found in these sources") and a
  failed source is never silently dropped.

Evidence discipline
-------------------

Screening here is *name-based*: unlike the entity lookup there is no
LEI-grade identifier bridging people across sources (UK PSC/officer
records expose no stable public person identifier). Every signal this
endpoint emits therefore carries a ``match`` block in its evidence —
the queried name, the similarity score, and whether a birth year
corroborated the match — and the response carries explicit caveats.
The same wording rules apply as for entity risk signals: signals are
rule-derived from source records, never inferred.

Scoring reuses the cross-check module's helpers (`_name_score`,
`_birth_year_compatible`) and its 0.88 similarity threshold so a
"strong match" means the same thing here as it does for the
RELATED_PEP / RELATED_SANCTIONED signals on the entity report.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel

from ..cross_check import _birth_year_compatible, _name_score
from ..ratelimit import limiter, lookup_tier
from ..risk import assess_hits
from ..sources import REGISTRY, SearchKind, SourceHit
from .search import _run_adapters

router = APIRouter()

#: Same threshold as ``cross_check.assess_cross_source_names`` — keep the
#: two in lockstep so "strong match" is one concept product-wide.
STRONG_MATCH_THRESHOLD = 0.88

_CAVEATS = [
    (
        "Person screening is name-based. A name match alone does not "
        "establish that records describe the same individual — verify with "
        "additional identifiers (date of birth, nationality, known roles) "
        "before drawing conclusions."
    ),
    (
        "No match found does not mean no risk: coverage varies by source "
        "and jurisdiction, and absence from these sources is not proof of "
        "absence."
    ),
]


class PersonMatch(BaseModel):
    """One hit, scored against the queried person."""

    hit: SourceHit
    name_score: float
    birth_year_compatible: bool
    strong: bool


class CheckedSource(BaseModel):
    """Per-source outcome record — powers honest 'what was checked' UI."""

    source_id: str
    name: str
    license: str
    attribution: str
    homepage: str
    live: bool
    hit_count: int
    error: str | None = None


class PersonCheckResponse(BaseModel):
    query: str
    birth_year: int | None
    matches: list[PersonMatch]
    risk_signals: list[dict[str, Any]]
    weak_match_count: int
    sources: list[CheckedSource]
    caveats: list[str]


def _score_hit(
    hit: SourceHit, query: str, birth_year: int | None
) -> PersonMatch:
    score = _name_score(query, hit.name or "")
    by_ok = _birth_year_compatible(birth_year, hit)
    return PersonMatch(
        hit=hit,
        name_score=round(score, 3),
        birth_year_compatible=by_ok,
        strong=score >= STRONG_MATCH_THRESHOLD and by_ok,
    )


async def _person_check_impl(
    name: str, birth_year: int | None
) -> PersonCheckResponse:
    """Body of ``/person-check`` — callable in-process (future MCP tool)."""
    results, errors = await _run_adapters(name, SearchKind.PERSON)

    matches: list[PersonMatch] = []
    for adapter_hits in results.values():
        for hit in adapter_hits:
            matches.append(_score_hit(hit, name, birth_year))
    # Strong matches first, then by descending similarity — the UI reads
    # top-to-bottom as "most likely the same person" downwards.
    matches.sort(key=lambda m: (not m.strong, -m.name_score))

    strong_hits = [m.hit for m in matches if m.strong]
    score_by_key = {
        (m.hit.source_id, m.hit.hit_id): m for m in matches if m.strong
    }
    signals = []
    for signal in assess_hits(strong_hits):
        payload = signal.to_dict()
        match = score_by_key.get((signal.source_id, signal.hit_id))
        if match is not None:
            # Every person-level claim must carry its matching evidence.
            payload["evidence"] = dict(payload.get("evidence") or {})
            payload["evidence"]["match"] = {
                "query_name": name,
                "name_score": match.name_score,
                "birth_year_checked": birth_year is not None,
                "birth_year_compatible": match.birth_year_compatible,
            }
        signals.append(payload)

    sources: list[CheckedSource] = []
    for source_id, adapter in REGISTRY.items():
        info = adapter.info
        if SearchKind.PERSON not in info.supports:
            continue
        sources.append(
            CheckedSource(
                source_id=source_id,
                name=info.name,
                license=info.license,
                attribution=info.attribution,
                homepage=info.homepage,
                live=info.live_available,
                hit_count=len(results.get(source_id, [])),
                error=errors.get(source_id),
            )
        )

    return PersonCheckResponse(
        query=name,
        birth_year=birth_year,
        matches=matches,
        risk_signals=signals,
        weak_match_count=sum(1 for m in matches if not m.strong),
        sources=sources,
        caveats=list(_CAVEATS),
    )


@router.get("/person-check", response_model=PersonCheckResponse)
@limiter.limit(lookup_tier)
async def person_check(
    request: Request,
    response: Response,
    name: str = Query(..., min_length=2, description="Person name to screen."),
    birth_year: int | None = Query(
        None,
        ge=1900,
        le=2026,
        description=(
            "Known birth year of the person, used to corroborate name "
            "matches where the source record carries a date of birth."
        ),
    ),
) -> PersonCheckResponse:
    """Screen one person across every person-capable source."""
    return await _person_check_impl(name=name, birth_year=birth_year)
