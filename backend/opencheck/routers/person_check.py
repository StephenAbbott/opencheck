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

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from ..bods import map_companies_house
from ..cross_check import _birth_year_compatible, _name_score
from ..ratelimit import limiter, lookup_tier
from ..reconcile import reconcile
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
    (
        "PEP coverage (EveryPolitician / OpenSanctions) is stronger for "
        "well-digitised polities than for smaller or less-documented "
        "national and regional legislatures — a non-hit is not proof of "
        "non-PEP status."
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
    #: Identifier-backed links between STRONG matches only (shared
    #: wikidata_qid / opensanctions_id / …) — the person-world version of
    #: the entity report's cross-source panel. Weak matches are excluded:
    #: bridging two below-threshold hits would manufacture a same-person
    #: claim the name evidence doesn't support.
    cross_source_links: list[dict[str, Any]] = []


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
        cross_source_links=[link.to_dict() for link in reconcile(strong_hits)],
    )


class AppointmentItem(BaseModel):
    """One Companies House appointment held by an officer."""

    company_name: str
    company_number: str | None = None
    company_status: str | None = None
    role: str | None = None
    appointed_on: str | None = None
    resigned_on: str | None = None


class PersonAppointmentsResponse(BaseModel):
    """A person's appointments across companies (Phase C enrichment).

    This is the identifier-backed view: unlike the name-based screen,
    everything here hangs off one stable Companies House officer id, so
    "the same person across companies" is the register's own assertion,
    not a name match.
    """

    officer_id: str
    name: str | None
    birth_date: str | None
    is_stub: bool
    total_results: int | None
    active_count: int
    appointments: list[AppointmentItem]
    #: BODS statements for the officer + appointments (personStatement
    #: carries the GB-COH-OFFICER identifier) — traceable evidence.
    bods: list[dict[str, Any]]
    attribution: str
    caveat: str


_APPOINTMENTS_CAVEAT = (
    "Appointments are as recorded by Companies House for this officer "
    "identifier. The same individual may hold further appointments under "
    "other officer identifiers — Companies House does not guarantee one "
    "identifier per person."
)


async def _person_appointments_impl(officer_id: str) -> PersonAppointmentsResponse:
    adapter = REGISTRY.get("companies_house")
    if adapter is None:  # pragma: no cover — registry always has CH
        raise HTTPException(status_code=503, detail="Companies House adapter unavailable")

    bundle = await adapter.fetch(officer_id)
    if bundle.get("is_stub"):
        return PersonAppointmentsResponse(
            officer_id=officer_id,
            name=None,
            birth_date=None,
            is_stub=True,
            total_results=None,
            active_count=0,
            appointments=[],
            bods=[],
            attribution=adapter.info.attribution,
            caveat=_APPOINTMENTS_CAVEAT,
        )
    if "officer_id" not in bundle:
        # The id dispatched to the company path — not an officer id.
        raise HTTPException(
            status_code=404,
            detail="Not a Companies House officer id.",
        )

    envelope = bundle.get("appointments") or {}
    dob = envelope.get("date_of_birth")
    birth_date: str | None = None
    if isinstance(dob, dict) and dob.get("year"):
        birth_date = (
            f"{int(dob['year']):04d}-{int(dob['month']):02d}"
            if dob.get("month")
            else f"{int(dob['year']):04d}"
        )

    items: list[AppointmentItem] = []
    for item in envelope.get("items") or []:
        appointed_to = item.get("appointed_to") or {}
        items.append(
            AppointmentItem(
                company_name=appointed_to.get("company_name") or "Unknown company",
                company_number=appointed_to.get("company_number"),
                company_status=appointed_to.get("company_status"),
                role=item.get("officer_role"),
                appointed_on=item.get("appointed_on"),
                resigned_on=item.get("resigned_on"),
            )
        )

    return PersonAppointmentsResponse(
        officer_id=officer_id,
        name=envelope.get("name"),
        birth_date=birth_date,
        is_stub=False,
        total_results=envelope.get("total_results"),
        active_count=sum(1 for i in items if not i.resigned_on),
        appointments=items,
        bods=list(map_companies_house(bundle)),
        attribution=adapter.info.attribution,
        caveat=_APPOINTMENTS_CAVEAT,
    )


@router.get("/person-appointments", response_model=PersonAppointmentsResponse)
@limiter.limit(lookup_tier)
async def person_appointments(
    request: Request,
    response: Response,
    officer_id: str = Query(
        ...,
        min_length=8,
        description="Companies House officer id (from a /person-check hit).",
    ),
) -> PersonAppointmentsResponse:
    """All Companies House appointments for one officer id."""
    return await _person_appointments_impl(officer_id)


class PositionItem(BaseModel):
    """One political position held, from the EveryPolitician record."""

    label: str
    country: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    #: True when the record carries a start but no end date — held now,
    #: as far as the dataset knows.
    current: bool = False


class PersonPositionsResponse(BaseModel):
    """Positions held by one EveryPolitician / OpenSanctions PEP record.

    Phase D — EveryPolitician as a first-class PEP source: the full
    positions-held history behind a PEP match, with dates and countries,
    rather than just the single-line summary on the search hit.
    """

    entity_id: str
    name: str | None
    is_stub: bool
    positions: list[PositionItem]
    wikidata_qid: str | None
    countries: list[str]
    #: Canonical record URL on OpenSanctions for onward verification.
    source_url: str
    attribution: str
    maintenance_note: str
    caveat: str


_EP_MAINTENANCE_NOTE = (
    "EveryPolitician is maintained by OpenSanctions through the Poliloom "
    "crowdsourcing pipeline; records are keyed to Wikidata Q-IDs and kept "
    "in sync with Wikidata."
)

_EP_COVERAGE_CAVEAT = (
    "Coverage is stronger for well-digitised polities than for smaller or "
    "less-documented national and regional legislatures. Positions listed "
    "here are what the dataset records — not necessarily a complete "
    "political history, and a non-hit is not proof of non-PEP status."
)


def _first(value: Any) -> Any:
    """First element of an FtM property list (or the value itself)."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _parse_positions(entity: dict[str, Any]) -> list[PositionItem]:
    """Positions held, from a yente FtM Person entity.

    Prefers ``positionOccupancies`` (nested Occupancy entities carrying
    the post, its country, and start/end dates); falls back to the plain
    ``position`` string list when occupancies are absent. Defensive
    throughout — FtM property shapes vary with dataset vintage.
    """
    props = entity.get("properties") or {}
    items: list[PositionItem] = []

    for occ in props.get("positionOccupancies") or []:
        if not isinstance(occ, dict):
            continue
        oprops = occ.get("properties") or {}
        post = _first(oprops.get("post"))
        label: str | None = None
        country: str | None = None
        if isinstance(post, dict):
            label = post.get("caption")
            pprops = post.get("properties") or {}
            raw_country = _first(pprops.get("country"))
            if isinstance(raw_country, str):
                country = raw_country.upper()
        elif isinstance(post, str):
            label = post
        start = _first(oprops.get("startDate"))
        end = _first(oprops.get("endDate"))
        items.append(
            PositionItem(
                label=label or "Position (unnamed)",
                country=country,
                start_date=start if isinstance(start, str) else None,
                end_date=end if isinstance(end, str) else None,
                current=bool(start) and not end,
            )
        )

    if not items:
        for pos in props.get("position") or []:
            if isinstance(pos, str):
                items.append(PositionItem(label=pos))

    # Most recent first; undated entries last (two stable passes).
    items.sort(key=lambda p: p.start_date or "", reverse=True)
    items.sort(key=lambda p: p.start_date is None)
    return items


async def _person_positions_impl(entity_id: str) -> PersonPositionsResponse:
    adapter = REGISTRY.get("everypolitician")
    if adapter is None:  # pragma: no cover — registry always has EP
        raise HTTPException(status_code=503, detail="EveryPolitician adapter unavailable")

    bundle = await adapter.fetch(entity_id)
    source_url = f"https://www.opensanctions.org/entities/{entity_id}/"
    if bundle.get("is_stub"):
        return PersonPositionsResponse(
            entity_id=entity_id,
            name=None,
            is_stub=True,
            positions=[],
            wikidata_qid=None,
            countries=[],
            source_url=source_url,
            attribution=adapter.info.attribution,
            maintenance_note=_EP_MAINTENANCE_NOTE,
            caveat=_EP_COVERAGE_CAVEAT,
        )

    entity = bundle.get("entity") or {}
    props = entity.get("properties") or {}
    wikidata = _first(props.get("wikidataId"))
    countries = [
        c.upper() for c in (props.get("country") or []) if isinstance(c, str)
    ]

    return PersonPositionsResponse(
        entity_id=entity_id,
        name=entity.get("caption"),
        is_stub=False,
        positions=_parse_positions(entity),
        wikidata_qid=wikidata if isinstance(wikidata, str) else None,
        countries=countries,
        source_url=source_url,
        attribution=adapter.info.attribution,
        maintenance_note=_EP_MAINTENANCE_NOTE,
        caveat=_EP_COVERAGE_CAVEAT,
    )


@router.get("/person-positions", response_model=PersonPositionsResponse)
@limiter.limit(lookup_tier)
async def person_positions(
    request: Request,
    response: Response,
    entity_id: str = Query(
        ...,
        min_length=4,
        description=(
            "OpenSanctions entity id of the EveryPolitician PEP record "
            "(from a /person-check hit)."
        ),
    ),
) -> PersonPositionsResponse:
    """Positions held for one EveryPolitician / OpenSanctions PEP record."""
    return await _person_positions_impl(entity_id)


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
