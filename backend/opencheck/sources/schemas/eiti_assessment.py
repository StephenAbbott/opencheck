"""Pydantic schema for the EITI Company Assessment adapter bundle.

The bundle is assembled by ``EitiAssessmentAdapter.fetch_by_lei`` / ``fetch``
from the committed, LEI-keyed index
``opencheck/data/eiti_assessment_index.json.gz`` (built by
``scripts/build_eiti_assessment_index.py``). There is no live path: the EITI
global database is queried once, offline, at index-build time.

Only fields the BODS mapper, the findings template and the frontend card read
are declared; everything else passes through via ``extra="allow"`` on ``_Base``.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class EitiExpectation(_Base):
    """One of the nine EITI supporting-company expectations, for one year.

    ``result`` is EITI's own wording — "Expectation met", "Expectation
    partially met", "Expectation not met", "Not applicable", "Not available".
    It is passed through verbatim rather than mapped to a boolean: "not
    available" means EITI did not assess, which is a different thing from a
    company failing, and collapsing the two would state something untrue.
    """

    label: str | None = None
    result: str | None = None
    response: str | None = None
    url: str | None = None
    comment: str | None = None
    #: exp_6 (beneficial ownership disclosure) only.
    bo_disclosure: str | None = None
    bo_url: str | None = None
    bo_disclosure_url: str | None = None
    stock_exchange: str | None = None
    stock_url: str | None = None


class EitiDeclaredSubsidiary(_Base):
    """One subsidiary the company declared to EITI under expectation 2.

    A **name and a country of operation, and nothing else** — EITI publishes no
    identifier for these, no ownership percentage and no share class. That is
    why this model has no identifier field to fill in later by accident, and
    why ``map_eiti_assessment`` emits no relationship statements.
    """

    name: str
    country: str | None = None
    #: EITI report years in which the company declared this subsidiary.
    years: list[str] = Field(default_factory=list)
    source: str | None = None


class EitiPublishedIdentifiers(_Base):
    """Identifiers EITI itself publishes for the supporting company.

    Recorded for display and provenance. All three are near-empty upstream
    (``legal_entity_id`` is populated for 3 companies of 10,116), and the
    adapter asserts none of them — see ``EitiAssessmentAdapter``.
    """

    open_corporates_id: str | None = None
    legal_entity_id: str | None = None
    estma_id: str | None = None


class EitiAssessmentMatch(_Base):
    """How OpenCheck resolved this company to the LEI it is keyed on.

    ``reviewed`` is always True in a committed index: the builder refuses to
    write one while any row is unreviewed. It is carried into the bundle so the
    claim travels with the data rather than living only in a build log.
    """

    method: str | None = None
    reviewed: bool = False
    #: The GLEIF legal name of the matched record. Shown alongside the EITI
    #: name whenever the two differ — several supporting companies have no LEI
    #: of their own and are anchored on the only LEI-bearing entity in the
    #: group (Chevron Corporation → Chevron U.S.A. Inc.), and a card that
    #: showed one name silently as the other would misrepresent both.
    gleif_legal_name: str | None = None


class EitiAssessmentBundle(_Base):
    """Top-level shape returned by EitiAssessmentAdapter.fetch_by_lei / fetch."""

    lei: str
    name: str
    hq_country: str | None = None
    hq_city: str | None = None
    sectors: list[str] = Field(default_factory=list)
    company_type: str | None = None
    business_activity: str | None = None
    eiti_published: EitiPublishedIdentifiers | None = None
    match: EitiAssessmentMatch | None = None
    #: assessment year -> expectation shorthand ("exp_1" … "exp_9") -> fields.
    assessments: dict[str, dict[str, EitiExpectation]] = Field(default_factory=dict)
    subsidiaries: list[EitiDeclaredSubsidiary] = Field(default_factory=list)
    #: Upstream extract date of the committed index (``meta.source_harvest``).
    source_snapshot: str | None = None
