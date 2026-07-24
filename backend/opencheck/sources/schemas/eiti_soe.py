"""Pydantic schema for the EITI SOE (State-Owned Enterprises) adapter bundle.

The bundle is assembled by ``EitiSoeAdapter.fetch_by_lei`` / ``fetch`` from the
committed, LEI-keyed index ``opencheck/data/eiti_soe_index.json.gz`` (built by
``scripts/build_eiti_soe_index.py``), plus optional live payment rows from the
SOE Datasette API when ``allow_live`` is on.

Only fields the BODS mapper and the frontend card read are declared; everything
else passes through via ``extra="allow"`` on ``_Base``.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class EitiSoePaymentRow(_Base):
    """One payment/context row for an SOE (live Datasette enrichment)."""

    year: str | int | None = None
    revenue_stream: str | None = None
    revenue_value: float | str | None = None
    currency: str | None = None
    project: str | None = None


class EitiSoeBundle(_Base):
    """Top-level shape returned by EitiSoeAdapter.fetch_by_lei / fetch."""

    lei: str
    entity_name: str | None = None
    is_state_owned: bool = True
    country: str | None = None
    sector: str | None = None
    commodities: list[str] = Field(default_factory=list)
    company_type: str | None = None
    government_entity: str | None = None
    opencorporates_id: str | None = None
    eiti_id_company: str | None = None
    eiti_id_government: str | None = None
    audited_financial_statement: str | None = None
    public_listing_or_website: str | None = None
    years: list[str] = Field(default_factory=list)
    #: How the SOE was resolved to this LEI: "opencorporates_id" | "name_country".
    match_method: str | None = None
    #: "high" | "medium" | "low" — drives the signal's confidence dot.
    match_confidence: str = "medium"
    payments: list[EitiSoePaymentRow] = Field(default_factory=list)
