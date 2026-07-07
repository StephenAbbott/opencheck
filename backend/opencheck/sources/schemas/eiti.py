"""Pydantic schema for the EITI adapter bundle.

The bundle combines two data layers:

* Organisation matches from the committed artifact
  ``opencheck/data/eiti_organisations.json.gz`` (built by
  ``scripts/build_eiti_index.py`` from ``/api/v2.0/organisation``).
* Live payment rows from ``/api/v2.0/revenue?organisation={id}`` — the
  only server-side filter on the EITI API verified to work.

Only fields the BODS mapper and the frontend card read are declared;
everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class EitiOrganisation(_Base):
    """One organisation record (one company in one reporting year)."""

    id: str
    year: str | None = None
    label: str | None = None


class EitiRevenueRow(_Base):
    """One payment row from /api/v2.0/revenue."""

    label: str | None = None
    revenue: float | None = None
    currency: str | None = None
    gfs_label: str | None = None
    gfs_code: str | None = None


class EitiRevenueYear(_Base):
    """Aggregated payments for one reporting year."""

    year: str | None = None
    organisation_id: str
    total_usd: float = 0.0
    rows: list[EitiRevenueRow] = Field(default_factory=list)


class EitiBundle(_Base):
    """Top-level shape returned by EitiAdapter.fetch_by_registration/fetch."""

    country: str  # ISO 3166-1 alpha-2, upper
    identification: str
    entity_name: str | None = None
    organisations: list[EitiOrganisation] = Field(default_factory=list)
    revenue_years: list[EitiRevenueYear] = Field(default_factory=list)
    streams: dict[str, float] = Field(default_factory=dict)
    total_usd: float = 0.0
    years: list[str] = Field(default_factory=list)
