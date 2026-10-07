"""Pydantic schema for the Zambia EITI portal adapter bundle.

The bundle is assembled by ``EitiZambiaAdapter`` from the committed, LEI-keyed
index ``opencheck/data/eiti_zambia_index.json.gz`` (built by
``scripts/build_eiti_zambia_index.py``). Only the fields the mapper, hit
builder and card read are declared; the rest passes through.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class EitiZambiaMatch(_Base):
    """How the portal company was resolved to this LEI at build time."""

    #: "tpin_via_name" (name match found the TPIN; every table joined on it)
    #: | "name_only" (the company appears only in the name-keyed tables).
    method: str
    confidence: str = "medium"
    aliases: list[str] = Field(default_factory=list)


class EitiZambiaTaxYear(_Base):
    year: str
    dataset: str
    payments: int
    amounts_summed: bool
    total_zmw: float | None = None


class EitiZambiaReconciliationYear(_Base):
    year: str
    total_zmw: float
    total_usd: float


class EitiZambiaLicence(_Base):
    code: str
    type: str | None = None
    status: str | None = None


class EitiZambiaBundle(_Base):
    """Top-level shape returned by EitiZambiaAdapter.fetch_by_lei / fetch."""

    lei: str
    gleif_legal_name: str
    tpins: list[str] = Field(default_factory=list)
    names_as_filed: list[str] = Field(default_factory=list)
    match: EitiZambiaMatch
    zra_tax: list[EitiZambiaTaxYear] = Field(default_factory=list)
    eiti_reconciliation: list[EitiZambiaReconciliationYear] = Field(default_factory=list)
    employment: list[dict] = Field(default_factory=list)
    licences: list[EitiZambiaLicence] = Field(default_factory=list)
    water_offences: list[dict] = Field(default_factory=list)
    identifiers: dict[str, str] = Field(default_factory=dict)
    datasets: dict[str, dict] = Field(default_factory=dict)
