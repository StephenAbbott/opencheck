"""Pydantic schema for the ESTMA adapter bundle.

The bundle is assembled by ``EstmaAdapter`` from the SQLite index built by
``scripts/build_estma_index.py`` and the committed crosswalk
``opencheck/data/estma_lei_crosswalk.csv``. Only the fields the mapper, hit
builder and card read are declared; the rest passes through.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class EstmaMatch(_Base):
    """How the ESTMA entity was tied to this LEI (the reviewed crosswalk)."""

    method: str
    basis: str
    gleif_name: str = ""
    note: str = ""


class EstmaIncludedIn(_Base):
    estma_id: str
    name: str | None = None


class EstmaReport(_Base):
    """One filed report. A report included in another entity's carries
    ``included_in`` and no amounts."""

    year: str
    period_start: str | None = None
    period_end: str | None = None
    report_type: str | None = None
    report_type_label: str | None = None
    url: str | None = None
    included_in: EstmaIncludedIn | None = None
    payee_total_cad: float | None = None
    payee_count: int | None = None
    project_count: int | None = None
    by_payment_type: list[dict] = Field(default_factory=list)
    by_country: list[dict] = Field(default_factory=list)
    payees: list[dict] = Field(default_factory=list)
    projects: list[dict] = Field(default_factory=list)


class EstmaEntity(_Base):
    estma_id: str
    name: str
    reports_payments: bool
    reports: list[EstmaReport] = Field(default_factory=list)
    includes: dict | None = None
    match: EstmaMatch


class EstmaBundle(_Base):
    """Top-level shape returned by EstmaAdapter.fetch_by_lei / fetch."""

    lei: str
    gleif_legal_name: str = ""
    entities: list[EstmaEntity] = Field(min_length=1)
    latest_year: str | None = None
    identifiers: dict[str, str] = Field(default_factory=dict)
    cut: str | None = None
