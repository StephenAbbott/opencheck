"""Pydantic schema for SEC EDGAR API responses.

Only the fields the BODS mapper (map_sec_edgar) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class EDGARReporter(_Base):
    reporter_cik: str | None = None
    name: str | None = None
    type_code: str | None = None
    percent_of_class: float | str | None = None  # float from _float(); str in some variants
    citizenship: str | None = None
    address: dict[str, Any] = Field(default_factory=dict)


class EDGARIssuer(_Base):
    name: str | None = None
    cik: str | None = None
    cusip: str | None = None


class EDGARFiling(_Base):
    reporter: EDGARReporter | None = None
    issuer: EDGARIssuer | None = None
    filer_cik: str | None = None
    filing_url: str | None = None
    form_type: str | None = None
    filed: str | None = None
    source_url: str | None = None


class EDGARBundle(_Base):
    """Top-level shape returned by SecEdgarAdapter.fetch."""

    issuer_cik: str  # required — mapper key
    filings: list[EDGARFiling] = Field(default_factory=list)
