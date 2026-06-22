"""Pydantic schema for KvK (Netherlands Chamber of Commerce) API responses.

Only the fields the BODS mapper (map_kvk) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class KvKCompany(_Base):
    """Company data from the KvK basisbedrijfsgegevens endpoint."""

    kvkNummer: str | None = None
    naam: str | None = None
    datumAanvang: str | None = None  # incorporation date (YYYYMMDD)
    datumEinde: str | None = None
    rechtsvorm: str | None = None
    statutaireNaam: str | None = None
    activiteiten: list[dict[str, Any]] = Field(default_factory=list)
    adressen: list[dict[str, Any]] = Field(default_factory=list)


class KvKBundle(_Base):
    """Top-level shape returned by KvKAdapter.fetch."""

    kvk_number: str  # required — mapper key
    company: KvKCompany | None = None
    legal_name: str | None = None
    # Set when the KvK number is absent from the open-data set (HTTP 404).
    # See KvKAdapter._COVERAGE_404 for the wording and the reason.
    coverage_note: str | None = None
