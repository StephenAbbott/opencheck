"""Pydantic schema for OpenCorporates API responses.

Only the fields the BODS mapper (map_opencorporates) actually reads are
declared.  Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class OCCompany(_Base):
    name: str | None = None
    jurisdiction_code: str | None = None
    company_number: str | None = None
    incorporation_date: str | None = None
    dissolution_date: str | None = None
    company_type: str | None = None
    company_status: str | None = None
    opencorporates_url: str | None = None
    registered_address: dict[str, Any] = Field(default_factory=dict)
    officers: list[dict[str, Any]] = Field(default_factory=list)


class OCOfficer(_Base):
    name: str | None = None
    position: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    inactive: bool | None = None
    opencorporates_url: str | None = None


class OCBundle(_Base):
    """Top-level shape returned by OpenCorporatesAdapter.fetch."""

    ocid: str  # required — mapper key (jurisdiction/number)
    company: OCCompany | None = None
    officers: list[dict[str, Any]] = Field(default_factory=list)
    network: dict[str, Any] | None = None
