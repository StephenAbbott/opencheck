"""Pydantic schema for Corporations Canada (ISED) API responses.

Only the fields the BODS mapper (map_corporations_canada) actually reads are
declared.  Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class CorpCanadaCorporation(_Base):
    """Shape of a single corporation object from the V1 /v1/companies endpoint.

    Note: the API spells the address field ``adresses`` (one 's' missing) —
    this is a documented quirk in the ISED API schema.
    """

    corporationId: str | None = None
    status: str | None = None
    act: str | None = None
    corporationNames: list[dict[str, Any]] = Field(default_factory=list)
    # Documented API typo: "adresses" (not "addresses")
    adresses: list[dict[str, Any]] = Field(default_factory=list)
    businessNumbers: dict[str, Any] | None = None
    activities: list[dict[str, Any]] = Field(default_factory=list)
    annualReturns: list[dict[str, Any]] = Field(default_factory=list)


class CorpCanadaBundle(_Base):
    """Top-level shape returned by CorporationsCanadaAdapter.fetch."""

    corp_id: str  # required — mapper key
    corporation: CorpCanadaCorporation | None = None
    directors: list[dict[str, Any]] = Field(default_factory=list)
    legal_name: str | None = None
