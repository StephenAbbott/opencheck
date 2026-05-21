"""Pydantic schema for Brønnøysund Register Centre (Norway) API responses.

Only the fields the BODS mapper (map_brreg) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class BrregEntity(_Base):
    """Shape of the Brreg /enheter/{orgnr} response."""

    organisasjonsnummer: str | None = None
    navn: str | None = None
    organisasjonsform: dict[str, Any] = Field(default_factory=dict)
    registreringsdatoEnhetsregisteret: str | None = None
    stiftelsesdato: str | None = None
    forretningsadresse: dict[str, Any] = Field(default_factory=dict)
    postadresse: dict[str, Any] = Field(default_factory=dict)


class BrregBundle(_Base):
    """Top-level shape returned by BrregAdapter.fetch."""

    orgnr: str  # required — mapper key
    entity: BrregEntity | None = None
    roles: list[dict[str, Any]] = Field(default_factory=list)
    legal_name: str | None = None
