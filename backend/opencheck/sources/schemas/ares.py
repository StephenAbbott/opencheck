"""Pydantic schema for Czech ARES API responses.

Only the fields the BODS mapper (map_ares) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class AresEntity(_Base):
    """Flattened entity data from ARES /ekonomicke-subjekty/{ico}."""

    ico: str | None = None
    obchodniJmeno: str | None = None
    sidlo: dict[str, Any] = Field(default_factory=dict)
    pravniForma: str | None = None
    datumVzniku: str | None = None
    datumZaniku: str | None = None


class AresPersonOrEntity(_Base):
    """A person or entity entry in owners/directors list."""

    type: str | None = None  # "person" or "entity"
    name: str | None = None
    given_name: str | None = None
    family_name: str | None = None
    birth_date: str | None = None
    nationality: str | None = None
    address: str | None = None
    ico: str | None = None
    country: str | None = None
    role: str | None = None
    role_label: str | None = None
    stake_percent: float | None = None
    start_date: str | None = None


class AresBundle(_Base):
    """Top-level shape returned by AresAdapter.fetch."""

    cz_ico: str  # required — mapper key
    name: str | None = None
    entity: AresEntity | None = None
    owners: list[dict[str, Any]] = Field(default_factory=list)
    directors: list[dict[str, Any]] = Field(default_factory=list)
