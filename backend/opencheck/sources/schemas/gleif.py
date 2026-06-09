"""Pydantic schema for GLEIF API responses.

Only the fields the BODS mapper (map_gleif) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class GLEIFLegalAddress(_Base):
    addressLines: list[str] = Field(default_factory=list)
    city: str | None = None
    region: str | None = None
    country: str | None = None
    postalCode: str | None = None


class GLEIFRegistrationAuthority(_Base):
    registrationAuthorityID: str | None = None
    registrationAuthorityEntityID: str | None = None


class GLEIFRegisteredAt(_Base):
    id: str | None = None


class GLEIFExpiration(_Base):
    date: str | None = None
    reason: str | None = None


class GLEIFEntity(_Base):
    legalName: dict[str, Any] = Field(default_factory=dict)
    legalAddress: GLEIFLegalAddress | None = None
    headquartersAddress: GLEIFLegalAddress | None = None
    registeredAt: GLEIFRegisteredAt | None = None
    jurisdiction: str | None = None
    legalForm: dict[str, Any] = Field(default_factory=dict)
    status: str | None = None
    creationDate: str | None = None          # → foundingDate in BODS
    expiration: GLEIFExpiration | None = None  # .date → dissolutionDate in BODS


class GLEIFAttributes(_Base):
    lei: str | None = None  # present in parent records
    entity: GLEIFEntity | None = None
    registration: dict[str, Any] = Field(default_factory=dict)


class GLEIFRecord(_Base):
    """Shape of the ``record`` field in the GLEIF bundle (GLEIF Level 1 CDF)."""

    id: str | None = None  # LEI code (used as fallback)
    attributes: GLEIFAttributes | None = None


class GLEIFRelationship(_Base):
    """Shape of direct_parent / ultimate_parent entries (GLEIF Level 2 RR)."""

    id: str | None = None
    attributes: GLEIFAttributes | None = None


class GLEIFException(_Base):
    """Shape of direct/ultimate parent reporting exceptions."""

    id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class GLEIFBundle(_Base):
    """Top-level shape returned by GleifAdapter.fetch."""

    lei: str  # required — mapper key
    record: GLEIFRecord | None = None
    direct_parent: GLEIFRelationship | None = None
    ultimate_parent: GLEIFRelationship | None = None
    direct_parent_exception: GLEIFException | None = None
    ultimate_parent_exception: GLEIFException | None = None
    # First page of direct subsidiaries (≤ 100 records) + GLEIF total count.
    direct_children: list[Any] = Field(default_factory=list)
    direct_children_total: int = 0
