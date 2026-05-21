"""Pydantic schema for Firmenbuch (Austria) API responses.

Only the fields the BODS mapper (map_firmenbuch) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class FBExtract(_Base):
    """Parsed extract from the Firmenbuch SOAP response."""

    name: str | None = None
    fn: str | None = None
    uid: str | None = None
    status: str | None = None
    address: str | None = None
    stamm_kapital: dict[str, Any] = Field(default_factory=dict)
    officers: list[dict[str, Any]] = Field(default_factory=list)
    shareholders: list[dict[str, Any]] = Field(default_factory=list)


class FBBundle(_Base):
    """Top-level shape returned by FirmenbuchAdapter.fetch."""

    fn: str  # required — mapper key (Firmenbuchnummer)
    extract: FBExtract | None = None
    legal_name: str | None = None
    soap_error: str | None = None
