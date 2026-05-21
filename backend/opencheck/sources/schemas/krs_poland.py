"""Pydantic schema for KRS Poland API responses.

Only the fields the BODS mapper (map_krs_poland) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class KRSCapital(_Base):
    amount: float | None = None
    currency: str | None = None


class KRSBundle(_Base):
    """Top-level shape returned by KrsPolandAdapter.fetch / _build_bundle."""

    pl_krs: str  # required — mapper key
    name: str | None = None
    nip: str | None = None
    regon: str | None = None
    legal_form: str | None = None
    legal_form_label: str | None = None
    address: Any = None  # str or dict depending on KRS record shape
    email: str | None = None
    website: str | None = None
    registration_date: str | None = None
    last_change_date: str | None = None
    rejestr: str | None = None
    capital: Any = None  # KRSCapital dict or None
    pkd: Any = None  # list[dict] or single dict or None
    directors: list[dict[str, Any]] = Field(default_factory=list)
    supervisory_board: list[dict[str, Any]] = Field(default_factory=list)
    shareholders: list[dict[str, Any]] = Field(default_factory=list)
