"""Pydantic schema for BCE Belgium (Crossroads Bank for Enterprises) responses.

Only the fields the BODS mapper (map_bce_belgium) actually reads are declared.
Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from . import _Base


class BCEBundle(_Base):
    """Top-level shape returned by BCEBelgiumAdapter.fetch."""

    enterprise_number: str  # required — mapper key
    dotted: str | None = None
    name: str | None = None
    name_nl: str | None = None
    name_fr: str | None = None
    name_de: str | None = None
    status: str | None = None
    juridical_form: str | None = None
    start_date: str | None = None
    address: str | None = None
    link: str | None = None
