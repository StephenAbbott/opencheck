"""Pydantic schema for Pennsylvania Department of State rows (data.pa.gov).

One row shape, dataset ``xvd7-5r2c``: a business × one listed party. SODA
omits a column that is null on a row, so every field but the filing number is
optional; ``extra="allow"`` keeps columns the Department adds later.
"""

from __future__ import annotations

from . import _Base


class PaDosRow(_Base):
    """A ``xvd7-5r2c`` row — one business and one party listed for it."""

    filing_number: str
    business_name_source_: str | None = None
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    typeofbusinessregistration: str | None = None
    creationdate: str | None = None
    party_type: str | None = None
    first_name: str | None = None
    middle_name: str | None = None
    last_name: str | None = None
    shortcountyname: str | None = None


class PaDosBundle(_Base):
    """Top-level shape returned by ``PaDosAdapter.fetch``."""

    key: str
    rows: list[PaDosRow] = []
    legal_name: str | None = None
    data_cut: str | None = None
