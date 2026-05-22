"""Pydantic schema for ACRA Singapore responses.

Only the fields the BODS mapper (map_acra_singapore) actually reads are
declared.  Everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from . import _Base


class ACRABundle(_Base):
    """Top-level shape returned by AcraSingaporeAdapter.fetch."""

    uen: str  # required — mapper key (may be "" for stubs)
    entity_name: str | None = None
    issuance_agency_desc: str | None = None
    uen_status_desc: str | None = None
    entity_type_desc: str | None = None
    uen_issue_date: str | None = None  # YYYY-MM-DD
    reg_street_name: str | None = None
    reg_postal_code: str | None = None
    link: str | None = None
