"""Pydantic schema for Cyprus DRCOR (data.gov.cy) fetch bundles.

Only the fields the BODS mapper (``map_cyprus_drcor``) actually reads are
declared.  Everything else passes through via ``extra="allow"``.

The bundle is assembled by ``CyprusDrcorAdapter.fetch`` from up to three
DKAN datastore queries (organisation, registered office, officials).
"""

from __future__ import annotations

from typing import Any

from . import _Base


class CyprusBundle(_Base):
    """Top-level shape returned by CyprusDrcorAdapter.fetch."""

    # Required — mapper key (may be "" for stubs).
    reg_no: str
    # Convenience display name surfaced to the lookup result handlers.
    name: str | None = None
    # Raw DKAN rows (column names vary; mapper reads them defensively).
    organisation: dict[str, Any] | None = None
    address: dict[str, Any] | None = None
    officials: list[dict[str, Any]] = []
    legal_name: str | None = None
    link: str | None = None
