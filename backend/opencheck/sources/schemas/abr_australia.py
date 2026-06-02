"""Pydantic schema for Australian Business Register (ABN Lookup) bundles.

Only the fields the BODS mapper (``map_abr_australia``) reads are declared;
everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class ABRBundle(_Base):
    """Top-level shape returned by AbrAustraliaAdapter.fetch."""

    # At least one of abn/acn identifies the entity; name is the display value.
    abn: str
    acn: str | None = None
    name: str | None = None
    entity_type_code: str | None = None
    entity_type_name: str | None = None
    abn_status: str | None = None
    abn_status_from: str | None = None
    state: str | None = None
    postcode: str | None = None
    gst: Any | None = None
    business_names: list[str] = []
    link: str | None = None
