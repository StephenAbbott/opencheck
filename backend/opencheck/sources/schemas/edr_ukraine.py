"""Pydantic schema for Ukraine ЄДР (data.gov.ua) fetch bundles.

Only the fields the BODS mapper (``map_edr_ukraine``) actually reads are
declared; everything else passes through via ``extra="allow"``.

The bundle is assembled by ``EdrUkraineAdapter.fetch`` from six tables of a
local SQLite index built by ``scripts/build_edr_ukraine_index.py``. The rows
are already parsed into columns by the builder, so unlike most adapters here
there is no upstream API whose shape can drift — what this schema guards is
the *index* staying in step with the adapter after a rebuild.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class EdrUkraineBundle(_Base):
    """Top-level shape returned by EdrUkraineAdapter.fetch."""

    # Required — the mapper key. 8 digits, zero-padded.
    edrpou: str
    name: str | None = None
    # One row from the deduplicated ``entity`` table.
    entity: dict[str, Any] | None = None
    # Repeating elements, each already split into columns by the builder.
    beneficiaries: list[dict[str, Any]] = []
    founders: list[dict[str, Any]] = []
    signers: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    # The executive authority a state enterprise belongs to, or the state's
    # holding where it is at least 25 % — at most one row per entity.
    executive_power: dict[str, Any] | None = None
    legal_name: str | None = None
    link: str | None = None
