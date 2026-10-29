"""Pydantic schema for Cyprus DRCOR (data.gov.cy) fetch bundles.

Only the fields the BODS mapper (``map_cyprus_drcor``) reads are declared;
everything else passes through via ``extra="allow"``. The bundle is
assembled by ``CyprusDrcorAdapter.fetch`` from the local SQLite index built
by ``scripts/build_cyprus_drcor_index.py`` — there is no upstream API whose
shape can drift; what this guards is the index staying in step with the
adapter after a rebuild.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class CyprusOfficial(_Base):
    seq: int = 0
    name: str
    position: str | None = None


class CyprusBundle(_Base):
    """Top-level shape returned by CyprusDrcorAdapter.fetch."""

    # The registration number (digits) and the register's type code — the
    # index key. ``C`` (a company, an HE number) for anything GLEIF files.
    reg_no: str
    type_code: str = "C"
    name: str | None = None
    legal_name: str | None = None
    org_type: str | None = None
    sub_type: str | None = None
    # ISO days, from the register's DD/MM/YYYY.
    registered_on: str | None = None
    status: str | None = None
    status_class: str | None = None
    status_date: str | None = None
    address: dict[str, Any] | None = None
    officials: list[CyprusOfficial] = []
    # The portal's publication day of the files the index was built from.
    cut: str | None = None
    link: str | None = None
    # A number the index does not hold: the note card (``is_stub`` False).
    not_found: bool = False
    coverage_note: str | None = None
