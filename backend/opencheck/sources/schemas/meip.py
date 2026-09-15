"""Pydantic schema for the OECD-UNSD MEIP adapter bundle (Phase 208).

The bundle is assembled by ``MeipAdapter.fetch_by_lei`` / ``fetch`` from the
``meip.sqlite`` store (``opencheck/meip.py``), which holds the OECD's own BODS
v0.4 statements. ``bods_statements`` are those statements exactly as the
OECD published them — ``map_meip`` passes them through — so only the fields
the hit builder and the finding template read are declared here.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class MeipGroup(_Base):
    """The group head a record belongs to (the register's "Parent MNE")."""

    record_id: str
    name: str = ""
    lei: str | None = None
    jurisdiction: str | None = None


class MeipRecord(_Base):
    """One register record carrying the subject LEI. A LEI can sit on more
    than one record — the register lists 113 companies in two groups — and
    every record is returned."""

    record_id: str
    statement_id: str
    name: str = ""
    jurisdiction: str | None = None
    #: ``subsidiary`` — the record is a group member; ``mne_head`` — it is one
    #: of the 500 group heads.
    mode: str
    group: MeipGroup
    #: The OECD's hierarchy classification of the membership: ``Known``,
    #: ``Partial``, ``Unknown`` (or ``MNE Head`` on the one annotated row).
    #: Empty for a head record.
    hierarchy: str = ""
    #: From the register spreadsheet's "Parent of Subsidiary" column — the
    #: BODS file carries only the edge to the group head.
    immediate_parent: str | None = None
    immediate_parent_record_id: str | None = None
    subsidiaries_total: int | None = None
    subsidiaries_with_lei: int | None = None


class MeipBundle(_Base):
    """Top-level shape returned by MeipAdapter.fetch_by_lei / fetch."""

    lei: str
    #: Register reference date, ISO — ``2024-12-31`` for the first release.
    edition: str = ""
    records: list[MeipRecord] = Field(default_factory=list)
    #: The OECD's statements, verbatim: the subject's entity statement(s),
    #: each upward relationship and its group head's entity statement.
    bods_statements: list[dict[str, Any]] = Field(default_factory=list)
