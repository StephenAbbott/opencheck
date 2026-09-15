"""Pydantic schema for Moldovan State Register bundles (dataset.gov.md weekly export).

The bundle is assembled by ``AspMoldovaAdapter.fetch`` from the SQLite index
over the weekly company XLSX: one ``company`` row, its ``officer`` rows
(directors, parsed from *Lista conducătorilor*) and its ``founder`` rows
(parsed from *Lista fondatorilor*).

Only the fields the mapper and the finding read are declared; ``extra="allow"``
on ``_Base`` keeps the rest.
"""

from __future__ import annotations

from . import _Base


class AspCompanyRow(_Base):
    """One company as the register holds it."""

    idno: str
    name: str
    legal_form: str | None = None
    registered_on: str | None = None
    address: str | None = None
    cuatm: str | None = None
    activities: str | None = None
    licensed_activities: str | None = None


class AspOfficerRow(_Base):
    """One director. ``interest_type`` is decided at index-build time."""

    name: str
    role: str | None = None
    role_key: str | None = None
    interest_type: str
    is_entity: bool = False
    seq: int | None = None


class AspFounderRow(_Base):
    """One founder.

    ``kind`` is ``person``, ``entity`` or ``state``. ``share_pct`` is the
    filed percentage of the capital, and is ``None`` both when no share was
    filed and when the stake is held jointly (``joint``), in which case
    ``share_text`` carries it as filed. ``resolved_idno`` is set for a
    corporate founder the index could identify, and ``resolution`` says how:
    ``embedded_idno`` (the register's text names the number) or
    ``name_match`` (exactly one company in the index carries the same folded
    name).
    """

    name: str
    kind: str
    share_pct: float | None = None
    share_text: str | None = None
    joint: bool = False
    grp: int | None = None
    resolved_idno: str | None = None
    resolved_name: str | None = None
    resolution: str | None = None
    seq: int | None = None


class AspMoldovaBundle(_Base):
    """Top-level shape returned by ``AspMoldovaAdapter.fetch``."""

    idno: str
    name: str | None = None
    company: AspCompanyRow | None = None
    officers: list[AspOfficerRow] = []
    founders: list[AspFounderRow] = []
    snapshot_date: str | None = None
    legal_name: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None
    is_stub: bool = True
    source_id: str | None = None
