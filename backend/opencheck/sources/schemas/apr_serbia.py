"""Pydantic schema for Serbian APR company-register bundles (monthly open data).

The bundle is assembled by ``AprSerbiaAdapter.fetch`` from the SQLite index
over APR's company register: one ``company`` row. The register publishes no
people, so there is nothing else.

Only the fields the mapper and the finding read are declared; ``extra="allow"``
on ``_Base`` keeps the rest.
"""

from __future__ import annotations

from . import _Base


class AprCompanyRow(_Base):
    """One company as the register holds it."""

    mb: str
    name: str
    name_latin: str | None = None
    legal_form: str | None = None
    status: str | None = None
    founded_on: str | None = None
    municipality: str | None = None
    municipality_code: str | None = None
    activity_code: str | None = None


class AprSerbiaBundle(_Base):
    """Top-level shape returned by ``AprSerbiaAdapter.fetch``."""

    mb: str
    name: str | None = None
    company: AprCompanyRow | None = None
    snapshot_date: str | None = None
    legal_name: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None
    is_stub: bool = True
    source_id: str | None = None
