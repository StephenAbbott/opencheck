"""Pydantic schema for Romanian ONRC bundles (data.gov.ro monthly dump).

The bundle is assembled by ``OnrcRomaniaAdapter.fetch`` from a pre-built
SQLite index over ``OD_FIRME.CSV`` (the company) and
``OD_REPREZENTANTI_LEGALI.CSV`` (its legal representatives), joined on the
register's own ``COD_INMATRICULARE``.

Only the fields the mapper and the finding read are declared; ``extra="allow"``
on ``_Base`` keeps the rest, so a column ONRC adds later cannot fail a lookup.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class OnrcCompanyRow(_Base):
    """One ``company`` row: a company as the register holds it."""

    registration_number: str
    cui: str | None = None
    name: str | None = None
    legal_form: str | None = None
    registered_on: str | None = None
    status_code: str | None = None
    status: str | None = None
    county: str | None = None
    locality: str | None = None
    address: str | None = None
    postal_code: str | None = None
    country: str | None = None
    website: str | None = None
    parent_country: str | None = None


class OnrcRepresentativeRow(_Base):
    """One ``representative`` row.

    ``name`` is a natural person on 3,424,132 of 3,689,931 rows and a legal
    entity on the rest — insolvency practitioner firms and corporate
    administrators. ``is_entity`` records which, decided at index-build time.

    ``birth_date`` is an ISO date where the register filed one (87.2% of rows).
    It is the **full published date**: ONRC publishes it openly under CC BY 4.0
    and OpenCheck republishes it at the precision filed, which is also what
    gives Romanian person matching a corroborating attribute.
    """

    name: str
    role: str | None = None
    role_slug: str | None = None
    is_entity: bool = False
    birth_date: str | None = None
    birth_locality: str | None = None
    birth_county: str | None = None
    birth_country: str | None = None
    locality: str | None = None
    county: str | None = None
    country: str | None = None
    seq: int | None = None


class OnrcRomaniaBundle(_Base):
    """Top-level shape returned by ``OnrcRomaniaAdapter.fetch``."""

    # Required — mapper key (may be "" for stubs).
    registration_number: str
    cui: str | None = None
    name: str | None = None
    company: OnrcCompanyRow | None = None
    representatives: list[OnrcRepresentativeRow] = []
    legal_name: str | None = None
    link: str | None = None
    is_stub: bool = True
    source_id: str | None = None
    raw: dict[str, Any] | None = None
