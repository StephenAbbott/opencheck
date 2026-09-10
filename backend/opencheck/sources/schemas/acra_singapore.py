"""Pydantic schema for Singapore ACRA rows from data.gov.sg ``datastore_search``.

Two row shapes reach a bundle:

* ``entity`` — a collection-1 row (*Entities Registered with ACRA* or
  *… with Other UEN Issuance Agencies*): eight text columns plus ``_id``.
* ``detail`` — a collection-2 row (*ACRA Information on Corporate Entities*,
  one file per first character of the name): 55 text columns, the literal
  string ``na`` for a missing value.

Only the fields the mapper and finding read are declared; ``extra="allow"`` on
``_Base`` keeps the rest, so a column ACRA adds later cannot fail a lookup.
"""

from __future__ import annotations

from . import _Base


class AcraUenRow(_Base):
    """A collection-1 row."""

    uen: str
    issuance_agency_desc: str | None = None
    uen_status_desc: str | None = None
    entity_name: str | None = None
    entity_type_desc: str | None = None
    uen_issue_date: str | None = None
    reg_street_name: str | None = None
    reg_postal_code: str | None = None


class AcraDetailRow(_Base):
    """A collection-2 row."""

    uen: str
    entity_name: str | None = None
    entity_type_description: str | None = None
    company_type_description: str | None = None
    business_constitution_description: str | None = None
    entity_status_description: str | None = None
    registration_incorporation_date: str | None = None
    uen_issue_date: str | None = None
    address_type: str | None = None
    block: str | None = None
    street_name: str | None = None
    level_no: str | None = None
    unit_no: str | None = None
    building_name: str | None = None
    postal_code: str | None = None
    other_address_line1: str | None = None
    other_address_line2: str | None = None
    primary_ssic_code: str | None = None
    primary_ssic_description: str | None = None
    secondary_ssic_code: str | None = None


class AcraSingaporeBundle(_Base):
    """Top-level shape returned by ``AcraSingaporeAdapter.fetch``."""

    uen: str  # required — mapper key
    entity: AcraUenRow | None = None
    detail: AcraDetailRow | None = None
    dataset: str | None = None
    record_resource_id: str | None = None
    legal_name: str | None = None
