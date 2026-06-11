"""Pydantic schema for Companies House API responses.

Only the fields the BODS mapper (map_companies_house) actually reads are
declared.  Everything else passes through via ``extra="allow"``.

Required fields (no default) are those whose absence would crash or
silently corrupt BODS output.  Everything else is Optional.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class CHAddress(_Base):
    premises: str | None = None
    address_line_1: str | None = None
    locality: str | None = None
    postal_code: str | None = None
    country: str | None = None


class CHProfile(_Base):
    company_number: str  # required — mapper key
    company_name: str | None = None
    company_status: str | None = None
    type: str | None = None
    date_of_creation: str | None = None
    jurisdiction: str | None = None
    registered_office_address: CHAddress | None = None
    # Former names the company traded under → BODS alternateNames.
    # Each item: {"name": ..., "effective_from": ..., "ceased_on": ...}.
    previous_company_names: list[dict[str, Any]] = Field(default_factory=list)


class CHPscIdentification(_Base):
    registration_number: str | None = None
    country_registered: str | None = None
    place_registered: str | None = None
    legal_authority: str | None = None
    legal_form: str | None = None


class CHNameElements(_Base):
    forename: str | None = None
    surname: str | None = None
    middle_name: str | None = None
    title: str | None = None


class CHDateOfBirth(_Base):
    year: int | None = None
    month: int | None = None


class CHPsc(_Base):
    kind: str  # required — drives entity-vs-person dispatch in mapper
    name: str | None = None
    name_elements: CHNameElements | None = None
    natures_of_control: list[str] = Field(default_factory=list)
    nationality: str | None = None
    notified_on: str | None = None
    ceased_on: str | None = None
    date_of_birth: CHDateOfBirth | None = None
    country_of_residence: str | None = None
    address: dict[str, Any] = Field(default_factory=dict)
    identification: CHPscIdentification | None = None
    etag: str | None = None


class CHPscList(_Base):
    items: list[CHPsc] = Field(default_factory=list)
    total_results: int | None = None


class CHPscStatement(_Base):
    """A 'persons with significant control statement' — a notice the company
    files *instead of* (or alongside) a PSC, e.g. 'no PSC exists' or 'PSC not
    yet identified'. The code is in ``statement``."""

    statement: str | None = None  # the CH statement_description code
    notified_on: str | None = None
    ceased_on: str | None = None
    linked_psc_name: str | None = None
    etag: str | None = None


class CHPscStatementList(_Base):
    items: list[CHPscStatement] = Field(default_factory=list)
    total_results: int | None = None


class CHOfficerAppointment(_Base):
    name: str | None = None
    officer_role: str | None = None
    appointed_on: str | None = None
    resigned_on: str | None = None
    nationality: str | None = None
    occupation: str | None = None
    country_of_residence: str | None = None
    date_of_birth: CHDateOfBirth | None = None
    address: dict[str, Any] = Field(default_factory=dict)
    appointed_to: dict[str, Any] = Field(default_factory=dict)


class CHAppointmentsList(_Base):
    items: list[CHOfficerAppointment] = Field(default_factory=list)
    total_results: int | None = None
    name: str | None = None


class CHBundle(_Base):
    """Top-level shape returned by CompaniesHouseAdapter._fetch_company_bundle."""

    company_number: str  # required
    profile: CHProfile
    pscs: CHPscList | None = None
    psc_statements: CHPscStatementList | None = None
    officers: dict[str, Any] = Field(default_factory=dict)
    related_companies: dict[str, Any] = Field(default_factory=dict)


class CHOfficerBundle(_Base):
    """Top-level shape returned by CompaniesHouseAdapter._fetch_officer_bundle."""

    officer_id: str  # required
    appointments: CHAppointmentsList
