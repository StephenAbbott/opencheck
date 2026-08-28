"""Pydantic schema for Greek General Commercial Registry (ΓΕΜΗ) responses.

Only the fields ``map_gemi_greece`` actually reads are declared; everything
else passes through via ``extra="allow"`` on ``_Base``. That matters more than
usual here — the live API returns ``phone`` and ``fax`` on every company
record, and neither appears anywhere in the published Swagger definitions, so
the spec is demonstrably not exhaustive.

Field types below follow the **observed** payloads rather than the spec where
the two disagree. The spec declares ``arGemi`` an integer; the API returns it
as a string. Codelist objects embedded in a company record are truncated to
``{id, descr}`` — no ``descrEn``, no ``isActive`` — even though the spec's
``CompanyStatus`` and ``LegalType`` definitions declare all four, so nothing
here may require the richer fields.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class GemiCodeRef(_Base):
    """An embedded codelist reference: ``{"id": 3, "descr": "Ενεργή"}``.

    ``id`` is an **integer** here but a **string** in the ``/metadata/*``
    codelists, which is why the adapter stringifies before looking one up.
    Both are permitted below because the register is not consistent about it
    (``prefecture`` has been observed with ``{"id": 0, "descr": null}``).
    """

    id: int | str | None = None
    descr: str | None = None


class GemiPerson(_Base):
    """One entry of ``persons[]`` — an officer or an owner.

    ``category`` is the discriminator, not ``role``: ``Εταίροι`` are partners
    with real ``percentage`` values, ``Διοικητικό συμβούλιο`` are board members
    whose ``percentage`` is always the literal ``"-"``. ``role`` is free text
    that combines duties (``"Ομόρρυθμο Μέλος, Διαχειριστής & Εκπρόσωπος"``) and
    varies by legal form, so it refines the mapping but cannot drive it.

    Exactly one of ``personName`` / ``businessName`` is populated — a partner
    may itself be a company.
    """

    personName: str | None = None
    businessName: str | None = None
    role: str | None = None
    dtFrom: str | None = None
    dtTo: str | None = None
    percentage: str | None = None
    category: str | None = None
    isRepresentativeAlone: bool | None = None
    isRepresentativeInCommon: bool | None = None


class GemiCompany(_Base):
    """A ΓΕΜΗ company record, as returned by ``/companies/{arGemi}``.

    The same shape is returned inside ``searchResults[]`` — the search endpoint
    returns complete records, not summaries.
    """

    # Identity — arGemi is required: without it there is no subject to map.
    arGemi: str
    afm: str | None = None
    coNameEl: str | None = None
    coNamesEn: list[str] = Field(default_factory=list)
    coTitlesEl: list[str] = Field(default_factory=list)
    coTitlesEn: list[str] = Field(default_factory=list)

    # Registered office
    street: str | None = None
    streetNumber: str | None = None
    city: str | None = None
    zipCode: str | None = None
    poBox: str | None = None
    municipality: GemiCodeRef | None = None
    prefecture: GemiCodeRef | None = None

    # Status and form
    status: GemiCodeRef | None = None
    legalType: GemiCodeRef | None = None
    gemiOffice: GemiCodeRef | None = None
    incorporationDate: str | None = None
    lastStatusChange: str | None = None
    isBranch: bool | None = None
    autoRegistered: bool | None = None

    # People, capital and activity
    persons: list[GemiPerson] = Field(default_factory=list)
    capital: list[dict[str, Any]] = Field(default_factory=list)
    stocks: list[dict[str, Any]] = Field(default_factory=list)
    activities: list[dict[str, Any]] = Field(default_factory=list)
    branch: list[Any] = Field(default_factory=list)

    objective: str | None = None
    url: str | None = None


class GemiGreeceBundle(_Base):
    """Top-level shape returned by ``GemiGreeceAdapter.fetch``."""

    gr_argemi: str  # required — mapper key
    company: GemiCompany | None = None
    documents: dict[str, Any] | None = None
    legal_name: str | None = None
