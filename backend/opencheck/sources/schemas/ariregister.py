"""Pydantic schema for Estonian e-Business Register bundles (Phase 330).

One shape for both tiers of ``AriregisterAdapter.fetch`` — the daily index
of RIK's open-data files and the printable page — in the row vocabulary the
files use (``kirje_id``, ``isiku_tyyp``, ``isiku_roll``, ``osaluse_protsent``,
``kontrolli_teostamise_viis``…), which ``map_ariregister`` has read since
the SOAP days. Only the fields the mapper and the finding read are
declared; ``extra="allow"`` on ``_Base`` keeps the rest.

The one thing the schema *forbids* is a personal identification code: the
open data stopped publishing them in November 2024 and the page path strips
them, so a bundle carrying one is a defect wherever it came from.
"""

from __future__ import annotations

import re

from pydantic import field_validator

from . import _Base

_PERSONAL_CODE = re.compile(r"^[1-6]\d{10}$")


class _NoPersonalCode(_Base):
    """A person row never carries an 11-digit Estonian personal code."""

    @field_validator("isikukood_registrikood", "isikukood", mode="before", check_fields=False)
    @classmethod
    def _reject_personal_code(cls, value: object) -> object:
        if isinstance(value, str) and _PERSONAL_CODE.match(value.strip()):
            raise ValueError("an Estonian personal identification code must not be carried")
        return value


class AriregisterOfficerRow(_NoPersonalCode):
    """One person on the registry card (kaardile_kantud_isikud)."""

    kirje_id: str | int | None = None
    isiku_tyyp: str | None = None
    isiku_roll: str | None = None
    eesnimi: str | None = None
    nimi_arinimi: str | None = None
    isikukood_hash: str | None = None
    isikukood_registrikood: str | None = None
    valis_kood: str | None = None
    valis_kood_riik: str | None = None
    synniaeg: str | None = None
    algus_kpv: str | None = None
    lopp_kpv: str | None = None


class AriregisterShareholderRow(_NoPersonalCode):
    """One entry in the register of shareholders (osanikud)."""

    kirje_id: str | int | None = None
    isiku_tyyp: str | None = None
    eesnimi: str | None = None
    nimi_arinimi: str | None = None
    isikukood_hash: str | None = None
    isikukood_registrikood: str | None = None
    valis_kood: str | None = None
    valis_kood_riik: str | None = None
    synniaeg: str | None = None
    osaluse_protsent: str | None = None
    osaluse_suurus: str | None = None
    osaluse_valuuta: str | None = None
    osaluse_omandiliik: str | None = None
    kontrolli_allikas: str | None = None
    algus_kpv: str | None = None
    lopp_kpv: str | None = None


class AriregisterBeneficialOwnerRow(_NoPersonalCode):
    """One declared beneficial owner (kasusaajad)."""

    kirje_id: str | int | None = None
    eesnimi: str | None = None
    nimi: str | None = None
    isikukood: str | None = None
    isikukood_hash: str | None = None
    valis_kood_riik: str | None = None
    synniaeg: str | None = None
    aadress_riik: str | None = None
    kontrolli_teostamise_viis: str | None = None
    lahknevusteade_esitatud: bool | None = None
    algus_kpv: str | None = None
    lopp_kpv: str | None = None


class AriregisterOffCardRow(_NoPersonalCode):
    """One person outside the registry card (kaardivalised_isikud): a
    founder, supervisory board member, AS shareholder, auditor… — the
    shareholder row's shape plus the role."""

    kirje_id: str | int | None = None
    isiku_tyyp: str | None = None
    isiku_roll: str | None = None
    eesnimi: str | None = None
    nimi_arinimi: str | None = None
    isikukood_hash: str | None = None
    isikukood_registrikood: str | None = None
    valis_kood: str | None = None
    valis_kood_riik: str | None = None
    synniaeg: str | None = None
    aadress_riik: str | None = None
    osaluse_protsent: str | None = None
    osaluse_suurus: str | None = None
    osaluse_valuuta: str | None = None
    osaluse_omandiliik: str | None = None
    kontrolli_allikas: str | None = None
    algus_kpv: str | None = None
    lopp_kpv: str | None = None


class AriregisterBundle(_Base):
    """Top-level shape returned by ``AriregisterAdapter.fetch`` for a record."""

    registry_code: str
    name: str = ""
    legal_form: str | None = None
    legal_form_subtype: str | None = None
    vat_number: str | None = None
    status: str | None = None
    status_code: str | None = None
    status_text: str | None = None
    registration_date: str | None = None
    address: str | None = None
    link: str | None = None
    shareholders: list[AriregisterShareholderRow] = []
    officers: list[AriregisterOfficerRow] = []
    beneficial_owners: list[AriregisterBeneficialOwnerRow] = []
    off_card_persons: list[AriregisterOffCardRow] = []
    cut: str | None = None
    tier: str | None = None
    index_miss: bool = False
    is_stub: bool = False
    source_id: str | None = None
    #: The Estonian companies above this one, keyed on registry code — each
    #: a bundle of this shape (index tier only; ``walk_chain``), and what
    #: the walk reached and did not follow.
    related_companies: dict[str, "AriregisterBundle"] = {}
    chain: dict | None = None
