"""Pydantic schema for ANAF v9 taxpayer records (webservicesp.anaf.ro).

One ``found`` element of ANAF's response is a record of five blocks:
``date_generale`` (the company), ``inregistrare_scop_Tva`` (VAT registration
and its periods), ``stare_inactiv`` (the inactive-taxpayer register, including
the de-registration date), ``inregistrare_SplitTVA`` and the two address
blocks. ``inregistrare_RTVAI`` (VAT on collection) is carried through by
``extra="allow"`` but nothing reads it.

Only the fields the mapper and the finding read are declared. ANAF adds fields
to this service between versions — ``statusRO_e_Factura`` and
``data_inreg_Reg_RO_e_Factura`` arrived after v6 — so everything else passes
through rather than failing a lookup.
"""

from __future__ import annotations

from typing import Any

from . import _Base


class AnafGeneral(_Base):
    """``date_generale`` — the company as ANAF holds it."""

    cui: int | str
    denumire: str | None = None
    adresa: str | None = None
    nrRegCom: str | None = None
    stare_inregistrare: str | None = None
    data_inregistrare: str | None = None
    cod_CAEN: str | None = None
    forma_juridica: str | None = None
    forma_organizare: str | None = None
    forma_de_proprietate: str | None = None
    organFiscalCompetent: str | None = None
    telefon: str | None = None
    codPostal: str | None = None
    statusRO_e_Factura: bool | None = None
    iban: str | None = None


class AnafVatPeriod(_Base):
    """One element of ``inregistrare_scop_Tva.perioade_TVA``."""

    data_inceput_ScpTVA: str | None = None
    data_sfarsit_ScpTVA: str | None = None
    data_anul_imp_ScpTVA: str | None = None
    mesaj_ScpTVA: str | None = None


class AnafVat(_Base):
    """``inregistrare_scop_Tva`` — VAT registration."""

    scpTVA: bool | None = None
    perioade_TVA: list[AnafVatPeriod] = []


class AnafInactive(_Base):
    """``stare_inactiv`` — the inactive-taxpayer register.

    ``dataRadiere`` is the de-registration (striking-off) date and is the one
    field here that changes what OpenCheck says about whether a company exists.
    """

    statusInactivi: bool | None = None
    dataInactivare: str | None = None
    dataReactivare: str | None = None
    dataPublicare: str | None = None
    dataRadiere: str | None = None


class AnafAddress(_Base):
    """``adresa_sediu_social`` / ``adresa_domiciliu_fiscal``.

    The two blocks use different field prefixes for the same concepts (``s``
    for the registered office, ``d`` for the fiscal domicile), so both sets are
    declared here and the mapper picks by prefix.
    """

    sdenumire_Strada: str | None = None
    snumar_Strada: str | None = None
    sdenumire_Localitate: str | None = None
    sdenumire_Judet: str | None = None
    scod_Postal: str | None = None
    sdetalii_Adresa: str | None = None
    stara: str | None = None
    ddenumire_Strada: str | None = None
    dnumar_Strada: str | None = None
    ddenumire_Localitate: str | None = None
    ddenumire_Judet: str | None = None
    dcod_Postal: str | None = None
    ddetalii_Adresa: str | None = None
    dtara: str | None = None


class AnafRecord(_Base):
    """One ``found`` element."""

    date_generale: AnafGeneral
    inregistrare_scop_Tva: AnafVat | None = None
    stare_inactiv: AnafInactive | None = None
    adresa_sediu_social: AnafAddress | None = None
    adresa_domiciliu_fiscal: AnafAddress | None = None


class AnafRomaniaBundle(_Base):
    """Top-level shape returned by ``AnafRomaniaAdapter.fetch``."""

    # Required — mapper key.
    cui: str
    queried_as: str | None = None
    name: str | None = None
    record: AnafRecord | None = None
    registration_number: str | None = None
    legal_name: str | None = None
    link: str | None = None
    coverage_note: str | None = None
    not_found: bool = False
    is_stub: bool = False
    source_id: str | None = None
    raw: dict[str, Any] | None = None
