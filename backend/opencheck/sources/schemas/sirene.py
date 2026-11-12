"""Pydantic schema for the ``sirene`` bundle (INSEE API Sirene 3.11 + the RNA).

Field names are INSEE's and the RNA file's. ``extra="allow"`` on ``_Base``
keeps anything INSEE adds later. The RNA record is the index's allowlisted
columns only; :class:`SireneRna` refuses the columns the builder must never
write (the management address, which names the declarant, and the director's
title), so a stray one fails validation instead of reaching a response.
"""

from __future__ import annotations

from pydantic import model_validator

from . import _Base


class SireneUnit(_Base):
    """The legal unit with its current period flattened in."""

    siren: str
    statutDiffusionUniteLegale: str | None = None
    dateCreationUniteLegale: str | None = None
    identifiantAssociationUniteLegale: str | None = None
    denominationUniteLegale: str | None = None
    sigleUniteLegale: str | None = None
    categorieJuridiqueUniteLegale: str | None = None
    etatAdministratifUniteLegale: str | None = None
    dateDebut: str | None = None
    activitePrincipaleUniteLegale: str | None = None
    nicSiegeUniteLegale: str | None = None
    economieSocialeSolidaireUniteLegale: str | None = None
    dateDernierTraitementUniteLegale: str | None = None


class SirenePeriod(_Base):
    """One history period of the unit, reduced to what the History tab reads.

    Refuses a natural person's name fields: a sole trader's names are never
    kept (the Sirene history ticket, decision E).
    """

    dateDebut: str | None = None
    dateFin: str | None = None
    etatAdministratifUniteLegale: str | None = None
    denominationUniteLegale: str | None = None
    categorieJuridiqueUniteLegale: str | None = None
    activitePrincipaleUniteLegale: str | None = None
    nomenclatureActivitePrincipaleUniteLegale: str | None = None
    nicSiegeUniteLegale: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _no_personal_names(cls, data: object) -> object:
        if isinstance(data, dict):
            leaked = sorted(
                k for k in data
                if k.startswith(("nomUniteLegale", "nomUsage", "prenom", "pseudonyme", "sexe", "changementNom"))
            )
            if leaked:
                raise ValueError(f"Sirene period carries a person's name fields: {leaked}")
        return data


class SireneHeadOffice(_Base):
    siret: str | None = None
    address: dict[str, str] = {}


class SireneRna(_Base):
    """One RNA (waldec) record, as the index keeps it."""

    id: str
    siren: str | None = None
    titre: str | None = None
    objet: str | None = None
    date_creat: str | None = None
    date_decla: str | None = None
    date_publi: str | None = None
    date_disso: str | None = None
    position: str | None = None
    nature: str | None = None
    groupement: str | None = None
    rup_mi: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _no_personal_columns(cls, data: object) -> object:
        if isinstance(data, dict):
            leaked = sorted(
                k for k in data if k.startswith("adrg_") or k in ("dir_civilite", "observation", "publiweb")
            )
            if leaked:
                raise ValueError(f"RNA record carries columns that must never be kept: {leaked}")
        return data


class SireneBundle(_Base):
    source_id: str
    hit_id: str
    siren: str
    legal_name: str = ""
    unit: SireneUnit
    periods: list[SirenePeriod] = []
    head_office: SireneHeadOffice | None = None
    head_office_read: bool = True
    rna: SireneRna | None = None
    rna_status: str
    rna_cut: str | None = None
    link: str
    is_stub: bool = False
