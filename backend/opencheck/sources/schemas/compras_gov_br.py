"""Pydantic schema for the Compras.gov.br (Brazilian procurement) bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``. No field
holds a supplier's name, a CPF, an address or a contact point (decision D on
the ticket): the index stores none.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class ComprasGovBrSupplier(_Base):
    """Totals over every award the index read for one CNPJ root."""

    root: str
    awards: int = 0
    #: Published award values summed by OpenCheck, whole reais.
    value_brl: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None
    zero_value: int = 0
    #: Awards under a direct-contracting title (authorising act or notice).
    direct: int = 0
    establishments: int = 0


class ComprasGovBrBuyer(_Base):
    """One of the supplier's largest buyers."""

    name: str = ""
    sphere: str = ""
    uasg: str = ""
    awards: int = 0
    value_brl: int = 0


class ComprasGovBrEstablishment(_Base):
    """One CNPJ of the company that won an award."""

    cnpj: str
    head_office: bool = False
    awards: int = 0


class ComprasGovBrAward(_Base):
    """One award, as the index keeps it (the latest and the largest)."""

    key: str
    process: str = ""
    awarded: str | None = None
    buyer: str = ""
    buyer_sphere: str = ""
    buyer_uasg: str = ""
    object: str = ""
    value_brl: int | None = None
    procedure: str = ""
    legal_basis: str = ""
    establishment: str = ""
    other_suppliers: int = 0


class ComprasGovBrBundle(_Base):
    source_id: str
    cnpj: str
    cnpj_display: str = ""
    root: str = ""
    root_display: str = ""
    legal_name: str = ""
    supplier: ComprasGovBrSupplier | None = None
    buyers: list[ComprasGovBrBuyer] = Field(default_factory=list)
    establishments: list[ComprasGovBrEstablishment] = Field(default_factory=list)
    awards: list[ComprasGovBrAward] = Field(default_factory=list)
    data_to: str | None = None
    link: str = ""
    registry_url: str = ""
    not_found: bool = False
    is_stub: bool = False
