"""Pydantic schema for the MTender (Moldovan public procurement) bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``. No contact
point, person or e-mail is ever carried: the index holds none.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class MTenderSupplier(_Base):
    """Totals over every active award the index read for one IDNO."""

    idno: str
    name: str | None = None
    awards: int = 0
    lots: int = 0
    #: Award values in MDL summed by OpenCheck, whole lei.
    value_mdl: int = 0
    #: Awards in another currency, counted and never converted.
    other_currency: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None
    #: Awards by procedure, keyed by the procedure's words.
    methods: dict[str, int] = Field(default_factory=dict)


class MTenderBuyer(_Base):
    """One of the supplier's largest buyers."""

    name: str = ""
    awards: int = 0
    value_mdl: int = 0


class MTenderAward(_Base):
    """One procurement the supplier was awarded, with its MTender link."""

    ocid: str
    awarded: str | None = None
    buyer: str = ""
    title: str = ""
    value: int | None = None
    currency: str = "MDL"
    procedure: str = ""
    category: str = ""
    lots: int = 1
    url: str = ""


class MTenderBundle(_Base):
    source_id: str
    idno: str
    legal_name: str = ""
    supplier: MTenderSupplier | None = None
    buyers: list[MTenderBuyer] = Field(default_factory=list)
    awards: list[MTenderAward] = Field(default_factory=list)
    data_from: str | None = None
    data_to: str | None = None
    link: str = ""
    not_found: bool = False
    is_stub: bool = False
