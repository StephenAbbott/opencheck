"""Pydantic schema for the Prozorro (Ukrainian public procurement) bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``. No person,
signatory, contact point or address is ever carried: the index holds none.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class ProzorroSupplier(_Base):
    """Totals over the active and terminated contracts read for one EDRPOU."""

    edrpou: str
    name: str | None = None
    contracts: int = 0
    terminated: int = 0
    #: Contract values in UAH summed by OpenCheck, whole hryvnias.
    value_uah: int = 0
    #: Contracts in another currency, counted and never converted.
    other_currency: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None
    #: Contracts by procedure, keyed by the procedure's words.
    methods: dict[str, int] = Field(default_factory=dict)


class ProzorroBuyer(_Base):
    """One of the supplier's largest buyers."""

    name: str = ""
    contracts: int = 0
    value_uah: int = 0


class ProzorroContract(_Base):
    """One contract the supplier signed, with its Prozorro link."""

    id: str
    ref: str = ""
    signed: str | None = None
    buyer: str = ""
    title: str = ""
    value: int | None = None
    currency: str = "UAH"
    procedure: str = ""
    category: str = ""
    status: str = ""
    url: str = ""


class ProzorroBundle(_Base):
    source_id: str
    edrpou: str
    legal_name: str = ""
    supplier: ProzorroSupplier | None = None
    buyers: list[ProzorroBuyer] = Field(default_factory=list)
    contracts: list[ProzorroContract] = Field(default_factory=list)
    data_from: str | None = None
    data_to: str | None = None
    harvest_started_from: str | None = None
    link: str = ""
    not_found: bool = False
    is_stub: bool = False
