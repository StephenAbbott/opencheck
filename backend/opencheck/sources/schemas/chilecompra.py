"""Pydantic schema for the ChileCompra (Mercado Público) adapter bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class ChileCompraSupplier(_Base):
    """The per-company totals over the index window."""

    rut: int
    dv: str
    name: str | None = None
    orders: int = 0
    order_value_clp: int = 0
    tenders_bid: int = 0
    tenders_won: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None


class ChileCompraBuyer(_Base):
    """One of the supplier's largest public-sector customers."""

    code: str
    name: str = ""
    orders: int = 0
    order_value_clp: int = 0
    tenders_won: int = 0


class ChileCompraRecord(_Base):
    """One tender the supplier bid on, or one purchase order it received —
    the TED "notice" equivalent, with its Mercado Público link."""

    #: "tender" | "order"
    kind: str
    code: str
    date: str | None = None
    buyer: str = ""
    title: str = ""
    #: Tender type ("Licitación Pública Mayor 1000 UTM (LP)") or how an order
    #: came about ("Direct award (trato directo): <ChileCompra's reason>").
    procedure: str = ""
    #: Tender status as ChileCompra files it (Adjudicada, Cerrada, Desierta …).
    status: str = ""
    #: "won" | "tendered" for a tender; "order" for a purchase order.
    role: str
    #: Awarded amount for a won tender (its offer currency), order total in CLP.
    value: int | None = None
    currency: str = ""
    url: str = ""


class ChileCompraBundle(_Base):
    source_id: str
    rut: str
    rut_display: str = ""
    legal_name: str = ""
    supplier: ChileCompraSupplier | None = None
    buyers: list[ChileCompraBuyer] = Field(default_factory=list)
    records: list[ChileCompraRecord] = Field(default_factory=list)
    window: str = ""
    data_from: str | None = None
    data_to: str | None = None
    link: str = ""
    not_found: bool = False
    is_stub: bool = False
