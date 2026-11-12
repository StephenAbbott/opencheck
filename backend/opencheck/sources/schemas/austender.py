"""Pydantic schema for the AusTender (Australian Government contracts) bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class AusTenderSupplier(_Base):
    """Totals over every contract notice the index read for one ABN."""

    abn: str
    name: str | None = None
    contracts: int = 0
    #: Published contract values summed by OpenCheck, whole AU dollars.
    value_aud: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None
    marked_cancelled: int = 0
    amended: int = 0
    shared: int = 0


class AusTenderBuyer(_Base):
    """One of the supplier's largest buying agencies."""

    name: str = ""
    contracts: int = 0
    value_aud: int = 0


class AusTenderContract(_Base):
    """One contract notice, with its AusTender link."""

    cn: str
    published: str | None = None
    buyer: str = ""
    title: str = ""
    value_aud: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    procedure: str = ""
    limited_tender_reason: str = ""
    amendments: int = 0
    #: The data reads "cancelled"; a cancelled amendment can set it.
    marked_cancelled: bool = False
    other_suppliers: int = 0
    url: str = ""


class AusTenderBundle(_Base):
    source_id: str
    abn: str
    abn_display: str = ""
    matched_via: str = ""
    legal_name: str = ""
    supplier: AusTenderSupplier | None = None
    buyers: list[AusTenderBuyer] = Field(default_factory=list)
    contracts: list[AusTenderContract] = Field(default_factory=list)
    data_to: str | None = None
    link: str = ""
    not_found: bool = False
    ambiguous: bool = False
    is_stub: bool = False
