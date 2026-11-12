"""Pydantic schema for the Find a Tender (UK procurement) adapter bundle.

Only the fields the BODS mapper, the finding and the frontend card read are
declared; everything else passes through via ``extra="allow"``. The builder
never copies a contact point, an address or ``parties[].control`` (connected
persons), so no such field exists here to validate.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class FindTenderSupplier(_Base):
    """Per-company totals over every notice in the index."""

    ch: str
    name: str | None = None
    awards: int = 0
    #: Awards under the Procurement Act 2023 (``legalBasis`` UKPGA).
    act_awards: int = 0
    buyers: int = 0
    first_date: str | None = None
    last_date: str | None = None


class FindTenderBuyer(_Base):
    name: str = ""
    awards: int = 0


class FindTenderRecord(_Base):
    """One award to the company, with the notice that records it."""

    ocid: str
    award_id: str
    notice: str = ""
    url: str = ""
    date: str | None = None
    #: "awarded" | "signed" | "published" | "updated" — what ``date`` is.
    date_basis: str = ""
    title: str = ""
    buyer: str = ""
    #: The award's (else the contract's) value — shared by ``suppliers``
    #: suppliers when more than one, and never summed.
    value: float | None = None
    currency: str = ""
    #: "active" | "pending" (awarded, contract not yet signed).
    award_status: str = ""
    #: "active" | "terminated" | "cancelled" | "" — the contract's, if any.
    contract_status: str = ""
    #: "act" (Procurement Act 2023) | "pre_act".
    regime: str
    above_threshold: bool | None = None
    suppliers: int = 1
    #: "coh" (the notice names the CH number) | "ppon" (via the crosswalk).
    via: str = "coh"
    #: How many of this procurement's awards (lots) went to the company.
    lots: int = 1
    #: "latest" | "largest" — why the card lists it.
    kept_as: str = "latest"


class FindTenderBundle(_Base):
    source_id: str
    company_number: str
    legal_name: str = ""
    supplier: FindTenderSupplier | None = None
    records: list[FindTenderRecord] = Field(default_factory=list)
    buyers: list[FindTenderBuyer] = Field(default_factory=list)
    pre_act_awards: int = 0
    via_ppon: int = 0
    data_from: str | None = None
    data_to: str | None = None
    source_as_of: str | None = None
    link: str = ""
    not_found: bool = False
    is_stub: bool = False
