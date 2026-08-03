"""Pydantic schema for the TED (Tenders Electronic Daily) adapter bundle.

The bundle combines two data layers:

* Flat search hits from ``POST /v3/notices/search`` (expert search on
  ``organisation-identifier-tenderer`` — eForms BT-501 values).
* Per-notice winner confirmation parsed from the eForms notice XML
  (``LotResult → LotTender → TenderingParty → Tenderer → CompanyID``).

Only fields the BODS mapper and the frontend card read are declared;
everything else passes through via ``extra="allow"``.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from . import _Base


class TedEuAwardedValue(_Base):
    """One per-lot awarded amount (``cbc:PayableAmount``)."""

    amount: str
    currency: str = ""


class TedEuNotice(_Base):
    """One notice where the entity appears as a tenderer/winner."""

    publication_number: str
    publication_date: str = ""
    notice_type: str = ""
    title: str = ""
    buyer_name: str = ""
    buyer_country: str = ""
    total_value: Any = None
    currency: str = ""
    cpv: list[str] = Field(default_factory=list)
    contract_conclusion_date: str = ""
    #: "won" | "tendered" | "unknown" (XML unavailable or chain unresolved).
    role: str = "unknown"
    lots_won: list[str] = Field(default_factory=list)
    awarded_values: list[TedEuAwardedValue] = Field(default_factory=list)
    award_dates: list[str] = Field(default_factory=list)
    contract_references: list[str] = Field(default_factory=list)
    #: BT-501 values on this notice that matched the queried identifiers.
    matched_company_ids: list[str] = Field(default_factory=list)
    confirmed: bool = False
    url: str = ""
    xml_url: str = ""


class TedEuBundle(_Base):
    """Top-level shape returned by TedEuAdapter.fetch_by_identifiers/fetch."""

    lei: str = ""
    legal_name: str = ""
    identifiers_queried: list[str] = Field(default_factory=list)
    total_notice_count: int = 0
    confirmed_wins: int = 0
    matched_company_ids: list[str] = Field(default_factory=list)
    notices: list[TedEuNotice] = Field(default_factory=list)
