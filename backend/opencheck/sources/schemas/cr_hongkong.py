"""Pydantic schema for Hong Kong Companies Registry open data responses.

The local-companies endpoint returns seven fields per company, matching the
published data dictionary (``data_dictionary_RO_PPB_en.docx``). Every field is
optional except ``Brn``: the register has been observed returning the literal
string ``"NULL"`` for a missing Chinese name and JSON ``null`` for an absent
re-domiciliation date, and ``extra="allow"`` on ``_Base`` keeps any field the
Registry adds later rather than failing the lookup on it.
"""

from __future__ import annotations

from pydantic import Field

from . import _Base


class CrHongKongCompany(_Base):
    """One live local company, as returned by ``/json/local/search``."""

    Brn: str
    Chinese_Company_Name: str | None = None
    English_Company_Name: str | None = None
    Address_of_Registered_Office: str | None = None
    Company_Type: str | None = None
    Date_of_Incorporation: str | None = None
    Re_domiciliation_Date: str | None = Field(default=None, alias="Re-domiciliation_Date")


class CrHongKongBundle(_Base):
    """Top-level shape returned by ``CrHongKongAdapter.fetch``."""

    hk_brn: str  # required — mapper key
    company: CrHongKongCompany | None = None
    legal_name: str | None = None
