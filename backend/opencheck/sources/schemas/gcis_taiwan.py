"""Pydantic schema for Taiwan GCIS company register responses.

Field names are the platform's. Numeric fields arrive as JSON numbers although
the Swagger types them as strings, so they are typed loosely; ``Share_Val`` and
``Equity_Amt`` are returned by the company endpoint but absent from its Swagger
schema. ``extra="allow"`` on ``_Base`` keeps anything the platform adds later.
"""

from __future__ import annotations

from . import _Base


class GcisTaiwanCompany(_Base):
    """One company, as returned by 公司登記基本資料-應用一."""

    Business_Accounting_NO: str
    Company_Name: str | None = None
    Company_Status_Desc: str | None = None
    Capital_Stock_Amount: int | float | str | None = None
    Paid_In_Capital_Amount: int | float | str | None = None
    Share_Val: int | float | str | None = None
    Equity_Amt: int | float | str | None = None
    Responsible_Name: str | None = None
    Company_Location: str | None = None
    Register_Organization_Desc: str | None = None
    Company_Setup_Date: str | None = None
    Change_Of_Approval_Data: str | None = None
    Revoke_App_Date: str | None = None
    Case_Status: str | None = None
    Case_Status_Desc: str | None = None
    Sus_App_Date: str | None = None
    Sus_Beg_Date: str | None = None
    Sus_End_Date: str | None = None


class GcisTaiwanDirector(_Base):
    """One row of 公司登記董監事資料集."""

    Person_Position_Name: str | None = None
    Person_Name: str | None = None
    Juristic_Person_Name: str | None = None
    Person_Shareholding: int | float | str | None = None


class GcisTaiwanBundle(_Base):
    """Top-level shape returned by ``GcisTaiwanAdapter.fetch``."""

    tw_ubn: str  # required — mapper key
    company: GcisTaiwanCompany | None = None
    directors: list[GcisTaiwanDirector] = []
    directors_read: bool = True
    legal_name: str | None = None
