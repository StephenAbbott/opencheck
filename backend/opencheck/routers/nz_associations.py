"""NZ director/shareholder associations endpoint (lazy, panel-only).

``GET /nz-associations?company_number=<n>`` reports, for each director and
shareholder of an NZ company, how many other companies they appear in under the
same name (address-tiered confidence). Never on the main lookup; fetched by the
frontend's NZ panel on demand.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..nz_associations import assemble_associations

router = APIRouter()

# An NZ company number (short) OR an NZBN (13 digits) — GLEIF stores either in
# registeredAs depending on the entity, so accept both.
_NUMBER_SHAPE = re.compile(r"^\d{1,13}$")


class AssociatedCompany(BaseModel):
    number: str
    name: str | None = None
    nzbn: str | None = None
    roles: list[str] = []
    share_percentage: float | int | None = None
    confidence: str  # "high" | "medium" | "low"
    basis: str
    link: str | None = None


class PersonAssociations(BaseModel):
    name: str
    role_here: list[str] = []
    other_company_count: int
    high_confidence_count: int
    address_match_count: int = 0
    name_only_count: int = 0
    as_director: int
    as_shareholder: int
    total_records_under_name: int = 0
    truncated: bool = False
    companies: list[AssociatedCompany] = []


class AssociationsResponse(BaseModel):
    company_number: str
    available: bool
    reason: str | None = None
    subject_name: str | None = None
    checked: int = 0
    not_checked: int = 0
    people: list[PersonAssociations] = []


@router.get("/nz-associations", response_model=AssociationsResponse)
async def nz_associations(
    company_number: str = Query(..., description="NZ company number (digits)."),
) -> Any:
    """Director/shareholder cross-company associations for an NZ company."""
    num = company_number.strip()
    if not _NUMBER_SHAPE.match(num):
        raise HTTPException(
            status_code=400,
            detail=f"{num!r} is not a valid New Zealand company number or NZBN (digits).",
        )
    return await assemble_associations(num)
