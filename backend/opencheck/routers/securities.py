"""Securities endpoint — ISINs linked to an entity's LEI, with sanction overlay.

``GET /securities?lei=<LEI>&page=<n>`` returns the GLEIF ISIN count, one page of
ISINs typed via OpenFIGI, and the sanctioned subset from OpenSanctions. Fetched
lazily by the frontend's Securities section so a lookup never pays for it.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from ..ratelimit import default_tier, limiter
from ..securities import PAGE_SIZE, assemble_securities

router = APIRouter()

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class Security(BaseModel):
    isin: str
    type: str | None = None
    name: str | None = None
    ticker: str | None = None
    exchange: str | None = None
    sanctioned: bool = False
    regimes: list[str] = []
    opensanctions_id: str | None = None


class SecuritiesResponse(BaseModel):
    lei: str
    available: bool
    total: int
    page: int
    page_size: int
    securities: list[Security]
    sanctioned: list[Security]
    sources: list[str]
    license_notices: list[dict[str, str]]


@router.get("/securities", response_model=SecuritiesResponse)
@limiter.limit(default_tier)
async def securities(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    page: int = Query(1, ge=1, le=500, description="Page of ISINs to return (size 20)."),
) -> Any:
    """Securities (ISINs) linked to an LEI, sanctioned ones surfaced first."""
    norm_lei = lei.strip().upper()
    if not _LEI_SHAPE.match(norm_lei):
        raise HTTPException(
            status_code=400,
            detail=(
                f"{norm_lei!r} is not a valid LEI. ISO 17442 LEIs are 20-character "
                "alphanumeric strings (e.g. 7LTWFZYICNSX8D621K86)."
            ),
        )
    return await assemble_securities(norm_lei, page=page, page_size=PAGE_SIZE)
