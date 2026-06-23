"""Subsidiary-network endpoint — GLEIF direct + ultimate children (lazy).

``GET /subsidiaries?lei=<LEI>`` returns a count-first summary of the subject's
direct and ultimate children (with a render-mode hint); ``?format=bods`` also
returns the BODS statements for the graph / export. Never on the main lookup.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..subsidiaries import assemble_subsidiaries

router = APIRouter()

_LEI_SHAPE = re.compile(r"^[A-Z0-9]{20}$")


class SubChild(BaseModel):
    lei: str
    name: str | None = None
    jurisdiction: str | None = None
    status: str | None = None
    relation: str  # "direct" | "ultimate" | "both"
    link: str | None = None


class SubJurisdiction(BaseModel):
    code: str
    count: int


class SubsidiariesResponse(BaseModel):
    lei: str
    available: bool
    reason: str | None = None
    direct_total: int = 0
    ultimate_total: int = 0
    distinct_fetched: int = 0
    indirect_only: int = 0
    node_estimate: int = 0
    render_mode: str = "graph"  # "graph" | "table"
    truncated: bool = False
    jurisdictions: list[SubJurisdiction] = []
    children: list[SubChild] = []
    bods: list[dict] | None = None


@router.get("/subsidiaries", response_model=SubsidiariesResponse)
async def subsidiaries(
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    format: str = Query("summary", description="'summary' or 'bods' (adds BODS statements)."),
) -> Any:
    """The subject's GLEIF subsidiary network (direct + ultimate children)."""
    norm = lei.strip().upper()
    if not _LEI_SHAPE.match(norm):
        raise HTTPException(
            status_code=400,
            detail=f"{norm!r} is not a valid LEI (20-character alphanumeric).",
        )
    return await assemble_subsidiaries(norm, include_bods=(format == "bods"))
