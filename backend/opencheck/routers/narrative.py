"""``/narrative`` — a grounded, source-cited summary of a lookup result.

The endpoint reuses the *exact same* cached lookup pipeline as ``/lookup`` (so the
narrative can never describe a different result than the page shows), distils the
result into an :class:`EvidencePacket`, and asks Claude for a summary whose every
claim cites packet evidence. The citation validator runs before the response is
returned, so a caller never receives an ungrounded claim.

Why no token streaming (yet): the trust guarantee is "every claim is grounded",
and grounding can only be checked once the whole structured answer exists.
Streaming raw tokens would put unvalidated text on screen. Phase 1 therefore
returns the validated narrative in one response; a future "stream the paragraph
*after* validation" path can be layered on without changing this contract.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..config import get_settings
from ..dispositions import (
    UTC,
    ClaimDisposition,
    DispositionRecord,
    DispositionStatus,
    compute_run_id,
    load_dispositions,
    save_dispositions,
)
from ..narrative import build_evidence_packet
from ..narrative.summarise import NarrativeUnavailable, summarise
from ..ratelimit import default_tier, heavy_tier, limiter
from .lookup import _lookup_impl

router = APIRouter()


class NarrativeResponse(BaseModel):
    lei: str | None = None
    subject_name: str
    summary: str
    claims: list[dict[str, Any]] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    overall_confidence: str = "low"
    model: str
    prompt_version: str
    # Deterministic identity of this exact narrative (see dispositions.py) —
    # analyst dispositions are keyed to it, so a regenerate starts a new sheet.
    run_id: str = ""
    generated_at: str = ""
    # The packet is returned so the UI can resolve each claim's cited ids
    # (f/r/g) back to the underlying fact, risk or gap for citation chips.
    packet: dict[str, Any]
    # Surfaced for transparency / debugging — what (if anything) was rejected.
    validation_ok: bool = True
    dropped_claims: list[dict[str, Any]] = Field(default_factory=list)
    validation_issues: list[str] = Field(default_factory=list)
    # Packet gaps no surviving claim cites ("clear fallbacks, not silent gaps").
    uncited_gaps: list[str] = Field(default_factory=list)


@router.get("/narrative", response_model=NarrativeResponse)
@limiter.limit(heavy_tier)
async def narrative(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    deepen_top: int = Query(5, ge=0, le=10),
    refresh: bool = Query(False, description="Bypass the short-lived replay cache."),
) -> NarrativeResponse:
    settings = get_settings()
    if not settings.narrative_enabled:
        raise HTTPException(status_code=404, detail="Narrative summaries are disabled.")
    if not settings.anthropic_api_key:
        raise HTTPException(
            status_code=503,
            detail="Narrative summaries are not configured (ANTHROPIC_API_KEY is unset).",
        )

    # Reuse the cached lookup pipeline — identical data to the /lookup page.
    resp = await _lookup_impl(lei=lei, deepen_top=deepen_top, refresh=refresh)
    packet = build_evidence_packet(resp.model_dump())

    try:
        # The Anthropic SDK call is blocking; keep it off the event loop.
        result = await asyncio.to_thread(
            summarise,
            packet,
            api_key=settings.anthropic_api_key,
            model=settings.narrative_model,
        )
    except NarrativeUnavailable as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return NarrativeResponse(
        lei=packet.lei,
        subject_name=packet.subject_name,
        summary=result.summary,
        claims=result.claims,
        limitations=result.limitations,
        overall_confidence=result.overall_confidence,
        model=result.model,
        prompt_version=result.prompt_version,
        run_id=compute_run_id(
            packet.lei,
            result.prompt_version,
            result.model,
            result.summary,
            [str(c.get("text", "")) for c in result.claims],
        ),
        generated_at=datetime.now(UTC).isoformat(),
        packet=packet.model_dump(),
        validation_ok=result.validation.ok,
        dropped_claims=result.validation.dropped_claims,
        validation_issues=result.validation.issues,
        uncited_gaps=result.validation.uncited_gaps,
    )


# ---------------------------------------------------------------------------
# Analyst dispositions — the defensible audit trail around a narrative run.
# ---------------------------------------------------------------------------


class DispositionIn(BaseModel):
    """One claim decision as sent by the client (timestamps are server-set)."""

    claim_id: str
    status: DispositionStatus
    comment: str | None = Field(default=None, max_length=2000)


class DispositionsPutRequest(BaseModel):
    lei: str = Field(..., description="ISO 17442 Legal Entity Identifier (20 chars).")
    run_id: str = Field(..., description="run_id of the narrative being signed off.")
    prompt_version: str = ""
    model: str = ""
    dispositions: list[DispositionIn] = Field(default_factory=list)


@router.put("/narrative/dispositions", response_model=DispositionRecord)
@limiter.limit(default_tier)
async def put_dispositions(
    request: Request,
    response: Response,
    req: DispositionsPutRequest,
) -> DispositionRecord:
    """Persist the analyst's claim dispositions for one narrative run.

    Whole-sheet overwrite (last-write-wins; single-analyst v1). ``decided_at``
    per claim and ``updated_at`` are stamped server-side; unchanged claims keep
    their original ``decided_at``. No model call is involved — dispositions are
    pure metadata around an existing narrative.
    """
    record = DispositionRecord(
        lei=req.lei.strip().upper(),
        run_id=req.run_id.strip(),
        prompt_version=req.prompt_version,
        model=req.model,
        dispositions=[
            ClaimDisposition(claim_id=d.claim_id, status=d.status, comment=d.comment)
            for d in req.dispositions
        ],
    )
    try:
        return await asyncio.to_thread(save_dispositions, record)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/narrative/dispositions", response_model=DispositionRecord)
@limiter.limit(default_tier)
async def get_dispositions(
    request: Request,
    response: Response,
    lei: str = Query(..., description="ISO 17442 Legal Entity Identifier (20 chars)."),
    run_id: str = Query(..., description="run_id of the narrative run."),
) -> DispositionRecord:
    """Return the stored disposition sheet for ``(lei, run_id)``, or 404."""
    try:
        record = await asyncio.to_thread(
            load_dispositions, lei.strip().upper(), run_id.strip()
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if record is None:
        raise HTTPException(status_code=404, detail="No dispositions stored for this run.")
    return record
