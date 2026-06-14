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
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..config import get_settings
from ..narrative import build_evidence_packet
from ..narrative.summarise import NarrativeUnavailable, summarise
from .lookup import lookup

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
    # The packet is returned so the UI can resolve each claim's cited ids
    # (f/r/g) back to the underlying fact, risk or gap for citation chips.
    packet: dict[str, Any]
    # Surfaced for transparency / debugging — what (if anything) was rejected.
    validation_ok: bool = True
    dropped_claims: list[dict[str, Any]] = Field(default_factory=list)
    validation_issues: list[str] = Field(default_factory=list)


@router.get("/narrative", response_model=NarrativeResponse)
async def narrative(
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
    resp = await lookup(lei=lei, deepen_top=deepen_top, refresh=refresh)
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
        packet=packet.model_dump(),
        validation_ok=result.validation.ok,
        dropped_claims=result.validation.dropped_claims,
        validation_issues=result.validation.issues,
    )
