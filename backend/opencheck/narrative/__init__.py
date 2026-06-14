"""LLM narrative summaries of OpenCheck results (Phase 0 — offline scaffold).

The feature turns an OpenCheck lookup/report into a short, compliance-grade
narrative where **every claim is grounded in a cited source**. The design keeps
the model on a tight leash:

1. ``build_evidence_packet`` distils a result into atomic, already-evidenced
   facts + structured risk items + sources + gaps. This packet is the *only*
   thing the model sees — it never retrieves or infers.
2. ``summarise`` asks Claude (structured tool output) to write a single
   executive paragraph plus per-claim citations, each claim referencing fact ids
   from the packet.
3. ``validate_narrative`` mechanically enforces grounding: any claim citing a
   fact id not in the packet (or citing nothing) is rejected — so "no
   unprovable information" is a guarantee, not a hope.

Phase 0 ships the packet builder, prompt, schema, validator and an offline eval
harness. The live Claude call is gated on ``ANTHROPIC_API_KEY``.
"""

from __future__ import annotations

from .packet import (
    EvidencePacket,
    Fact,
    Gap,
    RiskItem,
    SourceRef,
    build_evidence_packet,
)
from .summarise import (
    NarrativeResult,
    NarrativeUnavailable,
    summarise,
)
from .validate import ValidationResult, validate_narrative

__all__ = [
    "EvidencePacket",
    "Fact",
    "Gap",
    "RiskItem",
    "SourceRef",
    "build_evidence_packet",
    "validate_narrative",
    "ValidationResult",
    "summarise",
    "NarrativeResult",
    "NarrativeUnavailable",
]
