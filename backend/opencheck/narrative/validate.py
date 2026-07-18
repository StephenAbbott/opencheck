"""Mechanical citation validator — the trust mechanism.

The LLM only ever rephrases the packet, but we never *trust* that: every claim
it returns must cite at least one ``fact_id`` that actually exists in the packet.
Claims that cite nothing, or cite an unknown id, are dropped and recorded as
issues. A narrative is only shown if it survives validation, so "no unprovable
information" is enforced by code, not by prompt wording.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .packet import EvidencePacket


class ValidationResult(BaseModel):
    ok: bool
    valid_claims: list[dict[str, Any]] = Field(default_factory=list)
    dropped_claims: list[dict[str, Any]] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    # The summary paragraph, kept only if it survived (see below).
    summary: str = ""
    overall_confidence: str = "low"
    # Gap ids (``g*``) present in the packet but cited by no surviving claim.
    # "Clear fallbacks, not silent gaps": the narrative is *required* to state
    # what could not be verified, so an uncited gap is surfaced as an issue and
    # the UI/PDF render the gap list directly from the packet regardless — a
    # model failure can never hide a gap.
    uncited_gaps: list[str] = Field(default_factory=list)


def validate_narrative(
    packet: EvidencePacket,
    result: dict[str, Any],
    *,
    drop_summary_on_violation: bool = True,
) -> ValidationResult:
    """Validate an ``emit_summary`` tool result against the packet.

    A claim is valid iff it cites a non-empty set of ids that are all present in
    ``packet.evidence_ids()`` — i.e. facts (``f`` …), risks (``r`` …) or gaps
    (``g`` …). Gaps and absence-of-risk are legitimate, evidenced findings, so a
    claim about them is grounded by citing the relevant gap/fact id.
    """
    known = packet.evidence_ids()

    valid: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    issues: list[str] = []

    for claim in result.get("claims") or []:
        cites = list(claim.get("fact_ids") or [])
        if not cites:
            dropped.append(claim)
            issues.append(f"claim {claim.get('id', '?')!r} cites no ids")
            continue
        unknown = [c for c in cites if c not in known]
        if unknown:
            dropped.append(claim)
            issues.append(
                f"claim {claim.get('id', '?')!r} cites unknown id(s): {', '.join(unknown)}"
            )
            continue
        valid.append(claim)

    ok = not dropped
    summary = result.get("summary", "")
    if dropped and drop_summary_on_violation:
        # The paragraph asserts something we couldn't ground — withhold it. The
        # caller can re-prompt or fall back to a claims-only rendering.
        summary = ""

    # Gap-citation rule: every gap in the packet should be acknowledged by at
    # least one surviving claim. Uncited gaps don't invalidate the narrative
    # (``ok`` tracks ungrounded claims only) — they are recorded so callers can
    # render the packet's gap list explicitly instead of trusting the prose.
    cited: set[str] = set()
    for claim in valid:
        cited.update(claim.get("fact_ids") or [])
    uncited_gaps = sorted(g for g in packet.gap_ids() if g not in cited)
    for g in uncited_gaps:
        issues.append(f"gap {g!r} is not cited by any claim")

    return ValidationResult(
        ok=ok,
        valid_claims=valid,
        dropped_claims=dropped,
        issues=issues,
        summary=summary,
        overall_confidence=result.get("overall_confidence", "low"),
        uncited_gaps=uncited_gaps,
    )
