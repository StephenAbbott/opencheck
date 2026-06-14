"""Call Claude to turn an ``EvidencePacket`` into a grounded narrative.

The model is given the packet as its sole evidence and must answer via the
``emit_summary`` tool (structured output). Low temperature, no streaming here
(Phase 0). The result is *always* passed through ``validate_narrative`` before
it is returned, so a caller never receives an ungrounded claim.

The Anthropic call is gated on a key: with no key (the offline default) this
raises ``NarrativeUnavailable`` so the eval harness and tests degrade cleanly.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .packet import EvidencePacket
from .prompt import PROMPT_VERSION, SUMMARY_TOOL, SYSTEM_PROMPT, build_user_message
from .validate import ValidationResult, validate_narrative

DEFAULT_MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1200


class NarrativeUnavailable(RuntimeError):  # noqa: N818 — public name, kept stable
    """Raised when no API key is configured."""


class NarrativeResult(BaseModel):
    summary: str
    claims: list[dict[str, Any]] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    overall_confidence: str = "low"
    model: str
    prompt_version: str = PROMPT_VERSION
    validation: ValidationResult


def _raw_tool_output(message: Any) -> dict[str, Any]:
    """Pull the ``emit_summary`` tool input out of an Anthropic message."""
    for block in message.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "emit_summary":
            return dict(block.input)
    raise NarrativeUnavailable("model did not call emit_summary")


def summarise(
    packet: EvidencePacket,
    *,
    api_key: str | None = None,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.0,
) -> NarrativeResult:
    """Generate and validate a narrative for ``packet``.

    ``api_key`` is required; pass ``settings.anthropic_api_key``. Never hard-code
    or log the key.
    """
    if not api_key:
        raise NarrativeUnavailable(
            "ANTHROPIC_API_KEY is not set — narrative generation is disabled."
        )

    import anthropic  # lazy import; optional dependency

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        temperature=temperature,
        system=SYSTEM_PROMPT,
        tools=[SUMMARY_TOOL],
        tool_choice={"type": "tool", "name": "emit_summary"},
        messages=[{"role": "user", "content": build_user_message(packet)}],
    )

    raw = _raw_tool_output(message)
    validation = validate_narrative(packet, raw)
    return NarrativeResult(
        summary=validation.summary,
        claims=validation.valid_claims,
        limitations=raw.get("limitations") or [],
        overall_confidence=validation.overall_confidence,
        model=model,
        prompt_version=PROMPT_VERSION,
        validation=validation,
    )
