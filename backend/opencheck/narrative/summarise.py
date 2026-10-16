"""Call Claude to turn an ``EvidencePacket`` into a grounded narrative.

The model is given the packet as its sole evidence and must answer via the
``emit_summary`` tool (structured output). Low temperature, no streaming here
(Phase 0). The result is *always* passed through ``validate_narrative`` before
it is returned, so a caller never receives an ungrounded claim.

The Anthropic call is gated on a key: with no key (the offline default) this
raises ``NarrativeUnavailable`` so the eval harness and tests degrade cleanly.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from .packet import EvidencePacket
from .prompt import PROMPT_VERSION, SUMMARY_TOOL, SYSTEM_PROMPT, build_user_message
from .validate import ValidationResult, validate_narrative

DEFAULT_MODEL = "claude-sonnet-4-6"
#: Generous ceiling for the emit_summary tool call. 1200 was too tight for the
#: prompt-v6 output: the call was truncated mid-`claims`, the API dropped the
#: incomplete fields, and every downstream default kicked in silently — a
#: summary with ZERO claims (so the UI's evidence section disappears) and
#: overall_confidence "low". Five of the six Phase 111 curated files shipped
#: that way before anyone noticed. Truncation is now also a hard error below.
#: 4000 then proved too tight for heavily sanctioned groups: Rosneft (86
#: risks, 131 facts, ~38k input tokens) measured 2,663-3,292 output tokens
#: across six runs on 15 Oct 2026 under prompt v9 and one run on 14 Oct went
#: past 4000 (a 502). 8000 is headroom, not the bound: the prompt and tool
#: schema cap what the model writes (``SUMMARY_MAX_CHARS`` / ``MAX_CLAIMS`` in
#: prompt.py), so output stays roughly flat however large the packet is. The
#: ceiling costs nothing unless the tokens are generated.
MAX_TOKENS = 8000


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
    #: Tokens the model generated, so the eval harness can show how close a
    #: packet comes to ``MAX_TOKENS``. ``None`` when the client reports none.
    output_tokens: int | None = None


def _raw_tool_output(message: Any) -> dict[str, Any]:
    """Pull the ``emit_summary`` tool input out of an Anthropic message.

    A ``max_tokens`` stop is a hard error, never a partial result: the API
    drops the incomplete tool-input fields, which here silently strips the
    claims (and with them the UI's whole evidence section) while leaving a
    plausible-looking summary. Better no narrative than an uncited one.
    """
    if getattr(message, "stop_reason", None) == "max_tokens":
        raise NarrativeUnavailable(
            "narrative generation was truncated at max_tokens — the claims "
            "would be lost; raise MAX_TOKENS or shorten the packet"
        )
    for block in message.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "emit_summary":
            return _decode_list_fields(dict(block.input))
    raise NarrativeUnavailable("model did not call emit_summary")


def _decode_list_fields(raw: dict[str, Any]) -> dict[str, Any]:
    """Undo a list the model sent as JSON text.

    The tool schema types ``claims`` and ``limitations`` as arrays, but the
    model sometimes emits an array as one JSON-encoded string (seen on one
    of four Rosneft runs on 15 Oct 2026: ``claims`` arrived as 3,788
    characters of text). The validator iterates ``claims`` and would fail on
    the first character, so the text is decoded here; text that is not a
    JSON list is refused like any other unusable answer — never passed on as
    a claimless summary.
    """
    for key in ("claims", "limitations"):
        value = raw.get(key)
        if not isinstance(value, str):
            continue
        try:
            decoded = json.loads(value)
        except ValueError:
            decoded = None
        if not isinstance(decoded, list):
            raise NarrativeUnavailable(
                f"model returned {key} as text that is not a list — the claims would be lost"
            )
        raw[key] = decoded
    return raw


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
        output_tokens=getattr(getattr(message, "usage", None), "output_tokens", None),
    )
