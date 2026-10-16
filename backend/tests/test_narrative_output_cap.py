"""The AI summary's output is bounded whatever the packet's size.

Rosneft's summary answered 502 on 14 Oct 2026: the emit_summary call stopped
at ``max_tokens`` (4000), and the Phase 111 guard refused the claimless
result. Measured on 15 Oct under prompt v9, Rosneft's packet (86 risks, 131
facts, ~38k input tokens) produced 2,663-3,292 output tokens across six runs:
a ~2,250-character paragraph, restated again across 16-25 claims. The fix is
two levers: more headroom (``MAX_TOKENS``) and a bound on what the model
writes (``SUMMARY_MAX_CHARS`` / ``MAX_CLAIMS`` / ``MAX_LIMITATIONS``, in the
prompt and the tool schema). These tests pin both offline; whether a live
model obeys the caps is the eval harness's job (``within_caps``, run against
``07_many_related_party_risks.json``, which has Rosneft's shape).
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path

from opencheck.narrative import EvidencePacket, validate_narrative
from opencheck.narrative.prompt import (
    MAX_CLAIMS,
    MAX_LIMITATIONS,
    PROMPT_VERSION,
    SUMMARY_MAX_CHARS,
    SUMMARY_MAX_WORDS,
    SUMMARY_TOOL,
    SYSTEM_PROMPT,
    build_user_message,
)

# The package re-exports the ``summarise`` *function*; the module is what
# holds MAX_TOKENS.
prompt_mod = importlib.import_module("opencheck.narrative.prompt")
summarise_mod = importlib.import_module("opencheck.narrative.summarise")

GOLDEN = Path(__file__).parent / "golden_narrative" / "07_many_related_party_risks.json"


def test_max_tokens_has_headroom_over_the_measured_rosneft_output():
    # 3,292 was the largest Rosneft output measured under prompt v9; the
    # ceiling must sit well clear of it even before the caps bite.
    assert summarise_mod.MAX_TOKENS >= 8000
    assert summarise_mod.MAX_TOKENS >= 2 * 3292


def test_the_prompt_states_every_cap_with_its_value():
    out = SYSTEM_PROMPT[SYSTEM_PROMPT.index("OUTPUT"):]
    assert f"AT MOST {SUMMARY_MAX_WORDS} words" in out
    assert f"{SUMMARY_MAX_CHARS} characters" in out
    assert f"AT MOST {MAX_CLAIMS} claims" in out
    assert f"{MAX_LIMITATIONS} `limitations`" in out
    # Grouping instruction: many items of one kind are counted, not listed.
    assert "with a count" in out
    assert "Never list every party" in out
    # The f-string left no unformatted placeholder behind.
    assert "{" not in SYSTEM_PROMPT and "}" not in SYSTEM_PROMPT


def test_the_tool_schema_carries_the_same_caps():
    props = SUMMARY_TOOL["input_schema"]["properties"]
    assert props["summary"]["maxLength"] == SUMMARY_MAX_CHARS
    assert props["claims"]["maxItems"] == MAX_CLAIMS
    assert props["limitations"]["maxItems"] == MAX_LIMITATIONS
    assert str(SUMMARY_MAX_CHARS) in props["summary"]["description"]
    assert str(MAX_CLAIMS) in props["claims"]["description"]


def test_prompt_version_was_bumped_for_the_caps():
    assert PROMPT_VERSION == "2026-10-15-v10"
    assert prompt_mod.PROMPT_VERSION == summarise_mod.PROMPT_VERSION


def _packet(*ids: str) -> EvidencePacket:
    return EvidencePacket(
        subject_name="X",
        facts=[{"id": i, "statement": f"fact {i}", "source_name": "S"} for i in ids],
    )


def test_an_overshoot_is_recorded_but_never_drops_grounded_claims():
    packet = _packet("f1")
    claims = [
        {"id": f"c{n}", "text": "X.", "fact_ids": ["f1"], "confidence": "high"}
        for n in range(MAX_CLAIMS + 3)
    ]
    v = validate_narrative(
        packet,
        {"summary": "X." * SUMMARY_MAX_CHARS, "claims": claims, "overall_confidence": "high"},
    )
    assert v.ok
    assert len(v.valid_claims) == MAX_CLAIMS + 3
    assert v.summary  # the paragraph is grounded, so it is kept
    assert any(f"exceed the cap of {MAX_CLAIMS}" in i for i in v.issues)
    assert any(f"over the cap of {SUMMARY_MAX_CHARS}" in i for i in v.issues)


def test_output_within_the_caps_records_no_cap_issue():
    packet = _packet("f1")
    v = validate_narrative(
        packet,
        {
            "summary": "X is a thing.",
            "claims": [{"id": "c1", "text": "X.", "fact_ids": ["f1"], "confidence": "high"}],
            "overall_confidence": "high",
        },
    )
    assert not any("cap" in i for i in v.issues)


def test_the_golden_set_has_a_packet_of_rosneft_shape():
    """The eval harness must exercise a heavily sanctioned group, or the next
    one finds the ceiling in production as Rosneft did."""
    packet = EvidencePacket.model_validate(json.loads(GOLDEN.read_text()))
    related = [r for r in packet.risks if r.code.startswith("RELATED_")]
    assert len(packet.risks) >= 85
    assert len(related) >= 70
    assert len(build_user_message(packet)) >= 50_000
    # The same EU-list signal once per source, as production emits it today.
    eu = [r for r in packet.risks if r.code == "EU_HIGH_RISK_THIRD_COUNTRY"]
    assert len({r.source_id for r in eu}) == len(eu) >= 5


def test_summarise_sends_the_ceiling_and_reports_output_tokens(monkeypatch):
    sent: dict = {}

    class _Block:
        type = "tool_use"
        name = "emit_summary"
        input = {
            "summary": "X is a thing.",
            "claims": [{"id": "c1", "text": "X.", "fact_ids": ["f1"], "confidence": "high"}],
            "overall_confidence": "high",
        }

    class _Message:
        stop_reason = "tool_use"
        content = [_Block()]
        usage = types.SimpleNamespace(input_tokens=100, output_tokens=321)

    class _Messages:
        def create(self, **kwargs):
            sent.update(kwargs)
            return _Message()

    class _Client:
        def __init__(self, api_key):
            self.messages = _Messages()

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=_Client))
    result = summarise_mod.summarise(_packet("f1"), api_key="test-key")
    assert sent["max_tokens"] == summarise_mod.MAX_TOKENS
    assert sent["tools"] == [SUMMARY_TOOL]
    assert result.output_tokens == 321
    assert result.prompt_version == PROMPT_VERSION


class _Block:
    type = "tool_use"
    name = "emit_summary"

    def __init__(self, payload):
        self.input = payload


class _Message:
    stop_reason = "tool_use"

    def __init__(self, payload):
        self.content = [_Block(payload)]


def test_claims_sent_as_json_text_are_decoded():
    """One Rosneft run on 15 Oct 2026 sent ``claims`` as one JSON string; the
    validator iterated its characters."""
    claims = [{"id": "c1", "text": "X.", "fact_ids": ["f1"], "confidence": "high"}]
    raw = summarise_mod._raw_tool_output(
        _Message(
            {
                "summary": "X is a thing.",
                "claims": json.dumps(claims),
                "limitations": json.dumps(["One gap."]),
                "overall_confidence": "high",
            }
        )
    )
    assert raw["claims"] == claims
    assert raw["limitations"] == ["One gap."]
    assert validate_narrative(_packet("f1"), raw).ok


def test_claims_text_that_is_not_a_list_is_refused():
    import pytest

    with pytest.raises(summarise_mod.NarrativeUnavailable, match="not a list"):
        summarise_mod._raw_tool_output(
            _Message({"summary": "X.", "claims": "c1: X is a thing", "overall_confidence": "low"})
        )
