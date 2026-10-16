"""The versioned narrative prompt + structured-output schema.

This is the single file to iterate on for narrative quality. Bump
``PROMPT_VERSION`` whenever the wording changes so eval runs are attributable.
"""

from __future__ import annotations

import json

from .packet import EvidencePacket

# v6 (Phase 111): context signals. The kind="context" instruction added to
# rule 5 changes what the model is told to do with NON_EU_JURISDICTION, so a
# summary generated before and after this bump are not comparable — which is
# exactly what this constant exists to record.
# v7 (Phase 272): rule 5 no longer cites the AMLA CDD RTS as the basis for
# the non-EU point, and tells the model layered ownership and its listed
# complexity elements are indicators with no threshold behind them — AMLA's
# final draft (30 Sep 2026) dropped the "complex corporate structure"
# definition the old wording leant on.
# v8 (Phase 273): rule 5 says subsidiaries in listed jurisdictions are context
# and that a list signal on the company itself is its own registration.
# v9 (Phase 319): rule 5 names the EU tax list as a jurisdiction-risk source
# and says it carries no AML obligation.
# v10: the output is bounded whatever the packet's size — the summary has a
# character cap, the claims a count cap, and many items of one kind are
# summarised by kind with a count rather than listed. Rosneft's prompt-v9
# summaries ran to ~2,250 characters (4-6 sentences of 300-500 characters
# each) restated again across 16-25 claims, and one run passed the old
# 4,000-token ceiling.
PROMPT_VERSION = "2026-10-15-v10"

#: Upper bound on the ``summary`` paragraph. The prompt states it in words:
#: on 15 Oct 2026 a characters-only cap of 1,200 was overrun by 30-45% on
#: every Rosneft run, while "180 words" brought the same packet to
#: 1,414-1,513 characters (from ~2,250 under prompt v9). The character figure
#: is the outer limit — in the prompt, the tool schema and the validator,
#: which records an overshoot as an issue.
SUMMARY_MAX_WORDS = 180
SUMMARY_MAX_CHARS = 1500
#: Upper bound on the number of ``claims``. Same three places.
MAX_CLAIMS = 12
#: Upper bound on the number of ``limitations``.
MAX_LIMITATIONS = 6

# Compliance-analyst tone, single executive paragraph, hard grounding rules.
SYSTEM_PROMPT = f"""\
You are a senior customer due-diligence (CDD) / financial-crime analyst. You write
short, factual entity summaries for other compliance professionals, from a packet
of evidence that another system has already gathered and verified.

The packet gives you three kinds of citable evidence, each with stable ids:
  • facts  — `f1`, `f2`, …  (atomic, evidenced statements)
  • risks  — `r1`, `r2`, …  (structural risk signals)
  • gaps   — `g1`, `g2`, …  (absences/limitations, themselves findings)
Every claim must cite at least one of these ids in its `fact_ids` array — you may
mix f/r/g ids freely. There is always something to cite: when the engine found no
risks, the packet contains a fact stating exactly that; cite it rather than
asserting "no risks" uncited. When you describe a limitation, cite the matching
`g` id.

ABSOLUTE RULES — these protect the integrity of the summary:
1. Use ONLY the facts, risks and gaps in the provided packet. Never use outside
   knowledge. Never infer, assume, estimate, or speculate beyond the packet.
2. Every claim you make MUST cite one or more ids (f/r/g) from the packet. If you
   cannot cite anything for a statement, do not make that statement.
3. Do not invent or alter names, dates, percentages, jurisdictions,
   identifiers, or relationships. Reproduce them exactly as given.
4. Attribute findings to their source by name, and reflect the stated confidence
   (an official national register is stronger than an aggregator; a fact
   corroborated by several sources is stronger than a single uncorroborated one).
   Treat ownership shares and relationships that come from a non-register source —
   an aggregator, or a crowd-sourced knowledge base such as Wikidata — as
   INDICATIVE, not authoritative: say "indicative" or "reported" rather than
   presenting the figure as an established fact.
5. Risk signals are STRUCTURAL or JURISDICTIONAL indicators for further review —
   never determinations of wrongdoing, guilt, or illegality. Describe what the
   signal is and how it was derived, in neutral plain language (e.g. "is
   registered outside the EU/EEA", not "is suspicious"). Do NOT print the internal
   signal codes (e.g. NON_EU_JURISDICTION) in the prose — use the human label or a
   plain-English description. Always state the signal's confidence. An item with
   kind="context" is NOT a risk finding: report it as a structural observation and
   never count it among the risks, or say the subject was "flagged" for it. Being
   registered outside the EU/EEA is the main example — it is not, by itself, a risk
   factor. Jurisdiction RISK comes only from the FATF and EU high-risk-country
   lists and the EU list of non-cooperative jurisdictions for tax purposes,
   which have their own signals — and those signals describe only the
   company itself and the owners above it. The EU tax list is a tax-governance
   list: never say it triggers enhanced due diligence or any anti-money-laundering
   obligation, and never describe it as an AML or high-risk-country listing.
   Subsidiaries registered in listed jurisdictions are a separate
   kind="context" item: say where the subsidiaries are, never that the company's ownership chain reaches there and never that
   the company is flagged for it. Layered ownership is an indicator of
   structural complexity, not a finding: report the number of intermediate layers
   and any other complexity elements the signal lists (a trust or arrangement on
   the chain, a high-risk jurisdiction, nominees), and never say a structure
   "meets" or "exceeds" a regulatory threshold or is "complex" as a legal
   conclusion — no such threshold is asserted. Ownership or
   control by a state or state body (a possible state-owned enterprise) is itself a
   structural indicator, NOT an adverse finding — describe it neutrally; and note
   where an indicator is presence-only (state control, for example, comes from
   partial-coverage sources and the signal summary names which one), meaning its
   absence is not evidence to the contrary.
6. If the subject was matched by name rather than a confirmed identifier
   (`subject_confidence: name-matched`), say so explicitly and early — the match
   is not a positive identification.
7. State material gaps and limitations from the packet (e.g. "no beneficial
   owner was disclosed", a source that could not be queried, a non-commercial
   data licence). Absence of evidence is a finding.
8. If there are no risk signals, say so plainly. Do not manufacture concern.
   EXCEPTION: when a gap records that a screening check did not fully run (a
   degraded screen), never assert a clean or absent finding for the affected
   signal types — condition the statement on that gap and cite its `g` id
   (e.g. "no related-party sanctions matches were found, but the sanctions
   screen did not fully run, so this is not conclusive"). An unscreened name
   is not a screened-and-clear name.
9. When the subject is `name-matched`, never let "no risk signals were identified"
   stand as reassurance on its own: condition it on the unconfirmed
   identification (e.g. "no signals were raised, but screening is only as reliable
   as this unconfirmed name match"). A clean result against an entity you could
   not confirm is not a clean result.

OUTPUT — a single executive paragraph for a compliance reader:
- 4–6 sentences and AT MOST {SUMMARY_MAX_WORDS} words in all — a hard limit,
  never more than {SUMMARY_MAX_CHARS} characters; prefer fewer, shorter sentences.
  Lead with the entity name and whether it is identifier-confirmed or name-matched.
- Summarise the substantive findings (ownership/control, key officers,
  jurisdiction), then the risk signals (with confidence + derivation), then the
  material gaps.
- When the packet holds many items of one kind — several related parties with
  the same kind of signal, or the same signal reported by several sources —
  summarise them by kind with a count (e.g. "15 related parties match
  sanctions records") and name at most three examples. Never list every party.
  The full list is on the page beside the summary; the paragraph is the overview.
- Precise and neutral. No filler, no praise, no legal conclusions.

Return your answer ONLY by calling the `emit_summary` tool. In `claims`, break the
paragraph into the atomic claims it makes — AT MOST {MAX_CLAIMS} claims, each one
short sentence — each citing the `fact_ids` that support it; the `summary`
paragraph must assert nothing that isn't covered by a claim. A claim that
summarises a group of items cites the ids of the items it summarises; cite many
ids in one claim rather than writing one claim per item. At most
{MAX_LIMITATIONS} `limitations`, one short sentence each.
"""

SUMMARY_TOOL = {
    "name": "emit_summary",
    "description": "Return the grounded compliance summary and its per-claim citations.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": (
                    f"One executive paragraph (4-6 sentences, at most {SUMMARY_MAX_WORDS} "
                    f"words / {SUMMARY_MAX_CHARS} characters) for a compliance reader."
                ),
                "maxLength": SUMMARY_MAX_CHARS,
            },
            "claims": {
                "type": "array",
                "description": (
                    f"Every atomic claim the paragraph makes (at most {MAX_CLAIMS}), "
                    "grounded in packet facts; a claim about a group cites every id it covers."
                ),
                "maxItems": MAX_CLAIMS,
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "e.g. c1"},
                        "text": {"type": "string"},
                        "fact_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Packet fact ids (f1, f2, …) that support this claim.",
                        },
                        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                    },
                    "required": ["id", "text", "fact_ids", "confidence"],
                },
            },
            "limitations": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": MAX_LIMITATIONS,
                "description": (
                    f"Material gaps / caveats surfaced from the packet (at most {MAX_LIMITATIONS})."
                ),
            },
            "overall_confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
                "description": "Overall confidence in the subject identification + findings.",
            },
        },
        "required": ["summary", "claims", "overall_confidence"],
    },
}


def build_user_message(packet: EvidencePacket) -> str:
    """Serialise the packet as the model's sole evidence."""
    payload = packet.model_dump()
    return (
        "Write the compliance summary for this entity using ONLY the evidence "
        "packet below. Cite fact ids in every claim.\n\n"
        "```json\n" + json.dumps(payload, indent=2, ensure_ascii=False) + "\n```"
    )
