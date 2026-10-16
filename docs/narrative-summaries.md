# Grounded narrative summaries (Phases 0 + 1)

Adds an LLM-written, **source-cited** narrative summary of an OpenCheck entity
result for a CDD / financial-crime audience. The defining constraint: every
statement in the summary is grounded in OpenCheck's own data — nothing is
inferred, retrieved, or asserted beyond what the sources say. This is enforced
mechanically, not just by prompt wording.

## How grounding is guaranteed

The model never sees raw sources and never retrieves anything. The pipeline is:

1. **`build_evidence_packet()`** distils a lookup/report result into an
   `EvidencePacket` — atomic, already-evidenced **facts** (each carrying its
   source name, adapter id, BODS statement ids and a confidence derived from
   source authority), structured **risk items**, **sources consulted** (with
   licence), and **gaps** (absences that matter for due diligence). This packet
   is the *only* thing the model sees.
2. **`summarise()`** asks Claude (structured tool output, low temperature) for a
   single executive paragraph plus per-claim citations, each claim referencing
   evidence ids (`f`/`r`/`g`) from the packet.
3. **`validate_narrative()`** drops any claim citing an unknown id or nothing at
   all, and withholds the paragraph on violation. A claim that can't be tied to
   packet evidence never reaches the user.

Absence is treated as evidence: a clean entity carries a synthesised "the risk
engine ran and found nothing" fact, and every gap is itself a citable item, so
the model can state "no beneficial owner was disclosed" or "no risks were found"
*with* a citation rather than fabricating one.

## Phase 0 — offline core (commit `4fa5296`)

- `opencheck/narrative/`: `packet.py`, `prompt.py` (versioned system prompt +
  structured-output schema), `summarise.py` (Anthropic call, gated on
  `ANTHROPIC_API_KEY`), `validate.py` (citation validator).
- `scripts/eval_narrative.py`: offline eval harness over six synthetic golden
  packets, scoring a machine-checkable rubric (grounded / names subject / flags
  name-matches / surfaces risks / surfaces gaps / no raw codes / length / within
  caps). This is
  where prompt wording is iterated before any UI exists.
- `tests/golden_narrative/`: six synthetic packets (clean, sanctioned related
  party, no-PSC gap, name-matched/sparse, complex ownership layers, errored
  source). No PII.
- `tests/test_narrative.py`: offline builder + validator tests.
- Config: `ANTHROPIC_API_KEY`, `OPENCHECK_NARRATIVE_MODEL` (default
  `claude-sonnet-4-6`); `anthropic` added as a dependency.

Prompt is tuned to **compliance-analyst tone, one executive paragraph**: risks
described as structural/jurisdictional indicators (never determinations of
wrongdoing), confidence stated per signal, name-matches and incomplete screening
caveated rather than smoothed over, raw signal codes kept out of the prose.

## Phase 1 — endpoint + UI

**Backend** — `GET /narrative?lei=…`:

- Reuses the **same cached lookup pipeline** as `/lookup`, so the narrative can
  never describe a different result than the page shows.
- Builds the packet, runs `summarise` off the event loop (`asyncio.to_thread`),
  validates, and returns the summary, surviving claims, limitations and the full
  packet (so the UI can resolve cited ids to evidence).
- Flag- and key-gated: `404` when `OPENCHECK_NARRATIVE_ENABLED=false`, `503`
  when no API key.
- No token streaming by design: grounding can only be checked once the whole
  structured answer exists, so streaming raw tokens would put unvalidated text on
  screen. A "stream after validation" path can be added later without changing
  the contract.
- Facts and risks now carry the adapter `source_id` (via a REGISTRY name→id map)
  so citation chips link to the right source card reliably.

**Frontend** — on-demand `NarrativePanel` at the top of the entity result page:

- "Generate summary" button — no model call until the user asks.
- Renders the paragraph, an **Evidence** list with per-claim **citation chips**
  (labelled by source, with a confidence dot), a limitations block, and a
  disclaimer naming the model + prompt version.
- Clicking a chip scrolls to and flashes the originating source card, and emits
  an `oc:cite` event the BODS graph listens for — expanding and highlighting the
  cited statement node when present.

## Tests

- Backend: full suite green (**1953 passed**, 10 skipped, 5 xfailed), including
  new builder, validator and endpoint tests.
- Frontend: `tsc -b` and `vite build` both clean.

## How to run

```bash
# backend: put ANTHROPIC_API_KEY in backend/.env, then
cd backend && uv run uvicorn opencheck.app:app --reload
# frontend
cd frontend && npm run dev
# offline prompt iteration
cd backend && uv run python scripts/eval_narrative.py --show
```

## Output size is bounded (prompt v10)

Rosneft's summary (`253400JT3MQWNDKMJE44`) answered **502** on 14 Oct 2026:
the `emit_summary` call stopped at `max_tokens`, and `summarise.py` refuses a
truncated answer rather than ship a summary with no claims. It was intermittent
— the packet (86 risks, 131 facts, ~38k input tokens) sat just under the
ceiling. Measured on 15 Oct under prompt v9: 2,663–3,292 output tokens across
six runs, against `MAX_TOKENS = 4000`. The output was the paragraph written
twice — a ~2,250-character summary (4–6 sentences of 300–500 characters each)
restated across 16–25 claims — and it grew with the number of risks.

Two levers, both in place:

- **`MAX_TOKENS = 8000`** — headroom, not the bound. A ceiling costs nothing
  unless the tokens are generated.
- **Caps in the prompt and the tool schema** (`prompt.py`):
  `SUMMARY_MAX_WORDS` (180; the prompt states it in words, because a
  characters-only cap was overrun on every run), `SUMMARY_MAX_CHARS` (1,500,
  the outer limit — `maxLength` in the schema), `MAX_CLAIMS` (12, `maxItems`)
  and `MAX_LIMITATIONS` (6). Many items of one kind are summarised by kind with
  a count and at most three examples; a claim about a group cites every id it
  covers. The API treats the schema limits as guidance, so the validator
  records an overshoot in `validation_issues` — it never drops a grounded
  claim to meet a cap.

Measured on the same Rosneft packet under v10: 1,736–1,974 output tokens
(eight runs), 1,398–1,513-character summaries, 12 claims, ~30 s instead of
45–55 s. Eli Lilly: ~1,710 output tokens, 1,114 characters, 11 claims.

`summarise._raw_tool_output` also decodes `claims` / `limitations` sent as one
JSON-encoded string (one v10 run did), and refuses text that is not a list.
`tests/golden_narrative/07_many_related_party_risks.json` is a synthetic packet
of Rosneft's shape (88 risks, no real names) so the eval harness exercises the
heavy case; the harness's `within_caps` column and its per-packet output-token
line show how close each packet comes to the ceiling.

## Follow-ups (Phase 2)

- Variance check: run each golden packet 3–5× to confirm wording stability before
  trusting the prompt in production.
- Reuse the packet for the PDF export.
- A/B Sonnet vs Opus via `OPENCHECK_NARRATIVE_MODEL`; broaden the golden set.
- Optional "stream the paragraph after validation" for perceived latency.
