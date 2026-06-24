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
  name-matches / surfaces risks / surfaces gaps / no raw codes / length). This is
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

## Follow-ups (Phase 2)

- Variance check: run each golden packet 3–5× to confirm wording stability before
  trusting the prompt in production.
- Reuse the packet for the PDF export.
- A/B Sonnet vs Opus via `OPENCHECK_NARRATIVE_MODEL`; broaden the golden set.
- Optional "stream the paragraph after validation" for perceived latency.
