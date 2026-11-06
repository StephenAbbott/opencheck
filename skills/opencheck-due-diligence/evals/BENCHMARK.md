# Benchmark: opencheck-due-diligence, with and without the skill

Run on 5 November 2026 against the twelve cases in `evals.json`, on Claude
Opus 5.5 and Claude Haiku 5.5. NVIDIA's Auto Ontology `BENCHMARK.md` gave us the
idea; this one is smaller and says plainly what it cannot show.

## Result

| | Opus 5.5 with skill | Opus 5.5 without | Haiku 5.5 with skill | Haiku 5.5 without |
|---|---|---|---|---|
| Assertions passed | **52 / 53 (98%)** | 49 / 53 (92%) | **51 / 53 (96%)** | 48 / 53 (91%) |
| Factually wrong claims (checked against the recorded response) | **1** | 5 | **5** | 8 |
| Skill opened unprompted | 12 / 12 | n/a | 8 / 12 (12 / 12 after the description fix below) | n/a |
| Mean time per case | 41 s | 37 s | 34 s | 37 s |

Per case (passed / total; `w` = wrong claims):

| Case | Opus + skill | Opus | Haiku + skill | Haiku |
|---|---|---|---|---|
| degraded-batch-row-is-not-clean | 5/5 | 4/5 w1 | 5/5 w1 | 5/5 w2 |
| shell-name-matches-and-truncated-screen | 5/5 w1 | 5/5 | 5/5 | 4/5 |
| lapsed-lei-is-not-an-active-company | 4/4 | 4/4 w1 | 4/4 | 4/4 |
| no-lei-goes-to-register-lookup | 5/5 | 5/5 | 5/5 | 5/5 |
| gleif-consolidation-is-not-ownership | 3/4 † | 4/4 | 3/4 ‡ | 4/4 |
| per-source-figures-are-never-added | 4/4 | 4/4 | 3/4 † | 4/4 |
| sanctions-ownership-test-is-not-a-designation | 4/4 | 3/4 w1 | 4/4 w1 ‡ | 4/4 w2 |
| form-13f-managers-are-not-owners | 5/5 | 5/5 | 5/5 w2 | 5/5 w2 |
| context-pep-is-not-a-finding | 4/4 | 4/4 | 4/4 ‡ | 3/4 w2 |
| thin-answer-is-the-registers-limit | 3/3 | 3/3 | 3/3 | 3/3 |
| bulk-exploration-goes-to-datasets | 5/5 | 4/5 w2 | 5/5 | **2/5** |
| bods-relationships-reference-record-ids | 5/5 | 4/5 | 5/5 w1 ‡ | 5/5 |

† A miss the skill caused or did not prevent; the skill was changed afterwards
(see "Changes after the run") and the case passed on both models when re-run.
‡ Haiku did not open the skill in this run, so the "with skill" answer is
effectively a second baseline.

## What the skill changed

- **Bulk exploration** is the clearest gain. Without the skill, Haiku wrote a
  paced loop of 200 live lookups (about 40 minutes of the room's shared budget)
  and pointed only vaguely at the releases page; Opus wrote its own BODS loader
  and suggested the non-commercial dataset could be filtered for commercial
  use. With the skill both named the `dataset-azerbaijani-laundromat` release,
  its ready-made Neo4j bundle and its non-commercial licence.
- **Fewer wrong statements.** Without the skill the answers called a
  counter-designation "a sanctioned counterparty", added two bank stakes into
  "close to all of the company", put a GLEIF reporting exception in an
  "Owners" table, and labelled one company "high priority". The skill runs
  made 1 (Opus) and 5 (Haiku) such errors against 5 and 8.
- **Context versus risk** on Haiku: without the skill it led with "9 people …
  are flagged as politically exposed"; with it, "no confirmed PEP", then the
  seven own-role listings as context and two name-only matches.

## What it did not change, and why that matters

On most cases both configurations passed. The main reason is a good one:
OpenCheck's responses already explain themselves. Each lookup carries a
`summary` that says "CAUTION: … not a clean screen", signals say "Possible name
match only", the owners tool says "never add them", the subsidiaries list says
which children it leaves out, and since 5 November 2026 the MCP handshake
carries the reading rules too. An agent that reads the response carefully gets
most of it right. NVIDIA reported 47.7% → 100% because their tools return raw
results; OpenCheck's baseline starts above 90%.

The skill's job is therefore the cases the response cannot speak to — what to
do *instead* of calling the server, how to parse an export, how to word a table —
and making a smaller model behave like a larger one.

## How it was run

- **Fixtures, not live calls.** Every case is built on a real response recorded
  from `api.opencheck.world/mcp` on 5 Nov 2026 (`evals/files/`). Runs read the
  file instead of calling the server, so the benchmark is repeatable and cost the
  lookup budget nothing.
- **Clean sessions.** Each run is `claude -p` in an empty directory with no
  CLAUDE.md, no user settings and no MCP servers; the OpenCheck handshake
  instructions are appended to the system prompt, as any MCP client would receive
  them. "With skill" installs the skill under `.claude/skills/` and leaves it to
  the model to open — the description has to do its job. Pre-approved tools:
  read, write, edit, search, and `python3`.
- **Blind grading.** A separate Claude agent graded each case's two answers as
  "A" and "B" in random order, checked every claim against the recorded response
  and ran any script the answer wrote.
- **A discarded first run.** The first attempt used sub-agents of the session
  that built the skill. They had the OpenCheck repository's `CLAUDE.md` in
  context, which contains every rule the skill teaches, so the "without skill"
  arm was not a baseline. That run is not reported.

## Limits

- One run per cell, so a single case moving one assertion is within noise; the
  totals and the wrong-claim counts are the signal.
- Several assertions do not discriminate, because the fixture states the rule
  they check (the graders flagged these). Harder cases would ask for something
  the response does not already say.
- The graders are Claude too. Blinding guards against favouring one arm, not
  against a shared blind spot.
- Claude only, by decision. No other agent was tested.

## Changes after the run

1. **Description.** Haiku skipped the skill on a BODS export, a subsidiaries
   list, a PEP count and the sanctions question — prompts that named OpenCheck
   output but not "owners" or "due diligence". The description now says to use
   the skill whenever reading, summarising, counting or coding over any OpenCheck
   output. Re-run: Haiku opened it on all four.
2. **One state, several bodies.** Haiku with the skill refused to say that
   FINANSDEPARTEMENTET, "Government of Norway" and the trade ministry are one
   holding, over-applying "never add across sources". The skill now says that
   different bodies of one state are readings of one holding. Re-run: both models
   say "one holding read three ways … not 201%".
3. **Subsidiary tables.** Opus with the skill put an outside "50/50 JV" figure in
   a percentage column. The skill now says to leave percentage columns out of a
   GLEIF-based table, and that the list is never "all subsidiaries". Re-run: both
   models drop the column and say what the list leaves out.

## Re-running

The runner, blinding and grading prompts are in `evals/runner/`. From a machine
with Claude Code:

```bash
cd skills/opencheck-due-diligence/evals/runner
./run_all.sh claude-opus-5-5     # writes ./workspace/<model>/…
```

Grading is a judgement step: give each case's `eval_metadata.json`, the two
anonymised answers and the fixture to a grader (`grader_prompt.txt`).
