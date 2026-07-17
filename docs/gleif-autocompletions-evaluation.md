# GLEIF autocompletions vs fulltext — name-search evaluation (issue #27)

**Question.** OpenCheck's `GleifAdapter.search` resolves a company name with the
fulltext filter on the LEI-records endpoint
(`GET /api/v1/lei-records?filter[fulltext]=<name>`). GLEIF also exposes a
dedicated *autocompletions* endpoint
(`GET /api/v1/autocompletions?field=fulltext&q=<name>`) that searches the whole
record (legal name, other names, transliterations, previous names). Issue #27
asked: does autocompletions resolve real-world queries better, and should we
switch or blend the search path?

**Answer: no.** On this fixture the current fulltext path is at least as good as
autocompletions everywhere and strictly better overall and on the previous-name
and transliteration cases the issue expected autocompletions to win. **The search
path in `gleif.py` is left unchanged.** The harness, fixture and numbers below
are the deliverable.

## How to reproduce

From the `backend/` directory:

```bash
python scripts/eval_gleif_autocompletions.py
```

50 queries × 2 endpoints = 100 requests, issued sequentially with a 0.5 s delay
and cached to `scripts/.gleif_eval_cache.json` (git-ignored), so re-runs cost
zero API calls and reproduce the numbers below exactly. `--json out.json` dumps
the per-query result (which LEIs each endpoint returned, and the rank of the
expected LEI). No API key is needed.

## Ground truth

Every expected LEI comes from GLEIF itself, not from assumption. Seed entities
were found by searching GLEIF, their Level-1 records fetched, and their name
fields read directly (`entity.legalName.name`, `entity.otherNames[]`,
`entity.transliteratedOtherNames[]`). Each query string is then either a
**verbatim copy** of one of those fields (legal/other/previous/trading/translit
categories), a documented **leading-words shortening** of such a field
(`*_core`), or a documented **single-character mutation** of such a field
(`typo`). So the LEI↔name mapping is GLEIF's own assertion; the only thing the
harness measures is whether each endpoint surfaces that LEI for that string. Full
per-query provenance is in `backend/scripts/gleif_autocompletions_queries.json`.
Harvested 2026-07-16 via `api.gleif.org`.

**Correction (issue #34).** The Volkswagen ground truth was moved from
`549300PSVDV3P50KHS39` to `529900NNUPAGGOMPXZ31` in both the `legal_name` and
`typo` entries. The first record's legal name matches verbatim, but its GLEIF
registration status is **DUPLICATE** (entity status `NULL`); its successor is the
second, canonical **ACTIVE / ISSUED** record. Re-verified live on 2026-07-17:
with the corrected LEI, fulltext still returns Volkswagen within the top 5
(rank 4 instead of 2) and autocompletions still misses it entirely, so every
hit@1 / hit@5 figure below is unchanged.

## Results (n = 50, run 2026-07-16)

| Endpoint | hit@1 | hit@5 |
|---|---|---|
| **fulltext** (current) | **66.0 %** (33/50) | **74.0 %** (37/50) |
| autocompletions | 58.0 % (29/50) | 66.0 % (33/50) |

Excluding the 13 typo queries (which both endpoints miss entirely — see below),
on the 37 "real name variant" queries:

| Endpoint | hit@1 | hit@5 |
|---|---|---|
| **fulltext** | **89.2 %** (33/37) | **100 %** (37/37) |
| autocompletions | 78.4 % (29/37) | 89.2 % (33/37) |

### By category

| Category (n) | fulltext hit@1 / hit@5 | autocompletions hit@1 / hit@5 |
|---|---|---|
| legal_name (9) | 77.8 % / **100 %** | **88.9 %** / 88.9 % |
| legal_name_native (7) | 85.7 % / **100 %** | 85.7 % / 85.7 % |
| other_name (8) | 100 % / 100 % | 100 % / 100 % |
| previous_name (5) | **100 % / 100 %** | 40.0 % / 80.0 % |
| trading_name (1) | 100 % / 100 % | 100 % / 100 % |
| transliteration (5) | 80.0 % / **100 %** | 60.0 % / 80.0 % |
| transliteration_core (2) | 100 % / 100 % | 50.0 % / 100 % |
| typo (13) | 0 % / 0 % | 0 % / 0 % |

## Reading the numbers

- **Fulltext wins overall** on both hit@1 and hit@5, and reaches **100 % hit@5**
  on every non-typo category. Autocompletions never beats fulltext on hit@5 in
  any category.
- **The issue's core hypothesis did not hold.** Autocompletions was expected to
  help most with previous/trading names and transliterations, but it did *worse*
  there — notably previous_name (hit@1 40 % vs 100 %) and transliteration
  (hit@5 80 % vs 100 %). Fulltext already searches those record fields well.
- **Neither endpoint does fuzzy matching.** All 13 single-character typos miss
  on both endpoints (0 %). Autocompletions is not the fix for typo tolerance;
  closing that gap would need a different mechanism (e.g. an edit-distance /
  trigram fallback) and is out of scope here.
- Autocompletions' one edge is a marginal hit@1 lead on clean Latin legal names
  (88.9 % vs 77.8 %), but it gives that back on hit@5, so it is not a net win for
  a search box that shows a short candidate list.

## Caveats / honesty notes

- `autocompletions` is a *typeahead* endpoint. This harness sends whole typed
  queries (matching how `GleifAdapter.search` is actually called), so it does not
  exercise incremental short-prefix typing, where autocompletions might do
  relatively better. That flow does not exist in OpenCheck today, so it was not
  measured; if a live typeahead UI is ever added, this decision is worth
  revisiting with a prefix-oriented fixture.
- The fixture is 50 queries — enough to make a clear directional call, not a
  statistically tight one. Per-category n is small (1–13). The overall and
  previous-name gaps are large enough to be decisive; treat single-category
  ties as ties.
- All numbers above were produced by the live requests cached in this run; no
  figure is estimated.

## Decision

Keep `GleifAdapter.search` on `filter[fulltext]` **for the primary query** — the
`#27` question (switch to / blend in autocompletions) is a no. No change to the
autocompletions vs fulltext choice. But the typo gap this evaluation surfaced was
taken up as its own follow-up; see below.

The harness and fixture are committed so the comparison can be re-run cheaply if
the GLEIF API behaviour changes.

---

# Follow-up: typo-tolerance fallback (issue #33)

**Question.** The evaluation above found neither GLEIF endpoint does any fuzzy
matching: all 13 single-character-typo queries scored **0 % hit@1 and 0 % hit@5
on both** (`Volkswagon Aktiengesellschaft`, `Nesle S.A.`, `Mitsubushi
Corporation`, `Tencent Holdigs Limited`, …). A one-character slip in a long
legal name returns zero results with no recovery path. Issue #33 asked whether a
cheap **query-relaxation** fallback — retry with tokens dropped once the exact
query returns nothing — can recover typos without hurting clean queries.

**Answer: yes, for a meaningful minority of typos, at zero cost to clean
queries.** A leave-one-out relaxation implemented behind a zero-results trigger
recovers **5 / 13** typo queries at hit@5 (**3 / 13** at hit@1), up from 0 / 13,
and leaves every non-typo result byte-for-byte unchanged. It is implemented in
`GleifAdapter.search` (`opencheck/sources/gleif.py`) and covered by
`backend/tests/test_gleif.py`.

## Strategies measured

Following the issue's "add the fallback as a third endpoint in the harness and
compare against the 0/13 baseline before touching `gleif.py`", two relaxation
strategies were added to `_ENDPOINTS` in `eval_gleif_autocompletions.py`. **Both
fire only when the plain `fulltext` query returns zero results** — so on any
query fulltext already resolves they return the fulltext result unchanged (this
is how the harness proves clean queries are untouched):

- **`relax_droplast`** — progressively drop trailing tokens (`A B C D` → `A B C`
  → … → `A`), taking the first non-empty retry. Cheap, but only helps when the
  typo is near the end.
- **`relax_loo`** — *leave-one-out*: run one fulltext query per token with that
  token dropped, then rank the union by how many leave-one-out variants surfaced
  each LEI (consensus), tie-breaking on best rank. A one-character typo lives in
  exactly one token, so the variant that drops the typo'd token matches on the
  remaining correct tokens — robust to typo position.

## How to reproduce

From `backend/`:

```bash
python scripts/eval_gleif_autocompletions.py
```

The fixture is unchanged (50 queries). The run now issues 4 strategies. The two
relaxation strategies reuse the `fulltext` cache for the plain query and add live
calls only for the relaxed sub-queries of the zero-result queries, all cached to
`scripts/.gleif_eval_cache.json` (git-ignored) so re-runs cost zero API calls.

## Results (n = 50, run 2026-07-17)

| Strategy | overall hit@1 | overall hit@5 | typo hit@1 (n=13) | typo hit@5 (n=13) |
|---|---|---|---|---|
| **fulltext** (primary) | 66.0 % (33/50) | 74.0 % (37/50) | **0 % (0/13)** | **0 % (0/13)** |
| autocompletions | 50.0 % (25/50) | 60.0 % (30/50) | 0 % (0/13) | 0 % (0/13) |
| relax_droplast | 70.0 % (35/50) | 80.0 % (40/50) | 15.4 % (2/13) | 23.1 % (3/13) |
| **relax_loo** (implemented) | **72.0 % (36/50)** | **84.0 % (42/50)** | **23.1 % (3/13)** | **38.5 % (5/13)** |

The overall gain over fulltext is entirely the recovered typos: on **all 37
non-typo queries, `relax_loo` returns exactly the fulltext result** — every
per-category hit@1 / hit@5 for `legal_name`, `legal_name_native`, `other_name`,
`previous_name`, `trading_name`, `transliteration`, `transliteration_core` is
identical to fulltext (verified per-query: 0 of 37 differ), because the trigger
never fires when the primary query resolves. No clean query is degraded, delayed,
or given an extra API call.

`autocompletions` figures re-run fresh on 2026-07-17 sit a little below the
`#27` run (58 % / 66 % → 50 % / 60 %); GLEIF's live ranking drifts between runs.
The 0/13 typo baseline and the fulltext numbers are unchanged.

### Per-typo recovery (`relax_loo`)

| Typo query | fulltext | relax_droplast | relax_loo |
|---|---|---|---|
| Hyundai Motor Compny | — | #1 | **#1** |
| PETROLEO BRASILEIRO S A PETROBAS | — | #1 | **#1** |
| Bayerische Motorn Werke Aktiengesellschaft | — | — | **#1** |
| Banco Santender Mexico | — | — | **#2** |
| Saudi Arabian Oil Compny | — | #2 | **#2** |
| Tencent Holdigs Limited | — | #10 | #6 |
| Roche Holdng AG | — | — | #7 |
| Alibba Group Holding Limited | — | — | — |
| Koninklijke Philps N.V. | — | — | — |
| Volkswagon Aktiengesellschaft | — | — | — |
| Sony Grpup Corporation | — | — | — |
| Mitsubushi Corporation | — | — | — |
| Nesle S.A. | — | — | — |

`relax_loo` dominates `relax_droplast` (recovers the typo wherever it sits, not
only at the end) so it is the one implemented. The 6 unrecovered cases share a
shape leave-one-out cannot fix: the typo is in the **only distinctive token** of
a short name (`Volkswagon`, `Mitsubushi`, `Nesle`, `Alibba`, `Philps`) or the
de-typo'd remainder is too generic to rank the entity (`Sony Grpup` →
`Sony Corporation`). Closing those would need edit-distance / trigram matching
against a name corpus (issue #33 option 2) — larger, and out of scope here.

## Cost / etiquette (this run)

- **150 live GLEIF calls total**, all cached afterwards → re-runs are 0-call and
  reproduce the numbers above exactly. Breakdown: 50 `fulltext` primary + 50
  `autocompletions` + 50 `fulltext` relaxation sub-queries (the `droplast` and
  `loo` variants share the cache). The relaxation cost falls entirely on the 13
  zero-result typo queries; the 37 clean queries added no relaxation calls.
- **In production (`relax_loo` only)** the fallback costs *N* extra fulltext
  calls per zero-result query, where *N* is the token count — here 2–5 (41
  leave-one-out sub-queries across the 13 typo queries, avg 3.15, max 5), issued
  concurrently. Bounded, and only ever on a query that already returned nothing.

## Implementation

`GleifAdapter.search` runs the exact `filter[fulltext]` query first; **only when
it returns zero hits** does it call `_relaxed_search`, which performs the
leave-one-out consensus above and marks each surfaced hit's `summary` with
`approximate match` so callers / UI can flag it as a typo-tolerant suggestion (a
relaxed hit resolves a *different*, shorter query than the user typed — the
marking keeps that honest). The fallback is skipped when live mode is off, when
the query has fewer than two tokens (nothing to drop), or when it has an
implausibly large token count (guards the per-token fan-out). Tests in
`backend/tests/test_gleif.py` pin the trigger (zero → relaxed retry; non-zero →
no retry, no extra call, no marking), the consensus ranking, and the guards;
two mutation self-checks confirm the tests bite (disabling the fallback fails the
trigger tests; firing it on non-zero results fails the no-retry test).

## Decision (issue #33)

Implement `relax_loo` behind the zero-results trigger. It clears the issue's
bar — meaningful typo recovery (0 → 5/13 hit@5), no clean-query degradation
(0/37 changed), bounded extra API calls only on failing queries.
