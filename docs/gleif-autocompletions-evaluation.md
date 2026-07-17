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

Keep `GleifAdapter.search` on `filter[fulltext]`. No change to `gleif.py`. The
harness and fixture are committed so the comparison can be re-run cheaply if the
GLEIF API behaviour changes.
