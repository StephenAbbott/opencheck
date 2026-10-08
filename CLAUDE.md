# OpenCheck — development notes for Claude

## Local commands (macOS)

Use **`python3`**, not `python`, in all documented commands and examples — macOS
ships Python 3 as `python3` and has no bare `python` on the PATH (`python …`
fails with `command not found`). The same applies to any one-off scripts and the
test suite below.

## After every commit: post the local run commands

After making **any** git commit during a session, post (in the chat) the commands
the user needs to bring the stack up locally on the branch just committed to, so
they can test immediately. The workspace is mounted from the user's disk, so the
commits already exist locally — the user **checks out** the branch, they don't
fetch/pull from origin.

Template (fill in `<branch>`):

```
cd ~/code/opencheck
rm -f .git/*.lock 2>/dev/null            # clear any leftover sandbox lock files
git checkout <branch>

# Backend (one terminal):
cd backend && uv sync && uv run uvicorn opencheck.app:app --reload --port 8000

# Frontend (another terminal):
cd frontend && npm install && npm run dev
```

Notes to add when relevant: uvicorn `--reload` picks up backend changes
automatically, but the Vite dev server must be **restarted** to pick up new files
or `vite.config.ts` / `.env.local` changes; `.env.local` already proxies the API
to `http://127.0.0.1:8000`; `uv sync` / `npm install` are only needed when
dependencies changed but are harmless to run otherwise.

---

## Architecture overview

- **Backend**: FastAPI, split into `backend/opencheck/routers/` (health, search, lookup, export).
- **Frontend**: React + Tailwind, split into `frontend/src/components/` (icons, risk, export, cdd).
- **Sources**: each adapter lives in `backend/opencheck/sources/<name>.py`, registered in `sources/__init__.py`.
- **BODS mapping**: each adapter has a corresponding `map_<name>()` function in `bods/mappers/<country or source>.py` (Phase 246), re-exported from `bods/mapper.py` and exported from `bods/__init__.py`.

---

## Open Knowledge Format (OKF) bundle — `okf/`

OpenCheck ships an **[OKF v0.1](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
knowledge bundle** at `okf/` — a directory of markdown files with YAML
frontmatter that lets humans and AI agents understand the project, its data
sources, the BODS/LEI standards, and the API. OKF is "metadata as code": every
concept has a required `type` field, cross-links are plain markdown links, and
`index.md` / `log.md` are reserved filenames (see the spec §3–§9).

Structure: `overview.md`, `architecture.md`, `glossary.md` (project);
`standards/` (BODS v0.4, LEI/GLEIF anchoring); `api/` (one concept per
endpoint); `sources/` (one **Data Source** concept per registered adapter);
`licensing/matrix.md`.

**Two halves:**

- **Hand-authored** narrative concepts (project / standards / api). Edit these by
  hand.
- **Auto-generated** from the live registry: `sources/*.md`, `sources/index.md`,
  `licensing/matrix.md`, `licensing/index.md`. **Do not hand-edit these** — they
  are produced by the generator below and pull `SourceInfo` + `licensing.classify`.

**Tooling (in `backend/scripts/`):**

- `generate_okf.py` — the "enrichment agent". Regenerates the auto concepts from
  the registry. `--check` validates OKF conformance **and** that the generated
  concepts are in sync with the registry (timestamp lines are ignored in the
  drift comparison). Run it (without `--check`) and commit after adding/changing
  a source.
- `generate_okf_viz.py` — renders the whole bundle to a self-contained
  `okf/viz.html` (Cytoscape graph + rendered markdown; CDN-loaded, no backend).
  Regenerate after editing concepts.

**CI:** `.github/workflows/vendored-enum-drift.yml` has an `okf` job that installs
the backend and runs `generate_okf.py --check`, so a stale bundle (e.g. a new
source not regenerated) fails the build — alongside the vendored-enum drift jobs.

---

## Phase 8 — Licensing & AuraDB deferral (recorded 2026-06-07)

### Demo data licences

The `data/demo/` graph is assembled from two freely-shareable published
BODS v0.4 datasets. The combined graph is freely usable in talks,
blog posts, and derivative works under the most restrictive of the two
licences, OGL v3.0:

| Dataset | Licence |
|---|---|
| UK PSC (Companies House via Open Ownership) | OGL v3.0 |
| GLEIF L1 + L2 (GLEIF via Open Ownership) | CC0 1.0 |

Both licences are permissive and compatible. OGL v3.0 requires
attribution; CC0 does not. Pipeline code
(`bods-uk-psc-pipeline`, `bods-gleif-pipeline`) is AGPL-3.0 but is
**not** included in OpenCheck — OpenCheck only reads their published
BODS output. No AGPL obligations apply to OpenCheck.

Full attribution wording and source URLs: `data/demo/LICENCES.md`.

### AuraDB / hosted Neo4j — explicitly parked

**Decision (2026-06-07):** Do **not** move to a hosted Neo4j AuraDB
instance or adopt any embedded graph DB (Kuzu, Memgraph, MemGQL) as a
dependency of OpenCheck's runtime at this time.

**Rationale:** The demo use-case (curated 9-entity set, one-off
build, slides + local Neo4j Docker) is fully served by the current
stack: SQLite extraction → BODS JSON-Lines → `bods-neo4j` CSV → local
Neo4j. Adding a hosted graph DB introduces cost, network dependency,
and operational complexity before any evidence that DuckDB + the
curated set cannot handle the traversal load.

**Named revisit trigger:** Revisit when either:
1. A user-facing traversal query (multi-hop UBO resolution in the live
   `/lookup` flow) measurably exceeds 2 s median latency on the
   full-entity BODS data **with** DuckDB, **or**
2. The demo set grows beyond ~200 anchor entities and
   `extract_bods_subgraphs.py` + in-memory dedup becomes a bottleneck.

Until one of those triggers fires, the architecture stays: SQLite
source-of-truth → BODS JSON-Lines → Neo4j Docker for demos only.

---

## Current state (Phase 46)

### National ID search (frontend-only, Phase 46)

Three-tab search panel: **Company name** | **National ID** | **Paste an LEI**.

The National ID tab lets users enter a local company registration number and
resolve it to a LEI via GLEIF reverse lookup, then run the full OpenCheck
lookup automatically.

Key files:

| File | Purpose |
|---|---|
| `frontend/src/lib/raCodes.ts` | RA codes, labels, placeholders, format regexes for 21 countries, plus the GB sub-registry rules. Export: `RA_CODES`, `COUNTRY_OPTIONS`, `raCodeFor()`, `validateNationalId()`. Mirrors `backend/opencheck/ra_codes.py`; `backend/tests/test_ra_codes.py` parses this file and fails if they diverge |
| `frontend/src/lib/gleifNationalId.ts` | `searchByNationalId(raCode, id)` — fires three GLEIF filter endpoints in parallel (`registeredAs`, `validatedAs`, `otherValidationAuthorities.validatedAs`), deduplicates by LEI |

How it works:
1. User selects country → country picker resolves to an RA code (e.g. GB → RA000585)
2. User enters registration number → `searchByNationalId()` queries all three GLEIF filter fields scoped to that RA code
3. Single result → auto-navigates to `/lookup-stream`; multiple results → picker; zero results → amber notice with "try by name" fallback

Format validation is advisory (non-blocking). The amber border + warning fires only after `onBlur` (`nationalIdTouched` state) so it doesn't interrupt typing. GLEIF may store IDs in a normalised form that differs from the raw input — always allow submission.

**Pure frontend change — no backend routes added or modified.**

---

## Current state (Phase 45)

**Test suite**: see "Test suite" below — the current totals are in the closing paragraph of `docs/status.md`, not here (a count in this file went stale for two hundred phases).

**Frontend graph renderer**: Cytoscape.js (replaced `@openownership/bods-dagre` in Phase 44). Component: `frontend/src/components/BODSGraph.tsx`. Uses a React HTML overlay layer for BOVS icons and flags — never use Cytoscape's `background-image` for icons (canvas taint from Adobe Illustrator `xmlns:xlink` SVGs). BOVS icons are base64 data URIs in `frontend/src/lib/bovsIcons.ts`. Flags are served from `frontend/public/bods-dagre-images/flags/`. The overlay recomputes on `cy.on('viewport')`. Flag badges are at 45° NE circumference; risk signal badges at 315° NW.

**Risk signal overlays**: BOVS Option C implemented. `buildSignalMap()` in BODSGraph.tsx reads `evidence.statement_id` (SANCTIONED/PEP), `evidence.subject_statement_id` (RELATED_*), `evidence.matches[].statement_id` (TRUST/AMLA), `evidence.jurisdictions[].statement_id` (FATF/NON_EU), `evidence.longest_path[]` (COMPLEX_OWNERSHIP_LAYERS). Single signal → labelled pill at 315° NW; multiple → "N ⚠" stack badge.

**Estonian adapter**: `ariregister.py` is now a public web scraper — `GET ariregister.rik.ee/eng/company/{reg}/company_print_json`. No credentials. The previous SOAP/X-Road approach (Phase 37) used `ariregxmlv6.rik.ee` with `ARIREGISTER_USERNAME`/`ARIREGISTER_PASSWORD` credentials from a paid RIK contract that turned out not to grant data access. Do NOT revert to SOAP. The HTML parser extracts officers (→ Estonian role codes), shareholders (person vs entity from ID code length), and BOs — Estonia's legitimate-interest access restriction was postponed on its 2026-07-10 start date, so BO data remains available (degradation for the eventual switch is pinned by `_HTML_BO_WITHDRAWN` tests). `map_ariregister()` in mapper.py is unchanged.

**GLEIF RA code for Estonia**: `RA000181` (confirmed from live GLEIF data — the CLAUDE.md table below has a typo: RA000198 is wrong, RA000181 is correct).

---

## Lookup architecture: ONE pipeline drives both /lookup and /lookup-stream (Phase 47)

`routers/lookup.py` has a single async generator, `_lookup_pipeline()`, that
resolves the GLEIF anchor, builds derived identifiers, dispatches adapters,
converts results to SourceHits, deepens and assesses risk. It yields
`(event, payload)` tuples; `/lookup-stream` serialises them as SSE and
`/lookup` collects them into a `LookupResponse`. The endpoints **cannot
diverge** — the old hand-synchronised sync/SSE copies (and the
Corporations Canada regression `603c086` they caused) are gone.

**Adapters are self-describing.** Each national-register adapter declares its
lookup wiring on its own class (see `sources/base.py`):

```python
class BrregAdapter(SourceAdapter):
    id = "brreg"
    lookup_derivers = (
        LookupDeriver(frozenset({NO_RA_CODE}), "no_orgnr", normalise_orgnr),
    )
    lookup_pass_legal_name = True
```

`routers/lookup.py` builds `_RA_DERIVERS` and `_REGISTRY_SOURCES` from the
REGISTRY at import time; an adapter that declares lookup keys without a
matching `_bh_<name>()` hit builder raises at import. Special cases:
`lookup_dispatch_keys` overrides the dispatch key when it is derived
elsewhere (rpvs_slovakia reuses rpo's `sk_ico`; companies_house uses the GB
jurisdiction special case). BODS mappers are found by convention —
`opencheck.bods.map_<source_id>` — there is no `_MAPPERS` dict.

`tests/test_lookup_pipeline.py` enforces all of this (deriver keys must have
dispatch specs, specs must match adapter declarations, mappers must exist,
missing builders fail fast) and pins sync/stream parity.
`tests/test_sources.py` discovers adapter modules from the filesystem — no
hand-maintained expected-source lists anywhere. Deliberately unregistered
bulk/offline adapters are allowlisted in `_DELIBERATELY_UNREGISTERED`.
LEI-keyed sources (opensanctions, openaleph, climatetrace, bods_gleif) and
SEC EDGAR are handled inside `_dispatch()` / `_lookup_pipeline()` directly.

### Risk-signal instrumentation (Phase 110)

`opencheck/signalstats.py` counts risk signals per `(code, source_id)`,
`degraded_sources` per `(source_id, check, reason)`, and completed pipeline
runs — exposed at `GET /signalstats`, modelled on the `memwatch` → `/memstats`
pair (public, unauthenticated, `no-store`, no rate limit, aggregate only).
It answers "which sources actually contribute which signals" without the
client-side sweep that alternative would require — a sweep loads a free-tier
instance, risks rate limits whose degraded results read as "signal absent",
pulls CC BY-NC data at volume for analytics, and samples whichever LEIs were
picked.

Three things to know before touching it:

- **Counting lives inside `_merge_signals`**, so "count after dedup" is true
  by construction — the rules deciding what a distinct signal *is* are in
  that function, and related-party paths emit several signals per hit, so
  pre-dedup numbers overstate.
- **`record_as` is opt-in (`None` by default).** `_merge_signals` has two
  callers: the pipeline (counted, `record_as="lookup"`) and `/report`, a
  hand-run debugging endpoint (not counted). Counting `/report` would
  inflate the per-lookup denominator with debugging traffic. A new caller
  therefore cannot skew the numbers merely by existing.
- **`lookups` counts pipeline runs, not sessions.** Replayed runs are served
  from the replay cache and never reach the pipeline.

Privacy is structural, not policed: the recorders read only `code` /
`source_id` / `check` / `reason` — never `summary`, `hit_id`, `evidence` or a
degradation's free-text `detail` — so names and LEIs cannot reach a counter.
`test_signalstats.py` enforces that with names stuffed into every free-text
field, and end-to-end through a real lookup. Counters are in-process and
reset on deploy and Render spin-down; making them durable means scraping the
endpoint periodically, which is a separate decision.

### Source health on the sources page (Phase 161)

The weekly sweep (`scripts/source_health.py`, `.github/workflows/source-health.yml`)
publishes `source-health.json`, `source-health.md` and a rolling
`source-health-history.json` to the `source-health-latest` GitHub release —
the entity-pages arrangement, a URL an ephemeral-filesystem host reads
without a rebuild. `opencheck/source_health.py` reads them back for
`GET /source-health` (hourly refresh, stale-and-say-so on error,
`available: false` when nothing has been published) and *shapes* the report:
statuses, reasons, known gaps, liveness, latency, statement totals and the
last eight statuses per source; never `observed_fields` or `result_size`.
`frontend/src/lib/sourceHealth.ts` words it (`ok` → Healthy in the ok tone,
`degraded` → Degraded in the *context* tone, `fail` → Failed in the *warn*
tone, `skipped` → Not tested in neutral — never healthy, never omitted) and
`components/SourcesPage.tsx` renders it. Nothing at request time probes a
source: the page shows the last sweep's verdict and says when it was reached.
`OPENCHECK_SOURCE_HEALTH_FILE` points a developer at a local sweep.

### Findings regression (Phase 277)

The source-health sweep asserts each source is *alive*; the findings
regression asserts what the engine *finds*. `.github/workflows/findings-regression.yml`
(Mondays 08:30 UTC + Run workflow) runs `scripts/findings_regression.py`
against the **deployed** API — `/lookup?refresh=true`, then the MCP
`opencheck_lookup` replay of the same run — for the golden set in
`backend/findings_golden/` (six curated examples, Shell, Maersk, Bank Saderat
PLC, ASDA Stores, two clean controls). Rules that keep it honest:

- **Golden files are lower bounds and shapes, never snapshots.** A genuine
  upstream change is fixed by editing the subject's file in a reviewed PR.
- **A curated golden file and an `EXAMPLE_LEIS` card exist together or not at
  all** — `test_curated_golden_files_are_exactly_the_homepage_cards`. When you
  change a card, the weekly run checks it against production (`card_drift`);
  when you add or drop a curated example, add or drop its golden file.
- **Every finding names its failure class** (`kind_mismatch`,
  `structural_repeated`, `degraded_reads_clean`, `placeholder_badge`,
  `card_drift`, …; full table in `docs/findings-regression.md`). Label-map
  coverage is *not* here — `tests/test_signal_label_coverage.py` does it
  offline on every PR.
- **The diff is against the last published report** (`findings-regression-latest`
  release), not the last successful run, and codes whose rules moved between
  two reports are labelled "(rule change)" from the watchlist's
  `VERDICT_TEMPLATE` / `SIGNAL_RULES` stamps. When a phase changes what a rule
  emits, bump those stamps as the watchlist already requires, and the weekly
  diff will say so instead of reporting drift.
- **A hand-built signal dict must carry `kind`.** Consumers default a missing
  one to risk, which hides the gap; the regression reports it as
  `kind_mismatch` (it caught `SANCTIONED_SECURITY`).

### Cold start & per-source time budgets (Phase 47)

- The FastAPI lifespan kicks off `climatetrace.warm_caches()` (and, since, the
  other bulk-index warm-ups in `app._warm_caches_background`) in a
  background thread at startup — unless `OPENCHECK_WARM_CACHES_ON_START=0`,
  which the test suite sets (Phase 266) — so Render cold starts pre-download/parse
  the GEM CSVs, GLEIF GEM↔LEI mapping and GEOT artifact before the first
  lookup. Warm-up failures are logged and non-fatal (lazy fallback).
  The climatetrace adapter's index builds run via `asyncio.to_thread` —
  never on the event loop.
- Every adapter has a `lookup_timeout_s` wall-clock budget (default 30 s,
  declared on the class). The pipeline cancels and emits a
  `source_error` with `error_type: "timeout"` when exceeded. Overrides:
  cvr_denmark 90 s (Datafordeler is slow by design), openaleph 60 s
  (strategy cascade). Budgets are capped sanity-tested in
  `tests/test_lookup_pipeline.py` (must be ≤ 120 s).

### OpenAleph: FtM /match step, percolation + mentions enrichment

- The OpenAleph strategy cascade is: leiCode → OC URL → registration
  numbers → **FtM `POST /api/2/match`** → **percolate name
  (`POST /api/2/beta/percolate`)** → free-text `q=` name fallback.
  The match step converts the subject to an FtM Company via
  `opencheck/ftm.py` — bods-ftm's `entity_statement_to_ftm()` when
  installed (the `ftm` extra; Docker + CI ship the ICU toolchain
  g++/libicu-dev/pkg-config that followthemoney → pyicu needs), else a
  built-in converter with parity-tested identical output. **Requires
  `OPENALEPH_API_KEY`** — the flagship edge 405s anonymous POSTs to
  /match even though the app route allows them; without the key the step
  is skipped silently.
- Match-acceptance gating in `match_entity()`: hits whose own properties
  corroborate a subject identifier (leiCode / registrationNumber /
  opencorporatesUrl) are always kept, flagged
  `raw["identifier_corroborated"]` and ranked first; others survive only
  at ≥ 25% of the top hit's score (relative — FtM/BM25 scores vary with
  name length/rarity, so never use absolute thresholds).
- Text-based percolation (OpenAleph 5.3.1, Phase 96 — the endpoint
  OpenCheck requested in [openaleph/openaleph#105](https://github.com/openaleph/openaleph/issues/105)):
  `percolate_text()` POSTs arbitrary text to `/api/2/beta/percolate`
  (beta-namespaced upstream; path lives in `_PERCOLATE_PATH`) and returns
  the stored entities whose name-percolator queries fire on it, each with
  `percolator_match` / `surface_forms` / `score`. **`None` ≠ `[]`**:
  `None` = screen could not run (no key — the edge 405s anonymous POSTs
  like /match — or 404 pre-5.3.1, or HTTP failure); `[]` = ran, nothing
  matched. `fetch_by_name_percolate()` uses it as the subject-name
  strategy (schema=LegalEntity, `_bears_name`-gated — percolation matches
  partial names, slop 2); the name goes as raw JSON body text, **never**
  through the Lucene query_string parser, so the reserved-syntax bug
  class (quotes / `A/S` / dangling `+`) can't occur on this path. The
  `q=` fallback stays for keyless deployments. Hard-won live findings
  (2026-08-13): always pass a selective filter — unfiltered/LegalEntity
  percolation over famous names drowns in near-duplicate registry
  records, while `filter:schema=Person` is high-precision; latency ~1.8 s
  unfiltered on the 2.1M-entity flagship vs ~10 ms topic-scoped.
- Mentions enrichment (OpenAleph 5.3 `/entities/{id}/mentions`): fetched
  once per distinct normalised *name* (two fetches max, Phase 158) and
  applied to every hit carrying that name — "· mentioned in N documents" +
  `raw.openaleph_mentions` (title/collection/category/url per doc). Mentions
  are name-derived, so same-name records share them; enriching by *hit*
  left a third same-name record without the line and it could not group
  with the two above it. Informational only — never identifier
  corroboration.
- Related-party graph screening (`opencheck/openaleph_check.py`, Phase 97):
  `assess_openaleph_names(bods, degraded=, screening=)` runs in the risk
  stage alongside `assess_cross_source_names` / `assess_icij_names` (same
  `asyncio.gather`). ALL related-party names → **two** percolation calls:
  persons broad (`filter:schema=Person` — measured high-precision), entities
  topic-scoped (`_WATCHLIST_TOPICS` — unfiltered entity percolation drowns
  in registry-record noise). `surface_forms` → `names.normalise_name` →
  `subject_statement_id` attribution; then the cross_check gates (0.88
  similarity vs the hit's own names, single-token person guard, birth-year)
  and the cross_check topic ladder → `RELATED_*` signals with
  `source_id="openaleph"` (graph badges work unchanged). Gated matches with
  no signal-mapping topic (poi, corp.disqual, leak/court collections) go to
  the `screening` out-collector → `openaleph_screening` on the
  `risk_signals` event / LookupResponse / ReportResponse → the "Archive
  matches — OpenAleph" section in `components/cdd/QuickCheckPanel.tsx`. Informational, never identifier
  corroboration. No key / HTTP failure → `DegradedSource` records
  (issue #50) — never a silent clean screen. OS+OA duplicate signals for
  the same node are deliberately kept (dedupe keys include source_id) —
  **except `RELATED_PEP`**, merged per upstream record since Phase 247
  (see "PEP signals" below).

### Replay cache, shareable URLs, per-source retry (Phase 47)

- Completed pipeline runs are cached in memory for 15 min
  (`_REPLAY_CACHE`, keyed `LEI:deepen_top`, 64 entries max) and replayed by
  both endpoints; `?refresh=true` bypasses. Only runs that reach `done` are
  cached. Tests must not leak cache entries across fixtures — a conftest.py
  autouse fixture clears it around every test.
- `GET /lookup-source?lei=&source_id=` re-runs one source (per-source retry
  in the UI) via `_resolve_ctx()` + `_dispatch(ctx, only=...)`, and
  invalidates the replay cache for that LEI.
- Frontend: lookups are addressable via `?lei=` (pushState + popstate
  handling in App.tsx — query param, not a path, so no static-host rewrite
  rules needed). A mid-stream connection drop after `gleif_done` keeps
  partial results and shows a "Resume lookup" banner; failed source cards
  get a "Retry source" button wired to `/lookup-source`.

### Checklist for a new adapter

- [ ] `sources/<name>.py` — adapter class with `lookup_derivers` /
      `lookup_pass_legal_name` declared on the class
- [ ] `sources/schemas/<name>.py` — Pydantic bundle schema
- [ ] `sources/__init__.py` — import + REGISTRY entry
- [ ] `bods/mappers/<country>.py` — `map_<name>()` in its own module (a new
      country, or the existing module for that country), re-exported from
      `bods/mapper.py` (+ `bods/__init__.py` export). **Not in `mapper.py`
      itself** — Phase 246 moved every per-source section out and
      `tests/test_mapper_modules.py` fails on a new `map_*` defined there. A
      module imports from `..statements` and its siblings, never from
      `..mapper` (a cycle; the same test checks it). Phase 168 moved the shared
      statement factories to `bods/statements.py`.
      The register's own entity-type wording ("Local Company", "Public")
      goes to `entityType.details` via `make_entity_statement(entity_details=…)`,
      **never** `entityType.subtype` — a closed v0.4 codelist. A registry-wide
      guard in `tests/conftest.py` fails any test whose mapper emits an
      invalid subtype (Phase 214)
      The same guard (Phase 239) fails a test whose mapper emits a
      `jurisdiction.code` / `nationalities[].code` that is not ISO 3166, an
      identifier with an `id` and no `scheme`, or v0.3's
      `incorporatedInJurisdiction` — see "The risk engine's input contract"
- [ ] `routers/hit_builders.py` — `_bh_<name>()` hit builder (only this).
      Moved out of `routers/lookup.py` in Phase 168 and re-exported from it
- [ ] `tests/test_<name>.py` — adapter + mapper tests
- [ ] `.env` — API key if required (never committed)
- [ ] `README.md` + `ATTRIBUTIONS.md` — document the source
- [ ] `docs/sources.md` — add the adapter row (keep it in sync with `REGISTRY`;
      the active table = `REGISTRY` minus env-gated bulk-only adapters), and
      refresh the source counts in `README.md` (intro paragraph + adapter-table
      pointer line) and the social card `docs/social/opencheck-social-b.html`
- [ ] **Frontend homepage source count** — two hard-coded counts, in **two
      different files**: the hero subline in `frontend/src/App.tsx` ("…from N
      sources into one graph…") **and** the "How it works" step-3 title in
      `frontend/src/components/HomePanels.tsx` ("N open sources, in parallel").
      Easy to miss — separate from the README/social-card ones. This checklist
      said both were in `App.tsx` until the `eiti_assessment` adapter went
      looking for the second one: the step-3 title moved to `HomePanels.tsx`
      with the homepage-panels extraction and nothing here moved with it.
      `og_image.py` needs no edit — `_source_count()` reads the REGISTRY.
- [ ] **Regenerate the OKF bundle** — run `python3 backend/scripts/generate_okf.py`
      and `python3 backend/scripts/generate_okf_viz.py`, then commit the resulting
      `okf/` changes **in the same commit as the adapter**. The CI `okf` job runs
      `generate_okf.py --check` and fails on drift, so a new/changed source that
      isn't regenerated breaks the build (this is what broke the four commits after
      `malta_mbr`). `--check` ignores the `timestamp:` line, so restore the
      timestamp on otherwise-unchanged source concepts to avoid committing pure
      churn — only `sources/<name>.md` (new), `sources/index.md`,
      `licensing/matrix.md` and `viz.html` should carry real changes.

---

## Available skills

Two Cowork skills are available and should be used proactively:

- **`/beneficial-ownership-data`** — use for any questions about beneficial ownership data, policy, registers, the BODS standard, FATF, EU AML, GLEIF→BODS mapping, OpenOwnership, or BO data in procurement/extractives.
- **`/gleif-data`** — use for any questions about LEIs, the GLEIF registry, LEI issuers (LOUs), registration authorities, ownership relationships in GLEIF, or LEI statistics. Has live access to the GLEIF API and Statistics MCP servers.

---

## Identifier corroboration rule for `SourceHit.identifiers`

When building a `SourceHit` in `routers/lookup.py`, only include an identifier in `identifiers` if the source **independently publishes or validates** that identifier. The reconciler (`reconcile.py`) uses the presence of an identifier across multiple hits to assert cross-source corroboration — putting a borrowed identifier on a hit that doesn't actually contain it creates a false confirmation in the UI.

Specific rules:

- **`wikidata_qid`** — only on the **Wikidata** hit. Companies House and GLEIF do not publish Wikidata mappings; omitting it from their hits was fixed in commits `3454a36` and `fbc458e`.
- **`lei`** — only on hits from sources that independently publish or validate LEIs (e.g. GLEIF, OpenCorporates). Do not propagate `lei` from the derived dict to registry adapters (CH, KvK, etc.) that received it as a lookup key rather than asserting it themselves.
- When in doubt: if the source's own data payload doesn't contain the identifier, don't put it in `identifiers`.

---

## `docs/status.md` and the README Status section

Every phase ships an update to both files. Get the shape exactly right —
these have been broken repeatedly by edits that looked harmless.

### `docs/status.md` is ONE unbroken markdown table

- **Never insert a blank line between phase rows.** A blank line terminates
  the table on GitHub: every row after it renders as raw pipe-text outside
  the table, which is how new phases have silently stopped appearing
  (cleaned up in `7e9f69e`).
- One row per phase, one line per row — `| 137 | <headline> |`. No wrapping,
  no hard line breaks inside a headline (they run to several hundred
  characters, and that is correct). Escape a literal `|` as `\|`.
- Rows are **appended in ascending phase order**, directly under the previous
  row. Never re-sort, never split the table, never put a sub-heading between
  rows. Inject a new row with an append / `awk`, never with an editor step or
  a heredoc that re-emits surrounding lines — that is what has introduced the
  blank lines.
- **Every row must end with a commit citation** — `Commit \`hash\`.` or
  `Commits \`a\`, \`b\`.` (short hashes in backticks). The `/changelog` page
  derives its GitHub links from exactly this clause (`extractCommits` in
  `frontend/src/lib/changelog.ts`); a row without it renders on the changelog
  with no link. Phases 68–70 dropped the convention and lost their links
  (restored in `6ecc723`).
- The whole file contains exactly **three** blank lines: after the H1, after
  the intro sentence, and before the closing test-suite paragraph.
  `grep -c '^$' docs/status.md` returning anything but `3` means the table is
  broken.
- Bump the spelled-out phase count in the intro sentence in the same edit
  ("OpenCheck has shipped through one hundred and thirty-eight phases"). It
  goes stale silently.

### README Status section — one phase, one line

```markdown
## Status

**Latest: Phase N** — one-line summary of the main change in that phase.

→ [Full development history](docs/status.md)
```

The summary is a single sentence of plain prose, with no commit hash and no
PR number — a compressed version of the status.md row's opening clause.
Replace the Latest line each phase; do not accumulate `Previous:` / `Earlier:`
tiers beneath it.

### Verify on GitHub, not in the app

`parseStatusMarkdown` walks status.md line by line and skips anything that
isn't a row, so **the `/changelog` page renders perfectly even when the GitHub
table is broken**. A clean changelog is not evidence. Look at the rendered
`docs/status.md` and `README.md` on GitHub, or run the `grep -c` above.

---

## A failure is never cached as "no record" (Phase 262)

`Cache.put_absent(key)` is the only way to remember that an upstream has no
record, and only for a **definitive** answer — HTTP 404, or 402/403 for an
endpoint the account's tier lacks. `Cache.get_payload` stops honouring a
cached absence after `cache.ABSENT_TTL_DAYS` (7), whatever `max_age_days`
says. A 5xx, a 429, a network error or an unreadable body is **never
cached**: record a `degradation` (`reason_for_failure("HTTP 503")` etc.) and
return. Until Phase 262 ARES cached any VR `HTTPStatusError` as `None` and
then cached the bundle built from it, and OpenCorporates' `_get_optional`
cached `None` on any exception — neither expired, so one outage made a
company read "found, nothing filed" until the next deploy. A bundle built
while a part was absent or failed must not be cached either (ARES caches its
bundle, under `ares/bundle-v2/`, only when VR returned data).
`tests/test_cache_hygiene.py` pins both.

## Other conventions

- **CI (Phase 266).** `tests.yml` and the drift checks run on a pull request
  and on a push to `main` — once per change, not twice — and a newer push to a
  PR cancels the run it supersedes. Every job has `timeout-minutes`. Every
  action is pinned to a commit SHA with its tag in a comment (Dependabot,
  `.github/dependabot.yml`, proposes the updates); add a new action the same
  way. Workflows are `contents: read` at the top, and only the job that uploads
  a release asset asks for `contents: write`, with `persist-credentials: false`
  on its checkout (uploads use `GH_TOKEN`; nothing pushes).
  `dependency-audit.yml` runs pip-audit over the locked runtime dependencies
  when `pyproject.toml` / `uv.lock` change and weekly. `openaleph-client` was
  removed — nothing imported it, and it held urllib3 at 1.x; the OpenAleph
  User-Agent version it supplied is now the constant `_OA_VERSION`.
- API keys go in `.env` only — never committed to the repo.
- **Never put `str(exc)` or `f"{exc}"` into a response, an SSE event or anything
  stored** — use `opencheck.secret_scrub.describe_exception(exc)` (or
  `safe_message(exc)` without the type prefix). httpx puts the full request URL
  in every `HTTPStatusError`, and CVR (`?apiKey=`) and OpenCorporates
  (`?api_token=`) authenticate in the query string, so a 401 used to carry the
  key into `source_error`, the replay cache and saved reports (Phase 233). A
  status error becomes `HTTPStatusError: HTTP 401 Unauthorized from <host>`;
  anything else is scrubbed of configured secret values, credential-named query
  parameters and URL userinfo. `tests/test_secret_scrub.py` pins every sink.
- Schema files use `extra="allow"` via `_Base` so unknown API fields don't break validation.
- `validate_raw()` is called at the end of `fetch()` on the fully-assembled bundle, before returning.
- BODS interest type for **directors/managing officials** is `seniorManagingOfficial`, not `appointmentOfBoard`.
- `appointmentOfBoard` is for right-to-appoint-and-remove style ownership interests.

---

## Deployment

- Backend is deployed on **Render** (https://api.opencheck.world). Environment variables (API keys, etc.) must be set in the Render dashboard as well as in `.env` for local development.
- Frontend is served separately. The backend CORS origin is configured via `OPENCHECK_CORS_ORIGIN` in `.env`.
- Render free-tier instances spin down when idle — the first request after inactivity may be slow.

---

## Frontend: BODSGraph (Cytoscape.js)

**Do not use `@openownership/bods-dagre`** — it was removed in Phase 44. The graph is now pure Cytoscape.js + `cytoscape-dagre`.

**Icon rendering**: BOVS entity/person icons are in `frontend/src/lib/bovsIcons.ts` as base64 data URIs (9 icons). They are rendered in a React HTML overlay (`position: absolute, pointerEvents: none`) above the Cytoscape canvas. The canvas background-image approach does NOT work for these SVGs because Adobe Illustrator export includes `xmlns:xlink` which causes browsers to silently refuse drawing on a tainted canvas.

**Flag rendering**: Country flags served from `/bods-dagre-images/flags/{code}.svg`. Applied in the same HTML overlay as icons. Flag badge position: 45° NE circumference — `(cx + r·cos45°, cy − r·sin45°)`. Badge size: proportional to node radius (0.75r × 0.50r).

**Signal badge rendering**: BOVS Option C risk overlays at 315° NW circumference — `(cx − r·cos45°, cy − r·sin45°)`. Single signal: labelled pill. Multiple signals: stack badge "N ⚠" in worst-severity colour. Signal→statementId mapping via `buildSignalMap()` which reads evidence fields.

**Overlay update**: `cy.on('viewport', updateOverlays)` fires on every pan/zoom. All coordinates computed in screen-space pixels.

**Edge styling**: All styled clones (`.own`/`.control`) from bods-dagre were removed. Arrowheads injected via custom SVG marker `#oc-bovs-arrow` in SVG `<defs>`.

**BOVS arrowhead marker**: injected after draw() — `<marker id="oc-bovs-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerUnits="strokeWidth" markerWidth="8" markerHeight="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#333"/></marker>`. Applied to all `g.edgePath path` elements.

**Edge categories** (Phase 122 palette): `ownership` (**#3b82f6** = `oo.node.blue`, the FullCheck accent), `control` (orange #e65100, dotted), `role` (**#7c3aed** = `oo.node.purple`, the BackgroundCheck accent, dashed), `unknown` (grey #888). Ownership and role deliberately share the mode-badge node colours: the network mode's accent *is* the ownership edge, and roles are held by people, which is what the people mode screens. Control keeps its orange — the node tier has none, and control must stay distinguishable from both. Edge **label** text is darkened for WCAG 4.5:1 (#1d4ed8 ownership, #9a3412 control, #6d28d9 role, #595959 unknown); the line colours themselves do not reach it at text sizes. These values live in **`frontend/src/lib/graphStyle.ts`** (Phase 124), not in
`BODSGraph.tsx`: the Cytoscape stylesheet and the generated legend both read
`EDGE_STYLE` from there, so a colour change moves the diagram and its key
together. `backend/opencheck/reporting/diagram.py` draws the exported PDF/HTML
diagram with **copies** of `EDGE_STYLE` (all four relationship kinds: colour,
label colour, dash, legend name, `endedColor`), `ENDED_EDGE.arrowFill` /
`minContrast`, and `bodsGraph.ts`'s
`INTEREST_LABELS`, `categorise()` type sets and `buildEdgeLabel`'s two-line cap.
Since Phase 221 **`backend/tests/test_reporting_diagram_parity.py` parses the
TypeScript and fails when either side moves alone** — change both in one
commit. Before that nothing pinned them and the PDF drew every non-ownership
edge purple and solid while the canvas drew control orange and dotted.
PDF edge labels use the canvas's words ("Owns 75–100%", "Controls"), never the
register's raw `details` (that stays in the table under each figure), and are
placed by `_place_label`: centred on the edge nearer its fanned end, wrapped
to the horizontal room there, slid along the edge until clear of nodes and
other labels.

---

## Frontend: Risk signal system

**`frontend/src/components/risk/RiskChip.tsx`**: `RISK_PRESENTATION` maps signal codes to `{label, classes}`. `CONFIDENCE_DOT`: `high`=`●`, `medium`=`◐`, `low`=`○`.

**Signal codes and colours** (bg / text):
- `SANCTIONED`, `RELATED_SANCTIONED` → rose (#ffe4e6 / #be123c)
- `SANCTIONS_CONTROLLED`, `RELATED_SANCTIONS_CONTROLLED` → deep rose (#ffe4e6 / #9f1239) — OpenSanctions `sanction.control`; deliberately the same rose family as SANCTIONED one shade darker, **not** the red of `FATF_BLACK_LIST` (#fee2e2 / #991b1b): an earlier red-50/red-800 pass was indistinguishable from FATF on the rendered share card. Sits between SANCTIONED and SANCTIONS_LINKED in both colour and `SIGNAL_STYLE.severity` (7 / **6** / 3). Chip label mirrors OpenSanctions' own display name, "Sanction ownership or control"; `og_image.py` carries the shorter "Sanction control" because the long form truncates in the share card's fixed-width pill
- `SANCTIONS_LINKED`, `RELATED_SANCTIONS_LINKED` → amber (#fef3c7 / #b45309)
- `EXPORT_CONTROLLED`, `RELATED_EXPORT_CONTROLLED` → deep rose (#ffe4e6 / #9f1239), severity **5** — a listing of the party itself, above DEBARMENT (4), below SANCTIONS_CONTROLLED (6). `EXPORT_CONTROL_LINKED` (+related) → amber, severity 3; `EXPORT_RISK` (+related, upstream label "Trade risk") → orange, severity 2. No suppression within the export family — upstream declares no superset relationship (Phase 118)
- `COUNTER_SANCTIONED`, `RELATED_COUNTER_SANCTIONED` → slate (#f1f5f9 / #334155) — OpenSanctions `sanction.counter`. Deliberately **outside** the rose/amber sanctions ramp, not merely a lighter rose: the Phase 105 failure was a counter-designation by a non-democratic regime reading as a shade of "Sanctioned", and any red or amber reproduces it. Graph `SIGNAL_STYLE.severity` is **2** — below `SANCTIONS_LINKED` (3), inverting the structural ranking on purpose, since the graph stacks worst-severity-wins and a counter-listing must never outrank a signal with an actual compliance consequence. `RiskChip.test.ts` fails the build if either chip's classes match `/rose/` or `/amber/`
- `FATF_BLACK_LIST` → red (#fee2e2 / #991b1b)
- `PEP`, `RELATED_PEP` → violet (#f5f3ff / #6d28d9)
- `COMPLEX_CORPORATE_STRUCTURE` → retired Phase 272; slate context palette for stored reports only
- `FATF_GREY_LIST` → orange dark (#fff7ed / #9a3412)
- `NON_EU_JURISDICTION` → slate context palette (orange on the og share card)
- `OFFSHORE_LEAKS` → amber (#fef3c7 / #92400e)
- `TRUST_OR_ARRANGEMENT` → indigo (#eef2ff / #4338ca)
- `COMPLEX_OWNERSHIP_LAYERS` → sky (#f0f9ff / #0369a1)

**Signal→BODS node mapping** (evidence fields) — owned by `frontend/src/lib/signalScope.ts`, **not** by `BODSGraph.tsx`. Add a new evidence shape there and every consumer picks it up:
- `SANCTIONED`, `PEP` → `evidence.statement_id` (added in Phase 45 via `_bods_stable_id(source_id, hit_id)` in `risk.py`)
- `RELATED_SANCTIONED`, `RELATED_PEP`, and a related party's `OFFSHORE_LEAKS` (icij) → `evidence.subject_statement_id`, plus `evidence.subject_statement_ids[]` when the party spans several statements. Since Phase 282 a related-party screen emits **one signal per party per upstream record** naming every statement of the deduped party (`related_targets.attach_to_party`), never one copy per statement — a consumer that needs "which statements" reads the list, and anything counting signals counts parties
- `TRUST_OR_ARRANGEMENT`, `NOMINEE`, `STATE_CONTROLLED` (Phase 240) → `evidence.matches[].statement_id`
- `NON_EU_JURISDICTION`, `FATF_BLACK_LIST`, `FATF_GREY_LIST`, `EU_HIGH_RISK_THIRD_COUNTRY`, `SUBSIDIARY_LISTED_JURISDICTION` (Phase 273) → `evidence.jurisdictions[].statement_id`. Since Phase 273 the three list signals hold only the subject and its owners (`position`: `subject` / `above`); subsidiaries are in the slate context note
- `COMPLEX_OWNERSHIP_LAYERS` → `evidence.longest_path[]` (array of statementIds, subject first — the chain runs upwards from it) plus `evidence.subject_statement_id`

**Signal scoping across render sites (Phase 109)** — `RELATED_*` signals are assessed against the **merged** bundle late in `_lookup_pipeline` and ride on the top-level `risk_signals` event; a `/deepen` response carries only that source's own findings. So the three `BodsGraphExplorer` render sites see different lists, and the two per-bundle ones saw no cross-source signals at all: a node the risk panel called sanctions-linked rendered unbadged, i.e. as "checked and clean".

`lib/signalScope.ts` closes that gap. `scopeCrossSourceSignals(signals, statements)` keeps a signal only when its code starts with `RELATED_` **and** `signalStatementIds()` intersects the bundle's `statementId`s; `buildSignalMap()` (moved here from `BODSGraph.tsx`) is built on the same mapping, so the filter and the badge renderer cannot drift — that drift was the bug. Wiring: `App.tsx` → `QuickCheckPanel` → `SourceBucketCard subjectSignals` → `HitRow` → `DeepenBlock` (merged with `detail.risk_signals` via `mergeSignals`, plus a caption naming the count); `FullCheckPanel` → `SubsidiaryNetwork signals`. `EsgPanel`'s `DeepenBlock` defaults to `[]` — deliberate, it has no subject-level list.

Scoping is **RELATED_\* only, on purpose**. Subject-level codes stay out because their evidence is computed over the merged graph: a source bundle usually holds only a fragment of a `longest_path`, so badging `COMPLEX_OWNERSHIP_LAYERS` there would assert something untrue of the graph on screen (same for `FATF_*` via `jurisdictions[]`). `signalScope.test.ts` pins that exclusion — widening it should require editing a test, not relaxing a predicate.

**"As filed" annotations (Phase 108)** — `frontend/src/lib/annotations.ts` holds the pure logic plus a **module-scoped** toggle store (`getAsFiled` / `setAsFiled` / `subscribeAsFiled`, read in components via `useSyncExternalStore`). Deliberately not per-card React state: a lookup renders many source cards and they must switch together. Default is OpenCheck's reading, and the toggle only renders when `annotatedFieldCount(statements) > 0`. `annotationsAt()` matches the RFC6901 pointer **exactly** — never a prefix — so an annotation on `/recordDetails/interests/0/type` cannot be attributed to interest 1 or to the whole interest. Unescape `~1` before `~0` or a field named `a~1b` addresses `a/b`. Wired into `PersonStatementCard` (birthDate) and `RelationshipStatementCard` (interest types); add new call sites by passing the annotation array to `<AnnotatedValue>`.

---

## Design system: the primitives (Phase 122)

`frontend/src/components/ui/` — `Button`, `Chip`, `SectionLabel` /
`SectionHeading`, `Icon`, exported from `ui/index.tsx` (named exports, no
default, mirroring `icons/index.tsx`). **Use these rather than restyling a
`<button>` or a `<span>` in place.** They exist because the audit found the
same meaning wearing nine button styles, twelve chip families and eight
eyebrow variants — the drift came from every component styling its own.

- **`Button`** — `primary | secondary | ghost | warn | danger`, sizes `md`
  (44px, the default) and `sm` (36px, pointer-dense rows only). `warn` is
  "incomplete, not failed" (the re-run affordance); `danger` is reserved for
  a failure the user must act on, **never** for an empty result. Export
  `buttonClasses()` for an `<a>` that must look like a button — a second
  implementation is how they diverged the first time.
- **`Chip`** — tone chosen by what the chip *asserts*, never by how alarming
  it should look: `risk | context | warn | ok | neutral | accent`. Pass
  `confidence` to get the ●/◐/○ glyph **plus** its screen-reader label; v1
  marked the glyph `aria-hidden` and named the level nowhere else.
- **`Icon`** — one stroke set on a 24 grid, `currentColor`, `aria-hidden`
  unless it is the only content of its control. The four mode glyphs are
  the shipped v1 mode-card paths, copied coordinate-for-coordinate.

**Tokens added:** `oo.soft` / `oo.softBorder` (the `#eef1fb` / `#cfd6f5`
pair, previously hardcoded 24×), semantic `oo.warn.*` / `oo.ok.*` /
`oo.info.*`, the `oo.graph.*` relation palette, `oo.node.teal`, and an
eight-step named type scale (`text-oo-meta` 12 → `text-oo-display` 26).
The 520 existing `text-[NNpx]` arbitrary values migrate component by
component — new code uses the named steps.

---

## The six check modes (Phase 122/123, 185, 190)

`quick | full | background | subsidiaries | history | esg`, owned by
`frontend/src/lib/checkMode.ts` (pure, so it is testable — the frontend suite
is logic-only). The mode is the report's top-level structure: a tablist under
the subject and verdict, which stay put across a switch. `MODE_ACCENT` there
is the token file for the six accents (allowlisted for hex like
`lib/features.ts`); `TOPIC_MODES` names the three that sit after the divider.

- **The mode is in the URL** (`?mode=`), and QuickCheck deliberately writes
  no parameter — a shared QuickCheck link keeps the short form it has always
  had. `documentTitleFor` keeps QuickCheck's title byte-identical to the
  server-rendered `/entity` template (`NAME - OpenCheck`, hyphen not
  em-dash); only a non-default mode appends a segment.
- **`selectMode` is the single entry point.** It writes the URL, fires the
  analytics event once per actual change, and moves focus into
  `#panel-<mode>` — switching unmounts most of the page, and v1 left focus
  on `<body>`.
- **Climate & ESG is a tab, not a section.** It used to render inside
  QuickCheck, reachable by scrolling and by nothing else. It sits after a
  divider because it is a different question, not a fourth depth of check.
- **Subsidiaries (Phase 185) is the second topic tab**, before ESG: what the
  company owns, from every list OpenCheck holds — GLEIF Level 2 (moved from
  the bottom of FullCheck), OECD-UNSD MEIP (moved from the bottom of
  QuickCheck), EITI's declared list (moved out of the ESG card) and GEM's
  directly owned entities (never rendered before). The non-GLEIF lists come
  from `GET /subsidiaries/declared` (`opencheck/subsidiaries_declared.py`),
  which is **UI-only**: the documented API, `/export` and MCP stay on the
  GLEIF network. Lists are kept apart per source and never merged — they
  disagree because they measure different things, and the tab says so. An
  LEI is attached only where a source's own data carries one; a name match to
  another list is offered with a match chip, never asserted
  (`lib/subsidiariesMode.ts`). The phone tab strip is a 2-column grid, so an
  odd tab count spans the last cell.
- **History (Phase 190) is the third topic tab**, between Subsidiaries and
  ESG: how this company's records changed, merged across every register that
  keeps a change log. The merge was not new — `timeline/assemble.py` has
  clustered cross-source identity changes in a 400-day window and ranked their
  dates (effective > recorded > snapshot) since Phase 146, across **five**
  sources, not the two its docstrings claimed. What was new is that it has a
  home. Before this it rendered behind a "Changes over time" button on each of
  the five source cards whose source emits history, and **every one of them
  mounted the same entity-wide, all-source timeline** — up to five identical
  copies per report, each captioned by a source that contributed part of it,
  none addressable. If you are tempted to put a merged artifact behind a
  per-source affordance again, this is the phase that says don't.
  `lib/historyMode.ts` is the values layer (labels incl. the `cvr_denmark` one
  that was missing, `recordUrl` for all five registers, the coverage sentence,
  the 10-row cap); `/history` gained `registry_numbers` so a New Zealand,
  Estonian or Danish row can link back to its record — before it, only GLEIF
  and Companies House could. The tab states the scarcity: five registers keep
  a change log and the rest answer only about now, so a thin timeline is a
  record-keeping fact, not a fact about the company. **Phase 263 made it six**:
  the New York Department of State (`timeline/ny_dos.py`), reconstructed from
  the rows `ny_dos`'s ordinary fetch returns — see "New York DOS" below.
- Entity-scoped sections (risk signals, structural context, cross-source
  identifiers, possibly-same) are guarded `mode === "quick"`. They were
  `mode !== "background"`, which silently included the new ESG tab.

---

## Source findings: the sentence on a hit row (Phase 123)

`SourceHit.finding` is **a sentence**; `SourceHit.summary` is the identifier
fragment it has always been (`"GB · registered entity"`), consumed by the
search-result rows, the share card and `og_image.py`. **Do not repurpose
`summary`** — a dozen call sites depend on its shape.

Templates live in `backend/opencheck/findings.py`; its module docstring
carries the ten rules, and every template is built from `clauses_to_sentence`
so a missing field shortens the sentence rather than emitting `None` or a
dangling `at %`. The two rules that matter most: **assert nothing about risk
or corroboration** (that is the signals layer, with its own confidence
model), and **state absence in the same voice as presence** — silence reads
as "nothing to see".

**Nineteen** adapters have templates (`inpi` for its not-in-the-RNE row only): `gleif`, `bods_gleif`, `opensanctions`,
`companies_house`, `opencorporates`, `openaleph`, `ted_eu`, `wikidata`,
`everypolitician`, `gemi_greece`, `climatetrace`, `eiti_assessment`,
`eiti_soe`, `cr_hongkong`, `acra_singapore`, `meip`, `dlcp_dc`, `ny_dos`. Adding one means **two** edits — the template here *and*
`finding=finding_<name>(r)` in that adapter's `_bh_<name>()`; a template
nobody passes is dead code, and nothing fails to warn you. The
frontend falls back `finding → summary → nothing`
(`frontend/src/lib/sourceFinding.ts`), so an adapter without one renders
exactly as it did before. **When you add a template, check what the adapter
actually parses first** — the first seven turned up six cases where the
obvious sentence was not supportable: the GLEIF Parquet extract has no
percentages or parent names, OpenSanctions' dataset slugs cannot be widened
into regime names without inference, OpenCorporates has `gb` and not
"England and Wales". Say less rather than more.

**Two GLEIF templates, one vocabulary.** `finding_gleif()` reads the live
`GleifAdapter.fetch` bundle — the source every lookup shows;
`finding_bods_gleif()` reads Open Ownership's Parquet extract, which is in
`_DELIBERATELY_UNREGISTERED` and serves only the three curated
`bulkBods: true` examples. **Check which one you are editing** — the first
pass put the template on the curated path only, so the live GLEIF row had no
sentence at all.

Both say **"consolidated by"** and never "owns", "holds", "shareholder" or a
percentage, because GLEIF Level 2 is *accounting consolidation*
(`IS_DIRECTLY/ULTIMATELY_CONSOLIDATED_BY`) — a consolidating parent need not
be a shareholder, and GLEIF publishes no percentage anywhere in Level 2.
`test_findings.py` runs three parametrized guards over every producible GLEIF
sentence and fails the build on a `%`, on an ownership verb, or on a missing
`consolidat`. A missing parent is a **reporting exception** — a permitted
filing defined by the LEI ROC policy, worded as such and never as a refusal
to disclose; `_GLEIF_EXCEPTION_PHRASES` maps the five reasons `NON_PUBLIC`
absorbed in Reporting Exceptions Format 2.1 to the same wording as the code
that replaced them, and an unrecognised code still reports that an exception
was filed rather than guessing at its meaning. Exception reasons are read
from both `exceptionReason` (live API, OO dump) and `reason`, as
`mapper.py:1935` does. "Direct subsidiaries" is only said where the data says
direct: the live count comes from GLEIF's `/direct-children` endpoint, while
the Parquet relationship table holds ultimate links too.

---

## Brand: Check-mode badges (QuickCheck / FullCheck / BackgroundCheck)

Reusable circular badges for social-media overlays, one per check mode —
generated 2026-07-23, shipped as transparent PNG (1280×1280, 2x) + SVG in
`outputs/mode-badges/`. Regenerate via the Cowork skill `checkmode-badges`
(delivered as a `.skill` file to Stephen) rather than hand-editing the PNGs
— needed again for a 4th mode or any re-brand.

**Palette — reused from the shipped design system, nothing invented.** The
hex values already existed hardcoded in `frontend/public/logo.svg` and
`OpenCheckIcon` (`components/icons/index.tsx`); this pass formalised them
as named tokens (`oo.mark.*` / `oo.node.*` in `tailwind.config.js`,
mirrored as `--oo-mark-*` / `--oo-node-*` in `index.css`):

| Token | Hex | Source | Badge |
|---|---|---|---|
| `oo.mark.navy` | `#0d1b3e` | logo.svg mark navy | badge background (all 3) |
| `oo.mark.line` | `#93c5fd` | logo.svg network-edge colour | FullCheck glyph edges |
| `oo.mark.checkBlue` | `#2563eb` | logo.svg "Check" wordmark colour | — |
| `oo.node.green` | `#22c55e` | logo.svg / `OpenCheckIcon` network node | **QuickCheck** accent |
| `oo.node.blue` | `#3b82f6` | logo.svg / `OpenCheckIcon` network node | **FullCheck** accent |
| `oo.node.purple` | `#7c3aed` | logo.svg / `OpenCheckIcon` network node | **BackgroundCheck** accent (near-matches the PEP/RELATED_PEP violet `#6d28d9` in the risk-signal system above — fitting for a people-screening mode) |
| `oo.node.teal` | `#0d9488` | **invented, Phase 122** | **Climate & ESG** accent — the fourth mode. The one colour in the badge set not lifted from `logo.svg`: three modes had three logo nodes, a fourth has none. The alternative, reusing `oo.green` `#25cb55`, sits three hex values from QuickCheck's `#22c55e` and was indistinguishable from it in the mode tab strip |
| `oo.graph.same` | `#b45309` | possibly-same edge colour | **History** accent (Phase 190) — the palette's one warm, archival value, and already what the /features card for this feature wore, so the tab arrived in the colour the feature had all along. Tab glyph = the existing `history` in `ui/Icon.tsx` (a clock rewound), the same glyph the removed "Changes over time" button drew inline |
| `oo.graph.control` | `#e65100` | graph control-edge colour | **Subsidiaries** accent (Phase 185) — the fifth mode lists what a company *controls*, as FullCheck wears the ownership-edge blue. Nothing invented; glyph `#fdba74`. Tab glyph = `subsidiaries` in `ui/Icon.tsx` (one parent over three children on a bus) |

**Note this is a brand-mark tier, deliberately distinct from the UI's
`oo.navy` (`#191d23`) / `oo.blue` (`#3d30d4`)** — the logo has always
shipped with its own darker navy and a different blue than the app chrome;
that split already existed in production, this just names it rather than
introducing a new one.

**Badge construction (per mode):** `oo.mark.navy` circle, 640×640 CSS px
(2x device scale factor → 1280×1280 PNG output), double stroke ring in the
mode's `oo.node.*` accent (10px solid + 1.5px/50%-opacity hairline
inside it), drop shadow tinted `rgba(61,48,212,0.38)` — same hue as the
`oo-card` shadow token, just stronger, so the badge still reads as a stamp
over an arbitrary photo background. Centred icon, "Bitter" 700 white
wordmark below it, short accent-coloured underline rule.

Icons: QuickCheck = ⚡ (Noto Color Emoji), BackgroundCheck = 👤 (Noto Color
Emoji), FullCheck = the network image with linked nodes as shown in the
OpenCheck logo — literally the same 3-node triangle from `logo.svg` /
`OpenCheckIcon` (identical node/edge coordinates, not a redrawn shape).
**Gotcha:** the triangle's outer nodes sit at the SVG viewBox edges (x=0,
y=0, y=72 with r=11) — pad the viewBox by the node radius on every side or
the outer nodes render as clipped half-circles.

Fonts: Bitter (headings) + DM Sans (body), matching the app exactly —
self-hosted as base64-embedded `woff2` in the generation script rather
than a live Google Fonts fetch, since headless-Chromium screenshot
generation shouldn't depend on network access being available at render
time.

Files: `outputs/mode-badges/{quickcheck,fullcheck,backgroundcheck}-badge.png`
+ `fullcheck-badge.svg` and `esg-badge.svg` (fully vector, no emoji-font
dependency, safe to recolour/edit by hand). **`outputs/` is gitignored**, so
the badges live on Stephen's disk only and are delivered as files, never
committed. `esg-badge.svg` ships vector-only: its PNG must come from the
`checkmode-badges` skill, which embeds Bitter as base64 woff2 — a headless
render without that font substitutes the wordmark face silently. The ESG
glyph is the same leaf path as the mode tab (`components/ui/Icon.tsx`, name
`esg`), copied rather than redrawn, for the same reason FullCheck's glyph is
the literal logo triangle. Dated per-post share cards (e.g.
`outputs/backgroundcheck-share-2026-07-23.png`) drop the relevant badge
into a 1200×630 layout following the existing `opencheck-social-*.html`
convention — OpenCheck logo top-left, Bitter headline, accent-coloured
top/bottom bars, `opencheck.world` in `oo.blue`.

---

## A relationship names its parties by recordId — resolve, never compare (Phase 210)

BODS v0.4: `subject` / `interestedParty` hold the party's **recordId**. Every
OpenCheck mapper sets `statementId == recordId` for entities and people, so for
207 phases every consumer keyed its lookups on `statementId` and happened to
work. The OECD's MEIP statements (Phase 208) are the first with a hash
`statementId`, a `meip-entity-N` `recordId` and — on **every** statement — a
`declarationSubject` naming the group *head*. First production lookup: A/S
Norske Shell and SHELL PLC drew as two unlinked nodes.

- **One resolver each side:** `backend/opencheck/bods/refs.py`
  (`party_ref`, `statement_index`, `resolver`) and `frontend/src/lib/bodsRefs.ts`
  (`partyRef`, `refIndex`, `resolveRef`). They map any spelling — statementId,
  recordId, `declarationSubject` alias, bare string or legacy `describedBy*`
  wrapper — to the **statementId** every index in the codebase is keyed on.
  Unknown references come back unchanged so a dangling edge still reads as
  dangling. **Never compare a relationship reference against a statementId set
  directly** — resolve it first.
- **Tier order is strict and matters:** statementId > recordId >
  declarationSubject. As a peer of recordId, the OECD's head-on-every-statement
  alias pointed the head's id at the first subsidiary in the file.
- Wired into: `risk.py` (`_relationship_endpoints(stmt, resolve)` — all four
  loops build `_resolve = _refs_resolver(bods)`), the PDF/Markdown report
  `by_id`, `narrative/packet.py`, the Senzing / Neo4j / FtM exports, and on the
  frontend `bodsGraph.ts` (graph + tree), `reconcile.ts`, `backgroundCheck.ts`
  and `SourceBucketCard`'s statement lookup. `bods/rdf.py` was already right
  (it links on recordId, as the standard does) and `bods/validator.py` already
  accepted both. Regression: `tests/test_bods_refs.py` and
  `src/lib/bodsRefs.test.ts`, both on a bundle in the OECD's exact shape.

---

## OECD-UNSD MEIP is a source, as the OECD's own BODS (Phase 208)

From Phase 69 to 207 MEIP was a *signpost*: a card at the bottom of QuickCheck
(later the head of the Subsidiaries band) fed by its own `meip` SSE event, with
no statements and no graph nodes. In September 2026 the OECD published the
Global Register in **BODS v0.4** and Phase 208 reversed the July 2026 decision:
`sources/meip.py` is a registered, LEI-keyed adapter; `map_meip` is a
**passthrough** (`bods_statements` are the OECD's statements, verbatim — do not
"improve" them, the point is to show what the OECD published beside GLEIF); the
`meip` SSE event, `ReportResponse.meip`, `MeipSignpost.tsx` and the `MeipMatch`
types are gone. One code path.

- **The store is `data/meip.sqlite`** (66 MB; 29 MB gzipped), packed by
  `backend/scripts/build_meip.py` from the OECD's JSONL **and** the Global
  Register XLSX (`data/globalregister2024.xlsx`) — the spreadsheet supplies each
  member's immediate parent, which the BODS file omits (every edge runs straight
  to the group head). Published as the `meip-bods-2024` release asset
  (`OPENCHECK_MEIP_DB_URL` default) and downloaded at boot by `warm_meip_db()`,
  the PSC-graph boot rule; on Render it lives at `/var/data/meip.sqlite`
  (`OPENCHECK_MEIP_DB_FILE`). Without the file `covers_lei` is False for every
  LEI and the source is never announced. The Phase 69 JSON tables stay only as
  the Subsidiaries tab's fallback (`context.complete: false`).
- **Say "listed in the X group", never "owned by".** MEIP records group
  membership under a statistical methodology; every relationship is
  `unknownInterest` / `directOrIndirect: unknown`. `finding_meip` and the tab's
  `meipContextLine` both keep that vocabulary and `test_meip` fails on an
  ownership verb. `risk.py` excludes `meip` from the `COMPLEX_OWNERSHIP_LAYERS`
  count (`_LAYER_COUNT_EXCLUDED`) — one hop to the head is not a layer count.
- **Carried through as published, by decision (Stephen, 15 Sept 2026):** ~4% of
  head rows carry the LEI of a different entity in the group (Munich Re's head
  is a UK pension trustee); 113 LEIs sit in two groups and **both** memberships
  are shown; the `lei` on the hit is asserted because the OECD publishes it.
  A head's subsidiaries are the Subsidiaries tab's list, never graph nodes.
- **Build on a Mac, not on the mount:** SQLite cannot write on the connected
  folder ("disk I/O error") — `build_meip.py` writes to `/tmp` and copies.
  Inputs (`meip_bods.jsonl`, the XLSX, the sqlite and its gz) are gitignored.
- A scheduled check of the OECD page for the next edition fires in March 2027.
- **One added key (Phase 267):** `map_meip` copies each statement and adds
  `source.opencheckSourceId: "meip"`, so the licence lookups can name it. The
  OECD's values and ids are untouched and the store's objects are never
  mutated; `test_meip` compares everything else verbatim.

---

## Nigeria CAC BOR (cac_nigeria) — the v1 API (Phase 213)

The register's site was redesigned in September 2026 (Angular, `/api/v1`). The
August harvest's `POST /api/bor-search/get_psc` endpoints now answer **404**.
`scripts/build_cac_nigeria_index.py harvest` uses the new ones; every item here
cost debugging time.

- **Not BODS.** The homepage advertises "BODS-JSON" exports and shows a sample
  that is not valid BODS in any version (`statementID`, `shareValue`). The API
  returns CAC's own JSON; `report.json` is the printable report as JSON. Do not
  look for a BODS endpoint without re-reading the front-end bundle first.
- **Search is a substring match.** `searchTerm=771` returns 2,553 companies,
  `size` caps at 100, and `rcNumber` is `"RC 2457"` for some companies and bare
  digits for others. Search `"RC <n>"`, then the name; accept only an exact
  digits-only RC.
- **`/companies/{id}/psc` returns email, phone, full date of birth and ID
  number to anonymous callers.** The harvester copies `_PSC_FIELDS` — an
  **allowlist**. Never switch it to a denylist; `test_raw_harvest_holds_no_personal_contact_or_identity_fields` pins it.
- **Flag → CAMA condition** (confirmed row-by-row against `report.json`):
  `pscHoldsSharesOrInterest` 1, `pscVotingRights` 2, `pscRightToAppoint` 3,
  `pscSignificantInfluence` 4 (company), `pscExerciseSignificantInfluence` 5
  (trust or firm). `null` reads as NO.
- **Row `status` is history.** ACTIVE is current; INACTIVE rows are earlier
  filings (Dangote Cement: 8 INACTIVE + 1 ACTIVE). `map_cac_nigeria` builds a
  current owner's relationship from ACTIVE rows only and closes an owner with
  none — `recordStatus: closed`, **no `endDate`** (none is published).
- **Corporate owners' names are not in v1.** Only `surname`/`firstname`/
  `otherName` exist; a corporate PSC whose name CAC stores elsewhere is blank
  (NNPC's MOPI/MOFI rows — named by the August API — read "N/A (Not Provided)"
  even in CAC's PDF). Blank rows carry `governingLaw`/`register` and no
  nationality. They map to `unknownEntity` "Unnamed corporate owner", one party
  per row — **never `anonymousEntity`**, which would fire the opaque-ownership
  signal against a company that withheld nothing.
- Some filings now name an executive holding the parent's stake (MTN Nigeria:
  Ralph Mupita 73.39%). Carried as filed.

---

## New York DOS (ny_dos) — the All Filings family (Phase 263)

`sources/ny_dos.py`, `bods/mappers/us_ny.py`, `timeline/ny_dos.py`. Stephen's
decisions (29 Sept 2026): the weekly **All Filings** family, not the monthly
Active Corporations snapshot; the CEO as a person, **name and role only**;
History in the same phase; a wrong DOS ID is a **note card**. Things that will
be re-derived otherwise:

- **Four SODA queries per entity**, together, on `corpid_num` (the DOS ID):
  `63wc-4exh` All Filings, `3gg2-jgnp` status history, `ekwr-p59j` name
  history, `2tms-hftb` addresses — `addr_type` 3 (CEO) and 4 (principal
  executive office) only. Cached together on the DOS ID alone and only when
  all four answered (Phases 228 and 262); the name gate runs over the cache.
- **Assumed-name filings are not the entity's.** They sit in All Filings under
  a separate sequential numbering that collides with DOS IDs (`293750` MILK AND
  HONEY PRODUCTIONS, `293751` JUSTALK…; Corning's `49779` carries a 1983
  "ASSUMED NAME CORP INITIAL FILING" for J & M COFFEE SHOP).
  `entity_filings_of()` drops them; every reader goes through it or through
  `summarise()`, the one reading of the rows.
- **The Active Corporations snapshot drops dissolved companies**, which is why
  it was rejected: LENTOR CAPITAL LLC (`5810913`) dissolved on 24 July 2026
  with an ISSUED LEI and is in the status history, not the snapshot.
- **Foreign entities.** A foreign entity DOS records as Inactive only lost its
  authority in New York: not terminal, no `dissolutionDate`, and its
  `foundingDate` is `for_inc_date` (home incorporation), never the New York
  authority date. DOS's two-letter `juris` codes include non-ISO ones (`EN`,
  `EW`, `WL`, `QU`); `jurisdiction_of()` returns None for them and the code
  goes to `entityType.details` in words.
- **The CEO** is `seniorManagingOfficial`, no `beneficialOwnershipOrControl`
  (no BO regime, so no `bo_regimes.py` entry). A trailing comma-clause made
  only of title words is removed ("CHAN KENG LOKE, PRESIDENT"); placeholders
  ("VACANT VACANT", "THE CORPORATION") are no one. On History, CEO changes
  are the Tier-4 board stream dated `SNAPSHOT_WINDOW` between two statements
  (DOS publishes no appointment date) — the tab words it "approximate".
  `ny_ceo_statement_id` is shared by the mapper and the emitter.
- **The bucket is per event loop** (`_bucket()`): the four reads contend for
  it, and an `asyncio.Lock` that has been waited on binds to its loop.
- `SOCRATA_APP_TOKEN` (optional) goes in the `X-App-Token` header, never the
  URL. Licence `OPEN-NY-Terms` — commercial use yes, attribution not required
  (given anyway).

---

## Datafordeler CVR API (Denmark) — hard-won constraints

These are non-obvious and cost significant debugging time. Do not deviate from them.

- **Endpoint**: `https://graphql.datafordeler.dk/CVR/v2` — the `v` prefix is mandatory; `CVR/2` returns 404.
- **Auth**: `?apiKey=<raw_key>` query parameter only. No base64 encoding, no `service_user_id`, no `Authorization` header. The config field is `cvr_denmark_api_key`.
- **DAF-GQL-0008**: Aliases are forbidden. Every field must be queried by its canonical name.
- **DAF-GQL-0010**: Only one root field per GraphQL operation. A single query cannot fetch `CVR_Navn` and `CVR_Adressering` together — each must be a separate HTTP request.
- Consequence of DAF-GQL-0008/0010: the adapter issues **6 sequential/parallel HTTP requests** per lookup (one virksomhed lookup + 5 detail queries run via `asyncio.gather`).
- **sekvens field**: `sekvens=0` is the primary/current record for names and branches. Higher values (1, 2…) are secondary or historical. Always prefer `sekvens==0`.
- **Legal form text**: Use the API's own `vaerdiTekst` field first; fall back to the hardcoded `_LEGAL_FORM_MAP` only when `vaerdiTekst` is absent. The map's numeric codes do not match what the API returns for many entities.
- **Address preference**: The `AdresseringAnvendelse` field value for the primary business address is `"beliggenhedsadresse"` (lowercase). Use case-insensitive matching: `"beliggenhed" in (val or "").lower()`.
- **Timeout**: The Datafordeler API is slow. All CVR `client.post()` calls must use `timeout=45.0` explicitly, overriding the global 15 s read timeout in `http.py`.
- **GLEIF RA code for Denmark**: `RA000170` (Erhvervsstyrelsen/CVR).

---

## KvK (Netherlands) — rate limit handling

- The KvK open-data endpoint returns HTTP 429 when the global rate limit is hit.
- The shared `httpx.AsyncHTTPTransport(retries=2)` only retries on network errors, not HTTP 4xx responses.
- The adapter handles 429 with an explicit retry loop: up to `_MAX_RETRIES=3` retries, honouring the `Retry-After` response header when present, otherwise using exponential backoff starting at 2 s (capped at 30 s).

---

## INPI (France) — legal publishing prohibition

**Security constraint — must never be relaxed.**

INPI entries where `beneficiaireEffectif == True` MUST be silently skipped and never included in any output, BODS statements, or API responses. This is required by French law (Loi Sapin II / décret 2017-1094), which prohibits republishing beneficial ownership data from the INPI register. Always check this flag before processing any INPI record.

### BO rows are stripped from the raw payload too (Phase 262)

`sources/inpi.py::strip_beneficial_owners` removes every list item with a
truthy `beneficiaireEffectif` (and any `beneficiairesEffectifs` block), walking
the **whole** payload — historical formalities included. It runs before the
payload is cached **and** in `_make_bundle`, so an entry cached before Phase 262
is still never served raw. Dropping the rows only in the mapper was not enough:
`/deepen` returns an INPI bundle's `raw` to the Data drawer (`republish_raw`
defaults to True). Never move the strip into the mapper, and never add a path
that returns the RNE payload without going through `_make_bundle`.

### A 404 is an answer; a SIREN can have spaces (Phase 205)

- **The RNE does not hold associations or foundations** without a commercial
  registration (Transparency International France, `969500AEH12X8M5XEO53`,
  an *association déclarée*). `/api/companies/{siren}` answers 404 for them.
  `fetch` returns `{"company": None, "is_stub": False, "not_found": True,
  "coverage_note": COVERAGE_404}` (not cached; every other status still raises).
  `_bh_inpi` turns it into a **note card**, the KvK pattern (Stephen, 11 Sept):
  row sentence `finding_inpi` ("No record in the Registre National des
  Entreprises, which does not cover associations or foundations without a
  business activity."), the fuller note in the drawer, **no `siren` identifier**
  (INPI published none; the reconciler would read it as corroboration), and the
  MCP `sources` row stays `found: false` with the note attached. The row
  sentence matters: the drawer is not opened for a card with no statements, so
  a coverage note alone — which is all KvK has — stays out of sight.
- **`normalise_siren` removes all whitespace** and raises `ValueError` on
  anything that is not then 1–9 ASCII digits. GLEIF writes `542 051 180` for
  some issuers; stripping only the ends sent `/companies/941%20395%20501`.
- **The health probe asserts `expect_fields=("company",)`.** Without it a 404
  on the probe SIREN — now a quiet miss — would go green.

---

## Estonian adapter (ariregister) — hard-won constraints

**SOAP/X-Road API at `ariregxmlv6.rik.ee` — read-only history queries are now ALLOWED (narrowed ban).** The original blanket ban was written for the Phase 37 *paid* contract, which authenticated (HTTP 200) but returned zero rows for every query (RIK confirmed that contract type didn't grant data access). That premise is now false: the **free open-data API contract** credentials obtained 2026-05-29 (`ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD`) **do** return data. Confirmed live via `scripts/spike_ariregister_history.py` (Bolt returned 744 dated rows + a 50-entry registry-card log).

- **The live `/lookup` still uses the no-auth public scraper** in `fetch()` (see below) — do NOT route the lookup through SOAP.
- **The Time Machine (history only) uses SOAP**, read-only: `AriregisterAdapter.fetch_timeline_data()` calls `detailandmed_v2` (`ainult_kehtivad=0`, full registry-card history) + `tegelikudKasusaajad_v2` (beneficial-owner history), and `timeline/ariregister.py` maps the dated blocks into `ChangeEvent`s (NZ-emitter shape; `DateBasis.EFFECTIVE`/`HIGH`). Endpoint: `https://ariregxmlv6.rik.ee/`, producer namespace `http://arireg.x-road.eu/producer/`.
- **JSON dates are epoch-second floats and `{}` means "no end"** — the emitter requests XML (ISO dates, self-closing empties) for deterministic parsing; the epoch path is handled defensively in `_iso()`.
- **Shareholders are on the register card since 1 Sept 2023** (roles `OSAN` on-card / `O` off-card), so ownership history is available via `detailandmed_v2`.
- **BO access restriction POSTPONED on its 2026-07-10 start date** (https://news.err.ee/1610074816/ — the Ministry of Finance is revising the draft regulation; current public access remains, no new date announced). BO events stay deliberately isolated in `_bo_events()` in `timeline/ariregister.py` so the whole branch can be dropped when the restriction eventually lands (issues #22/#28 track it; a first removal shipped in `77e7b65` and was reverted when the postponement was announced — the revert commit shows exactly what to re-apply).
- **Render**: the Time Machine Estonia branch only lights up when `ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD` are set on Render (in addition to `.env` locally). Without them, `fetch_timeline_data()` returns `None` and the timeline silently omits Estonian events.

**Current lookup approach (Phase 45)**: Public web scraper. No credentials needed.
- **Main endpoint**: `GET https://ariregister.rik.ee/eng/company/{reg_code}/company_print_json`
- **Search endpoint**: `GET https://ariregister.rik.ee/eng/api/autocomplete?q={query}` → JSON
- **GLEIF RA code**: `RA000181` (NOT RA000198 — the table below has a typo, RA000181 is confirmed from live GLEIF data)
- **HTML structure**: Bootstrap label/value rows (`col-md-4 text-muted` / `col font-weight-bold`). Tables identified by header keywords.
- **Officer role mapping**: English labels → Estonian codes (e.g. "Management board member" → `JUHL`, "Procurist" → `PROK`, "Liquidator" → `LIKV`)
- **Person type detection**: 11-digit code starting with 3-6 = natural person (F); 8-digit = legal entity (J)
- **BO control mapping**: "Indirect ownership" → `K`, "Direct ownership" → `O`, "Voting rights" → `H`
- **Not found detection**: If `str(r.url)` does not contain `/eng/company/`, the server redirected away (company not found) → return stub bundle
- **Bundle format**: Unchanged from Phase 37 — `map_ariregister()` in `bods/mapper.py` needs no changes
- `ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD` are NOT used by the live-lookup scraper, but ARE read by `fetch_timeline_data()` for the SOAP history path (see the narrowed-ban note above)

---

## EITI: three databases, three ID systems

Three EITI endpoints are live at once and the adapters split across them. Every
item here cost real debugging time.

| Endpoint | What it serves | Used by |
|---|---|---|
| `eiti.org/api/v2.0` | payments + the organisation index | `eiti` |
| `soe-database.eiti.org` | the old flat summary data — **still live** | `eiti_soe` |
| `eiti-database.eiti.org` | the new Datasette-backed global database | `eiti_assessment` |

### Datasette 1.0-alpha gotchas (the new database)

- **SQL lives at `/eiti_database/-/query.json?sql=`.** The legacy
  `/eiti_database.json?sql=` **302-redirects** there, so a client without
  `follow_redirects=True` silently gets nothing back — not an error, nothing.
- **`sql_time_limit_ms` times out even `select count(*)`** on the wide views:
  `view_companies`, `view_commodities`, `view_countries`,
  `view_country_commodity_pairs`. **Query the narrow tables, never the wide
  views.**
- **1,000-row page cap.** Follow the `next` token; do not trust `_size=max`.
  `build_eiti_assessment_index.py::_fetch_all` pages with LIMIT/OFFSET and then
  asserts the harvested row count against `count(*)`, so a changed cap can never
  silently truncate a harvest again — that failure cost an earlier SOE build
  5,156 of its 5,332 companies.

### Table naming is a four-layer pipeline

`raw_*` → `resolved_*` → `clean_*` → `metadata_*` → `view_*`

> **`raw_*` values are JSON-encoded, quotes included.** A LEI arrives as
> `"\"549300071188HIDJEB11\""` — `length()` is **22**, not 20. This is why a
> naive validity check reports zero valid LEIs when there are three. Strip it
> before comparing anything (`_unjson()` in the assessment builder).

Pick the table by what it actually carries, not by what it is named:
`raw_company_assessment_subsidiaries` has the implementing country and the
assessment year; `metadata_company_relationships` describes the same edges with
`country_of_operation_iso3` NULL and `assessment_year` 0.

### Two traps worth naming

- **On the old host the SOE view name cannot be percent-encoded.**
  `SOE%20List` returns **404**. Datasette's own encoding for the space is
  `~20`, which is what `build_eiti_soe_index.py`'s `SOE_LIST_URL` uses and which
  still returns 200 (re-verified 2026-09-04); `SOE+List` also works. **Do not
  "fix" the `~20` to `%20`.**
- **The company IDs were regenerated between the two databases and are not
  portable.** Old `eiti_id_company` is a **UUIDv4** (random, unprefixed); new is
  a **UUIDv5** (name-based SHA-1) prefixed `eiti_id_company:`, alongside a
  **UUIDv7** surrogate row key. The v5 is a *deduplication* key, not an identity
  assertion — 12,009 distinct normalised names collapse to 10,116 ids — which is
  why `eiti_assessment` asserts no identifier of any kind. Anything that stored
  an old v4 id cannot look it up in the new database.

### What EITI does and does not publish as an identifier

`legal_entity_id` is a real column in `metadata_companies`, populated for
**3 companies of 10,116**; the Company Assessment reference sheet has the column
on all 124 rows but 121 hold the literal string `"Not available"`. There is no
OpenCorporates id in the new database at all, and `metadata_company_id_references`
holds one row (GB / Companies House). So an EITI `identification` value being a
national registry number is an *assumption*, not something the data states.

### One more, learned the hard way

EITI spells the same company differently in its own two sheets and the UUIDv5
dedup does not collapse the variants: `Anglo American`/`AngloAmerican`,
`ArcelorMittal`/`Arcelor Mittal`, `Barrick Gold`/`BARRICK`,
`Staatsolie`/`Staatsolie maatschappij Suriname N.V`. Canonicalise on a spaceless
key before joining EITI to EITI.

---

## Frontend curated examples (HomePanels.tsx)

`EXAMPLE_LEIS` in `frontend/src/components/HomePanels.tsx` (moved out of `App.tsx` in Phase 168) contains pre-computed `signals` arrays shown on the picker cards before the user clicks. These must be kept in sync with what the risk engine actually produces for each entity. When the risk engine changes (new signals, retired signals, confidence changes), update `EXAMPLE_LEIS` to match.

Current signal inventory used in picker cards: `TRUST_OR_ARRANGEMENT`, `COMPLEX_OWNERSHIP_LAYERS`, `SANCTIONED`, `RELATED_SANCTIONED`, `NON_EU_JURISDICTION`. Confidence `"high"` renders as `●`, `"medium"` as `◐`.

---

## Test suite

- Run `uv run pytest -q` from `backend/` (what CI runs, with `uv sync --extra ftm`
  first — without the extra ~20 icij/openaleph tests skip or fail). The current
  totals are the closing paragraph of `docs/status.md`; do not copy a count here.
- **The suite is offline by construction (Phase 266).** `conftest.py` sets
  `OPENCHECK_WARM_CACHES_ON_START=0`, so `with TestClient(app)` no longer runs
  the lifespan's boot downloads (GEM CSVs, the GLEIF GEM↔LEI mapping, the bulk
  indexes), and installs `tests/_network_guard.py`: any connection past
  loopback, or DNS for any name but localhost, raises `NetworkBlocked` (an
  `OSError`, so code takes its outage path) **and** fails the test during which
  it happened — the recorded attempt fails it, not the exception, which code
  under test is built to swallow. The guard also removes `HTTP(S)_PROXY` for
  the run, since a proxy on loopback would carry every request past it. Mock
  with respx / pytest-httpx; a test that must reach the network is
  `@pytest.mark.live`. `--run-live` leaves the network alone. A test that is
  *about* the warm-up turns it back on and stubs every step
  (`tests/test_offline_suite.py::stub_every_warm_up`, which fails if the
  warm-up grows a step it does not name). Verified with the network namespace
  removed (`unshare -rn`), not only by the guard.
- Async adapter tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`).
- HTTP mocking: use `respx` for httpx-based adapters; use `unittest.mock.AsyncMock` with `patch("...build_client", ...)` for adapters that call `build_client()` directly.
- GraphQL adapters (CVR): mock by inspecting the request body (`request.content`) to route different query strings to different fixture responses.
- `tests/test_sources.py` discovers adapter modules from the filesystem and `tests/test_app.py` compares `/sources` with `REGISTRY` — neither needs an entry for a new adapter; a deliberately unregistered one goes in `_DELIBERATELY_UNREGISTERED`.
- **mypy is a ratchet (Phase 266).** `[tool.mypy]` is non-strict; `scripts/mypy_ratchet.py` (a CI step in the backend job) fails a module carrying more errors than `mypy-baseline.json` records, a new module carrying any, and a module carrying **fewer** until `uv run python scripts/mypy_ratchet.py --update` locks the gain in — the design-lint pattern. `--update` refuses to raise a count without `--allow-increase`.
- **Three frontend copies are pinned by parsing the TypeScript** (`tests/test_frontend_parity.py`, Phase 266, the `test_ra_codes.py` pattern): `historyMode.ts` `HISTORY_SOURCES` / labels / `recordUrl` cases ↔ the emitter modules in `opencheck/timeline/`; `relationshipStatus.ts` ↔ `bods/lifecycle.py`; `bodsRefs.ts` ↔ `bods/refs.py`. A new timeline emitter must be named after its module and appear in all three frontend tables.
- **Live smoke tier (`tests/test_live_smoke.py`, `@pytest.mark.live`):** opt-in tests that hit the *real* GLEIF + Wikidata APIs to catch API-shape drift without recording payloads (the deliberate alternative to vcrpy/cassettes — no PII, secrets or licence-restricted data committed). **Skipped by default**; run with `pytest --run-live -m live` (or `OPENCHECK_RUN_LIVE=1`). The skip wiring is in `conftest.py` (`pytest_addoption` + `pytest_collection_modifyitems`); the `live` marker is registered in `pyproject.toml`. Only open, key-free sources belong here — never OpenSanctions (CC-BY-NC), OpenCorporates, or key-gated/PII-heavy sources.

---

## Spikes → production: test before you merge (hard-won, recorded 2026-06-26)

A **spike** is exploratory, throwaway-quality code to validate an idea fast (e.g.
the progressive-discovery / "Add next layer" graph expansion — destined for
**FullCheck** mode; see the QuickCheck/FullCheck Notion ticket). Spikes are
useful, but **merging a spike to `main` is moving it into production**, and that
has repeatedly outrun its test coverage here. Be conservative and surface the
gaps before promoting one.

- **Test every layer that changed, and make sure CI runs those tests.** Backend
  changes need pytest; frontend changes need `tsc` + the vitest suite. CI gates
  push/PR via `.github/workflows/tests.yml` (backend `pytest`, frontend
  `npm run build` + `npm test` + `npm run lint:design`, and the e2e smoke) — a
  change that touches React/TS but only has backend tests is **not**
  production-ready. The sandbox can't always run vitest
  (platform-mismatched `node_modules`); that is **not** the same as CI running
  it, so don't treat "tsc clean locally" as sufficient — confirm CI is green.
- **Three frontend tiers, and they answer different questions (Phase 168).**
  `src/lib/*.test.ts` runs in `node` and pins *what the app says* — a sentence,
  a tone, a count. `src/**/*.test.tsx` runs in `jsdom` with testing-library and
  pins *what the markup is* — how many of a thing there are, an element's
  accessible name, what a control does when pressed. `frontend/e2e/*.spec.ts`
  is Playwright over a real backend and a production build (`npm run test:e2e`)
  and pins *what a whole page is* — one `<h1>`, nothing overflowing sideways,
  no console errors. The suite was tier one alone for 163 phases, and the three
  regressions that cost the most (the v1/v2 component mix, the verdict rendered
  twice, the mode tabs overflowing at 390px) were all invisible to it, because
  none of them was a wrong value. Pick the cheapest tier that can see the claim
  you are making: a `.test.tsx` that only checks a string belongs in `lib/`.
- **Unit fixtures are not enough — exercise it against real data before declaring
  it done.** The progressive-discovery spike passed every test yet was wrong on
  live Shell data three times (expansion direction; cross-source duplicate
  subjects; an empty frontier) because those were data-shape failures fixtures
  didn't capture.
- **Don't let `SPIKE` / `TODO` shortcuts cross into `main` unguarded.** If they
  must, open a tracked "de-spike" ticket *before* merging and link it in the
  merge commit.
- **Prefer a `--no-ff` merge that names the spike** so the debt is visible in
  history, and keep general fixes that rode along (e.g. dev-proxy additions, the
  StrictMode hit dedup) as their own commits so they're easy to find and port.
- **If asked to merge a spike to `main`, say what testing is still missing first**
  rather than merging silently.

---

## GLEIF reverse-lookup: local ID → LEI

GLEIF supports querying by local identifier, which is the **inverse** of OpenCheck's normal
flow (LEI → `registeredAs` → national adapter). This isn't needed for the core lookup path,
but would enable a future "company number first" entry point where a user supplies a local
registry number instead of a LEI.

A local ID may appear in **three** different fields on the LEI record:

| GLEIF field path | Filter parameter |
|---|---|
| `entity.registeredAs` | `filter[entity.registeredAs]=<id>` |
| `registration.validatedAs` | `filter[registration.validatedAs]=<id>` |
| `registration.otherValidationAuthorities.validatedAs` | `filter[registration.otherValidationAuthorities.validatedAs]=<id>` |

The same entity can hold different local IDs across those fields (e.g. a national registry
code vs. a tax authority code). To avoid false matches from coincidental ID collisions across
registries, always add the RA code as a second filter:

```
https://api.gleif.org/api/v1/lei-records?filter[entity.registeredAs]=00102498&filter[entity.registeredAt]=RA000585
```

Each adapter in the RA table below has the correct RA code for this second filter.

**Future use**: a "find by company number" entry flow would query all three filter endpoints
(parallel requests, deduplicate by LEI), then hand the resolved LEI to the standard
`/lookup-stream` flow. The RA codes table already has everything needed.

**Autocompletions endpoint**: `https://api.gleif.org/api/v1/autocompletions?field=fulltext&q=<name>`
searches across the entire LEI record (not just legalName). Likely a superset of the existing
`filter[fulltext]` search used in `gleif.py`; worth evaluating if name search miss-rate is a problem.

Reference: https://documenter.getpostman.com/view/7679680/SVYrrxuU?version=latest

---

## GLEIF RA codes for active adapters

> **Every row below was re-verified live against the GLEIF Registration Authority
> API on 2026-08-28.** The previous version of this table was wrong in **nine**
> of eighteen rows (IE, LV, LT, FR, BE, AT, PL, SK, SG) — in every case the
> adapter source was right and the table was wrong, so the table has been
> corrected to match the adapters. **Trust the adapter constant, not this
> table**, and re-verify against GLEIF before relying on any code here.

| Country | Adapter | RA code | Verified |
|---|---|---|---|
| UK | companies_house / gleif | `RA000585` England & Wales · `RA000586` Northern Ireland · `RA000587` Scotland | 2026-08-28 — ⚠️ see the `gleif.py` bug note below |
| Netherlands | kvk | `RA000463` — Business Register (KvK) | 2026-08-28 |
| Norway | brreg | `RA000472` — Register of Business Enterprises (Foretaksregisteret) | 2026-08-28 — ⚠️ ambiguous; the adapter describes *Enhetsregisteret*, which is `RA000473`. `RA000270`, also named in `brreg.py`, **does not exist** in the GLEIF RA list |
| Ireland | cro | `RA000402` — Companies Register (CRO) | 2026-08-28 — table previously said RA000215 (wrong) |
| Latvia | ur_latvia | `RA000423` — Commerce Register (Uzņēmumu Reģistrs) | 2026-08-28 — table previously said RA000327 (wrong) |
| Lithuania | jar_lithuania | `RA000430` — Register of Legal Entities (Registrų centras) | 2026-08-28 — table previously said RA000330 (wrong) |
| France | inpi | `RA000189` — Register of Companies (Sirene, INSEE) · `RA000192` — Registre du Commerce et des Sociétés (Infogreffe) · `RA001129` — Registre national des entreprises (INPI) | 2026-09-11 — 149,237 active FR LEIs under RA000189, 8,145 under RA000192, **both carry the SIREN in `registeredAs`**, plain (`552032534`) or grouped in threes (`542 051 180`) depending on the LEI issuer — 391 sampled, no other shape. Both dispatch INPI and both map to `FR-INSEE` since Phase 205 (only RA000189 did before). `RA_BY_COUNTRY["FR"]` stays RA000189; `search_by_local_id` widens a French scope to every French code and both spellings. `RA000190` = AMF fund codes, not a company register. **RA001129 was added in GLEIF RA list v1.9 (30 Sept 2026, Phase 265)** with no LEI filed under it; it dispatches INPI, maps to `FR-INSEE`, and is searched with the other two — by the backend resolver and, since Phase 265, by the frontend picker (`sameNumberAuthorities` on the FR entry of `raCodes.ts`, parsed by `test_ra_codes.py`). Check the first records it carries are SIRENs |
| Sweden | bolagsverket | `RA000544` — Companies Register (Bolagsverket) | 2026-08-28 (also verified 2026-06-12; RA000523 in earlier notes was wrong) |
| Estonia | ariregister | `RA000181` — Commercial Register | 2026-08-28 (ignore any reference to RA000198) |
| Belgium | bce_belgium | `RA000025` — Crossroad Bank of Enterprises | 2026-08-28 — table previously said RA000143 (wrong) |
| Austria | firmenbuch | `RA000017` — Commercial Register (BM für Justiz) | 2026-08-28 — table previously said RA000128 (wrong) |
| Poland | krs_poland | `RA000484` — National Court Register (KRS) | 2026-08-28 — table previously said RA000439 (wrong) |
| Slovakia | rpo_slovakia / rpvs_slovakia | `RA000526` — Business Register (Ministerstvo spravodlivosti) | 2026-08-28 — table previously said RA000476 (wrong). `registeredAs` is the bare eight-digit IČO (re-checked 2026-09-28); scheme `SK-ICO` since Phase 257 |
| Singapore | acra_singapore | `RA000523` — Business Registry (ACRA) | 2026-09-10 — 12,292 of 13,326 active SG LEIs register under it, **all with the UEN in `registeredAs`** (table previously said RA000509, wrong). VCC sub-funds (`T21VC0144D-SF001`) are filed here too but ACRA publishes no sub-fund rows, so `normalise_uen` rejects them. Other SG authorities: `RA000524` MAS, `RA000669` Registry of Societies, `RA000781` Charity Portal, `RA000996` OPERA |
| Canada | corporations_canada | `RA000072` — Corporate Registry (federal; provinces are RA000073–RA000085) | 2026-08-28. `registeredAs` carries the check digit after a hyphen (`1709920-7`; re-checked 2026-09-28), which `normalise_corp_id` folds to `17099207`; scheme `CA-CC` since Phase 257 |
| Denmark | cvr_denmark | `RA000170` — Central Business Register (Erhvervsstyrelsen) | 2026-08-28 |
| Croatia | sudreg_croatia | `RA000156` — Croatian Court Registry (Sudski registar) | 2026-08-28 |
| Czechia | ares | `RA000163` — Commercial Register (Ministerstvo spravedlnosti) | 2026-08-28 — ⚠️ ambiguous; the adapter is named for **ARES**, which is `RA000168` (Register of Economic Entities, Ministerstvo financí) |
| Cyprus | cyprus_drcor | `RA000161` — Companies Section (DRCOR) | 2026-08-28 |
| Finland | prh | `RA000188` — Business Information System (PRH) | 2026-08-28 |
| Malta | malta_mbr | `RA000443` — Registry of Companies (MBR) | 2026-08-28 |
| Switzerland | zefix | `RA000548` in the adapter | 2026-08-28 — ⚠️ **mismatch**: `RA000548` is the *UID-Register* (Bundesamt für Statistik, covers CH **and** LI). Zefix, the commercial register, is `RA000549` |
| Australia | abr_australia | `RA000014` — Register of Companies (ASIC) · `RA000013` — Australian Business Register (ATO) | 2026-08-28 |
| New Zealand | nz_companies | `RA000466` — Companies Register (Companies Office) | 2026-08-28 (near-miss neighbour: `RA000749` NZ Business Number Register) |
| Brazil | cnpj_brazil | `RA000681` — National Registry for Legal Entity (Receita Federal / CNPJ) | 2026-08-28 (state Juntas Comerciais are RA000036–RA000062). `registeredAs` is punctuated (`33.856.394/0001-33`; re-checked 2026-09-28); scheme `BR-CNPJ` since Phase 257 |
| India | mca_india | `RA000394` — Companies Register (MCA21) | 2026-08-28 |
| Nigeria | cac_nigeria | `RA000469` — Company Registry (Corporate Affairs Commission) | 2026-09-16 — all 30 set LEIs re-checked, `registeredAs` = RC (also verified 2026-08-12, 2026-08-28; Africa's first public BO register). Offline curated example set of 30 LEI-anchored companies (`data/cac_nigeria_psc.json`, Phase 213); a live adapter is deferred pending CAC / Oasis Management engagement. LEI-keyed dispatch (not an RA deriver); asserts only the CAC-published RC number (`ng_cac_rc`), not the derived LEI. |
| Greece | gemi_greece | `RA000685` — General Commercial Registry (G.E.MI.), businessregistry.gr | 2026-08-28 — 20 of 25 sampled Greek LEI records use it |
| Hong Kong | cr_hongkong | `RA000388` — Companies Registry · `RA000389` — Business Registration Office (Inland Revenue Department) | 2026-09-10 — 600 active HK records sampled: 63% RA000388, 20% RA000389, **both carry the 8-digit BRN in `registeredAs`** (a few RA000389 records hold the 16-digit BR certificate number or a hyphenated form; the BRN is the first eight digits). `RA000390` = SFC fund codes, not a company register. Scheme `HK-BRN`, not org-id's `HK-CR` (the old CR No.). **Not in `RA_BY_COUNTRY`**: one country code would have to pick one of the two authorities and `/resolve-national-id` would then miss the other's companies — the Phase 140 failure shape |
| Moldova | asp_moldova | `RA000451` — State Register of Legal Entities (ASP) · `RA000950` — National Commission for Financial Markets · `RA000951` — National Bank of Moldova | 2026-09-15 — all 55 legal-address MD records: 50 RA000451, 2 each RA000950/RA000951, 1 RA999999 (the National Bank, no number); **all three real codes carry the 13-digit IDNO** in `registeredAs`, check digit weights 7-3-1. One record (`16479`, Mogo Loans SRL) is not an IDNO. `filter[entity.jurisdiction]=MD` also matches `US-MD` (Maryland) — filter on `entity.legalAddress.country`. Scheme `MD-IDNO`. **Not in `RA_BY_COUNTRY`**, for the same reason as Hong Kong |
| Serbia | apr_serbia | `RA000517` — Business Registers Agency (APR) | 2026-09-17 — all 304 GLEIF records with jurisdiction RS: 295 RA000517, 6 RA999999, 2 RA000684 (Securities Commission fund numbers), 1 RA000518 (APR's entrepreneurs register); **every RA000517 record files the 8-digit matični broj**, no zero-padding needed. 141 of 146 ISSUED records resolve against the 31 Aug 2026 cut (misses: 2 sole traders, the Chamber of Commerce, 2 RA999999 state bodies). Scheme `RS-APR` (org-id.guide). In `RA_BY_COUNTRY` — one authority |
| United States — District of Columbia | dlcp_dc | `RA000601` — DC Corporations Division (DLCP) | 2026-09-18 — 502 of the 726 GLEIF records with jurisdiction `US-DC` use it (the rest: RA999999 ×209, RA888888 ×4, RA000602 ×4, others ×7), **every one with the Corporations Division file number in `registeredAs`**. The file number is an opaque string in at least six live shapes (`000347`, `L00005029230`, `L21249`, `N00008414776`, `P00454`, `US-DC-LL012601299`) — never normalise it. 487 of 500 resolve against the register; by LEI status `ISSUED` 144/145, `LAPSED` 322/333, `RETIRED` 22/23, the misses being lapsed LEIs on legacy identifiers. **A name check is mandatory**: GLEIF files `L21249` for American Foreign Policy Council and DLCP's `L21249` is CONNIE-19 STREET LLC. Scheme `US-DC` — the ISO 3166-2 subdivision code the BODS mapper already stamps on a DC entity reached through GLEIF, so the two corroborate rather than each asserting an identifier the other lacks. **Not in `RA_BY_COUNTRY`**: the US has fifty-odd company registers and `US` cannot mean DC's; for the same reason `US-DC` is in `register_hops._NO_COUNTRY_ALIAS`, so no `REG-US` alias is built from it |
| United States — New York | ny_dos | `RA000628` — Corporation and Business Entity Database (Department of State, Division of Corporations) · `RA000747` — Registry of insurance companies (Department of Financial Services) | 2026-09-29 — all 16,819 GLEIF records with jurisdiction `US-NY`: 13,442 RA000628, 3,050 RA999999, 55 RA000747, 33 RA888888; ISSUED 3,921 of 4,848 under RA000628. **RA000628 also appears on 53 records in other jurisdictions** — foreign companies authorised in New York (Quantexa Inc, `US-DE`, `5215193`) — so the deriver keys on the RA code, never the jurisdiction. `registeredAs` is the bare DOS ID, 1–9 digits, unpadded on both sides. Resolution over the ISSUED records: 3,878 agree on the current name, 8 more only on a former name, 23 resolve only in the All Filings family (dissolved, or newer than the snapshot), **8 are wrong numbers** (Salt City FCU files `8512`, THE MUNICIPAL WASTE PAPER RECEPTACLE COMPANY) → note card, 4 are in no DOS dataset. RA000747 files an **NAIC code**, not a DOS ID: it maps to its own RA code in `_GLEIF_RA_TO_ORG_ID` so the US-state jurisdiction fallback cannot label it `US-NY` and hop it to DOS. Scheme `US-NY` (the DC rule); in `register_hops._NO_COUNTRY_ALIAS`; **not in `RA_BY_COUNTRY`** |

### ✅ FIXED 2026-08-28: Scotland/Northern Ireland, and two more RA maps

GLEIF's Companies House codes are `RA000585` England & Wales, **`RA000586`
Northern Ireland**, **`RA000587` Scotland** — confirmed against real records
(THON MARITIME LTD, `registeredAs "SC651281"`, sits under `RA000587`).
`RA000591` is **The Pensions Regulator**, not a company registry.

Fixed, with `backend/tests/test_ra_codes.py` and two canaries in
`test_gleif_bridge.py` pinning it:

1. **`routers/search.py::_ch_ra_code`** now returns `RA000587` for `SC`/`SO`/`SF`
   and `RA000586` for `NI`/`NC`/`R0`. It previously mapped SC to Northern
   Ireland's code and NI to the Pensions Regulator's, so the Companies House →
   LEI bridge silently found no LEI for either nation.
2. **`sources/gleif.py::_CH_RA_CODES`** — deleted. It carried the same wrong
   mapping as dead code, referenced nowhere. A second copy is how the first
   survived.
3. **`bods/mapper.py`** — `RA000587` added to the RA → org-id scheme map, which
   had 585 and 586 but not 587, so Scottish entities carried no `GB-COH`
   scheme.
4. **`routers/lookup.py::_RA_BY_COUNTRY`** — nine wrong codes (IE, LV, LT, FR,
   BE, AT, PL, SK, SG), the same nine this table had. NZ and GR added.
5. **`frontend/src/lib/raCodes.ts`** — **eleven of twenty wrong**, including
   Norway pointing at India's MCA (`RA000394`) and Sweden at Singapore's ACRA
   (`RA000523`). Both had already been corrected here and in the backend
   months earlier without this file being touched.

**Why it survived:** every wrong value was a real RA code belonging to a
different authority, so the reverse lookup filtered on a registry the company
was not registered at and returned nothing. It failed **closed** — a missed
match looks like an absent company, not like a bug.

### ⚠️ Phase 141: never read a country map directly — call `ra_code_for()`

Phase 140 fixed the *values* in all four copies and left the *shape*, and the
shape was the rest of the bug. The Companies House prefix rule lived in
`routers/search.py` and the country map in `routers/lookup.py`, so only the
`/search` bridge consulted both. `/resolve-national-id` — the endpoint behind
the MCP tool **and** the frontend country picker — read the flat map, so
`country="GB"` scoped a Scottish or Northern Irish number to England & Wales
and returned nothing. Found by live production testing after #177 merged, not
by the code review that produced #177.

**`backend/opencheck/ra_codes.py` is now the single source.** It holds
`RA_BY_COUNTRY`, the `SUB_REGISTRIES` prefix table, and `ra_code_for(country,
number)`. `routers/lookup.py` and `routers/search.py` re-export their old names
for compatibility but neither owns the data any more.

- **Always pass the number**, not just the country. `RA_BY_COUNTRY["GB"]`
  answers "which registry is this country's", which is a different question
  from "which registry is this company in", and for GB the two differ for every
  Scottish and Northern Irish company.
- **`frontend/src/lib/raCodes.ts` mirrors it** — `raCodeFor()` plus a
  `subRegistries` declaration per entry. `backend/tests/test_ra_codes.py`
  parses that file and fails if the codes or the prefix rules diverge, and
  pins that `components/SearchPanel.tsx` (App.tsx until Phase 246) calls
  `raCodeFor()` rather than reading `entry.raCode`.
- **`COUNTRY_OPTIONS` is derived from `RA_CODES`**, not hand-listed. The
  hand-listed version had silently dropped New Zealand, and Greece was never
  added to the frontend at all when the ΓΕΜΗ adapter shipped — so two working
  backend mappings could not be selected. An absent option raises nothing.
- Adding a country: add it to `RA_BY_COUNTRY` and to `RA_CODES`, and add it to
  `VERIFIED` in `test_ra_codes.py` with a live-GLEIF check. A country whose
  companies split across authorities also needs a `SUB_REGISTRIES` entry, its
  frontend mirror, and prefix cases in the test — `test_only_gb_declares_sub_registries`
  fails until the test file acknowledges the new one.

### Flags that did NOT survive verification

Recorded so they are not "re-found" later:

* **`zefix.py` is correct.** `CH_RA_CODES` is a frozenset containing **both**
  `RA000548` and `RA000549`, and `gleif.py` imports it. Live GLEIF splits Swiss
  entities across both (RA000549 ≈ 29/50, RA000548 ≈ 13/50) with different
  `registeredAs` formats (`CHE-482.520.153` vs `CHE157821489`); `normalise_uid`
  handles both. An earlier flag here came from a grep that returned only the
  first match in the file.
* **`brreg.py` is correct.** `RA000270` appears only inside a comment saying it
  is *not* used for Norwegian entities. Live GLEIF: `RA000472` ≈ 48/50,
  `RA000473` ≈ 2/50.
* **Czechia is correct.** `ares.py` dispatches on `RA000163` (Commercial
  Register, Ministry of Justice), which live GLEIF confirms as dominant
  (45/50). `RA000168` — literally named "ARES" — appeared once, on a
  municipality. The adapter is named after the *API it queries*, not the
  register it dispatches on.

Re-verify with the GLEIF RA endpoint before changing any of them:
`https://api.gleif.org/api/v1/registration-authorities` — but note the raw
`filter[country]` query parameter is **silently ignored** and returns the
unfiltered global list, which is how the wrong codes got in here. Use the GLEIF
MCP tool's `country=` argument, or read `registeredAt.id` off real `lei-records`
filtered by `entity.legalAddress.country`.

---

## BODS mapper key conventions

- `_stable_id(*parts)` — deterministic SHA-256-based ID; format `"opencheck-" + 24 hex chars`. Used as both `statementId` and `recordId` for entity/person statements.
- `make_entity_statement()`, `make_person_statement()`, `make_relationship_statement()` — factory functions in `mapper.py`. Always use these; never hand-build BODS statements.
- `_source_block(source_id, url)` — builds the `source` field (`bods/statements.py`). Every source_id must be in `SOURCE_NAMES` (6 were missing, fixed in Phase 43). Since Phase 267 it also writes `opencheckSourceId` — see "A statement names its source by id" below.
- `_official_registers` set in mapper.py — source IDs that get `"type": ["officialRegister"]` instead of `"thirdParty"]`.
- Relationship statements: `statementId != recordId` (unlike entity/person where they're equal).
- Risk signal `statement_id` in evidence: `_bods_stable_id(source_id, hit_id)` — added to SANCTIONED/PEP evidence in `risk.py` in Phase 45 so frontend can look up which node to overlay.

---

## Key files quick reference

| File | Purpose |
|---|---|
| `backend/opencheck/routers/lookup.py` | Main lookup endpoint + SSE stream, one pipeline for both; the replay cache, gate and fold are in `lookup_replay.py`, the FullCheck expansion in `routers/expand.py` |
| `backend/opencheck/bods/mapper.py` | GLEIF mapper + passthroughs, and the address every per-source mapper in `bods/mappers/` is re-exported from |
| `backend/opencheck/risk.py` | Risk signal rules (PEP, SANCTIONED, structural complexity, FATF/EU, etc.) |
| `backend/opencheck/cross_check.py` | RELATED_PEP / RELATED_SANCTIONED from cross-source name matching |
| `frontend/src/components/BODSGraph.tsx` | Cytoscape.js ownership graph with BOVS icons, flags, edge annotations, risk overlays |
| `frontend/src/components/risk/RiskChip.tsx` | Risk signal colours and labels |
| `frontend/src/lib/bovsIcons.ts` | Base64 data URIs for 9 BOVS entity/person icons |
| `frontend/public/bods-dagre-images/` | BOVS icons (SVG) + 265 country flag SVGs |
| `backend/tests/test_ariregister.py` | HTML-fixture tests for the web scraper adapter |


---

## The graph surface (Phase 124)

**One canvas, one text equivalent.** `BodsGraphExplorer` renders `BODSGraph`
with `BodsTree` in a "Read as text" disclosure underneath. Do not add a second
view: v1 had a Split/Graph/Tree switch *plus* a "View as table" toggle inside
BODSGraph *plus* SubsidiaryNetwork's children list, so one report could show two
different tables of the same statements side by side and a third list below.
`BodsRelationshipTable` was deleted; if you need something it did, it is in
BodsTree — signal labels per row (`signalsByNode`, the same `buildSignalMap` the
canvas badges read) and `TreeRow.isolated` for a party no relationship statement
names.

**The legend is generated, never hand-written.** `lib/graphStyle.ts` holds
`SIGNAL_STYLE`, `EDGE_STYLE` and `NODE_MARK`; `buildGraphLegend()` turns them
into the entries for *this* graph — edge kinds present, signal codes actually
badged, worst severity first. It lives in `lib/` so the legend can read it
without importing Cytoscape and so the logic-only frontend suite can pin it
(`graphStyle.test.ts` fails the build if a badge has no readable name, or an
edge kind has no non-colour cue). `BODSGraph` re-exports `SIGNAL_STYLE` for
existing callers, and `backend/tests/test_signal_label_coverage.py` reads the
new path.

**`possiblySame` edges are synthesised from the `sameAs` prop**, not from
`model.edges` — anything deriving "what does this graph draw" has to add them
explicitly.

**An ended relationship is drawn faint, never dropped (Phase 219).** The rule
lives in `lib/relationshipStatus.ts` and, for the PDF/HTML/Markdown diagram,
`backend/opencheck/bods/lifecycle.py` — parallel tests, no shared file, move
them together. Ended = the record is `recordStatus: "closed"` **or** every
interest has an `endDate` on or before today; read both, because Open
Ownership's PSC extract has closed records with no `endDate` and CH officer
resignations have `endDate` on records never closed. A closed record with no
date says "ended", never an invented date. `GraphEdge.ended` / `endedOn` drive
`edge[?ended]` in the stylesheet: the kind's `EDGE_STYLE.endedColor` (a lighter
tint held at **≥3:1 on white**, WCAG 1.4.11 — Phase 219's `line-opacity: 0.5`
measured 1.75–2.26:1 and was replaced in Phase 243), a **hollow arrowhead**
(`ENDED_EDGE.arrowFill`, the non-colour cue) and **no label background**,
because a two- or three-line autorotated label on a short edge otherwise hides
the faded line entirely. The label's "ended <date>" line is the non-colour cue;
the legend gets an "Ended relationship" modifier (not a sixth edge kind — an
ended shareholding is still ownership); the tree row says it in words. When B
pools a current and an ended record for one pair, the edge is current and
labelled from its current interests; the ended ones move into `details`. C
hides a *current* ultimate-consolidation edge only behind *current* direct
edges. Why a fade: BOVS has no historical-relationship rule, but completeness
forbids omitting a party and relevance allows "tinting or transparency"; every
dash pattern was already taken. `graphStyle.test.ts` and
`test_reporting_diagram_parity.py` both measure every `endedColor`. The risk engine reads the same rule since
Phase 220 — see "Ended relationships in the risk engine" below.

---

## The graph at scale (Phase 243)

`lib/graphScale.ts` (pure, tested in `graphScale.test.ts`) + `BODSGraph.tsx`.
Found by the Opus 5.5 check (D-H3, A-M1, A-M2): Shell's FullCheck is 16
owners/officers, SHELL PLC and 132 single-parent leaf subsidiaries, which
dagre drew as two rows thousands of pixels wide — 3px labels at Fit.

- **Sibling clusters.** On a rank wider than `RANK_CLUSTER_THRESHOLD` (40),
  the leaf siblings one hub shares are grouped by the joining edge's first
  source + kind + ended ("GLEIF: 94 subsidiaries"), groups of ≥
  `MIN_CLUSTER_SIZE` (5) only. Leaf *parents* above a hub group the same way
  ("… officers"). **A node carrying a signal badge is never grouped**
  (Stephen, 24 Sept 2026). Clusters are canvas-only: `BodsTree` still lists
  every row, and a search match or a selection from the tree opens the group
  that hides it. Cluster ids are stable across FullCheck expansion. Labels
  never say "owns" (GLEIF L2 is consolidation) — the test pins it.
- **Wrapped ranks.** After dagre, `wrapWideRanks` folds any rank that is all
  leaves (down) or all roots (up) and wider than `WRAP_MAX_COLS` into rows
  matching the canvas aspect, spaced for the widest node, and moves the ranks
  beyond it. A rank that mixes parents and leaves is left alone.
- **Fit to content.** The canvas height follows the drawn bounding box
  (`canvasHeightFor`, 360–640px) and "Fit" fits displayed elements only.
- **Overlay targets.** Every overlay control is a transparent box ≥ 24px
  square (`hitBox`, WCAG 2.5.8) around the drawn pill; badge type ≥ 11px. The
  overlay is **one tab stop** (`role="toolbar"`, roving tabindex, arrows /
  Home / End in reading order), a focused off-canvas mark pans into view, and
  a "Skip to text version" link comes first. A collapse toggle is drawn only
  where collapsing hides something (`collapsibleNodes`) — in a DAG most
  officers' "−" did nothing. The single and stacked signal badges are one
  `<button>`.
- **Legend and phones.** The signal legend starts open; "Read as text" starts
  open under 640px (`prefersTextFirst`), with the canvas still above it.

---

## Readable at Fit, marks that do not collide (Phase 250)

Follow-ups to Phase 243 found on Shell PLC's FullCheck. `lib/graphScale.ts`
(pure, pinned in `graphScale.test.ts`), `BODSGraph.tsx`,
`BodsGraphExplorer.tsx`, `lib/signalKind.ts`, `lib/reconcile.ts`.

- **Readable at Fit = labels render ≥ `READABLE_LABEL_PX` (9).** Three
  levers, in order, inside the layout effect: (1) when a fit is unreadable,
  `dense` turns on and sibling groups form on any rank wider than
  `WRAP_MAX_COLS`, not only past 40 — a second layout pass; sticky while the
  network grows, cleared when it shrinks; a badged node is still never
  grouped. (2) `wrapWideRanks({ viewport })` folds a rank into whichever
  column count lets Fit zoom furthest in (only when the unfolded fit is below
  `FOLD_ZOOM`), which can fold a rank narrower than `WRAP_MAX_COLS`. (3)
  `labelFontFor(zoom)` grows node labels in graph units up to
  `LABEL_FONT_MAX_PX` (14); every layout starts from the stylesheet size
  (`removeStyle`). Past the cap, a very wide graph can still render smaller —
  a phone, or a rank that mixes parents and leaves (never folded).
- **Marks are placed after the fit, in screen space.** Badges and toggles
  keep a pixel floor while nodes shrink, so `resolveMarkCollisions` lifts a
  signal badge / drops a toggle until its ≥24px hit box clears every other
  mark and every other node (and flag); a lifted badge draws a thread to its
  node. `signalPillSize` / `togglePillSize` are shared by the check and the
  render — change the size in one place.
- **The text version has its own fold state.** Canvas collapse no longer
  removes rows from "Read as text" (Stephen, 26 Sept 2026); choosing a row
  there calls `revealIn` to open whatever hides it on the canvas.
- **Network risk chips are grouped by code** (`groupNetworkSignals`: risk,
  then context on a "Structural context" row), so the chip count is the
  sentence's count by construction; `RiskChip group=` draws "×N" and opens
  every instance's evidence.
- **People fold a shorter name into a fuller one** (`foldPersonGroups`) —
  same birth month, token subset, exactly one candidate, ≥2 tokens. Wikidata
  roleholders carry P569 at Wikidata's precision for this (year-only stays
  `YYYY`, never `YYYY-01-01`).

---

## Ended relationships in the risk engine (Phase 220)

`risk.py` reads `bods/lifecycle.py` (`statement_lifecycle`) through two
helpers: `_ended_relationship_ids(bods)` and `former_party_ids(bods)`.
Stephen's decisions (17 Sept 2026), which are what the code implements:

- **Structural signals keep ended relationships, and say so.** No signal is
  dropped or re-graded; where an ended link is part of what a signal counted,
  its summary gains "including ended relationships" (`risk.INCLUDING_ENDED`)
  and its evidence gains `includes_ended_relationships: true` +
  `ended_relationship_statement_ids`. **The keys are absent — not `false` —
  when nothing ended contributed**, so a current-only signal is unchanged.
  Covered: `COMPLEX_OWNERSHIP_LAYERS` (the DFS prefers, between equally long
  paths, the one with fewer ended links; a current record for the same pair
  makes the link current), `STATE_CONTROLLED` (qualified only
  when a state owner has no current holding; the overlay anchors on a current
  holding first), `NON_EU_JURISDICTION` (qualified when a non-EU party is
  reachable *only* through an ended link — `_upstream_entity_ids(...,
  current_only=True)`), textual `NOMINEE`, and `OPAQUE_OWNERSHIP` (an
  unspecified-reason relationship that ended, or a withheld party that is
  former). `_subject_entity_id` asks the whole graph for a unique sink first
  and the current graph only when that is ambiguous — current-first made a
  subject's ended owner the sink.
- **Related-party screens keep former parties and call them "former".**
  `cross_check._collect_targets` and `icij_check._collect_targets` set
  `target["former"]`; the summary reads "Former related party / entity"
  (`cross_check.related_party_label`), evidence carries `former: true` (absent
  otherwise), and so does OpenAleph's screening entry and EveryPolitician's
  row finding (`finding_everypolitician(..., former=)`).
- **"Former" is anchor-free and conservative**: the interested party of at
  least one relationship, every such relationship ended, and not the subject
  of any current relationship. It never invents a former party; it can miss
  one (a former owner that still has current owners of its own in the bundle).
  The looked-up company is never former — it is never an interested party.
- **Left alone on purpose:** the structured `NOMINEE` path still skips a
  ceased Companies House PSC (`ceased_on`), an older decision;
  `TRUST_OR_ARRANGEMENT` and the FATF / EU high-risk lists read entity
  statements, not relationships.
- **Nothing to change in `/signalstats` or the picker cards**: no code, no
  confidence and no source changed, and both read codes only. The narrative
  model does see the new wording, through each signal's summary.

Tests: `tests/test_risk_ended_relationships.py` (both ended shapes, the
BANK SADERAT PLC shape — one closed PSC record).

---

## Saying it where it can be read (Phase 124)

**Do not use `title=` to explain anything.** It is invisible to keyboard,
invisible on touch, unstyleable, truncates at length and is announced
inconsistently. There are now zero non-`BtsCard` `title` attributes in
`frontend/src`, and the Phase 124 sweep found 18 of the 22 were the only place
something substantive was said.

Use, in order of preference: a **visible** label; `ui/Explain` (a focusable
button toggling the text in flow) for an explanation long enough to be a choice;
`ui/Described` + `aria-describedby` for a short one; `sr-only` only where the
layout genuinely cannot hold the sentence. `sr-only` alone is a last resort —
`Explain`'s docstring states the rule: hiding a sentence from sighted users and
showing it to screen readers reproduces the original bug with the audiences
swapped.

**Two confidences, and they are not interchangeable.** `ui/Chip`'s
`CONFIDENCE_GLYPH` / `CONFIDENCE_LABEL` (●◐○) describe **corroboration** — how
many sources assert a thing. `ui/MatchConfidenceChip` describes **match
strength** — how sure we are that a record is the same party. Do not merge
them: `CONFIDENCE_LABEL` reads "Corroborated by two or more sources", and
beside a single-source name match that is false, in the one place OpenCheck is
most careful (a name match is explicitly not an identity claim). Match strength
never borrows the ●◐○ glyphs. `SubsidiaryNetwork`'s `RelationBadge` is a third
thing again — a relation kind, not a confidence.

**One word per concept, in `frontend/src/lib/vocab.ts`.** Results, never hits.
`sourceLabel()` / `sourceList()` for any source id shown to a reader — never a
raw slug, never `.join(" and ")` over ids. `topicLabel()` for OpenAleph's
FollowTheMoney topics. `LOOKUP_VERB` / `PERSON_VERB` — there were four verbs for
two actions. `NOT_IN_GRAPH` for data a source publishes that OpenCheck does not
map. These are in `lib/` because the frontend suite is logic-only: a term that
exists only as a literal inside JSX cannot be pinned, which is how the four
verbs happened.

The ●◐○ confidence glyphs are defined once, in `ui/Chip.tsx`
(`CONFIDENCE_GLYPH` / `CONFIDENCE_LABEL`), and `ui/ConfidenceLegend` renders
their meaning visibly beside the chips.

---

## Honest progress and honest failure (Phase 124)

**`SearchLoadingGrid` renders SSE events and nothing else.** The logic is in
`lib/lookupProgress.ts`: a source is shown only in a state the stream has said
it is in, `total` is `null` rather than `0` while unknown (0 renders as a
complete bar), failures are counted separately from successes, and the label
only reaches the past tense when everything has settled. Before
`sources_applicable` there are no chips, because there is nothing true to draw.
**Any source that can appear in `sources_applicable` must eventually emit a
terminal event** — `source_completed` or `source_error`. `sec_edgar` did not
when a name resolved to no CIK, and the counter could never reach its own total.

**A panel that fetches outside `_lookup_pipeline` must report its failures.**
`/securities` and `/subsidiaries` get no `source_error`, are not in
`sources_applicable` and are not replay-cached, so nothing else knows they
failed. They report through `lib/panelErrors.ts` to `PanelErrorsNotice` —
**deliberately not into `degraded_sources`**, which arrives on the same event as
the signals and the backend-built verdict sentence so the three are provably
consistent; `onRiskSignals` also overwrites it wholesale. Report recovery as
well as failure, or a stale warning outlives the thing it warned about.

---

## Design-system lint (Phase 124)

`frontend/scripts/lint-design-system.mjs`, run by the frontend CI job and
`npm run lint:design`. Two rules: no raw hex outside the token files, no
`text-[NNpx]` outside the named scale.

A third rule bans **exact user-facing labels** listed in `BANNED_SYNONYMS`
(`lib/vocab.ts`, read by the lint so the two cannot disagree). **Phrases, not
words** — the first version banned single words and every one had legitimate
uses: "not a clean screen", "a fast screen of the subject", the SSE event named
`"hit"`, `Liveness = "stub"`, `min-h-screen`. It reported 90 false violations in
`App.tsx` alone. The matcher uses **TypeScript's own parser** (already a
devDependency), walking string literals and JSX text minus anything inside a
`className`. Do not rewrite it with a regex: word boundaries over whole files
flag `bucket.hits.length`, and restricting to quoted strings is worse, because
an apostrophe in JSX text ("doesn't") pairs with the next one and swallows the
code between.

**It is a ratchet.** `design-system-baseline.json` records what each file
carries; a file may never carry more, and a new file may carry none. It also
fails on a *stale* baseline, so a commit that improves a count must run
`npm run lint:design -- --update` and lock the gain in. `--update` refuses to
raise a count without `--allow-increase`. Allowlisted for hex:
`lib/graphStyle.ts` (Cytoscape takes colour strings, not class names — this is
the graph's token file) and `lib/bovsIcons.ts` (base64 data URIs).

**Two more metrics since Phase 241**, on the same per-file ratchet:
**raw Tailwind palette classes** (`bg-emerald-50`, `hover:text-rose-700` —
*every* Tailwind hue, so moving a colour from emerald to teal cannot read as
progress) and **raw `<button>` JSX elements outside `components/ui/`**
(counted by the TypeScript parser; test files exempt). Use the `oo-*` tokens
for what a colour *means* — `oo-risk-*` (a finding against the subject, the
only red-family tier; `Chip tone="risk"` reads it), `oo-warn-*`, `oo-ok-*`,
`oo-info-*`, `oo-esg-*` (the Climate & ESG tab, built on `oo.node.teal`) — and
`ui/Button` (a `person` variant, BackgroundCheck's purple, joined the five in
Phase 241). The 12/13/14/15/26px arbitrary sizes were codemodded onto
`oo-meta`/`oo-small`/`oo-body`/`oo-lead`/`oo-display`; 11px and 10px stay
arbitrary **on purpose** (Stephen, 24 Sept 2026: no `oo-micro` step), so the
ratchet keeps pushing them up to the 12px floor rather than legitimising them.
A licence condition is `warn`, never `risk`: the export panel's red
"share-alike" boxes read like sanctions findings.

---

## Coverage: one definition, every source named (Phase 241)

`backend/opencheck/coverage.py` (`source_coverage`, `coverage_sentence`,
`report_coverage`) and `frontend/src/lib/lookupProgress.ts` (`settledCount`,
`settledLine`, `noRecordSources`). Every surface that counts sources uses
these — the MCP summary, the batch row (so the watchlist baseline and the CSV),
the PDF/Markdown "What each source found", the loading grid, the "What each
source said" header and the Coverage column. Before, three denominators: the
MCP summary divided sources *with a record* by sources *with a record or an
error* ("11 of 11 sources returned data" for Shell, 13 applicable), and the
loading bar left the GLEIF anchor out while the header put it in.

- **Buckets per applicable source:** with data · no record · did not answer;
  *answered* = with data + no record. A source that returned a record and then
  errored (a deepen/read timeout) is answered *and* partial. The GLEIF anchor
  is applicable and answered whenever the lookup resolved.
- **Batch `coverage.answered` changed meaning**: it counts every source that
  replied; `with_data` is what `answered` meant from Phase 164 to 240. The
  watchlist reads a baseline without `with_data` as pre-241 and compares it on
  `with_data` only (`watchlist._coverage_view`) — otherwise every watch would
  report "coverage changed" on its first re-run after deploy.
- **Every source error is a degradation** (Stephen, 24 Sept 2026):
  `degradation.add_source_errors` runs in both `_lookup_pipeline` and
  `_build_report` — `check: source_fetch` when the source returned nothing,
  `source_read` when it returned a record and the read failed. An adapter's own
  record for the source is not repeated. The verdict says "one source answered
  only in part" for `source_read`.
- **No-record sources are named**: a chip row "Answered with no record" under
  "What each source said", and "Answered with no record: …" / "Did not answer:
  …" lines in the PDF/Markdown. Absence in the same voice as presence.

---

## The watchlist re-runs on deltas, never on a clock (Phase 215)

`opencheck/watchlist.py` + `routers/watch.py`; design in `docs/watchlist.md`.
A watched LEI is re-run only when GLEIF's Golden Copy delta names it with a
**material** field changed, or when OpenSanctions' entity delta names it.
Things that will be re-derived otherwise:

- **Tier 1 rides on `mirror_refresh.apply_delta`**, which hands the LEIs its
  three delta files named to `watchlist.on_gleif_delta` after the watermark
  is written. The hook never raises (a watcher failure must not fail the
  refresh — a test pins it) and must not add keys to `rows_applied`.
- **`GLEIF_MATERIAL_FIELDS` deliberately omits `NextRenewalDate` and
  `LastUpdateDate`.** Most of a day's 16,000 delta rows are renewal churn;
  the digest of the material fields is what filters it. Adding either
  column re-runs every watched LEI once a year for nothing. Address lines,
  managing LOU and validation sources stay out for the same reason; the
  address *countries*, the register identifier, successors (with names),
  category and conformity flag are in (Phase 300).
- **Adding a material field needs the baseline-shape rule, not a migration.**
  `diff_gleif_facts` compares only fields both sides carry, and
  `on_gleif_delta` decides on that diff (Phase 301 — not a digest of the
  projection, which a retired key could trip); a churn row whose baseline
  predates the field is upgraded in place (`upgrade_facts`,
  `gleif_rebaselined`) with no entry. Widening the set must never write
  "— → value" entries for every watched LEI. Keep `FIELD_WORDS` in
  `routers/watch.py` and `lib/watchlist.ts` in step with the tuple — a test
  pins the backend one.
- **A delta names parents too (Phase 302).** `apply_delta`'s `named` set
  holds the RR end nodes and each delta child's *previous* direct parent
  (`previous_direct_parents`, read before `load_rr`). Drop either and a
  watched parent stops hearing about subsidiaries joining or leaving.
  `direct_children` is LEIs only, from the relationships table's own
  standing — never the entities' parent column, never names.
- **Replace a release asset with `scripts/replace_release_asset.sh`, not
  `gh release upload --clobber`** (Phase 302). `--clobber` deletes first and
  retries only the upload, so one unretried API error fails the job (run #5,
  7 Oct 2026) and a failed upload after the delete leaves the release
  empty. The script stages `NAME.new`, checks its size, then deletes and
  renames idempotently with retries; `tests/test_replace_release_asset.py`
  drives it against a fake `gh`. `refresh-entity-pages-db` uses it; the
  other release-uploading workflows still use `--clobber`.
- **GLEIF's field-modification log (Phase 303) is read once per hit, never
  per watch.** One call on a GLEIF-tier re-run (since the oldest baseline;
  each list keeps its own share via `gleif_log.after`) and one when a list
  first watches an LEI (the 30 days before). Never on a schedule, never for
  OpenSanctions or catch-up re-runs, never through a client other than
  `build_client()` (the throttle). `fetch_since` never raises. The watchlist
  test fixture stubs it, because the fixture allows live calls; a new test
  that adds watches outside that fixture must stub it too or the network
  guard fails it. `watchlist.sqlite` is at migration 2 — never edit step 1
  or 2, append.
- **Legal Entity Events (Phase 301) are read only from a full build.**
  `gleif_facts` omits `corporate_events` (absent, not `None`) unless
  `EntityStore.carries_events` — `meta.detail_events`, set by the build
  script on a full build only. Do not set it from a delta, and do not make
  the key `None` on an older file: either would make the rebuild read as a
  new event on every watched LEI. The one exception to the both-sides rule
  is the since-rule — events *recorded* after a pre-events baseline's
  `gleif_watermark` are new. `resync_after_rebuild()` re-reads every watch
  once per `meta.built_at`, because a replaced file never passes through
  the delta hook. Address and other-name events are excluded
  (`CORPORATE_EVENT_EXCLUDED`); every other type, including future ones, is
  material.
- **Absence is a finding only when the producer answered.** `diff_snapshots`
  emits `signal_unchecked` (not `signal_retired`) when the source that
  produced a code is in `degraded_sources`, and `coverage_unchecked` when
  coverage fell because of degraded sources. The feed and the page word
  these as "could not re-check". Do not collapse the pair.
- **The token is a capability**: minted server-side, kept in the browser's
  `localStorage`, stored only as SHA-256. `rows_for_lei(with_hash=True)` is
  the one path that reads a hash back, for the re-run; never serialise it.
- **No mirror, no Tier 1.** `entity_pages.get_store()` is where the facts
  come from; on an instance without the file the page says the GLEIF tier
  cannot fire. Local dev without `OPENCHECK_WATCHLIST_DB_FILE` = 503.
- **Tier 2 is organisations only** (`_OS_ORG_SCHEMATA`) and the watched
  entity's own names. Person schemata are never read — that would put UBO
  names into the store, which is the v2/GDPR line.
- **The illustration is built** by `scripts/build_feature_images.py
  watchlist` from a real GLEIF fact (an LEI that lapsed on 15 Sept 2026) and
  a "re-run found no difference" entry, never an invented finding against a
  real company. Regenerating in a different Chromium/Pillow changes the
  bytes of every image; ship only the one you changed.

---

## A saved report is the server's own copy of a run (Phase 216)

`opencheck/saved_reports.py` + `routers/saved_reports.py`; design in
`docs/saved-reports.md`. Things that will be re-derived otherwise:

- **The payload is the lookup's event stream, not `LookupResponse`.** The
  React report is a fold over the stream, so replaying stored events is what
  renders the same report. `fold_lookup_events` (`opencheck/lookup_replay.py`,
  re-exported from `routers.lookup`) is the one fold
  — `_lookup_impl` calls it too — so a saved report's PDF/MCP view cannot drift
  from the live one. Keep `deepen_result` in the stored events: the stream
  skips it, the exports need its BODS.
- **Never accept a payload from a client.** A save names a run (`lei` +
  `run_completed_at`, which the `done` event now carries) and copies the held
  run out of `replay_entry()`. `SaveRequest` is `extra="forbid"`. A run that
  has aged out, or that a per-source retry cleared, answers 409 "run the check
  again" — never a silent re-run (Stephen, 16 Sept 2026).
- **`run_completed_at` ≠ `fetched_at`.** `fetched_at` stays replay-only so a
  live run is never badged as cached; `run_completed_at` names every run.
- **Narratives only if this server wrote them from the same run** —
  `routers.narrative.held_narrative`, held for the replay window.
- **The hash covers the exact bytes served.** `canonical_bytes` (sorted keys,
  compact, UTF-8) is serialised once, stored gzipped and served by
  `/saved-reports/{id}.json`; the hash is re-checked on read. Do not
  re-serialise a payload before hashing or serving it.
- **Two capabilities:** `report_id` (read, shared) and `manage_token`
  (extend/delete, stored as SHA-256). Extending never touches the hashed bytes.
- **Live disposition sheets live in the same SQLite file** when it is set;
  `dispositions.py` falls back to the old JSON files otherwise. A saved report
  carries a frozen copy of the sheet.
- Every route under `@limiter.limit` returning a dict takes
  `response: Response` — `test_every_saved_report_route_answers_with_the_limiter_on`.

### The saved-report page (Phase 217)

- **One event→handler table** (`LOOKUP_EVENT_HANDLERS` in `lib/api.ts`) and one
  handler builder (`useLookupStream().handlers` in `hooks/useLookupStream.ts`,
  wrapped by `buildLookupHandlers` in `App.tsx`) serve the live stream and
  `replayLookupEvents`. Add a new lookup event to the table, never to one side.
- **Anything that fetches when opened must consult `SavedReportContext`**
  (`components/cdd/savedReportContext.ts`) or be hidden on a saved report —
  the source Data drawer (`/deepen`), FullCheck (`/lookup`, `/expand`),
  NZ associations, securities, the licence matrix. A saved page that quietly
  shows today's record next to the saved one is the failure this avoids.
- **`deepen_result` carries the drawer's fields** (`bods_issues`,
  `risk_signals`, `license`, `license_notice`) but never `raw`; keep it that way.
- Wording, dates (UTC, "16 Sept 2026") and save eligibility live in
  `lib/savedReport.ts`; `/report/{id}` rolls up to `/report` in `canonicalPath`.

### Rendering a saved report (Phase 218)

- **`open_for_export` (routers/saved_reports.py) is the one way in** for the
  PDF, Markdown, `/export`, the share page and card: load → verify → fold →
  `saved` block. Never call `_lookup_impl` on a `saved_report_id` path —
  `test_saved_report_exports.py` monkeypatches it to raise.
- **Pass `saved=` to the report builders only when there is one.** Existing
  tests mock `build_report_pdf(report, *, narrative=None, dispositions=None)`.
- **A saved render reads no clock and no current table**: dates from
  `saved_at` / `run_completed_at`, licence from `saved["licensing"]`,
  `sources_consulted` from the run. Adding a `datetime.now()` or a live
  `assess()` to a report section breaks the "same record on any day" claim;
  route it through `saved` like `_generated_line` does.
- The saved share link is `/share/saved/{id}` (API host), not `/report/{id}`
  — the preview must be the record's card and date.


---

## What is knowable, per jurisdiction — the data is Stephen's Notion table (Phases 223–227)

`opencheck/knowability.py` + `data/jurisdictions.json`; the review page is
`docs/knowability.md`. One statement per jurisdiction of what its registers
publish about beneficial owners, who may see it, and what OpenCheck reads of
it — the layer the QuickCheck verdict strip, the FullCheck chain, the
PDF/Markdown report, the MCP `summary` and the entity pages will carry
(Phases B–D of the knowability ticket). Things that will be re-derived
otherwise:

- **Never hand-type a legal fact into the repo.** `jurisdictions.json` is
  generated by `scripts/sync_jurisdictions.py` from the Notion database
  *Beneficial ownership access status* (data source
  `b6be86df-6d76-422b-93bd-f1ece5a99781`), which Stephen maintains. Three
  ways in: `--notion` (an internal integration token in `NOTION_API_KEY`),
  `--csv` (a Notion export), `--rows-json` (the Notion MCP query shape).
  Then `scripts/generate_knowability_doc.py`; a stale `docs/knowability.md`
  fails `test_knowability.py`. A row with no *Last verified* date is carried
  as `review_status: unverified` and the doc says so beside it.
- **The access enum, not a date, is the model:** `public` ·
  `public_with_registration_or_justification` · `legitimate_interest` ·
  `restricted_no_lia_route_yet` · `authorities_and_obliged_entities_only` ·
  `no_register` · `in_progress`, with `access_since` and
  `next_change_expected`. `bo_access.py` is now a **view** over it
  (`data/eu_bo_access.json` is gone): the three restricted statuses give a
  "restricted" footnote, a public register with an announced change gives
  "becoming restricted", everything else none. Its public names and the
  `/sources` shape are unchanged.
- **The sentence describes and dates; it never judges.** `BANNED_VOCABULARY`
  (opaque, secrecy, haven, hidden, shell, risk, suspicious…) fails the build
  on any sentence. Nothing here feeds `risk.py` — a jurisdiction-level "no
  register" *signal* was rejected in Phase 111–113 because "we found
  nothing" conflates *no register* / *closed register* / *no adapter yet*;
  this statement is where that three-way distinction is made honestly, as a
  dated fact.
- **`opencheck_reads` is derived from the REGISTRY** (`SourceInfo.country`)
  and `bo_regimes.record_kinds` (an adapter "reads beneficial owners" only
  where a record kind is `assert_true`). It is never typed, so it cannot
  drift from what the pipeline dispatches; Stephen's free-text *OpenCheck
  source* column is a note, not the source of truth.
- **Absence is a sentence, never silence.** An unknown code gets
  `stated_absence: true` and "OpenCheck holds no register notes for X; an
  owner absent from this report says nothing about what X publishes".
  `US-DE` falls back to `US`.
- **Every sentence goes through `clauses_to_sentence`** (140-char cap), so
  verification is its own sentence rather than a clause a long threshold
  would push off the end.
- `GET /knowability?jurisdictions=GB,KY` is pure and cached an hour; unknown
  codes are answered, never 404. `chain_jurisdictions(bods, subject_id)`
  walks *upwards* through `risk._upstream_entity_ids`, **ended relationships
  included** (Stephen, 18 Sept 2026) — Phase C wires it into FullCheck.
- The quarterly BO-regimes legal review (first run 1 Oct 2026) now covers
  this table too: re-verify rows, re-sync, regenerate.
- **The `knowability` lookup event (Phase 224) is the statement's one way
  onto the page.** `_lookup_pipeline` yields it right after `gleif_done`
  (the first moment the jurisdiction is known, before the fan-out) with the
  statement's JSON plus `as_of`; `fold_lookup_events` copies it into
  `LookupResponse.knowability` **as recorded** — never re-rendered — so a
  saved report replays the sentence that was true on the day of the run
  (the sentence is dated; Phase 218's no-clock rule). No jurisdiction → no
  event, and the strip says nothing rather than guessing. On the frontend
  it is one entry in `LOOKUP_EVENT_HANDLERS` (`knowability: "onKnowability"`)
  so live and replay cannot drift; `lib/knowability.ts` is the pure values
  layer (badge tones `context`/`neutral` only — never `risk`/`warn`) and the
  "What can be known" band in `VerdictStrip` renders the server sentence
  verbatim with the per-field list behind `ui/Explain`. It is a fourth band
  under the three columns, not a chip in "What we found": the statement is
  never a signal and never counts.
- **The chain (Phase 226) is a second event, `knowability_chain`,** yielded
  after `subject_profile` because it needs the deepened graph:
  `chain_for_lei(lei, bods_all)` walks *up* from every statement carrying
  the LEI (`subject_statements`), in **path order** (each rank of owners
  before the rank above — `chain_jurisdictions_from`), ended links included,
  and carries one statement per code plus `as_of`. Folded as recorded into
  `LookupResponse.knowability_chain`. The exports (`html_report._knowability`
  → PDF, `markdown_report._knowability`, a "What can be known" section after
  "What each source found") and the MCP `knowability: {subject, chain[],
  as_of}` field all read the two frozen payloads through
  `knowability.report_statements()` — **never `statement_for` at render
  time** — so a saved report says what was true on the day of the run. The
  FullCheck panel's "What can be known along the path" list
  (`KnowabilityChainList`) starts from the frozen chain and, as the network
  expands (`BodsGraphExplorer.onNetworkChange` → `lib/knowabilityChain.ts
  chainCodes`), fetches statements for new codes through `GET /knowability`
  only; a frozen sentence is never replaced by a fetched one (`mergeChain`),
  and a saved report fetches nothing.
- **Entity pages (Phase 227)** carry the statement too, as
  `entity_pages.knowability_section(row.jurisdiction, today)` between the
  reference-data `<dl>` and the children — a local table read keyed on the
  jurisdiction, never a fetch (the module's no-adapter rule holds). Because
  the sentence is dated and the table is synced, the page ETag includes
  `knowability.GENERATED_AT` and today's date alongside `TEMPLATE_VERSION`
  (bumped to "2"), so a re-sync or a passed "change announced" date re-fetches
  every page.
- Running the scripts locally: `cd backend && uv run python
  scripts/sync_jurisdictions.py --notion` — the system `python3` has neither
  pydantic nor pytest. The script reads `NOTION_API_KEY` from `backend/.env`
  or the repo-root `.env` itself (the app's pydantic-settings does not run
  for scripts); the integration is *internal*, read-only, shared with that
  one database, and a Notion 404 means "not shared", not "wrong id".

---

## One lookup budget per client, whoever asks (Phase 234)

`opencheck/lookup_budget.py` + `mcp/guard.py`. Route limits count *requests*;
this counts *work*. Before it, `/expand-layer` ran 25 pipelines per request on
the 60/min default tier (~1,500 full lookups a minute from one IP), `/expand`
and `POST /watch/items` ran one each on the default tier, a batch ran twenty,
and MCP had no limit at all. Things that will be re-derived otherwise:

- **The charge is at the one moment a fresh pipeline starts** —
  `_lookup_pipeline_cached`, via `lookup_budget.charge()`. A replay is free,
  and so is **joining a run in flight**: a fresh run is a detached `_Flight`
  task buffering its events, and anyone asking for the same
  `(lei, deepen_top)` meanwhile follows the same buffer. A follower that goes
  away (an SSE tab closed) does not cancel the run; it completes into the
  replay cache. The budget is sized by `OPENCHECK_RATE_LIMIT_LOOKUP`, the
  string that sizes `/lookup`, so nothing can out-run `/lookup`.
- **Who is charged comes from a context variable** set by
  `ClientScopeMiddleware` (pure ASGI — never `BaseHTTPMiddleware`, which breaks
  SSE and `/mcp`). It reaches MCP tool bodies (tested), batch rows and every
  in-process `_lookup_impl` caller. **No client = server work = never charged**:
  the watchlist worker and warm-ups. So a new *request path* is charged
  automatically; a new *background task* is free by construction — keep it so.
- **A spent budget is a 429 with `retry_after_s`** — an `error` event on the
  stream, `HTTPException(429, headers={"Retry-After"})` from `_lookup_impl`.
  `/expand-layer` turns it into `deferred` anchors (never in `expanded`, so they
  stay on the frontier) and FullCheck's auto-run waits them out
  (`lib/expandLayer.ts`, at most `MAX_BUDGET_WAITS` per layer). Batch rows wait
  instead (`lookup_budget.waiting`, `OPENCHECK_BATCH_BUDGET_WAIT_S`) and a row
  still refused is `retryable`. A malformed LEI (400) and a run refused a
  pipeline slot (503) are refunded.
- **Failed FullCheck hops are `failed`, with a reason** — the old
  `except Exception: return [], []` drew a node whose owners could not be
  fetched exactly like a node with none.
- **`_PipelineGate`**: at most `OPENCHECK_LOOKUP_MAX_CONCURRENT` (4) pipelines
  at once, queue up to `OPENCHECK_LOOKUP_QUEUE_WAIT_S` then 503 (Phase 238:
  FIFO, and the interactive stream waits longer — see below). It and every
  flight are bound to the running event loop and rebuilt when it changes (the
  test suite runs several); `conftest.py` clears `_IN_FLIGHT` and the budget
  around every test. Nothing in a pipeline may call `_lookup_impl` — a nested
  run would wait on a slot its parent holds.
- **`deepen_top` is clamped to 0–10** in `_lookup_pipeline_cached` and
  `replay_entry` (`clamp_deepen_top`): every value is its own replay key, and
  the MCP tools accepted any int.
- **`/mcp` is behind `McpRateGuard`**, wrapped onto the route in `app.py` with
  `guard_routes`: default tier per request, plus each `tools/call` at its REST
  counterpart's tier (`TOOL_TIERS`). The session manager runs once per process,
  so an HTTP-level MCP test builds its own Starlette app from a fresh manager
  (`tests/test_lookup_budget.py::mcp_client`).
- **Discretionary GLEIF** (`gleif_throttle.discretionary()`): share cards,
  `/resolve-national-id`, `/subsidiaries`, `/securities` are refused at once
  when fewer than `OPENCHECK_GLEIF_LOOKUP_RESERVE` (10) slots are left, so a
  crawler on them cannot starve a lookup's anchor.
- **`lookup_budget.Quota`** (the `limits` library, in memory, reset on
  deploy) holds the per-client caps on shared capacity: new watchlists
  (`5/day`), saved reports (`20/day`, REST + MCP), and the MCP tool tiers.
  Check before the work, `hit()` after it succeeds. Watch caps are checked
  before the baseline runs; a list unopened for `OPENCHECK_WATCHLIST_STALE_DAYS`
  (90) is deleted by the worker's tick.

---

## The primary listing comes from PermID, frozen in one event (Phase 236)

`opencheck/listing.py` + `frontend/src/lib/listing.ts`; design in
`docs/listing.md`. Things that will be re-derived otherwise:

- **One `listing` lookup event**, started as a task after `gleif_done` and
  awaited before `subject_profile`; `fold_lookup_events` copies it into
  `LookupResponse.listing` as recorded and puts `publicListing` on a *copy*
  of the subject's GLEIF entity statement (`listing.apply_to_bods`) — so a
  saved report exports the listing of its own day. It is one entry in
  `LOOKUP_EVENT_HANDLERS`. Not a registered source; not in coverage counts.
- **No key → no task, no event, no line.** Gated on `PERMID_API_KEY` *and*
  `OPENCHECK_ALLOW_LIVE`. The UI never says "not listed": `not_listed`
  renders nothing.
- **A PermID failure is `status: "unavailable"` on the event, never a
  `DegradedSource`** (Stephen, 24 Sept 2026). Every reader of
  `degraded_sources` — the verdict, the MCP CAUTION, the batch chip — treats
  an entry as a screen that did not run. **Failures are cached for one
  hour under `permid/unavailable/<LEI>`** (Phase 285) — their own key, so a
  failure never overwrites a listing.
- **A 2xx is only a success if it is a JSON object** (Phase 285). On 3 Oct
  2026 the record service answered `200 application/ld+json` with the body
  `An error has occurred.` for every entity, and permid.org serves its
  website as `200 text/html` at any path. Either is `reason:
  "bad_response"` with a `detail` naming status, content type and a body
  excerpt. **The line names the failure**: "did not answer" only for
  `timeout`; "returned an error" for `upstream_error`/`bad_response`; "is
  limiting requests" for `rate_limited` (`UNAVAILABLE_LINES` /
  `UNAVAILABLE_TEXTS`, mirrored).
- **Search cannot replace the record service.** A quote search result's
  `isQuoteOf` is an *instrument*, and quote search by organisation PermID
  returns nothing; only the organisation record links LEI → primary quote.
- **Check `tr-org:hasLEI` on the organisation record** before using it: the
  search is a text search.
- **Links only for browser-verified venue patterns** (`VENUES` in
  `listing.py`); Euronext gets its ticker *search* page. Never call a venue
  page "filings"; `companyFilingsURLs` stays empty. PermID is not in BODS's
  closed `securitiesIdentifierSchemes`, so `security` is the ticker alone.
- permid.org is behind Cloudflare: a generic library User-Agent gets 403
  (error 1010); the search endpoint 406s on `Accept: application/ld+json`.
  The token is a query parameter — describe errors with `secret_scrub`.

---

## OFFSHORE_LEAKS is gated by date and jurisdiction, never by ICIJ's flag (Phase 237)

`icij_check.py`. Found by the Opus 5.5 check (DQ-3): CLP HOLDINGS LIMITED
(Jersey, founded 2021) carried a **high** OFFSHORE_LEAKS off a Panama Papers
intermediary whose documents end in 2015, because ICIJ's `match: true` set the
confidence. Things that will be re-derived otherwise:

- **`match: true` decides nothing** — not the score threshold, not the
  confidence. It stays on `evidence["icij_match"]`.
- **The node's country and cutoff come from the reconcile `extend` service**:
  `POST /api/v1/reconcile` with an `extend` form field (`{"ids": [...],
  "properties": [{"id": "country_codes"}, ...]}`) — not `queries`. Rows give
  `country_codes` and `valid_until` ("The Panama Papers data is current through
  2015"); `jurisdiction` / `incorporation_date` were empty on every node sampled
  (24 Sept 2026). Asked only for candidates that passed the name gates.
- **Date gate:** the party's `foundingDate` (entity) or `birthDate` (person)
  year after the leak's last year → the match is dropped. Cutoff = ICIJ's
  `valid_until`, else `_LEAK_CUTOFF_YEARS`, whose dataset-wide entries are the
  *latest* sub-collection year or the publication year, so the table can only
  keep what the precise cutoff would drop. The subject's date is the
  **earliest** across its identity set. An unknown leak is not date-gated, and
  the evidence says so.
- **Only a jurisdiction match makes it high, and only for entities**: the
  party's BODS `jurisdiction` country (`US-DE` → `US`) or address country among
  the node's `country_codes`. Persons are always `medium` (Stephen, 24 Sept
  2026). A failed extend call is **not** a `DegradedSource` — matches stay
  medium, the safe direction.
- **The gates are on the evidence**: `gates` (sentences such as "gate passed:
  incorporation 1972-11-03 ≤ leak cutoff 2015", "jurisdiction HK = HK",
  "name-only match: capped at medium"), `date_gate`, `jurisdiction_gate`.

---

## The lookup gate is counted, FIFO, and patient with the stream (Phase 238)

`opencheck/lookup_replay.py` (`_PipelineGate`, `_run_flight`; in `routers/lookup.py`
until Phase 246) + `opencheck/pipelinestats.py`;
the investigation is `claude/pipeline-gate-investigation-2026-09-24.md` in the
project. On 23 Sept 2026 two large companies opened together got the 503, and
nothing could say who held the four slots. Things that will be re-derived
otherwise:

- **Every admission, queue, refusal, wait and run time is counted** in
  `pipelinestats` and served as the `pipelines` section of `/signalstats`:
  started / queued / refused / **slot-seconds per caller kind**, current and
  peak slots and queue, queue-wait and run-time percentiles (nearest-rank over
  the last 500), and per source the time into the run at which it answered
  plus its timeouts. Read `by_caller.*.slot_seconds` to see who occupies the
  gate.
- **The caller kind comes from the request path**, set by
  `ClientScopeMiddleware` beside the client IP (`lookup_budget.current_caller_kind`,
  `pipelinestats.caller_kind_for_path`): `stream`, `api`, `batch`, `expand`,
  `export`, `narrative`, `watch`, `mcp`, `other`, and `server` for work with no
  request (the watchlist worker). A closed vocabulary — never a path, an IP or
  an LEI. A new route that starts pipelines is `other` until it is added to
  `_PATH_KINDS`.
- **The queue is first come, first served.** The old `asyncio.Condition` woke
  every waiter on every release to race for the slot. Now `release()` hands
  the slot straight to the oldest waiter; a waiter that leaves is removed, and
  one that leaves as it is handed a slot passes it on (`_abandon`). The wait is
  `asyncio.wait`, **not `wait_for`**, which can swallow a cancellation that
  lands as the slot is handed over.
- **Only `/lookup-stream` waits longer** — `OPENCHECK_LOOKUP_STREAM_QUEUE_WAIT_S`
  (300 s, never less than the general wait). A JSON client, batch row or MCP
  tool has nothing to show while waiting, so it keeps the 60 s and the 503.
  The flight's wait is decided by whoever *started* it.
- **`queued` events** (`position`, `running`, `limit`, `max_wait_s`) are pushed
  when a run joins the queue and whenever it moves up. They are
  `_TRANSIENT_EVENTS`: filtered out before the replay cache, so a replay and a
  saved report never carry them. The frontend reads them through
  `LOOKUP_EVENT_HANDLERS` (`queued: "onQueued"`) into `lookupProgress`'s
  `queued` phase — "OpenCheck is busy — waiting for a free slot, N checks ahead
  of yours…" — which ends at the run's first `source_started`.
- Option 2 (more slots) and option 4 (reserved interactive slots) were
  deliberately **not** done (Stephen, 24 Sept 2026): decide them from the
  counters.

---

## The risk engine's input contract (Phase 239)

The FATF, EU high-risk-third-country and non-EU checks match
`recordDetails.jurisdiction.code` against ISO lists, and GLEIF and a register
corroborate only a number both label with a scheme. Three defects reached
production because nothing checked those inputs: `bods_gleif` / `bods_uk_psc`
(and `scripts/extract_bods_subgraphs.py`) wrote v0.3's
`incorporatedInJurisdiction` and `risk._entity_jurisdiction` read it at the
statement's top level, so no Open Ownership bundle entity reached the checks
(Bank Saderat PLC drew no FATF black-list signal for its Iranian parents);
Wikidata fell back to the Q-ID as a country code (Rosneft `"Q159"`); and the
GLEIF mapper exported most registration numbers with `scheme: ""`.

- **The contract is `bods/validator.py::jurisdiction_input_issues`**, run on
  every statement every source mapper yields anywhere in the suite by the
  Phase 214 guard (`tests/_entity_subtype_guard.py`, which now also wraps
  iterator returns — the passthrough mappers returned `iter(list)` and were
  never checked). `code` is optional; where present it is ISO 3166-1 alpha-2
  or 3166-2. A source that cannot resolve a country gives the name alone,
  **never a stand-in**. `map_meip` is exempt from the scheme rule only
  (`PUBLISHER_VERBATIM`: the OECD's identifiers are the OECD's).
  `test_mapper_contract.py::test_every_registered_source_mapper_was_checked`
  runs last on a full run and fails if a registered source's mapper produced
  no statement anywhere in the suite.
- **Address types are scoped by record kind (Phase 274).** A person allows
  `residence | service | alternative`, an entity `registered | business |
  alternative` (`bods/validator.py::VALID_ADDRESS_TYPES_BY_RECORD`, pinned to
  the vendored schema). The same guard runs `address_type_issues` on every
  mapped statement (`map_meip` exempt). When a source does not say what an
  address is for, use `alternative`, not `registered` — the FtM mapper's
  `registered` on people reached production as a schema error.
- **One reader for an entity's jurisdiction:** `bods/jurisdiction.py` (`read`
  accepts the v0.4 key and both v0.3 placements; `upgrade` renames in place).
  `bods_data.load_bundle` upgrades the committed `data/cache/bods_data/`
  extracts on read — the files themselves still carry the v0.3 key.
- **Wikidata reads P297** for the P17 country and each P27 citizenship
  (nested `OPTIONAL` in `_FETCH_QUERY`, same row count). `_wikidata_country`:
  P297, then pycountry on the label (summaries cached before the change), then
  the name alone.
- **GLEIF RA → scheme** (`gleif_registration_scheme`): every RA code an adapter
  dispatches on maps to **the scheme that adapter's own mapper writes** (the
  "scheme follows the number" rule — `test_every_ra_code_an_adapter_dispatches_on_has_a_scheme`
  fails when an adapter claims an unmapped RA). Anything else takes **the RA
  code itself** as the scheme (Stephen, 24 Sept 2026) — never `REG-<country>`,
  which `register_hops` aliases to a country's one register. `is_ra_scheme()`
  (backend `ra_codes.py`, frontend `reconcile.ts`) lets an RA-code scheme
  bridge on jurisdiction + number exactly as the blank scheme did.
  RA000466 files the NZBN on most records → `NZ-NZBN` by value. EE is
  `EE-ARIREGISTER` and SE `SE-BLV` on both sides now (were `EE-RIK` / `SE-ON`;
  ariregister's own subject used `EE-KMKR`, which is the VAT number).
  `registeredAs` is written without spaces when it is all digits once they go
  (`normalise_registered_as`), and with single spaces otherwise (Malta's
  `C 83807`). `CA-CC` (`CA-CORP` until Phase 257) is in
  `register_hops._NO_COUNTRY_ALIAS`: the federal register cannot stand in for
  a provincial number.
- Adding RA codes to `_GLEIF_RA_TO_ORG_ID` **adds FullCheck register hops**
  (`register_hops` derives them from the table) — nineteen more schemes in
  Phase 239, each with its `REG-<country>` alias where the country has one hop.

---

## State owners and STATE_CONTROLLED (Phase 240)

`risk._state_controlled_signals` + `merge_state_controlled`,
`bods/state_bodies.py`, `sources/wikidata.py`, `bods/mappers/ftm.py`. Found by
the Opus 5.5 check (DQ-5): Equinor's three 67% owners and Rosneft's
government owners were all `registeredEntity`, so the signal never fired, and
Equinor's listed owners summed past 200%. Stephen's decisions (24 Sept 2026)
and the things that will be re-derived otherwise:

- **Three classifiers, no name matching.** Wikidata owners through `P279*` to
  ministry / government agency / government / executive branch
  (`_CLASS_ROOTS_QUERY`, cached per class; a business, company or SOE class
  wins over any government class, and "government organization" is not a
  root — SOEs sit under it). FtM `PublicBody` and the `gov.*` topics **except
  `gov.soe`, `gov.igo`, `gov.head`, `gov.religion`** — OpenSanctions tags
  ministries `gov.soe` too, but the topic means an enterprise. And any owner,
  from any source, whose `XI-LEI` GLEIF files as `RESIDENT_GOVERNMENT_ENTITY`,
  read from the Golden Copy mirror (`entity_pages.get_store()`, a local read —
  no mirror, no change): `classify_government_entities()` runs at the three
  places a source bundle is mapped and assessed (`deepen`, `_safe_deepen`, the
  FullCheck hop), copies what it changes, sets `entityType.details` and a
  `transformation` annotation recording what the source typed it, and leaves
  MEIP's verbatim statements alone (`VERBATIM_SOURCES`).
  FINANSDEPARTEMENTET (`549300L0BT3FJTN9MX24`) is the case: a `Company`
  tagged `gov.soe` in OpenSanctions, `STATE_GOVERNMENT` in GLEIF.
- **Wikidata end dates are read** (`P580`/`P582`):
  an owner whose every statement ended is emitted with `endDate` and drawn as
  ended — kept, not dropped. The ownership cache key is `ownership-v2/`.
- **Only owners above the subject count** (`_upstream_entity_ids` from
  `_subject_entity_id`): a state owning one of the subject's subsidiaries, or
  a ministry looked up itself, is not state control of the subject.
- **Grouped in the signal, never merged in the graph.** `STATE_CONTROLLED` is
  in `_STRUCTURAL_SIGNAL_CODES` with `merge_state_controlled` as its collapse
  resolver: it **pools** every source's `evidence.matches` (one per state
  node, so every node keeps its badge) and regroups them by the state's ISO
  code into `state_holdings` — one sentence per state, "Norway, 67% —
  FINANSDEPARTEMENTET (OpenSanctions); Ministry of Trade, Industry and
  Fisheries (Wikidata); formerly …". No identity between the bodies is
  claimed; a party with no country code is its own holding. "Including ended
  relationships" now means a *state* with no current holding, not a body.
- **Post-deploy:** the curated Rosneft (and possibly Ørsted) card may start
  showing `STATE_CONTROLLED` once Wikidata answers — check production and run
  the curated-narrative regen checklist before editing `EXAMPLE_LEIS`.

Tests: `tests/test_state_owners_phase240.py`, on the Equinor and Rosneft
shapes as read from Wikidata, OpenSanctions and GLEIF on 24 Sept 2026.

---

## The LEI's own registration status is not the company's (Phase 242)

`opencheck/lei_registration.py` + `lib/subjectProfile.ts`
(`leiRegistrationChip`, `leiRegistrationLine`). Found by the Opus 5.5 check
(DQ-4): American Foreign Policy Council (`549300W96W2VKSMVDF81`) has been
LAPSED since 19 Oct 2017 and only the SEO entity page said so.

- **Read once, from the anchor.** `_resolve_ctx` reads GLEIF's
  `registration` block off the record it already holds — live API, Golden
  Copy mirror and snapshot all carry it — into `ctx.lei_registration`; a
  curated Open Ownership bundle has none, so the live identifier call fills
  it. It rides on the `subject_profile` event as `lei_registration`, frozen
  with the run, so a saved report says what GLEIF recorded that day.
- **Never liveness.** `register_status` (from `entity.status`) is unchanged;
  `mapper.py`'s comment that LEI status ≠ entity status stands. Nothing here
  reaches `risk.py` or the verdict.
- **Only GLEIF's own dates** (Stephen, 24 Sept 2026). GLEIF has no "last
  validated" field. A lapse is dated by `nextRenewalDate` — the renewal that
  was missed; no other status gets a `since`. `lastUpdateDate` is "GLEIF last
  updated the record", never a validation (AFPC's reads 2026, nine years
  after the lapse).
- **ISSUED is stated, not flagged.** The `context`-tone chip beside the LEI
  (subject card, batch row) renders only when `flag` (status ≠ ISSUED); the
  identity-band row "LEI registration", the MCP `profile`, the batch row's
  `lei_registration` + two CSV columns, and the report's Identifiers table
  always state it. The MCP `summary` and every non-ISSUED sentence say it is
  the LEI record's status, not the company's.

---

## Provenance is checked behaviourally, not by grepping (Phases 229–230)

Liveness is declared in **two** places and an adapter can do one without the
other: `SourceHit.liveness` (the field on the hit) and `provenance.record_*()`
(the recorder, which is what the pipeline resolves and the UI renders, and
which defaults to `stub`). `onrc_romania` set the first and never the second,
so every ONRC card badged real Trade Register rows "Placeholder data — no live
source was contacted" (PR #275); `ariregister` was the same bug in Phase 45.
Both were found by reading live production output.

- **`opencheck/sources/provenance_audit.py` is the guard.** It runs each
  adapter's own probe read path inside a `provenance.recording()` scope with
  the network blocked and counted, and reads the **recorder** — never
  `SourceHit.liveness`, which is the field that made a static check pass for
  the wrong reason. `tests/test_provenance_audit.py` runs the whole registry in
  ~3 s on every PR, and proves it can fail by reintroducing both bugs.
- **A request that leaves an adapter while the recorder is empty is a defect**,
  whatever the upstream would have said: `build_client()` records `live` when
  the client is *constructed*, and an adapter building its own client records
  one explicitly **before** the request (see the comment at
  `ariregister.py`'s call site). Keep that order when writing an adapter.
- **`OFFLINE_COMPARED` is pinned by id**, not counted. A skip and a blocked
  request both read as "no news", so coverage can shrink with nothing going
  red; removing a source from that set is a statement and belongs in the commit
  that made it true. `MIN_REQUEST_TIME_CHECKS` does the same for the block.
- **`BULK_ARTIFACT_FETCHERS` is one table**, read by the AST guard in
  `tests/test_source_probes.py` *and* by the request-time rule. A bulk artifact
  download (ClimateTRACE's GEM CSVs) is a request with no provenance claim to
  make; two lists would mean an exemption true of one check and not the other.
- **`is_stub_bundle()` is the pipeline's rule in one place.**
  `Recorder.resolve(is_stub=True)` short-circuits to stub whatever was
  recorded, so anything grading provenance must pass the same `is_stub` the
  pipeline does — the sweep did not, and an adapter recording `live` and
  returning a stub bundle read "live" there and "Placeholder data" to the
  reader. `check_provenance` separates a stub bundle, nothing recorded at all,
  and the wrong thing recorded; one line used to collapse all three.
- **`probes.skip_reason()` is shared** by the sweep and the audit, and returns
  *which kind* — `credential` or `artifact`. Both mean untested and they are
  fixed in different places.
- **The index tier is warmed before the weekly sweep.**
  `scripts/warm_bulk_stores.py` calls `warm_index()` / `warm_meip_db()` — the
  functions production calls at boot — so the download lands where
  `requires_files` looks. The step is `continue-on-error` and `requires_files`
  stays as the fallback: a failed download skips the probe, and the report's
  "Not exercised for want of a local artifact" section names the
  `expect_liveness` that skip left unevaluated. Before this, `onrc_romania` and
  `meip` skipped every week carrying the one assertion the sweep exists for.
- Design and decisions: `docs/source-health-plan.md`.

---

## Where the regrown files went (Phase 246)

`bods/mapper.py` (11,144 lines), `App.tsx` (3,724) and `routers/lookup.py`
(3,338) had each regrown past their Phase 168 split. The seams, and what will
be re-derived otherwise:

- **Backend.** Every per-source section of `mapper.py` is its own module in
  `bods/mappers/` (one per country, or per source family: `eiti.py`,
  `climatetrace.py`, `sec_edgar.py` …), moved by top-level name with its
  helpers; `mapper.py` keeps GLEIF, the RA/US-state tables and the three
  passthroughs, and re-exports **every** name the modules define, private
  helpers included. `routers/lookup.py` lost the replay cache, gate, flights
  and fold to `opencheck/lookup_replay.py`, the `/expand*` endpoints to
  `routers/expand.py` (its own router, included in `app.py`), `/resolve-national-id`
  to `routers/national_id.py`, and the graph-shape counters to
  `opencheck/graph_shape.py`; `lookup.py` re-exports all of it, and the cache
  and in-flight dicts are the same objects under both names.
- **Patch a name where it is read.** A re-export is a second binding:
  `monkeypatch.setattr(lookup, "_QUEUE_POLL_S", …)` no longer reaches the gate,
  so tests patch `lookup_replay` (and `routers.expand` for a register hop's
  `_fetch_with_provenance` / `_mapper_for` / `assess_*`). The two deliberate
  exceptions call back through the module so the old patch points still work:
  a flight runs `lookup._lookup_pipeline`, and `/expand` runs
  `lookup._lookup_impl`. `fold_lookup_events` imports `LookupResponse` when it
  runs — `routers.lookup` imports `lookup_replay`, never the reverse.
- **Frontend.** `App()` keeps routing, the lookup mutation, mode selection and
  the page layout. The run's state and the handler builder are
  `hooks/useLookupStream.ts` (`reset()` replaces the thirty-setter resets that
  kept missing new fields); the saved copy, saving and the PDF/Markdown exports
  are `hooks/useSavedReport.ts`; the search panel's fields and GLEIF searches
  are `hooks/useSearchForm.ts`, rendered by `components/SearchPanel.tsx`. The
  header and footer are `components/SiteChrome.tsx`, the mode tablist and blurb
  `components/cdd/ModeTabs.tsx`, views and their paths `lib/views.ts`.
- **QuickCheck is lazy like the other five tabs** (`components/cdd/QuickCheckPanel.tsx`),
  and **every lazy mode panel sits inside `ui/PanelBoundary`**: a chunk that
  fails to load (a tab opened on a page served before a deploy) or a panel
  that throws now says so inside its tab instead of unmounting the whole app.
  Wrap a new lazy panel the same way, with `PanelLoading` as its fallback.

---

## PEP signals: one per upstream record, own-role PEPs are context (Phase 247)

`opencheck/pep_merge.py`, called on the cross-check + OpenAleph signals
**before** `_merge_signals` (so `/signalstats` counts the merged number) in
`_lookup_pipeline` and `_build_report`; `routers/expand.py` merges only.
Found by the Opus 5.5 check (DQ-7, DQ-8): Equinor carried 38 `RELATED_PEP`
for 13 people, and Shell's verdict asserted a PEP over nothing but "Possible
name match only" chips. Things that will be re-derived otherwise:

- **EveryPolitician returns OpenSanctions' entity id; OpenAleph's id is that
  id + `.` + 40 hex** (the collection signature). `upstream_record_id()`
  reads both through `sources.lineage.derived_from`. Grouping is
  `(upstream id, normalised related-party name)` — across person statements,
  so the two Anders Opedal nodes share one chip and
  `evidence.subject_statement_ids` (read by `signalScope.ts`) keeps both
  badges. The winner is the highest confidence, ties to the upstream source.
  **Only `RELATED_PEP`** — the sanctions family is untouched.
- **Own-role PEPs** (Stephen, 25 Sept 2026: keep, label, context): when every
  position on the OpenSanctions record is at the subject, the signal becomes
  `RELATED_PEP_SUBJECT_ROLE`, `kind: "context"` — a new code rather than a
  flag on `RELATED_PEP`, so the chip, the graph badge (slate, severity 0), the
  OG card, the narrative label and the verdict all read it correctly by
  construction. Positions come from `positionOccupancies[].post[]` via
  `REGISTRY["opensanctions"].fetch` (cached, one call per record, max 25). The
  organisation is matched by **name on either side of the comma** —
  `no_brreg` writes "Chairman, EQUINOR ASA", `dk_pep` writes "Ørsted A/S,
  board of directors (vice chairman)" — because OpenSanctions holds Equinor
  twice and the positions point at the LEI-less `Organization`. A failed read
  leaves the signal as it was (`pep_role_checked: false`), never a
  degradation.
- **The verdict's "possible" forms apply to name-match clauses only** (PEP,
  related sanctions/export/debarment, offshore leaks, counter-sanctions) —
  not to state ownership, jurisdiction or opacity (Stephen, 25 Sept 2026).
  `VERDICT_TEMPLATE` = 3; bump it again on any rewording.
- **`/person-check` uses `cross_check.match_confidence` too**: a signal's
  confidence is the lower of the rule's and the match's, and an
  uncorroborated one opens "Possible name match only".
- **Curated cards:** Ørsted's and Eesti Energia's `RELATED_PEP` medium chips
  still hold (Julia King; Jürgen Ligi) — re-check after deploy, and run the
  curated-narrative regen checklist, since Ørsted's other six board members
  become context.

Tests: `tests/test_pep_merge_phase247.py`, on the Equinor / Ørsted shapes read
from OpenSanctions and OpenAleph on 25 Sept 2026.

---

## SEC EDGAR asks for the SCHEDULE forms, not SC (Phase 252)

`sources/sec_edgar.py`. Until Phase 252 the adapter found **nothing** for
any US issuer, and production said "answered, no record" every time.

- **The December 2024 XML mandate renamed the forms**: structured filings are
  `SCHEDULE 13D` / `SCHEDULE 13G` (`/A`), legacy ones `SC 13D` / `SC 13G`.
  browse-edgar's `type=` is a **prefix** match, so `type=SC+13G` never returns
  a structured filing. `_STRUCTURED_FORM_TYPES` holds the right pair;
  `_LEGACY_FORM_TYPES` is read only when nothing structured survives, to count
  for the coverage note (`legacy_filing_count` is `None` otherwise).
- **Why it hid**: the tests mocked `SC+13G` URLs returning `SCHEDULE 13G`
  entries — a pairing EDGAR never serves — and the health probe accepted an
  empty answer with a coverage note, which is exactly what the bug produced.
  The probe is now Moody's (CIK 1059556) with `filings` required; the fixtures
  in `tests/fixtures/sec_edgar/` are Moody's real feed and filings.
- **Real filings, first read in Phase 252**: CUSIP is nested
  (`issuerCusips/issuerCusipNumber`), address fields are in the
  `http://www.sec.gov/edgar/common` namespace (13G's block is
  `issuerPrincipalExecutiveOfficeAddress`), and the event date is `dateOfEvent`
  (13D) or `eventDateRequiresFilingThisStatement` (13G), MM/DD/YYYY.
- **13G names reporting persons without a CIK.** One record per reporter is
  keyed on the reporter's CIK, else filer CIK + name (`_reporter_key`) — keyed
  on the filer alone, a joint filing (TCI Fund Management + Christopher Hohn)
  kept one reporter.
- **An exit filing is an ended relationship** (`reports_exit`: 0 shares held):
  `endDate` = the filing's event date, no `share`, or `recordStatus: closed`
  with no date when there is none. A filing below 5% but above zero is carried
  as filed — the holding continues, it has only stopped being reportable.
- **Phase 254: the CIK step failed first for many issuers.** A lookup
  reaches `fetch` only once a CIK is known — from OpenCorporates, else by
  matching the GLEIF legal name against `company_tickers.json`. Two misses:
  EDGAR's conformed names end in a state / country / series tag
  (`MOODYS CORP /DE/`, `ICU MEDICAL INC/DE`, `… /NEW`, `…/ADR` — 552 of 8,004
  titles), and an apostrophe became a space (`MOODY S`). `_edgar_title_key`
  strips the tag from **EDGAR's names only** (never GLEIF's — `A/S` must
  survive) and `_normalise_company_name` drops apostrophes. **A key two CIKs
  share resolves to neither** — the index used to give it to the first title
  in the file (`FIRST BANCORP /NC/` vs `/PR/`, `INDEPENDENT BANK CORP` MA vs
  MI), and the company-search fallback now needs exactly one match too.
  Measured on 420 titles against live GLEIF names: 100 GLEIF records newly
  resolve to the right CIK, none lost, one wrong first-wins match dropped.
  The reporter key's name now goes through the same normaliser too: JPMorgan
  filed a McDonald's 13G as "JPMORGAN CHASE & CO" and its amendment as
  "JPMORGAN CHASE & CO.", and both were kept.
- **Phase 259 (the "SEC EDGAR data quality issues" ticket):**
  - **EDGAR's country codes come from the SEC's table**, in
    `sources/edgar_codes.py` (`EDGAR_CODES`, `edgar_country`). The old map knew
    X1, the states and **X2, which it read as Canada — X2 is Burkina Faso**
    (Canada is Z4, provinces A0–B0). X0 = United Kingdom, so TCI and Hohn are
    GB now. Guam / Puerto Rico / US Virgin Islands keep their own ISO codes;
    Netherlands Antilles (P8) and Unknown (XX) carry a name alone, never a
    stand-in code. A filer that writes the SEC's name ("Delaware") is looked
    up in the same table. `tests/fixtures/sec_edgar/edgar_state_country_codes.json`
    is the published table verbatim; the test fails if a code or name drifts.
  - **Joint filings stay as filed** (Stephen, 28 Sept 2026): one relationship
    per reporting person, no control chain read out of the free-text items,
    and the interest's `details` say "Reported in one joint filing with …;
    holdings in a joint filing can be the same shares reported for each
    person, so they are not added together". `joint_with` rides on each
    filing record.
  - **`NON_EU_JURISDICTION` is one note per lookup.** It is in
    `_STRUCTURAL_SIGNAL_CODES` with `risk.merge_non_eu_jurisdiction` as its
    resolver, which POOLS every source's `evidence.jurisdictions` (so every
    badge stays) and names the sources in `evidence.reported_by`. The MCP
    rows, the PDF/Markdown signal line and the frontend's corroboration count
    (`signalEvidence.signalSourceIds`) read `reported_by`; anything counting
    `source_id` alone would under-count. "Including ended relationships" is
    now per COUNTRY: a jurisdiction entry reached only through ended links
    carries `via_ended_only: true`, and the qualifier survives only for a
    country that no current link, in any source, reaches — Apple's SEC
    EDGAR bundle reached the US through Vanguard Capital Management's
    current holding and still said "(including ended relationships)"
    because The Vanguard Group's exit filing was also US. `/signalstats`
    credits the merged note to its first source only.
  - Form 13F institutional holders are a separate, later phase.

---

## The subsidiary network's BODS, and person identifiers (Phase 255)

`opencheck/subsidiaries.py` + `bods/mapper.py::map_gleif_subsidiaries`, from a
five-network check on 28 Sept 2026 (Shell, Unilever, Quantexa, Moody's, Novo
Nordisk — the Notion ticket *Improve quality of OpenCheck subsidiaries data*).
Things that will be re-derived otherwise:

- **One relationship per parent–child pair.** A child that is both a direct
  and an ultimate child gets one `direct` statement whose details end
  `; also its ultimate consolidating parent` — `ALSO_ULTIMATE` in
  `frontend/src/lib/bodsGraph.ts`, which the graph reads to label the edge
  "Controls (direct + ultimate)"; `test_subsidiaries_phase255.py` fails if the
  two drift. It used to be two statements (221 of 541 were that duplicate).
- **Ultimate-only children get their path.** Their direct parent comes from
  the Golden Copy store first (`_store_direct_parents`, free), then from live
  `/direct-parent-relationship` — at most `_PARENT_LOOKUP_CAP` (25) calls,
  stopping at the first refusal. A parent in the network gets a `child →
  parent` direct edge (local id `{parent}:direct-child:{child}`, the id the
  parent's own network gives it) beside the indirect edge to the head, which
  the graph's rule C hides. A parent outside the network (often a lapsed LEI
  of a merged holding company — Unilever N.V., BG Group) is named in a
  `commenting` annotation on the indirect edge, never drawn.
- **Dates come from GLEIF's relationship records** (`/{kind}-child-relationships`,
  paged like the children): `statementDate` = the RR registration's
  `lastUpdateDate`, interest `startDate`/`endDate` = `RELATIONSHIP_PERIOD`.
- **Children first, enrichment after.** The children calls run alone, then the
  RR calls, then the parent lookups — a Phase 234 discretionary budget spent on
  dates must never cost the network. Enrichment that is refused leaves the
  network whole: it is cached (`complete`) but flagged `enriched: false` and
  trusted for an hour, not a week. A childless network asks for no RR records.
  The cache `shape` is 2; older entries are rebuilt.
- `/subsidiaries` gains `countries` (the jurisdictions rolled up to ISO
  3166-1). The lookup's GLEIF direct children are paged (10 × 100): Shell's
  export lost 5 of 105 to the old single page.
- **Every GLEIF entity statement** with a non-ISSUED LEI carries the Phase 242
  sentence as a `commenting` annotation ("…the status of the LEI record, not
  of the company"); the legal-form label falls back to `legalForm.other` for
  ELF `9999`; nine RA codes with org-id codes and no adapter (MY-SSM, PH-SEC,
  IT-RI, IL-ROC, PA-PRP, LK-DRC, PK-SEC, JP-JCN, MX-RFC) joined
  `_GLEIF_RA_TO_ORG_ID` — no adapter claims them, so no register hop follows.
  The adapter schemes that are not org-id codes (CA-CORP, BR-RFB, SK-RPO,
  RO-ONRC, HK-BRN, US-DE) were left alone: each is a both-sides decision.
  Phase 257 renamed the first three (see below); the other three stay, because
  no org-id code fits the number OpenCheck holds.
- **A person's non-document identifiers are annotations.** BODS keeps
  `personStatement.identifiers` for `{ISO3}-{PASSPORT|TAXID|IDCARD}`;
  `make_person_statement` moves anything else (Wikidata Q-ids, OpenSanctions /
  OpenAleph record ids, SEC CIKs, register person keys) into an `identifying`
  annotation written by `annotations.person_identifier_note` in one fixed
  sentence shape, read back by `person_identifiers_from_annotations` (the FtM
  and Senzing exports) and `personIdentifierNotes` in `lib/annotations.ts`
  (BackgroundCheck grouping, on the same `scheme:id` key). **Read person
  identifiers through those helpers, never `rd.identifiers` alone.**
  Phase 256: the sentence is `Identifier <id> (scheme <SCHEME>; <schemeName>).
  BODS keeps …` (the Phase 255 `<schemeName> identifier <id> (scheme …)` shape
  read "Wikidata Q identifier identifier Q…" and is still parsed, for saved
  reports). A personal tax number filed under an organisation-style scheme is
  rewritten and **kept** in `identifiers` — `annotations.PERSON_TAXID_SCHEMES`,
  today `RU-INN` (12 digits only; 10 is a company's) → `RUS-TAXID`.
- A listing's `operatingMarketIdentifierCode` comes from `listing.OPERATING_MIC`
  (the segment MICs in `VENUES`, from ISO 10383; every other venue MIC is its
  own operating MIC). A venue added to `VENUES` that is a segment MIC needs
  its entry there, or BODS gets the wrong operating MIC.
- **A partial `foundingDate`/`dissolutionDate` is left out**, never completed:
  `make_entity_statement` drops a year or year-month and records the source's
  words in a `transformation` annotation; `annotations.dropped_partial_date`
  reads it back (the consistency check compares at the coarser precision, as
  before). A datetime is cut to its date.
- Annotations that describe a whole statement target `/recordDetails`, never
  `/` (libcove: `annotation_statement_pointer_target_invalid`).
- `map_meip` orders the OECD's statements parties-first; ids unchanged.
- **MCP**: `opencheck_subsidiaries(lei, format, include_declared)` serves
  `/subsidiaries` (optionally the MEIP / EITI / GEM lists, apart and not BODS),
  and `opencheck_export_bods(include_subsidiaries=True)` runs the
  `/export?subsidiaries=true` merge. Default tier, like the REST route.

---

## GLEIF's ISIN file behind /securities, and where the GLEIF budget goes (Phase 258)

`opencheck/isin_index.py`, `opencheck/gleifstats.py`; design in
`docs/securities.md`. Things that will be re-derived otherwise:

- **The table is built on the server** from GLEIF's daily keyless zip
  (`mapping.gleif.org/api/v2/isin-lei/latest`) — at boot when absent, then
  when GLEIF names a newer file (every six hours; fifteen minutes after a
  failure). No release asset, nothing committed. Staged into a scratch table
  and read back `ORDER BY lei, isin`, so the input need not be sorted and
  memory stays ~50 MB; written beside the old file and `os.replace`d.
- **`counts` holds the count on its own** — never derive a count by
  decompressing `chunks` (636,388 ISINs for one LEI).
- **An LEI the table lacks is `total: 0`, not a miss** — the file is GLEIF's
  complete mapping. Only a table that is absent, wrong-schema or older than
  `OPENCHECK_ISIN_INDEX_MAX_AGE_DAYS` sends `/securities` to the Phase 253
  cached live call. `isin_list_source` says which answered.
- **ISIN order, not GLEIF's** (Stephen, 28 Sept 2026): the live API's order is
  unstable; a sorted list keeps pages and saved reports stable.
- **The suite never downloads it**: `conftest.py` sets
  `OPENCHECK_ISIN_INDEX_SYNC=0` and points the path at a file that does not
  exist, so a developer's locally built table cannot leak into tests.
- **Every GLEIF request is counted where it leaves** — the throttled
  transport — by route (`gleifstats.ROUTES`, set by `ClientScopeMiddleware`
  from the path) and by GLEIF endpoint (`ENDPOINTS`, from the URL), with
  `sent` / `http_429` / `refused_held_for_lookups` / `refused_rate_limited`.
  Served as `/signalstats` → `gleif`, with `isins_share` (the Golden Copy
  gate's number), `securities_served` and the table's `isin_index` status.
  Closed vocabularies only: an unknown route, endpoint or outcome is folded
  or dropped, never a new key. A route that calls GLEIF is `other` until it
  is added to `_PATH_ROUTES`.
- **`GleifRateLimitedError.reason`** — `held_for_lookups` when the Phase 234
  reserve refused (nothing sent), `rate_limited` in the penalty box or when
  the budget ran out; `gleif_throttle.unavailable_reason(exc, status=)` maps
  any failure to the three reasons. `/securities` and `/subsidiaries`
  (`unavailable_reason` + a `degraded_detail` naming the cause) both read it.
  Share cards say nothing about GLEIF to a reader — a refused teaser name
  renders the LEI-only card — so for them the counter is the whole record.

---

## Watchlist robustness, schema versions and backups (Phase 260)

From the Opus 5.5 check (C-M5, C-M6, C-L5). `opencheck/watchlist.py`,
`routers/watch.py`, `opencheck/sqlite_schema.py`, `opencheck/backups.py`,
`scripts/restore_backup.py`; operator's page `docs/backups.md`. Things that
will be re-derived otherwise:

- **The OpenSanctions backlog drains oldest first** (`pending[:OS_MAX_VERSIONS_PER_TICK]`),
  and the watermark moves past a version only once it was read or recorded
  as a gap. The old `[-12:]` slice skipped everything older than the newest
  twelve after an outage, silently. A failed download stops the tick where
  it was; a remaining backlog (`os_backlog`) makes the next tick read again
  without waiting for the three-hour interval.
- **A version that cannot be read is a gap, answered by a catch-up**:
  versions that aged out of `versions.json` (it holds ~100, ~25 days) before
  they were read, or a listed delta that answers 404. The gap is recorded
  (`meta.opensanctions_gaps`, `os_gaps`) and every watched LEI is queued on
  `TIER_CATCHUP` — an entry only when the re-run finds a difference, and a
  real trigger replaces a pending catch-up in `enqueue` rather than being
  folded into it. The feed, `lib/watchlist.ts` (`tierChip`,
  `triggerSentence`, `tierSentence`) and the page all word it.
- **No SQLite on the event loop.** Every `/watch` route body calls the store
  through `_db()` (`asyncio.to_thread`), and `rerun`, `tick` and `baseline`
  do the same (`_mirror_facts`, `_write_rerun`). The store waits up to 30 s
  on a write lock the mirror-refresh hook holds; on the loop that froze the
  server. `test_a_locked_store_does_not_freeze_the_event_loop` holds a real
  `BEGIN EXCLUSIVE` from another thread and measures the loop.
- **`PRAGMA user_version` on both files** via `sqlite_schema.migrate` and
  each store's `MIGRATIONS`. Version 1 = the shipped schema, so a pre-260
  file is stamped and unchanged. Append a `Migration` to change a schema;
  never edit a shipped step. A newer file is refused
  (`SchemaTooNewError` → 503 on the routes).
- **Backups go to a PRIVATE repo, encrypted** — never the public
  `StephenAbbott/opencheck` releases: saved reports are unlisted
  capabilities and a watchlist says what someone watches. `check_private()`
  runs before every upload. The file is the online-backup copy,
  integrity-checked, gzipped, sealed in 1 MiB AES-GCM chunks (nonce = prefix +
  counter + last flag, so truncation fails), decrypted and re-checked before
  upload. "Due" is read from the newest asset on the release, so deploys
  neither reset nor skip it. `/watchstats.backups` carries dates and errors,
  never the repository or token. The passphrase setting is named
  `backup_passphrase_secret` so `secret_scrub` redacts it.

---

## A statement names its source by id, and a miss is loud (Phase 267)

`opencheck/bods/source_ids.py`. Every licence decision about a statement —
RDF `bods:license`, Senzing `DATA_LICENSE` / `ATTRIBUTION`, a FullCheck
network's `contributing_source_ids` and `LICENSES.md` — needs the adapter id
behind it. Until Phase 267 it was recovered by matching `source.description`
(a display name) back against the registry, and a miss returned an empty set
with no trace. It missed on every OECD-UNSD MEIP statement (Shell's export on
30 Sept 2026 shipped one Senzing record without a licence), and any edit to a
display name in `SOURCE_NAMES` would have orphaned every statement already
stamped. Things that will be re-derived otherwise:

- **`_source_block` writes `opencheckSourceId`** beside `description`. It is a
  BODS extension field: v0.4's Source object is open, and libcovebods lists it
  among additional fields, not errors (Stephen, 30 Sept 2026 — chosen over an
  annotation per statement or a frozen alias table).
- **Read the id through `source_ids_of` / `source_id_of` /
  `contributing_source_ids`**, never `source.description`. Order: the stamped
  id when it is a REGISTRY id (an unregistered id such as `bods_gleif` has no
  licence row, so it falls through); then the description, for statements
  stamped before Phase 267 that saved reports and caches still hold; then
  `_LEGACY_DESCRIPTIONS` (the OECD's MEIP description). Nothing else.
- **A miss is loud** (Stephen, 30 Sept 2026): a warning once per distinct
  description, and the export routes count unattributed statements once per
  request (`record_unresolved(bods, "export" | "export_network")`) as
  `/signalstats.unresolved_sources` — `surface|reason`, reasons `no_source` /
  `unrecognised`, closed vocabularies only. Anything but zero is worth a look.
- `test_source_ids_phase267.py` round-trips every REGISTRY id through the
  stamp **and** through the description alone, and fails if two sources share
  a display name. Renaming a display name is now safe; adding a source needs
  nothing new here.
- `consistency.source_id_of` (lineage) reads the stamped id first too.
  **Left as they were:** `subject_identity.py`, `reconcile.py`,
  `liveness.py` and `rdf.py`'s anchor check still compare descriptions — to
  prefer the GLEIF statement or to label a row, not to decide a licence.

## A register number can be the subject (Phase 290)

`GET /lookup-register?scheme=&id=`, `/lookup-register-stream` and the MCP
tool `opencheck_register_lookup(scheme, id)` run due diligence on a company
with no LEI — most UK LLPs, most laundromat vehicles anywhere — anchored on
its number on a national register. One register read (the hop that owns the
scheme, `register_hops.hop_for`), then the same screens, risk engine, verdict,
profile and knowability the LEI pipeline runs. Things that will be re-derived
otherwise:

- **A subject reference is an LEI or `SCHEME:id`** (`GB-COH:OC346224`),
  everywhere a `lei` / `subject_lei` string names the subject:
  `subject_profile.subject_statements` / `build_subject_profile`,
  `subject_identity`, `knowability.chain_for_lei`, the three screens'
  `subject_lei=`, `shaping._subject_identifiers`. `subject_keys(ref)` is the
  one place the reference becomes identifier-merge keys: the scheme key, the
  mappers' `REG-<country>` fallback and the `JUR:<country>:` bare-number key
  — so a Companies House `GB-COH` statement and OpenAleph's `REG-GB` one
  are one subject. A `REG-` alias in the reference resolves to the country
  too. The pipeline always builds the reference from `_register_anchor`'s
  canonical scheme and `hop.normalise`d number (`register_subject`), so one
  spelling is one replay key.
- **The replay cache, the flight, the gate and the lookup budget are shared.**
  `lookup_replay._run_flight` picks `_register_lookup_pipeline` when the
  subject key has a colon; an LEI never does. `fold_lookup_events` reads
  `register_done` as it reads `gleif_done`. `RegisterLookupResponse` is
  `LookupResponse` with `lei: None` and `scheme` / `id` — every reader of a
  `LookupResponse` reads it unchanged.
- **The subject's own name is screened by name.** No LEI means no LEI-keyed
  OpenSanctions or OpenAleph record of the subject, so
  `assess_cross_source_names(..., subject_lei=ref, screen_subject=True)`
  screens the subject's names once, outside the related-party cap, with the
  subject-level code (`_SUBJECT_CODE`: `SANCTIONED`, not
  `RELATED_SANCTIONED`), anchored on `evidence.statement_id` with
  `evidence.subject: true` — the arrangement `icij_check._subject_targets`
  already had. Without `screen_subject` (the FullCheck hop) a register
  reference just excludes the subject, as an LEI does.
- **What a GLEIF anchor would add is said as absent, never faked:** no `lei`
  in `derived_identifiers`, no `listing` event, `lei_registration` null, and
  the MCP summary says "no LEI — anchored on the register, the company itself
  screened by name".
- **A register that publishes no name is said, not hidden.** KvK's open
  data carries no names (the LEI path gets one from GLEIF; the hop from the
  PSC filing), and the mapper emits nothing without one. The route and the
  tool take an optional `name` for exactly that, handed to
  `pass_legal_name` registers as the hop hands them the filing's; a nameless
  answer records a `source_read` degradation so the verdict reads incomplete
  rather than clean. The flight key carries the name after `#`
  (`_NAME_SEP`), never the subject reference. The register's own 404 is a
  404 of ours, not a 502 (follow-up, 5 Oct 2026).
- **`opencheck_search` candidates carry `identifiers: [{scheme, id}]`** —
  the LEI as `XI-LEI` plus every register number a hop exists for, mapped
  from the hit's derived keys through `RegisterHop.derived_key`; a key no
  tool can act on is left out, so every row is a next step. `mcp/guard.py`
  `TOOL_TIERS` must name every tool that runs a pipeline; a tool missing
  from it spends only the default request tier.
- **`/search` is ranked across sources, not grouped by source (Phase
  292).** `opencheck.search_rank.rank_hits` orders every hit by a match tier
  measured from the QUERY's side — `exact` (`org_comparable_name` or its
  despaced form), `same_name` (equal `org_name_residue`), `all_tokens`,
  `distinctive_tokens` (query-directed; NOT the symmetric
  `distinctive_token_agreement`, which would pass ":-) INVEST AS" against
  "Metastar Invest"), `fuzzy` — then register status read from the hit's
  `summary` segments (unrecognised ranks with live, never guessed), then
  LEI, then similarity, then original order. The web picker does not read
  `/search` (it queries GLEIF directly), so this orders the REST response
  and the MCP tool. `opencheck_search` cuts at `limit` (default 15, max 50)
  and says so with `total`/`truncated` — a silent cap is the Phase 279
  defect. A new adapter whose summary prints a status label the
  vocabularies in `search_rank` do not know ranks it with live.
