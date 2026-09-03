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
- **BODS mapping**: each adapter has a corresponding `map_<name>()` function in `bods/mapper.py`, exported from `bods/__init__.py`.

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

**Test suite**: 1733 passed, 6 skipped, 5 xfailed. Run `python3 -m pytest` from `backend/`.

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

### Cold start & per-source time budgets (Phase 47)

- The FastAPI lifespan kicks off `climatetrace.warm_caches()` in a
  background thread at startup, so Render cold starts pre-download/parse
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
  matches — OpenAleph" section in App.tsx. Informational, never identifier
  corroboration. No key / HTTP failure → `DegradedSource` records
  (issue #50) — never a silent clean screen. OS+OA duplicate signals for
  the same node are deliberately kept (dedupe keys include source_id).

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
- [ ] `bods/mapper.py` — `map_<name>()` function (+ `bods/__init__.py` export)
- [ ] `routers/lookup.py` — `_bh_<name>()` hit builder (only this)
- [ ] `tests/test_<name>.py` — adapter + mapper tests
- [ ] `.env` — API key if required (never committed)
- [ ] `README.md` + `ATTRIBUTIONS.md` — document the source
- [ ] `docs/sources.md` — add the adapter row (keep it in sync with `REGISTRY`;
      the active table = `REGISTRY` minus env-gated bulk-only adapters), and
      refresh the source counts in `README.md` (intro paragraph + adapter-table
      pointer line) and the social card `docs/social/opencheck-social-b.html`
- [ ] **Frontend homepage source count** — bump the "N sources" copy in
      `frontend/src/App.tsx`: the hero subline ("…from N sources into one
      graph…") **and** the "How it works" step-3 title ("N open sources, in
      parallel"). Easy to miss — these are hard-coded counts separate from the
      README/social-card ones.
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

## Other conventions

- API keys go in `.env` only — never committed to the repo.
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
together. `backend/opencheck/reporting/diagram.py` carries the same two values
for the exported PDF and **must move with any future change — nothing pins them
together**.

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
- `COMPLEX_CORPORATE_STRUCTURE` → red (#fef2f2 / #b91c1c)
- `FATF_GREY_LIST` → orange dark (#fff7ed / #9a3412)
- `NON_EU_JURISDICTION` → orange (#fff7ed / #c2410c)
- `OFFSHORE_LEAKS` → amber (#fef3c7 / #92400e)
- `TRUST_OR_ARRANGEMENT` → indigo (#eef2ff / #4338ca)
- `COMPLEX_OWNERSHIP_LAYERS` → sky (#f0f9ff / #0369a1)

**Signal→BODS node mapping** (evidence fields) — owned by `frontend/src/lib/signalScope.ts`, **not** by `BODSGraph.tsx`. Add a new evidence shape there and every consumer picks it up:
- `SANCTIONED`, `PEP` → `evidence.statement_id` (added in Phase 45 via `_bods_stable_id(source_id, hit_id)` in `risk.py`)
- `RELATED_SANCTIONED`, `RELATED_PEP` → `evidence.subject_statement_id`
- `TRUST_OR_ARRANGEMENT`, `NOMINEE`, AMLA composites → `evidence.matches[].statement_id`
- `NON_EU_JURISDICTION`, `FATF_BLACK_LIST`, `FATF_GREY_LIST` → `evidence.jurisdictions[].statement_id`
- `COMPLEX_OWNERSHIP_LAYERS` → `evidence.longest_path[]` (array of statementIds)

**Signal scoping across render sites (Phase 109)** — `RELATED_*` signals are assessed against the **merged** bundle late in `_lookup_pipeline` and ride on the top-level `risk_signals` event; a `/deepen` response carries only that source's own findings. So the three `BodsGraphExplorer` render sites see different lists, and the two per-bundle ones saw no cross-source signals at all: a node the risk panel called sanctions-linked rendered unbadged, i.e. as "checked and clean".

`lib/signalScope.ts` closes that gap. `scopeCrossSourceSignals(signals, statements)` keeps a signal only when its code starts with `RELATED_` **and** `signalStatementIds()` intersects the bundle's `statementId`s; `buildSignalMap()` (moved here from `BODSGraph.tsx`) is built on the same mapping, so the filter and the badge renderer cannot drift — that drift was the bug. Wiring: `App.tsx` → `SourceBucketCard subjectSignals` → `HitRow` → `DeepenBlock` (merged with `detail.risk_signals` via `mergeSignals`, plus a caption naming the count); `FullCheckPanel` → `SubsidiaryNetwork signals`. `EsgPanel`'s `DeepenBlock` defaults to `[]` — deliberate, it has no subject-level list.

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

## The four check modes (Phase 122/123)

`quick | full | background | esg`, owned by `frontend/src/lib/checkMode.ts`
(pure, so it is testable — the frontend suite is logic-only). The mode is
the report's top-level structure: a tablist under the subject and verdict,
which stay put across a switch.

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

Eight adapters have templates (`gleif`, `bods_gleif`, `opensanctions`,
`companies_house`, `opencorporates`, `openaleph`, `ted_eu`, `wikidata`). The
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

## Frontend curated examples (App.tsx)

`EXAMPLE_LEIS` in `frontend/src/App.tsx` contains pre-computed `signals` arrays shown on the picker cards before the user clicks. These must be kept in sync with what the risk engine actually produces for each entity. When the risk engine changes (new signals, retired signals, confidence changes), update `EXAMPLE_LEIS` to match.

Current signal inventory used in picker cards: `TRUST_OR_ARRANGEMENT`, `COMPLEX_OWNERSHIP_LAYERS`, `COMPLEX_CORPORATE_STRUCTURE`, `SANCTIONED`, `RELATED_SANCTIONED`, `NON_EU_JURISDICTION`. Confidence `"high"` renders as `●`, `"medium"` as `◐`.

---

## Test suite

- **1733 passed, 6 skipped, 5 xfailed** as of Phase 45. Run `python3 -m pytest` from `backend/`.
- Async adapter tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`).
- HTTP mocking: use `respx` for httpx-based adapters; use `unittest.mock.AsyncMock` with `patch("...build_client", ...)` for adapters that call `build_client()` directly.
- GraphQL adapters (CVR): mock by inspecting the request body (`request.content`) to route different query strings to different fixture responses.
- Always check `tests/test_sources.py` (expected registry set) and `tests/test_app.py` (expected `/sources` endpoint set) when adding a new adapter — both require explicit entries.
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
| France | inpi | `RA000189` — Register of Companies (Sirene, INSEE) | 2026-08-28 — table previously said RA000580 (wrong). Note `RA000192` is Infogreffe/RCS, a different register |
| Sweden | bolagsverket | `RA000544` — Companies Register (Bolagsverket) | 2026-08-28 (also verified 2026-06-12; RA000523 in earlier notes was wrong) |
| Estonia | ariregister | `RA000181` — Commercial Register | 2026-08-28 (ignore any reference to RA000198) |
| Belgium | bce_belgium | `RA000025` — Crossroad Bank of Enterprises | 2026-08-28 — table previously said RA000143 (wrong) |
| Austria | firmenbuch | `RA000017` — Commercial Register (BM für Justiz) | 2026-08-28 — table previously said RA000128 (wrong) |
| Poland | krs_poland | `RA000484` — National Court Register (KRS) | 2026-08-28 — table previously said RA000439 (wrong) |
| Slovakia | rpo_slovakia / rpvs_slovakia | `RA000526` — Business Register (Ministerstvo spravodlivosti) | 2026-08-28 — table previously said RA000476 (wrong) |
| Singapore | acra_singapore | `RA000523` — Business Registry (ACRA) | 2026-08-28 — table previously said RA000509 (wrong) |
| Canada | corporations_canada | `RA000072` — Corporate Registry (federal; provinces are RA000073–RA000085) | 2026-08-28 |
| Denmark | cvr_denmark | `RA000170` — Central Business Register (Erhvervsstyrelsen) | 2026-08-28 |
| Croatia | sudreg_croatia | `RA000156` — Croatian Court Registry (Sudski registar) | 2026-08-28 |
| Czechia | ares | `RA000163` — Commercial Register (Ministerstvo spravedlnosti) | 2026-08-28 — ⚠️ ambiguous; the adapter is named for **ARES**, which is `RA000168` (Register of Economic Entities, Ministerstvo financí) |
| Cyprus | cyprus_drcor | `RA000161` — Companies Section (DRCOR) | 2026-08-28 |
| Finland | prh | `RA000188` — Business Information System (PRH) | 2026-08-28 |
| Malta | malta_mbr | `RA000443` — Registry of Companies (MBR) | 2026-08-28 |
| Switzerland | zefix | `RA000548` in the adapter | 2026-08-28 — ⚠️ **mismatch**: `RA000548` is the *UID-Register* (Bundesamt für Statistik, covers CH **and** LI). Zefix, the commercial register, is `RA000549` |
| Australia | abr_australia | `RA000014` — Register of Companies (ASIC) · `RA000013` — Australian Business Register (ATO) | 2026-08-28 |
| New Zealand | nz_companies | `RA000466` — Companies Register (Companies Office) | 2026-08-28 (near-miss neighbour: `RA000749` NZ Business Number Register) |
| Brazil | cnpj_brazil | `RA000681` — National Registry for Legal Entity (Receita Federal / CNPJ) | 2026-08-28 (state Juntas Comerciais are RA000036–RA000062) |
| India | mca_india | `RA000394` — Companies Register (MCA21) | 2026-08-28 |
| Nigeria | cac_nigeria | `RA000469` — Company Registry (Corporate Affairs Commission) | 2026-08-28 (also verified 2026-08-12; Africa's first public BO register). Offline curated example set of 10 LEI-anchored companies (`data/cac_nigeria_psc.json`); a live adapter is deferred pending CAC / Oasis Management engagement. LEI-keyed dispatch (not an RA deriver); asserts only the CAC-published RC number (`ng_cac_rc`), not the derived LEI. |
| Greece | gemi_greece | `RA000685` — General Commercial Registry (G.E.MI.), businessregistry.gr | 2026-08-28 — 20 of 25 sampled Greek LEI records use it |

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
  pins that `App.tsx` calls `raCodeFor()` rather than reading `entry.raCode`.
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
- `_source_block(source_id, url)` — builds the `source` field. Every source_id must be in the `source_names` dict in mapper.py (6 were missing, fixed in Phase 43).
- `_official_registers` set in mapper.py — source IDs that get `"type": ["officialRegister"]` instead of `"thirdParty"]`.
- Relationship statements: `statementId != recordId` (unlike entity/person where they're equal).
- Risk signal `statement_id` in evidence: `_bods_stable_id(source_id, hit_id)` — added to SANCTIONED/PEP evidence in `risk.py` in Phase 45 so frontend can look up which node to overlay.

---

## Key files quick reference

| File | Purpose |
|---|---|
| `backend/opencheck/routers/lookup.py` | Main lookup endpoint + SSE stream — both must have identical derived-identifier blocks |
| `backend/opencheck/bods/mapper.py` | All BODS v0.4 mapping functions; ~6800 lines |
| `backend/opencheck/risk.py` | Risk signal rules (PEP, SANCTIONED, AMLA, FATF, etc.) |
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
