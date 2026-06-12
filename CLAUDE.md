# OpenCheck — development notes for Claude

## Architecture overview

- **Backend**: FastAPI, split into `backend/opencheck/routers/` (health, search, lookup, export).
- **Frontend**: React + Tailwind, split into `frontend/src/components/` (icons, risk, export, cdd).
- **Sources**: each adapter lives in `backend/opencheck/sources/<name>.py`, registered in `sources/__init__.py`.
- **BODS mapping**: each adapter has a corresponding `map_<name>()` function in `bods/mapper.py`, exported from `bods/__init__.py`.

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
| `frontend/src/lib/raCodes.ts` | RA codes, labels, placeholders, format regexes for 17 countries. Export: `RA_CODES`, `COUNTRY_OPTIONS`, `validateNationalId()` |
| `frontend/src/lib/gleifNationalId.ts` | `searchByNationalId(raCode, id)` — fires three GLEIF filter endpoints in parallel (`registeredAs`, `validatedAs`, `otherValidationAuthorities.validatedAs`), deduplicates by LEI |

How it works:
1. User selects country → country picker resolves to an RA code (e.g. GB → RA000585)
2. User enters registration number → `searchByNationalId()` queries all three GLEIF filter fields scoped to that RA code
3. Single result → auto-navigates to `/lookup-stream`; multiple results → picker; zero results → amber notice with "try by name" fallback

Format validation is advisory (non-blocking). The amber border + warning fires only after `onBlur` (`nationalIdTouched` state) so it doesn't interrupt typing. GLEIF may store IDs in a normalised form that differs from the raw input — always allow submission.

**Pure frontend change — no backend routes added or modified.**

---

## Current state (Phase 45)

**Test suite**: 1733 passed, 6 skipped, 5 xfailed. Run `python -m pytest` from `backend/`.

**Frontend graph renderer**: Cytoscape.js (replaced `@openownership/bods-dagre` in Phase 44). Component: `frontend/src/components/BODSGraph.tsx`. Uses a React HTML overlay layer for BOVS icons and flags — never use Cytoscape's `background-image` for icons (canvas taint from Adobe Illustrator `xmlns:xlink` SVGs). BOVS icons are base64 data URIs in `frontend/src/lib/bovsIcons.ts`. Flags are served from `frontend/public/bods-dagre-images/flags/`. The overlay recomputes on `cy.on('viewport')`. Flag badges are at 45° NE circumference; risk signal badges at 315° NW.

**Risk signal overlays**: BOVS Option C implemented. `buildSignalMap()` in BODSGraph.tsx reads `evidence.statement_id` (SANCTIONED/PEP), `evidence.subject_statement_id` (RELATED_*), `evidence.matches[].statement_id` (TRUST/AMLA), `evidence.jurisdictions[].statement_id` (FATF/NON_EU), `evidence.longest_path[]` (COMPLEX_OWNERSHIP_LAYERS). Single signal → labelled pill at 315° NW; multiple → "N ⚠" stack badge.

**Estonian adapter**: `ariregister.py` is now a public web scraper — `GET ariregister.rik.ee/eng/company/{reg}/company_print_json`. No credentials. The previous SOAP/X-Road approach (Phase 37) used `ariregxmlv6.rik.ee` with `ARIREGISTER_USERNAME`/`ARIREGISTER_PASSWORD` credentials from a paid RIK contract that turned out not to grant data access. Do NOT revert to SOAP. The HTML parser extracts officers (→ Estonian role codes), shareholders (person vs entity from ID code length), and BOs. `map_ariregister()` in mapper.py is unchanged.

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

## Other conventions

- API keys go in `.env` only — never committed to the repo.
- Schema files use `extra="allow"` via `_Base` so unknown API fields don't break validation.
- `validate_raw()` is called at the end of `fetch()` on the fully-assembled bundle, before returning.
- BODS interest type for **directors/managing officials** is `seniorManagingOfficial`, not `appointmentOfBoard`.
- `appointmentOfBoard` is for right-to-appoint-and-remove style ownership interests.

---

## Deployment

- Backend is deployed on **Render** (https://opencheck-api.onrender.com). Environment variables (API keys, etc.) must be set in the Render dashboard as well as in `.env` for local development.
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

**Edge categories**: `ownership` (blue #1565c0), `control` (orange #e65100), `role` (purple #6a1b9a, dashed), `unknown` (grey #888).

---

## Frontend: Risk signal system

**`frontend/src/components/risk/RiskChip.tsx`**: `RISK_PRESENTATION` maps signal codes to `{label, classes}`. `CONFIDENCE_DOT`: `high`=`●`, `medium`=`◐`, `low`=`○`.

**Signal codes and colours** (bg / text):
- `SANCTIONED`, `RELATED_SANCTIONED` → rose (#ffe4e6 / #be123c)
- `FATF_BLACK_LIST` → red (#fee2e2 / #991b1b)
- `PEP`, `RELATED_PEP` → violet (#f5f3ff / #6d28d9)
- `COMPLEX_CORPORATE_STRUCTURE` → red (#fef2f2 / #b91c1c)
- `FATF_GREY_LIST` → orange dark (#fff7ed / #9a3412)
- `NON_EU_JURISDICTION` → orange (#fff7ed / #c2410c)
- `OFFSHORE_LEAKS` → amber (#fef3c7 / #92400e)
- `TRUST_OR_ARRANGEMENT` → indigo (#eef2ff / #4338ca)
- `COMPLEX_OWNERSHIP_LAYERS` → sky (#f0f9ff / #0369a1)

**Signal→BODS node mapping** (evidence fields):
- `SANCTIONED`, `PEP` → `evidence.statement_id` (added in Phase 45 via `_bods_stable_id(source_id, hit_id)` in `risk.py`)
- `RELATED_SANCTIONED`, `RELATED_PEP` → `evidence.subject_statement_id`
- `TRUST_OR_ARRANGEMENT`, `NOMINEE`, AMLA composites → `evidence.matches[].statement_id`
- `NON_EU_JURISDICTION`, `FATF_BLACK_LIST`, `FATF_GREY_LIST` → `evidence.jurisdictions[].statement_id`
- `COMPLEX_OWNERSHIP_LAYERS` → `evidence.longest_path[]` (array of statementIds)

**`SourceBucketCard`** passes `detail.risk_signals` to `<BODSGraph signals={...} />`.

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

**Do NOT use the SOAP/X-Road API** at `ariregxmlv6.rik.ee`. The Phase 37 SOAP approach had a paid RIK contract that authenticated correctly (HTTP 200) but returned zero results for all queries. RIK confirmed the contract type did not grant data-query access.

**Current approach (Phase 45)**: Public web scraper. No credentials needed.
- **Main endpoint**: `GET https://ariregister.rik.ee/eng/company/{reg_code}/company_print_json`
- **Search endpoint**: `GET https://ariregister.rik.ee/eng/api/autocomplete?q={query}` → JSON
- **GLEIF RA code**: `RA000181` (NOT RA000198 — the table below has a typo, RA000181 is confirmed from live GLEIF data)
- **HTML structure**: Bootstrap label/value rows (`col-md-4 text-muted` / `col font-weight-bold`). Tables identified by header keywords.
- **Officer role mapping**: English labels → Estonian codes (e.g. "Management board member" → `JUHL`, "Procurist" → `PROK`, "Liquidator" → `LIKV`)
- **Person type detection**: 11-digit code starting with 3-6 = natural person (F); 8-digit = legal entity (J)
- **BO control mapping**: "Indirect ownership" → `K`, "Direct ownership" → `O`, "Voting rights" → `H`
- **Not found detection**: If `str(r.url)` does not contain `/eng/company/`, the server redirected away (company not found) → return stub bundle
- **Bundle format**: Unchanged from Phase 37 — `map_ariregister()` in `bods/mapper.py` needs no changes
- `ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD` in config.py are retained for backward compatibility but NOT read by the adapter

---

## Frontend curated examples (App.tsx)

`EXAMPLE_LEIS` in `frontend/src/App.tsx` contains pre-computed `signals` arrays shown on the picker cards before the user clicks. These must be kept in sync with what the risk engine actually produces for each entity. When the risk engine changes (new signals, retired signals, confidence changes), update `EXAMPLE_LEIS` to match.

Current signal inventory used in picker cards: `TRUST_OR_ARRANGEMENT`, `COMPLEX_OWNERSHIP_LAYERS`, `COMPLEX_CORPORATE_STRUCTURE`, `SANCTIONED`, `RELATED_SANCTIONED`, `NON_EU_JURISDICTION`. Confidence `"high"` renders as `●`, `"medium"` as `◐`.

---

## Test suite

- **1733 passed, 6 skipped, 5 xfailed** as of Phase 45. Run `python -m pytest` from `backend/`.
- Async adapter tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`).
- HTTP mocking: use `respx` for httpx-based adapters; use `unittest.mock.AsyncMock` with `patch("...build_client", ...)` for adapters that call `build_client()` directly.
- GraphQL adapters (CVR): mock by inspecting the request body (`request.content`) to route different query strings to different fixture responses.
- Always check `tests/test_sources.py` (expected registry set) and `tests/test_app.py` (expected `/sources` endpoint set) when adding a new adapter — both require explicit entries.

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

| Country | Adapter | RA code |
|---|---|---|
| UK | companies_house | RA000585 |
| Netherlands | kvk | RA000463 |
| Norway | brreg | RA000472 (verified live 2026-06-12 — RA000394 in earlier notes was wrong) |
| Ireland | cro | RA000215 |
| Latvia | ur_latvia | RA000327 |
| Lithuania | jar_lithuania | RA000330 |
| France | inpi | RA000580 |
| Sweden | bolagsverket | RA000544 (verified live 2026-06-12 — RA000523 in earlier notes was wrong) |
| Estonia | ariregister | **RA000181** (confirmed live; ignore any reference to RA000198) |
| Belgium | bce_belgium | RA000143 |
| Austria | firmenbuch | RA000128 |
| Poland | krs_poland | RA000439 |
| Slovakia | rpo_slovakia | RA000476 |
| Singapore | acra_singapore | RA000509 |
| Canada | corporations_canada | RA000072 |
| Denmark | cvr_denmark | RA000170 |
| Croatia | sudreg_croatia | RA000156 |

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
