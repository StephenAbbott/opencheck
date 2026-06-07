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

## Current state (Phase 45)

**Test suite**: 1733 passed, 6 skipped, 5 xfailed. Run `python -m pytest` from `backend/`.

**Frontend graph renderer**: Cytoscape.js (replaced `@openownership/bods-dagre` in Phase 44). Component: `frontend/src/components/BODSGraph.tsx`. Uses a React HTML overlay layer for BOVS icons and flags — never use Cytoscape's `background-image` for icons (canvas taint from Adobe Illustrator `xmlns:xlink` SVGs). BOVS icons are base64 data URIs in `frontend/src/lib/bovsIcons.ts`. Flags are served from `frontend/public/bods-dagre-images/flags/`. The overlay recomputes on `cy.on('viewport')`. Flag badges are at 45° NE circumference; risk signal badges at 315° NW.

**Risk signal overlays**: BOVS Option C implemented. `buildSignalMap()` in BODSGraph.tsx reads `evidence.statement_id` (SANCTIONED/PEP), `evidence.subject_statement_id` (RELATED_*), `evidence.matches[].statement_id` (TRUST/AMLA), `evidence.jurisdictions[].statement_id` (FATF/NON_EU), `evidence.longest_path[]` (COMPLEX_OWNERSHIP_LAYERS). Single signal → labelled pill at 315° NW; multiple → "N ⚠" stack badge.

**Estonian adapter**: `ariregister.py` is now a public web scraper — `GET ariregister.rik.ee/eng/company/{reg}/company_print_json`. No credentials. The previous SOAP/X-Road approach (Phase 37) used `ariregxmlv6.rik.ee` with `ARIREGISTER_USERNAME`/`ARIREGISTER_PASSWORD` credentials from a paid RIK contract that turned out not to grant data access. Do NOT revert to SOAP. The HTML parser extracts officers (→ Estonian role codes), shareholders (person vs entity from ID code length), and BOs. `map_ariregister()` in mapper.py is unchanged.

**GLEIF RA code for Estonia**: `RA000181` (confirmed from live GLEIF data — the CLAUDE.md table below has a typo: RA000198 is wrong, RA000181 is correct).

---

## Critical rule: new source adapters require changes in TWO places in `routers/lookup.py`

`routers/lookup.py` contains **two independent derived-identifier blocks** — one for the synchronous `/lookup` endpoint and one for the SSE `/lookup-stream` endpoint. They must be kept in sync.

When wiring a new adapter:

1. **Sync `/lookup` derived block** (search for `# Build derived identifiers` in the first half of the file) — add the RA code check here.
2. **SSE `/lookup-stream` derived block** (search for `# Build derived identifiers (same logic as /lookup)`) — add the **identical** RA code check here.

Forgetting step 2 means the frontend (which uses `/lookup-stream`) will never dispatch to the adapter, even though the `/lookup` API endpoint works correctly. This is exactly what happened with Corporations Canada (fixed in commit `603c086`).

Similarly, the result-handling `elif source_id == "<name>":` block in the SSE event loop, and the `applicable_ids.append` / `_add_task` calls, must all be present in the stream path.

### Checklist for a new adapter

- [ ] `sources/<name>.py` — adapter class
- [ ] `sources/schemas/<name>.py` — Pydantic bundle schema
- [ ] `sources/__init__.py` — import + REGISTRY entry
- [ ] `bods/mapper.py` — `map_<name>()` function
- [ ] `bods/__init__.py` — import + `__all__` entry
- [ ] `routers/lookup.py` sync path — RA code → derived dict, dispatch (`_w1.append`), result handler
- [ ] `routers/lookup.py` SSE path — **same three things** as sync path
- [ ] `tests/test_<name>.py` — adapter + mapper tests
- [ ] `tests/test_sources.py` — add to expected registry set
- [ ] `tests/test_app.py` — add to expected sources endpoint set
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

## GLEIF RA codes for active adapters

| Country | Adapter | RA code |
|---|---|---|
| UK | companies_house | RA000585 |
| Netherlands | kvk | RA000463 |
| Norway | brreg | RA000394 |
| Ireland | cro | RA000215 |
| Latvia | ur_latvia | RA000327 |
| Lithuania | jar_lithuania | RA000330 |
| France | inpi | RA000580 |
| Sweden | bolagsverket | RA000523 |
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
