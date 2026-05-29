# OpenCheck — development notes for Claude

## Architecture overview

- **Backend**: FastAPI, split into `backend/opencheck/routers/` (health, search, lookup, export).
- **Frontend**: React + Tailwind, split into `frontend/src/components/` (icons, risk, export, cdd).
- **Sources**: each adapter lives in `backend/opencheck/sources/<name>.py`, registered in `sources/__init__.py`.
- **BODS mapping**: each adapter has a corresponding `map_<name>()` function in `bods/mapper.py`, exported from `bods/__init__.py`.

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

## Frontend curated examples (App.tsx)

`EXAMPLE_LEIS` in `frontend/src/App.tsx` contains pre-computed `signals` arrays shown on the picker cards before the user clicks. These must be kept in sync with what the risk engine actually produces for each entity. When the risk engine changes (new signals, retired signals, confidence changes), update `EXAMPLE_LEIS` to match.

Current signal inventory used in picker cards: `TRUST_OR_ARRANGEMENT`, `COMPLEX_OWNERSHIP_LAYERS`, `COMPLEX_CORPORATE_STRUCTURE`, `SANCTIONED`, `RELATED_SANCTIONED`, `NON_EU_JURISDICTION`. Confidence `"high"` renders as `●`, `"medium"` as `◐`.

---

## Test suite

- **1738 passed, 6 skipped, 5 xfailed** as of BODS compliance audit Phases 1–8. Run `python -m pytest` from `backend/`.
- **Phase 44**: Migrated BODS graph renderer from `@openownership/bods-dagre` to Cytoscape.js. BODSGraph.tsx now uses a React HTML overlay layer for pixel-perfect BOVS icon and jurisdiction flag rendering — icons centred at 60% of node diameter, flags as BOVS Metadata Overlays at 45° (NE) circumference point.
- **Ariregister rewrite**: Estonian adapter rewritten from SOAP/X-Road to public web scraper (`/eng/company/{reg}/company_print_json`). No credentials required. RIK confirmed the public portal is freely accessible.
- **Risk signal overlays**: BOVS Option C — coloured pill badges at 315° (NW) circumference for SANCTIONED/PEP/FATF signals; stack badge "N ⚠" for multiple signals; colours match existing RiskChip palette.
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
| Estonia | ariregister | RA000198 |
| Belgium | bce_belgium | RA000143 |
| Austria | firmenbuch | RA000128 |
| Poland | krs_poland | RA000439 |
| Slovakia | rpo_slovakia | RA000476 |
| Singapore | acra_singapore | RA000509 |
| Canada | corporations_canada | RA000072 |
| Denmark | cvr_denmark | RA000170 |
