# OpenCheck

Customer due diligence risk checks driven by the Legal Entity Identifier (LEI), open data and open standards.

Try the demo version at https://opencheck.onrender.com/

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries GLEIF first, derives every cross-source identifier it can (UK Companies House number, OpenCorporates ID, Wikidata Q-ID, etc.), and uses those bridges to fan out across UK Companies House, OpenCorporates, OpenSanctions, OpenAleph, EveryPolitician, Wikidata, and OpenTender. 

Everything maps into [version 0.4 of the Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/), the cross-source links + risk signals are computed deterministically, and the whole bundle is one click away from a downloadable shareable export.

The risk-signal layer mirrors the [draft customer due diligence regulatory technical standards from the EU's Anti-Money Laundering Authority (AMLA)](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) draft conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

OpenCheck has shipped through seventeen phases (latest commit on `main` is the source of truth):

| Phase | Headline |
|------:|----------|
| 0 | Scaffold — FastAPI + React/Vite + 6 stub source adapters |
| 1 | Live UK Companies House + BODS v0.4 mapper + SSE streaming |
| 2 | Live GLEIF + OpenSanctions + OpenAleph + FtM/GLEIF mappers |
| 3 | Live Wikidata + EveryPolitician + reconciler + risk signals (incl. AMLA CDD RTS) |
| 4 | Cache-first dispatch + bods-dagre visualisation |
| 5 | Export endpoint (JSON / JSONL / ZIP) + OpenTender (DIGIWHIST) procurement source |
| 6 | LEI-anchored `/lookup` flow + BO design system + bods-dagre fix |
| 7 | BO design system applied to the frontend (Bitter / DM Sans / DM Mono, navy banner, card grid) |
| 8 | Acronyms spelled out, OpenAleph disabled, sources moved to a separate page, README refresh |
| 9 | Tooling fixes — `@vitejs/plugin-react` v5 / vite 8 alignment, README phase recap |
| 10 | Open Ownership processed BODS bundles for UK PSC + GLEIF as the canonical source |
| 11 | Cross-check related-party names against OpenSanctions + EveryPolitician — `RELATED_PEP` / `RELATED_SANCTIONED` |
| 12 | OO bundle as LEI lookup entry point + example LEI picker |
| 13 | `.env` loading from project root + BODS graph statement sanitiser + title/homepage link |
| 14 | bods-dagre `Invalid argument expected string` fix |
| 15 | Extraction script walks by `recordId` (not `statementId`) for correct subgraph extraction |
| 16 | OpenCorporates adapter (OCID-bridged via GLEIF) + BODS dagre relationship-edge fix + GODIN ribbon + Render deployment |
| 17 | FATF black/grey-list jurisdiction signals (`FATF_BLACK_LIST` / `FATF_GREY_LIST`) derived from BODS entity statements |

Test suite: 201 backend tests across the sixteen phases. Frontend type-checks clean.

## Quick start

The backend ships with cache-first dispatch: in stub mode (no API keys, no `OPENCHECK_ALLOW_LIVE`) every adapter returns deterministic placeholder data. Live mode is opt-in per source via env vars.

### Docker

```bash
cp .env.example .env
docker compose up --build
```

- Frontend: <http://localhost:5173>
- Backend: <http://localhost:8000> (OpenAPI docs at `/docs`)

### Local (without Docker)

Backend:

```bash
cd backend
uv sync
uv run uvicorn opencheck.app:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

The first frontend build copies bundled images for `@openownership/bods-dagre` into `public/bods-dagre-images/` (the BODS graph viewer needs them). If they're missing, run `npm run build` once.

## How it works

Paste a 20-character ISO 17442 LEI — for example `213800LH1BZH3DI6G760` (BP) or `253400JT3MQWNDKMJE44` (Rosneft) — and the backend:

1. Validates the LEI shape.
2. **Subject metadata.** If a pre-extracted Open Ownership bundle exists at `data/cache/bods_data/gleif/<LEI>.jsonl`, the legal name + jurisdiction are read directly from it (no live GLEIF call needed). Otherwise GLEIF is queried live.
3. Looks up the **Wikidata Q-ID** via SPARQL on property `P1278`.
4. Dispatches to every other adapter using whichever identifier they understand:
   - UK Companies House — direct fetch by company number when jurisdiction = GB. The Open Ownership processed UK PSC bundle (`data/cache/bods_data/uk/<GB-COH>.jsonl`) is the canonical answer when present; otherwise falls back to the live API.
   - OpenCorporates — fetched by `ocid` (e.g. `gb/00102498`), a field GLEIF returns on Level 1 records; delivers company profile + current officers as BODS statements.
   - OpenSanctions / OpenTender — search by the LEI string.
   - Wikidata — direct SPARQL fetch on the resolved Q-ID.
5. Maps each source's payload into BODS v0.4 statements, runs the cross-source reconciler, runs the risk-signal service, and **cross-checks every related person and entity in the BODS bundle against OpenSanctions + EveryPolitician by name** — fuzzy-matched with optional birth-year compatibility — to surface scoped `RELATED_PEP` / `RELATED_SANCTIONED` signals on the matching node.
6. Returns one unified report.

The frontend renders that report as a single subject card at the top (legal name, jurisdiction, derived identifiers as chips), an aggregated risk-chip strip, a cross-source links panel, an export button with format selector, and per-source "bucket" cards with a `Go deeper` drill-down per hit. A separate **About the sources** page (linked from the header) shows the source inventory.

### Open Ownership BODS bundles

Our live GLEIF / Companies House mappers produce a thin slice of BODS — the live APIs don't expose multi-layer ownership chains in a single response. Open Ownership publish the *processed* UK PSC and GLEIF datasets at [`bods-data.openownership.org`](https://bods-data.openownership.org/) with proper interconnected `subject` ↔ `interestedParty` relationships. We pre-extract per-subject subgraphs from the local SQLite dumps and ship them as JSON-Lines under `data/cache/bods_data/`. When a bundle exists for an LEI / company number, it overrides the live mapper output entirely.

The extraction tool ships in `backend/scripts/extract_bods_subgraphs.py`. Download the SQLite dumps once from the bods-data pages, then run for example:

```bash
cd backend
python scripts/extract_bods_subgraphs.py \
  --gleif /path/to/gleif_version_0_4.db \
  --uk /path/to/uk_version_0_4.db \
  --leis 213800LH1BZH3DI6G760 253400JT3MQWNDKMJE44 \
  --max-hops 3
```

`--max-hops` controls how many ownership layers to walk out from each LEI. The `COMPLEX_OWNERSHIP_LAYERS` AMLA rule needs ≥3, so 3 is the practical floor; 5 captures deeper offshore structures at the cost of bigger bundle files.

## Sources

Seven active adapters, each implementing the same `SourceAdapter` protocol (`search`, `fetch`, `info`):

| ID | Name | License | Description |
|----|------|---------|-------------|
| `companies_house` | UK Companies House | OGL-3.0 | Legal and beneficial ownership information from the UK corporate registry |
| `gleif` | GLEIF | CC0-1.0 | Legal entity information from the Global Legal Entity Identifier Foundation |
| `opencorporates` | OpenCorporates | OC Terms | Global company database, reached via the `ocid` field on GLEIF Level 1 records — adds company profile and current officers as BODS statements |
| `opensanctions` | OpenSanctions | CC BY-NC 4.0 | The open-source database of sanctions, watchlists, and politically exposed persons |
| `everypolitician` | EveryPolitician | CC BY-NC 4.0 | Global database of political office-holders (served via OpenSanctions PEPs dataset) |
| `wikidata` | Wikidata | CC0-1.0 | A free and open knowledge base that can be read and edited by both humans and machines |
| `opentender` | OpenTender (DIGIWHIST) | CC BY-NC-SA 4.0 | Search and analyse tender data from 35 jurisdictions |

The OpenAleph adapter is implemented but currently disabled in `REGISTRY` — its API is name-keyed rather than identifier-keyed, which doesn't fit the LEI flow cleanly yet. Re-enable in `backend/opencheck/sources/__init__.py` once we have a curated demo set for it.

NC-licensed sources propagate their share-alike / non-commercial obligations through `/deepen` and `/export`. The exported `LICENSES.md` warns reviewers before they re-publish.

## Risk signals

Twelve codes, all deterministic — every fire is documented with a `summary`, `confidence` (`high` / `medium` / `low`), and an `evidence` payload citing the underlying topic / collection / BODS statement IDs that triggered it.

Risk signals fall into three groups:

1. **Source-derived** — read straight off a single source's payload at search time.
2. **AMLA CDD RTS** — derived from the assembled BODS v0.4 bundle, mirroring the objective conditions in [the EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) for "complex corporate structures".
3. **Cross-source name match** — for every related person and entity inside the BODS bundle, search OpenSanctions and EveryPolitician by name (with optional birth-year compatibility) and surface a scoped signal on the matching node.

### Source-derived

- `PEP` — OpenSanctions `role.pep`-family topic, every EveryPolitician hit, or a Wikidata person with a currently-held position (P39 with no P582 end qualifier).
- `SANCTIONED` — OpenSanctions topic starting with `sanction`.
- `OFFSHORE_LEAKS` — OpenAleph hit in an ICIJ-family collection (Panama / Paradise / Pandora / Bahamas / Offshore Leaks). _OpenAleph is currently disabled in `REGISTRY` so this rule won't fire until the adapter is re-enabled._
- `OPAQUE_OWNERSHIP` — BODS bundle contains a `personStatement` with `personType=unknownPerson` or an `entityStatement` with `entityType=anonymousEntity`.

### AMLA CDD RTS (BODS v0.4 derived)

- `TRUST_OR_ARRANGEMENT` — entity with `entityType=arrangement` or a legal-form keyword (`trust`, `Stiftung`, `Anstalt`, `fideicomiso`, `Treuhand`, `foundation`). AMLA condition (a).
- `NON_EU_JURISDICTION` — any entity statement's `incorporatedInJurisdiction.code` outside the EU+EEA. AMLA condition (b). Configurable via `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` (additive, e.g. `GB,CH`) or `OPENCHECK_AMLA_EU_EEA_OVERRIDE` (full replace).
- `NOMINEE` — relationship interest type/details mentions nominee (English / French / camelCase variants), or person record mentions nominee. AMLA condition (c).
- `COMPLEX_OWNERSHIP_LAYERS` — DFS over the BODS relationship graph finds an entity-only chain ≥3 nodes (cycle-safe). Made meaningfully detectable by the Phase 10 Open Ownership bundles, which carry full multi-layer chains.
- `COMPLEX_CORPORATE_STRUCTURE` — composite (high), fires when `COMPLEX_OWNERSHIP_LAYERS` AND ≥1 of {trust, non-EU, nominee} both fire — the AMLA threshold rule end-to-end.
- `POSSIBLE_OBFUSCATION` — advisory (low) mirror of AMLA's subjective condition; explicitly notes the legitimate-economic-rationale caveat.

### FATF jurisdiction signals (BODS v0.4 derived)

For every `entityStatement` in the assembled BODS bundle, OpenCheck checks `incorporatedInJurisdiction.code` against the FATF lists current as of February 2026 (refreshed each FATF plenary: typically February, June, and October). Two independent signals, with different confidence levels reflecting FATF's own severity distinction:

- `FATF_BLACK_LIST` — `high` — entity in the FATF High-Risk Jurisdictions (Call for Action) list: **Democratic People's Republic of Korea (KP), Iran (IR), Myanmar (MM)**.
- `FATF_GREY_LIST` — `medium` — entity in the FATF Jurisdictions under Increased Monitoring list: Algeria, Angola, Bolivia, Bulgaria, Cameroon, Côte d'Ivoire, Democratic Republic of Congo, Haiti, Kenya, Kuwait, Laos, Lebanon, Monaco, Namibia, Nepal, Papua New Guinea, South Sudan, Syria, Venezuela, Vietnam, British Virgin Islands, Yemen.

Both signals are derived purely from the BODS jurisdiction codes — they fire independently of the AMLA CDD RTS composite rule and require no additional source calls. The country code sets live in `risk.py` (`FATF_BLACK_LIST_CODES` / `FATF_GREY_LIST_CODES`) and should be updated after each FATF plenary.

### Cross-source name match (Phase 11)

For every `personStatement` and `entityStatement` in the assembled BODS bundle, OpenCheck searches OpenSanctions (and EveryPolitician for persons) by name. Matches above a similarity threshold of 0.88 — with optional birth-year compatibility (±1 year, only when both sides supply a DOB) — produce **scoped** signals attached to the matching related-party's `statementId` (in `evidence.subject_statement_id`), not the subject. That means a sanctioned PSC behind an otherwise clean shell company surfaces on the right node in the graph.

- `RELATED_PEP` — a related person matches an OpenSanctions PEP record or appears in EveryPolitician.
- `RELATED_SANCTIONED` — a related person or entity matches an OpenSanctions `sanction*` record.

The normaliser folds standalone non-ASCII letters (Polish `ł`, Norwegian `ø`, German `ß`, Icelandic `ð`/`þ`, French `œ`) so transliterated and native spellings match. Bounded at `max_targets=25` per lookup to keep the OpenSanctions request volume sane on large PSC chains. The cross-check is a no-op when live mode is off or no OpenSanctions API key is configured.

## API surface

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe. |
| `GET /sources` | Inventory of the 7 source adapters with license, description, live status. |
| `GET /lookup?lei=<LEI>` | **Primary entry point**. LEI-anchored synthesis. |
| `GET /search?q=<q>&kind=<entity\|person>` | Free-text fan-out search. Power-user / debugging. |
| `GET /stream?q=<q>&kind=<...>` | Same fan-out, streamed as SSE. |
| `GET /deepen?source=<id>&hit_id=<id>` | Full record + BODS statements + risk signals for a single hit. |
| `GET /report?q=<q>&kind=<...>` | Free-text synthesis (the pre-LEI flow). |
| `GET /export?lei=<LEI>&format=zip\|json\|jsonl` | Downloadable BODS bundle. The `zip` form ships `bods.json` + `bods.jsonl` + `manifest.json` + `LICENSES.md`. |

The `/lookup` and `/export?lei=…` endpoints share their synthesis logic (`_build_report`), so the export bundle exactly mirrors what the user just saw.

## Configuration

Copy `.env.example` to `.env` and fill in the keys you have. None are required to run the project — every adapter falls back to stubs without one.

| Variable | Purpose |
|----------|---------|
| `OPENCHECK_ALLOW_LIVE` | Master switch. `true` enables live HTTP calls for adapters whose key is set. |
| `OPENCHECK_CORS_ORIGIN` | CORS origin for the frontend dev server. |
| `COMPANIES_HOUSE_API_KEY` | UK Companies House API key (free; <https://developer.company-information.service.gov.uk/>). |
| `OPENCORPORATES_API_KEY` | OpenCorporates API key — unlocks live company + officer data via the OC REST API. |
| `OPENSANCTIONS_API_KEY` | OpenSanctions API key (also unlocks the EveryPolitician PEPs dataset). |
| `OPENALEPH_API_KEY` | OpenAleph API key (optional — unlocks restricted collections). |
| `WIKIDATA_SPARQL_ENDPOINT` | Override the default Wikidata Query Service endpoint. |
| `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` | Comma-separated ISO codes added to the EU+EEA set used by `NON_EU_JURISDICTION` (e.g. `GB,CH`). |
| `OPENCHECK_AMLA_EU_EEA_OVERRIDE` | When set, replaces the EU+EEA default entirely. |
| `OPENCHECK_DATA_ROOT` | Override the cache root (used by tests; defaults to `./data`). |
| `ANTHROPIC_API_KEY` | Optional — reserved for future intent extraction / phrasing. |

## Tests

```bash
cd backend
uv run pytest             # 201 tests, ~5s
```

Frontend type check:

```bash
cd frontend
npm run build             # tsc + vite build
```

The backend tests use [`pytest-httpx`](https://github.com/Colin-b/pytest_httpx) to mock every live HTTP call, so the suite runs offline. Test files mirror the adapter / endpoint structure: `test_companies_house_live.py`, `test_gleif_live.py`, `test_lookup_endpoint.py`, `test_export_endpoint.py`, etc.

## Project structure

```
opencheck/
  backend/
    opencheck/
      app.py              FastAPI entry — /lookup, /search, /report, /export, /deepen, /stream
      sources/            One module per source adapter (8 implemented; 7 active in REGISTRY)
      bods/               BODS v0.4 mappers + validator
      bods_data.py        Open Ownership processed-bundle override layer (Phase 10)
      cross_check.py      Related-party name cross-check against OS + EveryPolitician (Phase 11)
      reconcile.py        Cross-source reconciler (LEI / Q-ID / GB-COH / OS-id bridges)
      risk.py             Risk-signal rules — 10 deterministic codes incl. AMLA CDD RTS
      cache.py            Two-tier cache (demos/ → live/)
      config.py           Pydantic settings; env vars listed above
    scripts/
      extract_bods_subgraphs.py   Walk local SQLite dumps to build per-LEI BODS bundles
    tests/                pytest suite (206 tests)
  frontend/               React + Vite + TypeScript + Tailwind + BO design system
    src/
      App.tsx             LEI input, subject card, risk chips, export panel, sources page
      components/         BODSGraph wraps @openownership/bods-dagre
      lib/api.ts          Typed client for the FastAPI surface
  docs/plan.md            Phase plan + design notes
  data/cache/             Two-tier cache root (live/ + bods_data/ gitignored)
  ATTRIBUTIONS.md         Per-source licensing
  LICENSE                 MIT (own code only — see ATTRIBUTIONS for source data)
```

## Licensing

OpenCheck's own code is [MIT-licensed](LICENSE). Data retrieved from third-party sources is licensed under each source's own terms — see [ATTRIBUTIONS.md](ATTRIBUTIONS.md). Downloaded exports include a `LICENSES.md` listing every source that contributed data, with re-use guidance for the most-restrictive license in the bundle.

The frontend also uses the [Beneficial Ownership Visualisation System](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/) design tokens and `@openownership/bods-dagre`, both © Open Ownership and re-used under CC BY 4.0 / Apache 2.0 respectively.

## Related projects and reading

- [Beneficial Ownership Data Standard (BODS) v0.4](https://standard.openownership.org/en/0.4.0/)
- [BODS RDF vocabulary 0.4](https://vocab.openownership.org/) — the alternative serialisation; the AMLA risk rules in `risk.py` are designed to be portable to a SPARQL/Oxigraph backbone.
- [GODIN — Global Open Data Integration Network](https://godin.gleif.org/) — the LEI-as-connector vision OpenCheck is built around.
- [AMLA draft CDD RTS public consultation](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en).
- [Open Ownership red flags in BODS data](https://www.openownership.org/en/blog/spotting-red-flags-in-beneficial-ownership-datasets/) and [risk-detection across BO + procurement + sanctions](https://www.openownership.org/en/blog/spotting-risks-by-combining-beneficial-ownership-public-procurement-and-sanctions-data/) — the prior-art OpenCheck builds on.

## Roadmap

- **Live opentender.eu integration** — the adapter is wired but `live_available=False` for now.
- **Surface `RELATED_*` signals on the BODS dagre graph** — currently they appear in the chip strip; ideally they'd render an OpenSanctions / EveryPolitician icon next to the matching node in the visualisation.
- **A "complex offshore" demo subject** that fires every AMLA chip simultaneously, for the consultation-friendly headline shot.
- **Re-enable OpenAleph** with an LEI-friendly entry path, once we have a curated demo set for it.
- **BODS RDF / SPARQL backbone** via Oxigraph — load the assembled BODS bundle into a triple store, expose `/sparql` for the published Open Ownership red-flag queries.

Open issues and discussion live in the [GitHub repo](https://github.com/StephenAbbott/opencheck).

## Deployment

A `render.yaml` blueprint is included for one-click deployment to [Render](https://render.com):

1. Push the repo to GitHub.
2. In the Render dashboard → **New → Blueprint**, point at the repo. Render creates both services automatically.
3. Set the secret env vars in the Render dashboard (under each service's **Environment** tab):
   - `COMPANIES_HOUSE_API_KEY`
   - `OPENCORPORATES_API_KEY`
   - `OPENSANCTIONS_API_KEY`
   - `OPENALEPH_API_KEY` (optional)
4. Once the backend service is live, copy its URL (e.g. `https://opencheck-api.onrender.com`) and set it as `VITE_API_BASE_URL` on the frontend static site, then trigger a redeploy of the frontend.

The backend runs as a Docker Web Service (uvicorn + Python 3.11); the frontend builds as a Render Static Site (Vite). Both use the free tier. The backend image bundles the pre-extracted BODS demo fixtures from `data/cache/` so the demo subjects work without a mounted volume.
