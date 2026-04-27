# OpenCheck

Customer due diligence risk checks driven by the Legal Entity Identifier (LEI) and open data.

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries GLEIF first, derives every cross-source identifier it can (UK Companies House number, Wikidata Q-ID, etc.), and uses those bridges to fan out across UK Companies House, OpenSanctions, OpenAleph, EveryPolitician, Wikidata, and OpenTender. 

Everything maps into [version 0.4 of the Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/), the cross-source links + risk signals are computed deterministically, and the whole bundle is one click away from a downloadable shareable export.

The risk-signal layer mirrors the [draft customer due diligence regulatory technical standards from the EU's Anti-Money Laundering Authority (AMLA)](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) draft conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

OpenCheck has shipped through six phases (latest commit on `main` is the source of truth):

| Phase | Headline |
|------:|----------|
| 0 | Scaffold — FastAPI + React/Vite + 6 stub source adapters |
| 1 | Live UK Companies House + BODS v0.4 mapper + SSE streaming |
| 2 | Live GLEIF + OpenSanctions + OpenAleph + FtM/GLEIF mappers |
| 3 | Live Wikidata + EveryPolitician + reconciler + risk signals (incl. AMLA CDD RTS) |
| 4 | Cache-first dispatch + bods-dagre visualisation |
| 5 | Export endpoint (JSON / JSONL / ZIP) + OpenTender (DIGIWHIST) procurement source |
| 6 | LEI-anchored `/lookup` flow + BO design system + bods-dagre fix |

Test suite: 183 backend tests across the seven phases. Frontend type-checks clean.

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
2. Calls **GLEIF** first. We need the legal name, jurisdiction, and `registeredAs` for cross-source bridging.
3. Looks up the **Wikidata Q-ID** via SPARQL on property `P1278`.
4. Dispatches to every other adapter using whichever identifier they understand:
   - UK Companies House — direct fetch by company number when GLEIF says jurisdiction = GB.
   - OpenSanctions / OpenAleph / OpenTender — search by the LEI string.
   - Wikidata — direct SPARQL fetch on the resolved Q-ID.
5. Maps each source's payload into BODS v0.4 statements, runs the cross-source reconciler, runs the risk-signal service, and returns one unified report.

The frontend renders that report as a single subject card at the top (legal name, jurisdiction, derived identifiers as chips), an aggregated risk-chip strip, a cross-source links panel, an export button with format selector, and per-source "bucket" cards with a `Go deeper` drill-down per hit.

## Sources

Seven adapters, each implementing the same `SourceAdapter` protocol (`search`, `fetch`, `info`):

| ID | Name | License | Description |
|----|------|---------|-------------|
| `companies_house` | UK Companies House | OGL-3.0 | Legal and beneficial ownership information from the UK corporate registry |
| `gleif` | GLEIF | CC0-1.0 | Legal entity information from the Global Legal Entity Identifier Foundation |
| `opensanctions` | OpenSanctions | CC BY-NC 4.0 | The open-source database of sanctions, watchlists, and politically exposed persons |
| `openaleph` | OpenAleph | per-collection | The open-source platform that securely stores large amounts of data and makes it searchable |
| `everypolitician` | EveryPolitician | CC BY-NC 4.0 | Global database of political office-holders (served via OpenSanctions PEPs dataset) |
| `wikidata` | Wikidata | CC0-1.0 | A free and open knowledge base that can be read and edited by both humans and machines |
| `opentender` | OpenTender (DIGIWHIST) | CC BY-NC-SA 4.0 | Search and analyse tender data from 35 jurisdictions |

OpenCorporates is on the roadmap and will land once an API key arrives.

NC-licensed sources propagate their share-alike / non-commercial obligations through `/deepen` and `/export`. The exported `LICENSES.md` warns reviewers before they re-publish.

## Risk signals

Ten codes, all deterministic — every fire is documented with a `summary`, `confidence` (`high` / `medium` / `low`), and an `evidence` payload citing the underlying topic / collection / BODS statement IDs.

Source-derived (search-time):

- `PEP` — OpenSanctions `role.pep`-family topic, every EveryPolitician hit, or a Wikidata person with a currently-held position (P39 with no P582 end qualifier).
- `SANCTIONED` — OpenSanctions topic starting with `sanction`.
- `OFFSHORE_LEAKS` — OpenAleph hit in an ICIJ-family collection (Panama / Paradise / Pandora / Bahamas / Offshore Leaks).
- `OPAQUE_OWNERSHIP` — BODS bundle contains a `personStatement` with `personType=unknownPerson` or an `entityStatement` with `entityType=anonymousEntity`.

AMLA CDD RTS (BODS v0.4 derived):

- `TRUST_OR_ARRANGEMENT` — entity with `entityType=arrangement` or a legal-form keyword (`trust`, `Stiftung`, `Anstalt`, `fideicomiso`, `Treuhand`, `foundation`). AMLA condition (a).
- `NON_EU_JURISDICTION` — any entity statement's `incorporatedInJurisdiction.code` outside the EU+EEA. AMLA condition (b). Configurable via `OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS` (additive, e.g. `GB,CH`) or `OPENCHECK_AMLA_EU_EEA_OVERRIDE` (full replace).
- `NOMINEE` — relationship interest type/details mentions nominee (English / French / camelCase variants), or person record mentions nominee. AMLA condition (c).
- `COMPLEX_OWNERSHIP_LAYERS` — DFS over the BODS relationship graph finds an entity-only chain ≥3 nodes (cycle-safe).
- `COMPLEX_CORPORATE_STRUCTURE` — composite (high), fires when `COMPLEX_OWNERSHIP_LAYERS` AND ≥1 of {trust, non-EU, nominee} both fire — the AMLA threshold rule end-to-end.
- `POSSIBLE_OBFUSCATION` — advisory (low) mirror of AMLA's subjective condition; explicitly notes the legitimate-economic-rationale caveat.

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
uv run pytest             # 183 tests, ~6s
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
      sources/            One module per source adapter (7 adapters)
      bods/               BODS v0.4 mappers + validator
      reconcile.py        Cross-source reconciler (LEI / Q-ID / GB-COH / OS-id bridges)
      risk.py             Risk-signal rules (10 codes incl. AMLA CDD RTS)
      cache.py            Two-tier cache (demos/ → live/)
      config.py           Pydantic settings; env vars listed above
    tests/                pytest suite (183 tests)
  frontend/               React + Vite + TypeScript + Tailwind + BO design system
    src/
      App.tsx             LEI input, subject card, risk chips, export panel
      components/         BODSGraph wraps @openownership/bods-dagre
      lib/api.ts          Typed client for the FastAPI surface
  docs/plan.md            Phase plan + design notes
  data/cache/             Two-tier cache root (live/ is gitignored)
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

- **OpenCorporates adapter** once the API key arrives — adds another LEI / company-number bridge.
- **Live opentender.eu integration** — the adapter is wired but `live_available=False` for now.
- **A "complex offshore" demo subject** that fires every AMLA chip simultaneously, for the consultation-friendly headline shot.
- **BODS RDF / SPARQL backbone** via Oxigraph — load the assembled BODS bundle into a triple store, expose `/sparql` for the published Open Ownership red-flag queries.

Open issues and discussion live in the [GitHub repo](https://github.com/StephenAbbott/opencheck).
