<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.onrender.com/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 27 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 44** — BODS graph renderer migrated from `@openownership/bods-dagre` (45 npm downloads/week, required UMD global + SVGInjector XHR workarounds, BezierJS edge-offset bug) to **Cytoscape.js** (7.9M downloads/week, MIT, academically published) with `cytoscape-dagre` for hierarchical layout. BOVS-compliant edge annotations built-in (ownership blue, control orange, role purple/dashed). Node icons and jurisdiction flags rendered via a React HTML overlay layer above the Cytoscape canvas — pixel-perfect centering at all zoom levels, flags as BOVS Metadata Overlays at the 45° (NE) circumference point per spec. 1738 backend tests unchanged.

*Previous: [Phase 43 — BODS compliance audit](docs/status.md)*

→ [Full development history](docs/status.md)

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

The first frontend build copies bundled images for `@openownership/bods-dagre` into `public/bods-dagre-images/`. If they're missing, run `npm run build` once.

## Documentation

| Page | Contents |
|------|----------|
| [How it works](docs/how-it-works.md) | Step-by-step lookup flow, per-adapter detail, Open Ownership BODS bundles, API surface, project structure |
| [Sources](docs/sources.md) | Full adapter table — 27 sources, license, entry point, description |
| [Risk signals](docs/risk-signals.md) | All 12 signal codes: source-derived, AMLA CDD RTS, FATF jurisdiction, cross-source name match, ICIJ Offshore Leaks |
| [Configuration](docs/configuration.md) | Environment variables, Render deployment, running the test suite |
| [Development history](docs/status.md) | All 43 phases |

## Licensing

OpenCheck's own code is [MIT-licensed](LICENSE). Data retrieved from third-party sources is licensed under each source's own terms — see [ATTRIBUTIONS.md](ATTRIBUTIONS.md). Downloaded exports include a `LICENSES.md` listing every source that contributed data, with re-use guidance for the most-restrictive licence in the bundle.

The frontend also uses the [Beneficial Ownership Visualisation System](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/) design tokens and `@openownership/bods-dagre`, both © Open Ownership and re-used under CC BY 4.0 / Apache 2.0 respectively.

## Roadmap

- **Live opentender.eu integration** — the adapter is wired but `live_available=False` for now.
- **Surface `RELATED_*` signals on the BODS dagre graph** — currently they appear in the chip strip; ideally they'd render an OpenSanctions / EveryPolitician icon next to the matching node.
- **A "complex offshore" demo subject** that fires every AMLA chip simultaneously.
- **BODS RDF / SPARQL backbone** via Oxigraph — load the assembled BODS bundle into a triple store, expose `/sparql` for the published Open Ownership red-flag queries.

Open issues and discussion live in the [GitHub repo](https://github.com/StephenAbbott/opencheck).

## Related projects

- [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/)
- [BODS RDF vocabulary 0.4](https://vocab.openownership.org/) — the `risk.py` rules are designed to be portable to a SPARQL/Oxigraph backbone.
- [GODIN — Global Open Data Integration Network](https://godin.gleif.org/) — the LEI-as-connector vision OpenCheck is built around.
- [AMLA draft CDD RTS public consultation](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en).
- [Open Ownership red flags in BODS data](https://www.openownership.org/en/blog/spotting-red-flags-in-beneficial-ownership-datasets/) and [risk-detection across BO + procurement + sanctions](https://www.openownership.org/en/blog/spotting-risks-by-combining-beneficial-ownership-public-procurement-and-sanctions-data/).
