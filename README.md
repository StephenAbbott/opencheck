<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, Australian ACN/ABN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 31 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 57** — Securities (ISINs) & the sanctioned-securities overlay

A new entity-level **Securities** panel showing the securities mapped to a company's LEI and surfacing any that are sanctioned — plus a first-class `SANCTIONED_SECURITY` risk chip.

1. **Three open datasets, three roles.** **GLEIF** (`/lei-records/{lei}/isins`) gives the authoritative LEI→ISIN list and total count — fetched as count + one page only, since major issuers carry tens of thousands (Deutsche Bank ≈ 22,500). **OpenFIGI** types just the ISINs actually shown (security type, ticker, exchange). The free **OpenSanctions `securities.csv`** export supplies the sanctioned subset by LEI + regimes (incl. EO 14071 investment bans) — covering GLEIF's blind spot (Rosneft has 0 ISINs in GLEIF but 12 sanctioned).
2. **Lazy and sanctions-first.** `GET /securities?lei=` assembles these on demand (never on the main lookup) and never enumerates every ISIN. The UI leads with a red banner of the sanctioned ISINs; the long tail is a count behind a searchable, type-filtered "Browse all" drawer.
3. **Risk chip everywhere.** If the LEI has sanctioned securities, the main lookup emits `SANCTIONED_SECURITY` (high confidence) — so it appears in the risk chips, the BODS export and the MCP `lookup` tool.
4. **Self-refreshing index.** A pure-stdlib extractor builds the compact `lei→sanctioned-ISINs` index (~187 LEIs / 13k ISINs from the 8.8 MB source); a weekly GitHub Action republishes it as a Release asset the backend loads at startup (local file or hosted URL).

*Previous: [Phase 56 — Brazil CNPJ register adapter](docs/status.md)*

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
| [Sources](docs/sources.md) | Full adapter table — 30 active sources plus inactive bulk-only adapters, license, entry point, description |
| [Risk signals](docs/risk-signals.md) | All 12 signal codes: source-derived, AMLA CDD RTS, FATF jurisdiction, cross-source name match, ICIJ Offshore Leaks |
| [Configuration](docs/configuration.md) | Environment variables, Render deployment, running the test suite |
| [Development history](docs/status.md) | All 54 phases |

## Licensing

OpenCheck's own code is [MIT-licensed](LICENSE). Data retrieved from third-party sources is licensed under each source's own terms — see [ATTRIBUTIONS.md](ATTRIBUTIONS.md). Downloaded exports include a `LICENSES.md` listing every source that contributed data, with re-use guidance for the most-restrictive licence in the bundle.

The frontend also uses the [Beneficial Ownership Visualisation System](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/) design tokens and `@openownership/bods-dagre`, both © Open Ownership and re-used under CC BY 4.0 / Apache 2.0 respectively.

## Roadmap

- **Live opentender.eu integration** — the adapter is wired but `live_available=False` for now.
- **A "complex offshore" demo subject** that fires every AMLA chip simultaneously.
- **BODS RDF / SPARQL backbone** via Oxigraph — load the assembled BODS bundle into a triple store, expose `/sparql` for the published Open Ownership red-flag queries.

Open issues and discussion live in the [GitHub repo](https://github.com/StephenAbbott/opencheck).

## Related projects

- [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/)
- [BODS RDF vocabulary 0.4](https://vocab.openownership.org/) — the `risk.py` rules are designed to be portable to a SPARQL/Oxigraph backbone.
- [GODIN — Global Open Data Integration Network](https://godin.gleif.org/) — the LEI-as-connector vision OpenCheck is built around.
- [AMLA draft CDD RTS public consultation](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en).
- [Open Ownership red flags in BODS data](https://www.openownership.org/en/blog/spotting-red-flags-in-beneficial-ownership-datasets/) and [risk-detection across BO + procurement + sanctions](https://www.openownership.org/en/blog/spotting-risks-by-combining-beneficial-ownership-public-procurement-and-sanctions-data/).
