<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, New Zealand company number, Australian ACN/ABN, Indian CIN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 38 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP, plus [Senzing JSON](https://www.senzing.com/docs/entity_specification/) entity records for entity resolution, [FollowTheMoney](https://followthemoney.tech/) entities for OpenSanctions / OpenAleph investigative workflows, a [BigQuery property-graph](https://cloud.google.com/bigquery/docs/property-graphs) package queryable with GQL, [Google AML AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input tables, and [BODS RDF](https://vocab.openownership.org/pages/4_convertingdata.html) as TriG for linked-data and SPARQL workflows).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 99** — Declared source liveness: three clocks, not one call to `date.today()`

Every BODS statement OpenCheck emitted claimed it had been retrieved at the moment the mapper ran. A bulk Parquet snapshot, a fixture committed to this repo, a fourteen-minute-old cache hit and a stub all asserted the same instant — and the BODS [dates guidance](https://standard.openownership.org/en/main/standard/modelling/dates-guidance.html) is firm that a republisher **must** state when it actually downloaded the data, so this was a conformance gap rather than a presentational one. Provenance is now recorded at the two chokepoints every adapter already passes through — the response cache and HTTP client construction — so 38 adapters needed no changes, and resolution is pessimistic: a lookup mixing cached and fresh requests reports the worst liveness and the oldest timestamp, because a bundle is only as fresh as its stalest component. `retrievedAt` is emitted only where a retrieval was genuinely observed; stub output carries none, and a committed fixture with no declared harvest date carries none either, since a checked-out file's mtime records when git wrote it to that machine, not when the data left the register. `statementDate` now falls back to the retrieval date rather than today, which for a months-old snapshot is much closer to the truth. Sources carry `liveness` through the API beside `degraded_sources`, on the same principle: data that is not current must not read as live, just as a check that could not run must not read as clean. Commit `33cccbd`.

**Previous: Phase 98** — Sanctions: telling an ownership chain apart from a handshake

OpenSanctions [now distinguishes](https://www.opensanctions.org/articles/2026-08-13-sanction-control/) `sanction.control` — a direct or indirect subsidiary, asset or vessel of a designated party, at any stake and any depth — from `sanction.linked`, plain one-hop adjacency. OpenCheck did not: a `startswith("sanction")` catch-all, duplicated across three modules, funnelled every subtopic into the softest bucket, so a company 52% owned by a sanctioned person rendered as amber "Sanctions-linked" over copy reading "the record is not itself sanctioned" — true, and the exact case an ownership-and-control test like OFAC's 50 Percent Rule exists to catch. The taxonomy now lives once, in `risk.py`, and new `SANCTIONS_CONTROLLED` / `RELATED_SANCTIONS_CONTROLLED` signals sit between `SANCTIONED` and `SANCTIONS_LINKED` at high confidence — the ownership assertion is deterministic, since upstream walked the chain; what is uncertain is the legal threshold, which the signal states outright rather than discounting into its confidence level. Because `sanction.linked` is a declared superset of `sanction.control`, the weaker chip is suppressed as the same fact stated less precisely — but a direct listing is a separate fact, so an entity both designated *and* inside another designated party's ownership chain reports both. A drift canary now asserts every published sanction-family topic is explicitly classified: `sanction.control` had been inside the screening scope all along, so those entities were always being fetched. Retrieving a topic is not the same as understanding it. Commits `2488c1c`, `00a7946`.

*Earlier: [Phase 97 — OpenAleph graph screening](docs/status.md), and everything before it.*


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

The BOVS icons and country-flag SVGs are committed under `frontend/public/`, so the dev server needs no extra build step.

## Documentation

| Page | Contents |
|------|----------|
| [How it works](docs/how-it-works.md) | Step-by-step lookup flow, per-adapter detail, Open Ownership BODS bundles, API surface, project structure |
| [Sources](docs/sources.md) | Full adapter table — active sources plus inactive bulk-only adapters, license, entry point, description |
| [Risk signals](docs/risk-signals.md) | All signal codes: source-derived, AMLA CDD RTS, FATF jurisdiction, state-controlled/SOE, cross-source name match, ICIJ Offshore Leaks |
| [Subsidiary network](docs/subsidiary-network.md) | Lazy GLEIF Level-2 reveal — direct + ultimate children mapped to BODS, graph (small) or table + export (large) |
| [Configuration](docs/configuration.md) | Environment variables, Render deployment, running the test suite |
| [Development history](docs/status.md) | All phases |

## Licensing

OpenCheck's own code is [MIT-licensed](LICENSE). Data retrieved from third-party sources is licensed under each source's own terms — see [ATTRIBUTIONS.md](ATTRIBUTIONS.md). Downloaded exports include a `LICENSES.md` listing every source that contributed data, with re-use guidance for the most-restrictive licence in the bundle; the RDF export additionally stamps each statement with its source's canonical licence URI (`bods:license`), so the licensing information is machine-readable and travels with the data itself.

The frontend renders ownership graphs with [Cytoscape.js](https://js.cytoscape.org/) (MIT). It re-uses the [Beneficial Ownership Visualisation System](https://www.openownership.org/en/publications/beneficial-ownership-visualisation-system/) design tokens (CC BY 4.0) and the BOVS entity/person icons and country-flag SVGs from Open Ownership's [visualisation library](https://github.com/openownership/visualisation-tool) (Apache 2.0) — both © Open Ownership. The committed assets live under `frontend/public/bods-dagre-images/`, a directory name retained from their original source.

## Roadmap

- **A "complex offshore" demo subject** that fires every AMLA chip simultaneously.
- **BODS RDF / SPARQL backbone** via Oxigraph — load the assembled BODS bundle into a triple store, expose `/sparql` for the published Open Ownership red-flag queries.

Open issues and discussion live in the [GitHub repo](https://github.com/StephenAbbott/opencheck).

## Related projects

- [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/)
- [BODS RDF vocabulary 0.4](https://vocab.openownership.org/) — the `risk.py` rules are designed to be portable to a SPARQL/Oxigraph backbone.
- [GODIN — Global Open Data Integration Network](https://godin.gleif.org/) — the LEI-as-connector vision OpenCheck is built around.
- [AMLA draft CDD RTS public consultation](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en).
- [Open Ownership red flags in BODS data](https://www.openownership.org/en/blog/spotting-red-flags-in-beneficial-ownership-datasets/) and [risk-detection across BO + procurement + sanctions](https://www.openownership.org/en/blog/spotting-risks-by-combining-beneficial-ownership-public-procurement-and-sanctions-data/).
