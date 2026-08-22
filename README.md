<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, New Zealand company number, Australian ACN/ABN, Indian CIN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 39 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP, plus [Senzing JSON](https://www.senzing.com/docs/entity_specification/) entity records for entity resolution, [FollowTheMoney](https://followthemoney.tech/) entities for OpenSanctions / OpenAleph investigative workflows, a [BigQuery property-graph](https://cloud.google.com/bigquery/docs/property-graphs) package queryable with GQL, [Google AML AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input tables, and [BODS RDF](https://vocab.openownership.org/pages/4_convertingdata.html) as TriG for linked-data and SPARQL workflows).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 121** — monitoring a source is not asking whether it replied

The Ariregister bug returned correct, live Estonian register data while recording no provenance observation, so OpenCheck badged it "Placeholder data" — and a weekly health check built the obvious way, asking each adapter whether it returned something, would have gone green every week for as long as the bug lived. Every one of the 39 registry adapters now has a probe run inside a `provenance.recording()` scope that asserts the **resolved liveness** as well as reachability; a weekly sweep exercises all of them against the real upstreams, and two guards move into the PR gate — probe/registry parity, and an AST check that an adapter building its own HTTP client records provenance in the same function. That guard immediately found a second, latent instance of the same defect, and the first sweep found a live one in Lithuania. A second pass closes both provenance gaps — one of them *over*-claiming `live` rather than under-claiming — and adds a GLEIF dispatch-drift check, since every national register is entered via a key derived from the GLEIF anchor and a renamed authority code would stop a source being reached at all while its own probe stayed green. A third completes it: BODS statement counts diffed week over week, so a source that still answers but has quietly stopped carrying ownership edges is caught, plus an age check for the committed snapshots whose failure mode is silence rather than an error. Commits `efe0da1`, `231ab81`, `131ff78`.

**Previous: Phase 120** — the false positives no threshold could kill, killed by shape

The name-collision survivors of PR #86 all share one anatomy: generic tokens match, the distinctive token differs. A new distinctive-token gate strips legal forms and filler and requires the residues to agree — killing every named collision (including two sitting *above* the old 0.93 threshold and still shipping) while the threshold comes back to 0.87 and recovers the true matches it had cost, LVMH's only offshore-leaks signal among them. Measured on a reproducibly rebuilt 14-subject corpus, with the harness and adjudication committed this time. Commit `c5e6002`.

*Earlier: [Phase 119 — OpenAleph signal volume, measured before mended](docs/status.md), [Phase 118 — export-control topics classified at last](docs/status.md), [Phase 117 — the evidence list no longer crowds out the page](docs/status.md), [Phase 116 — the exported report separates risk findings from structural context](docs/status.md), and everything before it.*





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
| [Dates](docs/dates.md) | The four date clocks, which sources supply a declaration date, how precision is recorded |
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
