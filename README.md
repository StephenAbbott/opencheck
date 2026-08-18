<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, New Zealand company number, Australian ACN/ABN, Indian CIN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 38 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP, plus [Senzing JSON](https://www.senzing.com/docs/entity_specification/) entity records for entity resolution, [FollowTheMoney](https://followthemoney.tech/) entities for OpenSanctions / OpenAleph investigative workflows, a [BigQuery property-graph](https://cloud.google.com/bigquery/docs/property-graphs) package queryable with GQL, [Google AML AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input tables, and [BODS RDF](https://vocab.openownership.org/pages/4_convertingdata.html) as TriG for linked-data and SPARQL workflows).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 113** — the depth shown depended on which source finished last

Two loose ends from the Phase 111/112 work, both worse than the notes that logged them. `COMPLEX_OWNERSHIP_LAYERS` was recorded as a lost-graph-badge problem; reproducing it showed the **headline figure was wrong** — the number of ownership layers reported was whichever source happened to finish last, so a lookup where one source finds a five-layer chain and another finds three reports 5 or 3 purely on ordering, and a user reading "3 layers" had no way to know a deeper chain had been found and discarded. The fix is far smaller than assumed: statement ids are namespaced per source, so no ownership edge bridges two sources, the merged bundle is a set of disjoint subgraphs, and its longest path **is** the maximum of the per-source longest paths — verified rather than assumed. A resolver on the existing collapse (deepest chain wins) is therefore exactly equivalent to the merged-bundle computation that was thought necessary, and the surviving evidence is now the chain that justifies the number. Second, AMLA condition **(a)** is now scoped to the layered path, matching (b): point (a) says a trust or foundation "in any of the layers", so one on a side branch had been counting towards the complex-structure composite. Point (c) is deliberately left bundle-wide — "involved in the structure" is looser wording, and a test pins that asymmetry so it is not tidied into false consistency. Commit `f0973df`.

**Previous: Phase 112** — walking every place that claims what the engine produces

Phase 111 changed the risk engine; this phase fixes everything downstream still asserting the old output, and adds the guard that should have caught it. Four maps name signal codes and **nothing checked any of them against what the backend can actually emit** — the existing test starts from the frontend, so a code the frontend has never heard of passes in silence. That is exactly how `SANCTIONED_SECURITY` shipped with no graph badge at all, rendering as a grey "!" at severity 0: a sanctions-family finding shown as the lowest-priority marker on the graph, invisible to review because the code existed only as a string literal inside a function rather than a module constant. The dedup rule is now stated rather than inferred — membership of the globally-collapsing set is not "is this structural?" but "does its evidence identify particular nodes?", since anything naming nodes loses all but the last source's and with them their graph badges. `TRUST_OR_ARRANGEMENT` and `NOMINEE` moved out on that basis; `COMPLEX_OWNERSHIP_LAYERS` deliberately did not, because per source it would report conflicting layer counts. The pre-baked curated summaries were regenerated — after catching that Phase 111 changed the prompt wording without bumping `PROMPT_VERSION` — and all six had narrated non-EU status as a risk, **including two EU companies** flagged for non-EU subsidiaries. `COMPLEX_CORPORATE_STRUCTURE` vanished from every curated example, having fired throughout on layers plus a single condition. Commits `7d7d86d`, `30a7220`, `afa9442`, `ae0b541`, `08fe406`.

*Earlier: [Phase 111 — being foreign is not a risk finding](docs/status.md), and everything before it.*





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
