<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, New Zealand company number, Australian ACN/ABN, Indian CIN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 38 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP, plus [Senzing JSON](https://www.senzing.com/docs/entity_specification/) entity records for entity resolution, [FollowTheMoney](https://followthemoney.tech/) entities for OpenSanctions / OpenAleph investigative workflows, a [BigQuery property-graph](https://cloud.google.com/bigquery/docs/property-graphs) package queryable with GQL, [Google AML AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input tables, and [BODS RDF](https://vocab.openownership.org/pages/4_convertingdata.html) as TriG for linked-data and SPARQL workflows).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 111** — being foreign is not a risk finding

Every UK company with an LEI carried a "Non-EU jurisdiction" chip at **high** confidence — the same rung as `SANCTIONED` — and three of the six homepage examples advertised it, so BP was flagged for being British and Eli Lilly for being American, next to genuine adverse findings. Read against AMLA's draft CDD RTS, the claim has no basis in the instrument it cites: the only operative "outside the EU" provision is a sub-condition of the complex-structure test, and AMLR **Annex III(3)** lists higher-risk geography only as *qualified* categories of third country (FATF-listed, ineffective AML/CFT system, significant corruption, sanctioned, terrorist financing, financial secrecy) while **Annex II** puts third countries with effective systems in the **lower**-risk column. Two correctness bugs fell out of the same reading — Article 12(1) requires "more than one" condition and the composite fired on one, so any three-layer group with a single non-EU entity was reported as a complex corporate structure; and condition (b) is scoped to the layered path, not the whole bundle. `NON_EU_JURISDICTION` is now `kind="context"` at low confidence in its own "Structural context" section, a classification that lives on the signal because the risk *count* is rendered independently by three surfaces. Replacing it, a new `EU_HIGH_RISK_THIRD_COUNTRY` signal reads the EU's own Article 29 list, kept separate from FATF because the two diverge — Algeria and Namibia are EU-listed but came off the FATF grey list in June 2026. Deliberately **not** built: a jurisdiction-level "no beneficial ownership register" signal, whose only available observation conflates *no register*, *closed register* and *we have no adapter yet* — turning a coverage gap into an accusation. The rule this establishes: a geographic risk chip only where an authoritative, externally maintained, dated list says so. Commits `2621bad`, `071d43d`, `d3dc4ef`, `b25d315`.

**Previous: Phase 110** — counting what the sources actually contribute

"Is OpenAleph screening contributing anything in production?" was unanswerable without running lookups by hand and counting. The obvious alternative — sweeping a few hundred LEIs — is both expensive and misleading: every lookup is a live fan-out across ~38 upstreams, so a sweep loads a free-tier instance, risks rate limits whose degraded results read as "signal absent", pulls non-commercial data at volume for what is effectively analytics, and samples whichever LEIs were picked. Counting server-side has none of those problems — it observes traffic that was going to happen anyway, is exact rather than sampled, and keeps working without anyone re-running anything. A new `signalstats` module counts signals per source and code, degradations per source and reason (a signal count without the count of screens that failed to run is not a low number, it is an unknown one), and completed lookups as the denominator, exposed at `/signalstats` on the same public, aggregate-only contract as the existing `/memstats`. Counting sits inside the deduplication function so "count what a user sees" is true by construction, and is opt-in so the free-text debugging endpoint that shares that function cannot inflate the denominator. Privacy is structural rather than policed: the recorders read only closed-vocabulary fields, so names and identifiers cannot reach a counter — the tests stuff names into every free-text field and assert none appear. Counters are in-process and reset on deploy. Commit `97e0925`.

*Earlier: [Phase 109 — an unbadged node is a claim, and it was the wrong one](docs/status.md), and everything before it.*





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
