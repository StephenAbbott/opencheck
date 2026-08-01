<img width="898" height="331" alt="image" src="https://github.com/user-attachments/assets/cd7bea0c-ff06-4508-84d4-e420dba345fa" />

# OpenCheck

Customer due diligence risk checks powered by the Legal Entity Identifier (LEI), open data and open standards - including the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS). 

Try the demo at **https://opencheck.world/**

## What is OpenCheck?

You paste in a [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei). OpenCheck queries [GLEIF](https://www.gleif.org/) first, derives every cross-source identifier it can (UK Companies House number, Norwegian organisation number, Irish company registration number, Finnish Y-tunnus, Latvian registration number, Lithuanian entity code, Estonian registry code, Czech IČO, Polish KRS number, Austrian Firmenbuchnummer, Slovak IČO, French SIREN, Dutch KvK number, Swedish organisation number, Swiss UID, Canadian corporation number, Belgian enterprise number, Danish CVR number, Croatian MBS, Maltese registration number, Brazilian CNPJ, New Zealand company number, Australian ACN/ABN, OpenCorporates ID, Wikidata Q-ID, and more), and uses those bridges to fan out across 35 national and international corporate data sources.

Everything maps into [BODS v0.4](https://standard.openownership.org/en/0.4.0/). Cross-source links and risk signals are computed deterministically, and the whole bundle is one click away from a downloadable export (JSON / JSONL / XML / ZIP, plus [Senzing JSON](https://www.senzing.com/docs/entity_specification/) entity records for entity resolution, [FollowTheMoney](https://followthemoney.tech/) entities for OpenSanctions / OpenAleph investigative workflows, a [BigQuery property-graph](https://cloud.google.com/bigquery/docs/property-graphs) package queryable with GQL, [Google AML AI](https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model) input tables, and [BODS RDF](https://vocab.openownership.org/pages/4_convertingdata.html) as TriG for linked-data and SPARQL workflows).

The risk-signal layer mirrors the [EU AMLA draft customer due diligence regulatory technical standards](https://www.amla.europa.eu/policy/public-consultations/consultation-draft-rts-customer-due-diligence_en) conditions for "complex corporate structures" — trust/arrangement, non-EU jurisdiction, nominee, ≥3 ownership layers, plus the composite threshold rule and an advisory mirror of the subjective obfuscation condition.

## Status

**Latest: Phase 87** — the rigour 2.x switch points in `names.py`, cashed in

The four switch points Phase 85 documented and Phase 86 unlocked are resolved — one per commit, each gated by the name-matching eval (zero demo-corpus pairs cross the 0.88 threshold in any of them) with tests pinning both the `ftm`-extra and base-install paths. Transliteration lands as a deliberate hybrid: rigour's `maybe_ascii` turns out to be ISO-9-flavoured and diverges from the Latin forms OpenSanctions publishes (`ЛУКОЙЛ`→`LUKOJL` scores 0.83 against published `Lukoil`), so the curated Cyrillic/Greek tables stay authoritative and rigour extends coverage to Armenian and Georgian — the scripts the tables never had (`Ամերիաբանկ` now matches `Ameriabank` exactly), in comparable forms and BODS display alternates alike. The dense-script switch to `rigour.text.scripts` exposed and fixed a latent bug: normalised (NFKD-decomposed) Korean names silently failed the single-token matchability guard. The sec_edgar legal-form strip and the Danish `A/S` despace twist are re-verified against rigour 2.3.1 and kept — its org-type data still misses bare `COMPANY`/`CO` and `A/S` — with canary tests that flag the moment a future rigour closes either gap; the 0.x-era BCP-47 retry in language-code normalisation is deleted (rigour 2.x parses `zh-Hans` natively). Upstream, bods-ftm's shipped example — which had rotted to converting into zero FTM entities — is rebuilt as canonical BODS 0.4 with smoke tests pinning it to the converter. Commits `0a677c6`, `b7d8a04`, `a1dd695`, `9b4a829`.

**Previous: Phase 86** — followthemoney 4.x: the dependency ceiling on rigour lifts

[bods-ftm](https://github.com/StephenAbbott/bods-ftm) now runs followthemoney 4.x ([upstream PR #1](https://github.com/StephenAbbott/bods-ftm/pull/1), pinned), which removes the `rigour <1.0` cap that had held Phases 84–85 on the 0.x line: the `ftm` extra resolves **rigour 2.3.1** — OpenSanctions' Rust-cored data-cleaning library, with ICU transliteration, curated org-type data and script detection compiled in. Verified as a matching-behaviour change: the full suite is green with rigour 2.x active in the identifier and name paths, the BODS↔FtM converter needed zero API changes, and the name-matching eval gate shows no demo-corpus pair crossing the 0.88 match threshold in either direction. One planning assumption inverted: FtM 4.x makes the pyicu/ICU build chain *mandatory* rather than dropping it, so the Docker/CI toolchain stays and the parity-tested pure-Python fallbacks now serve only installs without the extra. This unlocks the documented `names.py` switch points (ICU transliteration, script detection, richer org-type stripping, the `analyze_names`/`compare_parts` alignment scorer) — cashing them in is the next arc. Commit `5ce78c2`.

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
