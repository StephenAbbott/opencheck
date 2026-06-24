# How OpenCheck works

Search by company name, enter a local registration number, or paste a 20-character ISO 17442 LEI — for example `213800LH1BZH3DI6G760` (BP) or `253400JT3MQWNDKMJE44` (Rosneft) — and the backend:

1. Validates the LEI shape.
2. **Subject metadata.** If a pre-extracted Open Ownership bundle exists at `data/cache/bods_data/gleif/<LEI>.jsonl`, the legal name + jurisdiction are read directly from it (no live GLEIF call needed). Otherwise GLEIF is queried live.
3. Looks up the **Wikidata Q-ID** via SPARQL on property `P1278`.
4. Dispatches to every other adapter using whichever identifier they understand:
   - **UK Companies House** — direct fetch by `gb_coh` when jurisdiction = GB. The Open Ownership processed UK PSC bundle (`data/cache/bods_data/uk/<GB-COH>.jsonl`) is the canonical answer when present; otherwise falls back to the live API. Active directors from `officers.items` are mapped to `personStatement` + `relationshipStatement` (`seniorManagingOfficial`, `beneficialOwnershipOrControl: false`); secretaries and resigned directors are excluded.
   - **Brreg — Brønnøysundregistrene (Norway)** — fetched by `no_orgnr` (derived from GLEIF RA code `RA000472`); delivers company profile and role-holders (CEO, board chair, board members, deputies, and other officers) as BODS statements via the public Enhetsregisteret REST API. No API key required; licensed NLOD 2.0.
   - **CRO — Companies Registration Office Ireland** — fetched by `ie_crn` (derived from GLEIF RA code `RA000402`); delivers company profile (status, type, registration date, address) from the CRO Open Data Portal CKAN API. No API key required; licensed CC BY 4.0.
   - **PRH — Finnish Patent and Registration Office** — fetched by `fi_ytunnus` (Y-tunnus, derived from GLEIF RA code `RA000188`); delivers entity details from the YTJ (Business Information System) Open Data API. Officer data is not publicly available (the paid Virre service covers it). No API key required; licensed CC BY 4.0.
   - **UR Latvia — Latvian Register of Enterprises** — fetched by `lv_regcode` (11-digit registration number, derived from GLEIF RA code `RA000423`); queries the CKAN Datastore API on data.gov.lv to join five open datasets: the business register (entity profile), beneficial owners (UBO declarations from the Latvian BO register), officers (executive/supervisory board members, liquidators, and other representatives), SIA shareholders (LLC share-register entries), and historical names. All five tables are live-queryable via the CKAN Datastore API without downloading the bulk CSVs. Maps the full dataset to BODS v0.4 entity, person, and ownership-or-control statements. No API key required; open government data.
   - **JAR Lithuania — Lithuanian Register of Legal Entities** — fetched by `lt_code` (9-digit entity code, derived from GLEIF RA code `RA000430`); scrapes the Registrų centras public JAR search page to retrieve entity name, address, legal form, and registration status. Maps to a BODS v0.4 entity statement. BO / participant data (formerly JADIS) is excluded — it is being migrated to the restricted JANGIS system. No API key required; CC BY 4.0.
   - **ARES (Czechia)** — fetched by `cz_ico` (8-digit IČO, derived from GLEIF RA code `RA000163`); queries the ARES REST API aggregate endpoint for entity basics (name, address, legal form, registration date, status) and the VR (Veřejný rejstřík / commercial register) endpoint for shareholders (akcionáři / společníci) and directors (statutární orgány). Emits full BODS v0.4 entity, person, and ownership-or-control statements. Returns a graceful stub for entities not in the commercial register (VR 404). No API key required; CC BY 4.0.
   - **BCE Belgium — Banque-Carrefour des Entreprises / Kruispuntbank van Ondernemingen** — fetched by `be_enterprise_number` (10-digit enterprise number, derived from GLEIF RA code `RA000025`); delivers entity name (Dutch/French/German), status, juridical form, start date, and registered address from a local SQLite database built from the monthly KBO open data ZIP by `scripts/extract_bce.py`. Also supports name search via FTS5 on the `/search` endpoint. No API key required; KBO reuse licence. Activated when `BCE_BELGIUM_DB_FILE` is set.
   - **Corporations Canada (ISED)** — fetched by `ca_corp_id` (numeric corporation number, derived from GLEIF RA code `RA000072`); queries the ISED API Gateway V1 endpoint for corporation details (name, status, act of incorporation, registered address, business number) and the V2 endpoint for current directors. Directors are mapped to BODS v0.4 `seniorManagingOfficial` relationship statements. Requires `CORPORATIONS_CANADA_API_KEY`; licensed OGL-Canada 2.0.
   - **Ariregister (Estonia)** — fetched by the Estonian registry code (derived from GLEIF RA code `RA000181`); scrapes the public e-Business Register at `ariregister.rik.ee` (no credentials required) for entity profile, officers, shareholders, and beneficial owners.
   - **INPI (France)** — fetched by `fr_siren` (derived from GLEIF RA code `RA000189`); delivers company profile and officers as BODS statements via the Registre National des Entreprises API. Individual persons in `composition.pouvoirs` with `typeDePersonne == "INDIVIDU"` and `beneficiaireEffectif == false` are mapped to person + relationship statements using the full 65-code `roleEntreprise` codelist; BO records (`beneficiaireEffectif == true`) are silently excluded per Loi Sapin II.
   - **KvK (Netherlands)** — fetched by `nl_kvk` (derived from GLEIF RA code `RA000463`); delivers company details and authorised representatives via the Kamer van Koophandel Handelsregister API.
   - **Bolagsverket (Sweden)** — fetched by `se_org_number` (derived from GLEIF RA code `RA000544`); delivers company profile and board-level officers via the Swedish Companies Registration Office API.
   - **Zefix (Switzerland)** — fetched by `ch_uid` (derived from GLEIF RA code `RA000412`); delivers company profile and authorised signatories from the Zefix central business name index.
   - **Firmenbuch (Austria)** — fetched by `at_fn` (Firmenbuchnummer, derived from GLEIF RA code `RA000128`); delivers entity name, address, status, and officers (managing directors, signatories, supervisory board) from the Austrian commercial register via the Justiz Online SOAP API. Requires free `FIRMENBUCH_API_KEY`.
   - **KRS Poland (Krajowy Rejestr Sądowy)** — fetched by `pl_krs` (derived from GLEIF RA code `RA000439`); delivers entity basics and board/officer data from the Polish National Court Register via the KRS REST API. Names are masked in the public API. No API key required.
   - **RPO Slovakia — Register právnických osôb** — fetched by `sk_ico` (8-digit IČO, derived from GLEIF RA code `RA000526`); delivers entity name, address, establishment date, termination date, registration number, and court from Slovakia's Register of Legal Persons via the ŠÚ SR REST API. Maps to a BODS v0.4 entity statement. No API key required; CC BY 4.0.
   - **RPVS Slovakia — Register partnerov verejného sektora** — also fetched by `sk_ico`, independently of RPO; queries the Ministry of Justice OData API (`rpvs.gov.sk/opendatav2`) to retrieve the entity's public-sector partner registration and all its verified beneficial owner (KUV / konečný užívateľ výhod) declarations. Two-step: resolves IČO → internal `CisloVlozky` (entry number), then fetches the full `Partneri` record with `KonecniUzivateliaVyhod`, `PartneriVerejnehoSektora`, and `OpravneneOsoby` expanded. Maps to BODS v0.4 entity, person, and ownership-or-control statements; KUV validity windows (`PlatnostOd`/`PlatnostDo`) and the `JeVerejnyCinitel` (public official) flag are preserved. Covers entities that supply goods or services to public bodies above Slovakia's legal procurement thresholds — participation is mandatory for qualifying suppliers and the KUV declarations are verified by an authorised person (lawyer or notary). No API key required; CC BY 4.0.
   - **CVR Denmark (Det Centrale Virksomhedsregister)** — fetched by `dk_cvr` (derived from GLEIF RA code `RA000170`, Erhvervsstyrelsen); queries the Datafordeler GraphQL API for entity basics (name, address, legal form, sector, status); bitemporal records filtered to current. Requires free `CVR_DENMARK_API_KEY` from portal.datafordeler.dk.
   - **Sudski registar (Croatia)** — fetched by `hr_mbs` (derived from GLEIF RA code `RA000156`); delivers entity basics (legal name, short name, legal form, status, founding date, registered seat) and `HR-MBS` + `HR-OIB` identifiers via the public `sudreg_javni` v3 JSON API (OAuth2 client credentials). Entity statements only; officers and BO not published. Requires `SUDREG_CLIENT_ID` / `SUDREG_CLIENT_SECRET` (free from sudreg-data.gov.hr).
   - **ABR Australia (Australian Business Register)** — for Australian-jurisdiction entities, routes by digit length: 9-digit ACN (from GLEIF RA code `RA000014`, ASIC) or 11-digit ABN (from `RA000013`, ABR); delivers entity name and type, ABN/GST status, registered state and postcode, and trading names via the free ABN Lookup JSON web services. Entity statements only; requires free `ABN_GUID`.
   - **OpenCorporates** — fetched by `ocid` (e.g. `gb/00102498`), a field GLEIF returns on Level 1 records; delivers company profile, current officers, and network relationships (from the live API or the OC Relationships bulk file) as BODS statements.
   - **SEC EDGAR** — for US-jurisdiction entities, searches by legal name via the EDGAR company-search atom feed to find the subject company's CIK, then retrieves the most recent Schedule 13D and 13G filings (major shareholders reporting >5 % of any registered equity class, mandatory XML format since December 2024) as BODS statements. No API key required.
   - **OpenAleph** — LEI → OC URL → registration numbers → legal name cascade; delivers entity records from open knowledge bases indexed by OCCRP's AlephData platform. Timeout 60 s. No API key required.
   - **OpenSanctions / OpenTender** — search by the LEI string.
   - **Wikidata** — direct SPARQL fetch on the resolved Q-ID.
5. Maps each source's payload into BODS v0.4 statements, runs the cross-source reconciler, runs the risk-signal service, **cross-checks every related person and entity in the BODS bundle against OpenSanctions + EveryPolitician by name** — fuzzy-matched with optional birth-year compatibility — to surface scoped `RELATED_PEP` / `RELATED_SANCTIONED` signals, and **cross-checks all names against the ICIJ Offshore Leaks reconciliation API** to surface `OFFSHORE_LEAKS` signals for any Panama Papers / Pandora Papers / Paradise Papers matches.
6. Returns one unified report.

The frontend renders that report as a single subject card at the top (legal name, jurisdiction, derived identifiers as chips), an aggregated risk-chip strip, a cross-source links panel, an export button with format selector, and per-source "bucket" cards with a `Go deeper` drill-down per hit. A separate **About the sources** page (linked from the header) shows the source inventory.

## Open Ownership BODS bundles

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

## API surface

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe. |
| `GET /sources` | Inventory of all active source adapters with license, description, and live status. |
| `GET /lookup?lei=<LEI>` | **Primary entry point**. LEI-anchored synthesis; returns a complete `LookupResponse` once all sources resolve. |
| `GET /lookup-stream?lei=<LEI>` | Same pipeline as `/lookup`, streamed as SSE events (`gleif_done`, `source_hit`, `source_error`, `risk`, `done`). The frontend uses this for progressive rendering. |
| `GET /lookup-source?lei=<LEI>&source_id=<id>` | Re-runs a single source adapter and invalidates the replay cache for that LEI. Used by the "Retry source" button on failed source cards. |
| `GET /search?q=<q>&kind=<entity\|person>` | Free-text fan-out search. Power-user / debugging. |
| `GET /stream?q=<q>&kind=<...>` | Same fan-out, streamed as SSE. |
| `GET /deepen?source=<id>&hit_id=<id>` | Full record + BODS statements + risk signals for a single hit. |
| `GET /report?q=<q>&kind=<...>` | Free-text synthesis (the pre-LEI flow). |
| `GET /export?lei=<LEI>&format=zip\|json\|jsonl\|xml` | Downloadable BODS bundle. `zip` ships `bods.json` + `bods.jsonl` + `bods.xml` + `manifest.json` + `LICENSES.md`; `json` / `jsonl` / `xml` return the statements only. The `xml` format uses the [canonical BODS v0.4 XML serialisation](https://github.com/StephenAbbott/bods-xml). Add `&subsidiaries=true` to opt the GLEIF subsidiary network into the bundle (off by default — a large group can add hundreds of statements; deduped by `statementId`). |
| `GET /subsidiaries?lei=<LEI>&format=summary\|bods` | GLEIF Level-2 **subsidiary network** — direct + ultimate children merged and tagged `direct` / `ultimate` / `both`, with exact counts, a jurisdiction spread and a `render_mode` hint (graph ≤ 150 nodes, else table); `format=bods` adds the BODS statements. Lazy, never on the main lookup; gated on `OPENCHECK_ALLOW_LIVE`. See [docs/subsidiary-network.md](subsidiary-network.md). |
| `GET /securities?lei=<LEI>&page=<n>` | Securities (ISINs) mapped to the LEI (GLEIF + OpenFIGI) with a sanctioned-securities overlay. See [docs/securities.md](securities.md). |
| `GET /history?lei=<LEI>&include_noise=<bool>` | The Time Machine change-over-time timeline (GLEIF + Companies House) on one shared model. See [docs/time-machine.md](time-machine.md). |
| `GET /nz-associations?company_number=<n>` | New Zealand director/shareholder cross-company associations (nominee / mass-directorship review). See [docs/nz-associations.md](nz-associations.md). |

The enrichment endpoints (`/subsidiaries`, `/securities`, `/history`, `/nz-associations`) are lazy and panel-only — fetched on demand, cached, and never part of the main lookup synthesis.

`/lookup`, `/lookup-stream`, and `/export?lei=…` all share the same single async generator `_lookup_pipeline()` — the export bundle exactly mirrors what the user saw. Completed pipeline runs are cached in memory for 15 minutes (keyed by LEI; `?refresh=true` bypasses). Lookups are addressable via `?lei=` query parameter in the frontend URL, so results pages are shareable.

## Project structure

```
opencheck/
  backend/
    opencheck/
      app.py              FastAPI entry — /lookup, /lookup-stream, /lookup-source, /search, /report, /export, /deepen
      routers/
        lookup.py         Single _lookup_pipeline() async generator drives both /lookup and /lookup-stream
      sources/            One module per source adapter (28 active in REGISTRY + 3 inactive/bulk-only)
        brightquery.py    BrightQuery / OpenData.org — LEI-keyed US entity + executive data
        opencorporates.py OpenCorporates — company profile, officers, network relationships
        oc_relationships.py  OC Relationships bulk-file lookup (indexed by jurisdiction/number)
        sec_edgar.py      SEC EDGAR — Schedule 13D/13G major-shareholder filings for US-listed companies
      bods/               BODS v0.4 mappers + validator (full 24-code interestType codelist)
      bods_data.py        Open Ownership processed-bundle override layer (Phase 10)
      cross_check.py      Related-party name cross-check against OS + EveryPolitician (Phase 11)
      icij_check.py       ICIJ Offshore Leaks name cross-check via reconciliation API (Phase 22)
      reconcile.py        Cross-source reconciler (LEI / Q-ID / GB-COH / OS-id bridges)
      risk.py             Risk-signal rules — deterministic codes incl. AMLA CDD RTS + FATF
      cache.py            Two-tier cache (demos/ → live/)
      config.py           Pydantic settings; env vars listed in docs/configuration.md
    scripts/
      extract_bods_subgraphs.py    Walk local OO SQLite dumps → per-LEI BODS bundles
      extract_bce.py               Walk Belgian BCE/KBO open data ZIP → SQLite DB with FTS5 name index
      extract_brightquery.py       Walk BrightQuery bulk files → SQLite DB indexed by LEI
      build_geot_projects.py       Condense GEM GEOT xlsx ownership-closure sheets → geot_projects.json.gz
    tests/                pytest suite (1835 tests)
  frontend/               React + Vite + TypeScript + Tailwind + BO design system
    src/
      App.tsx             Three-tab search (company name / national ID / LEI), subject card, risk chips, export panel
      components/
        BODSGraph.tsx     Cytoscape.js ownership graph — BOVS icon overlays, flag badges, risk signal overlays (lazy-loaded)
      lib/
        api.ts            Typed client for the FastAPI surface
        raCodes.ts        RA codes, labels, placeholders, format regexes for 17 countries (Phase 50)
        gleifNationalId.ts  searchByNationalId() — parallel GLEIF filter queries for national ID lookup (Phase 50)
  docs/                   Supplementary documentation (this file + status, sources, risk-signals, configuration)
  data/
    cache/                Two-tier cache root (live/ + bods_data/ gitignored)
    demo/                 Curated 9-entity BODS subgraph for Neo4j demo (Phase 49)
  ATTRIBUTIONS.md         Per-source licensing
  LICENSE                 MIT (own code only — see ATTRIBUTIONS for source data)
```
