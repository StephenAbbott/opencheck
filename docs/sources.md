# OpenCheck — Sources

Thirty-eight active adapters, each implementing the same `SourceAdapter` protocol (`search`, `fetch`, `info`). Three further adapters are committed but inactive (bulk-data only) — see [Inactive / bulk-only adapters](#inactive--bulk-only-adapters) below.

| ID | Name | License | Entry point | Description |
|----|------|---------|-------------|-------------|
| `gleif` | GLEIF | CC0-1.0 | LEI | Legal entity information from the Global Legal Entity Identifier Foundation |
| `companies_house` | UK Companies House | OGL-3.0 | `gb_coh` from GLEIF | Legal and beneficial ownership information from the UK corporate registry |
| `brreg` | Brønnøysundregistrene (Norway) | NLOD-2.0 | `no_orgnr` from GLEIF (`RA000472`) | Norwegian central business register — company profile and role-holders (CEO, board, officers) from the public Enhetsregisteret REST API; no API key required |
| `cro` | Companies Registration Office Ireland | CC-BY-4.0 | `ie_crn` from GLEIF (`RA000402`) | Irish company register — entity details (status, type, registration date, address) from the CRO Open Data Portal CKAN API; no API key required |
| `prh` | PRH — Finnish Patent and Registration Office | CC-BY-4.0 | `fi_ytunnus` from GLEIF (`RA000188`) | Finnish company register — entity details from the YTJ Open Data API; officer data requires the paid Virre service; no API key required |
| `ur_latvia` | UR — Latvian Register of Enterprises | Open Government Data (PSI) | `lv_regcode` from GLEIF (`RA000423`) | Latvian business register — entity profile, beneficial owners, officers, shareholders, and historical names via the CKAN Datastore API on data.gov.lv; no API key required |
| `jar_lithuania` | JAR — Lithuanian Register of Legal Entities | CC-BY-4.0 | `lt_code` from GLEIF (`RA000430`) | Lithuanian company register — entity name, code, address, legal form, and registration status from the Registrų centras public JAR search; no API key required |
| `ares` | ARES (Czechia) | CC-BY-4.0 | `cz_ico` from GLEIF (`RA000163`) | Czech business register — entity basics, shareholders, directors, and share capital via the ARES REST API; no API key required |
| `krs_poland` | KRS — National Court Register (Poland) | Open (PSI) | `pl_krs` from GLEIF (`RA000484`) | Polish National Court Register — entity basics and board/officer data (names masked in public API) via the KRS REST API; no API key required |
| `firmenbuch` | Firmenbuch — Austrian Commercial Register | CC-BY-4.0 | `at_fn` from GLEIF (`RA000017`) | Austrian commercial register HVD — entity name, address, status, and officers (managing directors, signatories, supervisory board) via the Justiz Online SOAP API. Requires free `FIRMENBUCH_API_KEY` |
| `rpo_slovakia` | RPO Slovakia — Register právnických osôb | CC-BY-4.0 | `sk_ico` from GLEIF (`RA000526`) | Slovak Register of Legal Persons — entity name, address, establishment date, registration number, and court via the ŠÚ SR REST API; no API key required |
| `rpvs_slovakia` | RPVS Slovakia — Register partnerov verejného sektora | CC-BY-4.0 | `sk_ico` from GLEIF (`RA000526`) | Slovak Public Sector Partners Register — verified beneficial ownership (KUV) declarations for entities supplying public bodies above statutory thresholds, via the Ministry of Justice OData API; also triggered by `sk_ico` alongside RPO; no API key required |
| `corporations_canada` | Corporations Canada (ISED) | OGL-Canada 2.0 | `ca_corp_id` from GLEIF (`RA000072`) | Canadian federal corporate registry — corporation details (name, status, act of incorporation, registered address, business number) and current directors via the ISED API Gateway. Directors mapped to BODS `seniorManagingOfficial` statements. Requires `CORPORATIONS_CANADA_API_KEY` |
| `abr_australia` | Australian Business Register (ABN Lookup) | CC-BY-3.0-AU | `au_acn` from GLEIF (`RA000014`, ASIC) or `au_abn` (`RA000013`, ABR) | Australian company/business data — ABN, ACN, entity name and type, ABN/GST status, registered state and postcode, and business (trading) names — via the free ABN Lookup JSON web services (hourly-updated). Entity statements only; no officer or ownership data. Requires a free `ABN_GUID` |
| `cnpj_brazil` | Receita Federal — CNPJ register (Brazil) | Brazilian public open data | `br_cnpj` from GLEIF (`RA000681`) | Brazilian company register — Receita Federal CNPJ open data: entity details **plus the QSA** (Quadro de Sócios e Administradores — partners & administrators) mapped to BODS person/entity + ownership-or-control statements. Key-less, served via OpenCNPJ with a BrasilAPI fallback; no API key required |
| `cvr_denmark` | CVR — Det Centrale Virksomhedsregister | Danish Open Government Data (CVR brugervilkår) | `dk_cvr` from GLEIF (`RA000170`) | Danish Central Business Register — entity basics (name, address, legal form, sector, status) via the Datafordeler GraphQL API; bitemporal data filtered to current records; CVRPerson (natural persons) excluded; entity statements only with `DK-CVR` scheme. Requires `CVR_DENMARK_API_KEY` (free from portal.datafordeler.dk) |
| `sudreg_croatia` | Sudski registar — Croatian Court Register | HR Open Data (Otvorena dozvola) | `hr_mbs` from GLEIF (`RA000156`) | Croatian Court Register — entity basics (legal name, short name, legal form, status, founding date, registered seat, share capital) and `HR-MBS` + `HR-OIB` identifiers via the public `sudreg_javni` v3 JSON API (OAuth2 client credentials); officers and beneficial owners not published; entity statements only. Requires `SUDREG_CLIENT_ID` / `SUDREG_CLIENT_SECRET` (free from sudreg-data.gov.hr) |
| `malta_mbr` | Malta Business Registry (MBR) | CC-BY-4.0 | `mt_crn` from GLEIF (`RA000443`) | Maltese company register — core entity details (name, legal form, status, registered office, registration number and date) via the MBR Open Data API (`openapi.baros.mbr.mt`), an EU Open Data Directive High-Value Dataset; entity statements only (no officers or beneficial owners); no name search (entered via the LEI flow); no API key required |
| `gemi_greece` | ΓΕΜΗ — Greek General Commercial Registry | ODC-BY-1.0 | `gr_argemi` from GLEIF (`RA000685`) | Greek commercial register — entity details (name in Greek and Latin script, ΑΦΜ, legal form, status, registered office, incorporation date) **plus `persons[]`**: board members of an ΑΕ, and for ΙΚΕ / ΕΕ / ΟΕ / ΕΠΕ the partners with their percentage holdings. An ΑΕ publishes no shareholders — its share register is not part of ΓΕΜΗ publicity. Commercial register, not a beneficial ownership regime, so no `beneficialOwnershipOrControl` is asserted. Rate limited to 8 requests/minute. Requires a free `GEMI_API_KEY` |
| `mca_india` | Ministry of Corporate Affairs — Company Master Data (India) | GODL-India | `in_cin` from GLEIF (`RA000394`) | India's national company register extract on the OGD Platform (data.gov.in) — CIN, name, status, class/category, authorised & paid-up capital, registration date, RoC, registered office address and NIC classification for ~3.67M companies. Entity statements only; no officer or ownership data; exact-match search (names uppercase). Requires a free `DATA_GOV_IN_API_KEY` |
| `nz_companies` | New Zealand Companies Register (NZBN) | CC-BY-4.0 | `nz_company_number` from GLEIF (`RA000466`) | New Zealand company register via the NZBN API (Companies Office / MBIE). The company number resolves to the NZBN through the directory search, then the FullEntity endpoint returns entity details **plus directors** (`seniorManagingOfficial`), **shareholders with share allocations** (`shareholding` with `share.exact`) and the ultimate holding company — a real ownership graph with percentages. Requires a free `NZBN_API_KEY` (`Ocp-Apim-Subscription-Key`) |
| `cac_nigeria` | Nigeria CAC — Persons with Significant Control register | Public register (bor.cac.gov.ng) | LEI-keyed offline match; GLEIF RA `RA000469` | **Africa's first public beneficial ownership register** (Corporate Affairs Commission). **Curated example set** of 10 LEI-anchored Nigerian companies harvested from the CAC's public search register and committed at `data/cac_nigeria_psc.json` (built by `scripts/build_cac_nigeria_index.py`). Real beneficial ownership graphs — the five statutory CAMA PSC conditions map to `shareholding` / `votingRights` / `appointmentOfBoard` / `otherInfluenceOrControl`, with `beneficialOwnershipOrControl` asserted only for natural persons. **Offline / no live call** — the CAC's official API is restricted to Nigerian government agencies; a live adapter is deferred pending engagement with the CAC / Oasis Management. Asserts only the CAC-published RC number (`ng_cac_rc`), not the LEI (which OpenCheck derives via GLEIF). |
| `ariregister` | Estonian e-Business Register (Ariregister) | Open (PSI) | registry code from GLEIF (`RA000181`) | Estonian commercial register — entity profile, officers, shareholders, and beneficial owners via the public Ariregister website (`ariregister.rik.ee`); web scraper approach, no credentials required. Estonia's planned switch to legitimate-interest BO access was postponed on its 2026-07-10 start date — BO data remains available until a revised framework is adopted (no date announced) |
| `inpi` | INPI — Registre National des Entreprises | Open (PSI) | `fr_siren` from GLEIF | French national business registry — company profile, officers, and non-BO individual persons (full 65-code `roleEntreprise` codelist) via the RNE API; BO records excluded per Loi Sapin II |
| `kvk` | KvK — Handelsregister | Open (PSI) | `nl_kvk` from GLEIF | Netherlands Chamber of Commerce commercial register — company details and authorised representatives |
| `bolagsverket` | Bolagsverket | Open (PSI) | `se_org_number` from GLEIF | Swedish Companies Registration Office — company profile and board-level officers |
| `zefix` | Zefix | Open (PSI) | `ch_uid` from GLEIF | Switzerland central business name index — company profile and authorised signatories |
| `opencorporates` | OpenCorporates | OC Terms | `ocid` from GLEIF | Global company database — company profile, current officers, and network relationships as BODS statements |
| `openaleph` | OpenAleph (OCCRP Aleph) | Open (varies by dataset) | LEI → OC URL → registration numbers → legal name cascade | Open knowledge bases indexed by OCCRP's AlephData platform — entity records from investigative datasets, company registers, and document collections; 60 s timeout; no API key required |
| `sec_edgar` | SEC EDGAR (Schedule 13D/13G) | Public Domain | legal name search for US-jurisdiction entities | Major shareholders (>5 %) of US-listed companies from mandatory Schedule 13D and 13G XML filings. No API key required; coverage limited to filings from December 2024 onward |
| `opensanctions` | OpenSanctions | CC BY-NC 4.0 | LEI search | The open-source database of sanctions, watchlists, and politically exposed persons |
| `everypolitician` | EveryPolitician | CC BY-NC 4.0 | LEI search | Global database of political office-holders (served via OpenSanctions PEPs dataset) |
| `wikidata` | Wikidata | CC0-1.0 | Q-ID via SPARQL | A free and open knowledge base that can be read and edited by both humans and machines |
| `climatetrace` | Global Energy Monitor / Climate TRACE | CC-BY-4.0 | LEI | **ESG** — asset-level CO₂ emissions (Climate TRACE) plus energy and heavy-industry ownership reach (GEM Global Energy Ownership Tracker): the entity's direct and group-wide power/industrial assets and live projects with per-sector breakdowns; no API key required |
| `eiti` | EITI — Extractive Industries Transparency Initiative | EITI open data (attribution) | national registry number via GLEIF `registeredAt`/`registeredAs` (any of EITI's 65 implementing countries) | **ESG** — company-level payments to governments (taxes, royalties, licence fees) with GFS revenue classification, USD-normalised, per reporting year; organisation matching via the committed `eiti_organisations.json.gz` index (the API's identification filter is not implemented server-side), live payment rows from `/api/v2.0/revenue?organisation=`; no API key required |
| `eiti_soe` | EITI State-Owned Enterprises Database | EITI open data (attribution) | LEI matched against the committed SOE index | **CDD** — state-owned-enterprise flag and SOE context (sector, commodities, audited-financial-statement links, stock listings) for the ~100 SOEs reported through the EITI. Distinct from the `eiti` payments adapter; each SOE is resolved to an LEI at index-build time via GLEIF (`opencorporates_id` → reverse lookup, name+country fallback) by `backend/scripts/build_eiti_soe_index.py` → committed `eiti_soe_index.json.gz`. The BODS mapping emits a `stateBody` government + `controlByLegalFramework` relationship, which raises the `STATE_CONTROLLED` signal. The LEI is derived (not published by EITI), so it is not asserted as a cross-source identifier. No API key required |
| `eiti_bo` | EITI countries — national beneficial ownership registers | Public registers (per-register terms; DRC and Armenia state no licence — included with attribution) | LEI matched against the committed pooled index | **CDD** — beneficial ownership of extractive companies **pooled from the national BO registers of EITI implementing countries** (one source, not one adapter per register — the pooled universe is small, smaller still filtered to LEI holders). Registers at launch: **DRC** ITIE-RDC Registre des propriétaires effectifs (the only EITI BO register anywhere with a bulk download — XLSX export with ownership %, voting rights and PEP flags; Loi n°25/048 du 1 juillet 2025), **Armenia** State Register declarations at old.e-register.am (per-declaration **BODS v0.2 JSON**, upconverted to v0.4 with the originals recorded via annotations; seed list = EITI Armenia's 27 declaring metal-ore mining companies), and **Nigeria** (the `cac_nigeria` harvest filtered to NEITI solid-minerals-covered companies, filter evidence dated per record — the NEITI portal itself is frozen ~2023). Indonesia slot reserved (AHU API in maintenance). Excluded: Tajikistan (all-rights-reserved), Trinidad & Tobago (frozen ~2021). Harvested offline by `backend/scripts/build_eiti_bo_index.py` → committed `eiti_bo_index.json.gz`; **LEI-only at launch** — each company is resolved to an LEI at build time (registration-number equality against GLEIF first, e.g. Zangezur's `27.140.00009`; normalised-name equality as fallback), and unmatched companies stay in the committed raw harvests, counted in the artifact manifest. Asserts only register-published identifiers (`am_regnum`/`am_tin`/`ng_cac_rc`/`cd_nif`), never the derived LEI. Re-run the harvest subcommands + `build` to refresh; when the EITI open data portal (CKAN, with Datopian; due ~Sept 2026) launches, evaluate swapping per-register fetchers for portal sourcing. No API key required |
| `ted_eu` | TED — Tenders Electronic Daily (EU procurement) | EU open data (Decision 2011/833/EU, incl. commercial) | LEI + GLEIF `registeredAs` + derived national numbers via `organisation-identifier-tenderer` (eForms BT-501) on the TED Search API v3 | **CDD** — EU public procurement award notices where the entity appears as a tenderer/winner. One anonymous search POST (exact `IN()` identifier match; the `~` operator is unsupported on identifier fields), then the newest ≤10 notices are winner-confirmed from the eForms notice XML (`LotResult → LotTender → TenderingParty → Tenderer → CompanyID` chain) and labelled **won** / **tendered** / unconfirmed, with per-lot awarded values, award dates and contract references. **Coverage: eForms era only (≈2024 onwards)** — no history, so "no hits" ≠ "never won"; awards won via subsidiaries sit under the subsidiary's identifier. The LEI is always queried but its fill rate in BT-501 was zero as of 2026-08 (0 in 5,031 sampled identifiers) — recall comes from the national registration numbers; the lookup self-upgrades as LEI adoption in eForms grows. No API key required |
| `wikirate` | Wikirate | CC-BY-4.0 | LEI (Wikidata Q-ID fallback) via `filter[company_identifier[value]]` | **ESG** — open, community-researched corporate ESG metric answers (environment, human rights, supply chains, governance) from designers such as the World Benchmarking Alliance and Net Zero Tracker; OpenCheck shows totals + a sample of the most recent researched answers (sorted most-recent-year-first) and links out to wikirate.org for the full record; publishes LEI/Wikidata/OpenCorporates/CIK identifiers (strong cross-source corroborator; GODIN member); requires `WIKIRATE_API_KEY` (Cloudflare blocks anonymous server-side requests) |

## Inactive / bulk-only adapters

These adapters are committed and tested but **not exposed as live sources**. Each relies on bulk data files rather than a queryable API: the source is built into a local SQLite database and activated by an environment variable, so with no file configured (the production default) they return nothing. They will be turned on once OpenCheck adopts a bulk-data strategy, or once the source begins offering an API. ACRA and Cyprus go a step further than Belgium — they are **not registered in `REGISTRY` and not wired into the lookup dispatch at all**, so they never appear on `/sources`.

| ID | Name | License | Entry point | Status & description |
|----|------|---------|-------------|----------------------|
| `bce_belgium` | Belgian Crossroads Bank for Enterprises (BCE/KBO) | Custom-KBO-Reuse | `be_enterprise_number` from GLEIF (`RA000025`) | Registered + wired, but env-gated. Entity name (NL/FR/DE), status, juridical form, start date, registered address from a local SQLite DB built from the monthly KBO open data ZIP; FTS5 name search. Activate via `BCE_BELGIUM_DB_FILE` |
| `acra_singapore` | Singapore ACRA Business Registry | Singapore-OGL-1.0 | jurisdiction `SG` from GLEIF (`RA000509`) | Not in `REGISTRY`, not wired. Entity data (UEN, name, status, type, registration date, address) from the data.gov.sg monthly CSVs, built into a local SQLite DB; entity statements only. Activate via `ACRA_SINGAPORE_DB_FILE` |
| `cyprus_drcor` | Cyprus DRCOR — Registrar of Companies | CC-BY-4.0 | `cy_he` from GLEIF (`RA000161`) | Not in `REGISTRY`, not wired. Organisations, registered office, and officials (directors/secretaries; no shareholders) from three monthly data.gov.cy CSVs, built into a local SQLite DB via `scripts/extract_cyprus.py`; entity + officer statements. data.gov.cy exposes no query API (`/api/1/datastore/query` returns 404). Activate via `CYPRUS_DRCOR_DB_FILE` |

## Signpost sources (not mapped to BODS)

These are **not** `SourceAdapter`s and are deliberately excluded from the active-adapter count above. They contribute no BODS statements or graph nodes; instead they flag the availability of richer data elsewhere and link out to it. They match on the subject LEI and render at the bottom of the results page, beneath the data-source cards and the ESG box.

| ID | Name | License | Entry point | Description |
|----|------|---------|-------------|-------------|
| `meip` | OECD-UNSD Multinational Enterprise Information Platform | [OECD Terms](https://www.oecd.org/termsandconditions/) (attribution) | LEI matched against the vendored Global Register | **Signpost.** The annual "Global Register" of the 500 largest MNEs and their 126k+ subsidiaries. When the subject LEI is one of the ~30k LEI-carrying subsidiaries (or one of the 500 MNE heads), OpenCheck shows a card with the subsidiary/parent-MNE context, alternative names, address, and cross-reference identifiers (LEI, OpenCorporates, Refinitiv PermID, S&P Capital IQ — corroborated against GLEIF's own), and links users to the OECD site to download and reuse the full dataset. Vendored tables built from the annual CSV by `backend/scripts/build_meip.py`; no live API. |

## Data currency

OpenCheck republishes other people's data, so every source card and every BODS
statement declares how current its payload actually is. The BODS
[dates guidance](https://standard.openownership.org/en/main/standard/modelling/dates-guidance.html)
is firm about this: `source.retrievedAt` applies "only where data is being
republished", and a republisher **must** state when it downloaded the data.

Liveness is resolved per fetch, not declared per adapter — a live-capable
adapter serving a cached response is `cached`, not `live` — and a lookup that
mixes cached and fresh requests reports the **worst** liveness and the
**oldest** retrieval time, because a bundle is only as fresh as its stalest
component.

| Mode | What it means | `retrievedAt` |
|------|---------------|---------------|
| `live` | Fetched from the source during this lookup | the HTTP fetch time |
| `cached` | Served from OpenCheck's response cache (`data/cache/live/`) | when the cache entry was written |
| `snapshot` | Read from a bulk dataset (`bods_uk_psc`, `bods_gleif`, and pre-extracted Open Ownership subgraphs) | the dataset's own publication date, or the local extract's date |
| `curated` | A fixture committed to the repository (`cac_nigeria`, `eiti_bo`, demo fixtures) | the declared harvest date, else **omitted** |
| `stub` | Placeholder data — no source was contacted | **omitted entirely** |

Two omissions are deliberate. **Stub output never claims a retrieval time**: a
placeholder must not carry provenance. And a **committed fixture with no
declared harvest date claims none either** — a checked-out file's mtime records
when git wrote it to that machine, which says nothing about when the data left
the register. `cac_nigeria` and `eiti_bo` are the exceptions that prove the rule: their
indexes declare genuine harvest dates (`meta.harvested` / `meta.built`), so
they report one.

Sources that are structurally never live: `cac_nigeria` (curated example set —
the CAC's official API is restricted to Nigerian government agencies),
`eiti_bo` (pooled offline harvest of the DRC, Armenia and Nigeria registers —
none offers an API),
`bods_uk_psc` and `bods_gleif` (bulk Parquet), and the pre-extracted Open
Ownership subgraphs that `gleif` and `companies_house` serve as canonical
output. Everything else is live-capable and degrades to `cached` or `stub`
depending on configuration.

The frontend badges anything that is not a fresh live call; `live` is the
unmarked default, since badging it would tell a reader nothing they had not
already assumed. The API carries the same information in `source_liveness`
(keyed by `source_id`) and on each hit's `liveness` / `retrieved_at`.

### Beneficial ownership is asserted, never inferred

BODS distinguishes three states for `interests[].beneficialOwnershipOrControl`:
`true`, `false`, and **absent** — "not stated". OpenCheck emits the flag only
where a source actually said something.

A shareholding is a *legal* holding. Whether it is also a *beneficial* one is a
separate fact, and only a register or a beneficial ownership declaration regime
can supply it. Sources that do, and may therefore assert the flag:
`companies_house` (PSC), `bods_uk_psc`, `bods_gleif`, `ur_latvia`,
`rpvs_slovakia`, `cac_nigeria`, `eiti_bo`, `ariregister`. Everything else describes
registered holdings, and omits the flag unless the source states it explicitly —
an explicit `false` is information, not silence, and is always passed through.

SEC EDGAR is the interesting case. A 13D/13G "beneficial owner" is an SEC-rules
term meaning voting or dispositive power: an investment adviser voting client
shares has it without any economic interest. The filing's
`typeOfReportingPerson` code distinguishes the two, so filers reporting in a
custodial or advisory capacity (`IA`, `BD`, `IC`, `EP`, `SA`, `BK`) make no
beneficial ownership claim, and the capacity is stated in the interest's
`details` rather than silently dropped.

Over-claiming here is the wrong direction of error for a transparency tool: it
is a reputational assertion about a named person, and it travels into every
export — RDF, FtM, Senzing, BigQuery — well beyond any caveat the interface can
attach.

### Which date goes where

Full detail, including how date precision is recorded, is in [Dates](dates.md).

Four clocks, kept apart:

| Field | Question it answers | Where it comes from |
|-------|---------------------|---------------------|
| `interests[].startDate` / `endDate` | When was it true? | the register |
| `statementDate` | When did the source declare it? | the register's own declaration date where published, else the retrieval date, else today |
| `source.retrievedAt` | When did OpenCheck download it? | the liveness table above |
| `publicationDetails.publicationDate` | When did OpenCheck publish this statement? | today |

Sources publishing a declaration date OpenCheck uses today:

| Source | Field | Note |
|--------|-------|------|
| `gleif` | `registration.lastUpdateDate` | Moves with each LEI record update; also used for the Level 2 relationship statements the subject reports |
| `companies_house` | PSC `notified_on`, or `ceased_on` for a closed record | A closed record asserts "this ended", declared at cessation |
| `sec_edgar` | 13D/13G filing date | Issuer details use the most recent filing |
| `bods_gleif`, `bods_uk_psc` | Open Ownership's own `statementDate` | Passed through verbatim from the bulk Parquet — re-deriving it would replace a real declaration date with our processing date |
| `krs_poland` | `dataOstatniegoWpisu` | "Date of the last entry" in the court register. Not `dataRejestracjiWKRS`, the original registration |
| `ur_latvia` | officer `last_modified_at`, else `registered_on` | When UR last revised the officer record |
| `ares` | `datumAktualizace` | When ARES last refreshed the record. Not `datumVzniku`, the founding date |
| `brreg` | `rollegrupper[].sistEndret` | When Enhetsregisteret last changed that group of roles |
| `ted_eu` | latest notice `publication-date` | TED's publication of the notice is the declaration |

Everything else falls back to the retrieval date.

Three registers were investigated and have **no** usable declaration date, recorded here so the question does not get re-opened: **Estonia** (the scraped page exposes only a founding date), **Denmark** (Datafordeler CVR is bitemporal, but the adapter's queries request only `virkningFra`/`virkningTil` — validity time, not transaction time), and **Brazil** (probed live against both OpenCNPJ and BrasilAPI; every `data_*` field is an event or founding date, and `data_situacao_cadastral` is the status-effective date, not a declaration). Note that an interest's
*start* date is not a declaration date: a director appointed in 1998 was not
declared in 1998 and certainly was not published by OpenCheck in 1998, so
`appointed_on` (Companies House officers) and the INPI role start date appear
only as `interests[].startDate`.

## Notes

NC-licensed sources (OpenSanctions, EveryPolitician) propagate their non-commercial obligations through `/deepen` and `/export`. The exported `LICENSES.md` warns reviewers before they re-publish. (OpenTender / DIGIWHIST procurement was retired and its code removed — the live `ted_eu` adapter answers the EU-procurement question against TED directly, under a licence that permits commercial reuse.)

Full per-source attribution and licence details are in [ATTRIBUTIONS.md](../ATTRIBUTIONS.md).
