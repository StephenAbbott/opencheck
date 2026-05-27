# Attributions

OpenCheck retrieves data from the open data sources listed below. Each source has its own license. When OpenCheck presents data in the UI it attributes it to the originating source; when it exports data it writes a `LICENSE_NOTICE.md` listing every source that contributed to the export.

OpenCheck's own source code is MIT-licensed (see [`LICENSE`](LICENSE)).

## GLEIF — Global Legal Entity Identifier Foundation

- **Data:** LEI-CDF (Level 1) entity records; RR-CDF (Level 2) relationship records and reporting exceptions
- **API:** <https://api.gleif.org/>
- **License:** [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/)
- **Attribution:** "Contains LEI data from GLEIF, available under CC0 1.0."

## UK Companies House

- **Data:** company profiles, officers, Persons with Significant Control (PSC)
- **API:** <https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference>
- **License:** [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- **Attribution:** "Contains public sector information licensed under the Open Government Licence v3.0 (Companies House)."
- **Entry point:** `gb_coh` derived from GLEIF `registeredAs` field

## Brønnøysundregistrene — Norwegian Register Centre (Brreg)

- **Data:** company profiles and role-holders (board members, daily managers, contact persons, and other officers) from the Enhetsregisteret (Central Coordinating Register for Legal Entities)
- **API:** <https://data.brreg.no/enhetsregisteret/api/docs>
- **License:** [NLOD 2.0 — Norwegian Licence for Open Government Data](https://data.norge.no/nlod/en/2.0)
- **Attribution:** "Contains data from Brønnøysundregistrene via the Enhetsregisteret, licensed under NLOD 2.0."
- **Entry point:** `no_orgnr` (9-digit organisation number) derived from GLEIF RA code `RA000472`
- **Note:** Beneficial ownership data (reelle rettighetshavere) is restricted to users in Norway with legitimate interest and is not available via the public API.

## Companies Registration Office Ireland (CRO)

- **Data:** company profiles (name, status, type, registration date, address, NACE code) from the CRO Open Data Portal
- **Portal:** <https://opendata.cro.ie/>
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from the Companies Registration Office of Ireland, available under CC BY 4.0 via the CRO Open Data Portal (opendata.cro.ie)."
- **Entry point:** `ie_crn` (company registration number) derived from GLEIF RA code `RA000402`
- **Note:** Officer and director data is not available from the Open Data Portal tier. The CRO Open Services API (`services.cro.ie/cws`) provides richer data including officers, but requires an API key issued by the CRO.

## PRH — Patentti- ja rekisterihallitus (Finnish Patent and Registration Office)

- **Data:** entity records for all organisations registered in Finland, via the YTJ (Yritys- ja yhteisötietojärjestelmä / Business Information System) Open Data API
- **API:** <https://avoindata.prh.fi/en/ytj/swagger-ui>
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from Patentti- ja rekisterihallitus (PRH) / Finnish Patent and Registration Office, via the YTJ Open Data API (avoindata.prh.fi), licensed under CC BY 4.0."
- **Entry point:** `fi_ytunnus` (Y-tunnus / Finnish Business ID, format `XXXXXXX-X`) derived from GLEIF RA code `RA000188`
- **Note:** Beneficial ownership and officer data are not publicly available from PRH. The paid Virre Information Service provides role-holder records; OpenCheck currently maps entity data only.

## UR — Latvian Register of Enterprises (Uzņēmumu reģistrs)

- **Data:** entity profiles, beneficial owner declarations, officers (executive/supervisory board members, liquidators, and other representatives), SIA shareholders (LLC share-register entries), and historical names — sourced from five open datasets published on Latvia's national open data portal (data.gov.lv) by the Register of Enterprises (UR)
- **Portal:** <https://data.gov.lv/dati/lv/organization/ur>
- **API:** CKAN Datastore API on data.gov.lv (`https://data.gov.lv/dati/api/3/action/`) — all five datasets are live-queryable row-by-row without downloading the bulk CSV files
- **Resource IDs:**
  - Business register: `25e80bf3-f107-4ab4-89ef-251b5b9374e9`
  - Beneficial owners: `20a9b26d-d056-4dbb-ae18-9ff23c87bdee`
  - Historical names: `ad772b8b-76e4-4334-83d9-3beadf513aa6`
  - Officers: `e665114a-73c2-4375-9470-55874b4cfa6b`
  - Members (SIA shareholders): `837b451a-4833-4fd1-bfdd-b45b35a994fd`
- **License:** Open Government Data (PSI Directive / Latvian Public Information Law)
- **Attribution:** "Contains data from the Latvian Register of Enterprises (UR), open data published on data.gov.lv."
- **Entry point:** `lv_regcode` (11-digit Latvian registration number) derived from GLEIF RA code `RA000423`
- **Note:** Latvia was the first country to publish its national beneficial ownership data in BODS format (BODS v0.2, 2021); that historical dataset is available at <https://data.gov.lv/dati/lv/dataset/plg-bods>. The current live datasets do not carry percentage thresholds for beneficial owners — they record the declared UBO without specifying the interest mechanism. Individual identity numbers are partially masked (`DDMMYY-*****`).

## JAR — Juridinių Asmenų Registras (Lithuanian Register of Legal Entities)

- **Data:** entity name, code, address, legal form, and registration status for all entities registered in Lithuania — sourced from the public JAR search interface maintained by Registrų centras (Centre of Registers)
- **Interface:** Public HTML search at <https://www.registrucentras.lt/jar/p/>
- **Bulk download:** CC BY 4.0 daily CSV at <https://www.registrucentras.lt/aduomenys/?byla=JAR_IREGISTRUOTI.csv> (key fields: `jaAsm_Kodas`, `jaAsm_Pavadinimas`, `jaAsm_Adresas`, `jaAsm_FormKodas`, `jaAsm_StatusKodas`, `jaAsm_Reg`)
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from the Lithuanian Register of Legal Entities (JAR), published by Registrų centras, available under CC BY 4.0. Source: registrucentras.lt."
- **Entry point:** `lt_code` (9-digit Lithuanian entity code) derived from GLEIF RA code `RA000430` (Registrų centras)
- **Rate limit:** 100 public queries per IP address per day via the HTML search interface. Results are cached to stay within this limit.
- **Note on beneficial ownership:** Participant and shareholder data was formerly available via the JADIS open data system but is being migrated to JANGIS, a restricted sub-system accessible only to those with legitimate interest and not available as open data. This adapter covers entity data only; BO data is intentionally excluded.

## ARES — Administrativní registr ekonomických subjektů (Czechia)

- **Data:** entity name, IČO, address, legal form, registration status, shareholders (akcionáři / společníci), directors (statutární orgány), and share capital — sourced from the ARES REST API operated by the Czech Ministry of Finance. Aggregate data comes from the `/ekonomicke-subjekty/{ico}` endpoint; commercial-register (VR) data from `/ekonomicke-subjekty-vr/{ico}`.
- **API:** <https://ares.gov.cz/ekonomicke-subjekty-v-be/rest> — no authentication required
- **Open-data catalogue:** <https://data.mf.gov.cz/>
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from ARES (Administrativní registr ekonomických subjektů), published by the Ministry of Finance of Czechia (Ministerstvo financí ČR) under CC BY 4.0. Source: ares.gov.cz."
- **Entry point:** `cz_ico` (8-digit IČO, zero-padded) derived from GLEIF RA code `RA000163` (Obchodní rejstřík / Commercial Register, Ministry of Justice)
- **Note:** ARES aggregates data from multiple sub-registers: ROS (base register), VR (commercial register, Ministry of Justice), RES (statistical register), and RZP (trade licence register). Shareholder and director data is only available for entities registered in the VR; ARES returns 404 on the VR endpoint for entities not in the commercial register — handled gracefully. Beneficial ownership declarations are not available via the public API.

## Ariregister — Estonian e-Business Register

- **Data:** company profiles, shareholders, officers, and beneficial owners from the e-Business Register (äriregister) open data bulk files
- **Portal:** <https://avaandmed.ariregister.rik.ee/en>
- **License:** Open Data (PSI Directive / Estonian Public Information Act)
- **Attribution:** "Contains data from the Estonian e-Business Register (Äriregister), open data published by the Centre of Registers and Information Systems (RIK)."
- **Entry point:** Estonian registry code (8-digit) derived from GLEIF RA code `RA000181`
- **Note:** OpenCheck loads a local SQLite database built from the RIK bulk files. Activated when `ARIREGISTER_DB_FILE` is set.

## INPI — Registre National des Entreprises (France)

- **Data:** company profiles and officers from the French national business registry (Registre National des Entreprises / RNE)
- **API:** <https://api.inpi.fr/> (Infogreffe / INPI RNE API)
- **License:** Open (PSI Directive / Licence Ouverte Etalab)
- **Attribution:** "Contains data from the Registre National des Entreprises (INPI / Infogreffe), open data."
- **Entry point:** `siren` (9-digit SIREN number) derived from GLEIF RA code `RA000189`

## KvK — Kamer van Koophandel (Netherlands)

- **Data:** company details and authorised representatives from the Dutch Chamber of Commerce Handelsregister
- **API:** <https://developers.kvk.nl/>
- **License:** Open (PSI Directive / Dutch open data policy)
- **Attribution:** "Contains data from the Kamer van Koophandel (KvK) Handelsregister, open data."
- **Entry point:** `kvk_number` derived from GLEIF RA code `RA000463`

## Bolagsverket (Sweden)

- **Data:** company profiles and board-level officers from the Swedish Companies Registration Office
- **API:** <https://bolagsverket.se/apierochoppnadata>
- **License:** Open (PSI Directive / Swedish open data policy)
- **Attribution:** "Contains data from Bolagsverket (Swedish Companies Registration Office), open data."
- **Entry point:** `se_org_number` (10-digit Swedish organisation number) derived from GLEIF RA code `RA000544`

## Zefix — Central Business Name Index (Switzerland)

- **Data:** company profiles and authorised signatories from the Swiss central business name index
- **API:** <https://www.zefix.admin.ch/en/search/entity/welcome>
- **License:** Open (Swiss open government data)
- **Attribution:** "Contains data from Zefix (Swiss Federal Commercial Registry Office), open data."
- **Entry point:** `che_uid` (Swiss UID, format `CHE-XXX.XXX.XXX`) derived from GLEIF RA codes `RA000412` / `RA000548` / `RA000549`

## Firmenbuch — Austrian Commercial Register

- **Data:** company name, business address, entity status, and officers (managing directors, authorised signatories, supervisory board members, liquidators) from the Austrian commercial register (Firmenbuch), available as a High-Value Dataset (HVD) under EU open data legislation (EU Implementing Regulation 2023/138, Annex 5)
- **API:** Firmenbuch HVD SOAP 1.2 API at `https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws` — requires a free API key issued by Justiz Online
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from the Austrian commercial register (Firmenbuch), open data under CC BY 4.0, provided by the Austrian Federal Ministry of Justice."
- **Entry point:** `at_fn` (Firmenbuchnummer, e.g. `123456a`) derived from GLEIF RA code `RA000017`
- **Key registration:** Free API key available at <https://justizonline.gv.at/jop/web/iwg/register>. Set the `FIRMENBUCH_API_KEY` environment variable to enable live data. Without it, the adapter returns a stub entry.
- **Note — data scope:** The free HVD API key supports `UMFANG=Kurzinformation`, which returns company name, business address, and officers (managing directors / Geschäftsführer, authorised signatories / Prokuristen, supervisory board / Aufsichtsrat, liquidators). Shareholder data (Gesellschafter, Kommanditisten) and registered capital require `UMFANG=aktueller Auszug` or `historischer Auszug`, which need a paid Justiz Online subscription — OpenCheck does not currently support this. Austrian UBO declarations (wirtschaftliche Eigentümer) are held in the separate WiEReG register and are also not available via this API.

## RPO — Register právnických osôb (Slovak Register of Legal Persons)

- **Data:** entity name history, address history, IČO, establishment date, termination date, registration numbers, registration offices, and source register type (e.g. Obchodný register) — sourced from the Register of Legal Persons (RPO) REST API operated by the Statistical Office of the Slovak Republic (Štatistický úrad SR / ŠÚ SR)
- **API:** `https://api.statistics.sk/rpo/v1/search` — no authentication required
- **Portal:** <https://rpo.statistics.sk/>
- **Documentation:** <https://susrrpo.docs.apiary.io/>
- **Open data catalogue:** <https://data.slovensko.sk/datasety/b2325a3a-e702-47d0-8fa1-13739f3d2370>
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from the Slovak Register of Legal Persons (RPO), published by the Statistical Office of the Slovak Republic (ŠÚ SR) under CC BY 4.0. Source: rpo.statistics.sk."
- **Entry point:** `sk_ico` (8-digit IČO, zero-padded) derived from GLEIF RA code `RA000526` (Obchodný register SR / Slovak Commercial Register, Ministry of Justice)
- **Note on data scope:** The RPO API returns entity-level data only. Officers, shareholders, and beneficial ownership declarations are not available via this API. Beneficial ownership for companies supplying public bodies is covered separately by the RPVS adapter — see below.
- **Note on IČO:** Some entities in RPO have `identifiers[].value = "Neuvedené"` (not provided) — these are non-commercial entities (e.g. state organisations) registered before IČO assignment became mandatory. OpenCheck skips those entities as they cannot be cross-referenced by identifier.

## RPVS — Register partnerov verejného sektora (Slovak Public Sector Partners Register)

- **Data:** public-sector partner registrations and verified beneficial ownership (KUV — konečný užívateľ výhod) declarations for entities that supply goods, services, or other assets to Slovak public bodies above statutory value thresholds. The register covers the partner entity itself (name, IČO, validity dates) plus one or more beneficial owners per partner entry. KUV records include the person's name, date of birth, titles, a public-official flag (`JeVerejnyCinitel`), and the validity window of the declaration. Each entry is verified by an authorised person (a lawyer, notary, bank, or other qualifying entity recorded as `OpravnenaOsoba`).
- **API:** OData v4 at `https://rpvs.gov.sk/opendatav2/` — no authentication required
- **Swagger:** `https://rpvs.gov.sk/opendatav2/swagger/index.html`
- **Portal:** `https://rpvs.gov.sk/rpvs`
- **Publisher:** Ministry of Justice of the Slovak Republic (Ministerstvo spravodlivosti Slovenskej republiky)
- **License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Contains data from the Slovak Public Sector Partners Register (RPVS), published by the Ministry of Justice of the Slovak Republic under CC BY 4.0. Source: rpvs.gov.sk."
- **Entry point:** `sk_ico` (8-digit IČO, zero-padded) — the same identifier used by the RPO adapter, both derived from GLEIF RA code `RA000526`. RPVS and RPO are fetched independently when a Slovak entity is resolved; RPVS is only present in the response when the entity is (or was) registered in the RPVS.
- **BODS mapping:** each RPVS partner entry produces an entity statement (scheme `SK-RPVS`), a person or entity statement per active KUV, and an ownership-or-control relationship statement per KUV using interest type `unknownInterest` with `beneficialOwnershipOrControl: true`. The RPVS does not publish the specific ownership mechanism (share percentage, voting rights, etc.) — only the identity of the KUV — so `unknownInterest` is the appropriate BODS v0.4 type. KUV validity windows are mapped to `startDate`/`endDate` on the interest; `JeVerejnyCinitel` is mapped to the BODS `politicalExposure` block.
- **Coverage note:** Participation in the RPVS is mandatory for entities meeting the legal thresholds (broadly: contracts with public bodies worth ≥ €100,000 for single contracts, ≥ €250,000 for recurring arrangements, or receiving state aid or subsidies above €100,000). Voluntary registration is also permitted. The register was established under the Act on Register of Public Sector Partners (Act No. 315/2016) and is cross-referenced with the Slovak BODS dataset published by Open Ownership at `https://bods-data.openownership.org/source/slovakia/`.
- **Historical BODS mapping:** Open Ownership mapped an earlier version of this data to BODS v0.2 via the `register-ingester-sk` + `register-transformer-sk` pipeline. OpenCheck maps directly to BODS v0.4 from the live OData API.
- **Note on IČO uniqueness:** a single IČO may have multiple versioned partner entries (`PartneriVerejnehoSektora`) over time as the entity re-registers or updates its KUV declarations. OpenCheck resolves to the internal `CisloVlozky` (register entry number) and fetches all versioned entries, presenting only the currently active KUVs (`PlatnostDo == null`) in the BODS output.

## BCE / KBO — Belgian Crossroads Bank for Enterprises (Banque-Carrefour des Entreprises / Kruispuntbank van Ondernemingen)

- **Data:** entity name (Dutch/French/German), legal status, juridical form, start date, and registered-office address for Belgian enterprises. The BCE/KBO is the authoritative central business register for Belgium, maintained by the FPS Economy (FOD Economie / SPF Économie). It covers approximately 1.5 million enterprises.
- **Open data portal:** <https://kbopub.economie.fgov.be/kbo-open-data/>
- **Download:** Monthly ZIP (enterprise.csv, denomination.csv, address.csv, and others) at <https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/>
- **License:** KBO Reuse Licence — free reuse with attribution; non-commercial reuse requires notification to <kbo-bce-webservice@economie.fgov.be>; commercial reuse requires a formal agreement. Full licence text: <https://kbopub.economie.fgov.be/kbo-open-data/static/doc/Licentie/Licentie.pdf>
- **Attribution:** "Data from the Belgian Crossroads Bank for Enterprises (BCE/KBO), made available by the FPS Economy, SMEs, Self-Employed and Energy, Belgium."
- **Entry point:** `be_enterprise_number` (10-digit, no dots, e.g. `0433795975`) derived from GLEIF `registeredAs` field when `registeredAt.id == "RA000025"` (BCE/KBO GLEIF RA code). Belgian enterprise numbers are published in dotted format (`NNNN.NNN.NNN`) in GLEIF records; OpenCheck normalises them by stripping dots.
- **Activation:** build the local SQLite database with `python scripts/extract_bce.py --zip-file /path/to/KboOpenData_*.zip --output bce.db`, then set `BCE_BELGIUM_DB_FILE=/path/to/bce.db` in `.env`.
- **BODS mapping:** each BCE entity produces a single `entityStatement` with scheme `BE-BCE_KBO`, a derived Belgian VAT number identifier (`BE` + enterprise number, scheme `XI-VAT`), founding date from `StartDate`, and registered-office address. No beneficial ownership data is available from the BCE open data publication.
- **UBO register exclusion:** Belgium's UBO register (UBO-register / Registre UBO) is not included. It is not available as open data — access requires demonstrating legitimate interest and Belgium is in the process of legislating that public access will be limited to users with a justified legal basis (in line with AMLD6 implementation). This aligns with the C-601/20 CJEU judgment and the EU's sixth Anti-Money Laundering Directive.
- **Enterprise number format:** Belgian enterprise numbers are 10-digit numeric strings formatted as `NNNN.NNN.NNN` (e.g. `0433.795.975` for Ageas SA/NV). The first digit is always 0 for legal persons registered before 2023 and 1 for natural persons carrying on an enterprise. The enterprise number also serves as the base for the Belgian VAT number (prefix `BE`).
- **Name languages:** most Belgian enterprises have names in at least Dutch and French; enterprises in the German-speaking eastern cantons may also have German names. The adapter stores all three (`name_nl`, `name_fr`, `name_de`) and the FTS5 index covers all three columns, so name search works regardless of the language of the query.

## Corporations Canada — Innovation, Science and Economic Development Canada (ISED)

- **Data:** corporation records (name, status, act of incorporation, registered address, business number, directors) for companies incorporated under Canadian federal statutes including the Canada Business Corporations Act and the Boards of Trade Act
- **API:** ISED API Gateway — `https://apigateway-passerelledapi.ised-isde.canada.ca/corporations/api`
  - `GET /v1/corporations/<corpId>.json?lang=eng` — full corporation record
  - `GET /v2/corporations/<corpId>/directors.json?lang=eng` — current directors
- **License:** [Open Government Licence – Canada (OGL-Canada 2.0)](https://open.canada.ca/en/open-government-licence-canada)
- **Attribution:** "Contains information licensed under the Open Government Licence – Canada. Source: Corporations Canada, Innovation, Science and Economic Development Canada."
- **Entry point:** `ca_corp_id` (numeric corporation number) derived from GLEIF RA code `RA000072`
- **Key registration:** Requires `CORPORATIONS_CANADA_API_KEY` (public-plan API key from <https://api.ised-isde.canada.ca/corporations/api>)
- **Note:** The V1 API always returns HTTP 200 even for unknown corporations; OpenCheck detects not-found responses by checking whether the first element of the response array is a dict (found) or a string (error message). Directors are mapped to BODS v0.4 `seniorManagingOfficial` relationship statements.

## CVR — Det Centrale Virksomhedsregister (Danish Central Business Register)

- **Data:** company profiles (legal name, address, legal form, sector code, entity status, start date) for all businesses registered in Denmark. CVR is the authoritative statutory register maintained by Erhvervsstyrelsen (the Danish Business Authority). It covers approximately 900,000 active entities.
- **API:** Datafordeler GraphQL API at `https://graphql.datafordeler.dk/CVR/2.1` — authenticated via API key; free registration at <https://portal.datafordeler.dk/>
- **Portal:** <https://datacvr.virk.dk/> (public CVR browser); <https://datafordeler.dk/> (API distribution)
- **License:** Danish Open Government Data — CVR brugervilkår (terms of use). The CVR data may be used freely for any purpose with attribution; see <https://datacvr.virk.dk/artikel/vilkaar-og-betingelser> for the full terms. Commercial use is permitted with attribution.
- **Attribution:** "Indeholder data fra Det Centrale Virksomhedsregister (CVR), Erhvervsstyrelsen / Danish Business Authority. Data distribueret via Datafordelerens CVR GraphQL API."
- **Entry point:** `dk_cvr` (8-digit CVR number, zero-padded) derived from GLEIF RA code `RA000170` (Erhvervsstyrelsen)
- **Key registration:** Free API key from <https://portal.datafordeler.dk/>. Set the `CVR_DENMARK_API_KEY` environment variable to enable live data. Without it, the adapter returns a stub entry.
- **Technical notes:** The Datafordeler CVR API uses a bitemporal data model (`virkningFra` / `virkningTil` effect periods). OpenCheck filters to the current valid records at query time (Python-side, `virkningTil = null`). Fetch uses two sequential GraphQL queries: (1) `CVR_Virksomhed` by `CVRNummer` to obtain the internal `CVREnhedsId`; (2) a batch query for `CVR_Navn`, `CVR_Adressering`, `CVR_Branche`, `CVR_Virksomhedsform`, and `CVR_FuldtAnsvarligDeltagerRelation` (fully-liable participants, e.g. general partners in a K/S) using that ID.
- **Scope:** Natural persons (CVRPerson) require MitID Erhverv credentials and are not available via the standard API key. OpenCheck maps entity data only; beneficial ownership disclosures are not published via CVR open data.

## OpenCorporates

- **Data:** global company registry data — company profiles, registered addresses, officer appointments, and network relationships
- **API:** <https://api.opencorporates.com/>
- **License:** [OpenCorporates Terms and Conditions](https://opencorporates.com/info/terms-and-conditions) — free tier for non-commercial use; requires attribution
- **Attribution:** "Company data from OpenCorporates (opencorporates.com)."
- **Entry point:** `ocid` field on GLEIF Level 1 records, format `jurisdiction/company_number` (e.g. `gb/00102498`)

## BrightQuery (OpenData.org)

- **Data:** US company and executive data covering 185,000+ entities with LEIs
- **Source:** <https://opendata.org/>
- **License:** [ODC Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/)
- **Attribution:** "Contains data from BrightQuery / OpenData.org, licensed under ODC-By."
- **Entry point:** LEI direct lookup from a local SQLite database. Activated when `BRIGHTQUERY_DB_FILE` is set.

## SEC EDGAR (Schedule 13D / 13G)

- **Data:** major shareholders (>5 %) of US-listed companies from mandatory Schedule 13D and 13G XML filings (December 2024 onward)
- **Portal:** <https://www.sec.gov/cgi-bin/browse-edgar>
- **License:** Public Domain (US government works)
- **Attribution:** "Contains data from SEC EDGAR (U.S. Securities and Exchange Commission), public domain."
- **Entry point:** legal name search for US-jurisdiction entities; no API key required

## OpenSanctions

- **Data:** sanctions lists, PEPs, crime-linked entities, and cross-dataset references (including the OpenOwnership dataset and the GEM energy ownership dataset)
- **API:** <https://api.opensanctions.org/>
- **License:** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Attribution:** "Data from OpenSanctions.org, licensed CC BY-NC 4.0."
- **Entry point:** LEI string search

## EveryPolitician

- **Data:** current politicians and positions across 258 countries and territories, maintained by the OpenSanctions team via the Poliloom crowdsourcing tool
- **Website:** <https://everypolitician.org>
- **Poliloom launch article:** <https://www.opensanctions.org/articles/2026-03-24-poliloom/>
- **License:** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Attribution:** "EveryPolitician data, maintained by OpenSanctions. Licensed CC BY-NC 4.0."
- **Entry point:** name cross-check against BODS person statements derived from other sources

## Wikidata

- **Data:** structured entity data; used as the primary identifier bridge between sources via Q-IDs
- **API:** <https://query.wikidata.org/> (SPARQL) and <https://www.wikidata.org/w/api.php>
- **License:** [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) for structured data
- **Attribution:** "Wikidata structured data, CC0 1.0."
- **Entry point:** Q-ID resolved via SPARQL on property P1278 (LEI)

## OpenTender (DIGIWHIST)

- **Data:** public procurement tender data from 35 jurisdictions
- **Portal:** <https://opentender.eu/>
- **License:** [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)
- **Attribution:** "Procurement data from OpenTender (DIGIWHIST), licensed CC BY-NC-SA 4.0."
- **Entry point:** LEI string search

## Global Energy Monitor (GEM) / Climate TRACE

- **Data (GEM):** Ownership of fossil-fuel infrastructure assets worldwide — power plants, oil and gas fields, coal mines, pipelines, and related facilities. The ownership tracker (`all_entities.csv` and related files inside `ownership.zip`) maps facility owners to named legal entities with LEI codes where known. OpenCheck downloads this file at startup and uses it to bridge LEI → GEM entity ID, enabling ESG screening by LEI.
- **Data (Climate TRACE):** Satellite- and sensor-derived greenhouse gas emissions estimates per asset and per owner. OpenCheck queries the Climate TRACE API v7 to retrieve aggregate CO₂e (GWP 100-year) totals and sector-level breakdowns for any entity found in the GEM ownership index.
- **GEM ownership data:** <https://globalenergymonitor.org/> — `ownership.zip` published at <https://github.com/climatetracecoalition/climate-trace-tools/tree/main/climate_trace_tools/data/ownership>
- **Climate TRACE API:** <https://api.climatetrace.org/> — documentation at <https://api.climatetrace.org/docs>
- **License (GEM):** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **License (Climate TRACE):** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution:** "Fossil-fuel asset ownership data from Global Energy Monitor, CC BY 4.0. Emissions estimates from Climate TRACE, CC BY 4.0."
- **Entry point:** LEI matched against the GEM ownership index; Climate TRACE API queried for entities found in that index
- **Category:** ESG — this data is surfaced in a separate Environmental, Social, and Governance (ESG) Data panel in the OpenCheck UI, distinct from the customer due diligence sources above
- **Note:** OpenCheck uses the GEM ownership data directly under its CC BY 4.0 licence. The OpenSanctions GEM dataset is **not** used, as OpenSanctions applies a CC BY-NC 4.0 licence to all its datasets (including the GEM-derived one), which would restrict commercial use. The source file is re-downloaded on each server start because Render's filesystem is ephemeral.

## OpenAleph

- **Data:** investigative data collections (company registries, leaks, sanctions lists, court records)
- **Endpoint:** <https://search.openaleph.org/>
- **License:** per-collection (CC BY, CC BY-SA, or restricted). OpenCheck reads each collection's license metadata from the API and surfaces it on the source card.
- **Attribution:** per-collection, e.g. "Data from [collection name] in OpenAleph, licensed [license]."
- **Note:** The OpenAleph adapter is implemented but currently disabled in the registry. Its API is name-keyed rather than identifier-keyed, which does not fit the LEI-anchored flow cleanly yet.

---

## BODS — Beneficial Ownership Data Standard

Exports conform to the [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/), a project of Open Ownership.

---

If you export OpenCheck data that includes content derived from any CC BY-NC source (OpenSanctions, EveryPolitician, some OpenAleph collections) or the CC BY-NC-SA source (OpenTender), your use of that data is constrained by the non-commercial (and, for OpenTender, share-alike) license. OpenCheck warns you about this at export time.
