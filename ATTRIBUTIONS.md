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
