# Attributions

OpenCheck retrieves data from the open data sources listed below. Each source has its own license. When OpenCheck presents data in the UI it attributes it to the originating source; when it exports data it writes a `LICENSE_NOTICE.md` listing every source that contributed to the export.

OpenCheck's own source code is MIT-licensed (see [`LICENSE`](LICENSE)).

## UK Companies House

- **Data:** company profiles, officers, Persons with Significant Control (PSC)
- **API:** <https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference>
- **License:** [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- **Attribution:** "Contains public sector information licensed under the Open Government Licence v3.0 (Companies House)."

## GLEIF — Global Legal Entity Identifier Foundation

- **Data:** LEI-CDF (Level 1) entity records; RR-CDF (Level 2) relationship records and reporting exceptions
- **API:** <https://api.gleif.org/>
- **License:** [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/)
- **Attribution:** "Contains LEI data from GLEIF, available under CC0 1.0."

## OpenCorporates

- **Data:** global company registry data — company profiles, registered addresses, officer appointments
- **API:** <https://api.opencorporates.com/>
- **License:** [OpenCorporates Terms and Conditions](https://opencorporates.com/info/terms-and-conditions) — free tier for non-commercial use; requires attribution
- **Attribution:** "Company data from OpenCorporates (opencorporates.com)."
- **Note:** OpenCheck reaches OpenCorporates via the `ocid` field on GLEIF Level 1 records, which uses the format `jurisdiction/company_number` (e.g. `gb/00102498`). This bridges the LEI namespace directly to the OpenCorporates REST API.

## OpenSanctions

- **Data:** sanctions lists, PEPs, crime-linked entities, cross-dataset references (including the OpenOwnership dataset and the GEM energy ownership dataset)
- **API:** <https://api.opensanctions.org/>
- **License:** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Attribution:** "Data from OpenSanctions.org, licensed CC BY-NC 4.0."

## OpenAleph

- **Data:** investigative data collections (company registries, leaks, sanctions lists, court records)
- **Endpoint:** <https://search.openaleph.org/>
- **License:** per-collection (CC BY, CC BY-SA, or restricted). OpenCheck reads each collection's license metadata from the API and surfaces it on the source card.
- **Attribution:** per-collection, e.g. "Data from [collection name] in OpenAleph, licensed [license]."

## EveryPolitician

- **Data:** current politicians and positions across 258 countries and territories, maintained by the OpenSanctions team via the Poliloom crowdsourcing tool
- **Website:** <https://everypolitician.org>
- **Poliloom launch article:** <https://www.opensanctions.org/articles/2026-03-24-poliloom/>
- **License:** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Attribution:** "EveryPolitician data, maintained by OpenSanctions. Licensed CC BY-NC 4.0."

## Wikidata

- **Data:** structured entity data; used as the primary identifier bridge between sources via Q-IDs
- **API:** <https://query.wikidata.org/> (SPARQL) and <https://www.wikidata.org/w/api.php>
- **License:** [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) for structured data
- **Attribution:** "Wikidata structured data, CC0 1.0."

## BODS — Beneficial Ownership Data Standard

Exports conform to the [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/en/0.4.0/), a project of Open Ownership.

---

If you export OpenCheck data that includes content derived from any CC BY-NC source (OpenSanctions, EveryPolitician, some OpenAleph collections), your use of that data is constrained by the non-commercial license. OpenCheck warns you about this at export time.
