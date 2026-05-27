# OpenCheck — Sources

Twenty-six active adapters (plus one bulk-data adapter activated via env var), each implementing the same `SourceAdapter` protocol (`search`, `fetch`, `info`):

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
| `bce_belgium` | Belgian Crossroads Bank for Enterprises (BCE/KBO) | Custom-KBO-Reuse | `be_enterprise_number` from GLEIF (`RA000025`) | Belgian business register — entity name (NL/FR/DE), status, juridical form, start date, and registered address from a local SQLite database built from the monthly KBO open data ZIP. Supports name search via FTS5. Activated via `BCE_BELGIUM_DB_FILE` |
| `corporations_canada` | Corporations Canada (ISED) | OGL-Canada 2.0 | `ca_corp_id` from GLEIF (`RA000072`) | Canadian federal corporate registry — corporation details (name, status, act of incorporation, registered address, business number) and current directors via the ISED API Gateway. Directors mapped to BODS `seniorManagingOfficial` statements. Requires `CORPORATIONS_CANADA_API_KEY` |
| `cvr_denmark` | CVR — Det Centrale Virksomhedsregister | Danish Open Government Data (CVR brugervilkår) | `dk_cvr` from GLEIF (`RA000170`) | Danish Central Business Register — entity basics (name, address, legal form, sector, status) via the Datafordeler GraphQL API; bitemporal data filtered to current records; CVRPerson (natural persons) excluded; entity statements only with `DK-CVR` scheme. Requires `CVR_DENMARK_API_KEY` (free from portal.datafordeler.dk) |
| `ariregister` | Estonian e-Business Register (Ariregister) | Open (PSI) | registry code from GLEIF (`RA000181`) | Estonian commercial register — entity profile, officers, and beneficial owners via the live e-Business Register SOAP/XML API (`ariregxmlv6.rik.ee`). Requires `ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD` (free RIK contract) |
| `inpi` | INPI — Registre National des Entreprises | Open (PSI) | `fr_siren` from GLEIF | French national business registry — company profile, officers, and non-BO individual persons (full 65-code `roleEntreprise` codelist) via the RNE API; BO records excluded per Loi Sapin II |
| `kvk` | KvK — Handelsregister | Open (PSI) | `nl_kvk` from GLEIF | Netherlands Chamber of Commerce commercial register — company details and authorised representatives |
| `bolagsverket` | Bolagsverket | Open (PSI) | `se_org_number` from GLEIF | Swedish Companies Registration Office — company profile and board-level officers |
| `zefix` | Zefix | Open (PSI) | `ch_uid` from GLEIF | Switzerland central business name index — company profile and authorised signatories |
| `opencorporates` | OpenCorporates | OC Terms | `ocid` from GLEIF | Global company database — company profile, current officers, and network relationships as BODS statements |
| `sec_edgar` | SEC EDGAR (Schedule 13D/13G) | Public Domain | legal name search for US-jurisdiction entities | Major shareholders (>5 %) of US-listed companies from mandatory Schedule 13D and 13G XML filings. No API key required; coverage limited to filings from December 2024 onward |
| `opensanctions` | OpenSanctions | CC BY-NC 4.0 | LEI search | The open-source database of sanctions, watchlists, and politically exposed persons |
| `everypolitician` | EveryPolitician | CC BY-NC 4.0 | LEI search | Global database of political office-holders (served via OpenSanctions PEPs dataset) |
| `wikidata` | Wikidata | CC0-1.0 | Q-ID via SPARQL | A free and open knowledge base that can be read and edited by both humans and machines |
| `opentender` | OpenTender (DIGIWHIST) | CC BY-NC-SA 4.0 | LEI search | Search and analyse tender data from 35 jurisdictions |

## Notes

The OpenAleph adapter is implemented but currently disabled in `REGISTRY` — its API is name-keyed rather than identifier-keyed, which doesn't fit the LEI flow cleanly yet. Re-enable in `backend/opencheck/sources/__init__.py` once we have a curated demo set for it.

NC-licensed sources (OpenSanctions, EveryPolitician, OpenTender) propagate their share-alike / non-commercial obligations through `/deepen` and `/export`. The exported `LICENSES.md` warns reviewers before they re-publish.

Full per-source attribution and licence details are in [ATTRIBUTIONS.md](../ATTRIBUTIONS.md).
