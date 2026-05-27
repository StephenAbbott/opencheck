# OpenCheck — Development History

OpenCheck has shipped through forty-one phases. The latest commit on `main` is the source of truth.

| Phase | Headline |
|------:|----------|
| 0 | Scaffold — FastAPI + React/Vite + 6 stub source adapters |
| 1 | Live UK Companies House + BODS v0.4 mapper + SSE streaming |
| 2 | Live GLEIF + OpenSanctions + OpenAleph + FtM/GLEIF mappers |
| 3 | Live Wikidata + EveryPolitician + reconciler + risk signals (incl. AMLA CDD RTS) |
| 4 | Cache-first dispatch + bods-dagre visualisation |
| 5 | Export endpoint (JSON / JSONL / ZIP) + OpenTender (DIGIWHIST) procurement source |
| 6 | LEI-anchored `/lookup` flow + BO design system + bods-dagre fix |
| 7 | BO design system applied to the frontend (Bitter / DM Sans / DM Mono, navy banner, card grid) |
| 8 | Acronyms spelled out, OpenAleph disabled, sources moved to a separate page, README refresh |
| 9 | Tooling fixes — `@vitejs/plugin-react` v5 / vite 8 alignment, README phase recap |
| 10 | Open Ownership processed BODS bundles for UK PSC + GLEIF as the canonical source |
| 11 | Cross-check related-party names against OpenSanctions + EveryPolitician — `RELATED_PEP` / `RELATED_SANCTIONED` |
| 12 | OO bundle as LEI lookup entry point + example LEI picker |
| 13 | `.env` loading from project root + BODS graph statement sanitiser + title/homepage link |
| 14 | bods-dagre `Invalid argument expected string` fix |
| 15 | Extraction script walks by `recordId` (not `statementId`) for correct subgraph extraction |
| 16 | OpenCorporates adapter (OCID-bridged via GLEIF) + BODS dagre relationship-edge fix + GODIN ribbon + Render deployment |
| 17 | FATF black/grey-list jurisdiction signals (`FATF_BLACK_LIST` / `FATF_GREY_LIST`) derived from BODS entity statements |
| 18 | OpenCorporates officer mapping (full BODS v0.4 interestType codelist), network relationship support, OC Relationships bulk-file infrastructure; GLEIF exception field-name fix (`reason` / `exceptionReason`); BODS validator completed to full 24-code codelist |
| 19 | BrightQuery / OpenData.org adapter — 185k+ US entities with LEIs; extraction + diagnostic scripts; entity-level risk signals shown on all source card headers |
| 20 | UI: risk signals visible on every source card header without clicking Go deeper; deepen panel de-duplicated |
| 21 | National corporate registry adapters — INPI (France, `fr_siren`), KvK (Netherlands, `nl_kvk`), Bolagsverket (Sweden, `se_org_number`), Zefix (Switzerland, `ch_uid`) — each with full BODS v0.4 officer mapping |
| 22 | ICIJ Offshore Leaks name cross-check — batched reconciliation API, `OFFSHORE_LEAKS` signals scoped to matching BODS statement; no API key required |
| 23 | Estonian e-Business Register (Ariregister) adapter — full open data: entity basics, shareholders, officers, beneficial owners, all mapped to BODS v0.4; national registry code emitted with `EE-RIK` scheme |
| 24 | SEC EDGAR Schedule 13D/13G adapter — major shareholders (>5 %) of US-listed companies from mandatory XML filings (December 2024 onward); no API key required |
| 25 | Brønnøysundregistrene (Norway) adapter — entity data and role-holders (CEO, board, officers) from the public Enhetsregisteret API; BODS v0.4 role mapping; NLOD 2.0; no API key required |
| 26 | Companies Registration Office Ireland (CRO) adapter — entity data from the CRO Open Data Portal CKAN API; CC BY 4.0; no API key required |
| 27 | PRH (Finland) adapter — entity data from the YTJ Open Data API; CC BY 4.0; no API key required |
| 28 | UR Latvia adapter — entity profiles, beneficial owners, officers, and shareholders from Latvia's Register of Enterprises via the CKAN Datastore API on data.gov.lv; open government data; no API key required |
| 29 | JAR Lithuania adapter — entity data from Lithuania's Register of Legal Entities (Registrų centras) via the public JAR HTML search interface; `lt_code` derived from GLEIF RA code `RA000430`; CC BY 4.0; no API key required |
| 30 | ARES (Czechia) adapter — entity data, shareholders, directors, and share capital from the Czech business register via the ARES REST API; `cz_ico` derived from GLEIF RA code `RA000163`; CC BY 4.0; no API key required |
| 31 | KRS Poland adapter — entity data and officer/board information from the Polish National Court Register via the KRS API; `pl_krs` derived from GLEIF RA code `RA000484`; public data; no API key required |
| 32 | Austrian Firmenbuch adapter — company name, address, status, and officers from the Austrian commercial register HVD SOAP API; `at_fn` (Firmenbuchnummer) derived from GLEIF RA code `RA000017`; CC BY 4.0; requires free `FIRMENBUCH_API_KEY` |
| 33 | RPO Slovakia adapter — entity data from Slovakia's Register of Legal Persons (Register právnických osôb) via the ŠÚ SR REST API; `sk_ico` (IČO) derived from GLEIF RA code `RA000526`; CC BY 4.0; no API key required |
| 34 | RPVS Slovakia adapter — beneficial ownership declarations from the Slovak Public Sector Partners Register (Register partnerov verejného sektora) via the Ministry of Justice OData API; covers entities supplying public bodies above statutory thresholds with verified KUV (konečný užívateľ výhod) disclosures; also triggered by `sk_ico`; CC BY 4.0; no API key required |
| 35 | BCE Belgium adapter — Belgian Crossroads Bank for Enterprises (BCE/KBO) — entity basics (name, status, juridical form, start date, registered address) from a local SQLite database built from the monthly KBO open data ZIP; `be_enterprise_number` derived from GLEIF RA code `RA000025`; supports name search via FTS5; KBO reuse licence; no API key required; activated via `BCE_BELGIUM_DB_FILE` |
| 36 | Corporations Canada (ISED) adapter — corporation records (name, status, act, registered address, business number, directors) for federally incorporated Canadian companies via the ISED API Gateway; `ca_corp_id` derived from GLEIF RA code `RA000072`; directors mapped to BODS v0.4 `seniorManagingOfficial` statements; OGL-Canada 2.0; requires `CORPORATIONS_CANADA_API_KEY` |
| 37 | Ariregister (Estonia) rewritten to use the live SOAP/XML API (`ariregxmlv6.rik.ee`) — replaces the bulk-SQLite adapter; calls `detailandmed_v2` for entity + persons and `tegelikudKasusaajad_v2` for beneficial owners; credentials switched from `ARIREGISTER_DB_FILE` to `ARIREGISTER_USERNAME` / `ARIREGISTER_PASSWORD` (free RIK contract); `_ee_date()` extended to accept ISO dates returned by the live API |
| 38 | GLEIF: direct subsidiaries in BODS output — first page of Level 2 parent–child relationships emitted as child `entityStatement` + `relationshipStatement` (`appointmentOfBoard`, `beneficialOwnershipOrControl: false`); subsidiary count surfaced in UI below the GLEIF source card; Wikidata QID accuracy fix |
| 39 | Companies House: active directors → BODS — `officers.items[]` entries with a director role and no `resigned_on` are emitted as `personStatement` (knownPerson) + `relationshipStatement` (`seniorManagingOfficial`, `beneficialOwnershipOrControl: false`); officer ID extracted from `links.officer.appointments` for stable local IDs; secretary and resigned roles excluded |
| 40 | INPI France: non-BO individuals → BODS — `composition.pouvoirs[]` entries with `typeDePersonne == "INDIVIDU"` and `beneficiaireEffectif == false` are now mapped to person + relationship statements; full 65-code `roleEntreprise` codelist from INPI data dictionary embedded in mapper; external professional roles (auditors, liquidators, fiscal reps — codes 14, 71, 72, 77, 109, 150, 220) → `otherInfluenceOrControl`; all governance/management roles → `seniorManagingOfficial`; French label in `details`; `dateEffetRoleDeclarant` → `startDate`; `beneficiaireEffectif == true` entries silently skipped per Loi Sapin II / décret 2017-1094 |
| 41 | lib-cove-bods validation — `tests/test_bods_libcovebods.py` adds 23 tests running all mapper outputs through the BODS v0.4 JSON schema validator and additional quality checks (`libcovebods>=0.16`); fixes applied: `share.exclusiveMinimum` changed from boolean to numeric per JSON Schema Draft 2020-12; UK sub-regions (England/Scotland/Wales/Northern Ireland) mapped to `GB` in `_country_obj`; identifier scheme codes corrected to registered org.ids codes throughout (`NO-BRREG`→`NO-BRC`, `CZ-ARES`→`CZ-ICO`, `CH-UID`→`CH-FDJP`, `CH-ZEFIX`→`CH-COA`, `EE-ARIREGISTER`→`EE-KMKR`, `FR-SIREN`→`FR-INSEE`, `SG-UEN`→`SG-ACRA`, `OC-{jur}`→jurisdiction-specific org.ids code with `COA` fallback); GLEIF RA table France entry corrected (`RA000189` → `FR-INSEE`); corporate PSC `beneficialOwnershipOrControl` set to `false` when interested party is an entity (not a natural person) |

Test suite: 913 backend tests (4 skipped). Frontend type-checks clean.
