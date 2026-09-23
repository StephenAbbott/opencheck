# OpenCheck — Sources

Forty-eight active adapters, each implementing the same `SourceAdapter` protocol (`search`, `fetch`, `info`). Four further adapters are committed but inactive (bulk-data only) — see [Inactive / bulk-only adapters](#inactive--bulk-only-adapters) below.

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
| `cr_hongkong` | Hong Kong Companies Registry | DATA.GOV.HK-Terms | `hk_brn` from GLEIF (`RA000388` Companies Registry, `RA000389` Business Registration Office) | Hong Kong company register — core entity details for **live local companies** (English and Chinese names, Business Registration Number / Unique Business Identifier, company type, registered office, incorporation and re-domiciliation dates) via the Companies Registry's open data API on DATA.GOV.HK (`data.cr.gov.hk`). Entity statements only (no officers, secretaries, shareholders or beneficial owners); the BRN is identified as `HK-BRN` and the Chinese name goes to `alternateNames`. `fetch` drops the record when the register's names contradict GLEIF's legal name (simplified/traditional Chinese folded with OpenCC first). Name search is prefix-only; a miss is "not in the live register" and nothing more. Non-Hong Kong companies are not covered. No API key required |
| `acra_singapore` | Singapore ACRA — Accounting and Corporate Regulatory Authority | Singapore-OGL-1.0 | `sg_uen` from GLEIF (`RA000523`) | Singapore entity register via data.gov.sg `datastore_search`, looked up by **Unique Entity Number** (no name matching). Two collections: the UEN register (*Entities Registered with ACRA*, plus *Other UEN Issuance Agencies* for variable capital companies) and *ACRA Information on Corporate Entities* (company type, detailed status, incorporation date, full address, former names 1–15), whose 27 per-letter datasets are resolved from collection metadata at run time and routed by the register's own current name. Entity statements only (no officers, shareholders or beneficial owners); `SG-ACRA` scheme; former names in `alternateNames`; the register's type in `entityType.details`. `fetch` drops a record whose current and former names all contradict GLEIF's legal name; VCC sub-funds (`…-SF001`) are not published and are skipped. No name search. Works without a key (4 requests per 10 s); optional `DATA_GOV_SG_API_KEY` (`x-api-key`) raises it to 8 |
| `anaf_romania` | ANAF — Agenția Națională de Administrare Fiscală (Romania) | No stated licence — public register, attribution requested | `ro_fiscal_or_reg_id` from GLEIF (`RA000497` ONRC, `RA000719` Tax Payer Register) | Romanian taxpayer register via ANAF's keyless public web service (`POST webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva`), keyed on the **fiscal code (CUI)**: name, registered office and fiscal domicile, trade-register number, legal form, CAEN code, registration date, VAT registration, the inactive-taxpayer register (including the striking-off date) and RO e-Factura status. Entity statements only — ANAF publishes no people. **The dispatch key may be a CUI or an ONRC registration number**, because `RA000497` files either with nothing to mark which (463 of 1,140 sampled records carry a CUI); a registration number is resolved to a fiscal code through the `onrc_romania` index, and without that index the card carries a coverage note rather than looking empty. ANAF's own limits are 100 codes per request and 1 request per second, both enforced in the adapter. ANAF states no data licence: OpenCheck treats this as public-register fact, cites ANAF, and does not republish it in bulk. No name search; no API key |
| `onrc_romania` | ONRC — Oficiul Național al Registrului Comerțului (Romania) | CC-BY-4.0 | `ro_fiscal_or_reg_id` from GLEIF (`RA000497` ONRC, `RA000719` Tax Payer Register), shared with `anaf_romania` | Romanian Trade Register — the company (name, both registration-number formats, legal form, registered office, status) and its **legal representatives** with the register's own role wording and their filed dates of birth, from a local SQLite index built by `scripts/build_onrc_romania_index.py` off the monthly data.gov.ro dump. **Scoped to the GLEIF Romanian LEI population by default**: the register holds 2,855,557 companies and 9,033 Romanian LEI records exist, so the index keeps the 8,491 companies a lookup can actually reach — 4.3 MB instead of 1.2 GB. Sole traders (PFA, II, PF, AF, IF) are excluded as natural persons. No shareholders and no beneficial owners: both sit behind ONRC's paid *certificat constatator*. **This index is also what makes `anaf_romania` reach most Romanian LEI holders** — it maps a trade-register number to the fiscal code ANAF is keyed on, taking resolution from 47.8% to 97.7% (measured over all 8,677 dispatchable records, 15 Sep 2026). data.gov.ro drops connections from datacentre ranges, so the index is built off-datacentre and shipped in. Without `ONRC_ROMANIA_DB_FILE` the source is not announced at all; with it but no row for the company, a coverage note. Name search over the index; no API key |
| `asp_moldova` | ASP — State Register of Legal Entities (Moldova) | DATASET.GOV.MD-Reuse | `md_idno` from GLEIF (`RA000451` State Register, `RA000950` CNPF, `RA000951` National Bank — all three file the IDNO) | Moldova's State Register of Legal Entities (RSUD), published by the Agenția Servicii Publice as a **weekly full XLSX snapshot** on dataset.gov.md (about 38 MB and 300,000 rows; no query API). The adapter indexes it into SQLite itself: on a live deployment it resolves the newest resource through CKAN `package_show`, downloads it and builds the index at startup and whenever the snapshot is more than a week old (the portal does not block datacentre networks). Entity details (name, IDNO, legal form, registration date, address, activity codes), **directors** typed by their filed role (Administrator → `seniorManagingOfficial`, Comitet / Direcţie de conducere → `boardMember`, liquidators and insolvency administrators → `controlByLegalFramework`) and **founders with their percentage of the capital** (`shareholding` with `share.exact`). Names only — no dates of birth or identifiers — and no beneficial owners; joint-stock companies file no founders. A corporate founder is identified (`MD-IDNO`) only when the register's text names its IDNO or its folded name matches exactly one company in the export, and a name match is annotated as OpenCheck's work. Liquidated companies, sole traders and peasant farms are not indexed, so an IDNO for a liquidated company gets a coverage note, not a record. The dataset states no licence; the portal's reuse conditions permit commercial reuse. Name search over the index; no API key |
| `apr_serbia` | APR — Business Registers Agency company register (Serbia) | SODL-1.0 | `rs_mb` from GLEIF (`RA000517` Business Registers Agency) | Serbia's register of companies (*Регистар привредних друштава*), published by the Agencija za privredne registre through its keyless open-data API at `openapi.apr.gov.rs` — **bulk only**: one `GET` returns the whole register as a single JSON object (about 58 MB and 134,000 companies, cut on the last day of each month); query parameters are ignored and there is no per-company path. The adapter indexes it into SQLite itself: on a live deployment it downloads the register and builds the index at startup, and once the cut it holds is a month old it reads the opening bytes of APR's response for the cut date and rebuilds only when it is newer (APR does not block datacentre networks). The JSON is read as a stream, so memory stays flat. APR's server does not send its intermediate TLS certificate; the adapter pins it and keeps verification on. Entity statements only: business name as registered (Cyrillic or Latin; a Cyrillic name adds its Serbian Latin form to `alternateNames`), matični broj as `RS-APR`, legal form in `entityType.details`, founding date, and the municipality as the registered address — APR publishes no street address. **No people**: no directors, shareholders or beneficial owners. All four published statuses are indexed; deleted companies and entrepreneurs are not in the feed, so a number for either gets a coverage note, not a record. The financial-statements and NGO feeds are not indexed. Name search over the index, in either script; no API key |
| `dlcp_dc` | DLCP — Department of Licensing and Consumer Protection Corporations Division (Washington, DC) | CC-BY-4.0 | `us_dc_file_number` from GLEIF (`RA000601`) | Washington DC company register on one keyless ArcGIS FeatureServer at maps2.dcgis.dc.gov — Corporate Registration (504,036 rows), Trade Name (62,088) and Beneficial Owners (491,151), joined on the file number. Entity basics, registered agent, trade names as `alternateNames`, and the **owners and controllers filed on the biennial report** under D.C. Code § 29-102.11(a)(6). That disclosure tests a **governance or distributional** interest above 10 %, or a control test below it, and DLCP publishes **no role and no percentage** — so every interest is `unknownInterest` / `directOrIndirect: unknown` with no share, and a nonprofit's whole board appears in it. `beneficialOwnershipOrControl` is asserted for a natural-person owner and false for a corporate one (the owner type is not published; the adapter classifies each row from the legal form in its name). The file number GLEIF files is name-gated before the record is attached — GLEIF's `L21249` reaches a different DC company — with a single-unambiguous-active-match fallback on the name. No API key |
| `gemi_greece` | ΓΕΜΗ — Greek General Commercial Registry | ODC-BY-1.0 | `gr_argemi` from GLEIF (`RA000685`) | Greek commercial register — entity details (name in Greek and Latin script, ΑΦΜ, legal form, status, registered office, incorporation date) **plus `persons[]`**: board members of an ΑΕ, and for ΙΚΕ / ΕΕ / ΟΕ / ΕΠΕ the partners with their percentage holdings. An ΑΕ publishes no shareholders — its share register is not part of ΓΕΜΗ publicity. Commercial register, not a beneficial ownership regime, so no `beneficialOwnershipOrControl` is asserted. Rate limited to 20 requests/minute. Requires a free `GEMI_API_KEY` |
| `mca_india` | Ministry of Corporate Affairs — Company Master Data (India) | GODL-India | `in_cin` from GLEIF (`RA000394`) | India's national company register extract on the OGD Platform (data.gov.in) — CIN, name, status, class/category, authorised & paid-up capital, registration date, RoC, registered office address and NIC classification for ~3.67M companies. Entity statements only; no officer or ownership data; exact-match search (names uppercase). Requires a free `DATA_GOV_IN_API_KEY` |
| `nz_companies` | New Zealand Companies Register (NZBN) | CC-BY-4.0 | `nz_company_number` from GLEIF (`RA000466`) | New Zealand company register via the NZBN API (Companies Office / MBIE). The company number resolves to the NZBN through the directory search, then the FullEntity endpoint returns entity details **plus directors** (`seniorManagingOfficial`), **shareholders with share allocations** (`shareholding` with `share.exact`) and the ultimate holding company — a real ownership graph with percentages. Requires a free `NZBN_API_KEY` (`Ocp-Apim-Subscription-Key`) |
| `cac_nigeria` | Nigeria CAC — Persons with Significant Control register | Public register (bor.cac.gov.ng) | LEI-keyed offline match; GLEIF RA `RA000469` | **Africa's first public beneficial ownership register** (Corporate Affairs Commission). **Curated example set** of 30 LEI-anchored Nigerian companies harvested from the CAC's public register API (`borapp.cac.gov.ng/api/v1`, re-harvested 2026-09-16 after the site's redesign) and committed at `data/cac_nigeria_psc.json` (`scripts/build_cac_nigeria_index.py harvest` then `build`). Every PSC filing is kept with its register status: an owner with no ACTIVE filing becomes a `closed` relationship (no end date — the register publishes none), and a filing with every name field blank becomes an `unknownEntity` "Unnamed corporate owner" rather than being dropped (NNPC Ltd's two 50% holders among them). The harvester copies an allowlist of fields, so the emails, phone numbers, full dates of birth and identity numbers the register's API returns are never committed. Real beneficial ownership graphs — the five statutory CAMA PSC conditions map to `shareholding` / `votingRights` / `appointmentOfBoard` / `otherInfluenceOrControl`, with `beneficialOwnershipOrControl` asserted only for natural persons. **Offline / no live call** — the CAC's official API is restricted to Nigerian government agencies; a live adapter is deferred pending engagement with the CAC / Oasis Management. Asserts only the CAC-published RC number (`ng_cac_rc`), not the LEI (which OpenCheck derives via GLEIF). |
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
| `eiti` | EITI — Extractive Industries Transparency Initiative | EITI open data (attribution) | national registry number via GLEIF `registeredAt`/`registeredAs` (any of EITI's 65 implementing countries); **US** via a derived `us_ein` | **ESG** — company-level payments to governments (taxes, royalties, licence fees) with GFS revenue classification, USD-normalised, per reporting year; organisation matching via the committed `eiti_organisations.json.gz` index (the API's identification filter is not implemented server-side), live payment rows from `/api/v2.0/revenue?organisation=`. **US matching is the one country that cannot key on `registeredAs`**: EITI's US identifications are federal EINs and GLEIF publishes a US `registeredAs` as the state file number, so `_build_derived()` supplies `us_ein` from the committed `eiti_us_ein_by_lei.json` crosswalk (`backend/scripts/build_eiti_us_ein_index.py`: each EITI EIN confirmed against the `ein` field of the EDGAR registrant that carries it, then resolved to a US LEI on exact legal-name equality; every row records its evidence and confidence, and ambiguous or unmatched EINs are written to an `unresolved` list rather than guessed). The US bucket is frozen — the US left EITI in November 2017 — so this is 29 identifications from one 2015 reporting year, 27 of them actual EINs; rebuild the crosswalk whenever `build_eiti_index.py` refreshes the organisation index. No API key required |
| `eiti_soe` | EITI State-Owned Enterprises Database | EITI open data (attribution) | LEI matched against the committed SOE index | **CDD** — state-owned-enterprise flag and SOE context (sector, commodities, audited-financial-statement links, stock listings) for the ~100 SOEs reported through the EITI. Distinct from the `eiti` payments adapter; each SOE is resolved to an LEI at index-build time via GLEIF (`opencorporates_id` → reverse lookup, name+country fallback) by `backend/scripts/build_eiti_soe_index.py` → committed `eiti_soe_index.json.gz`. The BODS mapping emits a `stateBody` government + `controlByLegalFramework` relationship, which raises the `STATE_CONTROLLED` signal. The LEI is derived (not published by EITI), so it is not asserted as a cross-source identifier. No API key required |
| `meip` | OECD-UNSD Multinational Enterprise Information Platform (MEIP) | OECD-Terms (attribution) | LEI matched against the local `meip.sqlite` store (a GitHub release asset, downloaded at boot) | **CDD** — group membership from the OECD-UNSD Global Register of the 500 largest multinational enterprise groups and their 126,658 subsidiaries (register of 31 December 2024), served **as the OECD's own BODS v0.4 statements** since the September 2026 release: the subject's entity statement, one `unknownInterest` relationship per group membership (from the subsidiary to its group head; 113 LEIs sit in two groups and both are kept), and the group head's entity statement, passed through unmodified. No shares, no dates, no natural persons; the OECD's `Known` / `Partial` / `Unknown` hierarchy classification rides on the relationship. A group head's subsidiaries are listed on the Subsidiaries tab, not drawn as graph nodes, and a member's immediate parent comes from the register spreadsheet (the BODS file carries only the edge to the head). Excluded from the `COMPLEX_OWNERSHIP_LAYERS` count — every edge is a single hop to the head. Packed by `backend/scripts/build_meip.py` from the BODS JSONL + Global Register XLSX into a 66 MB SQLite (29 MB gzipped); without the file the source covers nothing. Was a signpost card from Phase 69 to Phase 207. |
| `eiti_assessment` | EITI Company Assessment | EITI open data (attribution) | LEI matched against the committed assessment index | **ESG** — EITI's assessment of its supporting companies against nine expectations, including **expectation 6 (company discloses beneficial ownership)** and **expectation 2 (company publishes a list of controlled subsidiaries)** — plus the declared subsidiary list itself (1,230 rows across 51 implementing countries). Records a company's *disclosure posture*, not its ownership: no risk signal, no person or relationship statements. The declared subsidiaries are names and countries only — EITI publishes no identifier for them — so they are rendered as evidence and deliberately never enter the BODS graph. Supporting companies are resolved to LEIs offline by `backend/scripts/build_eiti_assessment_index.py`, which refuses to build until a human has reviewed every match; 64 of 99 resolved. **No identifiers are asserted at all** — the LEI is derived, EITI's `legal_entity_id` is populated for 3 companies of 10,116, and its `eiti_id_company` is a name-derived deduplication key EITI regenerated wholesale in this release. No API key required |
| `eiti_bo` | EITI countries — national beneficial ownership registers | Public registers (per-register terms; DRC and Armenia state no licence — included with attribution) | LEI matched against the committed pooled index | **CDD** — beneficial ownership of extractive companies **pooled from the national BO registers of EITI implementing countries** (one source, not one adapter per register — the pooled universe is small, smaller still filtered to LEI holders). Registers at launch: **DRC** ITIE-RDC Registre des propriétaires effectifs (the only EITI BO register anywhere with a bulk download — XLSX export with ownership %, voting rights and PEP flags; Loi n°25/048 du 1 juillet 2025), **Armenia** State Register declarations at old.e-register.am (per-declaration **BODS v0.2 JSON**, upconverted to v0.4 with the originals recorded via annotations; seed list = EITI Armenia's 27 declaring metal-ore mining companies), and **Nigeria** (the `cac_nigeria` harvest filtered to NEITI solid-minerals-covered companies, filter evidence dated per record — the NEITI portal itself is frozen ~2023). Indonesia slot reserved (AHU API in maintenance). Excluded: Tajikistan (all-rights-reserved), Trinidad & Tobago (frozen ~2021). Harvested offline by `backend/scripts/build_eiti_bo_index.py` → committed `eiti_bo_index.json.gz`; **LEI-only at launch** — each company is resolved to an LEI at build time (registration-number equality against GLEIF first, e.g. Zangezur's `27.140.00009`; normalised-name equality as fallback), and unmatched companies stay in the committed raw harvests, counted in the artifact manifest. Asserts only register-published identifiers (`am_regnum`/`am_tin`/`ng_cac_rc`/`cd_nif`), never the derived LEI. Re-run the harvest subcommands + `build` to refresh; when the EITI open data portal (CKAN, with Datopian; due ~Sept 2026) launches, evaluate swapping per-register fetchers for portal sourcing. No API key required |
| `ted_eu` | TED — Tenders Electronic Daily (EU procurement) | EU open data (Decision 2011/833/EU, incl. commercial) | LEI + GLEIF `registeredAs` + derived national numbers via `organisation-identifier-tenderer` (eForms BT-501) on the TED Search API v3 | **CDD** — EU public procurement award notices where the entity appears as a tenderer/winner. One anonymous search POST (exact `IN()` identifier match; the `~` operator is unsupported on identifier fields), then the newest ≤10 notices are winner-confirmed from the eForms notice XML (`LotResult → LotTender → TenderingParty → Tenderer → CompanyID` chain) and labelled **won** / **tendered** / unconfirmed, with per-lot awarded values, award dates and contract references. When the XML cannot be had — `ted.europa.eu` sits behind an AWS WAF that challenges datacenter IPs (HTTP 202, empty body) since September 2026 — the role comes from the Search API's own `winner-identifier` field instead (role only, no lots or amounts), and each notice records which path it came from in `role_basis`. **Coverage: eForms era only (≈2024 onwards)** — no history, so "no hits" ≠ "never won"; awards won via subsidiaries sit under the subsidiary's identifier. The LEI is always queried but its fill rate in BT-501 was zero as of 2026-08 (0 in 5,031 sampled identifiers) — recall comes from the national registration numbers; the lookup self-upgrades as LEI adoption in eForms grows. No API key required |
| `wikirate` | Wikirate | CC-BY-4.0 | LEI (Wikidata Q-ID fallback) via `filter[company_identifier[value]]` | **ESG** — open, community-researched corporate ESG metric answers (environment, human rights, supply chains, governance) from designers such as the World Benchmarking Alliance and Net Zero Tracker; OpenCheck shows totals + a sample of the most recent researched answers (sorted most-recent-year-first) and links out to wikirate.org for the full record; publishes LEI/Wikidata/OpenCorporates/CIK identifiers (strong cross-source corroborator; GODIN member); requires `WIKIRATE_API_KEY` (Cloudflare blocks anonymous server-side requests) |

## Inactive / bulk-only adapters

These adapters are committed and tested but **not exposed as live sources**. Each relies on bulk data files rather than a queryable API: the source is built into a local SQLite database and activated by an environment variable, so with no file configured (the production default) they return nothing. They will be turned on once OpenCheck adopts a bulk-data strategy, or once the source begins offering an API. Cyprus, Ukraine and Romania go a step further than Belgium — they are **not registered in `REGISTRY` and not wired into the lookup dispatch at all**, so they never appear on `/sources`.

| ID | Name | License | Entry point | Status & description |
|----|------|---------|-------------|----------------------|
| `bce_belgium` | Belgian Crossroads Bank for Enterprises (BCE/KBO) | Custom-KBO-Reuse | `be_enterprise_number` from GLEIF (`RA000025`) | Registered + wired, but env-gated. Entity name (NL/FR/DE), status, juridical form, start date, registered address from a local SQLite DB built from the monthly KBO open data ZIP; FTS5 name search. Activate via `BCE_BELGIUM_DB_FILE` |
| `edr_ukraine` | ЄДР — Unified State Register of Legal Entities (Ukraine) | CC-BY-4.0 | `ua_edrpou` from GLEIF (`RA000567` ЄДР, `RA001026` NSSMC, `RA001027` NBU — all three carry the EDRPOU) | Not in `REGISTRY`, not wired. **Beneficial owners** (533,958 entities carry a named UBO; a further 117,266 record the register's stated reason for their absence), founders with their hryvnia holdings, heads and signatories, governing-body members, and the executive authority above a state enterprise. No government API exists — the only official channel is a weekly 327 MB ZIP of XML (3.16 GB uncompressed, 2,017,706 entities), built into a local SQLite index via `scripts/build_edr_ukraine_index.py`. Addresses and activity codes are withheld at source under martial law (Law 4576-IX) and are never reconstructed here. Activate via `EDR_UKRAINE_DB_FILE` |
| `cyprus_drcor` | Cyprus DRCOR — Registrar of Companies | CC-BY-4.0 | `cy_he` from GLEIF (`RA000161`) | Not in `REGISTRY`, not wired. Organisations, registered office, and officials (directors/secretaries; no shareholders) from three monthly data.gov.cy CSVs, built into a local SQLite DB via `scripts/extract_cyprus.py`; entity + officer statements. data.gov.cy exposes no query API (`/api/1/datastore/query` returns 404). Activate via `CYPRUS_DRCOR_DB_FILE` |

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

## Lineage — which sources republish which

Several sources are copies of others, and OpenCheck says so. Each adapter may
declare `derived_from`; the `/sources` endpoint (and the MCP `opencheck_list_sources`
tool) exposes it, and every surface that counts "sources that agree" as
corroboration — the "LEI confirmed by N sources" badge, the FullCheck network's
"corroborated by ≥2 independent sources" count and the identity band's "matched
across N independent sources" — counts *independent origins* after collapsing
this lineage. A source with no declaration is treated as original, so an adapter
that says nothing under-claims its dependence rather than over-claiming
corroboration. The table lives in `backend/opencheck/sources/lineage.py`; the
browser copy `frontend/src/lib/lineage.json` is generated from it by
`backend/scripts/gen_lineage.py` and a test fails when it is stale.

| Source | Republishes | Why |
|--------|-------------|-----|
| `opencorporates` | every national register (`is_national_register`) | OpenCorporates is a mirror of the official registers; its Companies House officers agree with Companies House because they are Companies House |
| `openaleph` | `companies_house`, `gleif`, `opensanctions` | The entity records a company lookup returns come from OpenAleph's `gb-coh-psc-*`, `lei-*` and OpenSanctions collections. Leak and document collections are original, but lineage is declared per source, so OpenAleph is discounted throughout — the safe side |
| `opensanctions` | `gleif`, `companies_house` | OpenSanctions' *company records* are its GLEIF and UK PSC mirrors (a Novo Nordisk record carries GLEIF's LEI and creation date verbatim). Its sanctions, PEP and debarment *listings* are original — lineage only discounts agreement about the record, never a finding |
| `everypolitician` | `opensanctions` | Reads the OpenSanctions database (same canonical FtM ids) |
| `bods_gleif`, `bods_uk_psc` | `gleif`, `companies_house` | The bulk BODS datasets are the same registers in a different container |

Two rules follow. A source whose upstream is also present is dropped in favour
of the upstream (GLEIF + OpenSanctions sharing an LEI is one confirmation). Two
derivatives of a shared upstream that is absent (OpenCorporates and OpenAleph,
both mirroring Companies House) count once between them. On the 2 September 2026
Shell plc lookup that turns "five sources describe this entity" into three
independent origins (Companies House, GLEIF, Wikidata), and the 22 officer
records Companies House and OpenCorporates agree on into one observation.

## Liveness — what each register says about whether the entity still exists

Nearly every register publishes a company status, and until Phase 151 the
mappers handled it six different ways (three wrote `dissolutionDate: "unknown"`,
one wrote `null`, one annotated, twenty-odd dropped it). Every adapter now goes
through one path, `backend/opencheck/bods/liveness.py`, which classifies the
register's own label into **live**, **pending** (a terminal process under way —
liquidation, strike-off listed, bankruptcy), **terminal** (dissolved, struck off,
cancelled, deregistered, amalgamated) or **unknown** (the register said nothing,
or something the mapper does not classify — never guessed either way), and writes:

- `recordDetails.dissolutionDate` **only** for `terminal` with a real date
  (partial dates rounded per the BODS dates guidance and annotated). Never a
  sentinel: the schema requires `YYYY-MM-DD`.
- a `commenting` annotation on `/recordDetails` in a fixed grammar —
  *"`<Source>` records this entity as `active` | `in a terminal process` |
  `dissolved`[ since `YYYY-MM-DD`][ — register status: “`<verbatim label>`”]."* —
  for live, pending and terminal alike, so that "this source said live" is
  distinguishable from "this source said nothing". `read_register_status`
  (Python) and `readRegisterStatus` (`frontend/src/lib/liveness.ts`) parse it back.

The structured-records card shows a **Register status** row for pending and
terminal statuses only; an active status is not a finding.

| Source | Status field(s) read | Notes |
|--------|----------------------|-------|
| `companies_house` | `company_status`, `date_of_cessation` | dissolved / converted-closed / closed / removed → terminal; liquidation, receivership, administration, voluntary-arrangement, insolvency-proceedings → pending |
| `gleif` | `entity.status` (ACTIVE / INACTIVE), `entity.expiration.date` + `.reason` | `registration.status` (LAPSED / RETIRED) is about the LEI record, not the entity, and is **not** read as liveness |
| `opencorporates` | `inactive` (OC's normalised bool), `dissolution_date`, `current_status` (verbatim label) | previously the dissolution date was dropped |
| `cvr_denmark` | normalised status (`_STATUS_MAP`) + `virksomhedOphoersdato` | an end date outranks the label |
| `gemi_greece` | codelist `isActive` + `lastStatusChange` | unknown codelist entry stays unknown |
| `acra_singapore` | `entity_status_description` (collection 2), else `uen_status_desc` (collection 1: Registered / Deregistered) | no date published; "In Liquidation - …", "Gazetted To Be Struck Off" and receivership are pending; "Dissolved - …", "Struck Off", "Deregistered" terminal; an unlisted label stays unknown |
| `anaf_romania` | `stare_inactiv.dataRadiere`, `stare_inactiv.statusInactivi`, `stare_inregistrare` | the striking-off date is terminal and sets `dissolutionDate`; a taxpayer declared **inactive** is pending, never terminal — it has not been dissolved; `INREGISTRAT …` is live |
| `apr_serbia` | `NazivStatus` | *Активан* is live; *У ликвидацији* (liquidation), *У стечају* (bankruptcy) and *У принудној ликвидацији* (forced liquidation) are pending; no date is published; deleted companies are not in the feed, so terminal never arises; an unknown status fails the index build |
| `asp_moldova` | liquidators and insolvency administrators among the directors (*Data lichidării* rows are not indexed) | a *Lichidator* or insolvency administrator on file is pending; otherwise live |
| `onrc_romania` | `COD` from `OD_STARE_FIRMA` against `N_STARE_FIRMA` (197 codes) | `1084` radiată is terminal; dizolvare / lichidare / faliment / insolvență are pending; `1048` funcțiune is live; any other code stays unknown |
| `abr_australia` | `abn_status`, `abn_status_from` | cancelled ABN = terminal for the registration resolved |
| `mca_india` | `CompanyStatus` | no date published; was `"unknown"` before; Dormant is live |
| `kvk` | `datumEinde` | end date only — no label in the open-data profile |
| `ur_latvia` | `terminated` (date), `closed` | the parsed date was previously computed and discarded |
| `rpo_slovakia` | `termination` (date) | |
| `malta_mbr` | `state`, `status_effective_date` | |
| `cr_hongkong` | presence in the dataset | the open data lists live companies only and has no status field, so a returned record is written as live with no verbatim label; absence is never read as dissolution |
| `nz_companies` | `entityStatusDescription` | Registered → live; Removed → terminal; insolvency states → pending |
| `corporations_canada` | `status` + dissolution `activities[]` dates | |
| `zefix` | `status` ACTIVE / BEING_CANCELLED / CANCELLED | |
| `bolagsverket` | `avregistreradOrganisation`, `avregistreringsorsak`, pending winding-up list, `verksamOrganisation.kod` | |
| `ariregister` | page status label | "Entered into the register" → live; "Deleted from the register" → terminal |
| `cro` | `company_status`, `company_status_date` | Normal → live; Strike Off Listed and the insolvency states → pending |
| `cnpj_brazil` | `situacao_cadastral`, `data_situacao_cadastral` | ATIVA → live; BAIXADA / NULA → terminal; SUSPENSA / INAPTA left unclassified |
| `brreg` | `slettedato`, `konkurs`, `underAvvikling`, `underTvangsavviklingEllerTvangsopplosning` | |
| `prh` | `endDate`, `liquidations` | |
| `firmenbuch` | AUFRECHT → `aktiv` / `gelöscht` | |
| `cac_nigeria` | `status` | ACTIVE → live; INACTIVE means returns outstanding, not dissolution, so left unclassified |
| `jar_lithuania` | status (Lithuanian labels) | Veikiantis → live; Išregistruotas → terminal; Likviduojamas / Bankrutuojantis / Reorganizuojamas → pending; Sustabdyta left unclassified |
| `ares` | normalised status, `datumZaniku` | |
| `bce_belgium` | `status` AC / ST | |
| `cyprus_drcor` | organisation status | |
| `opensanctions`, `openaleph` (FtM) | `dissolutionDate` property | only a dissolution date is a positive statement; FtM `status` is free text and not classified |
| `climatetrace` (GEOT) | `entity_status` | unchanged: GEM publishes no date; its own annotation remains |

Not read (no usable status in the payload, or not yet): `sudreg_croatia` (an
integer status code without a published codelist), `inpi`, `wikidata` (P576 is
not queried), `sec_edgar`, `krs_poland`, the EITI sources, `wikirate`, `ted_eu`,
`brightquery`.

## Record consistency (shadow mode)

With lineage (Phase 150) and liveness (Phase 151) in place, `backend/opencheck/consistency.py`
compares what *independent* sources say about the *same* entity — statements grouped
by shared strong identifier, the rule the FullCheck network merges on — on four aligned
fields: **liveness** (the Phase 151 class), **jurisdiction**, **founding date** (registers
and GLEIF only; Wikidata's *inception* is the business, not the incorporation, and is
never compared), and **one-per-entity identifiers** (the LEI, and a register number
within one jurisdiction; OpenCorporates ids are per-registration and never a clash).
Each pair is `agree` / `disagree` between independent sources, `mirror` / `stale` where
one republishes the other (counted, never shown), or `one_missing`.

Nothing reaches the results page yet. `GET /consistencystats` counts outcomes per
`field|source_a|source_b` with a `disagree_rate`, on the `/signalstats` contract
(aggregate only, closed-vocabulary keys, resets on deploy). A comparison earns a
place on the page only when its measured disagree rate is under 10 % after at least
two weeks of traffic — the base-rate gate that keeps "sources disagree" from being noise.

## Notes

NC-licensed sources (OpenSanctions, EveryPolitician) propagate their non-commercial obligations through `/deepen` and `/export`. The exported `LICENSES.md` warns reviewers before they re-publish. (OpenTender / DIGIWHIST procurement was retired and its code removed — the live `ted_eu` adapter answers the EU-procurement question against TED directly, under a licence that permits commercial reuse.)

Full per-source attribution and licence details are in [ATTRIBUTIONS.md](../ATTRIBUTIONS.md).
