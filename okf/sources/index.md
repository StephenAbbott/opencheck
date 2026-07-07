# National company / beneficial-ownership registers

* [Australian Business Register (ABN Lookup)](/sources/abr_australia.md) - Australian company and business data — ABN, ACN, entity name and type, ABN/GST status, registered state and postcode, and business (trading) names — from the Australian Business Register's free ABN Lookup web services. Entity-level only; no officer or ownership data.
* [ARES (Czechia)](/sources/ares.md) - Czech ARES business register (Administrativní registr ekonomických subjektů), aggregating data from the commercial register (Obchodní rejstřík), trade licence register, and other sub-registers.  Published by the Ministry of Finance under CC BY 4.0.
* [Estonian e-Business Register (e-Äriregister)](/sources/ariregister.md) - Estonian company data including entity details, shareholders (with ownership percentages), board members, and beneficial owners, from the public e-Business Register portal (RIK).
* [Belgian Crossroads Bank for Enterprises (BCE/KBO)](/sources/bce_belgium.md) - Belgian company data including entity name, status, juridical form, start date, and registered address, from the BCE/KBO open data publication by FPS Economy.
* [Bolagsverket — Swedish Companies Registration Office](/sources/bolagsverket.md) - Swedish company data from Bolagsverket's open data API (värdefulla datamängder), including entity details and registered address.
* [Brønnøysundregistrene — Norwegian Register Centre](/sources/brreg.md) - Norwegian company data from the Enhetsregisteret (Central Coordinating Register for Legal Entities), including entity details and role-holders.
* [Receita Federal — CNPJ register (Brazil)](/sources/cnpj_brazil.md) - Brazilian company data from the Receita Federal CNPJ register (open data), including the QSA — partners and administrators. Served key-lessly via OpenCNPJ with a BrasilAPI fallback.
* [UK Companies House](/sources/companies_house.md) - Legal and beneficial ownership information from the UK corporate registry.
* [Corporations Canada — ISED federal register](/sources/corporations_canada.md) - Federal Canadian company data from Corporations Canada (Innovation, Science and Economic Development Canada), covering companies incorporated under federal statutes including the Canada Business Corporations Act.
* [CRO — Companies Registration Office Ireland](/sources/cro.md) - Irish company data from the Companies Registration Office (CRO), sourced via the CRO Open Data Portal (CC BY 4.0). Provides entity details for all registered Irish companies.
* [CVR — Det Centrale Virksomhedsregister](/sources/cvr_denmark.md) - Danish Central Business Register (CVR) — the authoritative register of all Danish businesses, maintained by Erhvervsstyrelsen (the Danish Business Authority). Accessed via the Datafordeler GraphQL API (non-restricted entity data).
* [Firmenbuch — Austrian Commercial Register](/sources/firmenbuch.md) - Austrian company name, address, status, and officers (managing directors, authorised signatories, supervisory board) from the Firmenbuch (commercial register), via the BMJ High Value Dataset API. Shareholder data requires a paid subscription.
* [INPI — Registre National des Entreprises](/sources/inpi.md) - French company data from the Registre National des Entreprises (RNE), operated by INPI, sourced via the SIREN number.
* [JAR — Lithuanian Register of Legal Entities](/sources/jar_lithuania.md) - Lithuanian company data from the Register of Legal Entities (JAR), maintained by Registrų centras. Provides entity name, code, address, legal form, and registration status for all entities registered in Lithuania.
* [KRS — Polish National Court Register](/sources/krs_poland.md) - Entity data from Poland's National Court Register (Krajowy Rejestr Sądowy / KRS), maintained by the Ministry of Justice. Provides company name, NIP, REGON, legal form, registered address, share capital, board composition, and primary business activity (PKD code). Note: personal data (names, PESEL) is masked in the public API extract.
* [KvK — Netherlands Chamber of Commerce](/sources/kvk.md) - Dutch company data from the Netherlands Chamber of Commerce (KvK) open-data API, sourced via the KvK registration number.
* [Malta Business Registry (MBR)](/sources/malta_mbr.md) - Maltese company data from the Malta Business Registry (MBR) Open Data API (CC BY 4.0). Provides core entity details — name, status, legal form, registered office and registration date — for companies on the Maltese register.
* [New Zealand Companies Register (NZBN)](/sources/nz_companies.md) - New Zealand company data from the NZBN API (Companies Office / MBIE): entity details, directors, shareholders with share allocations, and the ultimate holding company.
* [PRH — Finnish Patent and Registration Office](/sources/prh.md) - Finnish company data from the Patentti- ja rekisterihallitus (PRH) via the YTJ Open Data API, including entity details for all organisations registered in Finland. Officer data is not publicly available.
* [RPO Slovakia](/sources/rpo_slovakia.md) - Slovak Register of Legal Persons (Register právnických osôb), operated by the Statistical Office of the Slovak Republic.
* [RPVS Slovakia](/sources/rpvs_slovakia.md) - Slovak Public Sector Partners Register (Register partnerov verejného sektora), published by the Ministry of Justice of the Slovak Republic.  Lists entities supplying goods or services to public bodies above statutory thresholds, with verified ultimate beneficial owner (KUV) disclosures.
* [Sudski registar — Croatian Court Register](/sources/sudreg_croatia.md) - Croatian company data from the Sudski registar (Court Register) public API, including legal name, MBS and OIB identifiers, legal form, status, founding date, registered seat and share capital.
* [UR — Latvian Register of Enterprises](/sources/ur_latvia.md) - Latvian company data from the Register of Enterprises (UR), sourced via Latvia's open-data portal (data.gov.lv). Provides entity profiles, beneficial owners, officers, and shareholders for companies registered in Latvia.
* [Zefix — Swiss Commercial Registry](/sources/zefix.md) - Swiss company data from the Federal Commercial Registry (Zefix / FCRO), sourced via the Swiss UID.

# Aggregators & cross-border databases

* [EveryPolitician](/sources/everypolitician.md) - EveryPolitician is a global database of political office-holders, from rulers, law-makers to judges and more.
* [GLEIF](/sources/gleif.md) - Legal entity information from the Global Legal Entity Identifier Foundation.
* [OpenAleph](/sources/openaleph.md) - The open source platform that securely stores large amounts of data and makes it searchable for easy collaboration.
* [OpenCorporates](/sources/opencorporates.md) - The world's largest open legal-entity database, providing a single unified set of company records from government registries.
* [OpenSanctions](/sources/opensanctions.md) - Sanctions lists, PEPs, debarments, and regulatory actions from the OpenSanctions open-source database.
* [SEC EDGAR (Schedule 13D/13G)](/sources/sec_edgar.md) - Major shareholders (>5 % beneficial owners) of US-listed companies from mandatory SEC Schedule 13D and 13G filings. Coverage is limited to XML filings submitted from December 2024 onward.
* [Wikidata](/sources/wikidata.md) - A free and open knowledge base that can be read and edited by both humans and machines.

# ESG / climate sources

* [Global Energy Monitor / Climate TRACE](/sources/climatetrace.md) - Global fossil-fuel asset ownership data (GEM) combined with satellite-derived emissions estimates (Climate TRACE). LEI resolution uses the GLEIF-certified GEM Entity ID mapping (June 2026). Enables ESG and climate risk screening by LEI.
* [EITI — Extractive Industries Transparency Initiative](/sources/eiti.md) - Company-level payments to governments (taxes, royalties, licence fees) disclosed under the EITI Standard by 65+ implementing countries, with GFS revenue classification.
