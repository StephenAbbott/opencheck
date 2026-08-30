# Beneficial ownership regimes per source

<!-- GENERATED from backend/opencheck/bods/bo_regimes.py — edit that file, then regenerate. -->

Phase B deliverable of the `beneficialOwnershipOrControl` audit (2026-08-28); entries manually
checked by Stephen against current legislation on 2026-08-30.
BODS embeds no intrinsic definition of "beneficial owner": `beneficialOwnershipOrControl: true`
marks an interest known to constitute beneficial ownership *under the applicable jurisdiction's
definition*. This page records, per OpenCheck source, which definition that is.

**Watch item (2026-08-30):** whether Estonia and Latvia amend their definitions from "more than
25%" to the AMLR's "25% or more" ahead of, or at, the 10 July 2027 application date.

Two levels of rule interact:

- **BODS-level (jurisdiction-independent):** a beneficial owner is a natural person, so a
  relationship whose interested party is an *entity* never carries `true` — Open Ownership's own
  UK PSC and GLEIF pipelines emit `false` on every entity-party relationship (verified against
  `data/demo/*.jsonl`: 2,471 entity rows all `false`, 44 person rows all `true`).
- **Regime-level (this registry):** whether a given record kind is a BO declaration
  (`assert_true`), a registered holding the register makes no BO claim about (`omit`), or a
  verbatim copy of upstream BODS (`copy_verbatim`).

## Summary

| Source | Jurisdiction | Regime | Threshold | Basis of reporting | Natural person only |
|---|---|---|---|---|---|
| `companies_house` | United Kingdom | bo_register | more than 25 % | first qualifying link | no |
| `bods_uk_psc` | United Kingdom | bo_register | more than 25 % | — | no |
| `ariregister` | Estonia | bo_register | more than 25 % (üle 25 %) | ultimate BO | yes |
| `ur_latvia` | Latvia | bo_register | vairāk nekā 25 % (more than 25 %) | ultimate BO | yes |
| `rpvs_slovakia` | Slovakia | bo_register | najmenej 25 % (at least 25 %) | ultimate BO | yes |
| `sec_edgar` | United States | securities_disclosure | more than 5 % of a class of registered voting equity securities | — | no |
| `cac_nigeria` | Nigeria | bo_register | at least 5 % | — | no |
| `bods_gleif` | Global (GLEIF) | consolidation | — | — | no |
| `wikidata` | None (crowdsourced) | crowdsourced | — | — | no |

## EU horizon (AMLR / AMLD6)

- `amlr`: Regulation (EU) 2024/1624 (AMLR), Arts 22, 51-54, 63
- `amlr_threshold`: 25 % or more (>=), multiplied through chains, summed across chains
- `amlr_applies_from`: 2027-07-10
- `amld6`: Directive (EU) 2024/1640 (AMLD6), register + access provisions
- `amld6_access_phase1`: 2025-07-10
- `amld6_access_phase2_arts_11_12_13_15`: 2026-07-10
- `amld6_full_transposition`: 2027-07-10

The AMLR moves the EU ownership test to **25 % or more** (`>=`), multiplied through each level of
a chain and summed across chains, from **10 July 2027** — this changes the Estonian and Latvian
operators below (currently `>`); Slovakia is already at `>=`. The UK is outside the package.

## `companies_house` — Companies House register of People with Significant Control (PSC)

- **Jurisdiction:** United Kingdom (GB)
- **Regime kind:** bo_register
- **Legal basis:**
  - Companies Act 2006, Part 21A and Schedule 1A (inserted by SBEEA 2015)
  - The Register of People with Significant Control Regulations 2016 (SI 2016/339)
  - Economic Crime and Corporate Transparency Act 2023 (identity verification; central-register reforms)
  - The Register of People with Significant Control (Amendment) Regulations 2025 (SI 2025/1036)
- **Definition:** A PSC meets any of five conditions: (1) holds, directly or indirectly, more than 25 % of shares; (2) more than 25 % of voting rights; (3) the right to appoint or remove a majority of the board; (4) significant influence or control; (5) significant influence or control over a trust or firm that itself meets a condition.
- **Threshold:** more than 25 % (`> 25 %`)
- **Reporting basis:** FIRST QUALIFYING LINK, not necessarily the ultimate natural person: a company records the first registrable person OR registrable relevant legal entity (RLE) in each chain. A chain therefore legitimately stops at a corporate PSC — the UK register is not an ultimate-BO register.
- **Natural person only:** no
- **Fallback:** Statement codes (no PSC identified / steps not completed / super-secure etc.), not senior management
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `psc_individual` → `assert_true`
  - `psc_corporate_rle` → `assert_false`
  - `psc_statement` → `per_statement_code`
  - `officer_director` → `omit`
- **Pending changes:**
  - ECCTA is delivered through ~50 statutory instruments, many still pending (see Stephen's Notion tracker 'Track implementation of ECCTA', 1b57f3dc292880a5a6c6fef6d927638e)
  - Identity verification: voluntary from 8 Apr 2025; mandatory rollout from 18 Nov 2025 with a 12-month transition for existing directors and PSCs (~4M verified by Jun 2026); no threshold change
  - PSC statutory guidance on 'significant influence or control' reissued 4 Mar 2026 (2026 company + LLP statutory guidance) — condition-4 interpretation source
  - Updated 2026 PSC-reporting guidance published Jan 2026 but NOT yet in force pending parliamentary approval
  - Register of Overseas Entities: not to be solely relied on for verifying BO (Reg 28, ECCTA Consequential Provisions Regs 2025); ROE brought into the discrepancy-reporting regime (Reg 30A)
  - English Limited Partnership PSC loophole NOT fixed by ECCTA (BBC/Finance Uncovered, Nov 2023)
  - UK is outside the EU AML package — AMLR >=25 % does NOT apply
- **Notes:**
  - UK government has formally adopted BODS as its open standard for BO data
  - OpenCheck matches Open Ownership's bulk-pipeline behaviour: individual PSC -> true, RLE -> false
- **Sources:**
  - <https://www.legislation.gov.uk/ukpga/2006/46/schedule/1A>
  - <https://www.gov.uk/guidance/people-with-significant-control-pscs>
  - <https://www.legislation.gov.uk/uksi/2025/1036/note/made>
- **Last verified:** 2026-08-30 — **review status: verified**

## `bods_uk_psc` — Open Ownership UK PSC bulk BODS dataset (register v2 pipeline)

- **Jurisdiction:** United Kingdom (GB)
- **Regime kind:** bo_register
- **Legal basis:**
  - As companies_house — OO republishes the CH PSC register as BODS
- **Definition:** As companies_house.
- **Threshold:** more than 25 % (`> 25 %`)
- **Reporting basis:** As companies_house; OO pipeline emits true only for natural-person interested parties
- **Natural person only:** no
- **Fallback:** As companies_house
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `relationship` → `copy_verbatim`
- **Notes:**
  - Verified 2026-08-28 on data/demo: person->true (44), entity->false (1,899), no exceptions
- **Sources:**
  - <https://bods-data.openownership.org/>
- **Last verified:** 2026-08-30 — **review status: verified**

## `ariregister` — e-äriregister — tegelikud kasusaajad (beneficial owners) alongside shareholders/officers

- **Jurisdiction:** Estonia (EE)
- **Regime kind:** bo_register
- **Legal basis:**
  - Rahapesu ja terrorismi rahastamise tõkestamise seadus (RahaPTS) § 9 (definition)
  - RahaPTS §§ 76-77 (duty to submit BO data to the business register)
- **Definition:** The natural person who, taking advantage of their influence, ultimately owns or controls the legal person. Direct ownership: shareholding/ownership interest of more than 25 % held by the natural person. Indirect ownership: a company controlled by the natural person (alone or together) holds more than 25 %.
- **Threshold:** more than 25 % (üle 25 %) (`> 25 %`)
- **Reporting basis:** ULTIMATE beneficial owner (through chains), with control-mechanism code per BO
- **Natural person only:** yes
- **Fallback:** Senior management (juhatus) recorded where no qualifying person is identified
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `kasusaaja_bo` → `assert_true`
  - `shareholder` → `omit`
  - `officer` → `omit`
  - `corporate_shareholder` → `omit`
- **Pending changes:**
  - Public-access restriction (legitimate-interest regime) drafted to start 10 Jul 2026; Justice Minister refused to endorse the bill 25 Jun 2026; change POSTPONED, no new date (ERR, err.ee/1610074771)
  - EC infringement procedure opened 25 Sep 2025 over 6AMLD register-access compliance
  - AMLR >=25 % + chain multiplication applies from 10 Jul 2027
- **Notes:**
  - Adapter carries include_beneficial_owners kill-switch for the day access is restricted
  - Stephen's tracking ticket: 'Track EU legitimate interest changes' (Notion 38b7f3dc...)
  - Sheet 'EU BO LIA tracker' marks EE legitimate-interest access as in place (abiinfo.rik.ee/en/node/367) — reconcile with the postponement during the manual pass
- **Sources:**
  - <https://www.riigiteataja.ee/akt/114032025023>
  - <https://abiinfo.rik.ee/en/node/367>
  - <https://www.err.ee/1610074771/tegelike-kasusaajate-andmete-varjamine-lukkub-edasi>
- **Last verified:** 2026-08-30 — **review status: verified**

## `ur_latvia` — Uzņēmumu reģistrs — patiesie labuma guvēji (PLG) register

- **Jurisdiction:** Latvia (LV)
- **Regime kind:** bo_register
- **Legal basis:**
  - Noziedzīgi iegūtu līdzekļu legalizācijas un terorisma un proliferācijas finansēšanas novēršanas likums (NILLTPFN likums), Art 1(5)(a) (definition)
  - NILLTPFN likums Ch III.1 (Arts 18.1-18.3, registration with UR)
- **Definition:** The natural person who ultimately owns or directly or indirectly controls the legal person: 'vairāk nekā 25 % no juridiskās personas kapitāla daļām vai balsstiesīgajām akcijām' (more than 25 % of capital shares or voting shares), through direct or indirect participation, or control by other means.
- **Threshold:** vairāk nekā 25 % (more than 25 %) (`> 25 %`)
- **Reporting basis:** ULTIMATE beneficial owner. UR publishes the BO list WITHOUT the qualifying mechanism or percentage, and separately publishes members/shareholders — the two record sets are not linked, which is why the mapper cannot mark a BO's qualifying shareholding true (audit finding 3)
- **Natural person only:** yes
- **Fallback:** Board members recorded where BO cannot be established (declaration to that effect)
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `beneficial_owner` → `assert_true`
  - `member_shareholder` → `omit`
  - `officer` → `omit`
  - `corporate_member` → `omit`
- **Pending changes:**
  - Register REMAINS public after 1 Jul 2026; from that date a BO may request case-by-case restriction where disclosure creates a real crime risk to them or family
  - Chief State Notary decision No 1-5n/85 of 18 Jun 2026 on BO registration practice — obtain and review
  - AMLR >=25 % + chain multiplication applies from 10 Jul 2027
- **Notes:**
  - Latvia was the first country to publish national BODS data (2021)
- **Sources:**
  - <https://www.ur.gov.lv/lv/patieso-labuma-guveju-skaidrojums>
  - <https://likumi.lv/ta/id/178987>
  - <https://www.err.ee/1610050456/erinevalt-eestist-jatab-lati-tegelike-kasusaajate-registri-avalikuks>
- **Last verified:** 2026-08-30 — **review status: verified**

## `rpvs_slovakia` — Register partnerov verejného sektora (RPVS) — konečný užívateľ výhod (KUV)

- **Jurisdiction:** Slovakia (SK)
- **Regime kind:** bo_register
- **Legal basis:**
  - Zákon č. 297/2008 Z. z. (AML Act) § 6a (KUV definition)
  - Zákon č. 315/2016 Z. z. (RPVS Act — registration, verification by oprávnená osoba)
- **Definition:** Always a natural person ('vždy fyzická osoba'): holds 'podiel najmenej 25 % na hlasovacích právach ... alebo na jej základnom imaní' (a share of AT LEAST 25 % of voting rights or registered capital, direct or indirect — indirect computed by multiplying stakes through layers); or right to at least 25 % of profit; or power to appoint/remove governing bodies; or other decisive influence.
- **Threshold:** najmenej 25 % (at least 25 %) (`>= 25 %`)
- **Reporting basis:** ULTIMATE beneficial owner, VERIFIED by an oprávnená osoba (authorised professional) who files and legally answers for the identification — the strongest verification regime of the four
- **Natural person only:** yes
- **Fallback:** Members of vrcholový manažment (senior management) where no qualifying person exists — exception, requires documented analysis
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `kuv` → `assert_true`
  - `corporate_party` → `omit`
- **Pending changes:**
  - NOTE: Slovakia's threshold is ALREADY >=25 % — it differs from UK/EE/LV (>25 %); AMLR alignment is a no-op on the operator
  - Public access to the RPO (business-register BO data) was restricted from 10 Jul 2025 without a legitimate-interest regime; EC infringement procedure 25 Sep 2025. RPVS itself remains public by design (procurement transparency)
- **Notes:**
  - RPVS publishes no mechanism/percentage per KUV — hence unknownInterest with true in the mapper
- **Sources:**
  - <https://www.slov-lex.sk/pravne-predpisy/SK/ZZ/2008/297/#paragraf-6a>
  - <https://rpvs.gov.sk/rpvs>
  - <https://www.aksamec.sk/konecny-uzivatel-vyhod/>
- **Last verified:** 2026-08-30 — **review status: verified**

## `sec_edgar` — SEC EDGAR Schedule 13D/13G filings

- **Jurisdiction:** United States (US)
- **Regime kind:** securities_disclosure
- **Legal basis:**
  - Securities Exchange Act of 1934 §§ 13(d), 13(g)
  - 17 CFR 240.13d-3 (Rule 13d-3 — definition of beneficial owner)
- **Definition:** A DIFFERENT legal concept from AML beneficial ownership: any person who directly or indirectly has or shares VOTING power and/or INVESTMENT (dispositive) power over a security. No economic-benefit test — an adviser voting client shares is a 13d-3 beneficial owner.
- **Threshold:** more than 5 % of a class of registered voting equity securities (`> 5 %`)
- **Reporting basis:** Direct disclosure by the filer; both natural persons and entities file
- **Natural person only:** no
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `filer_natural_person` → `assert_true`
  - `filer_custodial` → `omit`
  - `filer_entity` → `assert_false`
- **Notes:**
  - Implemented in Phase C (2026-08-30): person filers carry a Rule 13d-3 label in interest.details; entity filers now get false (was true — entity parties never carry the flag)
- **Sources:**
  - <https://www.ecfr.gov/current/title-17/chapter-II/part-240/subject-group-ECFR9dc4a11b1e6d51b/section-240.13d-3>
- **Last verified:** 2026-08-30 — **review status: verified**

## `cac_nigeria` — CAC register of Persons with Significant Control / beneficial ownership (bor.cac.gov.ng)

- **Jurisdiction:** Nigeria (NG)
- **Regime kind:** bo_register
- **Legal basis:**
  - Companies and Allied Matters Act (CAMA) 2020, ss. 119-120
  - Persons with Significant Control Regulations 2022
- **Definition:** Significant control at a 5 % threshold: holding directly or indirectly at least 5 % of shares or voting rights, right to appoint/remove directors, or otherwise exercising significant influence or control.
- **Threshold:** at least 5 % (`>= 5 %`)
- **Reporting basis:** Direct disclosure of persons with significant control; one of the lowest thresholds globally (EITI-influenced)
- **Natural person only:** no
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `psc_natural_person` → `assert_true`
  - `psc_corporate` → `assert_false`
- **Sources:**
  - <https://bor.cac.gov.ng/>
  - <https://www.openownership.org/en/blog/nigeria-and-the-beneficial-ownership-data-standard/>
- **Last verified:** 2026-08-30 — **review status: verified**

## `bods_gleif` — GLEIF Level 2 relationship records via Open Ownership BODS 0.4 dataset

- **Jurisdiction:** Global (GLEIF)
- **Regime kind:** consolidation
- **Legal basis:**
  - GLEIF RR-CDF: 'ultimate/direct accounting consolidating parent' per IFRS 10 / US GAAP consolidation — NOT a beneficial ownership concept
- **Definition:** None — accounting consolidation between legal entities; no natural persons appear.
- **Reporting basis:** Entity-to-entity consolidation links, self-reported by LEI holders
- **Natural person only:** no
- **Fallback:** Reporting exceptions (with reason codes) where a parent is not reported
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `relationship` → `copy_verbatim`
- **Notes:**
  - Native gleif adapter likewise emits false on parent/child links (entity parties)
- **Sources:**
  - <https://bods-data.openownership.org/source/gleif_version_0_4/>
- **Last verified:** 2026-08-30 — **review status: verified**

## `wikidata` — Wikidata P127 (owned by) / P749 (parent organization)

- **Jurisdiction:** None (crowdsourced)
- **Regime kind:** crowdsourced
- **Definition:** None — community-maintained claims with no legal disclosure regime or threshold behind them.
- **Reporting basis:** Unverified crowdsourced statements, sometimes referenced to press or registers
- **Natural person only:** no
- **`beneficialOwnershipOrControl` policy per record kind:**
  - `owner_natural_person` → `omit`
  - `owner_entity` → `assert_false`
- **Notes:**
  - person->true leak fixed in Phase C (2026-08-30): mapper now omits the flag for person owners
- **Sources:**
  - <https://www.wikidata.org/wiki/Property:P127>
- **Last verified:** 2026-08-30 — **review status: verified**

