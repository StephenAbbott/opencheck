"""Per-jurisdiction beneficial ownership regimes — the legal rules registry.

Phase B deliverable of the ``beneficialOwnershipOrControl`` audit (Notion:
"Audit of beneficialOwnershipOrControl property", 2026-08-28).

BODS embeds NO intrinsic definition of "beneficial owner": the flag marks an
interest known to constitute beneficial ownership *under the applicable
jurisdiction's definition* (standard.openownership.org, primer/datamodel).
That makes the applicable definition a per-source fact this codebase must
carry.  This module is the single place it lives — the mapper, the tests,
the docs page (``docs/bo-regimes.md``, generated from this file) and future
audits all read from here.

Two levels of rule interact:

* **BODS-level (jurisdiction-independent):** a beneficial owner is a natural
  person, so a relationship whose interested party is an ENTITY never carries
  ``beneficialOwnershipOrControl: true`` — Open Ownership's own UK PSC and
  GLEIF pipelines emit ``false`` on every entity-party relationship (verified
  against data/demo/*.jsonl, 2026-08-28: 2,471 entity rows all false, 44
  person rows all true).
* **Regime-level (this registry):** whether a given RECORD KIND in a given
  source is a beneficial ownership declaration (assert ``true``), a
  registered/legal holding the register makes no BO claim about (OMIT the
  flag — "not stated"), or a verbatim copy of someone else's BODS output.

``review_status``: entries were manually checked by Stephen against the
current legislation on 2026-08-30 ("to the best of my ability" — treat as a
good-faith confirmation, not legal advice).  ``last_verified`` records when a
human last confirmed an entry against primary sources — treat entries older
than ~6 months as stale (several EU frameworks are moving through AMLD6/AMLR
alignment; see ``EU_AML_PACKAGE``).  WATCH ITEM (Stephen, 2026-08-30): whether
Estonia and Latvia amend their definitions from "more than 25 %" to the
AMLR's "25 % or more" ahead of, or at, the 10 July 2027 application date.
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ----------------------------------------------------------------------
# EU horizon — the package that will move most of the thresholds below
# ----------------------------------------------------------------------
# AMLR (Regulation (EU) 2024/1624) Arts 51–54: beneficial ownership through
# ownership interest becomes "25 % or more" (>= 25, NOT the AMLD4 "more than
# 25 %"), assessed via multiplication of holdings at each level of the chain
# and addition across chains; control (Art 53) is assessed independently of
# ownership; where no BO is identified, ALL senior managing officials are
# recorded (Arts 22, 63).  The AMLR applies directly from 10 July 2027 — the
# EE/LV thresholds below (currently "more than 25 %") change on that date
# unless amended earlier; Slovakia is already at "at least 25 %".
#
# AMLD6 (Directive (EU) 2024/1640) governs the registers: staggered
# deadlines — certain register-access provisions from 10 July 2025 (the
# European Commission opened infringement procedures on 25 Sep 2025 against
# BE, DK, DE, EE, GR, IT, CY, HR, PL, SK and SE over comprehensive access),
# legitimate-interest access articles (Arts 11–13, 15) from 10 July 2026,
# full transposition by 10 July 2027.  (Dates confirmed in Stephen's manual
# pass, 2026-08-30.)
EU_AML_PACKAGE: dict[str, str] = {
    "amlr": "Regulation (EU) 2024/1624 (AMLR), Arts 22, 51-54, 63",
    "amlr_threshold": "25 % or more (>=), multiplied through chains, summed across chains",
    "amlr_applies_from": "2027-07-10",
    "amld6": "Directive (EU) 2024/1640 (AMLD6), register + access provisions",
    "amld6_access_phase1": "2025-07-10",
    "amld6_access_phase2_arts_11_12_13_15": "2026-07-10",
    "amld6_full_transposition": "2027-07-10",
}


@dataclass(frozen=True)
class BORegime:
    """The beneficial ownership rules that govern one OpenCheck source.

    ``record_kinds`` maps each kind of record the adapter emits to the
    ``beneficialOwnershipOrControl`` policy for it:

    * ``"assert_true"``     — the record IS a BO declaration under this regime
    * ``"assert_false"``    — the record can never constitute BO (definitional:
                              entity interested parties)
    * ``"omit"``            — the register makes no BO claim; emit nothing
    * ``"copy_verbatim"``   — upstream BODS data; the flag passes through
    * ``"per_statement_code"`` — PSC-statement style: depends on the code
    """

    source_id: str
    jurisdiction: str
    jurisdiction_code: str | None
    register_name: str
    regime_kind: str  # bo_register | securities_disclosure | consolidation | company_register | crowdsourced
    legal_basis: tuple[str, ...]
    bo_definition: str
    threshold_wording: str | None   # exact statutory wording, quoted
    threshold_operator: str | None  # ">" or ">="
    threshold_value: float | None   # percent
    reporting_basis: str            # what the register actually records
    natural_person_only: bool       # does the REGIME require a natural person?
    fallback: str | None
    record_kinds: dict[str, str]
    pending_changes: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    last_verified: str = "2026-08-30"
    review_status: str = "verified"  # manually checked by Stephen against current legislation, 2026-08-30


REGIMES: dict[str, BORegime] = {}


def _add(regime: BORegime) -> None:
    REGIMES[regime.source_id] = regime


# ----------------------------------------------------------------------
# United Kingdom — PSC regime (live Companies House adapter)
# ----------------------------------------------------------------------
_add(BORegime(
    source_id="companies_house",
    jurisdiction="United Kingdom",
    jurisdiction_code="GB",
    register_name="Companies House register of People with Significant Control (PSC)",
    regime_kind="bo_register",
    legal_basis=(
        "Companies Act 2006, Part 21A and Schedule 1A (inserted by SBEEA 2015)",
        "The Register of People with Significant Control Regulations 2016 (SI 2016/339)",
        "Economic Crime and Corporate Transparency Act 2023 (identity verification; central-register reforms)",
        "The Register of People with Significant Control (Amendment) Regulations 2025 (SI 2025/1036)",
    ),
    bo_definition=(
        "A PSC meets any of five conditions: (1) holds, directly or indirectly, "
        "more than 25 % of shares; (2) more than 25 % of voting rights; (3) the "
        "right to appoint or remove a majority of the board; (4) significant "
        "influence or control; (5) significant influence or control over a "
        "trust or firm that itself meets a condition."
    ),
    threshold_wording="more than 25 %",
    threshold_operator=">",
    threshold_value=25.0,
    reporting_basis=(
        "FIRST QUALIFYING LINK, not necessarily the ultimate natural person: a "
        "company records the first registrable person OR registrable relevant "
        "legal entity (RLE) in each chain. A chain therefore legitimately stops "
        "at a corporate PSC — the UK register is not an ultimate-BO register."
    ),
    natural_person_only=False,  # RLEs are registrable PSCs
    fallback="Statement codes (no PSC identified / steps not completed / super-secure etc.), not senior management",
    record_kinds={
        "psc_individual": "assert_true",
        "psc_corporate_rle": "assert_false",   # entity interested party — definitional
        "psc_statement": "per_statement_code",
        "officer_director": "omit",            # decision 2026-08-28: was assert_false
    },
    pending_changes=(
        "ECCTA is delivered through ~50 statutory instruments, many still pending "
        "(see Stephen's Notion tracker 'Track implementation of ECCTA', 1b57f3dc292880a5a6c6fef6d927638e)",
        "Identity verification: voluntary from 8 Apr 2025; mandatory rollout from 18 Nov 2025 with a "
        "12-month transition for existing directors and PSCs (~4M verified by Jun 2026); no threshold change",
        "PSC statutory guidance on 'significant influence or control' reissued 4 Mar 2026 "
        "(2026 company + LLP statutory guidance) — condition-4 interpretation source",
        "Updated 2026 PSC-reporting guidance published Jan 2026 but NOT yet in force pending parliamentary approval",
        "Register of Overseas Entities: not to be solely relied on for verifying BO (Reg 28, ECCTA "
        "Consequential Provisions Regs 2025); ROE brought into the discrepancy-reporting regime (Reg 30A)",
        "English Limited Partnership PSC loophole NOT fixed by ECCTA (BBC/Finance Uncovered, Nov 2023)",
        "UK is outside the EU AML package — AMLR >=25 % does NOT apply",
    ),
    notes=(
        "UK government has formally adopted BODS as its open standard for BO data",
        "OpenCheck matches Open Ownership's bulk-pipeline behaviour: individual PSC -> true, RLE -> false",
    ),
    sources=(
        "https://www.legislation.gov.uk/ukpga/2006/46/schedule/1A",
        "https://www.gov.uk/guidance/people-with-significant-control-pscs",
        "https://www.legislation.gov.uk/uksi/2025/1036/note/made",
    ),
))

# The Open Ownership UK PSC bulk dataset: same regime, flag copied verbatim.
_add(BORegime(
    source_id="bods_uk_psc",
    jurisdiction="United Kingdom",
    jurisdiction_code="GB",
    register_name="Open Ownership UK PSC bulk BODS dataset (register v2 pipeline)",
    regime_kind="bo_register",
    legal_basis=("As companies_house — OO republishes the CH PSC register as BODS",),
    bo_definition="As companies_house.",
    threshold_wording="more than 25 %",
    threshold_operator=">",
    threshold_value=25.0,
    reporting_basis="As companies_house; OO pipeline emits true only for natural-person interested parties",
    natural_person_only=False,
    fallback="As companies_house",
    record_kinds={"relationship": "copy_verbatim"},
    notes=(
        "Verified 2026-08-28 on data/demo: person->true (44), entity->false (1,899), no exceptions",
    ),
    sources=("https://bods-data.openownership.org/",),
))


# ----------------------------------------------------------------------
# Estonia — äriregister (e-Business Register) kasusaajad
# ----------------------------------------------------------------------
_add(BORegime(
    source_id="ariregister",
    jurisdiction="Estonia",
    jurisdiction_code="EE",
    register_name="e-äriregister — tegelikud kasusaajad (beneficial owners) alongside shareholders/officers",
    regime_kind="bo_register",
    legal_basis=(
        "Rahapesu ja terrorismi rahastamise tõkestamise seadus (RahaPTS) § 9 (definition)",
        "RahaPTS §§ 76-77 (duty to submit BO data to the business register)",
    ),
    bo_definition=(
        "The natural person who, taking advantage of their influence, ultimately "
        "owns or controls the legal person. Direct ownership: shareholding/"
        "ownership interest of more than 25 % held by the natural person. "
        "Indirect ownership: a company controlled by the natural person (alone "
        "or together) holds more than 25 %."
    ),
    threshold_wording="more than 25 % (üle 25 %)",
    threshold_operator=">",
    threshold_value=25.0,
    reporting_basis="ULTIMATE beneficial owner (through chains), with control-mechanism code per BO",
    natural_person_only=True,
    fallback="Senior management (juhatus) recorded where no qualifying person is identified",
    record_kinds={
        "kasusaaja_bo": "assert_true",
        "shareholder": "omit",
        "officer": "omit",
        "corporate_shareholder": "omit",  # entity party; register makes no BO claim on the holding itself
    },
    pending_changes=(
        "Public-access restriction (legitimate-interest regime) drafted to start 10 Jul 2026; "
        "Justice Minister refused to endorse the bill 25 Jun 2026; change POSTPONED, no new date (ERR, err.ee/1610074771)",
        "EC infringement procedure opened 25 Sep 2025 over 6AMLD register-access compliance",
        "AMLR >=25 % + chain multiplication applies from 10 Jul 2027",
    ),
    notes=(
        "Adapter carries include_beneficial_owners kill-switch for the day access is restricted",
        "Stephen's tracking ticket: 'Track EU legitimate interest changes' (Notion 38b7f3dc...)",
        "Sheet 'EU BO LIA tracker' marks EE legitimate-interest access as in place (abiinfo.rik.ee/en/node/367) — reconcile with the postponement during the manual pass",
    ),
    sources=(
        "https://www.riigiteataja.ee/akt/114032025023",
        "https://abiinfo.rik.ee/en/node/367",
        "https://www.err.ee/1610074771/tegelike-kasusaajate-andmete-varjamine-lukkub-edasi",
    ),
))


# ----------------------------------------------------------------------
# Latvia — Uzņēmumu reģistrs (UR)
# ----------------------------------------------------------------------
_add(BORegime(
    source_id="ur_latvia",
    jurisdiction="Latvia",
    jurisdiction_code="LV",
    register_name="Uzņēmumu reģistrs — patiesie labuma guvēji (PLG) register",
    regime_kind="bo_register",
    legal_basis=(
        "Noziedzīgi iegūtu līdzekļu legalizācijas un terorisma un proliferācijas "
        "finansēšanas novēršanas likums (NILLTPFN likums), Art 1(5)(a) (definition)",
        "NILLTPFN likums Ch III.1 (Arts 18.1-18.3, registration with UR)",
    ),
    bo_definition=(
        "The natural person who ultimately owns or directly or indirectly "
        "controls the legal person: 'vairāk nekā 25 % no juridiskās personas "
        "kapitāla daļām vai balsstiesīgajām akcijām' (more than 25 % of capital "
        "shares or voting shares), through direct or indirect participation, or "
        "control by other means."
    ),
    threshold_wording="vairāk nekā 25 % (more than 25 %)",
    threshold_operator=">",
    threshold_value=25.0,
    reporting_basis=(
        "ULTIMATE beneficial owner. UR publishes the BO list WITHOUT the "
        "qualifying mechanism or percentage, and separately publishes members/"
        "shareholders — the two record sets are not linked, which is why the "
        "mapper cannot mark a BO's qualifying shareholding true (audit finding 3)"
    ),
    natural_person_only=True,
    fallback="Board members recorded where BO cannot be established (declaration to that effect)",
    record_kinds={
        "beneficial_owner": "assert_true",
        "member_shareholder": "omit",   # decision 2026-08-28: was assert_false
        "officer": "omit",              # decision 2026-08-28: was assert_false
        "corporate_member": "omit",
    },
    pending_changes=(
        "Register REMAINS public after 1 Jul 2026; from that date a BO may request case-by-case "
        "restriction where disclosure creates a real crime risk to them or family",
        "Chief State Notary decision No 1-5n/85 of 18 Jun 2026 on BO registration practice — obtain and review",
        "AMLR >=25 % + chain multiplication applies from 10 Jul 2027",
    ),
    notes=(
        "Latvia was the first country to publish national BODS data (2021)",
    ),
    sources=(
        "https://www.ur.gov.lv/lv/patieso-labuma-guveju-skaidrojums",
        "https://likumi.lv/ta/id/178987",
        "https://www.err.ee/1610050456/erinevalt-eestist-jatab-lati-tegelike-kasusaajate-registri-avalikuks",
    ),
))


# ----------------------------------------------------------------------
# Slovakia — RPVS (Register partnerov verejného sektora)
# ----------------------------------------------------------------------
_add(BORegime(
    source_id="rpvs_slovakia",
    jurisdiction="Slovakia",
    jurisdiction_code="SK",
    register_name="Register partnerov verejného sektora (RPVS) — konečný užívateľ výhod (KUV)",
    regime_kind="bo_register",
    legal_basis=(
        "Zákon č. 297/2008 Z. z. (AML Act) § 6a (KUV definition)",
        "Zákon č. 315/2016 Z. z. (RPVS Act — registration, verification by oprávnená osoba)",
    ),
    bo_definition=(
        "Always a natural person ('vždy fyzická osoba'): holds 'podiel najmenej "
        "25 % na hlasovacích právach ... alebo na jej základnom imaní' (a share "
        "of AT LEAST 25 % of voting rights or registered capital, direct or "
        "indirect — indirect computed by multiplying stakes through layers); or "
        "right to at least 25 % of profit; or power to appoint/remove governing "
        "bodies; or other decisive influence."
    ),
    threshold_wording="najmenej 25 % (at least 25 %)",
    threshold_operator=">=",
    threshold_value=25.0,
    reporting_basis=(
        "ULTIMATE beneficial owner, VERIFIED by an oprávnená osoba (authorised "
        "professional) who files and legally answers for the identification — "
        "the strongest verification regime of the four"
    ),
    natural_person_only=True,
    fallback="Members of vrcholový manažment (senior management) where no qualifying person exists — exception, requires documented analysis",
    record_kinds={
        "kuv": "assert_true",
        "corporate_party": "omit",
    },
    pending_changes=(
        "NOTE: Slovakia's threshold is ALREADY >=25 % — it differs from UK/EE/LV (>25 %); AMLR alignment is a no-op on the operator",
        "Public access to the RPO (business-register BO data) was restricted from 10 Jul 2025 without a legitimate-interest regime; "
        "EC infringement procedure 25 Sep 2025. RPVS itself remains public by design (procurement transparency)",
    ),
    notes=(
        "RPVS publishes no mechanism/percentage per KUV — hence unknownInterest with true in the mapper",
    ),
    sources=(
        "https://www.slov-lex.sk/pravne-predpisy/SK/ZZ/2008/297/#paragraf-6a",
        "https://rpvs.gov.sk/rpvs",
        "https://www.aksamec.sk/konecny-uzivatel-vyhod/",
    ),
))


# ----------------------------------------------------------------------
# Secondary asserting regimes
# ----------------------------------------------------------------------
_add(BORegime(
    source_id="sec_edgar",
    jurisdiction="United States",
    jurisdiction_code="US",
    register_name="SEC EDGAR Schedule 13D/13G filings",
    regime_kind="securities_disclosure",
    legal_basis=(
        "Securities Exchange Act of 1934 §§ 13(d), 13(g)",
        "17 CFR 240.13d-3 (Rule 13d-3 — definition of beneficial owner)",
    ),
    bo_definition=(
        "A DIFFERENT legal concept from AML beneficial ownership: any person "
        "who directly or indirectly has or shares VOTING power and/or "
        "INVESTMENT (dispositive) power over a security. No economic-benefit "
        "test — an adviser voting client shares is a 13d-3 beneficial owner."
    ),
    threshold_wording="more than 5 % of a class of registered voting equity securities",
    threshold_operator=">",
    threshold_value=5.0,
    reporting_basis="Direct disclosure by the filer; both natural persons and entities file",
    natural_person_only=False,
    fallback=None,
    record_kinds={
        "filer_natural_person": "assert_true",   # true UNDER THE SEC DEFINITION — must be labelled as such in details
        "filer_custodial": "omit",               # IA/BD/IC/BK/EP/SA reporting-capacity codes
        "filer_entity": "assert_false",          # entity interested party — definitional
    },
    notes=(
        "Implemented in Phase C (2026-08-30): person filers carry a Rule 13d-3 label in interest.details; "
        "entity filers now get false (was true — entity parties never carry the flag)",
    ),
    sources=(
        "https://www.ecfr.gov/current/title-17/chapter-II/part-240/subject-group-ECFR9dc4a11b1e6d51b/section-240.13d-3",
    ),
))

_add(BORegime(
    source_id="cac_nigeria",
    jurisdiction="Nigeria",
    jurisdiction_code="NG",
    register_name="CAC register of Persons with Significant Control / beneficial ownership (bor.cac.gov.ng)",
    regime_kind="bo_register",
    legal_basis=(
        "Companies and Allied Matters Act (CAMA) 2020, ss. 119-120",
        "Persons with Significant Control Regulations 2022",
    ),
    bo_definition=(
        "Significant control at a 5 % threshold: holding directly or indirectly "
        "at least 5 % of shares or voting rights, right to appoint/remove "
        "directors, or otherwise exercising significant influence or control."
    ),
    threshold_wording="at least 5 %",
    threshold_operator=">=",
    threshold_value=5.0,
    reporting_basis="Direct disclosure of persons with significant control; one of the lowest thresholds globally (EITI-influenced)",
    natural_person_only=False,
    fallback=None,
    record_kinds={
        "psc_natural_person": "assert_true",
        "psc_corporate": "assert_false",
    },
    sources=(
        "https://bor.cac.gov.ng/",
        "https://www.openownership.org/en/blog/nigeria-and-the-beneficial-ownership-data-standard/",
    ),
))

_add(BORegime(
    source_id="bods_gleif",
    jurisdiction="Global (GLEIF)",
    jurisdiction_code=None,
    register_name="GLEIF Level 2 relationship records via Open Ownership BODS 0.4 dataset",
    regime_kind="consolidation",
    legal_basis=(
        "GLEIF RR-CDF: 'ultimate/direct accounting consolidating parent' per "
        "IFRS 10 / US GAAP consolidation — NOT a beneficial ownership concept",
    ),
    bo_definition="None — accounting consolidation between legal entities; no natural persons appear.",
    threshold_wording=None,
    threshold_operator=None,
    threshold_value=None,
    reporting_basis="Entity-to-entity consolidation links, self-reported by LEI holders",
    natural_person_only=False,
    fallback="Reporting exceptions (with reason codes) where a parent is not reported",
    record_kinds={"relationship": "copy_verbatim"},  # OO emits false on every entity link
    notes=("Native gleif adapter likewise emits false on parent/child links (entity parties)",),
    sources=("https://bods-data.openownership.org/source/gleif_version_0_4/",),
))

_add(BORegime(
    source_id="wikidata",
    jurisdiction="None (crowdsourced)",
    jurisdiction_code=None,
    register_name="Wikidata P127 (owned by) / P749 (parent organization)",
    regime_kind="crowdsourced",
    legal_basis=(),
    bo_definition="None — community-maintained claims with no legal disclosure regime or threshold behind them.",
    threshold_wording=None,
    threshold_operator=None,
    threshold_value=None,
    reporting_basis="Unverified crowdsourced statements, sometimes referenced to press or registers",
    natural_person_only=False,
    fallback=None,
    record_kinds={
        "owner_natural_person": "omit",   # decision 2026-08-28: was assert_true — audit finding 4
        "owner_entity": "assert_false",   # entity interested party — definitional
    },
    notes=("person->true leak fixed in Phase C (2026-08-30): mapper now omits the flag for person owners",),
    sources=("https://www.wikidata.org/wiki/Property:P127",),
))


# ----------------------------------------------------------------------
# Lookups
# ----------------------------------------------------------------------
def get_regime(source_id: str) -> BORegime | None:
    """The BO regime governing *source_id*, or None for non-asserting sources."""
    return REGIMES.get(source_id)


def boc_policy(source_id: str, record_kind: str) -> str:
    """The beneficialOwnershipOrControl policy for one record kind.

    Unknown source or record kind -> ``"omit"`` — the conservative default the
    Phase 102 policy already enforces for commercial registers.
    """
    regime = REGIMES.get(source_id)
    if regime is None:
        return "omit"
    return regime.record_kinds.get(record_kind, "omit")
