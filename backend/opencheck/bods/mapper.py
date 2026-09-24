"""Map source payloads to BODS v0.4 statements.

BODS v0.4 statements come in three kinds — entity, person, relationship —
each wrapped in a ``recordDetails`` object. OpenCheck uses deterministic
statement IDs derived from the source adapter ID plus a stable local key,
so re-mapping the same payload always produces the same IDs. This matters
for deduplication across runs and for the visualisation library, which
keys on statement IDs.

Reference: https://standard.openownership.org/en/0.4.0/
"""

from __future__ import annotations

import re
from typing import Any

import pycountry

from ..elf import resolve_elf
from . import liveness as _liveness
from .unique import unique_statements
from .annotations import annotate, commenting

# Phase 168 moved the statement factories and the two largest per-source
# sections into their own modules; Phase 246 moved every other source's
# section to ``bods/mappers/<country or source>.py``, leaving here only the
# GLEIF mapper (the anchor every lookup starts from), its RA-code and US-state
# tables and the Open Ownership / MEIP passthroughs. Everything is re-exported
# here — including the
# private helpers — because 62 files import them from `bods.mapper`, and
# `sources/probes.py` resolves a mapper by `getattr(mapper, name)`. Moving
# code is not the same as moving its address.
from .statements import (  # noqa: F401  (re-exported: see the module docstring)
    BODSBundle,
    SOURCE_NAMES,
    _BO_ASSERTING_SOURCES,
    _CH_UK_COUNTRY_STRINGS,
    _INTEREST_PREFIX,
    _SHARE_BAND_RE,
    _addr,
    _birth_date_precision_note,
    _country_code,
    _country_obj,
    _parse_nature,
    _publication_details_block,
    _source_block,
    _stable_id,
    _statement_date,
    _today,
    is_majority_stake,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
    source_may_assert_beneficial_ownership,
)

from .mappers.ftm import (  # noqa: F401  (re-exported: see the module docstring)
    _FTM_BO_ASSERTING_DATASETS,
    _FTM_EDGE_SCHEMAS,
    _FTM_ENTITY_SCHEMAS,
    _FTM_PERSON_SCHEMAS,
    _FTM_ROLE_TO_INTEREST_TYPE,
    _FTM_SUBJECT,
    _ftm_addresses,
    _ftm_asserts_beneficial_ownership,
    _ftm_edge_interest,
    _ftm_edge_relationships,
    _ftm_entity_statement,
    _ftm_identifiers,
    _ftm_jurisdiction,
    _ftm_percentage,
    _ftm_person_statement,
    _ftm_resolve_nationality,
    _ftm_statement,
    map_everypolitician,
    map_ftm,
    map_openaleph,
    map_opensanctions,
)

from .mappers.wikidata import (  # noqa: F401  (re-exported: see the module docstring)
    _OC_POSITION_TO_INTEREST_TYPE,
    _OC_RELATIONSHIP_TYPE_TO_INTEREST,
    _emit_wikidata_owner,
    _normalise_wikidata_date,
    _oc_build_interests_from_relationship,
    _oc_match_position,
    _oc_parse_network_relationships,
    _wikidata_jurisdiction,
    map_opencorporates,
    map_wikidata,
)

from .mappers.companies_house import (  # noqa: F401  (re-exported, Phase 246)
    _ChCompany,
    _ChPscHop,
    _MANAGING_OFFICIAL_ROLES,
    _PSC_STATEMENT_NO_BO,
    _PSC_STATEMENT_REASON,
    _annotate_verified_people,
    _ch_director_statements,
    _ch_officer_grouping_note,
    _ch_officer_id,
    _ch_officer_local_id,
    _ch_officer_person_local_id,
    _ch_officer_url,
    _ch_psc_statement_statements,
    _emit_company_statements,
    _map_companies_house_officer,
    _map_corporate_psc,
    _map_individual_psc,
    _profile_addresses,
    _rollup_ch_chains,
    map_companies_house,
)
from .mappers.greece import (  # noqa: F401  (re-exported, Phase 246)
    _GEMI_CATEGORY_INTERESTS,
    _GEMI_ISO_DATE,
    _GEMI_MANAGER_TOKENS,
    _GEMI_PARTNER_CLASSES,
    _gemi,
    _gemi_addresses,
    _gemi_date,
    _gemi_identifiers,
    _gemi_interests,
    _gemi_is_past,
    _gemi_partner_class,
    _gemi_person_local_id,
    map_gemi_greece,
)
from .mappers.switzerland import (  # noqa: F401  (re-exported, Phase 246)
    _ZEFIX_CANTON_TO_NAME,
    _ZEFIX_UID_RE,
    _re,
    _zefix_address,
    _zefix_format_uid,
    map_zefix,
)
from .mappers.netherlands import (  # noqa: F401  (re-exported, Phase 246)
    map_kvk,
)
from .mappers.france import (  # noqa: F401  (re-exported, Phase 246)
    _INPI_OTHER_INFLUENCE_CODES,
    _INPI_ROLE_LABELS,
    _inpi_address,
    _inpi_individu_statements,
    _inpi_role_interest_type,
    map_inpi,
)
from .mappers.sweden import (  # noqa: F401  (re-exported, Phase 246)
    _bv_address,
    map_bolagsverket,
)
from .mappers.croatia import (  # noqa: F401  (re-exported, Phase 246)
    _SUDREG_SOURCE_URL,
    _sudreg_address,
    map_sudreg_croatia,
)
from .mappers.eiti import (  # noqa: F401  (re-exported, Phase 246)
    _EITI_MATCH_METHODS,
    _EITI_SCHEME_BY_COUNTRY,
    _EITI_SOE_URL,
    _eiti_assessment_latest_year,
    _eiti_assessment_lei_match,
    _norm_for_compare,
    map_eiti,
    map_eiti_assessment,
    map_eiti_soe,
)
from .mappers.wikirate import (  # noqa: F401  (re-exported, Phase 246)
    _WIKIRATE_IDENTIFIER_SCHEMES,
    map_wikirate,
)
from .mappers.ted import (  # noqa: F401  (re-exported, Phase 246)
    map_ted_eu,
)
from .mappers.estonia import (  # noqa: F401  (re-exported, Phase 246)
    _EE_BO_CONTROL_MAP,
    _EE_OFFICER_ROLE_MAP,
    _ee_date,
    _ee_full_name,
    _ee_person_id,
    map_ariregister,
)
from .mappers.brightquery import (  # noqa: F401  (re-exported, Phase 246)
    _BQ_IDENTIFIER_MAP,
    _BQ_SOURCE_URL,
    _bq_features,
    _bq_get_feature,
    _bq_get_value,
    _bq_other_ids,
    map_brightquery,
)
from .mappers.sec_edgar import (  # noqa: F401  (re-exported, Phase 246)
    _SEC_CUSTODIAL_REPORTER_CODES,
    _iso2_to_country_name,
    _pycountry,
    _sec_beneficial_ownership,
    map_sec_edgar,
)
from .mappers.norway import (  # noqa: F401  (re-exported, Phase 246)
    _BRREG_ROLE_MAP,
    _brreg_address,
    _brreg_full_name,
    _brreg_person_local_id,
    _company_url_brreg,
    map_brreg,
)
from .mappers.ireland import (  # noqa: F401  (re-exported, Phase 246)
    _CRO_ENTITY_TYPES,
    _cro_address,
    _cro_entity_type,
    map_cro,
)
from .mappers.malta import (  # noqa: F401  (re-exported, Phase 246)
    map_malta_mbr,
)
from .mappers.hong_kong import (  # noqa: F401  (re-exported, Phase 246)
    map_cr_hongkong,
)
from .mappers.brazil import (  # noqa: F401  (re-exported, Phase 246)
    _BR_OWNER_KEYWORDS,
    _br_interest_type,
    map_cnpj_brazil,
)
from .mappers.new_zealand import (  # noqa: F401  (re-exported, Phase 246)
    _nz_ids,
    map_nz_companies,
)
from .mappers.finland import (  # noqa: F401  (re-exported, Phase 246)
    _PRH_ENTITY_TYPES,
    _prh_address,
    _prh_current_name,
    _prh_entity_type,
    map_prh,
)
from .mappers.latvia import (  # noqa: F401  (re-exported, Phase 246)
    _LV_ENTITY_TYPES,
    _LV_GOVERNING_BODY_INTEREST,
    _lv_date,
    _lv_nationality,
    map_ur_latvia,
)
from .mappers.climatetrace import (  # noqa: F401  (re-exported, Phase 246)
    _GEM_ENTITY_TYPE_MAP,
    _GEM_UNIDENTIFIED_OWNERS,
    _GEM_UNIDENTIFIED_OWNER_IDS,
    _GEM_UNIDENTIFIED_REASON,
    _emit_gem_interested_party,
    _gem_entity_type,
    _gem_status_note,
    _gem_unidentified_owner,
    map_climatetrace,
)
from .mappers.austria import (  # noqa: F401  (re-exported, Phase 246)
    _at_date_iso,
    map_firmenbuch,
)
from .mappers.nigeria import (  # noqa: F401  (re-exported, Phase 246)
    _CAC_UNNAMED_DETAILS,
    _CAC_UNNAMED_OWNER,
    _cac_interests,
    _cac_merge_interests,
    _cac_owner_statements,
    map_cac_nigeria,
)
from .mappers.lithuania import (  # noqa: F401  (re-exported, Phase 246)
    _LT_ENTITY_TYPES,
    map_jar_lithuania,
)
from .mappers.czechia import (  # noqa: F401  (re-exported, Phase 246)
    map_ares,
)
from .mappers.poland import (  # noqa: F401  (re-exported, Phase 246)
    map_krs_poland,
)
from .mappers.slovakia import (  # noqa: F401  (re-exported, Phase 246)
    map_rpo_slovakia,
    map_rpvs_slovakia,
)
from .mappers.belgium import (  # noqa: F401  (re-exported, Phase 246)
    map_bce_belgium,
)
from .mappers.canada import (  # noqa: F401  (re-exported, Phase 246)
    _cc_corp_url,
    _cc_current_name,
    map_corporations_canada,
)
from .mappers.singapore import (  # noqa: F401  (re-exported, Phase 246)
    _ACRA_LIVE_LABELS,
    _ACRA_PENDING_LABELS,
    _ACRA_TERMINAL_LABELS,
    _acra_address,
    _acra_liveness,
    map_acra_singapore,
)
from .mappers.denmark import (  # noqa: F401  (re-exported, Phase 246)
    map_cvr_denmark,
)
from .mappers.cyprus import (  # noqa: F401  (re-exported, Phase 246)
    _CY_ORG_OFFICIAL_TOKENS,
    _cy_official_is_org,
    map_cyprus_drcor,
)
from .mappers.australia import (  # noqa: F401  (re-exported, Phase 246)
    map_abr_australia,
)
from .mappers.india import (  # noqa: F401  (re-exported, Phase 246)
    _MCA_LIVE_STATUSES,
    _MCA_PENDING_STATUSES,
    _MCA_TERMINAL_STATUSES,
    map_mca_india,
)
from .mappers.eiti_bo import (  # noqa: F401  (re-exported, Phase 246)
    _BODS02_INTEREST_TYPES,
    _DRC_HONORIFIC_RE,
    _DRC_NATIONALITIES,
    _eiti_bo_armenia_subject_orig_id,
    _eiti_bo_date,
    _eiti_bo_map_armenia,
    _eiti_bo_map_drc,
    _eiti_bo_map_nigeria,
    _norm_or_empty,
    map_eiti_bo,
)
from .mappers.ukraine import (  # noqa: F401  (re-exported, Phase 246)
    _UA_CAPITAL_TOLERANCE,
    _UA_CHAIR_TOKENS,
    _UA_JURISDICTION,
    _UA_SCHEME,
    _UA_SCHEME_NAME,
    _ua_bo_interests,
    _ua_derived_shares,
    _ua_person_or_entity,
    map_edr_ukraine,
)
from .mappers.romania import (  # noqa: F401  (re-exported, Phase 246)
    RO_CUI_SCHEME,
    RO_CUI_SCHEME_NAME,
    RO_ONRC_SCHEME,
    RO_ONRC_SCHEME_NAME,
    RO_ROLE_INTEREST,
    _RO_PENDING_STATUS,
    _RO_TERMINAL_STATUS,
    _anaf_address,
    _anaf_date,
    _ro_address,
    _ro_liveness,
    map_anaf_romania,
    map_onrc_romania,
)
from .mappers.moldova import (  # noqa: F401  (re-exported, Phase 246)
    MD_IDNO_SCHEME,
    MD_IDNO_SCHEME_NAME,
    _MD_JURISDICTION,
    _md_person_key,
    map_asp_moldova,
)
from .mappers.serbia import (  # noqa: F401  (re-exported, Phase 246)
    _RS_JURISDICTION,
    map_apr_serbia,
)
from .mappers.us_dc import (  # noqa: F401  (re-exported, Phase 246)
    _DC_INTEREST_DETAILS,
    _DC_JURISDICTION,
    _DC_UNNAMED_OWNER,
    _dc_business_address,
    _dc_epoch_to_date,
    map_dlcp_dc,
)


def ch_person_statement_id(company_number: str, officer: dict[str, Any]) -> str:
    """The ``statementId`` :func:`_ch_director_statements` gives this officer.

    Public because the Time Machine needs it: a board row on the History tab
    addresses the same person the graph draws, and the only safe way to know
    that id is to derive it the way the mapper does (Phase 198). Deriving it
    anywhere else would be two spellings of one identity, which is the class
    of bug Phase 193 spent a phase removing.
    """
    return _stable_id(
        "companies_house", "person", _ch_officer_person_local_id(company_number, officer)
    )


# ----------------------------------------------------------------------
# GLEIF → BODS
# ----------------------------------------------------------------------
#
# Mirrors OpenOwnership's canonical GLEIF → BODS pipeline
# (https://github.com/openownership/bods-gleif-pipeline):
#
# * Subject entity: one ``registeredEntity`` statement, identified by LEI
#   (``XI-LEI``) and by the GLEIF ``RegistrationAuthority`` scheme when
#   the record carries a ``registeredAt.id`` (e.g. ``RA000585`` for UK
#   Companies House).
# * Each accounting consolidation parent (direct / ultimate) → one entity
#   statement for the parent + one relationship statement with an
#   ``otherInfluenceOrControl`` interest. ``beneficialOwnershipOrControl``
#   is always ``false`` — LEI-RR captures accounting consolidation, not
#   beneficial ownership.
# * Reporting exceptions (``NO_LEI``, ``NATURAL_PERSONS``,
#   ``NON_CONSOLIDATING`` etc.) produce a bridging statement
#   (``anonymousEntity`` or ``unknownPerson``) plus a relationship whose
#   interest ``details`` carry the GLEIF exception reason — so companies
#   that report "my parent is a natural person" don't silently disappear.

# ---------------------------------------------------------------------------
# RA code → org-id.guide scheme code
#
# Maps GLEIF Registration Authority codes to org-id.guide list identifiers so
# that national company numbers are emitted with a proper ``scheme`` in BODS
# entity statements rather than with ``scheme: ""``.
#
# Source of truth for RA codes: https://api.gleif.org/api/v1/registration-authorities/<RA>
# Source of truth for org-id codes: https://org-id.guide
#
# Extend this dict as further jurisdictions are confirmed.  Unknown RA codes
# fall through to a blank scheme with schemeName "GLEIF Registration Authorities List".
# ---------------------------------------------------------------------------
_GLEIF_RA_TO_ORG_ID: dict[str, tuple[str, str]] = {
    # Estonia — Commercial Register (Äriregister, RIK). EE-RIK until Phase
    # 239, while the ariregister adapter wrote EE-ARIREGISTER for the same
    # registry code, so the two never corroborated. The scheme follows the
    # adapter (the Hong Kong / DC rule).
    "RA000181": ("EE-ARIREGISTER", "Estonian e-Business Register (Äriregister)"),
    # France — Sirene (INSEE)
    "RA000189": ("FR-INSEE", "Sirene — Institut National de la Statistique et des Études Économiques (France)"),
    # France — Registre du Commerce et des Sociétés (Infogreffe). Its
    # ``registeredAs`` is the same SIREN INSEE issues (8,145 active LEIs,
    # plain or grouped in threes, 2026-09-11), so it takes the same scheme —
    # the Hong Kong and Swiss precedent: the scheme follows the number, not
    # the authority. One scheme also keeps one FullCheck hop for France.
    "RA000192": ("FR-INSEE", "SIREN — Registre du Commerce et des Sociétés, Infogreffe (France)"),
    # United States — District of Columbia Corporations Division (DLCP).
    # ``registeredAs`` is the file number on 502 of the 726 US-DC LEI records
    # (2026-09-18). The scheme is the ISO 3166-2 subdivision code, matching
    # what ``_US_STATE_REGISTRY_NAMES`` already stamps on a DC entity reached
    # through GLEIF — one scheme, so the two sources corroborate each other
    # rather than each asserting an identifier the other appears to lack.
    "RA000601": ("US-DC", "District of Columbia Department of Licensing and Consumer Protection"),
    # Netherlands — Kamer van Koophandel (KvK)
    "RA000463": ("NL-KVK", "Netherlands Chamber of Commerce (KvK)"),
    # Sweden — Bolagsverket (Swedish Companies Registration Office). SE-ON
    # until Phase 239; the bolagsverket adapter writes SE-BLV.
    "RA000544": ("SE-BLV", "Swedish Companies Registration Office (Bolagsverket)"),
    # Switzerland — Federal Statistical Office UID Register (uid.admin.ch)
    "RA000548": ("CH-FDJP", "Swiss Commercial Register (Federal Office of Justice)"),
    # Switzerland — Handelsregister / ZEFIX (Federal Office of Justice)
    "RA000549": ("CH-FDJP", "Swiss Commercial Register (Federal Office of Justice)"),
    # United Kingdom — Companies House (England & Wales)
    "RA000585": ("GB-COH", "Companies House"),
    # United Kingdom — Companies House (Northern Ireland)
    "RA000586": ("GB-COH", "Companies House"),
    # United Kingdom — Companies House (Scotland). Missing until 2026-08-28,
    # so Scottish entities carried no scheme code on their BODS identifiers.
    "RA000587": ("GB-COH", "Companies House"),
    # RA000591 = The Pensions Regulator (UK) — not a company registry; no org-id code.
    # Belgium — Crossroads Bank for Enterprises (BCE / KBO)
    "RA000025": ("BE-BCE_KBO", "Crossroads Bank for Enterprises (Belgium)"),
    # Hong Kong — both authorities GLEIF files HK companies under carry the
    # Business Registration Number (the Unique Business Identifier since
    # 27 Dec 2023) in ``registeredAs``: RA000388 Companies Registry and
    # RA000389 Business Registration Office (Inland Revenue Department).
    # Verified on 600 live records, 2026-09-10. Not org-id's HK-CR, which is
    # the old seven-character CR No.
    "RA000388": ("HK-BRN", "Hong Kong Business Registration Number (Unique Business Identifier)"),
    "RA000389": ("HK-BRN", "Hong Kong Business Registration Number (Unique Business Identifier)"),
    # Singapore — ACRA Business Registry. ``registeredAs`` is the Unique Entity
    # Number on 12,292 of 13,326 active SG LEI records (2026-09-10).
    "RA000523": ("SG-ACRA", "Unique Entity Number (UEN) — Accounting and Corporate Regulatory Authority (Singapore)"),
    # Ukraine — all THREE authorities GLEIF files Ukrainian entities under
    # carry the 8-digit EDRPOU in ``registeredAs`` (verified on all 303 live
    # UA legal-address records, 2026-09-14): RA000567 the ЄДР itself,
    # RA001027 the National Bank, RA001026 the securities commission.
    # Romania — the trade register and the tax register file DIFFERENT
    # numbers, so unlike Hong Kong or Ukraine these two RA codes do NOT
    # share a scheme. RA000497 files the ONRC registration number or the
    # fiscal code with nothing to mark which; RA000719 files a fiscal code,
    # 51/51 in the live sample. Verified on 1,200 records, 2026-09-14.
    "RA000497": ("RO-ONRC", "Registrul Comerțului — National Trade Register Office (Romania)"),
    "RA000719": ("RO-CUI", "Cod Unic de Înregistrare — fiscal code (Romania)"),
    # Moldova — every Moldovan LEI record files the 13-digit IDNO, whichever
    # authority it names (55 legal-address MD records, 2026-09-15): RA000451
    # the State Register of Legal Entities itself, RA000950 the National
    # Commission for Financial Markets, RA000951 the National Bank.
    "RA000451": ("MD-IDNO", "IDNO — State Register of Legal Entities (Moldova)"),
    "RA000950": ("MD-IDNO", "IDNO — State Register of Legal Entities (Moldova)"),
    "RA000951": ("MD-IDNO", "IDNO — State Register of Legal Entities (Moldova)"),
    # Serbia — RA000517 is the Business Registers Agency (APR) and files the
    # 8-digit matični broj: 295 of the 304 RS records, 2026-09-17. RA000518
    # (entrepreneurs) and RA000684 (securities regulator fund numbers) do not
    # file a number APR's company register holds.
    "RA000517": ("RS-APR", "Matični broj — Business Registers Agency (Serbia)"),
    "RA000567": ("UA-EDR", "EDRPOU — Unified State Register (Ukraine)"),
    "RA001026": ("UA-EDR", "EDRPOU — Unified State Register (Ukraine)"),
    "RA001027": ("UA-EDR", "EDRPOU — Unified State Register (Ukraine)"),
    # ── Phase 239: every RA code an OpenCheck adapter dispatches on ──────
    # Until then these fell through to ``scheme: ""`` — Equinor exported
    # ``{"id": "923 609 016", "scheme": ""}`` — so a GLEIF record and the
    # register's own record could not corroborate by identifier and FullCheck
    # had no register hop for them. Each scheme is the one the adapter's own
    # mapper writes for the number GLEIF files in ``registeredAs``, so the two
    # meet (the rule RA000192 and the Hong Kong pair already follow). RA codes
    # are the table in CLAUDE.md, verified live against GLEIF 2026-08-28.
    # Norway — Register of Business Enterprises (Foretaksregisteret), and the
    # Central Coordinating Register (Enhetsregisteret), which files the same
    # nine-digit organisation number.
    "RA000472": ("NO-BRC", "Brønnøysund Register Centre — organisation number (Norway)"),
    "RA000473": ("NO-BRC", "Brønnøysund Register Centre — organisation number (Norway)"),
    # Denmark — Central Business Register (Erhvervsstyrelsen)
    "RA000170": ("DK-CVR", "Central Business Register — CVR number (Denmark)"),
    # Ireland — Companies Registration Office
    "RA000402": ("IE-CRO", "Companies Registration Office (Ireland)"),
    # Latvia — Register of Enterprises (Uzņēmumu reģistrs)
    "RA000423": ("LV-UR", "Latvian Register of Enterprises (UR)"),
    # Lithuania — Register of Legal Entities (Registrų centras)
    "RA000430": ("LT-JAR", "Register of Legal Entities (Lithuania)"),
    # Austria — Firmenbuch (BM für Justiz)
    "RA000017": ("AT-FB", "Firmenbuch (Austrian Commercial Register)"),
    # Poland — National Court Register (KRS)
    "RA000484": ("PL-KRS", "National Court Register — KRS number (Poland)"),
    # Slovakia — Business Register; ``registeredAs`` is the IČO, which the
    # rpo_slovakia mapper writes as SK-RPO.
    "RA000526": ("SK-RPO", "Register of Legal Entities — IČO (Slovakia)"),
    # Canada — federal Corporate Registry (Corporations Canada). The
    # provincial registries (RA000073–RA000085) number differently and take
    # the RA-code fallback below.
    "RA000072": ("CA-CORP", "Corporations Canada — federal corporation number"),
    # Croatia — Court Registry (Sudski registar); ``registeredAs`` is the MBS.
    "RA000156": ("HR-MBS", "Court Registry — MBS (Croatia)"),
    # Czechia — Commercial Register (Ministerstvo spravedlnosti); the IČO.
    "RA000163": ("CZ-ICO", "Identification number — IČO (Czechia)"),
    # Cyprus — Department of Registrar of Companies and Intellectual Property
    "RA000161": ("CY-DRCOR", "Registrar of Companies (Cyprus)"),
    # Finland — Business Information System (PRH / YTJ); the Y-tunnus.
    "RA000188": ("FI-PRH", "Finnish Patent and Registration Office — Business ID (Y-tunnus)"),
    # Malta — Malta Business Registry
    "RA000443": ("MT-MBR", "Malta Business Registry"),
    # Australia — ASIC's Register of Companies files the ACN; the Australian
    # Business Register (ATO) files the ABN.
    "RA000014": ("AU-ACN", "Australian Company Number (ASIC)"),
    "RA000013": ("AU-ABN", "Australian Business Number (Australian Business Register)"),
    # New Zealand — Companies Office
    "RA000466": ("NZ-COH", "New Zealand Companies Register"),
    # Brazil — Receita Federal CNPJ register
    "RA000681": ("BR-RFB", "CNPJ — Receita Federal (Brazil)"),
    # India — Ministry of Corporate Affairs (MCA21); the CIN.
    "RA000394": ("IN-MCA", "Ministry of Corporate Affairs — CIN (India)"),
    # Nigeria — Corporate Affairs Commission; the RC number.
    "RA000469": ("NG-CAC", "Corporate Affairs Commission — RC number (Nigeria)"),
    # Greece — General Commercial Registry (ΓΕΜΗ)
    "RA000685": ("GR-GEMI", "General Commercial Registry (ΓΕΜΗ)"),
}

#: GLEIF's two "no Registration Authority List entry" codes — ``RA999999``
#: (an authority not on the list, named in ``registeredAt.other``) and
#: ``RA888888`` (none available). They take the RA-code fallback like any
#: other unmapped code, with ``other`` as the scheme name where GLEIF gives one.
_GLEIF_UNLISTED_RA_CODES: frozenset[str] = frozenset({"RA999999", "RA888888"})


def normalise_registered_as(value: Any) -> str:
    """GLEIF's ``registeredAs`` as a register writes it (Phase 239).

    GLEIF files what each LEI issuer typed: "923 609 016" for a Norwegian
    organisation number, "542 051 180" for a SIREN, "052 266 823" for an ACN.
    A number that is all digits once its spaces go is written without them —
    the form every one of those registers publishes, so a spaced copy never
    met its own register's record. Anything else keeps single spaces, because
    for some registers the space is part of the number (Malta's "C 83807").
    """
    text = " ".join(str(value or "").split())
    compact = text.replace(" ", "")
    return compact if compact.isdigit() else text


def gleif_registration_scheme(
    ra_id: str,
    jurisdiction_code: str | None,
    other: str | None = None,
    registered_as: str = "",
) -> tuple[str, str]:
    """``(scheme, schemeName)`` for a number GLEIF files under ``ra_id``.

    0. New Zealand's Companies Office (RA000466) files the 13-digit NZBN on
       most records and the company number on the rest; the value says
       which (sampled live, 24 Sept 2026).

    1. An RA code in ``_GLEIF_RA_TO_ORG_ID`` → its org-id scheme.
    2. An unmapped RA in a US state → the ISO 3166-2 subdivision code
       (org-id.guide has no per-state entries; see ``_US_STATE_REGISTRY_NAMES``).
    3. Anything else → **the RA code itself** (Stephen, 24 Sept 2026). A blank
       scheme told a reader nothing and let no two statements corroborate;
       ``REG-<country>`` was rejected because ``register_hops`` aliases it to
       the country's one register, and a fund code or a provincial number
       would then be looked up on a register that did not issue it. The RA
       code is exactly what GLEIF asserts — which authority issued the number
       — and cannot alias to anything.
    """
    ra = (ra_id or "").strip().upper()
    if ra == "RA000466" and re.fullmatch(r"94\d{11}", registered_as or ""):
        return "NZ-NZBN", "New Zealand Business Number"
    if ra in _GLEIF_RA_TO_ORG_ID:
        return _GLEIF_RA_TO_ORG_ID[ra]
    jur = (jurisdiction_code or "").strip().upper()
    if jur.startswith("US-") and ra not in _GLEIF_UNLISTED_RA_CODES:
        return jur, _US_STATE_REGISTRY_NAMES.get(jur, f"{jur} company registry")
    other_name = (other or "").strip()
    if ra in _GLEIF_UNLISTED_RA_CODES and other_name:
        return ra, other_name
    return ra, f"GLEIF Registration Authorities List — {ra}"

# ---------------------------------------------------------------------------
# US state company registries
#
# org-id.guide does not carry per-state entries for US company registries.
# GLEIF uses state-level ISO 3166-2 subdivision codes as jurisdiction codes
# (e.g. "US-DE" for Delaware) and assigns a separate RA code to each state
# registry (e.g. RA000602 for Delaware Division of Corporations).
#
# Rather than enumerate all 50+ GLEIF RA codes in _GLEIF_RA_TO_ORG_ID, we
# resolve US-jurisdiction entities by cross-referencing the entity's
# ``jurisdiction`` field: when the RA code is unknown and the jurisdiction
# starts with "US-", we use the ISO 3166-2 subdivision code itself as the
# BODS identifier ``scheme`` and look up the official registry name here.
#
# Scheme example:  {"id": "3954875", "scheme": "US-DE",
#                   "schemeName": "Delaware Division of Corporations"}
# ---------------------------------------------------------------------------
_US_STATE_REGISTRY_NAMES: dict[str, str] = {
    "US-AL": "Alabama Secretary of State",
    "US-AK": "Alaska Division of Corporations, Business & Professional Licensing",
    "US-AZ": "Arizona Corporation Commission",
    "US-AR": "Arkansas Secretary of State",
    "US-CA": "California Secretary of State",
    "US-CO": "Colorado Secretary of State",
    "US-CT": "Connecticut Secretary of State",
    "US-DC": "District of Columbia Department of Licensing and Consumer Protection",
    "US-DE": "Delaware Division of Corporations",
    "US-FL": "Florida Division of Corporations",
    "US-GA": "Georgia Secretary of State",
    "US-HI": "Hawaii Department of Commerce and Consumer Affairs",
    "US-ID": "Idaho Secretary of State",
    "US-IL": "Illinois Secretary of State",
    "US-IN": "Indiana Secretary of State",
    "US-IA": "Iowa Secretary of State",
    "US-KS": "Kansas Secretary of State",
    "US-KY": "Kentucky Secretary of State",
    "US-LA": "Louisiana Secretary of State",
    "US-ME": "Maine Secretary of State",
    "US-MD": "Maryland Department of Assessments and Taxation",
    "US-MA": "Massachusetts Secretary of State",
    "US-MI": "Michigan Department of Licensing and Regulatory Affairs",
    "US-MN": "Minnesota Secretary of State",
    "US-MS": "Mississippi Secretary of State",
    "US-MO": "Missouri Secretary of State",
    "US-MT": "Montana Secretary of State",
    "US-NE": "Nebraska Secretary of State",
    "US-NV": "Nevada Secretary of State",
    "US-NH": "New Hampshire Secretary of State",
    "US-NJ": "New Jersey Division of Revenue and Enterprise Services",
    "US-NM": "New Mexico Secretary of State",
    "US-NY": "New York Department of State",
    "US-NC": "North Carolina Secretary of State",
    "US-ND": "North Dakota Secretary of State",
    "US-OH": "Ohio Secretary of State",
    "US-OK": "Oklahoma Secretary of State",
    "US-OR": "Oregon Secretary of State",
    "US-PA": "Pennsylvania Department of State",
    "US-PR": "Puerto Rico Department of State",
    "US-RI": "Rhode Island Department of State",
    "US-SC": "South Carolina Secretary of State",
    "US-SD": "South Dakota Secretary of State",
    "US-TN": "Tennessee Secretary of State",
    "US-TX": "Texas Secretary of State",
    "US-UT": "Utah Division of Corporations and Commercial Code",
    "US-VT": "Vermont Secretary of State",
    "US-VA": "Virginia State Corporation Commission",
    "US-WA": "Washington Secretary of State",
    "US-WV": "West Virginia Secretary of State",
    "US-WI": "Wisconsin Department of Financial Institutions",
    "US-WY": "Wyoming Secretary of State",
}

# Reporting-exception reasons — GLEIF Level 2 Reporting Exceptions Format 2.1
# (https://www.gleif.org/en/lei-data/access-and-use-lei-data/level-2-data-reporting-exceptions-2-1-format)
# and the GLEIF Reporting Exception Ontology
# (https://www.gleif.org/ontology/v1.0/ReportingException/).
#
# Modelling follows the Open Ownership / Open Data Services GLEIF → BODS
# pipeline (https://github.com/openownership/bods-gleif-pipeline): each
# exception produces a bridging person/entity statement plus a relationship
# whose interest ``details`` carry the exception reason, and every statement
# created from an exception carries a ``commenting`` annotation naming the
# reason — so companies that report "my parent is a natural person" don't
# silently disappear, and consumers can tell an exception bridge from a
# real party.
#
# BODS types are chosen per reason:
# * ``unknownPerson`` — NATURAL_PERSONS / NO_KNOWN_PERSON: any controlling
#   party is a natural person GLEIF does not identify (only entities carry
#   LEIs), or no controlling person is known at all.
# * ``unknownEntity`` — NO_LEI / NON_CONSOLIDATING: a parent entity exists
#   but is not identified in GLEIF (no LEI, or outside the accounting
#   consolidation net). Not *anonymised* — merely not identified here.
# * ``anonymousEntity`` — NON_PUBLIC and its five deprecated variants: a
#   consolidating parent exists and is known but is deliberately withheld
#   from publication. This is the only genuinely opacity-relevant family
#   (see ``risk._opaque_ownership_signals``).
#
# Exception reason → (interested_party_type, person_type or entity_type,
#                     bridge display name, human-readable details).
_GLEIF_EXCEPTION_REASONS = {
    "NATURAL_PERSONS": (
        "person",
        "unknownPerson",
        "Natural person(s) (GLEIF reporting exception)",
        "GLEIF reporting exception NATURAL_PERSONS: the entity is controlled"
        " by natural person(s) without any intermediate legal entity meeting"
        " the definition of accounting consolidating parent",
    ),
    "NO_KNOWN_PERSON": (
        "person",
        "unknownPerson",
        "No known controlling person (GLEIF reporting exception)",
        "GLEIF reporting exception NO_KNOWN_PERSON: there is no known person"
        " controlling the entity (e.g. diversified shareholding)",
    ),
    "NO_LEI": (
        "entity",
        "unknownEntity",
        "Parent without an LEI (GLEIF reporting exception)",
        "GLEIF reporting exception NO_LEI: a parent exists but does not"
        " consent to have an LEI, so it is not identified in GLEIF",
    ),
    "NON_CONSOLIDATING": (
        "entity",
        "unknownEntity",
        "Non-consolidating parent (GLEIF reporting exception)",
        "GLEIF reporting exception NON_CONSOLIDATING: the entity is"
        " controlled by legal entities not subject to preparing consolidated"
        " financial statements",
    ),
    "NON_PUBLIC": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception NON_PUBLIC: a parent exists but the"
        " relationship is non-public and is not disclosed",
    ),
    # The five reasons below were deprecated in Reporting Exceptions Format
    # 2.1 and consolidated under NON_PUBLIC from 2022-03-01; they are kept
    # for records that still carry the old codes.
    "BINDING_LEGAL_COMMITMENTS": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception BINDING_LEGAL_COMMITMENTS (deprecated,"
        " now NON_PUBLIC): binding legal commitments prevent disclosure of"
        " the parent",
    ),
    "LEGAL_OBSTACLES": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception LEGAL_OBSTACLES (deprecated, now"
        " NON_PUBLIC): obstacles in laws or regulations prevent disclosure"
        " of the parent",
    ),
    "DISCLOSURE_DETRIMENTAL": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception DISCLOSURE_DETRIMENTAL (deprecated, now"
        " NON_PUBLIC): disclosure would be detrimental to the entity or its"
        " parent",
    ),
    "DETRIMENT_NOT_EXCLUDED": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception DETRIMENT_NOT_EXCLUDED (deprecated, now"
        " NON_PUBLIC): detriment to the parent from disclosure could not be"
        " excluded",
    ),
    "CONSENT_NOT_OBTAINED": (
        "entity",
        "anonymousEntity",
        "Undisclosed parent (GLEIF reporting exception)",
        "GLEIF reporting exception CONSENT_NOT_OBTAINED (deprecated, now"
        " NON_PUBLIC): the parent's consent to disclose the relationship"
        " was not obtained",
    ),
}

#: Reasons whose bridge party is deliberately withheld (a parent exists,
#: consolidates, and is known — but is not published). Imported by
#: ``risk.py`` to drive the OPAQUE_OWNERSHIP classification.
GLEIF_UNDISCLOSED_REASONS: frozenset[str] = frozenset(
    {
        "NON_PUBLIC",
        "BINDING_LEGAL_COMMITMENTS",
        "LEGAL_OBSTACLES",
        "DISCLOSURE_DETRIMENTAL",
        "DETRIMENT_NOT_EXCLUDED",
        "CONSENT_NOT_OBTAINED",
    }
)

def map_gleif(bundle: dict[str, Any]) -> BODSBundle:
    """Map a GLEIF adapter bundle to BODS v0.4 statements.

    Input shape matches ``GleifAdapter.fetch`` output:

        {
          "lei": ...,
          "record": {...},                            # Level 1 CDF
          "direct_parent": {...} | None,              # Level 2 RR
          "ultimate_parent": {...} | None,            # Level 2 RR
          "direct_parent_exception": {...} | None,    # Reporting exception
          "ultimate_parent_exception": {...} | None,  # Reporting exception
        }
    """
    result = BODSBundle()

    record = bundle.get("record") or {}
    subject_attrs = record.get("attributes") or record
    subject_entity_block = subject_attrs.get("entity") or {}
    lei = (
        bundle.get("lei")
        or subject_attrs.get("lei")
        or record.get("id")
        or ""
    )
    if not lei:
        return result

    subject_url = f"https://www.gleif.org/lei/{lei}"
    # The subject record's own last-update date, reused for the Level 2
    # relationship statements it reports.
    subject_statement_date = _gleif_registration_date(subject_attrs)
    subject_statement = _gleif_entity_statement(
        lei, subject_entity_block, subject_url, attrs=subject_attrs
    )
    result.statements.append(subject_statement)
    subject_sid = subject_statement["statementId"]

    for kind, parent, exception in (
        (
            "direct",
            bundle.get("direct_parent"),
            bundle.get("direct_parent_exception"),
        ),
        (
            "ultimate",
            bundle.get("ultimate_parent"),
            bundle.get("ultimate_parent_exception"),
        ),
    ):
        if parent:
            result.extend(
                _gleif_parent_statements(
                    lei, subject_sid, kind, parent, subject_statement_date
                )
            )
        elif exception:
            result.extend(
                _gleif_exception_statements(
                    lei, subject_sid, kind, exception, subject_statement_date
                )
            )

    for child in bundle.get("direct_children") or []:
        result.extend(
            _gleif_child_statements(lei, subject_sid, child, subject_statement_date)
        )

    # When the direct and the ultimate parent are the same LEI (John Swire &
    # Sons for Swire Pacific, DBS Group Holdings for DBS Bank), both passes
    # emit that parent's statement under the same statementId. The two
    # relationships already point at it; keep one party statement for both
    # (Phase 235). Reporting-exception bridges are keyed by kind and stay two:
    # GLEIF does not say a direct and an ultimate exception are one party.
    result.statements = unique_statements(result.statements)
    return result


def _gleif_parent_statements(
    lei: str,
    subject_sid: str,
    kind: str,
    parent: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit entity + relationship statements for one GLEIF Level 2 parent.

    ``subject_statement_date`` is the subject LEI record's
    ``registration.lastUpdateDate``. GLEIF's parent endpoints return the
    *parent's* Level 1 record, not the relationship (RR) record, so the RR's own
    update date is not available to us; the subject's is the closest thing we
    genuinely hold, since the Level 2 relationship is reported by the subject.
    """
    parent_attrs = parent.get("attributes") or parent
    parent_entity_block = parent_attrs.get("entity") or {}
    parent_lei = parent_attrs.get("lei") or parent.get("id") or ""
    if not parent_lei:
        return []

    parent_url = f"https://www.gleif.org/lei/{parent_lei}"
    parent_statement = _gleif_entity_statement(
        parent_lei, parent_entity_block, parent_url, attrs=parent_attrs
    )
    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent:{parent_lei}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=parent_statement["statementId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": (
                    f"GLEIF Level 2 {kind}-parent (accounting consolidation)"
                ),
            }
        ],
        source_url=parent_url,
        statement_date=subject_statement_date,
    )
    return [parent_statement, rel]


def _gleif_child_statements(
    lei: str,
    subject_sid: str,
    child: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit entity + relationship statements for one GLEIF direct subsidiary.

    Relationship direction (mirrors the parent case but inverted):
    * ``subject``           = child entity  (the one being controlled)
    * ``interestedParty``   = queried entity (the one doing the controlling)

    Only the first page of children is passed in here; the total count is
    surfaced separately via the bundle's ``direct_children_total`` field
    (stored in the GLEIF hit's ``raw`` dict for the frontend to read).
    """
    child_attrs = child.get("attributes") or child
    child_entity_block = child_attrs.get("entity") or {}
    child_lei = child_attrs.get("lei") or child.get("id") or ""
    if not child_lei:
        return []

    child_url = f"https://www.gleif.org/lei/{child_lei}"
    child_statement = _gleif_entity_statement(
        child_lei, child_entity_block, child_url, attrs=child_attrs
    )
    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:direct-child:{child_lei}",
        subject_statement_id=child_statement["statementId"],
        interested_party_statement_id=subject_sid,
        interested_party_type="entity",
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": "GLEIF Level 2 direct-child (accounting consolidation)",
            }
        ],
        source_url=child_url,
        statement_date=subject_statement_date,
    )
    return [child_statement, rel]


def map_gleif_subsidiaries(
    subject_lei: str,
    subject_attrs: dict[str, Any],
    children: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Map a subject entity + its merged direct/ultimate children to BODS.

    Used by the lazy ``/subsidiaries`` reveal. ``children`` is a list of
    ``{"record": <GLEIF L1 data object>, "relations": ["direct"|"ultimate", …]}``.
    A child that is **both** a direct and an ultimate child gets **two**
    relationshipStatements (``directOrIndirect`` ``direct`` and ``indirect``) —
    the graph merges them into one annotated edge, but the statements stay
    distinct in the data and the export.
    """
    if not subject_lei:
        return []
    subj_url = f"https://www.gleif.org/lei/{subject_lei}"
    subj = _gleif_entity_statement(
        subject_lei, (subject_attrs or {}).get("entity") or {}, subj_url,
        attrs=subject_attrs,
    )
    out: list[dict[str, Any]] = [subj]
    subj_sid = subj["statementId"]
    seen: set[str] = set()
    for c in children:
        rec = c.get("record") or {}
        attrs = rec.get("attributes") or rec
        child_lei = attrs.get("lei") or rec.get("id") or ""
        if not child_lei or child_lei in seen:
            continue
        seen.add(child_lei)
        child_url = f"https://www.gleif.org/lei/{child_lei}"
        child_stmt = _gleif_entity_statement(
            child_lei, attrs.get("entity") or {}, child_url, attrs=attrs
        )
        out.append(child_stmt)
        for kind in sorted(set(c.get("relations") or [])):
            out.append(make_relationship_statement(
                source_id="gleif",
                local_id=f"{subject_lei}:{kind}-child:{child_lei}",
                subject_statement_id=child_stmt["statementId"],
                interested_party_statement_id=subj_sid,
                interested_party_type="entity",
                interests=[{
                    "type": "otherInfluenceOrControl",
                    "directOrIndirect": "direct" if kind == "direct" else "indirect",
                    "beneficialOwnershipOrControl": False,
                    "details": f"GLEIF Level 2 {kind}-child (accounting consolidation)",
                }],
                source_url=child_url,
            ))
    return out


def _gleif_exception_statements(
    lei: str,
    subject_sid: str,
    kind: str,
    exception: dict[str, Any],
    subject_statement_date: str | None = None,
) -> list[dict[str, Any]]:
    """Emit a bridging person/entity statement + relationship for an exception.

    Mirrors the Open Ownership GLEIF pipeline's reporting-exception handling:
    the bridge and relationship each carry a ``commenting`` annotation naming
    the exception reason, and the relationship interest ``details`` carry the
    reason's meaning plus the exception category (and the legal
    ``ExceptionReference`` when the entity supplied one).
    """
    attrs = exception.get("attributes") or exception
    # Live GLEIF API uses "reason"; OO SQLite dump uses "exceptionReason".
    reason = (attrs.get("reason") or attrs.get("exceptionReason") or "").upper()
    category = (attrs.get("category") or attrs.get("exceptionCategory") or "").upper()
    reference = attrs.get("reference") or attrs.get("exceptionReference") or ""
    ip_type, ip_subtype, bridge_name, details = _GLEIF_EXCEPTION_REASONS.get(
        reason,
        (
            "entity",
            "unknownEntity",
            "Unknown parent (GLEIF reporting exception)",
            f"GLEIF reporting exception: {reason or 'unspecified reason'}",
        ),
    )
    if category:
        details += f" (ExceptionCategory: {category})"
    if reference:
        details += f"; legal reference: {reference}"

    exception_note = commenting(
        "/",
        (
            f"This statement was created due to a {reason or 'GLEIF'}"
            f" GLEIF Reporting Exception for {lei}. Reporting exceptions are"
            " permitted reasons, defined by the LEI ROC policy, for an entity"
            " not to report an accounting consolidation parent."
        ),
        creation_date=_today(),
    )

    bridge_local_id = f"{lei}:{kind}-parent-exception:{reason or 'unspecified'}"
    if ip_type == "person":
        bridge = make_person_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            full_name=bridge_name,
            person_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )
    else:
        bridge = make_entity_statement(
            source_id="gleif",
            local_id=bridge_local_id,
            name=bridge_name,
            entity_type=ip_subtype,
            source_url=f"https://www.gleif.org/lei/{lei}",
        )
    annotate(bridge, dict(exception_note))

    rel = make_relationship_statement(
        source_id="gleif",
        local_id=f"{lei}:{kind}-parent-exception-rel:{reason or 'unspecified'}",
        subject_statement_id=subject_sid,
        interested_party_statement_id=bridge["statementId"],
        interested_party_type=ip_type,
        interests=[
            {
                "type": "otherInfluenceOrControl",
                "directOrIndirect": "direct" if kind == "direct" else "indirect",
                "beneficialOwnershipOrControl": False,
                "details": details,
            }
        ],
        source_url=f"https://www.gleif.org/lei/{lei}",
        statement_date=subject_statement_date,
    )
    annotate(rel, dict(exception_note))
    return [bridge, rel]


def _gleif_scalar(value: Any) -> str:
    """Coerce a GLEIF attribute expected to be a single string.

    GLEIF returns ``ocid`` / ``qcc`` as scalar strings. Guard against a list
    (take the first non-empty element) or ``None`` so a schema quirk can never
    put a Python list into a BODS identifier ``id``. Returns ``""`` when empty.
    """
    if isinstance(value, list):
        value = next((v for v in value if v), None)
    if value is None:
        return ""
    return str(value).strip()


def _gleif_id_values(value: Any) -> list[str]:
    """Normalise a GLEIF multi-valued identifier field to a list of strings.

    ``bic`` / ``mic`` / ``spglobal`` are arrays in the live GLEIF API — an
    entity can hold many BICs (Deutsche Bank carries 70+) and an exchange
    operator several MICs (London Stock Exchange has ``["ECHO", "XLON"]``) —
    but GLEIF has historically returned a bare string for single-valued cases.
    Accept ``str``, ``list`` or ``None``; return de-duplicated,
    order-preserving, non-empty trimmed strings so *every* available
    identifier is linked to the LEI, not just the first.
    """
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item is None:
            continue
        s = str(item).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _gleif_registration_date(attrs: dict[str, Any] | None) -> str | None:
    """GLEIF ``registration.lastUpdateDate`` as a plain ISO date.

    GLEIF publishes it with a time component (e.g. "2023-03-31T07:01:00Z");
    BODS date fields want ``YYYY-MM-DD``, so take the date portion.
    """
    registration = (attrs or {}).get("registration") or {}
    last_update = registration.get("lastUpdateDate") or ""
    return last_update[:10] or None


def _gleif_entity_statement(
    lei: str,
    entity_block: dict[str, Any],
    source_url: str,
    *,
    attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a BODS entity statement from a GLEIF Level 1 entity block.

    ``attrs`` is the full ``record.attributes`` dict (one level above
    ``entity``). It carries the cross-reference identifiers that GLEIF
    publishes via its LEI Mapping programme:

    * ``ocid``     — OpenCorporates identifier (e.g. ``"gb/00102498"``)
    * ``qcc``      — QCC Global Enterprise Identifier / QCC Code (e.g. ``"QGBVC89DTN"``)
    * ``mic``      — Market Identifier Code ISO 10383 (e.g. ``"XLON"``)
    * ``bic``      — Bank Identifier Code ISO 9362 (e.g. ``"BARCGB22"``)
    * ``spglobal`` — S&P CIQ Company ID (e.g. ``"32307"`` for NVIDIA). Published
                     via GLEIF's LEI Mapping programme; S&P Global is not currently
                     listed on org-id.guide so the scheme is recorded as
                     ``"S&P CIQ Company ID"``.

    These are mapped to BODS identifiers when non-null, enabling
    downstream adapters to use them for additional cross-source queries.
    """
    legal_name = (entity_block.get("legalName") or {}).get("name") or f"LEI {lei}"
    jurisdiction_code = entity_block.get("jurisdiction")
    jurisdiction: tuple[str, str] | None = None
    if jurisdiction_code:
        jurisdiction = _gleif_jurisdiction(jurisdiction_code)

    identifiers: list[dict[str, str]] = [
        {
            "id": lei,
            "scheme": "XI-LEI",
            "schemeName": "Global Legal Entity Identifier Index",
        }
    ]

    # GLEIF records the registration authority in ``entity.registeredAt``:
    #   {"id": "RA000585", "other": null}   # standard RA code
    #   {"id": "RA999999", "other": "My Authority"}   # free-text authority
    #
    # Resolution priority:
    #  1. Known RA code in _GLEIF_RA_TO_ORG_ID → use the mapped org-id scheme.
    #  2. Unknown RA code + US-* jurisdiction → use the ISO 3166-2 subdivision
    #     code as scheme (e.g. "US-DE") and look up the registry name in
    #     _US_STATE_REGISTRY_NAMES.  org-id.guide has no per-state US entries
    #     but ISO 3166-2 codes are unambiguous and machine-readable.
    #  3. Anything else → the RA code itself (Phase 239; was a blank scheme).
    #     See ``gleif_registration_scheme``; the number is written the way the
    #     register writes it — ``normalise_registered_as``.
    registered_as = normalise_registered_as(entity_block.get("registeredAs"))
    registered_at = entity_block.get("registeredAt") or {}
    ra_id = registered_at.get("id")
    if registered_as and ra_id:
        org_id_scheme, org_id_name = gleif_registration_scheme(
            ra_id, jurisdiction_code, registered_at.get("other"), registered_as
        )
        identifiers.append(
            {
                "id": registered_as,
                "scheme": org_id_scheme,
                "schemeName": org_id_name,
            }
        )

    # GLEIF LEI Mapping cross-reference identifiers (from ``record.attributes``).
    # GLEIF surfaces its BIC-to-LEI, MIC-to-LEI and OpenCorporates / S&P Global /
    # QCC mapping programmes inline on every LEI record. ``ocid`` and ``qcc`` are
    # single strings; ``bic``, ``mic`` and ``spglobal`` are arrays (an entity can
    # hold dozens of BICs and an exchange operator several MICs). We emit one
    # BODS identifier per value so *all* available identifiers are linked to the
    # LEI — the richer the identifier graph, the more datasets the LEI connects.
    # Each is only included when the GLEIF API returns a non-null value.
    if attrs:
        ocid = _gleif_scalar(attrs.get("ocid"))
        if ocid:
            identifiers.append(
                {
                    "id": ocid,
                    "scheme": "OpenCorporates",
                    "schemeName": "OpenCorporates company ID",
                    "uri": f"https://opencorporates.com/companies/{ocid}",
                }
            )

        qcc = _gleif_scalar(attrs.get("qcc"))
        if qcc:
            identifiers.append(
                {
                    "id": qcc,
                    "scheme": "QCC Code",
                    "schemeName": "QCC Global Enterprise Identifier (QCC Code)",
                }
            )

        # MIC — one identifier per Market Identifier Code (ISO 10383).
        for mic_val in _gleif_id_values(attrs.get("mic")):
            identifiers.append(
                {
                    "id": mic_val,
                    "scheme": "ISO-10383",
                    "schemeName": "Market Identifier Code (ISO 10383)",
                }
            )

        # BIC — one identifier per Bank Identifier Code (ISO 9362).
        for bic_val in _gleif_id_values(attrs.get("bic")):
            identifiers.append(
                {
                    "id": bic_val,
                    "scheme": "ISO-9362",
                    "schemeName": "Bank Identifier Code (ISO 9362)",
                }
            )

        # S&P CIQ Company ID — one identifier per value. S&P Global is not
        # currently listed on org-id.guide so the scheme is recorded as a
        # descriptive string per BODS v0.4 guidance.
        for spglobal_val in _gleif_id_values(attrs.get("spglobal")):
            identifiers.append(
                {
                    "id": spglobal_val,
                    "scheme": "S&P CIQ Company ID",
                    "schemeName": "S&P CIQ Company ID",
                }
            )

    addresses = _gleif_addresses(entity_block)

    # Collect alternate names from otherNames and transliteratedOtherNames,
    # deduplicating and excluding the primary legal name.
    seen_names: set[str] = {legal_name}
    alternate_names: list[str] = []
    for name_block in (
        *(entity_block.get("otherNames") or []),
        *(entity_block.get("transliteratedOtherNames") or []),
    ):
        n = (name_block.get("name") or "").strip()
        if n and n not in seen_names:
            seen_names.add(n)
            alternate_names.append(n)

    # GLEIF's registration.lastUpdateDate is when GLEIF last asserted this
    # record's contents — the source's own declaration date, so it is a
    # statementDate. It is NOT a publicationDate: publicationDetails describes
    # OpenCheck's publication of this statement, and OpenCheck published it now.
    gleif_statement_date = _gleif_registration_date(attrs)

    # entity.creationDate → foundingDate (ISO 8601 date or datetime; take date part).
    creation_date_raw = entity_block.get("creationDate") or ""
    founding_date = creation_date_raw[:10] if creation_date_raw else None

    # entity.status (ACTIVE / INACTIVE) is the LEI system's view of whether the
    # legal entity still exists; entity.expiration.date/.reason say when and
    # why it stopped (dissolved, merged, ...). Both go through the shared
    # liveness path (Phase 151): INACTIVE without an expiration date is now
    # visible as a status annotation rather than lost. ``registration.status``
    # (ISSUED / LAPSED / RETIRED / ...) is a different question — whether the
    # LEI *record* is maintained — and is deliberately not read as liveness.
    expiration = entity_block.get("expiration") or {}
    expiration_date_raw = expiration.get("date") or ""
    expiration_date = expiration_date_raw[:10] if expiration_date_raw else None
    entity_status = str(entity_block.get("status") or "")

    stmt = make_entity_statement(
        source_id="gleif",
        local_id=lei,
        name=legal_name,
        jurisdiction=jurisdiction,
        identifiers=identifiers,
        addresses=addresses,
        alternate_names=alternate_names,
        founding_date=founding_date,
        source_url=source_url,
        statement_date=gleif_statement_date,
    )
    gleif_liveness = _liveness.classify(
        entity_status, live=("ACTIVE",), terminal=("INACTIVE",)
    )
    if gleif_liveness == _liveness.UNKNOWN and expiration_date:
        # An expiration date with no status field (older cached records)
        # still means the entity ended.
        gleif_liveness = _liveness.TERMINAL
    raw_status = entity_status
    if expiration.get("reason"):
        raw_status = f"{entity_status} ({expiration['reason']})" if entity_status else str(expiration["reason"])
    _liveness.apply_register_status(
        stmt,
        source_label=SOURCE_NAMES["gleif"],
        liveness=gleif_liveness,
        raw=raw_status or None,
        since=expiration_date,
    )

    # Resolve GLEIF's ISO 20275 legal-form code (entity.legalForm.id, e.g.
    # "2JZ4" = "Foundation") to a human label carried as the non-schema
    # `legalFormLabel` annotation. This is what the AMLA trust/arrangement risk
    # signal keys off, so a GLEIF-only foundation/trust (no national-register
    # hit) is still caught — matching the legal form, never the entity name.
    legal_form_label = resolve_elf((entity_block.get("legalForm") or {}).get("id"))
    if legal_form_label:
        stmt["recordDetails"]["legalFormLabel"] = legal_form_label

    return stmt


def _gleif_jurisdiction(code: str) -> tuple[str, str]:
    """Resolve a GLEIF jurisdiction code to ``(name, code)``.

    GLEIF uses ISO 3166-1 alpha-2 codes at the country level and
    ISO 3166-2 codes (e.g. ``GB-ENG``) at the subdivision level.
    """
    upper = code.upper()
    alpha_2 = upper.split("-")[0]
    country = pycountry.countries.get(alpha_2=alpha_2)
    if not country:
        return (code, code)
    if "-" in upper:
        subdivision = pycountry.subdivisions.get(code=upper)
        if subdivision:
            return (f"{subdivision.name}, {country.name}", upper)
    return (country.name, alpha_2)


def _gleif_addresses(entity_block: dict[str, Any]) -> list[dict[str, str]]:
    addresses: list[dict[str, str]] = []
    legal_address = entity_block.get("legalAddress")
    if legal_address:
        addresses.append(_gleif_address(legal_address, address_type="registered"))
    hq_address = entity_block.get("headquartersAddress")
    if hq_address:
        addresses.append(_gleif_address(hq_address, address_type="business"))
    return addresses


def _gleif_address(block: dict[str, Any], *, address_type: str) -> dict[str, Any]:
    parts = [
        *(block.get("addressLines") or []),
        block.get("city"),
        block.get("region"),
        block.get("postalCode"),
        block.get("country"),
    ]
    joined = ", ".join([p for p in parts if p])
    return _addr(address_type, joined, block.get("country", ""))


# ----------------------------------------------------------------------
# Open Ownership BODS bulk data — passthrough mappers
# ----------------------------------------------------------------------
# The bods_gleif and bods_uk_psc adapters already reconstruct full BODS
# v0.4 statements inside their fetch() method and return them under the
# ``bods_statements`` key. These mapper functions are simple passthroughs
# so the _MAPPERS dispatch in app.py can route to them uniformly.
# ----------------------------------------------------------------------


def map_bods_gleif(bundle: dict[str, Any]) -> BODSBundle:
    """Passthrough mapper for the Open Ownership GLEIF bulk data adapter.

    The adapter returns ``{"bods_statements": [...], ...}`` directly from
    its Parquet reconstruction step; we just yield those statements.
    """
    return iter(bundle.get("bods_statements", []))


def map_bods_uk_psc(bundle: dict[str, Any]) -> BODSBundle:
    """Passthrough mapper for the Open Ownership UK PSC bulk data adapter.

    Same pattern as map_bods_gleif — statements are pre-built by the
    adapter; this function makes them visible to the _MAPPERS dispatch.
    """
    return iter(bundle.get("bods_statements", []))


def map_meip(bundle: dict[str, Any]) -> BODSBundle:
    """Passthrough mapper for the OECD-UNSD MEIP register (Phase 208).

    The OECD publishes the register in BODS v0.4 itself, so the adapter
    returns its statements verbatim under ``bods_statements`` — the OECD's
    ``statementId`` / ``recordId`` / ``source`` / ``publicationDetails`` and
    the one annotation the file carries. Nothing is re-mapped: the first
    source whose statements reach the graph as the publisher wrote them.
    """
    return iter(bundle.get("bods_statements", []))
_DC_COUNTRY = {"name": "United States", "code": "US"}
