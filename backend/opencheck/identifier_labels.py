"""Readable names for the lookup's derived identifier keys.

``derived_identifiers`` is keyed on the pipeline's own dispatch keys
(``no_orgnr``, ``ocid``, ``us_ny_dos_id`` …). Those are names for code, not for
a reader: the Markdown and PDF reports printed them title-cased ("No orgnr",
"Ocid") until the 23 Nov 2026 production check. Every key an adapter declares
(a ``LookupDeriver.derived_key`` or a ``lookup_dispatch_keys`` entry) and every
key the pipeline sets itself needs a row here;
``tests/test_identifier_labels.py`` fails on a key with none.
"""

from __future__ import annotations

IDENTIFIER_LABELS: dict[str, str] = {
    # Set by the pipeline itself.
    "lei": "Legal Entity Identifier (LEI)",
    "ocid": "OpenCorporates ID",
    "wikidata_qid": "Wikidata ID",
    "edgar_cik": "SEC EDGAR CIK",
    "us_ein": "US Employer Identification Number (EIN)",
    "zm_tpin": "Zambian taxpayer identification number (TPIN)",
    "gb_coh": "Companies House number",
    # Older payloads.
    "company_number": "Company registration number",
    "vat": "VAT number",
    # Declared by register adapters.
    "ar_cuit": "Argentine CUIT",
    "at_fn": "Austrian company register number (Firmenbuchnummer)",
    "au_abn": "Australian Business Number (ABN)",
    "au_acn": "Australian Company Number (ACN)",
    "be_enterprise_number": "Belgian enterprise number",
    "bg_eik": "Bulgarian unified identification code (EIK)",
    "br_cnpj": "Brazilian CNPJ",
    "ca_corp_id": "Canadian federal corporation number",
    "che_uid": "Swiss enterprise identification number (UID)",
    "co_rues": "Colombian NIT or registration number",
    "cy_he": "Cypriot registration number",
    "cz_ico": "Czech identification number (IČO)",
    "dk_cvr": "Danish CVR number",
    "ee_registry_code": "Estonian registry code",
    "fi_ytunnus": "Finnish Business ID (Y-tunnus)",
    "gr_argemi": "Greek G.E.MI. number",
    "hk_brn": "Hong Kong business registration number",
    "hr_mbs": "Croatian registration number (MBS)",
    "ie_crn": "Irish company number",
    "il_roc": "Israeli company number",
    "in_cin": "Indian corporate identification number (CIN)",
    "jp_jcn": "Japanese corporate number",
    "kr_bizr_no": "Korean business registration number",
    "kr_jurir_no": "Korean corporate registration number",
    "kvk_number": "Dutch KvK number",
    "lt_code": "Lithuanian company code",
    "lv_regcode": "Latvian registration number",
    "md_idno": "Moldovan IDNO",
    "mt_crn": "Maltese company number",
    "no_orgnr": "Norwegian organisation number",
    "nz_company_number": "New Zealand company number",
    "pe_ruc": "Peruvian RUC",
    "pl_krs": "Polish KRS number",
    "ro_fiscal_or_reg_id": "Romanian fiscal code or registration number",
    "rs_mb": "Serbian registration number (matični broj)",
    "se_org_number": "Swedish organisation number",
    "sg_uen": "Singapore UEN",
    "si_maticna": "Slovenian registration number (matična številka)",
    "siren": "French SIREN",
    "sk_ico": "Slovak identification number (IČO)",
    "tw_ubn": "Taiwanese Unified Business Number",
    "ua_edrpou": "Ukrainian EDRPOU code",
    "us_ct_business_id": "Connecticut business ID",
    "us_dc_file_number": "District of Columbia file number",
    "us_fl_doc": "Florida document number",
    "us_ny_dos_id": "New York DOS ID",
    "us_pa_filing_number": "Pennsylvania filing number",
}


def identifier_label(key: str) -> str:
    """The reader's name for a derived identifier key; a key with no row falls
    back to its words, never to an empty cell."""
    return IDENTIFIER_LABELS.get(key) or key.replace("_", " ").capitalize()
