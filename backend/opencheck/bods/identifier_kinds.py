"""What kind of identifier a BODS ``scheme`` names, and which country issued it.

Read by the Senzing exporter (``bods/senzing.py``) to choose the Senzing
feature for each identifier, and by the test suite's mapper guard, which fails
a mapper that writes a scheme this module has not been told about.

Why it matters (OpenSanctions changelog #54, 13 Nov 2026): Senzing keeps two
records apart when they carry the same identifier type and country with
different numbers, and treats a number with a different or missing type or
country as supporting evidence only. So a wrong type or country can stop a
true merge or make a false one, while a missing one only weakens the evidence.
Until this module every non-LEI identifier went out as ``NATIONAL_ID`` — a VAT
number, an industry code, a Wikidata id — with a country read from any
two-letter prefix (``XI-VAT`` gave ``XI``, which is no country).

The reading is a fixed table, never a guess from ``schemeName`` (free text
that varies by source). In order:

1. ``LEI`` — the LEI schemes, or a value that passes the LEI check digits.
2. :data:`SCHEME_KINDS` — every scheme that is *not* a register number, by
   name: tax and VAT numbers (``tax``), other databases' and registers' own
   keys (``other``), passports (``passport``), and codes that say nothing
   about who a party is — industry, securities, market codes (``drop``).
3. A person's document scheme, ``<ISO3>-PASSPORT|TAXID|IDCARD`` (the BODS
   person identifier pattern).
4. A register number (``register``): a scheme the GLEIF mapper writes for a
   registration authority, a bare GLEIF RA code, ``REG`` / ``REG-<country>``,
   a US state subdivision (``US-DE``), or one of :data:`REGISTER_SCHEMES`.
5. Anything else is ``unclassified`` and exported as ``OTHER_ID`` — supporting
   evidence, never a reason to keep two records apart. The mapper guard fails
   a test that produces one, so a new scheme gets a decision here.
"""

from __future__ import annotations

import functools
import re

from ..identifiers import classify_lei
from ..ra_codes import is_ra_scheme
from .countries import country_code

LEI = "lei"
REGISTER = "register"
TAX = "tax"
PASSPORT = "passport"
OTHER = "other"
DROP = "drop"
UNCLASSIFIED = "unclassified"

KINDS: tuple[str, ...] = (LEI, REGISTER, TAX, PASSPORT, OTHER, DROP, UNCLASSIFIED)

#: Schemes that name an LEI.
LEI_SCHEMES: frozenset[str] = frozenset({"XI-LEI", "LEI"})

#: Every scheme OpenCheck writes that is not a register number, upper-cased.
SCHEME_KINDS: dict[str, str] = {
    # Tax and VAT numbers — Senzing's TAX_ID.
    "AM-TIN": TAX,       # Armenian taxpayer number (EITI BO)
    "AR-CUIT": TAX,      # Clave Única de Identificación Tributaria (AFIP / ARCA)
    "AT-UID": TAX,       # Umsatzsteuer-Identifikationsnummer
    "CA-BN": TAX,        # Canada Revenue Agency Business Number
    "CD-NIF": TAX,       # Numéro d'identification fiscale (DRC, EITI BO)
    "CL-RUT": TAX,       # Rol Único Tributario
    "CZ-DIC": TAX,       # Daňové identifikační číslo
    "EE-KMKR": TAX,      # Estonian VAT number
    "GR-AFM": TAX,       # ΑΦΜ, Greek tax registry number
    "HR-OIB": TAX,       # Osobni identifikacijski broj (tax number)
    "KR-BRN": TAX,       # 사업자등록번호, issued by the National Tax Service
    "MX-RFC": TAX,       # Registro Federal de Contribuyentes (SAT)
    "PE-RUC": TAX,       # Registro Único de Contribuyentes (SUNAT)
    "PL-NIP": TAX,       # Numer Identyfikacji Podatkowej
    "RO-CUI": TAX,       # Cod Unic de Înregistrare, the fiscal code (ANAF)
    "RU-INN": TAX,       # a company's INN (a person's is rewritten to RUS-TAXID)
    "US-EIN": TAX,       # Employer Identification Number
    "XI-VAT": TAX,       # a VAT number; the country is in the number, not the scheme
    "ZM-TPIN": TAX,      # Zambia Revenue Authority taxpayer number
    # Other databases' and registers' own keys — Senzing's OTHER_ID.
    "BRIGHTQUERY": OTHER,
    "CAPIQ": OTHER,
    "CA-ESTMA": OTHER,             # an ESTMA reporting-entity id
    "CH-COA": OTHER,               # Zefix's internal id, not the UID
    "DK-CVR-ENHEDSNUMMER": OTHER,  # CVR's participant number (a person key)
    "EE-ARIREGISTER-HASH": OTHER,  # the register's person key
    "EFORMS-BT-501": OTHER,        # an eForms field, which names no register
    "EITI-IDENTIFICATION": OTHER,  # whatever number EITI filed; it states no register
    "GB-COH-OFFICER": OTHER,       # a Companies House officer id
    "GB-COH-PSC": OTHER,           # a Companies House PSC item id
    "GEM-ENTITY": OTHER,
    "ISO-9362": OTHER,             # BIC
    "LV-SEPA": OTHER,              # SEPA creditor identifier
    "OPENALEPH": OTHER,
    "OPENCORPORATES": OTHER,
    "OPENCORPORATES-COMPANY-NUMBER": OTHER,  # a number with no register named
    "OPENSANCTIONS": OTHER,
    "PERMID": OTHER,
    "PITCHBOOK": OTHER,
    "QCC CODE": OTHER,
    "S&P CIQ COMPANY ID": OTHER,
    "SK-RPVS": OTHER,              # the public-sector partners register's own number
    "US-NPI": OTHER,
    "US-SAM-CAGE": OTHER,
    "US-SAM-UEI": OTHER,
    "US-SEC": OTHER,               # SEC Central Index Key
    "US-SEC-CIK": OTHER,
    "WIKIDATA": OTHER,
    # Codes that say nothing about who a party is.
    "CUSIP": DROP,
    "FI-TOL": DROP,      # Finnish industry classification
    "ISIN": DROP,
    "ISO-10383": DROP,   # MIC
    "NACE2": DROP,
    "OPENFIGI": DROP,
    "TICKER": DROP,
}

#: Register schemes an adapter writes that the GLEIF RA table does not name.
REGISTER_SCHEMES: frozenset[str] = frozenset(
    {
        "AM-SRALE",   # Armenian State Register of Legal Entities
        "FR-RNA",     # Répertoire national des associations
        "NZ-NZBN",
        "PL-REGON",   # the statistical register's number
        "SK-OR",
    }
)

_PERSON_DOCUMENT = re.compile(r"^([A-Z]{3})-(PASSPORT|TAXID|IDCARD)$")
_PERSON_DOCUMENT_KIND = {"PASSPORT": PASSPORT, "TAXID": TAX, "IDCARD": REGISTER}
_REG_COUNTRY = re.compile(r"^REG(?:-([A-Z]{2}))?$")
_US_STATE = re.compile(r"^US-[A-Z]{2}$")


@functools.lru_cache(maxsize=1)
def _gleif_register_schemes() -> frozenset[str]:
    # Imported here: bods/mapper.py imports most of bods/, and this module is
    # imported by senzing.py, which bods/__init__ loads alongside it.
    from . import mapper

    schemes = {scheme.upper() for scheme, _ in mapper._GLEIF_RA_TO_ORG_ID.values()}
    schemes |= {
        mapper.CL_RUT_SCHEME.upper(),
        mapper.CO_RUE_SCHEME.upper(),
        mapper.AR_CUIT_SCHEME.upper(),
        mapper.PE_RUC_SCHEME.upper(),
    }
    return frozenset(schemes)


def _norm(scheme: str | None) -> str:
    return re.sub(r"\s+", " ", (scheme or "").strip()).upper()


def scheme_kind(scheme: str | None) -> str:
    """The kind of identifier *scheme* names (one of :data:`KINDS`), from the
    scheme alone — the value is not read."""
    key = _norm(scheme)
    if not key:
        return UNCLASSIFIED
    if key in LEI_SCHEMES:
        return LEI
    if key in SCHEME_KINDS:
        return SCHEME_KINDS[key]
    doc = _PERSON_DOCUMENT.match(key)
    if doc:
        return _PERSON_DOCUMENT_KIND[doc.group(2)]
    if (
        key in REGISTER_SCHEMES
        or is_ra_scheme(key)
        or _REG_COUNTRY.match(key)
        or _US_STATE.match(key)
        or key in _gleif_register_schemes()
    ):
        return REGISTER
    return UNCLASSIFIED


def identifier_kind(identifier: dict) -> str:
    """The kind of a BODS identifier object: its scheme's kind, except that a
    value passing the LEI check digits is an LEI whatever the scheme says (an
    LEI filed under a register's scheme is still an LEI)."""
    value = str(identifier.get("id") or "").strip()
    if value and classify_lei(value):
        return LEI
    return scheme_kind(identifier.get("scheme"))


def scheme_country(scheme: str | None) -> str:
    """The ISO 3166-1 alpha-2 country that issued numbers under *scheme*, or
    ``""`` when the scheme does not say.

    ``REG-<country>`` names its country last; a person's document scheme
    starts with an alpha-3 code; any other scheme names a country only by a
    two-letter ISO prefix (``GB-COH``, ``US-DE``). ``XI-`` (org-id's
    "international") and every unprefixed scheme — a GLEIF RA code, a
    database's own key — have none. Never read from ``schemeName``.
    """
    key = _norm(scheme)
    reg = _REG_COUNTRY.match(key)
    if reg:
        return country_code(reg.group(1)) if reg.group(1) else ""
    doc = _PERSON_DOCUMENT.match(key)
    if doc:
        return country_code(doc.group(1))
    head, sep, _ = key.partition("-")
    if sep and len(head) == 2 and head.isalpha():
        return country_code(head)
    return ""
