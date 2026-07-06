"""Tests for source API schema validation (Pydantic models + validate_raw).

Each source has three cases:
  1. valid_bundle  — all required fields present → no error raised
  2. missing_required — a required field is absent → SourceSchemaError raised
  3. extra_field_ignored — an unknown field is present → no error (extra="allow")
"""

from __future__ import annotations

import pytest

from opencheck.sources.schemas import SourceSchemaError, validate_raw

# ---------------------------------------------------------------------------
# Companies House
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.companies_house import CHBundle, CHOfficerBundle


_CH_BUNDLE_VALID = {
    "source_id": "companies_house",
    "company_number": "00102498",
    "profile": {"company_number": "00102498", "company_name": "BP P.L.C."},
    "officers": {},
    "pscs": {"items": []},
    "related_companies": {},
}

_CH_OFFICER_BUNDLE_VALID = {
    "source_id": "companies_house",
    "officer_id": "zS_RY9pRYlJ9XwGJEOFtkJgrf8s",
    "appointments": {"items": []},
}


def test_ch_bundle_valid():
    validate_raw("companies_house", CHBundle, _CH_BUNDLE_VALID)


def test_ch_bundle_missing_company_number():
    bad = {k: v for k, v in _CH_BUNDLE_VALID.items() if k != "company_number"}
    with pytest.raises(SourceSchemaError, match="companies_house"):
        validate_raw("companies_house", CHBundle, bad)


def test_ch_bundle_missing_profile_company_number():
    bad = {**_CH_BUNDLE_VALID, "profile": {"company_name": "BP P.L.C."}}
    with pytest.raises(SourceSchemaError, match="companies_house"):
        validate_raw("companies_house", CHBundle, bad)


def test_ch_bundle_extra_field_ignored():
    bundle = {**_CH_BUNDLE_VALID, "new_registry_field": "some_value"}
    validate_raw("companies_house", CHBundle, bundle)


def test_ch_officer_bundle_valid():
    validate_raw("companies_house", CHOfficerBundle, _CH_OFFICER_BUNDLE_VALID)


def test_ch_officer_bundle_missing_officer_id():
    bad = {k: v for k, v in _CH_OFFICER_BUNDLE_VALID.items() if k != "officer_id"}
    with pytest.raises(SourceSchemaError, match="companies_house"):
        validate_raw("companies_house", CHOfficerBundle, bad)


def test_ch_bundle_psc_kind_required():
    """CHPsc.kind is required — a PSC without it should fail validation."""
    bad = {
        **_CH_BUNDLE_VALID,
        "pscs": {"items": [{"name": "Acme Holdings Ltd"}]},  # missing 'kind'
    }
    with pytest.raises(SourceSchemaError, match="companies_house"):
        validate_raw("companies_house", CHBundle, bad)


# ---------------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.gleif import GLEIFBundle


_GLEIF_BUNDLE_VALID = {
    "source_id": "gleif",
    "lei": "7ZW8QJWVPR4P1J1KQY45",
    "record": {
        "id": "7ZW8QJWVPR4P1J1KQY45",
        "attributes": {
            "lei": "7ZW8QJWVPR4P1J1KQY45",
            "entity": {"legalName": {"name": "BP P.L.C."}, "status": "ACTIVE"},
        },
    },
    "direct_parent": None,
    "ultimate_parent": None,
    "direct_parent_exception": None,
    "ultimate_parent_exception": None,
}


def test_gleif_bundle_valid():
    validate_raw("gleif", GLEIFBundle, _GLEIF_BUNDLE_VALID)


def test_gleif_bundle_missing_lei():
    bad = {k: v for k, v in _GLEIF_BUNDLE_VALID.items() if k != "lei"}
    with pytest.raises(SourceSchemaError, match="gleif"):
        validate_raw("gleif", GLEIFBundle, bad)


def test_gleif_bundle_extra_field_ignored():
    bundle = {**_GLEIF_BUNDLE_VALID, "experimental_field": "beta"}
    validate_raw("gleif", GLEIFBundle, bundle)


def test_gleif_bundle_optional_parents_absent():
    """Omitting parent/exception keys should still pass (all optional)."""
    minimal = {"source_id": "gleif", "lei": "7ZW8QJWVPR4P1J1KQY45"}
    validate_raw("gleif", GLEIFBundle, minimal)


# ---------------------------------------------------------------------------
# OpenCorporates
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.opencorporates import OCBundle


_OC_BUNDLE_VALID = {
    "source_id": "opencorporates",
    "hit_id": "gb/00102498",
    "ocid": "gb/00102498",
    "company": {
        "name": "BP P.L.C.",
        "jurisdiction_code": "gb",
        "company_number": "00102498",
    },
    "officers": [],
    "network": None,
}


def test_oc_bundle_valid():
    validate_raw("opencorporates", OCBundle, _OC_BUNDLE_VALID)


def test_oc_bundle_missing_ocid():
    bad = {k: v for k, v in _OC_BUNDLE_VALID.items() if k != "ocid"}
    with pytest.raises(SourceSchemaError, match="opencorporates"):
        validate_raw("opencorporates", OCBundle, bad)


def test_oc_bundle_extra_field_ignored():
    bundle = {**_OC_BUNDLE_VALID, "raw_company": {"results": {}}}
    validate_raw("opencorporates", OCBundle, bundle)


# ---------------------------------------------------------------------------
# Bolagsverket
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.bolagsverket import BVBundle


# organisationsnamn / organisationsdatum are OBJECTS in the real v4.x API — the
# mapper reads organisationsnamn.organisationsnamnLista[].namn and
# organisationsdatum.registreringsdatum. (Regression: they were modelled as
# strings, so the real object shape failed validation once populated.)
_BV_BUNDLE_VALID = {
    "source_id": "bolagsverket",
    "org_number": "5560000106",
    "company": {
        "organisationsnamn": {
            "organisationsnamnLista": [{"namn": "AB Volvo"}]
        },
        "organisationsdatum": {"registreringsdatum": "1915-08-07"},
        "organisationsform": {"kod": "AB", "klartext": "Aktiebolag"},
    },
    "legal_name": "AB Volvo",
    "is_stub": False,
}


def test_bv_bundle_valid():
    validate_raw("bolagsverket", BVBundle, _BV_BUNDLE_VALID)


def test_bv_bundle_object_named_fields_accepted():
    """organisationsnamn/organisationsdatum must validate as objects, not strings."""
    model = validate_raw("bolagsverket", BVBundle, _BV_BUNDLE_VALID)
    assert model.company is not None
    namn = model.company.organisationsnamn["organisationsnamnLista"][0]["namn"]
    assert namn == "AB Volvo"


def test_bv_bundle_missing_org_number():
    bad = {k: v for k, v in _BV_BUNDLE_VALID.items() if k != "org_number"}
    with pytest.raises(SourceSchemaError, match="bolagsverket"):
        validate_raw("bolagsverket", BVBundle, bad)


def test_bv_bundle_extra_field_ignored():
    bundle = {**_BV_BUNDLE_VALID, "new_field": "xyz"}
    validate_raw("bolagsverket", BVBundle, bundle)


# ---------------------------------------------------------------------------
# KvK (Netherlands)
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.kvk import KvKBundle


_KVK_BUNDLE_VALID = {
    "source_id": "kvk",
    "kvk_number": "34327589",
    "company": {"naam": "ASML Holding N.V.", "kvkNummer": "34327589"},
    "legal_name": "ASML Holding N.V.",
    "is_stub": False,
}


def test_kvk_bundle_valid():
    validate_raw("kvk", KvKBundle, _KVK_BUNDLE_VALID)


def test_kvk_bundle_missing_kvk_number():
    bad = {k: v for k, v in _KVK_BUNDLE_VALID.items() if k != "kvk_number"}
    with pytest.raises(SourceSchemaError, match="kvk"):
        validate_raw("kvk", KvKBundle, bad)


def test_kvk_bundle_extra_field_ignored():
    bundle = {**_KVK_BUNDLE_VALID, "extra": True}
    validate_raw("kvk", KvKBundle, bundle)


# ---------------------------------------------------------------------------
# Brreg (Norway)
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.brreg import BrregBundle


_BRREG_BUNDLE_VALID = {
    "source_id": "brreg",
    "orgnr": "914778271",
    "entity": {"organisasjonsnummer": "914778271", "navn": "Equinor ASA"},
    "roles": [],
    "legal_name": "Equinor ASA",
    "is_stub": False,
}


def test_brreg_bundle_valid():
    validate_raw("brreg", BrregBundle, _BRREG_BUNDLE_VALID)


def test_brreg_bundle_missing_orgnr():
    bad = {k: v for k, v in _BRREG_BUNDLE_VALID.items() if k != "orgnr"}
    with pytest.raises(SourceSchemaError, match="brreg"):
        validate_raw("brreg", BrregBundle, bad)


def test_brreg_bundle_extra_field_ignored():
    bundle = {**_BRREG_BUNDLE_VALID, "future_field": "yes"}
    validate_raw("brreg", BrregBundle, bundle)


# ---------------------------------------------------------------------------
# KRS Poland
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.krs_poland import KRSBundle


_KRS_BUNDLE_VALID = {
    "source_id": "krs_poland",
    "hit_id": "0000024765",
    "pl_krs": "0000024765",
    "is_stub": False,
    "name": "PKN ORLEN S.A.",
    "directors": [],
    "supervisory_board": [],
    "shareholders": [],
}


def test_krs_bundle_valid():
    validate_raw("krs_poland", KRSBundle, _KRS_BUNDLE_VALID)


def test_krs_bundle_missing_pl_krs():
    bad = {k: v for k, v in _KRS_BUNDLE_VALID.items() if k != "pl_krs"}
    with pytest.raises(SourceSchemaError, match="krs_poland"):
        validate_raw("krs_poland", KRSBundle, bad)


def test_krs_bundle_extra_field_ignored():
    bundle = {**_KRS_BUNDLE_VALID, "link": "https://example.com"}
    validate_raw("krs_poland", KRSBundle, bundle)


# ---------------------------------------------------------------------------
# Firmenbuch (Austria)
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.firmenbuch import FBBundle


_FB_BUNDLE_VALID = {
    "source_id": "firmenbuch",
    "fn": "473888w",
    "extract": {"name": "OMV AG", "fn": "473888w"},
    "legal_name": "OMV AG",
    "is_stub": False,
}


def test_fb_bundle_valid():
    validate_raw("firmenbuch", FBBundle, _FB_BUNDLE_VALID)


def test_fb_bundle_missing_fn():
    bad = {k: v for k, v in _FB_BUNDLE_VALID.items() if k != "fn"}
    with pytest.raises(SourceSchemaError, match="firmenbuch"):
        validate_raw("firmenbuch", FBBundle, bad)


def test_fb_bundle_extra_field_ignored():
    bundle = {**_FB_BUNDLE_VALID, "soap_error": None}
    validate_raw("firmenbuch", FBBundle, bundle)


def test_fb_bundle_no_extract_is_fine():
    """extract=None is valid — SOAP call may return nothing."""
    bundle = {**_FB_BUNDLE_VALID, "extract": None}
    validate_raw("firmenbuch", FBBundle, bundle)


# ---------------------------------------------------------------------------
# ARES (Czech Republic)
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.ares import AresBundle


_ARES_BUNDLE_VALID = {
    "source_id": "ares",
    "hit_id": "27082440",
    "cz_ico": "27082440",
    "name": "Alza.cz a.s.",
    "is_stub": False,
    "entity": {"ico": "27082440", "obchodniJmeno": "Alza.cz a.s."},
    "owners": [],
    "directors": [],
}


def test_ares_bundle_valid():
    validate_raw("ares", AresBundle, _ARES_BUNDLE_VALID)


def test_ares_bundle_missing_cz_ico():
    bad = {k: v for k, v in _ARES_BUNDLE_VALID.items() if k != "cz_ico"}
    with pytest.raises(SourceSchemaError, match="ares"):
        validate_raw("ares", AresBundle, bad)


def test_ares_bundle_extra_field_ignored():
    bundle = {**_ARES_BUNDLE_VALID, "new_api_section": {}}
    validate_raw("ares", AresBundle, bundle)


# ---------------------------------------------------------------------------
# SEC EDGAR
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.sec_edgar import EDGARBundle


_EDGAR_BUNDLE_VALID = {
    "source_id": "sec_edgar",
    "hit_id": "320193",
    "issuer_cik": "320193",
    "filings": [
        {
            "reporter": {"name": "Berkshire Hathaway Inc.", "reporter_cik": "1067983"},
            "issuer": {"name": "Apple Inc.", "cik": "320193"},
            "form_type": "SC 13G/A",
            "filed": "2024-02-14",
            "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/000095013424000001/primary_doc.xml",
        }
    ],
}


def test_edgar_bundle_valid():
    validate_raw("sec_edgar", EDGARBundle, _EDGAR_BUNDLE_VALID)


def test_edgar_bundle_missing_issuer_cik():
    bad = {k: v for k, v in _EDGAR_BUNDLE_VALID.items() if k != "issuer_cik"}
    with pytest.raises(SourceSchemaError, match="sec_edgar"):
        validate_raw("sec_edgar", EDGARBundle, bad)


def test_edgar_bundle_empty_filings_valid():
    bundle = {**_EDGAR_BUNDLE_VALID, "filings": []}
    validate_raw("sec_edgar", EDGARBundle, bundle)


def test_edgar_bundle_extra_field_ignored():
    bundle = {**_EDGAR_BUNDLE_VALID, "new_sec_field": "SC 13G"}
    validate_raw("sec_edgar", EDGARBundle, bundle)


# ---------------------------------------------------------------------------
# BCE Belgium
# ---------------------------------------------------------------------------
from opencheck.sources.schemas.bce_belgium import BCEBundle


_BCE_BUNDLE_VALID = {
    "source_id": "bce_belgium",
    "enterprise_number": "0403019261",
    "dotted": "0403.019.261",
    "name": "AB InBev SA/NV",
    "name_nl": "AB InBev NV",
    "name_fr": "AB InBev SA",
    "name_de": "",
    "status": "Active",
    "juridical_form": "SA/NV",
    "start_date": "1977-01-01",
    "is_stub": False,
}


def test_bce_bundle_valid():
    validate_raw("bce_belgium", BCEBundle, _BCE_BUNDLE_VALID)


def test_bce_bundle_missing_enterprise_number():
    bad = {k: v for k, v in _BCE_BUNDLE_VALID.items() if k != "enterprise_number"}
    with pytest.raises(SourceSchemaError, match="bce_belgium"):
        validate_raw("bce_belgium", BCEBundle, bad)


def test_bce_bundle_extra_field_ignored():
    bundle = {**_BCE_BUNDLE_VALID, "vat": "BE0403019261"}
    validate_raw("bce_belgium", BCEBundle, bundle)


# ---------------------------------------------------------------------------
# SourceSchemaError — structural checks
# ---------------------------------------------------------------------------


def test_source_schema_error_carries_source_id():
    bad = {"hit_id": "x"}  # missing 'lei'
    with pytest.raises(SourceSchemaError) as exc_info:
        validate_raw("gleif", GLEIFBundle, bad)
    assert exc_info.value.source_id == "gleif"


def test_source_schema_error_message_mentions_field():
    bad = {"hit_id": "x"}  # missing 'lei'
    with pytest.raises(SourceSchemaError) as exc_info:
        validate_raw("gleif", GLEIFBundle, bad)
    assert "lei" in str(exc_info.value)
