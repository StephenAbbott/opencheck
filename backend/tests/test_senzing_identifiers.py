"""How the Senzing export types an identifier and names its country
(`bods/identifier_kinds.py`, read by `bods/senzing.py`).

OpenSanctions changelog #54 (13 Nov 2026): Senzing keeps two records apart
only when the identifier type and country match and the numbers differ; a
missing type or country is supporting evidence. A wrong one is the costly
mistake, so the table is fixed and every scheme a mapper writes must be in it
(the mapper guard in ``tests/_entity_subtype_guard.py`` enforces that).
"""

from __future__ import annotations

import pytest

from opencheck.bods import identifier_kinds as ik
from opencheck.bods import map_to_senzing


def _entity(*identifiers: dict) -> dict:
    return {
        "statementId": "e1", "recordId": "e1", "recordType": "entity",
        "recordDetails": {
            "entityType": {"type": "registeredEntity"}, "name": "Acme",
            "identifiers": list(identifiers),
        },
    }


def _id_features(*identifiers: dict) -> list[dict]:
    [rec] = map_to_senzing([_entity(*identifiers)])
    return [
        f for f in rec["FEATURES"]
        if any(k.endswith("_NUMBER") for k in f)
    ]


@pytest.mark.parametrize(
    ("scheme", "kind"),
    [
        ("XI-LEI", ik.LEI),
        ("GB-COH", ik.REGISTER),
        ("EE-ARIREGISTER", ik.REGISTER),
        ("RA000561", ik.REGISTER),
        ("REG-BZ", ik.REGISTER),
        ("REG", ik.REGISTER),
        ("US-DE", ik.REGISTER),
        ("SK-OR", ik.REGISTER),
        ("XI-VAT", ik.TAX),
        ("PL-NIP", ik.TAX),
        ("CA-BN", ik.TAX),
        ("GBR-TAXID", ik.TAX),
        ("RUS-TAXID", ik.TAX),
        ("FRA-PASSPORT", ik.PASSPORT),
        ("DEU-IDCARD", ik.REGISTER),
        ("WIKIDATA", ik.OTHER),
        ("OpenCorporates", ik.OTHER),
        ("S&P CIQ Company ID", ik.OTHER),
        ("ISO-9362", ik.OTHER),
        ("US-SEC-CIK", ik.OTHER),
        ("EE-ARIREGISTER-HASH", ik.OTHER),
        ("NACE2", ik.DROP),
        ("FI-TOL", ik.DROP),
        ("ISIN", ik.DROP),
        ("ISO-10383", ik.DROP),
        ("TICKER", ik.DROP),
        ("SOMETHING-NEW", ik.UNCLASSIFIED),
        ("", ik.UNCLASSIFIED),
    ],
)
def test_scheme_kind(scheme: str, kind: str) -> None:
    assert ik.scheme_kind(scheme) == kind


@pytest.mark.parametrize(
    ("scheme", "country"),
    [
        ("GB-COH", "GB"),
        ("US-DE", "US"),
        ("US-SEC-CIK", "US"),
        ("REG-BZ", "BZ"),
        ("REG-GB", "GB"),
        ("GBR-TAXID", "GB"),
        ("RUS-TAXID", "RU"),
        ("XI-VAT", ""),       # org-id's "international" is no country
        ("XI-LEI", ""),
        ("RA000561", ""),     # a GLEIF RA code says nothing about the country
        ("REG", ""),
        ("WIKIDATA", ""),
        ("GEM-ENTITY", ""),   # three letters that are not a person-document scheme
        ("ISO-9362", ""),
    ],
)
def test_scheme_country(scheme: str, country: str) -> None:
    assert ik.scheme_country(scheme) == country


def test_country_is_never_read_from_scheme_name() -> None:
    assert _id_features({"id": "123", "scheme": "RA000561",
                         "schemeName": "Registrar of Companies, United Kingdom"}) == [
        {"NATIONAL_ID_NUMBER": "123", "NATIONAL_ID_TYPE": "RA000561"},
    ]


def test_register_number_is_a_national_id_with_its_country() -> None:
    assert _id_features({"id": "00102498", "scheme": "GB-COH"}) == [
        {"NATIONAL_ID_NUMBER": "00102498", "NATIONAL_ID_TYPE": "GB-COH",
         "NATIONAL_ID_COUNTRY": "GB"},
    ]


def test_vat_number_is_a_tax_id_with_no_invented_country() -> None:
    assert _id_features(
        {"id": "BE0202239951", "scheme": "XI-VAT"},
        {"id": "5260250995", "scheme": "PL-NIP"},
    ) == [
        {"TAX_ID_NUMBER": "BE0202239951", "TAX_ID_TYPE": "XI-VAT"},
        {"TAX_ID_NUMBER": "5260250995", "TAX_ID_TYPE": "PL-NIP", "TAX_ID_COUNTRY": "PL"},
    ]


def test_other_databases_keys_are_other_ids() -> None:
    assert _id_features(
        {"id": "Q154950", "scheme": "WIKIDATA"},
        {"id": "1059556", "scheme": "US-SEC-CIK"},
    ) == [
        {"OTHER_ID_NUMBER": "Q154950", "OTHER_ID_TYPE": "WIKIDATA"},
        {"OTHER_ID_NUMBER": "1059556", "OTHER_ID_TYPE": "US-SEC-CIK", "OTHER_ID_COUNTRY": "US"},
    ]


def test_codes_that_identify_no_one_are_left_out() -> None:
    assert _id_features(
        {"id": "64.19", "scheme": "NACE2"},
        {"id": "US0378331005", "scheme": "ISIN"},
        {"id": "XLON", "scheme": "ISO-10383"},
    ) == []


def test_unknown_scheme_is_supporting_evidence_only() -> None:
    assert _id_features({"id": "X1", "scheme": "SOMETHING-NEW"}) == [
        {"OTHER_ID_NUMBER": "X1", "OTHER_ID_TYPE": "SOMETHING-NEW"},
    ]


def test_passport_carries_its_country() -> None:
    person = {
        "statementId": "p1", "recordId": "p1", "recordType": "person",
        "recordDetails": {
            "personType": "knownPerson", "names": [{"fullName": "A B"}],
            "identifiers": [
                {"id": "12AB34567", "scheme": "FRA-PASSPORT"},
                {"id": "770000000000", "scheme": "RUS-TAXID"},
            ],
        },
    }
    [rec] = map_to_senzing([person])
    assert {"PASSPORT_NUMBER": "12AB34567", "PASSPORT_COUNTRY": "FR"} in rec["FEATURES"]
    assert {"TAX_ID_NUMBER": "770000000000", "TAX_ID_TYPE": "RUS-TAXID",
            "TAX_ID_COUNTRY": "RU"} in rec["FEATURES"]


def test_lei_value_under_a_register_scheme_is_still_an_lei() -> None:
    assert _id_features({"id": "5493001KJTIIGC8Y1R12", "scheme": "GB-COH"}) == [
        {"LEI_NUMBER": "5493001KJTIIGC8Y1R12"},
    ]


def test_scheme_name_mentioning_lei_does_not_make_an_lei() -> None:
    # The old rule read "LEI" anywhere in scheme + schemeName.
    assert _id_features({"id": "HRB 1234", "scheme": "RA000197",
                         "schemeName": "Amtsgericht Leipzig"}) == [
        {"NATIONAL_ID_NUMBER": "HRB 1234", "NATIONAL_ID_TYPE": "RA000197"},
    ]


def test_every_feature_attribute_is_described_in_the_data_dictionary() -> None:
    from opencheck.data_dictionary import COLUMN_DESCRIPTIONS
    from opencheck.bods.senzing import _KIND_FEATURE

    described = COLUMN_DESCRIPTIONS["senzing"]
    for keys in _KIND_FEATURE.values():
        for key in keys:
            if key:
                assert key in described, key
