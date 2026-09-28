"""Phase 256 — three defects found by the Phase 255 post-deploy checks.

* The person-identifier note read "Wikidata Q identifier identifier Q525666"
  and gave back "Wikidata Q" as the scheme name.
* A person's Russian INN (a personal tax number) was moved to an annotation;
  BODS has a slot for it, ``RUS-TAXID``.
* A listing set ``marketIdentifierCode`` without
  ``operatingMarketIdentifierCode``.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from opencheck import listing
from opencheck.bods.annotations import person_identifiers_from_annotations
from opencheck.bods.ftm import map_to_ftm
from opencheck.bods.mapper import map_ftm
from opencheck.bods.statements import make_person_statement

_REPO = Path(__file__).resolve().parents[2]


def _person(identifiers):
    return make_person_statement(
        source_id="wikidata", local_id="Q525666", full_name="Igor Sechin", identifiers=identifiers
    )


def test_the_note_names_the_scheme_once_and_reads_back_whole():
    stmt = _person([{"id": "Q525666", "scheme": "WIKIDATA", "schemeName": "Wikidata Q identifier",
                     "uri": "https://www.wikidata.org/wiki/Q525666"}])
    (note,) = stmt["annotations"]
    assert "identifier identifier" not in note["description"]
    assert note["description"].startswith(
        "Identifier Q525666 (scheme WIKIDATA; Wikidata Q identifier). BODS keeps"
    )
    assert person_identifiers_from_annotations(stmt) == [
        {"id": "Q525666", "scheme": "WIKIDATA", "schemeName": "Wikidata Q identifier",
         "uri": "https://www.wikidata.org/wiki/Q525666"}
    ]


def test_a_scheme_name_with_brackets_reads_back_whole():
    stmt = _person([{"id": "abc", "scheme": "EE-ARIREGISTER-HASH",
                     "schemeName": "Estonian e-Business Register (person hash)"}])
    (ident,) = person_identifiers_from_annotations(stmt)
    assert ident["schemeName"] == "Estonian e-Business Register (person hash)"
    assert ident["scheme"] == "EE-ARIREGISTER-HASH"


def test_the_phase_255_sentence_is_still_read():
    """Saved reports keep the statements of their day."""
    stmt = {"annotations": [{
        "motivation": "identifying", "statementPointerTarget": "/recordDetails",
        "description": ("Wikidata Q identifier identifier Q525666 (scheme WIKIDATA). BODS keeps "
                        "a person's identifiers for identity documents, so this one is published here."),
        "url": "https://www.wikidata.org/wiki/Q525666",
    }]}
    (ident,) = person_identifiers_from_annotations(stmt)
    assert ident["id"] == "Q525666" and ident["scheme"] == "WIKIDATA"


def test_a_personal_inn_is_a_taxid_and_stays_in_identifiers():
    stmt = _person([{"id": "770370393938", "scheme": "RU-INN", "schemeName": "Russian INN"}])
    assert stmt["recordDetails"]["identifiers"] == [
        {"id": "770370393938", "scheme": "RUS-TAXID", "schemeName": "Russian INN"}
    ]
    assert "annotations" not in stmt


def test_a_company_length_inn_on_a_person_is_not_called_a_taxid():
    stmt = _person([{"id": "7706107510", "scheme": "RU-INN", "schemeName": "Russian INN"}])
    assert "identifiers" not in stmt["recordDetails"]
    assert person_identifiers_from_annotations(stmt)[0]["scheme"] == "RU-INN"


def test_sechin_from_opensanctions_validates_and_exports_his_inn():
    """The shape OpenSanctions gave on 28 Sept 2026."""
    (person,) = list(map_ftm(
        {"id": "Q525666", "schema": "Person", "properties": {
            "name": ["Игор Сечин"], "birthDate": ["1960-09-07"],
            "wikidataId": ["Q525666"], "innCode": ["770370393938"]}},
        source_id="opensanctions",
        source_url_builder=lambda i: f"https://www.opensanctions.org/entities/{i}/",
    ))
    assert person["recordDetails"]["identifiers"] == [
        {"id": "770370393938", "scheme": "RUS-TAXID", "schemeName": "Russian INN"}
    ]
    schemes = {i["scheme"] for i in person_identifiers_from_annotations(person)}
    assert schemes == {"OPENSANCTIONS", "WIKIDATA"}

    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import validate_bods_statements

    report = validate_bods_statements([person])
    assert report["json_errors"] == [] and report["additional_errors"] == []

    (ftm,) = [e for e in map_to_ftm([person]) if e.get("schema") == "Person"]
    assert "770370393938" in repr(ftm["properties"])


# ---------------------------------------------------------------------------
# The listing's operating MIC
# ---------------------------------------------------------------------------


def _listed(mic: str) -> dict:
    venue = listing.VENUES[mic]
    return {"status": "listed", "quote": {"ticker": "SHEL", "mic": mic},
            "exchange": {"name": venue.name, "country": venue.country}}


@pytest.mark.parametrize(("mic", "operating"), [
    ("XLON", "XLON"), ("XNGS", "XNAS"), ("XMSM", "XDUB"), ("XTKS", "XJPX"), ("ARCX", "XNYS"),
])
def test_a_listing_carries_its_operating_mic(mic, operating):
    out = listing.securities_listing(_listed(mic))
    assert out["marketIdentifierCode"] == mic
    assert out["operatingMarketIdentifierCode"] == operating


def test_every_venue_has_an_operating_mic_that_is_one():
    """Pinned against the ISO 10383 facts read on 28 Sept 2026: the segment
    MICs map to their operating MIC, and the rest are operating MICs."""
    segments = set(listing.OPERATING_MIC)
    for mic in listing.VENUES:
        op = listing.operating_mic(mic)
        assert op and len(op) == 4
        if mic not in segments:
            assert op == mic
    assert listing.operating_mic("ZZZZ") is None


def test_shells_listing_validates_under_libcovebods():
    pytest.importorskip("libcovebods")
    from opencheck.bods.statements import make_entity_statement
    from tests.test_bods_libcovebods import validate_bods_statements

    stmt = make_entity_statement(source_id="gleif", local_id="21380068P1DRHMJ8KU70", name="SHELL PLC")
    stmt["recordDetails"]["publicListing"] = {
        "hasPublicListing": True,
        "securitiesListings": [listing.securities_listing(_listed("XLON"))],
    }
    report = validate_bods_statements([stmt])
    assert report["json_errors"] == [] and report["additional_errors"] == []
