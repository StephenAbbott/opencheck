"""A FtM record that names several countries gets no jurisdiction.

The FtM jurisdiction ticket (24 Nov 2026): OpenSanctions' and OpenAleph's
records of Bank Saderat PLC, an English company (GB-COH 01126618), carry
``country: ["ir", "gb"]``. ``_ftm_jurisdiction`` took the first value, so
three of the subject's statements said Iran and FATF_BLACK_LIST /
EU_HIGH_RISK_THIRD_COUNTRY listed IR with ``position: "subject"`` on them.
The rule is now the registration country's (``ftm_registration_country``):
the first property the record fills — ``jurisdiction``, then ``country`` —
must name exactly one country, or the record has none, said in a
``commenting`` annotation listing what the source named.
"""

from __future__ import annotations

import pytest

from opencheck.bods import map_opensanctions
from opencheck.bods.mappers.ftm import _ftm_jurisdiction
from opencheck.config import get_settings
from opencheck.risk import (
    EU_HIGH_RISK_THIRD_COUNTRY,
    FATF_BLACK_LIST,
    assess_structure,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _entity(props: dict, entity_id: str = "NK-saderat-plc") -> dict:
    payload = {
        "entity": {
            "id": entity_id,
            "schema": "Company",
            "caption": "Bank Saderat PLC",
            "properties": {"name": ["Bank Saderat PLC"], **props},
        }
    }
    return next(
        s for s in map_opensanctions(payload) if s.get("recordType") == "entity"
    )


@pytest.mark.parametrize(
    "props, expected",
    [
        ({"country": ["ir", "gb"]}, None),
        ({"country": ["gb", "ir"]}, None),
        ({"jurisdiction": ["IR", "GB"]}, None),
        ({"country": ["gb", "GB", "United Kingdom"]}, ("United Kingdom", "GB")),
        ({"jurisdiction": ["US", "US-DE"]}, ("US-DE", "US-DE")),
        ({"jurisdiction": ["gb"], "country": ["ir"]}, ("United Kingdom", "GB")),
        ({"jurisdiction": ["gb", "ir"], "country": ["gb"]}, None),
        ({"country": ["Atlantis", "Lemuria"]}, None),
        ({"country": ["Atlantis"]}, ("Atlantis", None)),
        ({}, None),
    ],
)
def test_one_country_or_none(props, expected) -> None:
    assert _ftm_jurisdiction(props) == expected


def test_bank_saderat_plc_record_names_no_jurisdiction_and_says_why() -> None:
    stmt = _entity({"country": ["ir", "gb"], "registrationNumber": ["01126618"]})
    rd = stmt["recordDetails"]
    assert "jurisdiction" not in rd
    notes = [
        a for a in rd.get("annotations") or stmt.get("annotations") or []
        if a.get("motivation") == "commenting" and "countries" in a["description"]
    ]
    assert len(notes) == 1
    note = notes[0]
    assert note["statementPointerTarget"] == "/recordDetails"
    assert "Iran" in note["description"] and "(IR)" in note["description"]
    assert "United Kingdom (GB)" in note["description"]
    assert "country property" in note["description"]
    # The registration number stays bare, as the registration-country rule
    # already said.
    assert {"scheme": "REG", "id": "01126618"}.items() <= next(
        i for i in rd["identifiers"] if i["id"] == "01126618"
    ).items()


def test_a_single_country_record_carries_no_note() -> None:
    stmt = _entity({"country": ["gb"]})
    assert stmt["recordDetails"]["jurisdiction"] == {
        "name": "United Kingdom",
        "code": "GB",
    }
    annotations = stmt.get("annotations") or stmt["recordDetails"].get("annotations") or []
    assert not [a for a in annotations if "countries" in a.get("description", "")]


def _subject_positions(signals, code):
    return [
        j
        for s in signals
        if s.code == code
        for j in s.evidence.get("jurisdictions") or []
        if j.get("position") == "subject"
    ]


def test_the_subject_is_no_longer_read_as_iranian() -> None:
    """Before the fix this bundle drew FATF / EU signals at the subject."""
    stmt = _entity({"country": ["ir", "gb"]})
    signals = assess_structure(
        "opensanctions", {"entity_id": "NK-saderat-plc"}, [stmt],
        hit_id="NK-saderat-plc",
    )
    assert not _subject_positions(signals, FATF_BLACK_LIST)
    assert not _subject_positions(signals, EU_HIGH_RISK_THIRD_COUNTRY)


def test_a_record_that_says_iran_alone_still_reads_iran() -> None:
    """A genuinely Iranian record keeps its signal: only the choice is refused."""
    stmt = _entity({"country": ["ir"]}, entity_id="NK-bank-saderat-iran")
    signals = assess_structure(
        "opensanctions", {"entity_id": "NK-bank-saderat-iran"}, [stmt],
        hit_id="NK-bank-saderat-iran",
    )
    assert _subject_positions(signals, FATF_BLACK_LIST)
