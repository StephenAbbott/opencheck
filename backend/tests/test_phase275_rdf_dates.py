"""Phase 275 — the RDF export types each date by the precision it has.

BODS allows partial dates (a person's ``birthDate`` may be ``YYYY`` or
``YYYY-MM``), and Companies House publishes directors' and PSCs' birth dates
as year and month only. ``rdf._date_lit`` typed everything without a ``T`` as
``xsd:date``, so ``"1942-12"^^xsd:date`` went out ill-typed: 55 of them across
four production exports on 2 Oct 2026, every one a ``YYYY-MM`` birth date, and
each skipped silently by a SPARQL date filter. Every other date in those
exports was already well typed, and still is (replayed in the phase notes).
"""

from __future__ import annotations

import logging

import pytest
from rdflib import Dataset, Literal
from rdflib.namespace import XSD

from opencheck.bods import make_person_statement, to_rdf
from opencheck.bods.rdf import BODS, _date_lit


@pytest.mark.parametrize(
    ("value", "datatype"),
    [
        ("1942", XSD.gYear),
        ("1942-12", XSD.gYearMonth),
        ("1942-01", XSD.gYearMonth),
        ("1942-12-05", XSD.date),
        ("2026-07-21T10:00:00Z", XSD.dateTime),
        ("2026-07-21T10:00:00.123456+00:00", XSD.dateTime),
        (" 1942-12 ", XSD.gYearMonth),
    ],
)
def test_each_precision_gets_its_own_well_formed_datatype(value: str, datatype) -> None:
    lit = _date_lit(value)
    assert lit.datatype == datatype
    assert not lit.ill_typed, f"{value!r} as {datatype} is ill-typed"


@pytest.mark.parametrize(
    "value",
    ["1942-13", "2026-02-30", "2026-07-21T10:00", "circa 1950", "unknown", "19421"],
)
def test_a_value_no_xsd_type_fits_is_a_plain_literal(value: str) -> None:
    lit = _date_lit(value)
    assert lit.datatype is None
    assert str(lit) == value


def _parse_quietly(trig: str, caplog: pytest.LogCaptureFixture) -> Dataset:
    ds = Dataset()
    with caplog.at_level(logging.WARNING, logger="rdflib"):
        ds.parse(data=trig, format="trig")
    return ds


def _literals(ds: Dataset, prop) -> list[Literal]:
    return [o for _, _, o, _ in ds.quads((None, prop, None, None))]


def test_companies_house_month_precision_birth_date(caplog: pytest.LogCaptureFixture) -> None:
    """The production shape: a director whose register publishes 1942-12."""
    person = make_person_statement(
        source_id="companies_house",
        local_id="officer-1",
        full_name="Jane Roe",
        birth_date="1942-12",
    )
    ds = _parse_quietly(to_rdf([person]), caplog)
    (birth,) = _literals(ds, BODS.birthDate)
    assert birth == Literal("1942-12", datatype=XSD.gYearMonth)
    assert not birth.ill_typed
    assert not [r for r in caplog.records if "Failed to convert Literal" in r.getMessage()]


def test_every_date_field_goes_through_the_same_typing() -> None:
    """foundingDate, interest dates, retrievedAt and the statement dates all
    share the helper, so a partial value in any of them is typed honestly."""
    entity = {
        "statementId": "ent-1", "recordId": "ent-1", "recordType": "entity",
        "recordStatus": "new", "statementDate": "2026-07-21",
        "publicationDetails": {"publicationDate": "2026-07-21T09:30:00Z"},
        "source": {"type": ["officialRegister"], "retrievedAt": "2026-07-21"},
        "recordDetails": {
            "entityType": {"type": "registeredEntity"},
            "name": "Acme Ltd", "foundingDate": "1990", "dissolutionDate": "2001-06",
        },
    }
    rel = {
        "statementId": "rel-1", "recordId": "rel-1", "recordType": "relationship",
        "recordDetails": {
            "subject": "ent-1", "interestedParty": "ent-1",
            "interests": [{"type": "shareholding", "startDate": "2016-04", "endDate": "2020"}],
        },
    }
    ds = Dataset()
    ds.parse(data=to_rdf([entity, rel]), format="trig")
    by_prop = {
        prop: _literals(ds, getattr(BODS, prop))[0].datatype
        for prop in ("foundingDate", "dissolutionDate", "startDate", "endDate",
                     "statementDate", "publicationDate", "retrievedAt")
    }
    assert by_prop == {
        "foundingDate": XSD.gYear,
        "dissolutionDate": XSD.gYearMonth,
        "startDate": XSD.gYearMonth,
        "endDate": XSD.gYear,
        "statementDate": XSD.date,
        "publicationDate": XSD.dateTime,
        "retrievedAt": XSD.date,  # a date-only retrievedAt was ill-typed xsd:dateTime
    }



def test_no_ill_typed_literal_anywhere_in_a_mixed_export() -> None:
    people = [
        make_person_statement(source_id="companies_house", local_id=f"p{i}",
                              full_name=f"Person {i}", birth_date=d)
        for i, d in enumerate(["1942", "1942-12", "1942-12-05"])
    ]
    ds = Dataset()
    ds.parse(data=to_rdf(people), format="trig")
    ill = [o for _, _, o, _ in ds.quads((None, None, None, None))
           if isinstance(o, Literal) and o.ill_typed]
    assert ill == []
    assert sorted(str(o.datatype).split("#")[1] for o in _literals(ds, BODS.birthDate)) == [
        "date", "gYear", "gYearMonth",
    ]
