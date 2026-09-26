"""Phase 250 — Wikidata roleholders carry their birth date, at Wikidata's precision.

FullCheck merges two person nodes when their names **and** birth months agree
(``frontend/src/lib/reconcile.ts::personKey``). Wikidata's roleholders had no
birth date, so Shell PLC's board drew "Andrew Mackenzie" (Wikidata) beside
"MACKENZIE, Andrew Stewart" (Companies House) and "Wael Sawan" beside
"SAWAN, Wael (W.)". The rows below are the shapes Wikidata's SPARQL service
returned for Shell (Q154950) on 26 Sept 2026.
"""

from __future__ import annotations

from opencheck.bods import map_wikidata
from opencheck.sources import wikidata
from opencheck.sources.wikidata import _ROLEHOLDER_QUERY, _dob_at_precision, _parse_roleholders

WD = "http://www.wikidata.org/entity/"


def _row(qid: str, name: str, role: str, dob: str | None = None, prec: str | None = None) -> dict:
    row = {
        "person": {"type": "uri", "value": f"{WD}{qid}"},
        "personLabel": {"type": "literal", "value": name},
        "roleLabel": {"type": "literal", "value": role},
    }
    if dob is not None:
        row["dob"] = {"type": "literal", "value": dob}
    if prec is not None:
        row["dobPrecision"] = {"type": "literal", "value": prec}
    return row


def test_query_asks_for_birth_date_and_its_precision() -> None:
    assert "P569" in _ROLEHOLDER_QUERY
    assert "wikibase:timePrecision" in _ROLEHOLDER_QUERY
    # A deprecated P569 is a value Wikidata itself marks as wrong.
    assert "DeprecatedRank" in _ROLEHOLDER_QUERY


def test_precision_is_kept_never_widened() -> None:
    assert _dob_at_precision("1956-12-20T00:00:00Z", "11") == "1956-12-20"
    # Wael Sawan: month precision. SPARQL pads it to the 1st; that day is invented.
    assert _dob_at_precision("1974-07-01T00:00:00Z", "10") == "1974-07"
    # Year precision must not become "1 January" — the merge keys on the month.
    assert _dob_at_precision("1956-01-01T00:00:00Z", "9") == "1956"
    assert _dob_at_precision("1950-01-01T00:00:00Z", "8") is None
    assert _dob_at_precision("+1956-12-20T00:00:00Z", "11") == "1956-12-20"
    assert _dob_at_precision(None, "11") is None
    assert _dob_at_precision("not a date", "11") is None


def test_one_person_two_roles_one_birth_date() -> None:
    people = _parse_roleholders([
        _row("Q1", "Andrew Mackenzie", "board member", "1956-12-20T00:00:00Z", "11"),
        _row("Q1", "Andrew Mackenzie", "chairperson", "1956-12-20T00:00:00Z", "11"),
        _row("Q2", "Wael Sawan", "chief executive officer", "1974-07-01T00:00:00Z", "10"),
        _row("Q3", "Euleen Goh", "board member"),
    ])
    by = {p["qid"]: p for p in people}
    assert by["Q1"]["birth_date"] == "1956-12-20"
    assert len(by["Q1"]["roles"]) == 2
    assert by["Q2"]["birth_date"] == "1974-07"
    assert "birth_date" not in by["Q3"]
    assert all("_dobs" not in p for p in people)


def test_disputed_birth_dates_publish_none() -> None:
    people = _parse_roleholders([
        _row("Q9", "Disputed Person", "board member", "1960-03-01T00:00:00Z", "11"),
        _row("Q9", "Disputed Person", "board member", "1961-03-01T00:00:00Z", "11"),
    ])
    assert "birth_date" not in people[0]


def test_mapper_writes_birth_date_on_the_roleholder() -> None:
    bundle = {
        "source_id": "wikidata",
        "qid": "Q154950",
        "summary": {
            "qid": "Q154950",
            "label": "Shell",
            "is_person": False,
            "is_entity": True,
            "instance_of": [],
            "citizenships": [],
            "positions": [],
            "identifiers": {"lei": "21380068P1DRHMJ8KU70"},
            "country": None,
            "dob": None,
            "dod": None,
            "inception": None,
            "parent_orgs": [],
            "roleholders": [
                {"qid": "Q2", "name": "Wael Sawan", "birth_date": "1974-07",
                 "roles": [{"label": "chief executive officer", "start": None}]},
                {"qid": "Q3", "name": "Euleen Goh",
                 "roles": [{"label": "board member", "start": None}]},
            ],
        },
    }
    people = {
        s["recordDetails"]["names"][0]["fullName"]: s["recordDetails"]
        for s in map_wikidata(bundle)
        if s["recordType"] == "person"
    }
    assert people["Wael Sawan"]["birthDate"] == "1974-07"
    assert "birthDate" not in people["Euleen Goh"]


def test_roleholder_cache_key_is_versioned() -> None:
    # Summaries cached before Phase 250 have no birth dates; a new key makes
    # every entity re-ask rather than wait out the cache.
    import inspect

    assert "roleholders-v2/" in inspect.getsource(wikidata.WikidataAdapter)
