"""Tests for the Cyprus DRCOR adapter and BODS mapper.

The adapter queries a local SQLite DB (built by scripts/extract_cyprus.py from
the data.gov.cy CSVs). Tests inject an in-memory DB via ``adapter._db`` so no
filesystem or network access is needed.

NOTE: the fixture column names mirror the adapter's ``_COLS`` candidates. When
you build the real DB, confirm the CSV headers printed by extract_cyprus.py
match these; adjust ``_COLS`` if they differ.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from opencheck.sources.cyprus_drcor import (
    CyprusDrcorAdapter,
    CY_DRCOR_RA_CODE,
    normalise_he_number,
    he_type_code,
)
from opencheck.sources.base import SearchKind
from opencheck.bods.mapper import map_cyprus_drcor


# ---------------------------------------------------------------------------
# Fixtures — rows modelled on DRCOR open-data columns
# ---------------------------------------------------------------------------

ORG_ROW = {
    "registration_no": "489243",
    "organisation_name": "VELRY GROUP LTD",
    "organisation_type": "Limited Company",
    "organisation_type_code": "HE",
    "organisation_status": "Active",
    "registration_date": "2018-08-29",
}
OFFICE_ROW = {
    "registration_no": "489243",
    "street": "Lordou Vyronos",
    "building": "61-63",
    "territory": "Larnaca",
}
OFFICIALS_ROWS = [
    {
        "registration_no": "489243",
        "person_or_organisation_name": "ALICE PAPADOPOULOU",
        "official_position": "Director",
    },
    {
        "registration_no": "489243",
        "person_or_organisation_name": "FIDUCIARY SERVICES LIMITED",
        "official_position": "Secretary",
    },
]


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE organisations (reg_no_norm TEXT, name TEXT, data TEXT)")
    conn.execute("CREATE TABLE registered_office (reg_no_norm TEXT, data TEXT)")
    conn.execute("CREATE TABLE officials (reg_no_norm TEXT, data TEXT)")
    conn.execute(
        "INSERT INTO organisations VALUES (?,?,?)",
        ("489243", ORG_ROW["organisation_name"], json.dumps(ORG_ROW)),
    )
    conn.execute(
        "INSERT INTO registered_office VALUES (?,?)", ("489243", json.dumps(OFFICE_ROW))
    )
    for r in OFFICIALS_ROWS:
        conn.execute("INSERT INTO officials VALUES (?,?)", ("489243", json.dumps(r)))
    conn.commit()
    return conn


@pytest.fixture
def adapter():
    return CyprusDrcorAdapter()


@pytest.fixture
def live_adapter():
    a = CyprusDrcorAdapter()
    a._db = _make_db()  # inject in-memory DB (bypasses settings/filesystem)
    return a


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def test_ra_code():
    assert CY_DRCOR_RA_CODE == "RA000161"


@pytest.mark.parametrize(
    "raw,expected",
    [("ΗΕ 489243", "489243"), ("HE489243", "489243"), ("489243", "489243"), (" 489243 ", "489243")],
)
def test_normalise_he_number(raw, expected):
    assert normalise_he_number(raw) == expected


def test_he_type_code():
    assert he_type_code("ΗΕ 489243") == "HE"
    assert he_type_code("HE489243") == "HE"


def test_requires_no_api_key(adapter):
    assert not adapter.info.requires_api_key


# ---------------------------------------------------------------------------
# Fetch (local SQLite)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_full_bundle(live_adapter):
    bundle = await live_adapter.fetch("ΗΕ 489243", legal_name="VELRY GROUP LTD")
    assert bundle["is_stub"] is False
    assert bundle["reg_no"] == "489243"
    assert bundle["name"] == "VELRY GROUP LTD"
    assert bundle["organisation"]["organisation_type_code"] == "HE"
    assert bundle["address"]["territory"] == "Larnaca"
    assert len(bundle["officials"]) == 2


@pytest.mark.asyncio
async def test_fetch_stub_when_no_db(adapter):
    # No DB file configured and none injected → stub.
    bundle = await adapter.fetch("489243", legal_name="VELRY GROUP LTD")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_stub_when_no_org_row(live_adapter):
    bundle = await live_adapter.fetch("000000")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_search_returns_stub_without_db(adapter):
    hits = await adapter.search("VELRY", SearchKind.ENTITY)
    assert hits and hits[0].is_stub is True


@pytest.mark.asyncio
async def test_search_finds_by_like_with_db(live_adapter):
    hits = await live_adapter.search("VELRY GROUP LTD", SearchKind.ENTITY)
    assert hits and hits[0].is_stub is False
    assert hits[0].identifiers["cy_he"] == "489243"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------

def _bundle():
    return {
        "source_id": "cyprus_drcor",
        "reg_no": "489243",
        "name": "VELRY GROUP LTD",
        "organisation": ORG_ROW,
        "address": OFFICE_ROW,
        "officials": OFFICIALS_ROWS,
        "legal_name": "VELRY GROUP LTD",
        "link": "https://data.gov.cy/",
        "is_stub": False,
    }


def test_mapper_emits_entity_person_and_relationships():
    statements = list(map_cyprus_drcor(_bundle()))
    by_type: dict[str, list] = {"entity": [], "person": [], "relationship": []}
    for s in statements:
        by_type[s["recordType"]].append(s)

    # Company entity + the corporate official ("FIDUCIARY SERVICES LIMITED").
    assert len(by_type["entity"]) == 2
    # Alice is a natural person.
    assert len(by_type["person"]) == 1
    # One relationship per official.
    assert len(by_type["relationship"]) == 2

    company = by_type["entity"][0]
    assert company["recordDetails"]["name"] == "VELRY GROUP LTD"
    ident = company["recordDetails"]["identifiers"][0]
    assert ident["scheme"] == "CY-DRCOR"
    assert ident["id"] == "HE489243"
    assert company["recordDetails"]["jurisdiction"]["code"] == "CY"

    interests = by_type["relationship"][0]["recordDetails"]["interests"]
    assert interests[0]["type"] == "seniorManagingOfficial"
    assert interests[0]["beneficialOwnershipOrControl"] is False
    details = {r["recordDetails"]["interests"][0]["details"] for r in by_type["relationship"]}
    assert details == {"Director", "Secretary"}


def test_mapper_skips_stub():
    assert list(map_cyprus_drcor({"is_stub": True})) == []
