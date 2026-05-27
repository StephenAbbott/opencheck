"""Tests for the ACRA Singapore adapter and BODS mapper.

Uses an in-memory SQLite database populated with fixture rows — no real DB
file required, no network calls.
"""

from __future__ import annotations

import sqlite3
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from opencheck.sources.acra_singapore import (
    AcraSingaporeAdapter,
    ACRA_RA_CODE,
    normalise_uen,
)
from opencheck.bods.mapper import map_acra_singapore


# ---------------------------------------------------------------------------
# Fixtures — representative ACRA rows
# ---------------------------------------------------------------------------

ROWS: list[dict[str, Any]] = [
    {
        "uen": "200312345E",
        "issuance_agency_desc": "ACRA",
        "uen_status_desc": "Live",
        "entity_name": "STARK ENTERPRISES PRIVATE LIMITED",
        "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
        "uen_issue_date": "2003-04-01",
        "reg_street_name": "1 RAFFLES PLACE",
        "reg_postal_code": "048616",
    },
    {
        "uen": "199804567K",
        "issuance_agency_desc": "ACRA",
        "uen_status_desc": "Struck Off",
        "entity_name": "GOLDEN ACACIA CAPITAL PTE LTD",
        "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
        "uen_issue_date": "1998-07-15",
        "reg_street_name": "",
        "reg_postal_code": "",
    },
]


def _make_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection pre-loaded with fixture rows."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE entities (
            uen                  TEXT PRIMARY KEY,
            issuance_agency_desc TEXT,
            uen_status_desc      TEXT,
            entity_name          TEXT,
            entity_type_desc     TEXT,
            uen_issue_date       TEXT,
            reg_street_name      TEXT,
            reg_postal_code      TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE VIRTUAL TABLE entities_fts USING fts5(
            entity_name,
            uen UNINDEXED,
            content='entities',
            content_rowid='rowid'
        )
        """
    )
    for row in ROWS:
        conn.execute(
            """
            INSERT INTO entities VALUES (
                :uen, :issuance_agency_desc, :uen_status_desc, :entity_name,
                :entity_type_desc, :uen_issue_date, :reg_street_name, :reg_postal_code
            )
            """,
            row,
        )
    conn.execute("INSERT INTO entities_fts(entities_fts) VALUES('rebuild')")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Helper: patch adapter's _conn() to return the in-memory DB
# ---------------------------------------------------------------------------


def _adapter_with_db() -> AcraSingaporeAdapter:
    adapter = AcraSingaporeAdapter()
    adapter._db = _make_db()
    return adapter


# ---------------------------------------------------------------------------
# Unit tests: normalise_uen
# ---------------------------------------------------------------------------


def test_normalise_uen_strips_whitespace() -> None:
    assert normalise_uen("  200312345e  ") == "200312345E"


def test_normalise_uen_empty() -> None:
    assert normalise_uen("") == ""


# ---------------------------------------------------------------------------
# Unit tests: adapter constants
# ---------------------------------------------------------------------------


def test_ra_code_value() -> None:
    assert ACRA_RA_CODE == "RA000523"


# ---------------------------------------------------------------------------
# Unit tests: adapter.fetch — direct UEN lookup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_by_uen_live() -> None:
    adapter = _adapter_with_db()
    bundle = await adapter.fetch("200312345E", legal_name="Stark Enterprises")
    assert bundle["uen"] == "200312345E"
    assert "STARK" in (bundle["entity_name"] or "")
    assert bundle["uen_status_desc"] == "Live"
    assert bundle["uen_issue_date"] == "2003-04-01"
    assert bundle["is_stub"] is False


@pytest.mark.asyncio
async def test_fetch_by_uen_normalises_case() -> None:
    adapter = _adapter_with_db()
    bundle = await adapter.fetch("200312345e")
    assert bundle["uen"] == "200312345E"
    assert bundle["is_stub"] is False


@pytest.mark.asyncio
async def test_fetch_unknown_uen_returns_stub() -> None:
    adapter = _adapter_with_db()
    bundle = await adapter.fetch("999999999Z", legal_name="Unknown Co")
    assert bundle["is_stub"] is True


# ---------------------------------------------------------------------------
# Unit tests: adapter.fetch — name-based lookup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_by_name_finds_entity() -> None:
    adapter = _adapter_with_db()
    # Pass the full entity name as hit_id (the sg_name derived key pattern).
    bundle = await adapter.fetch(
        "STARK ENTERPRISES PRIVATE LIMITED",
        legal_name="STARK ENTERPRISES PRIVATE LIMITED",
    )
    assert bundle["is_stub"] is False
    assert bundle["uen"] == "200312345E"


@pytest.mark.asyncio
async def test_fetch_no_db_returns_stub() -> None:
    adapter = AcraSingaporeAdapter()
    # No DB connected — _conn() returns None.
    with patch.object(adapter, "_conn", return_value=None):
        bundle = await adapter.fetch("200312345E", legal_name="Stark")
    assert bundle["is_stub"] is True


# ---------------------------------------------------------------------------
# Unit tests: adapter.search
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_by_name() -> None:
    from opencheck.sources.base import SearchKind

    adapter = _adapter_with_db()
    hits = await adapter.search("STARK", SearchKind.ENTITY)
    assert len(hits) >= 1
    assert hits[0].source_id == "acra_singapore"
    assert hits[0].identifiers.get("sg_uen") == "200312345E"


@pytest.mark.asyncio
async def test_search_no_results() -> None:
    from opencheck.sources.base import SearchKind

    adapter = _adapter_with_db()
    hits = await adapter.search("ZZZNOMATCH99", SearchKind.ENTITY)
    assert hits == []


@pytest.mark.asyncio
async def test_search_no_db_returns_stub() -> None:
    from opencheck.sources.base import SearchKind

    adapter = AcraSingaporeAdapter()
    with patch.object(adapter, "_conn", return_value=None):
        hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


# ---------------------------------------------------------------------------
# Unit tests: BODS mapper — map_acra_singapore
# ---------------------------------------------------------------------------


def test_map_acra_singapore_entity_statement() -> None:
    bundle: dict[str, Any] = {
        "source_id": "acra_singapore",
        "uen": "200312345E",
        "entity_name": "STARK ENTERPRISES PRIVATE LIMITED",
        "issuance_agency_desc": "ACRA",
        "uen_status_desc": "Live",
        "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
        "uen_issue_date": "2003-04-01",
        "reg_street_name": "1 RAFFLES PLACE",
        "reg_postal_code": "048616",
        "link": "https://www.bizfile.gov.sg/",
        "is_stub": False,
    }
    stmts = list(map_acra_singapore(bundle))
    assert len(stmts) == 1  # entity statement only

    stmt = stmts[0]
    assert stmt["recordType"] == "entity"
    rd = stmt.get("recordDetails") or {}
    assert "STARK" in (rd.get("name") or "")

    ids = rd.get("identifiers") or []
    uen_id = next((i for i in ids if i.get("scheme") == "SG-UEN"), None)
    assert uen_id is not None
    assert uen_id["id"] == "200312345E"

    assert rd.get("foundingDate") == "2003-04-01"

    # Jurisdiction should be SG.
    jur = rd.get("jurisdiction") or {}
    assert jur.get("code") == "SG"


def test_map_acra_singapore_skips_empty_uen() -> None:
    bundle: dict[str, Any] = {
        "source_id": "acra_singapore",
        "uen": "",
        "entity_name": "Mystery Co",
        "is_stub": True,
    }
    stmts = list(map_acra_singapore(bundle))
    assert stmts == []


def test_map_acra_singapore_struck_off() -> None:
    bundle: dict[str, Any] = {
        "source_id": "acra_singapore",
        "uen": "199804567K",
        "entity_name": "GOLDEN ACACIA CAPITAL PTE LTD",
        "issuance_agency_desc": "ACRA",
        "uen_status_desc": "Struck Off",
        "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
        "uen_issue_date": "1998-07-15",
        "reg_street_name": None,
        "reg_postal_code": None,
        "link": None,
        "is_stub": False,
    }
    stmts = list(map_acra_singapore(bundle))
    assert len(stmts) == 1
    # dissolutionDate key should exist when struck off.
    rd = stmts[0].get("recordDetails") or {}
    assert "dissolutionDate" in rd


def test_map_acra_singapore_address_present() -> None:
    bundle: dict[str, Any] = {
        "source_id": "acra_singapore",
        "uen": "200312345E",
        "entity_name": "STARK ENTERPRISES PRIVATE LIMITED",
        "uen_status_desc": "Live",
        "entity_type_desc": "PRIVATE COMPANY LIMITED BY SHARES",
        "uen_issue_date": "2003-04-01",
        "reg_street_name": "1 RAFFLES PLACE",
        "reg_postal_code": "048616",
        "link": None,
        "is_stub": False,
    }
    stmts = list(map_acra_singapore(bundle))
    rd = stmts[0].get("recordDetails") or {}
    # The mapper produces an "addresses" list in recordDetails when street/postal is set.
    addrs = rd.get("addresses") or []
    assert len(addrs) >= 1
    addr_text = " ".join(str(a) for a in addrs)
    assert "RAFFLES" in addr_text or "048616" in addr_text
