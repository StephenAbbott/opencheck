"""Tests for the Malta Business Registry adapter and BODS mapper.

The MBR Open Data API is key-less and returns entity data only, so the
adapter resolves a single company by registration number and the mapper
produces a single BODS entity statement.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.bods.mapper import map_malta_mbr
from opencheck.sources.base import SearchKind
from opencheck.sources.malta_mbr import (
    MT_RA_CODE,
    MaltaMbrAdapter,
    normalise_mt_crn,
)

# Representative MBR detail response (wrapped in a top-level ``data`` object).
MALTA_DETAIL: dict[str, Any] = {
    "data": {
        "name": "BLUE LAGOON HOLDING LIMITED",
        "type": "Private Limited Liability Company",
        "state": "Active",
        "address": "MERCIECA SUITE 4 TRIQ IT-TABIB ANTON TABONE",
        "country": None,
        "area_of_activity": "64200",
        "locality": "RABAT GHAWDEX",
        "postcode": "VCT 9020",
        "registration_date": "2014-06-05",
        "registration_number": "C 113927",
        "status_effective_date": None,
        "street": None,
    }
}


# ---------------------------------------------------------------------------
# normalise_mt_crn + constant
# ---------------------------------------------------------------------------


class TestNormaliseMtCrn:
    def test_inserts_single_space(self) -> None:
        assert normalise_mt_crn("C113927") == "C 113927"

    def test_collapses_extra_whitespace(self) -> None:
        assert normalise_mt_crn("C   113927") == "C 113927"

    def test_uppercases_prefix(self) -> None:
        assert normalise_mt_crn("c 113927") == "C 113927"

    def test_leaves_canonical_unchanged(self) -> None:
        assert normalise_mt_crn("C 113927") == "C 113927"

    def test_trims(self) -> None:
        assert normalise_mt_crn("  C 1  ") == "C 1"

    def test_non_matching_returned_trimmed_upper(self) -> None:
        assert normalise_mt_crn("  foo-bar ") == "FOO-BAR"


def test_ra_code() -> None:
    assert MT_RA_CODE == "RA000443"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


def _bundle() -> dict[str, Any]:
    return {
        "source_id": "malta_mbr",
        "mt_crn": "C 113927",
        "company": MALTA_DETAIL["data"],
        "legal_name": "",
        "is_stub": False,
    }


class TestMapMaltaMbr:
    def test_stub_yields_nothing(self) -> None:
        assert list(map_malta_mbr({"is_stub": True, "mt_crn": "C 113927"})) == []

    def test_empty_yields_nothing(self) -> None:
        assert list(map_malta_mbr({})) == []

    def test_single_entity_statement(self) -> None:
        stmts = list(map_malta_mbr(_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1
        # entity-only source — never any person/relationship statements
        assert [s for s in stmts if s["recordType"] in ("person", "relationship")] == []

    def test_entity_core_fields(self) -> None:
        stmt = next(s for s in map_malta_mbr(_bundle()) if s["recordType"] == "entity")
        rd = stmt["recordDetails"]
        assert rd["name"] == "BLUE LAGOON HOLDING LIMITED"
        assert rd["jurisdiction"]["code"] == "MT"
        assert rd.get("foundingDate") == "2014-06-05"
        assert rd["entityType"]["type"] == "registeredEntity"
        # legal form carried as entityType details
        assert rd["entityType"]["details"] == "Private Limited Liability Company"

    def test_entity_identifier_scheme(self) -> None:
        stmt = next(s for s in map_malta_mbr(_bundle()) if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in stmt["recordDetails"]["identifiers"]}
        assert ids["MT-MBR"] == "C 113927"

    def test_registered_address_concatenated(self) -> None:
        stmt = next(s for s in map_malta_mbr(_bundle()) if s["recordType"] == "entity")
        addrs = stmt["recordDetails"].get("addresses") or []
        assert addrs, "expected a registered address"
        text = addrs[0]["address"]
        assert "MERCIECA SUITE 4" in text
        assert "RABAT GHAWDEX" in text
        assert "VCT 9020" in text
        assert addrs[0]["country"]["code"] == "MT"

    def test_official_register_source(self) -> None:
        for stmt in map_malta_mbr(_bundle()):
            assert stmt["source"]["type"] == ["officialRegister"]

    def test_deterministic_ids(self) -> None:
        ids1 = [s["statementId"] for s in map_malta_mbr(_bundle())]
        ids2 = [s["statementId"] for s in map_malta_mbr(_bundle())]
        assert ids1 == ids2 and ids1


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_returns_empty() -> None:
    adapter = MaltaMbrAdapter()
    assert await adapter.search("Blue Lagoon", SearchKind.ENTITY) == []


@pytest.mark.asyncio
async def test_fetch_builds_bundle(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None
    mock_cache.put.return_value = None

    mock_resp = MagicMock()
    mock_resp.is_success = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = MALTA_DETAIL

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = AsyncMock(return_value=mock_resp)

    with (
        patch("opencheck.sources.malta_mbr.Cache", return_value=mock_cache),
        patch("opencheck.sources.malta_mbr.build_client", return_value=mock_client),
    ):
        adapter = MaltaMbrAdapter()
        bundle = await adapter.fetch("C113927", legal_name="Blue Lagoon")

    get_settings.cache_clear()

    assert bundle["is_stub"] is False
    assert bundle["mt_crn"] == "C 113927"  # normalised on the way in
    assert bundle["company"]["name"] == "BLUE LAGOON HOLDING LIMITED"
    # the request used the canonical, space-encoded registration number
    called_url = mock_client.get.call_args.args[0]
    assert called_url.endswith("/companies/C%20113927")


@pytest.mark.asyncio
async def test_fetch_returns_stub_when_live_disabled(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.has.return_value = False
    mock_cache.get_payload.return_value = None

    with patch("opencheck.sources.malta_mbr.Cache", return_value=mock_cache):
        adapter = MaltaMbrAdapter()
        bundle = await adapter.fetch("C 113927", legal_name="Blue Lagoon")

    get_settings.cache_clear()

    assert bundle["is_stub"] is True
    assert bundle["company"] is None
    assert bundle["mt_crn"] == "C 113927"
