"""Tests for the Slovak RPO adapter and BODS mapper.

Uses fixture data representative of live RPO API responses.  No network
calls are made; ``_cache`` is mocked out and ``live_available`` is forced
True so the code paths that build bundles from API responses are exercised.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.rpo_slovakia import (
    RpoSlovakiaAdapter,
    SK_RPO_RA_CODE,
    normalise_ico,
    _extract_ico,
    _extract_current,
    _extract_current_address,
    _entity_link,
)
from opencheck.bods.mapper import map_rpo_slovakia


# ---------------------------------------------------------------------------
# Fixtures — raw RPO API response snapshots
# ---------------------------------------------------------------------------

# Representative RPO search result for an active Slovak a.s. (joint-stock company).
# Based on the actual RPO /search?fullName=... response structure.
RAW_ACTIVE_AS: dict[str, Any] = {
    "id": "c7e1a234-0001-4000-8000-000000000001",
    "identifiers": [
        {
            "type": {"id": "ICO", "value": "IČO"},
            "value": "31320155",
        }
    ],
    "fullNames": [
        {
            "validFrom": "1992-01-01",
            "validTo": None,
            "value": {"value": "SLOVENSKÁ SPORITEĽŇA, a.s."},
        },
        {
            "validFrom": "1990-01-01",
            "validTo": "1991-12-31",
            "value": {"value": "ŠTÁTNA SPORITEĽŇA"},
        },
    ],
    "addresses": [
        {
            "validFrom": "2000-01-01",
            "validTo": None,
            "value": {
                "street": "Tomášikova",
                "buildingNumber": "48",
                "municipality": "Bratislava",
                "postalCode": "832 37",
                "country": "SK",
            },
        }
    ],
    "establishment": "1990-01-01",
    "termination": None,
    "sourceRegister": {
        "registrationNumbers": [
            {"value": "B 601/B"},
        ],
        "registrationOffices": [
            {"value": "Mestský súd Bratislava III"},
        ],
        "value": {"value": "Obchodný register"},
    },
}

# Representative RPO result for a dissolved s.r.o. (LLC).
RAW_DISSOLVED_SRO: dict[str, Any] = {
    "id": "c7e1a234-0002-4000-8000-000000000002",
    "identifiers": [
        {
            "type": {"id": "ICO", "value": "IČO"},
            "value": "12345678",
        }
    ],
    "fullNames": [
        {
            "validFrom": "2000-05-15",
            "validTo": None,
            "value": {"value": "EXAMPLE SK, s.r.o."},
        }
    ],
    "addresses": [
        {
            "validFrom": "2000-05-15",
            "validTo": None,
            "value": {
                "street": "Obchodná",
                "buildingNumber": "5",
                "municipality": "Košice",
                "postalCode": "040 01",
                "country": "SK",
            },
        }
    ],
    "establishment": "2000-05-15",
    "termination": "2022-03-01",
    "sourceRegister": {
        "registrationNumbers": [{"value": "Sro 12345/K"}],
        "registrationOffices": [{"value": "Mestský súd Košice"}],
        "value": {"value": "Obchodný register"},
    },
}

# Entity whose IČO identifier is marked "Neuvedené" (not provided).
RAW_NO_ICO: dict[str, Any] = {
    "id": "c7e1a234-0003-4000-8000-000000000003",
    "identifiers": [
        {
            "type": {"id": "ICO", "value": "IČO"},
            "value": "Neuvedené",
        }
    ],
    "fullNames": [
        {
            "validFrom": "2010-01-01",
            "validTo": None,
            "value": {"value": "NO ICO ENTITY"},
        }
    ],
    "addresses": [],
    "establishment": "2010-01-01",
    "termination": None,
    "sourceRegister": {},
}


# ---------------------------------------------------------------------------
# Unit tests: normalise_ico
# ---------------------------------------------------------------------------


class TestNormaliseIco:
    def test_zero_pads_short(self) -> None:
        assert normalise_ico("123456") == "00123456"

    def test_eight_digit_unchanged(self) -> None:
        assert normalise_ico("31320155") == "31320155"

    def test_strips_whitespace(self) -> None:
        assert normalise_ico("  31320155  ") == "31320155"

    def test_int_input(self) -> None:
        assert normalise_ico(31320155) == "31320155"


# ---------------------------------------------------------------------------
# Unit tests: _extract_ico
# ---------------------------------------------------------------------------


class TestExtractIco:
    def test_extracts_ico_value(self) -> None:
        assert _extract_ico(RAW_ACTIVE_AS["identifiers"]) == "31320155"

    def test_zero_pads_result(self) -> None:
        idents = [{"type": {"value": "IČO"}, "value": "123456"}]
        assert _extract_ico(idents) == "00123456"

    def test_neuvedene_returns_none(self) -> None:
        assert _extract_ico(RAW_NO_ICO["identifiers"]) is None

    def test_empty_list_returns_none(self) -> None:
        assert _extract_ico([]) is None


# ---------------------------------------------------------------------------
# Unit tests: _extract_current
# ---------------------------------------------------------------------------


class TestExtractCurrent:
    def test_returns_entry_without_valid_to(self) -> None:
        result = _extract_current(RAW_ACTIVE_AS["fullNames"])
        assert result == "SLOVENSKÁ SPORITEĽŇA, a.s."

    def test_prefers_later_valid_from_when_multiple_active(self) -> None:
        names = [
            {"validFrom": "2000-01-01", "validTo": None, "value": {"value": "Newer Name"}},
            {"validFrom": "1990-01-01", "validTo": None, "value": {"value": "Older Name"}},
        ]
        assert _extract_current(names) == "Newer Name"

    def test_falls_back_to_most_recent_when_all_have_valid_to(self) -> None:
        names = [
            {"validFrom": "2010-01-01", "validTo": "2015-01-01", "value": {"value": "Last Name"}},
            {"validFrom": "2000-01-01", "validTo": "2009-12-31", "value": {"value": "Earlier Name"}},
        ]
        assert _extract_current(names) == "Last Name"

    def test_empty_list_returns_none(self) -> None:
        assert _extract_current([]) is None


# ---------------------------------------------------------------------------
# Unit tests: _extract_current_address
# ---------------------------------------------------------------------------


class TestExtractCurrentAddress:
    def test_builds_address_string(self) -> None:
        result = _extract_current_address(RAW_ACTIVE_AS["addresses"])
        assert result is not None
        assert "Tomášikova" in result
        assert "48" in result
        assert "Bratislava" in result

    def test_empty_list_returns_none(self) -> None:
        assert _extract_current_address([]) is None


# ---------------------------------------------------------------------------
# Unit tests: _entity_link
# ---------------------------------------------------------------------------


class TestEntityLink:
    def test_returns_portal_url(self) -> None:
        link = _entity_link("31320155")
        assert "31320155" in link
        assert link.startswith("http")


# ---------------------------------------------------------------------------
# Unit tests: SK_RPO_RA_CODE
# ---------------------------------------------------------------------------


def test_ra_code() -> None:
    assert SK_RPO_RA_CODE == "RA000526"


# ---------------------------------------------------------------------------
# Adapter: info property
# ---------------------------------------------------------------------------


class TestAdapterInfo:
    def test_id(self) -> None:
        adapter = RpoSlovakiaAdapter()
        assert adapter.info.id == "rpo_slovakia"

    def test_supports_entity_only(self) -> None:
        from opencheck.sources.base import SearchKind
        adapter = RpoSlovakiaAdapter()
        assert SearchKind.ENTITY in adapter.info.supports
        assert SearchKind.PERSON not in adapter.info.supports

    def test_homepage(self) -> None:
        adapter = RpoSlovakiaAdapter()
        assert adapter.info.homepage.startswith("http")

    def test_license(self) -> None:
        adapter = RpoSlovakiaAdapter()
        assert "CC" in adapter.info.license.upper()

    def test_attribution(self) -> None:
        adapter = RpoSlovakiaAdapter()
        assert adapter.info.attribution

    def test_no_api_key_required(self) -> None:
        adapter = RpoSlovakiaAdapter()
        assert adapter.info.requires_api_key is False


# ---------------------------------------------------------------------------
# Adapter: search() — stub path
# ---------------------------------------------------------------------------


class TestSearch:
    def test_stub_search_returns_hit(self) -> None:
        adapter = RpoSlovakiaAdapter()
        hits = adapter._stub_search("Test Company")
        assert len(hits) == 1
        assert "Test Company" in hits[0].name
        assert hits[0].is_stub is True
        assert hits[0].source_id == "rpo_slovakia"

    @pytest.mark.asyncio
    async def test_live_search_calls_api(self) -> None:
        from opencheck.sources.base import SearchKind
        adapter = RpoSlovakiaAdapter()

        with (
            patch("opencheck.sources.rpo_slovakia.get_settings") as mock_settings,
            patch.object(adapter, "_get_list", new=AsyncMock(return_value=[RAW_ACTIVE_AS])),
        ):
            mock_settings.return_value.allow_live = True
            hits = await adapter.search("SLOVENSKÁ SPORITEĽŇA", SearchKind.ENTITY)

        assert len(hits) == 1
        assert hits[0].source_id == "rpo_slovakia"
        assert hits[0].hit_id == "31320155"
        assert "SLOVENSKÁ" in hits[0].name

    @pytest.mark.asyncio
    async def test_person_search_returns_empty(self) -> None:
        from opencheck.sources.base import SearchKind
        adapter = RpoSlovakiaAdapter()
        hits = await adapter.search("Alice Example", SearchKind.PERSON)
        assert hits == []


# ---------------------------------------------------------------------------
# Adapter: _entity_hit()
# ---------------------------------------------------------------------------


class TestEntityHit:
    def test_active_entity(self) -> None:
        adapter = RpoSlovakiaAdapter()
        hit = adapter._entity_hit(RAW_ACTIVE_AS)
        assert hit is not None
        assert hit.source_id == "rpo_slovakia"
        assert hit.hit_id == "31320155"
        assert "SLOVENSKÁ" in hit.name
        assert "active" in hit.summary
        assert hit.identifiers["sk_ico"] == "31320155"
        assert hit.is_stub is False

    def test_dissolved_entity_summary(self) -> None:
        adapter = RpoSlovakiaAdapter()
        hit = adapter._entity_hit(RAW_DISSOLVED_SRO)
        assert hit is not None
        assert "dissolved" in hit.summary

    def test_no_ico_returns_none(self) -> None:
        adapter = RpoSlovakiaAdapter()
        result = adapter._entity_hit(RAW_NO_ICO)
        assert result is None


# ---------------------------------------------------------------------------
# Adapter: fetch() — build_bundle path
# ---------------------------------------------------------------------------


class TestFetch:
    @pytest.mark.asyncio
    async def test_fetch_by_ico(self) -> None:
        adapter = RpoSlovakiaAdapter()
        with (
            patch("opencheck.sources.rpo_slovakia.get_settings") as mock_settings,
            patch.object(adapter, "_get_list", new=AsyncMock(return_value=[RAW_ACTIVE_AS])),
        ):
            mock_settings.return_value.allow_live = True
            bundle = await adapter.fetch("31320155")

        assert bundle["is_stub"] is False
        assert bundle["source_id"] == "rpo_slovakia"
        assert bundle["sk_ico"] == "31320155"
        assert "SLOVENSKÁ" in bundle["name"]
        assert bundle["status"] == "active"
        assert bundle["establishment"] == "1990-01-01"
        assert bundle["termination"] is None

    @pytest.mark.asyncio
    async def test_fetch_zero_pads_ico(self) -> None:
        adapter = RpoSlovakiaAdapter()
        short_ico_raw = dict(RAW_ACTIVE_AS, identifiers=[
            {"type": {"id": "ICO", "value": "IČO"}, "value": "1234567"}
        ])
        with (
            patch("opencheck.sources.rpo_slovakia.get_settings") as mock_settings,
            patch.object(adapter, "_get_list", new=AsyncMock(return_value=[short_ico_raw])),
        ):
            mock_settings.return_value.allow_live = True
            bundle = await adapter.fetch("01234567")

        assert bundle["hit_id"] == "01234567"

    @pytest.mark.asyncio
    async def test_fetch_stub_when_live_disabled(self) -> None:
        adapter = RpoSlovakiaAdapter()
        adapter._cache = MagicMock()
        adapter._cache.has.return_value = False
        with patch("opencheck.sources.rpo_slovakia.get_settings") as mock_settings:
            mock_settings.return_value.allow_live = False
            bundle = await adapter.fetch("31320155")
        assert bundle["is_stub"] is True
        assert bundle["source_id"] == "rpo_slovakia"

    @pytest.mark.asyncio
    async def test_fetch_dissolved_status(self) -> None:
        adapter = RpoSlovakiaAdapter()
        with (
            patch("opencheck.sources.rpo_slovakia.get_settings") as mock_settings,
            patch.object(adapter, "_get_list", new=AsyncMock(return_value=[RAW_DISSOLVED_SRO])),
        ):
            mock_settings.return_value.allow_live = True
            bundle = await adapter.fetch("12345678")

        assert bundle["status"] == "dissolved"
        assert bundle["termination"] == "2022-03-01"


# ---------------------------------------------------------------------------
# BODS mapper: map_rpo_slovakia
# ---------------------------------------------------------------------------


class TestMapRpoSlovakia:
    def _make_bundle(self, raw: dict[str, Any] = RAW_ACTIVE_AS) -> dict[str, Any]:
        adapter = RpoSlovakiaAdapter()
        ico = _extract_ico(raw.get("identifiers") or []) or "00000000"
        return adapter._build_bundle(raw, ico)

    def test_yields_entity_statement(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        assert len(stmts) == 1
        assert stmts[0]["recordType"] == "entity"

    def test_entity_name(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        rd = stmts[0]["recordDetails"]
        assert "SLOVENSKÁ" in rd["name"]

    def test_jurisdiction_sk(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        rd = stmts[0]["recordDetails"]
        jur = rd.get("jurisdiction") or {}
        assert jur.get("code") == "SK"

    def test_sk_rpo_identifier(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        rd = stmts[0]["recordDetails"]
        idents = rd.get("identifiers") or []
        schemes = [i["scheme"] for i in idents]
        assert "SK-RPO" in schemes

    def test_or_identifier_when_reg_number_present(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        rd = stmts[0]["recordDetails"]
        idents = rd.get("identifiers") or []
        schemes = [i["scheme"] for i in idents]
        assert "SK-OR" in schemes

    def test_founding_date(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        rd = stmts[0]["recordDetails"]
        assert rd.get("foundingDate") == "1990-01-01"

    def test_stub_yields_nothing(self) -> None:
        assert list(map_rpo_slovakia({"is_stub": True})) == []

    def test_empty_bundle_yields_nothing(self) -> None:
        assert list(map_rpo_slovakia({})) == []

    def test_dissolved_entity_mapped(self) -> None:
        adapter = RpoSlovakiaAdapter()
        bundle = adapter._build_bundle(RAW_DISSOLVED_SRO, "12345678")
        stmts = list(map_rpo_slovakia(bundle))
        assert len(stmts) == 1
        rd = stmts[0]["recordDetails"]
        assert "EXAMPLE" in rd["name"]

    def test_source_url_present(self) -> None:
        bundle = self._make_bundle()
        stmts = list(map_rpo_slovakia(bundle))
        # Statement should carry some form of source reference.
        stmt = stmts[0]
        stmt_str = str(stmt)
        assert "rpo" in stmt_str.lower() or "statistics.sk" in stmt_str.lower()
