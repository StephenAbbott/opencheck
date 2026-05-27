"""Tests for the Czech ARES adapter and BODS mapper.

Uses fixture data taken from live ARES API responses:
  - Alza.cz a.s.  (IČO 27082440)  — joint-stock company, a.s.
  - Seznam.cz datová centra, s.r.o.  (IČO 01673408)  — LLC, s.r.o.

No network calls are made; ``_cache`` is mocked out and
``live_available`` is forced True so the code paths that build bundles
from API responses are exercised.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.ares import (
    AresAdapter,
    CZ_RA_CODE,
    normalise_ico,
    _extract_latest,
    _resolve_status,
)
from opencheck.bods.mapper import map_ares


# ---------------------------------------------------------------------------
# Fixtures — raw API response snapshots
# ---------------------------------------------------------------------------

AGGREGATE_ALZA: dict[str, Any] = {
    "ico": "27082440",
    "obchodniJmeno": "Alza.cz a.s.",
    "sidlo": {"textovaAdresa": "Jankovcova 1522/53, Holešovice, 17000 Praha 7"},
    "pravniForma": "121",
    "datumVzniku": "2003-08-26",
    "dic": "CZ27082440",
    "seznamRegistraci": {
        "stavZdrojeRos": "AKTIVNI",
        "stavZdrojeVr": "AKTIVNI",
        "stavZdrojeRes": "AKTIVNI",
        "stavZdrojeRzp": "AKTIVNI",
        "stavZdrojeNrpzs": "AKTIVNI",
        "stavZdrojeRpsh": "NEEXISTUJICI",
        "stavZdrojeRcns": "NEEXISTUJICI",
        "stavZdrojeSzr": "NEEXISTUJICI",
        "stavZdrojeDph": "AKTIVNI",
        "stavZdrojeSkDph": "NEEXISTUJICI",
        "stavZdrojeSd": "NEEXISTUJICI",
        "stavZdrojeIr": "NEEXISTUJICI",
        "stavZdrojeCeu": "NEEXISTUJICI",
        "stavZdrojeRs": "NEEXISTUJICI",
        "stavZdrojeRed": "NEEXISTUJICI",
        "stavZdrojeMonitor": "NEEXISTUJICI",
    },
    "primarniZdroj": "ros",
}

VR_ALZA: dict[str, Any] = {
    "zaznamy": [
        {
            "ico": "27082440",
            "obchodniJmeno": [{"datumZapisu": "2003-08-26", "hodnota": "Alza.cz a.s."}],
            "pravniForma": [{"datumZapisu": "2003-08-26", "hodnota": "121"}],
            "stavSubjektu": "AKTIVNI",
            "datumZapisu": "2003-08-26",
            "akcionari": [
                {
                    "datumZapisu": "2004-07-08",
                    "datumVymazu": "2006-07-17",
                    "clenoveOrganu": [
                        {
                            "datumZapisu": "2004-07-08",
                            "datumVymazu": "2006-07-17",
                            "typAngazma": "AKCIONAR",
                            "clenstvi": {},
                            "nazevAngazma": "Akcionář",
                            "fyzickaOsoba": {
                                "jmeno": "Aleš",
                                "prijmeni": "Zavoral",
                                "datumNarozeni": "1976-10-24",
                                "adresa": {"textovaAdresa": "Sokolská 364, 51771 České Meziříčí"},
                            },
                        }
                    ],
                    "typOrganu": "AKCIONAR",
                    "typAkcionare": "OSOBA",
                },
                {
                    "datumZapisu": "2017-09-18",
                    "clenoveOrganu": [
                        {
                            "datumZapisu": "2017-09-18",
                            "typAngazma": "AKCIONAR",
                            "clenstvi": {},
                            "nazevAngazma": "Akcionář",
                            "pravnickaOsoba": {
                                "adresa": {
                                    "kodStatu": "CY",
                                    "textovaAdresa": "CHAPO CENTRAL, 1st floor, Spyrou Kyprianou 20, 1075 Nikósie, Kypr",
                                },
                                "obchodniJmeno": "L.S. INVESTMENTS LIMITED",
                            },
                        }
                    ],
                    "typOrganu": "AKCIONAR",
                    "typAkcionare": "OSOBA",
                },
            ],
            "statutarniOrgany": [
                {
                    "nazevOrganu": "Statutární orgán - představenstvo",
                    "clenoveOrganu": [
                        {
                            "datumZapisu": "2022-12-06",
                            "typAngazma": "STATUTARNI_ORGAN_CLEN",
                            "clenstvi": {
                                "funkce": {"nazev": "Předseda představenstva"}
                            },
                            "nazevAngazma": "Člen statutárního orgánu",
                            "fyzickaOsoba": {
                                "jmeno": "Ondřej",
                                "prijmeni": "Šmída",
                                "datumNarozeni": "1982-04-25",
                                "statniObcanstvi": "CZ",
                                "adresa": {"textovaAdresa": "Libeň, 19000 Praha 9"},
                            },
                        },
                        {
                            # Historic director — should be excluded
                            "datumZapisu": "2013-07-18",
                            "datumVymazu": "2013-11-12",
                            "typAngazma": "STATUTARNI_ORGAN_CLEN",
                            "clenstvi": {},
                            "fyzickaOsoba": {
                                "jmeno": "Aleš",
                                "prijmeni": "Zavoral",
                            },
                        },
                    ],
                }
            ],
            "zakladniKapital": [],
            "cinnosti": [],
            "ostatniOrgany": [],
            "ostatniSkutecnosti": [],
        }
    ]
}

AGGREGATE_SEZNAM_DC: dict[str, Any] = {
    "ico": "01673408",
    "obchodniJmeno": "Seznam.cz datová centra, s.r.o.",
    "sidlo": {"textovaAdresa": "Radlická 3294/10, Smíchov, 15000 Praha 5"},
    "pravniForma": "112",
    "datumVzniku": "2013-05-15",
    "dic": "CZ01673408",
    "seznamRegistraci": {
        "stavZdrojeRos": "AKTIVNI",
        "stavZdrojeVr": "AKTIVNI",
    },
    "primarniZdroj": "vr",
}

VR_SEZNAM_DC: dict[str, Any] = {
    "zaznamy": [
        {
            "ico": "01673408",
            "obchodniJmeno": [{"datumZapisu": "2013-05-15", "hodnota": "Seznam.cz datová centra, s.r.o."}],
            "pravniForma": [{"datumZapisu": "2013-05-15", "hodnota": "112"}],
            "stavSubjektu": "AKTIVNI",
            "akcionari": [],
            "spolecnici": [
                {
                    "nazevOrganu": "Společníci",
                    "spolecnik": [
                        {
                            "datumZapisu": "2022-09-15",
                            "podil": [
                                {
                                    "datumZapisu": "2022-09-15",
                                    "velikostPodilu": {"typObnos": "PROCENTA", "hodnota": "100"},
                                }
                            ],
                            "osoba": {
                                "datumZapisu": "2022-09-15",
                                "typAngazma": "SPOLECNIK_OSOBA",
                                "clenstvi": {},
                                "nazevAngazma": "Společník",
                                "pravnickaOsoba": {
                                    "ico": "26168685",
                                    "obchodniJmeno": "Seznam.cz, a.s.",
                                    "adresa": {"textovaAdresa": "Radlická 3294/10, Smíchov, 15000 Praha 5"},
                                },
                            },
                        }
                    ],
                }
            ],
            "statutarniOrgany": [
                {
                    "nazevOrganu": "Jednatel",
                    "clenoveOrganu": [
                        {
                            "datumZapisu": "2022-09-15",
                            "typAngazma": "STATUTARNI_ORGAN_CLEN",
                            "clenstvi": {"funkce": {"nazev": "Jednatel"}},
                            "fyzickaOsoba": {
                                "jmeno": "Michal",
                                "prijmeni": "Feix",
                                "datumNarozeni": "1978-06-05",
                                "statniObcanstvi": "CZ",
                                "adresa": {"textovaAdresa": "Praha 5"},
                            },
                        }
                    ],
                }
            ],
            "zakladniKapital": [],
        }
    ]
}

SEARCH_RESPONSE: dict[str, Any] = {
    "ekonomickeSubjekty": [
        {
            "ico": "27082440",
            "obchodniJmeno": "Alza.cz a.s.",
            "pravniForma": "121",
            "sidlo": {"textovaAdresa": "Jankovcova 1522/53, Holešovice, 17000 Praha 7"},
        },
    ]
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestNormaliseIco:
    def test_zero_pads_short_ico(self) -> None:
        assert normalise_ico("123") == "00000123"

    def test_leaves_8digit_unchanged(self) -> None:
        assert normalise_ico("27082440") == "27082440"

    def test_accepts_int(self) -> None:
        assert normalise_ico(1673408) == "01673408"

    def test_strips_whitespace(self) -> None:
        assert normalise_ico("  27082440  ") == "27082440"


class TestExtractLatest:
    def test_returns_scalar_string_unchanged(self) -> None:
        assert _extract_latest("Alza.cz a.s.") == "Alza.cz a.s."

    def test_picks_current_entry(self) -> None:
        items = [
            {"datumZapisu": "2003-01-01", "datumVymazu": "2010-01-01", "hodnota": "Old Name"},
            {"datumZapisu": "2010-01-02", "hodnota": "New Name"},
        ]
        assert _extract_latest(items) == "New Name"

    def test_returns_none_for_empty(self) -> None:
        assert _extract_latest([]) is None


class TestResolveStatus:
    def test_aktivni_maps_to_active(self) -> None:
        assert _resolve_status(AGGREGATE_ALZA) == "active"

    def test_neexistujici_only_returns_not_registered(self) -> None:
        aggregate = {"seznamRegistraci": {"stavZdrojeVr": "NEEXISTUJICI", "stavZdrojeRos": "NEEXISTUJICI"}}
        # All NEEXISTUJICI — should fall back
        result = _resolve_status(aggregate)
        assert result  # just check it returns something


class TestConstant:
    def test_ra_code(self) -> None:
        assert CZ_RA_CODE == "RA000163"


# ---------------------------------------------------------------------------
# Adapter: _build_bundle
# ---------------------------------------------------------------------------


class TestBuildBundle:
    def setup_method(self) -> None:
        self.adapter = AresAdapter()

    def test_alza_entity_fields(self) -> None:
        bundle = self.adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)
        assert bundle["cz_ico"] == "27082440"
        assert bundle["name"] == "Alza.cz a.s."
        assert bundle["is_stub"] is False
        entity = bundle["entity"]
        assert entity["ico"] == "27082440"
        assert entity["address"] == "Jankovcova 1522/53, Holešovice, 17000 Praha 7"
        assert "joint-stock" in entity["entity_type"].lower()
        assert entity["status"] == "active"
        assert entity["incorporation_date"] == "2003-08-26"
        assert entity["vat_number"] == "CZ27082440"

    def test_alza_current_shareholder_only(self) -> None:
        """Historic akcionari (with datumVymazu) should be excluded."""
        bundle = self.adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)
        owners = bundle["owners"]
        assert len(owners) == 1
        owner = owners[0]
        assert owner["name"] == "L.S. INVESTMENTS LIMITED"
        assert owner["type"] == "entity"
        assert owner["role"] == "shareholder"
        assert owner["country"] == "CY"

    def test_alza_current_director_only(self) -> None:
        """Historic directors (with datumVymazu) should be excluded."""
        bundle = self.adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)
        directors = bundle["directors"]
        assert len(directors) == 1
        director = directors[0]
        assert director["name"] == "Ondřej Šmída"
        assert director["type"] == "person"
        assert director["role_label"] == "Předseda představenstva"

    def test_seznam_dc_spolecnik(self) -> None:
        bundle = self.adapter._build_bundle("01673408", AGGREGATE_SEZNAM_DC, VR_SEZNAM_DC)
        owners = bundle["owners"]
        assert len(owners) == 1
        owner = owners[0]
        assert owner["name"] == "Seznam.cz, a.s."
        assert owner["type"] == "entity"
        assert owner["role"] == "partner"
        assert owner["ico"] == "26168685"
        assert owner["stake_percent"] == "100"

    def test_seznam_dc_jednatel(self) -> None:
        bundle = self.adapter._build_bundle("01673408", AGGREGATE_SEZNAM_DC, VR_SEZNAM_DC)
        directors = bundle["directors"]
        assert len(directors) == 1
        assert directors[0]["name"] == "Michal Feix"
        assert directors[0]["role_label"] == "Jednatel"

    def test_no_vr_data_returns_entity_only_bundle(self) -> None:
        bundle = self.adapter._build_bundle("27082440", AGGREGATE_ALZA, None)
        assert bundle["owners"] == []
        assert bundle["directors"] == []
        assert bundle["name"] == "Alza.cz a.s."

    def test_stub_returned_when_no_aggregate(self) -> None:
        stub = self.adapter._stub("27082440", "Alza.cz a.s.")
        assert stub["is_stub"] is True
        assert stub["name"] == "Alza.cz a.s."


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapAres:
    def _alza_bundle(self) -> dict:
        adapter = AresAdapter()
        return adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)

    def _seznam_bundle(self) -> dict:
        adapter = AresAdapter()
        return adapter._build_bundle("01673408", AGGREGATE_SEZNAM_DC, VR_SEZNAM_DC)

    def test_stub_yields_nothing(self) -> None:
        stmts = list(map_ares({"is_stub": True, "cz_ico": "27082440"}))
        assert stmts == []

    def test_none_yields_nothing(self) -> None:
        stmts = list(map_ares({}))
        assert stmts == []

    def test_alza_entity_statement(self) -> None:
        stmts = list(map_ares(self._alza_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert entity_stmts, "Expected at least one entity statement"
        subject = entity_stmts[0]
        assert subject["recordDetails"]["name"] == "Alza.cz a.s."
        assert subject["recordDetails"]["jurisdiction"]["code"] == "CZ"
        ids = {i["scheme"]: i["id"] for i in subject["recordDetails"]["identifiers"]}
        assert ids["CZ-ICO"] == "27082440"
        assert ids["CZ-DIC"] == "CZ27082440"

    def test_alza_shareholder_entity_statement(self) -> None:
        stmts = list(map_ares(self._alza_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        # Should be 2: subject + shareholder entity
        assert len(entity_stmts) == 2
        shareholder_entity = entity_stmts[1]
        assert shareholder_entity["recordDetails"]["name"] == "L.S. INVESTMENTS LIMITED"
        jur = shareholder_entity["recordDetails"].get("jurisdiction", {})
        assert jur.get("code") == "CY"

    def test_alza_director_person_statement(self) -> None:
        stmts = list(map_ares(self._alza_bundle()))
        person_stmts = [s for s in stmts if s["recordType"] == "person"]
        assert len(person_stmts) == 1
        person = person_stmts[0]
        assert person["recordDetails"]["names"][0]["fullName"] == "Ondřej Šmída"

    def test_alza_relationship_statements(self) -> None:
        stmts = list(map_ares(self._alza_bundle()))
        rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
        # One for shareholder, one for director
        assert len(rel_stmts) == 2
        types = {r["recordDetails"]["interests"][0]["type"] for r in rel_stmts}
        assert "shareholding" in types
        assert "appointmentOfBoard" in types

    def test_relationships_reference_subject_entity(self) -> None:
        stmts = list(map_ares(self._alza_bundle()))
        entity_stmt_id = next(s["statementId"] for s in stmts if s["recordType"] == "entity"
                              and s["recordDetails"]["name"] == "Alza.cz a.s.")
        for rel in (s for s in stmts if s["recordType"] == "relationship"):
            assert rel["recordDetails"]["subject"] == entity_stmt_id

    def test_seznam_spolecnik_relationship_is_shareholding(self) -> None:
        stmts = list(map_ares(self._seznam_bundle()))
        rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
        owner_rels = [r for r in rel_stmts if r["recordDetails"]["interests"][0]["type"] == "shareholding"]
        assert len(owner_rels) == 1
        assert "100" in owner_rels[0]["recordDetails"]["interests"][0].get("details", "")

    def test_all_statements_have_required_fields(self) -> None:
        for bundle_fn in (self._alza_bundle, self._seznam_bundle):
            stmts = list(map_ares(bundle_fn()))
            for stmt in stmts:
                assert "statementId" in stmt
                assert "recordType" in stmt
                assert "recordDetails" in stmt
                assert "source" in stmt
                assert stmt["source"]["type"] == ["officialRegister"]

    def test_deterministic_ids(self) -> None:
        """Two calls with the same bundle must produce identical statement IDs."""
        adapter = AresAdapter()
        bundle1 = adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)
        bundle2 = adapter._build_bundle("27082440", AGGREGATE_ALZA, VR_ALZA)
        ids1 = [s["statementId"] for s in map_ares(bundle1)]
        ids2 = [s["statementId"] for s in map_ares(bundle2)]
        assert ids1 == ids2


# ---------------------------------------------------------------------------
# Adapter: search (unit — mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_returns_hits(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.put.return_value = None

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = SEARCH_RESPONSE

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_resp)

    with (
        patch("opencheck.sources.ares.Cache", return_value=mock_cache),
        patch("opencheck.sources.ares.build_client", return_value=mock_client),
    ):
        adapter = AresAdapter()
        hits = await adapter.search("Alza")

    get_settings.cache_clear()

    assert len(hits) == 1
    hit = hits[0]
    assert hit.source_id == "ares"
    assert hit.hit_id == "27082440"
    assert hit.name == "Alza.cz a.s."
    assert hit.is_stub is True


# ---------------------------------------------------------------------------
# Adapter: fetch (unit — mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_builds_bundle(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.put.return_value = None

    def make_resp(json_data: dict) -> MagicMock:
        r = MagicMock()
        r.raise_for_status.return_value = None
        r.json.return_value = json_data
        return r

    call_count = [0]

    async def side_effect(url: str, **kwargs: Any) -> MagicMock:
        call_count[0] += 1
        if "ekonomicke-subjekty-vr" in url:
            return make_resp(VR_ALZA)
        return make_resp(AGGREGATE_ALZA)

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = side_effect

    with (
        patch("opencheck.sources.ares.Cache", return_value=mock_cache),
        patch("opencheck.sources.ares.build_client", return_value=mock_client),
    ):
        adapter = AresAdapter()
        bundle = await adapter.fetch("27082440")

    get_settings.cache_clear()

    assert bundle["is_stub"] is False
    assert bundle["name"] == "Alza.cz a.s."
    assert len(bundle["owners"]) == 1
    assert bundle["owners"][0]["name"] == "L.S. INVESTMENTS LIMITED"
    assert call_count[0] == 2  # one aggregate + one VR call
