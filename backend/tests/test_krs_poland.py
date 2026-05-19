"""Tests for the Polish KRS adapter and BODS mapper.

Uses fixture data representative of live KRS API responses:
  - PKN ORLEN S.A.  (KRS 0000028860)  — joint-stock company (S.A.)
  - Example sp. z o.o.  (KRS 0000000001)  — limited liability company with shareholders

No network calls are made; ``_cache`` is mocked out and
``live_available`` is forced True so the code paths that build bundles
from API responses are exercised.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.krs_poland import (
    KrsPolandAdapter,
    PL_KRS_RA_CODE,
    normalise_krs,
    _normalise_nip,
    _normalise_regon,
    _build_address,
    _parse_date,
    _extract_board_member,
    _extract_shareholder,
    _extract_pkd,
)
from opencheck.bods.mapper import map_krs_poland


# ---------------------------------------------------------------------------
# Fixtures — raw API response snapshots
# ---------------------------------------------------------------------------

# Simplified but representative KRS OdpisAktualny response for a S.A. entity.
# Based on structure from: GET /OdpisAktualny/0000028860?rejestr=P&format=json
RAW_SA: dict[str, Any] = {
    "odpis": {
        "naglowekA": {
            "dataRejestracjiWKRS": "14.06.2001",
            "dataOstatniegoWpisu": "15.03.2024 R.",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "nazwa": "PKN ORLEN SPÓŁKA AKCYJNA",
                    "formaPrawna": "SPÓŁKA AKCYJNA",
                    "identyfikatory": {
                        "nip": "774-00-01-454",
                        "regon": "611251560",
                    },
                },
                "siedzibaIAdres": {
                    "adres": {
                        "ulica": "UL. CHEMIKÓW",
                        "nrDomu": "7",
                        "nrLokalu": "",
                        "kodPocztowy": "09-411",
                        "miejscowosc": "PŁOCK",
                        "kraj": "POLSKA",
                    },
                    "adresPocztyElektronicznej": "info@orlen.pl",
                    "adresStronyInternetowej": "www.orlen.pl",
                },
                "kapital": {
                    "wysokoscKapitaluZakladowego": {
                        "wartosc": "534636326,25",
                        "waluta": "PLN",
                    },
                    "lacznaLiczbaAkcjiUdzialow": "427709020",
                    "wartoscJednejAkcji": {
                        "wartosc": "1,25",
                    },
                },
                "wspolnicySpzoo": [],
            },
            "dzial2": {
                "reprezentacja": {
                    "nazwaOrganu": "Zarząd",
                    "sklad": [
                        {
                            "imiona": {"imie": "I*****", "imieDrugie": ""},
                            "nazwisko": {"nazwiskoICzlon": "O*******", "nazwiskoIICzlon": ""},
                            "funkcjaWOrganie": "PREZES ZARZĄDU",
                            "czyZawieszona": False,
                        }
                    ],
                },
                "organNadzoru": {
                    "nazwaOrganu": "Rada Nadzorcza",
                    "sklad": [],
                },
            },
            "dzial3": {
                "przedmiotDzialalnosci": {
                    "przedmiotPrzewazajacejDzialalnosci": [
                        {
                            "kodDzial": "19",
                            "kodKlasa": "20",
                            "kodPodklasa": "Z",
                            "opis": "Wytwarzanie i przetwarzanie koksu i produktów rafinacji ropy naftowej",
                        }
                    ]
                }
            },
        },
    }
}

# Simplified KRS response for a sp. z o.o. with shareholders.
RAW_SPZOO: dict[str, Any] = {
    "odpis": {
        "naglowekA": {
            "dataRejestracjiWKRS": "05.09.2018",
            "dataOstatniegoWpisu": "10.01.2024",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "nazwa": "EXAMPLE SP. Z O.O.",
                    "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
                    "identyfikatory": {
                        "nip": "5262857292",
                        "regon": "38251234512345",  # 14-char, should be truncated to 9
                    },
                },
                "siedzibaIAdres": {
                    "adres": {
                        "ulica": "UL. PLATERÓWEK",
                        "nrDomu": "3",
                        "nrLokalu": "5",
                        "kodPocztowy": "03-308",
                        "miejscowosc": "WARSZAWA",
                        "kraj": "POLSKA",
                    },
                    "adresPocztyElektronicznej": "",
                    "adresStronyInternetowej": "",
                },
                "kapital": {
                    "wysokoscKapitaluZakladowego": {
                        "wartosc": "5000,00",
                        "waluta": "PLN",
                    },
                    "lacznaLiczbaAkcjiUdzialow": None,
                    "wartoscJednejAkcji": {},
                },
                "wspolnicySpzoo": [
                    {
                        "imiona": {"imie": "P****", "imieDrugie": ""},
                        "nazwisko": {"nazwiskoICzlon": "Ł*******", "nazwiskoIICzlon": ""},
                        "posiadaneUdzialy": "60 UDZIAŁÓW O WARTOŚCI 3000,00 PLN",
                        "czyPosiadaCaloscUdzialow": False,
                    },
                    {
                        "imiona": {"imie": "A***", "imieDrugie": ""},
                        "nazwisko": {"nazwiskoICzlon": "K*****", "nazwiskoIICzlon": ""},
                        "posiadaneUdzialy": "40 UDZIAŁÓW O WARTOŚCI 2000,00 PLN",
                        "czyPosiadaCaloscUdzialow": False,
                    },
                ],
            },
            "dzial2": {
                "reprezentacja": {
                    "nazwaOrganu": "Zarząd",
                    "sklad": [
                        {
                            "imiona": {"imie": "P****", "imieDrugie": ""},
                            "nazwisko": {"nazwiskoICzlon": "Ł*******", "nazwiskoIICzlon": ""},
                            "funkcjaWOrganie": "PREZES ZARZĄDU",
                            "czyZawieszona": False,
                        }
                    ],
                },
                "organNadzoru": {},
            },
            "dzial3": {
                "przedmiotDzialalnosci": {
                    "przedmiotPrzewazajacejDzialalnosci": [
                        {
                            "kodDzial": "73",
                            "kodKlasa": "11",
                            "kodPodklasa": "Z",
                            "opis": "Działalność agencji reklamowych",
                        }
                    ]
                }
            },
        },
    }
}


# Cooperative (spółdzielnia) fixture — ``organNadzoru`` is a **list** of
# organ dicts (cooperative variant) rather than a single dict.  This is the
# shape that triggered AttributeError: 'list' object has no attribute 'get'.
# KRS number 0000119004 → SPÓŁDZIELNIA PRODUCENTÓW DROBIU "EKO - GRIL"
RAW_SPOLDZ: dict[str, Any] = {
    "odpis": {
        "naglowekA": {
            "dataRejestracjiWKRS": "20.08.2002",
            "dataOstatniegoWpisu": "12.11.2023",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "nazwa": 'SPÓŁDZIELNIA PRODUCENTÓW DROBIU "EKO - GRIL"',
                    "formaPrawna": "SPÓŁDZIELNIA",
                    "identyfikatory": {
                        "nip": "7961234567",
                        "regon": "123456789",
                    },
                },
                "siedzibaIAdres": {
                    "adres": {
                        "ulica": "UL. ROLNA",
                        "nrDomu": "12",
                        "nrLokalu": "",
                        "kodPocztowy": "08-400",
                        "miejscowosc": "GARWOLIN",
                        "kraj": "POLSKA",
                    },
                    "adresPocztyElektronicznej": "",
                    "adresStronyInternetowej": "",
                },
                "kapital": {},
                "wspolnicySpzoo": [],
            },
            "dzial2": {
                # Board of management — single dict form (normal)
                "reprezentacja": {
                    "nazwaOrganu": "Zarząd",
                    "sklad": [
                        {
                            "imiona": {"imie": "J*****", "imieDrugie": ""},
                            "nazwisko": {"nazwiskoICzlon": "K*****", "nazwiskoIICzlon": ""},
                            "funkcjaWOrganie": "PREZES ZARZĄDU",
                            "czyZawieszona": False,
                        }
                    ],
                },
                # Supervisory organs — **list** form (cooperative variant).
                # Uses key "nazwa" (not "nazwaOrganu") in each element.
                "organNadzoru": [
                    {
                        "nazwa": "RADA NADZORCZA",
                        "sklad": [
                            {
                                "imiona": {"imie": "A***", "imieDrugie": ""},
                                "nazwisko": {"nazwiskoICzlon": "B*****", "nazwiskoIICzlon": ""},
                                "funkcjaWOrganie": "PRZEWODNICZĄCY RADY",
                                "czyZawieszona": False,
                            },
                            {
                                "imiona": {"imie": "C***", "imieDrugie": ""},
                                "nazwisko": {"nazwiskoICzlon": "D*****", "nazwiskoIICzlon": ""},
                                "funkcjaWOrganie": "CZŁONEK RADY",
                                "czyZawieszona": False,
                            },
                        ],
                    }
                ],
            },
            "dzial3": {
                "przedmiotDzialalnosci": {
                    "przedmiotPrzewazajacejDzialalnosci": [
                        {
                            "kodDzial": "01",
                            "kodKlasa": "47",
                            "kodPodklasa": "Z",
                            "opis": "Chów i hodowla drobiu",
                        }
                    ]
                }
            },
        },
    }
}


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestNormaliseKrs:
    def test_zero_pads_short_number(self) -> None:
        assert normalise_krs("28860") == "0000028860"

    def test_leaves_10digit_unchanged(self) -> None:
        assert normalise_krs("0000028860") == "0000028860"

    def test_accepts_int(self) -> None:
        assert normalise_krs(28860) == "0000028860"

    def test_strips_whitespace(self) -> None:
        assert normalise_krs("  0000028860  ") == "0000028860"


class TestNormaliseNip:
    def test_strips_hyphens(self) -> None:
        assert _normalise_nip("774-00-01-454") == "7740001454"

    def test_passthrough_digits_only(self) -> None:
        assert _normalise_nip("5262857292") == "5262857292"

    def test_strips_whitespace(self) -> None:
        assert _normalise_nip("  526-285-72-92  ") == "5262857292"


class TestNormaliseRegon:
    def test_truncates_14_char_to_9(self) -> None:
        assert _normalise_regon("38251234512345") == "382512345"

    def test_leaves_9_char_unchanged(self) -> None:
        assert _normalise_regon("611251560") == "611251560"

    def test_shorter_than_9_returned_as_is(self) -> None:
        assert _normalise_regon("12345") == "12345"


class TestBuildAddress:
    def test_full_address(self) -> None:
        adres = {
            "ulica": "UL. CHEMIKÓW",
            "nrDomu": "7",
            "nrLokalu": "",
            "kodPocztowy": "09-411",
            "miejscowosc": "PŁOCK",
            "kraj": "POLSKA",
        }
        result = _build_address(adres)
        assert result == "UL. CHEMIKÓW 7, 09-411 PŁOCK"

    def test_with_apartment(self) -> None:
        adres = {
            "ulica": "UL. PLATERÓWEK",
            "nrDomu": "3",
            "nrLokalu": "5",
            "kodPocztowy": "03-308",
            "miejscowosc": "WARSZAWA",
            "kraj": "POLSKA",
        }
        result = _build_address(adres)
        assert result == "UL. PLATERÓWEK 3/5, 03-308 WARSZAWA"

    def test_foreign_country_included(self) -> None:
        adres = {
            "ulica": "MAIN ST",
            "nrDomu": "1",
            "nrLokalu": "",
            "kodPocztowy": "10001",
            "miejscowosc": "NEW YORK",
            "kraj": "STANY ZJEDNOCZONE",
        }
        result = _build_address(adres)
        assert result == "MAIN ST 1, 10001 NEW YORK, Stany Zjednoczone"

    def test_empty_dict_returns_none(self) -> None:
        assert _build_address({}) is None


class TestParseDate:
    def test_standard_format(self) -> None:
        assert _parse_date("14.06.2001") == "2001-06-14"

    def test_with_r_suffix(self) -> None:
        assert _parse_date("15.03.2024 R.") == "2024-03-15"

    def test_none_returns_none(self) -> None:
        assert _parse_date(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_date("") is None

    def test_invalid_format_returns_none(self) -> None:
        assert _parse_date("not-a-date") is None


class TestExtractBoardMember:
    def test_masked_name(self) -> None:
        member = {
            "imiona": {"imie": "I*****", "imieDrugie": ""},
            "nazwisko": {"nazwiskoICzlon": "O*******", "nazwiskoIICzlon": ""},
            "funkcjaWOrganie": "PREZES ZARZĄDU",
            "czyZawieszona": False,
        }
        rec = _extract_board_member(member)
        assert rec is not None
        assert rec["name_masked"] is True
        assert rec["role"] == "PREZES ZARZĄDU"
        assert rec["suspended"] is False

    def test_empty_member_returns_none(self) -> None:
        assert _extract_board_member({}) is None


class TestExtractShareholder:
    def test_masked_shareholder(self) -> None:
        wspolnik = {
            "imiona": {"imie": "P****", "imieDrugie": ""},
            "nazwisko": {"nazwiskoICzlon": "Ł*******", "nazwiskoIICzlon": ""},
            "posiadaneUdzialy": "60 UDZIAŁÓW O WARTOŚCI 3000,00 PLN",
            "czyPosiadaCaloscUdzialow": False,
        }
        rec = _extract_shareholder(wspolnik)
        assert rec is not None
        assert rec["name_masked"] is True
        assert rec["shares_description"] == "60 UDZIAŁÓW O WARTOŚCI 3000,00 PLN"
        assert rec["holds_all_shares"] is False


class TestExtractPkd:
    def test_extracts_primary_code(self) -> None:
        dzial3 = {
            "przedmiotDzialalnosci": {
                "przedmiotPrzewazajacejDzialalnosci": [
                    {
                        "kodDzial": "19",
                        "kodKlasa": "20",
                        "kodPodklasa": "Z",
                        "opis": "Wytwarzanie i przetwarzanie koksu",
                    }
                ]
            }
        }
        result = _extract_pkd(dzial3)
        assert result is not None
        assert result["code"] == "19.20Z"
        assert "koksu" in result["description"]

    def test_empty_returns_none(self) -> None:
        assert _extract_pkd({}) is None

    def test_empty_list_returns_none(self) -> None:
        dzial3 = {"przedmiotDzialalnosci": {"przedmiotPrzewazajacejDzialalnosci": []}}
        assert _extract_pkd(dzial3) is None


class TestConstant:
    def test_ra_code(self) -> None:
        assert PL_KRS_RA_CODE == "RA000484"


# ---------------------------------------------------------------------------
# Adapter: _build_bundle
# ---------------------------------------------------------------------------


class TestBuildBundle:
    def setup_method(self) -> None:
        self.adapter = KrsPolandAdapter()

    def test_sa_entity_fields(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        assert bundle["pl_krs"] == "0000028860"
        assert bundle["name"] == "PKN ORLEN SPÓŁKA AKCYJNA"
        assert bundle["is_stub"] is False
        assert bundle["legal_form"] == "SPÓŁKA AKCYJNA"
        assert bundle["legal_form_label"] == "S.A. (joint-stock company)"
        assert bundle["nip"] == "7740001454"
        assert bundle["regon"] == "611251560"
        assert bundle["address"] == "UL. CHEMIKÓW 7, 09-411 PŁOCK"
        assert bundle["email"] == "info@orlen.pl"
        assert bundle["website"] == "www.orlen.pl"

    def test_sa_registration_date_parsed(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        assert bundle["registration_date"] == "2001-06-14"
        assert bundle["last_change_date"] == "2024-03-15"

    def test_sa_capital(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        capital = bundle["capital"]
        assert capital is not None
        assert capital["currency"] == "PLN"
        assert "534636326" in capital["amount"]
        assert capital["total_shares"] == "427709020"

    def test_sa_director_masked(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        directors = bundle["directors"]
        assert len(directors) == 1
        director = directors[0]
        assert director["name_masked"] is True
        assert director["role"] == "PREZES ZARZĄDU"
        assert director["organ"] == "Zarząd"

    def test_sa_no_shareholders_section(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        assert bundle["shareholders"] == []

    def test_sa_pkd(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        pkd = bundle["pkd"]
        assert pkd is not None
        assert pkd["code"] == "19.20Z"

    def test_sa_link(self) -> None:
        bundle = self.adapter._build_bundle("0000028860", RAW_SA, "P")
        assert "0000028860" in bundle["link"]
        assert "ekrs.ms.gov.pl" in bundle["link"]

    def test_spzoo_shareholders(self) -> None:
        bundle = self.adapter._build_bundle("0000000001", RAW_SPZOO, "P")
        shareholders = bundle["shareholders"]
        assert len(shareholders) == 2
        assert all(s["name_masked"] for s in shareholders)
        assert shareholders[0]["shares_description"] is not None

    def test_spzoo_regon_truncated(self) -> None:
        bundle = self.adapter._build_bundle("0000000001", RAW_SPZOO, "P")
        # 14-char REGON should be truncated to 9 digits
        assert bundle["regon"] == "382512345"

    def test_spzoo_capital_5000(self) -> None:
        bundle = self.adapter._build_bundle("0000000001", RAW_SPZOO, "P")
        assert bundle["capital"]["amount"] == "5000.00"

    def test_stub_returned(self) -> None:
        stub = self.adapter._stub("0000028860", "PKN ORLEN")
        assert stub["is_stub"] is True
        assert stub["name"] == "PKN ORLEN"
        assert stub["pl_krs"] == "0000028860"

    # --- Cooperative (spółdzielnia) fixture: organNadzoru as a list ---

    def test_spoldz_supervisory_list_form(self) -> None:
        """organNadzoru as a list must not crash and must yield supervisory members."""
        bundle = self.adapter._build_bundle("0000119004", RAW_SPOLDZ, "S")
        assert bundle["is_stub"] is False
        supervisory = bundle["supervisory_board"]
        assert len(supervisory) == 2

    def test_spoldz_supervisory_organ_name(self) -> None:
        """Organ name from list-form 'nazwa' key should be preserved."""
        bundle = self.adapter._build_bundle("0000119004", RAW_SPOLDZ, "S")
        organs = {m["organ"] for m in bundle["supervisory_board"]}
        assert "RADA NADZORCZA" in organs

    def test_spoldz_supervisory_roles(self) -> None:
        bundle = self.adapter._build_bundle("0000119004", RAW_SPOLDZ, "S")
        roles = {m["role"] for m in bundle["supervisory_board"]}
        assert "PRZEWODNICZĄCY RADY" in roles
        assert "CZŁONEK RADY" in roles

    def test_spoldz_director_single_dict_form(self) -> None:
        """reprezentacja (single dict form) should still parse correctly."""
        bundle = self.adapter._build_bundle("0000119004", RAW_SPOLDZ, "S")
        directors = bundle["directors"]
        assert len(directors) == 1
        assert directors[0]["role"] == "PREZES ZARZĄDU"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapKrsPoland:
    def _sa_bundle(self) -> dict:
        adapter = KrsPolandAdapter()
        return adapter._build_bundle("0000028860", RAW_SA, "P")

    def _spzoo_bundle(self) -> dict:
        adapter = KrsPolandAdapter()
        return adapter._build_bundle("0000000001", RAW_SPZOO, "P")

    def test_stub_yields_nothing(self) -> None:
        stmts = list(map_krs_poland({"is_stub": True, "pl_krs": "0000028860"}))
        assert stmts == []

    def test_none_yields_nothing(self) -> None:
        stmts = list(map_krs_poland({}))
        assert stmts == []

    def test_sa_entity_statement(self) -> None:
        stmts = list(map_krs_poland(self._sa_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1
        subject = entity_stmts[0]
        assert subject["recordDetails"]["name"] == "PKN ORLEN SPÓŁKA AKCYJNA"
        assert subject["recordDetails"]["incorporatedInJurisdiction"]["code"] == "PL"

    def test_sa_entity_identifiers(self) -> None:
        stmts = list(map_krs_poland(self._sa_bundle()))
        entity_stmt = next(s for s in stmts if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity_stmt["recordDetails"]["identifiers"]}
        assert ids["PL-KRS"] == "0000028860"
        assert ids["PL-NIP"] == "7740001454"
        assert ids["PL-REGON"] == "611251560"

    def test_sa_entity_founding_date(self) -> None:
        stmts = list(map_krs_poland(self._sa_bundle()))
        entity_stmt = next(s for s in stmts if s["recordType"] == "entity")
        assert entity_stmt["recordDetails"].get("foundingDate") == "2001-06-14"

    def test_no_person_statements(self) -> None:
        """KRS masks personal data — no person statements should be emitted."""
        for bundle_fn in (self._sa_bundle, self._spzoo_bundle):
            stmts = list(map_krs_poland(bundle_fn()))
            person_stmts = [s for s in stmts if s["recordType"] == "person"]
            assert person_stmts == [], "KRS adapter must not emit person statements (data masked)"

    def test_no_relationship_statements(self) -> None:
        """Without person statements, no OOC relationship statements should appear."""
        for bundle_fn in (self._sa_bundle, self._spzoo_bundle):
            stmts = list(map_krs_poland(bundle_fn()))
            rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
            assert rel_stmts == []

    def test_entity_statement_has_required_fields(self) -> None:
        stmts = list(map_krs_poland(self._sa_bundle()))
        for stmt in stmts:
            assert "statementId" in stmt
            assert "recordType" in stmt
            assert "recordDetails" in stmt
            assert "source" in stmt
            assert stmt["source"]["type"] == ["officialRegister"]

    def test_entity_type_sa_registered_entity(self) -> None:
        stmts = list(map_krs_poland(self._sa_bundle()))
        entity_stmt = next(s for s in stmts if s["recordType"] == "entity")
        assert entity_stmt["recordDetails"]["entityType"]["type"] == "registeredEntity"

    def test_deterministic_ids(self) -> None:
        """Two calls with the same bundle must produce identical statement IDs."""
        adapter = KrsPolandAdapter()
        bundle1 = adapter._build_bundle("0000028860", RAW_SA, "P")
        bundle2 = adapter._build_bundle("0000028860", RAW_SA, "P")
        ids1 = [s["statementId"] for s in map_krs_poland(bundle1)]
        ids2 = [s["statementId"] for s in map_krs_poland(bundle2)]
        assert ids1 == ids2


# ---------------------------------------------------------------------------
# Adapter: search (always returns [])
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_always_returns_empty() -> None:
    from opencheck.sources.base import SearchKind
    adapter = KrsPolandAdapter()
    hits = await adapter.search("ORLEN", SearchKind.ENTITY)
    assert hits == []


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

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = RAW_SA

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = AsyncMock(return_value=mock_resp)

    with (
        patch("opencheck.sources.krs_poland.Cache", return_value=mock_cache),
        patch("opencheck.sources.krs_poland.build_client", return_value=mock_client),
    ):
        adapter = KrsPolandAdapter()
        bundle = await adapter.fetch("0000028860")

    get_settings.cache_clear()

    assert bundle["is_stub"] is False
    assert bundle["name"] == "PKN ORLEN SPÓŁKA AKCYJNA"
    assert bundle["pl_krs"] == "0000028860"
    assert bundle["nip"] == "7740001454"


@pytest.mark.asyncio
async def test_fetch_returns_stub_when_live_disabled(monkeypatch, tmp_path) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None

    with patch("opencheck.sources.krs_poland.Cache", return_value=mock_cache):
        adapter = KrsPolandAdapter()
        bundle = await adapter.fetch("0000028860", legal_name="PKN ORLEN")

    get_settings.cache_clear()

    assert bundle["is_stub"] is True
    assert bundle["name"] == "PKN ORLEN"


@pytest.mark.asyncio
async def test_fetch_falls_back_to_s_register(monkeypatch, tmp_path) -> None:
    """When rejestr=P returns 404, adapter should fall back to S."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.put.return_value = None

    not_found = MagicMock()
    not_found.status_code = 404
    not_found.raise_for_status.return_value = None

    found = MagicMock()
    found.status_code = 200
    found.raise_for_status.return_value = None
    found.json.return_value = RAW_SA

    call_count = [0]

    async def side_effect(url: str, **kwargs: Any) -> MagicMock:
        call_count[0] += 1
        if "rejestr=P" in url:
            return not_found
        return found

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = side_effect

    with (
        patch("opencheck.sources.krs_poland.Cache", return_value=mock_cache),
        patch("opencheck.sources.krs_poland.build_client", return_value=mock_client),
    ):
        adapter = KrsPolandAdapter()
        bundle = await adapter.fetch("0000028860")

    get_settings.cache_clear()

    assert bundle["is_stub"] is False
    assert bundle["rejestr"] == "S"
    assert call_count[0] >= 2  # at least P (404) + S (200)
