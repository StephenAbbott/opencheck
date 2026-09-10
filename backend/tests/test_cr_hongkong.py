"""Tests for the Hong Kong Companies Registry adapter and mapper.

Fixtures in ``tests/data/cr_hongkong_live.json`` are **real responses**
captured from ``data.cr.gov.hk`` on 2026-09-10, unmodified:

* ``guarantee_dummy_brn`` — a company limited by guarantee with a *dummy* BRN
  (``C1572528``: the old CR No. with a letter prefix) and a Chinese name.
* ``hsbc`` — a public company whose Chinese name is the literal ``"NULL"``.
* ``mtr`` — a public company with both names.
* ``tmf_secretaries_null_chinese`` — the company whose BRN GLEIF files on
  Chery Global Innovations (Hong Kong) Limited's record: the name-mismatch case.
* ``_miss_http_400`` — the register's miss, which is HTTP 400, not 404.

The simplified/traditional name pairs below are the GLEIF legal name and the
register's Chinese name for real companies observed in the same sweep.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck import degradation
from opencheck.bods import liveness
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, map_cr_hongkong
from opencheck.findings import MAX_FINDING_CHARS, finding_cr_hongkong
from opencheck.register_hops import hop_for
from opencheck.sources import REGISTRY
from opencheck.sources.base import SearchKind
from opencheck.sources.cr_hongkong import (
    HK_BRN_SCHEME,
    HK_RA_CODES,
    CrHongKongAdapter,
    clean_field,
    names_agree,
    normalise_hk_brn,
    parse_hk_date,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.cr_hongkong import CrHongKongBundle

_FIXTURES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "data" / "cr_hongkong_live.json").read_text(encoding="utf-8")
)["responses"]


def _company(key: str) -> dict[str, Any]:
    return json.loads(json.dumps(_FIXTURES[key]))


def _bundle(key: str, **overrides: Any) -> dict[str, Any]:
    company = _company(key)
    return {
        "source_id": "cr_hongkong",
        "hk_brn": company["Brn"],
        "company": company,
        "legal_name": "",
        "is_stub": False,
        **overrides,
    }


# ---------------------------------------------------------------------------
# The identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("00173611", "00173611"),
        ("7341475", "07341475"),                  # the register pads; so do we
        ("c1572528", "C1572528"),                 # dummy BRN: letter + old CR No.
        ("7255270400001264", "72552704"),         # full BR certificate number (GLEIF RA000389)
        ("58879114-000-08-25-2", "58879114"),     # hyphenated form (GLEIF RA000389)
        (" 30958055 ", "30958055"),
    ],
)
def test_normalise_hk_brn(raw: str, expected: str) -> None:
    assert normalise_hk_brn(raw) == expected


@pytest.mark.parametrize("bad", ["", "   ", "BXT996", "SO000054", "ABC12345", "HK-BRN"])
def test_normalise_hk_brn_rejects_what_is_not_a_brn(bad: str) -> None:
    """SFC fund codes (RA000390) and charity numbers are not BRNs — skip, never guess."""
    with pytest.raises(ValueError):
        normalise_hk_brn(bad)


def test_ra_codes_and_deriver() -> None:
    assert HK_RA_CODES == frozenset({"RA000388", "RA000389"})
    deriver = CrHongKongAdapter.lookup_derivers[0]
    assert deriver.ra_codes == HK_RA_CODES
    assert deriver.derived_key == "hk_brn"
    assert CrHongKongAdapter.lookup_pass_legal_name is True


def test_both_ra_codes_map_to_the_hk_brn_scheme() -> None:
    for code in HK_RA_CODES:
        assert _GLEIF_RA_TO_ORG_ID[code][0] == HK_BRN_SCHEME == "HK-BRN"


def test_hk_brn_is_a_register_hop() -> None:
    """FullCheck can expand a Hong Kong node that carries a BRN and no LEI."""
    hop = hop_for("HK-BRN")
    assert hop is not None and hop.source_id == "cr_hongkong"
    assert hop.normalise("7341475") == "07341475"
    assert hop.pass_legal_name is True


def test_info() -> None:
    info = REGISTRY["cr_hongkong"].info
    assert info.requires_api_key is False
    assert info.is_national_register is True
    assert info.country == "HK"
    assert info.supports == [SearchKind.ENTITY]


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------


def test_literal_null_is_read_as_empty() -> None:
    assert clean_field("NULL") == ""
    assert clean_field(None) == ""
    assert clean_field(" MTR ") == "MTR"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("14-03-2011", "2011-03-14"), (None, None), ("", None), ("2011-03-14", None), ("NULL", None)],
)
def test_parse_hk_date(raw: Any, expected: str | None) -> None:
    assert parse_hk_date(raw) == expected


# ---------------------------------------------------------------------------
# The name check
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("legal_name", "company"),
    [
        # Identical Latin names, including HSBC's "-THE-" suffix.
        ("HONGKONG AND SHANGHAI BANKING CORPORATION LIMITED -THE-", _FIXTURES["hsbc"]),
        # GLEIF's legal name is the Chinese name; the register's English name differs entirely.
        ("香港鐵路有限公司", _FIXTURES["mtr"]),
        # Simplified (GLEIF) vs traditional (register).
        ("威格进出口有限公司", {"Brn": "77673193", "Chinese_Company_Name": "威格進出口有限公司",
                              "English_Company_Name": "WEIGE TRADE (HK) LIMITED"}),
        # The phrase-sensitive case that an s2t fold got wrong (托 → 託).
        ("维克托国际贸易有限公司", {"Brn": "66429117", "Chinese_Company_Name": "維克托國際貿易有限公司",
                                "English_Company_Name": "VICTOR INTERNATIONAL TRADE CO., LIMITED"}),
        # Full-width brackets plus simplified characters.
        ("开普实业（香港）有限公司", {"Brn": "73959056",
                                  "Chinese_Company_Name": "開普實業(香港)有限公司",
                                  "English_Company_Name": "Creatcup Industry And Commerce (Hong Kong) Limited"}),
        # Legal-form spelling differences.
        ("Victor International Trade Company Limited", {"Brn": "66429117",
                                                         "English_Company_Name": "VICTOR INTERNATIONAL TRADE CO., LIMITED"}),
    ],
)
def test_names_agree_on_real_pairs(legal_name: str, company: dict[str, Any]) -> None:
    assert names_agree(legal_name, company) is True


def test_names_disagree_when_gleif_filed_another_companys_brn() -> None:
    """GLEIF's Chery Global Innovations record carries TMF Secretaries' BRN."""
    tmf = _FIXTURES["tmf_secretaries_null_chinese"]
    assert names_agree("奇瑞全球創新(香港)有限公司", tmf) is False
    assert names_agree("Chery Global Innovations (Hong Kong) Limited", tmf) is False


def test_an_empty_legal_name_cannot_contradict_the_identifier() -> None:
    assert names_agree("", _FIXTURES["hsbc"]) is True


def test_the_literal_null_chinese_name_never_matches() -> None:
    assert names_agree("NULL", _FIXTURES["hsbc"]) is False


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key", ["guarantee_dummy_brn", "hsbc", "mtr", "tmf_secretaries_null_chinese"])
def test_schema_accepts_live_payloads(key: str) -> None:
    validate_raw("cr_hongkong", CrHongKongBundle, _bundle(key))


def test_schema_requires_brn() -> None:
    bundle = _bundle("mtr")
    del bundle["company"]["Brn"]
    with pytest.raises(SourceSchemaError):
        validate_raw("cr_hongkong", CrHongKongBundle, bundle)


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


def test_maps_one_entity_statement_with_hk_brn() -> None:
    statements = list(map_cr_hongkong(_bundle("mtr")))
    assert len(statements) == 1
    rd = statements[0]["recordDetails"]
    assert rd["name"] == "MTR CORPORATION LIMITED"
    assert rd["identifiers"] == [
        {
            "id": "30958055",
            "scheme": "HK-BRN",
            "schemeName": "Hong Kong Business Registration Number (Unique Business Identifier)",
        }
    ]
    assert rd["jurisdiction"] == {"name": "Hong Kong", "code": "HK"}
    assert rd["foundingDate"] == "2000-04-26"
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Public company limited by shares"}
    assert rd["addresses"][0]["type"] == "registered"
    assert rd["addresses"][0]["country"]["code"] == "HK"


def test_chinese_name_goes_to_alternate_names() -> None:
    rd = list(map_cr_hongkong(_bundle("mtr")))[0]["recordDetails"]
    assert rd["alternateNames"] == ["香港鐵路有限公司"]


def test_literal_null_chinese_name_is_not_an_alternate_name() -> None:
    rd = list(map_cr_hongkong(_bundle("hsbc")))[0]["recordDetails"]
    assert "NULL" not in json.dumps(rd)
    assert not rd.get("alternateNames")


def test_dummy_brn_is_carried_as_filed() -> None:
    rd = list(map_cr_hongkong(_bundle("guarantee_dummy_brn")))[0]["recordDetails"]
    assert rd["identifiers"][0]["id"] == "C1572528"


def test_presence_is_recorded_as_live() -> None:
    """The dataset lists live companies only — presence is the register's liveness."""
    statement = list(map_cr_hongkong(_bundle("mtr")))[0]
    status = liveness.read_register_status(statement)
    assert status is not None and status["liveness"] == liveness.LIVE
    assert "dissolutionDate" not in statement["recordDetails"]


def test_never_asserts_people_or_relationships() -> None:
    statements = list(map_cr_hongkong(_bundle("mtr")))
    assert all(s["recordDetails"].get("entityType") for s in statements)
    assert "beneficialOwnershipOrControl" not in json.dumps(statements)


@pytest.mark.parametrize("bundle", [{}, {"is_stub": True}, {"hk_brn": "", "company": {}, "is_stub": False}])
def test_mapper_is_defensive(bundle: dict[str, Any]) -> None:
    assert list(map_cr_hongkong(bundle)) == []


# ---------------------------------------------------------------------------
# Finding
# ---------------------------------------------------------------------------


def test_finding() -> None:
    assert (
        finding_cr_hongkong(_bundle("guarantee_dummy_brn"))
        == "Live company limited by guarantee, incorporated 14 March 2011."
    )


def test_finding_degrades_without_fields() -> None:
    assert finding_cr_hongkong(_bundle("mtr", company={"Brn": "30958055"})) == (
        "Live on the companies register."
    )


def test_finding_mentions_re_domiciliation_and_fits_the_cap() -> None:
    company = _company("mtr") | {"Re-domiciliation_Date": "02-06-2025"}
    finding = finding_cr_hongkong(_bundle("mtr", company=company))
    assert finding == (
        "Live public company limited by shares, incorporated 26 April 2000, "
        "re-domiciled into Hong Kong 2 June 2025."
    )
    assert len(finding) <= MAX_FINDING_CHARS


def test_finding_is_none_for_a_stub() -> None:
    assert finding_cr_hongkong({"is_stub": True}) is None


# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------


def _response(status: int, payload: Any) -> MagicMock:
    response = MagicMock(status_code=status, is_success=200 <= status < 300)
    response.json.return_value = payload
    return response


def _run_fetch(response: MagicMock, *, brn: str = "30958055", legal_name: str = "") -> tuple[dict, list, AsyncMock]:
    adapter = CrHongKongAdapter()
    settings = MagicMock(allow_live=True)
    get = AsyncMock(return_value=response)
    with patch("opencheck.sources.cr_hongkong.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put"), \
         patch("opencheck.sources.cr_hongkong.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        with degradation.recording() as recorded:
            result = asyncio.run(adapter.fetch(brn, legal_name=legal_name))
    return result, list(recorded), get


def test_fetch_returns_the_company() -> None:
    result, recorded, get = _run_fetch(
        _response(200, [_company("mtr")]), legal_name="MTR Corporation Limited"
    )
    assert result["is_stub"] is False
    assert result["company"]["Brn"] == "30958055"
    assert recorded == []
    params = get.await_args.kwargs["params"]
    assert params == {"query[0][key1]": "Brn", "query[0][key2]": "equal", "query[0][key3]": "30958055"}


def test_http_400_no_result_is_a_clean_miss_not_a_degradation() -> None:
    miss = _FIXTURES["_miss_http_400"]
    result, recorded, _ = _run_fetch(_response(miss["status_code"], miss["body"]), brn="99999999")
    assert result["company"] is None
    assert result["is_stub"] is True
    assert recorded == []


def test_name_mismatch_drops_the_record() -> None:
    result, recorded, _ = _run_fetch(
        _response(200, [_company("tmf_secretaries_null_chinese")]),
        brn="07341475",
        legal_name="奇瑞全球創新(香港)有限公司",
    )
    assert result["company"] is None
    assert result["is_stub"] is True
    assert result["name_mismatch"] is True
    assert recorded == []


@pytest.mark.parametrize("status", [403, 500, 503])
def test_upstream_failure_degrades(status: int) -> None:
    result, recorded, _ = _run_fetch(_response(status, {}))
    assert result["company"] is None
    assert any(d.source_id == "cr_hongkong" and str(status) in d.detail for d in recorded)


def test_an_unexpected_400_degrades_with_the_registers_message() -> None:
    result, recorded, _ = _run_fetch(
        _response(400, {"status": 400, "message": "Invalid requested Filter."})
    )
    assert result["company"] is None
    assert any("Invalid requested Filter." in d.detail for d in recorded)


def test_malformed_brn_skips_without_a_call() -> None:
    result, recorded, get = _run_fetch(_response(200, []), brn="BXT996")
    assert result["is_stub"] is True
    assert get.await_count == 0


def _run_search(query: str, response: MagicMock) -> tuple[list, AsyncMock]:
    adapter = CrHongKongAdapter()
    settings = MagicMock(allow_live=True)
    get = AsyncMock(return_value=response)
    with patch("opencheck.sources.cr_hongkong.get_settings", return_value=settings), \
         patch.object(adapter._cache, "has", return_value=False), \
         patch.object(adapter._cache, "get_payload", return_value=None), \
         patch.object(adapter._cache, "put"), \
         patch("opencheck.sources.cr_hongkong.build_client") as client:
        client.return_value.__aenter__.return_value.get = get
        hits = asyncio.run(adapter.search(query, SearchKind.ENTITY))
    return hits, get


def test_name_search_is_a_prefix_search() -> None:
    hits, get = _run_search("MTR CORPORATION", _response(200, _FIXTURES["_search_mtr_corporation"]))
    params = get.await_args.kwargs["params"]
    assert params["query[0][key1]"] == "Comp_name"
    assert params["query[0][key2]"] == "begins_with"
    assert hits and hits[0].identifiers == {"hk_brn": "30958055"}
    assert hits[0].name == "MTR CORPORATION LIMITED"
    assert "lei" not in hits[0].identifiers


def test_simplified_chinese_query_is_sent_as_traditional() -> None:
    _, get = _run_search("香港铁路", _response(400, {"status": 400, "message": "No result found."}))
    assert get.await_args.kwargs["params"]["query[0][key3]"] == "香港鐵路"


def test_a_brn_shaped_query_is_looked_up_exactly() -> None:
    _, get = _run_search("7341475", _response(200, [_company("tmf_secretaries_null_chinese")]))
    params = get.await_args.kwargs["params"]
    assert params["query[0][key1]"] == "Brn"
    assert params["query[0][key3]"] == "07341475"


def test_search_rejects_person_kind() -> None:
    assert asyncio.run(CrHongKongAdapter().search("x", SearchKind.PERSON)) == []
