"""Open DART (South Korea) — the FSS disclosure filings, reached from the
business or corporate registration number GLEIF files.

Fixtures in ``tests/fixtures/dart_korea/`` are **real answers** of the Open
DART API for Samsung Electronics (corp_code 00126380), captured on
10 Nov 2026 with the key removed. The crosswalk index is produced by the
**real builder** (``scripts/build_dart_korea_index.py``) with its network
reads replaced, and read back through the real adapter.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, map_dart_korea
from opencheck.bods.mappers.korea import (
    kr_birth_month,
    kr_largest_shareholder,
    kr_reporter_is_person,
    kr_shareholder_is_entity,
)
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_dart_korea
from opencheck.licensing import classify
from opencheck.routers.hit_builders import _bh_dart_korea
from opencheck.secret_scrub import scrub
from opencheck.sources import REGISTRY
from opencheck.sources import dart_korea as dk

_FIXTURES = Path(__file__).parent / "fixtures" / "dart_korea"
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_dart_korea_index.py"
SAMSUNG = "00126380"
SAMSUNG_BRN = "1248100998"
SAMSUNG_CRN = "1301110006246"
SAMSUNG_LEI = "9884007ER46L6N7EI764"
_CTX = type("Ctx", (), {"legal_name": "Samsung Electronics Co., Ltd."})()


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_dart_korea_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _rows(name: str) -> list[dict[str, Any]]:
    return list(_fixture(name)["list"])


def _bundle(**overrides: Any) -> dict[str, Any]:
    company = {k: v for k, v in _fixture(f"company_{SAMSUNG}").items() if k not in ("status", "message")}
    report = dk.latest_periodic_report(_rows(f"list_{SAMSUNG}"))
    bundle: dict[str, Any] = {
        "source_id": "dart_korea",
        "hit_id": SAMSUNG_BRN,
        "kr_number": SAMSUNG_BRN,
        "corp_code": SAMSUNG,
        "legal_name": "Samsung Electronics Co., Ltd.",
        "company": company,
        "profile_read": True,
        "report": report,
        "reports_read": True,
        "shareholders": _rows(f"hyslrSttus_{SAMSUNG}_2026_11012"),
        "shareholders_read": True,
        "executives": _rows(f"exctvSttus_{SAMSUNG}_2026_11012"),
        "executives_read": True,
        "major_holders": _rows(f"majorstock_{SAMSUNG}"),
        "major_holders_read": True,
        "link": dk.filing_url(report["rcept_no"]) if report else dk.HOMEPAGE,
        "is_stub": False,
    }
    bundle.update(overrides)
    return bundle


def _map(**overrides: Any) -> list[dict[str, Any]]:
    with provenance.recording():
        return list(map_dart_korea(_bundle(**overrides)))


def _by_type(statements: list[dict[str, Any]], record_type: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == record_type]


# ---------------------------------------------------------------------------
# Numbers, reports, the dispatch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["124-81-00998", "1248100998", " 124 81 00998 "])
def test_business_registration_numbers_normalise(raw: str) -> None:
    assert dk.normalise_bizr_no(raw) == SAMSUNG_BRN
    assert dk.format_bizr_no(SAMSUNG_BRN) == "124-81-00998"


@pytest.mark.parametrize("raw", ["130111-0006246", "1301110006246"])
def test_corporate_registration_numbers_normalise(raw: str) -> None:
    assert dk.normalise_jurir_no(raw) == SAMSUNG_CRN
    assert dk.format_jurir_no(SAMSUNG_CRN) == "130111-0006246"


@pytest.mark.parametrize("bad", ["", "12481009", "124810099812", "ABC-81-00998"])
def test_a_number_of_the_wrong_length_is_refused(bad: str) -> None:
    with pytest.raises(ValueError):
        dk.normalise_bizr_no(bad)
    assert dk.number_kind(bad) is None


def test_the_derivers_key_on_the_two_ra_codes() -> None:
    adapter = REGISTRY["dart_korea"]
    by_key = {d.derived_key: d.ra_codes for d in adapter.lookup_derivers}
    assert by_key == {"kr_bizr_no": frozenset({"RA000657"}), "kr_jurir_no": frozenset({"RA000419"})}
    assert _GLEIF_RA_TO_ORG_ID["RA000657"][0] == "KR-BRN"
    assert _GLEIF_RA_TO_ORG_ID["RA000419"][0] == "KR-CRN"


def test_report_names_read_year_code_and_period() -> None:
    assert dk.report_of("반기보고서 (2026.06)") == ("2026", "11012", "2026-06")
    assert dk.report_of("사업보고서 (2025.12)") == ("2025", "11011", "2025-12")
    assert dk.report_of("[기재정정]분기보고서 (2025.09)") == ("2025", "11014", "2025-09")
    assert dk.report_of("주요사항보고서(자기주식취득결정)") is None


def test_the_latest_periodic_report_is_the_half_year() -> None:
    report = dk.latest_periodic_report(_rows(f"list_{SAMSUNG}"))
    assert report is not None
    assert (report["bsns_year"], report["reprt_code"], report["period"]) == ("2026", "11012", "2026-06")
    assert report["rcept_no"] == "20260814003699"


def test_the_key_is_scrubbed_from_any_message() -> None:
    text = "GET https://opendart.fss.or.kr/api/company.json?crtfc_key=abcdef0123456789abcdef0123456789abcdef01&corp_code=1"
    assert "abcdef0123" not in scrub(text)


def test_the_licence_is_open() -> None:
    assert classify("KR-Public-Data").color == "green"


# ---------------------------------------------------------------------------
# The crosswalk builder and the index
# ---------------------------------------------------------------------------


def test_name_keys_drop_the_legal_form() -> None:
    assert builder.ko_key("삼성전자(주)") == builder.ko_key("삼성전자 주식회사") == builder.ko_key("삼성전자")
    assert builder.en_key("SAMSUNG ELECTRONICS CO., LTD.") == builder.en_key("Samsung Electronics Co Ltd")


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, str]]:
    records = [
        {"lei": SAMSUNG_LEI, "ra_code": "RA000657", "registered_as": "124-81-00998",
         "number": SAMSUNG_BRN, "registration_status": "ISSUED",
         "names": ["Samsung Electronics Co., Ltd.", "삼성전자 주식회사"]},
        # A name match whose number disagrees: refused.
        {"lei": "XXXXXXXXXXXXXXXXXX01", "ra_code": "RA000419", "registered_as": "110111-0000001",
         "number": "1101110000001", "registration_status": "ISSUED", "names": ["삼성전자(주)"]},
    ]
    filers = [
        {"corp_code": SAMSUNG, "corp_name": "삼성전자", "corp_eng_name": "SAMSUNG ELECTRONICS CO,.LTD",
         "stock_code": "005930", "modify_date": "20260801"},
        {"corp_code": "00999999", "corp_name": "다른회사", "corp_eng_name": "", "stock_code": "", "modify_date": "20250101"},
    ]
    calls: list[str] = []

    def profile(_key: str, code: str) -> dict[str, Any] | None:
        calls.append(code)
        return _fixture(f"company_{SAMSUNG}") if code == SAMSUNG else None

    monkeypatch.setattr(builder, "MIN_GLEIF", 1)
    monkeypatch.setattr(builder, "MIN_FILERS", 1)
    monkeypatch.setattr(builder, "gleif_records", lambda: records)
    monkeypatch.setattr(builder, "corp_codes", lambda _key, _zip=None: filers)
    monkeypatch.setattr(builder, "read_profile", profile)
    out = tmp_path / "dart_korea.sqlite"
    meta = builder.build(out, key="k", previous=None, max_calls=10)
    assert calls == [SAMSUNG]
    return out, meta


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("DART_KOREA_DB_FILE", None)
    else:
        os.environ["DART_KOREA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    dk.reset_connection()


@pytest.fixture
def index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    out, meta = _build(tmp_path, monkeypatch)
    _point_at(out)
    yield out, meta
    _point_at(None)


def test_the_builder_accepts_only_an_equal_number(index) -> None:
    _out, meta = index
    assert meta["schema_version"] == dk.INDEX_SCHEMA_VERSION
    assert meta["leis_matched"] == "1" and meta["issued_matched"] == "1"
    assert meta["complete"] == "true"
    filer = dk.filer_for_number(SAMSUNG_BRN)
    assert filer and filer["corp_code"] == SAMSUNG
    assert dk.filer_for_number(SAMSUNG_CRN)["corp_code"] == SAMSUNG
    assert dk.filer_for_number("1101110000001") is None


def test_the_builder_reuses_an_unchanged_filer(index, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out, _ = index
    sent: list[str] = []
    monkeypatch.setattr(builder, "read_profile", lambda _k, code: sent.append(code))
    meta = builder.build(tmp_path / "again.sqlite", key="k", previous=out, max_calls=10)
    assert sent == [] and meta["leis_matched"] == "1"


# ---------------------------------------------------------------------------
# HTTP behaviour (real transport, respx at the edge)
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter(tmp_path: Path, index) -> dk.DartKoreaAdapter:
    instance = dk.DartKoreaAdapter()
    instance._cache = Cache(root=tmp_path / "cache")
    dk.reset_budget()
    return instance


def _live(key: str = "test-key"):
    settings = get_settings().model_copy(update={"allow_live": True, "opendart_api_key": key})
    return patch("opencheck.sources.dart_korea.get_settings", return_value=settings)


def _answer(name: str) -> httpx.Response:
    return httpx.Response(200, json=_fixture(name))


def _routes(router: respx.MockRouter, *, status: str | None = None) -> dict[str, respx.Route]:
    def body(name: str) -> httpx.Response:
        if status is not None:
            return httpx.Response(200, json={"status": status, "message": "refused"})
        return _answer(name)

    return {
        "company": router.get(f"{dk.API_BASE}/company.json").mock(return_value=body(f"company_{SAMSUNG}")),
        "list": router.get(f"{dk.API_BASE}/list.json").mock(return_value=body(f"list_{SAMSUNG}")),
        "majorstock": router.get(f"{dk.API_BASE}/majorstock.json").mock(return_value=body(f"majorstock_{SAMSUNG}")),
        "hyslrSttus": router.get(f"{dk.API_BASE}/hyslrSttus.json").mock(
            return_value=body(f"hyslrSttus_{SAMSUNG}_2026_11012")
        ),
        "exctvSttus": router.get(f"{dk.API_BASE}/exctvSttus.json").mock(
            return_value=body(f"exctvSttus_{SAMSUNG}_2026_11012")
        ),
    }


def _fetch(adapter: dk.DartKoreaAdapter, number: str) -> tuple[dict[str, Any], list[Any], str]:
    with degradation.recording() as recorded, provenance.recording() as rec:
        result = asyncio.run(adapter.fetch(number, legal_name="Samsung Electronics Co., Ltd."))
    return result, list(recorded), rec.resolve(is_stub=bool(result.get("is_stub"))).liveness


def test_fetch_reads_the_five_parts(adapter: dk.DartKoreaAdapter) -> None:
    with _live(), respx.mock(assert_all_called=True) as router:
        routes = _routes(router)
        result, recorded, liveness = _fetch(adapter, "124-81-00998")
        sent = routes["hyslrSttus"].calls[0].request.url.params
        assert sent["corp_code"] == SAMSUNG and sent["bsns_year"] == "2026" and sent["reprt_code"] == "11012"
    assert recorded == []
    assert liveness == "live"
    assert result["is_stub"] is False and result["profile_read"] is True
    assert result["corp_code"] == SAMSUNG
    assert result["link"].endswith("rcpNo=20260814003699")
    assert len(result["shareholders"]) > 10 and len(result["executives"]) > 5

    # Cached for a day: a second fetch sends nothing.
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=dk.API_BASE)
        again, _, _ = _fetch(adapter, SAMSUNG_BRN)
        assert route.call_count == 0
    assert again["shareholders"] == result["shareholders"]


def test_a_spent_daily_cap_is_rate_limited_and_never_cached(adapter: dk.DartKoreaAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _routes(router, status="020")
        result, recorded, _ = _fetch(adapter, SAMSUNG_BRN)
    assert result["is_stub"] is False  # the crosswalk's row stands in
    assert result["profile_read"] is False
    assert recorded and {d.reason for d in recorded} == {degradation.REASON_RATE_LIMITED}
    assert not list(adapter._cache._root().rglob(f"*{SAMSUNG}*"))


def test_no_data_is_an_answer(adapter: dk.DartKoreaAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        routes = _routes(router)
        routes["majorstock"].mock(return_value=httpx.Response(200, json={"status": "013", "message": "no data"}))
        result, recorded, _ = _fetch(adapter, SAMSUNG_BRN)
    assert recorded == []
    assert result["major_holders"] == [] and result["major_holders_read"] is True


def test_a_refused_key_is_not_configured(adapter: dk.DartKoreaAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        _routes(router, status="010")
        _result, recorded, _ = _fetch(adapter, SAMSUNG_BRN)
    assert recorded and recorded[0].reason == degradation.REASON_NOT_CONFIGURED


def test_no_key_sends_nothing_and_says_so(adapter: dk.DartKoreaAdapter) -> None:
    with _live(key=""), respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=dk.API_BASE)
        result, recorded, _ = _fetch(adapter, SAMSUNG_BRN)
        assert route.call_count == 0
    assert result["is_stub"] is True
    assert recorded and recorded[0].reason == degradation.REASON_NOT_CONFIGURED


def test_a_number_the_crosswalk_lacks_is_a_note_card(adapter: dk.DartKoreaAdapter) -> None:
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=dk.API_BASE)
        result, recorded, liveness = _fetch(adapter, "2208100001")
        assert route.call_count == 0
    assert recorded == []
    assert result["not_found"] is True and result["is_stub"] is False
    assert liveness == "snapshot"
    hit = _bh_dart_korea(result, "2208100001", _CTX)
    assert hit.identifiers == {}


def test_without_an_index_the_source_is_not_announced(tmp_path: Path) -> None:
    _point_at(tmp_path / "missing.sqlite")
    try:
        assert REGISTRY["dart_korea"].covers_lei(SAMSUNG_LEI) is False
    finally:
        _point_at(None)


# ---------------------------------------------------------------------------
# The mapping
# ---------------------------------------------------------------------------


def test_the_company_statement_carries_both_numbers() -> None:
    statements = _map()
    subject = next(s for s in _by_type(statements, "entity") if s["recordDetails"]["name"] == "삼성전자(주)")
    ids = {(i["scheme"], i["id"]) for i in subject["recordDetails"]["identifiers"]}
    assert ("KR-BRN", "124-81-00998") in ids and ("KR-CRN", "130111-0006246") in ids
    assert subject["recordDetails"]["jurisdiction"]["code"] == "KR"


def test_no_statement_claims_beneficial_ownership() -> None:
    for statement in _by_type(_map(), "relationship"):
        for interest in statement["recordDetails"]["interests"]:
            assert "beneficialOwnershipOrControl" not in interest or interest["beneficialOwnershipOrControl"] is False
            # "not of beneficial ownership" is the caveat; nobody is called an owner.
            assert not re.search(r"\bowners?\b|\bowned\b", (interest.get("details") or "").lower())


def test_the_largest_shareholder_is_its_own_row() -> None:
    name, share = kr_largest_shareholder(_rows(f"hyslrSttus_{SAMSUNG}_2026_11012"))
    assert name.startswith("삼성생명") and share == pytest.approx(8.51)


def test_a_five_per_cent_report_never_puts_the_combined_figure_in_share() -> None:
    statements = _map()
    reported = [
        i for s in _by_type(statements, "relationship") for i in s["recordDetails"]["interests"]
        if "5% holding report" in (i.get("details") or "")
    ]
    assert reported and all("share" not in i for i in reported)


def test_executives_carry_a_partial_birth_date_and_no_sex() -> None:
    people = _by_type(_map(), "person")
    assert people
    for person in people:
        birth = person["recordDetails"].get("birthDate")
        assert birth is None or len(birth) == 7
    assert "sexdstn" not in json.dumps(_map(), ensure_ascii=False)


@pytest.mark.parametrize(
    ("raw", "expected"), [("1960년 12월", "1960-12"), ("1975.03", "1975-03"), ("", None), ("unknown", None)]
)
def test_birth_months(raw: str, expected: str | None) -> None:
    assert kr_birth_month(raw) == expected


def test_who_is_an_entity_and_who_a_person() -> None:
    assert kr_shareholder_is_entity("삼성생명보험㈜", "계열회사") is True
    assert kr_shareholder_is_entity("이재용", "본인") is False
    assert kr_reporter_is_person("이재용") is True
    assert kr_reporter_is_person("BlackRock Fund Advisors") is False


def test_a_stub_or_not_found_bundle_maps_to_nothing() -> None:
    with provenance.recording():
        assert list(map_dart_korea({"source_id": "dart_korea", "is_stub": True})) == []
        assert list(map_dart_korea({"source_id": "dart_korea", "is_stub": False, "not_found": True})) == []


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


def test_the_finding() -> None:
    text = finding_dart_korea(_bundle())
    assert text.startswith("Listed on KOSPI; largest shareholder 삼성생명보험㈜ 8.51% (half-year report)")
    assert len(text) <= MAX_FINDING_CHARS
    assert not re.search(r"\bowners?\b|\bowned\b", text.lower())


def test_the_finding_says_what_was_not_read() -> None:
    text = finding_dart_korea(_bundle(reports_read=False, report=None, shareholders=[], executives=[],
                                      shareholders_read=False, executives_read=False))
    assert "periodic reports not read" in text


def test_the_hit_asserts_the_numbers_dart_publishes() -> None:
    hit = _bh_dart_korea(_bundle(), SAMSUNG_BRN, _CTX)
    assert hit.identifiers == {"kr_bizr_no": SAMSUNG_BRN, "kr_jurir_no": SAMSUNG_CRN}
    assert hit.summary.startswith("KR-BRN 124-81-00998")
