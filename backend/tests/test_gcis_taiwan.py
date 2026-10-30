"""Tests for the Taiwan company register adapter (MOEA GCIS) and its mapper.

Fixtures in ``tests/fixtures/gcis_taiwan/`` are **real answers** of the GCIS
platform captured on 29 Oct 2026, bodies verbatim:

* ``tsmc`` — a listed company: natural-person directors with share counts,
  independent directors with none, and the National Development Fund
  (行政院國家發展基金管理會) holding a seat through a representative.
* ``visionbay_juristic`` — a subsidiary whose every seat, supervisor's
  included, is held by one legal person (鴻海精密工業) through four
  representatives; its holding repeated on all four rows.
* ``orsted_holding`` — a Danish legal person (丹麥商) holding all four seats.
* ``yujung_limited`` / ``ktg_limited`` — 有限公司: the figure is a capital
  contribution in NT$, not a share count.
* ``formosa_marine_foreign_branch`` — a foreign company (賴比瑞亞商, Liberia)
  registered in Taiwan: status 核准登記, no capital, a representative.
* ``skyworth_over_100`` — filed holdings that exceed the issued shares.
* ``asml_tainan_branch_only`` — a branch's UBN: no company record.

The decisions these pin are Stephen's of 29 Oct 2026, on the Notion ticket
"Taiwan / MOEA GCIS company registration open data (directors and
supervisors)".
"""

from __future__ import annotations

import asyncio
import json
import ssl
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods import annotations as ann
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, map_gcis_taiwan
from opencheck.bods.mappers.taiwan import (
    TW_POSITIONS,
    TW_STATUS_LIVENESS,
    tw_foreign_prefix,
    tw_percentages,
)
from opencheck.bods.officer_roles import OFFICER_POSITION_KEY
from opencheck.cache import Cache
from opencheck.findings import MAX_FINDING_CHARS, finding_gcis_taiwan
from opencheck.host_throttle import HostRateLimitedError
from opencheck.register_hops import hop_for
from opencheck.routers.hit_builders import _bh_gcis_taiwan
from opencheck.sources import REGISTRY
from opencheck.sources import gcis_taiwan as tw
from opencheck.sources.base import SearchKind

_FIXTURES = Path(__file__).parent / "fixtures" / "gcis_taiwan"
_API = "https://data.gcis.nat.gov.tw/od/data/api"


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _bundle(name: str, **overrides: Any) -> dict[str, Any]:
    data = _fixture(name)
    basic = data["basic"].strip()
    dirs = data["dirs"].strip()
    bundle = {
        "source_id": "gcis_taiwan",
        "tw_ubn": data["ubn"],
        "company": json.loads(basic)[0] if basic else None,
        "directors": json.loads(dirs) if dirs else [],
        "directors_read": True,
        "legal_name": "",
        "is_stub": not basic,
    }
    bundle.update(overrides)
    return bundle


def _map(name: str, **overrides: Any) -> list[dict[str, Any]]:
    with provenance.recording():
        return list(map_gcis_taiwan(_bundle(name, **overrides)))


def _by_type(statements: list[dict[str, Any]], record_type: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == record_type]


def _name(statement: dict[str, Any]) -> str:
    rd = statement["recordDetails"]
    return rd.get("name") or rd["names"][0]["fullName"]


def _rels_of(statements: list[dict[str, Any]], party_name: str) -> list[dict[str, Any]]:
    party = next(s for s in statements if s["recordType"] != "relationship" and _name(s) == party_name)
    return [
        s for s in _by_type(statements, "relationship")
        if s["recordDetails"]["interestedParty"] == party["recordId"]
    ]


# ---------------------------------------------------------------------------
# The identifier and the dispatch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ubn",
    ["22099131", "24757737", "70440190", "60741502", "22818694", " 22099131 "],
)
def test_real_ubns_normalise(ubn: str) -> None:
    assert tw.normalise_ubn(ubn) == ubn.strip()


@pytest.mark.parametrize("bad", ["12345678", "2209913", "220991311", "ABCDEFGH", "", "22099132"])
def test_a_number_that_cannot_be_a_ubn_is_never_sent(bad: str) -> None:
    with pytest.raises(ValueError):
        tw.normalise_ubn(bad)


def test_the_seventh_digit_seven_rule() -> None:
    # 10000073: the weighted digit sum is 9 — not a multiple of five — and the
    # number passes only because the seventh digit's product (7 × 4 = 28) may
    # count as 1 rather than 10.
    assert tw.ubn_check_digit_ok("10000073")
    assert not tw.ubn_check_digit_ok("10000072")


def test_the_deriver_keys_on_the_ministry_ra_code_only() -> None:
    adapter = REGISTRY["gcis_taiwan"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({"RA000551"})
    # RA001009 (the Financial Supervisory Commission) files funds' numbers,
    # which the company register does not hold.
    assert "RA001009" not in deriver.ra_codes
    assert deriver.derived_key == "tw_ubn"


def test_ra000551_maps_to_tw_cr_and_is_a_register_hop() -> None:
    assert _GLEIF_RA_TO_ORG_ID["RA000551"][0] == tw.TW_CR_SCHEME == "TW-CR"
    assert "RA001009" not in _GLEIF_RA_TO_ORG_ID
    hop = hop_for("TW-CR")
    assert hop is not None and hop.source_id == "gcis_taiwan"
    assert hop.normalise("22099131") == "22099131"


@pytest.mark.parametrize(
    ("raw", "iso"),
    [("0760221", "1987-02-21"), ("1150821", "2026-08-21"), ("880930", "1999-09-30"),
     ("", None), ("1151340", None), ("abc", None)],
)
def test_roc_dates(raw: str, iso: str | None) -> None:
    assert tw.parse_roc_date(raw) == iso


def test_search_spellings_keep_the_registers_tai() -> None:
    # OpenCC writes 台 as 臺; the register writes 台灣積體電路製造. The query's own
    # 台 is kept, and 臺 is the second try.
    assert tw.search_spellings("台积电") == ["台積電", "臺積電"]
    assert tw.search_spellings("臺北") == ["臺北", "台北"]
    assert tw.search_spellings("Visionbay") == ["Visionbay"]


# ---------------------------------------------------------------------------
# TLS: one flag cleared, for this host only
# ---------------------------------------------------------------------------


def test_the_ssl_context_clears_only_the_strict_flag() -> None:
    context = tw.gcis_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    strict = getattr(ssl, "VERIFY_X509_STRICT", 0)
    if strict:
        assert not context.verify_flags & strict
    default = httpx.create_ssl_context()
    assert context.verify_flags | strict == default.verify_flags | strict


def test_the_relaxed_context_is_used_for_gcis_and_nowhere_else() -> None:
    # The adapter is the only caller that passes ``verify``.
    import opencheck

    package = Path(opencheck.__file__).parent
    callers = [
        p.relative_to(package).as_posix()
        for p in package.rglob("*.py")
        if "build_client(verify=" in p.read_text(encoding="utf-8")
    ]
    assert callers == ["sources/gcis_taiwan.py"]


# ---------------------------------------------------------------------------
# HTTP behaviour (real transport, respx at the edge)
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter(tmp_path: Path) -> tw.GcisTaiwanAdapter:
    instance = tw.GcisTaiwanAdapter()
    instance._cache = Cache(root=tmp_path)
    return instance


def _live():
    from opencheck.config import get_settings

    settings = get_settings().model_copy(update={"allow_live": True})
    return patch("opencheck.sources.gcis_taiwan.get_settings", return_value=settings)


def _route(router: respx.MockRouter, dataset: str, body: str, status: int = 200) -> respx.Route:
    return router.get(f"{_API}/{dataset}").mock(
        return_value=httpx.Response(status, text=body, headers={"content-type": "application/json"})
    )


def _fetch(adapter: tw.GcisTaiwanAdapter, ubn: str) -> tuple[dict[str, Any], list[Any]]:
    with degradation.recording() as recorded, provenance.recording():
        result = asyncio.run(adapter.fetch(ubn))
    return result, list(recorded)


def test_fetch_reads_the_company_and_its_directors(adapter: tw.GcisTaiwanAdapter) -> None:
    data = _fixture("tsmc")
    with _live(), respx.mock(assert_all_called=True) as router:
        company = _route(router, tw.COMPANY_DATASET, data["basic"])
        directors = _route(router, tw.DIRECTORS_DATASET, data["dirs"])
        result, recorded = _fetch(adapter, "22099131")
        assert company.call_count == directors.call_count == 1
        sent = company.calls[0].request.url.params
        assert sent["$filter"] == "Business_Accounting_NO eq 22099131"
        assert sent["$format"] == "json"
    assert recorded == []
    assert result["is_stub"] is False
    assert result["company"]["Company_Name"] == "台灣積體電路製造股份有限公司"
    assert len(result["directors"]) == 10

    # The answer is cached: a second fetch sends nothing.
    with _live(), respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=_API)
        again, _ = _fetch(adapter, "22099131")
        assert route.call_count == 0
    assert again["company"] == result["company"]


def test_an_empty_body_is_no_record_and_says_what_the_number_is(adapter: tw.GcisTaiwanAdapter) -> None:
    kind = json.dumps([
        {"Year": "115", "exist": "N", "TYPE": "公司"},
        {"Year": "115", "exist": "Y", "TYPE": "分公司"},
        {"Year": "115", "exist": "N", "TYPE": "商業"},
    ], ensure_ascii=False)
    with _live(), respx.mock() as router:
        _route(router, tw.COMPANY_DATASET, "")
        _route(router, tw.DIRECTORS_DATASET, "")
        _route(router, tw.KIND_DATASET, kind)
        result, recorded = _fetch(adapter, "82969793")
    assert recorded == []
    assert result["not_found"] is True and result["is_stub"] is False
    assert result["register_kinds"] == ["分公司"]
    assert result["coverage_note"] == tw.COVERAGE_BRANCH
    # Not cached: a number can become a company.
    assert not list((adapter._cache._root()).rglob("*82969793*"))

    hit = _bh_gcis_taiwan(result, "82969793", type("Ctx", (), {"legal_name": "X"})())
    assert hit.identifiers == {}  # the register confirmed no number
    assert hit.finding == tw.COVERAGE_BRANCH


def test_a_limited_partnership_number_gets_the_partnership_note(adapter: tw.GcisTaiwanAdapter) -> None:
    kind = json.dumps([{"Year": "115", "exist": "N", "TYPE": t} for t in ("公司", "分公司", "商業")],
                      ensure_ascii=False)
    with _live(), respx.mock() as router:
        _route(router, tw.COMPANY_DATASET, "")
        _route(router, tw.DIRECTORS_DATASET, "")
        _route(router, tw.KIND_DATASET, kind)
        result, _ = _fetch(adapter, "42905149")
    assert result["coverage_note"] == tw.COVERAGE_OTHER
    assert "有限合夥" in result["coverage_note"]


@pytest.mark.parametrize(
    ("body", "reason", "words"),
    [
        ("非授權介接之IP(203.0.113.9)，請查明後繼續。", "upstream_error", "not registered"),
        ("超出本日最大介接次數。", "rate_limited", "daily limit"),
        ("本平臺介接服務達上限，請稍候再試。", "rate_limited", "connection limit"),
    ],
)
def test_the_platforms_refusal_sentences_are_degradations(
    adapter: tw.GcisTaiwanAdapter, body: str, reason: str, words: str
) -> None:
    with _live(), respx.mock() as router:
        _route(router, tw.COMPANY_DATASET, body)
        _route(router, tw.DIRECTORS_DATASET, body)
        result, recorded = _fetch(adapter, "22099131")
    assert result["is_stub"] is True
    assert recorded and all(d.reason == reason for d in recorded)
    assert words in recorded[0].detail
    assert "203.0.113.9" not in " ".join(d.detail for d in recorded)  # never the IP
    assert not list(adapter._cache._root().rglob("*22099131*"))


def test_a_company_whose_directors_did_not_answer_is_shown_and_said(adapter: tw.GcisTaiwanAdapter) -> None:
    data = _fixture("tsmc")
    with _live(), respx.mock() as router:
        _route(router, tw.COMPANY_DATASET, data["basic"])
        _route(router, tw.DIRECTORS_DATASET, "", status=503)
        result, recorded = _fetch(adapter, "22099131")
    assert result["is_stub"] is False
    assert result["directors_read"] is False and result["directors"] == []
    assert {d.check for d in recorded} >= {degradation.CHECK_SOURCE_READ}
    assert not list(adapter._cache._root().rglob("*22099131*"))
    assert finding_gcis_taiwan(result).endswith("directors and supervisors not read.")


def test_a_request_the_pacing_refuses_is_rate_limited(adapter: tw.GcisTaiwanAdapter) -> None:
    async def refuse(*_a: Any, **_k: Any) -> Any:
        raise HostRateLimitedError(tw._API_BASE, "no slot")

    with _live(), patch.object(httpx.AsyncClient, "get", refuse):
        result, recorded = _fetch(adapter, "22099131")
    assert result["is_stub"] is True
    assert recorded and recorded[0].reason == degradation.REASON_RATE_LIMITED


def test_offline_nothing_is_sent(adapter: tw.GcisTaiwanAdapter) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=_API)
        result, _ = _fetch(adapter, "22099131")
        assert route.call_count == 0
    assert result["is_stub"] is True


def test_search_by_name_and_by_number(adapter: tw.GcisTaiwanAdapter) -> None:
    row = json.loads(_fixture("tsmc")["basic"])[0]
    with _live(), respx.mock() as router:
        search = _route(router, tw.SEARCH_DATASET, json.dumps([row], ensure_ascii=False))
        hits = asyncio.run(adapter.search("台湾积体电路", SearchKind.ENTITY))
        assert "Company_Name like 台灣積體電路" in search.calls[0].request.url.params["$filter"]
    assert [(h.hit_id, h.identifiers) for h in hits] == [("22099131", {"tw_ubn": "22099131"})]

    with _live(), respx.mock() as router:
        _route(router, tw.COMPANY_DATASET, json.dumps([row], ensure_ascii=False))
        hits = asyncio.run(adapter.search("22099131", SearchKind.ENTITY))
    assert hits[0].name == "台灣積體電路製造股份有限公司"


# ---------------------------------------------------------------------------
# Mapping: the company
# ---------------------------------------------------------------------------


def test_the_company_statement() -> None:
    statements = _map("tsmc")
    entity = statements[0]
    rd = entity["recordDetails"]
    assert rd["name"] == "台灣積體電路製造股份有限公司"
    assert rd["identifiers"] == [
        {"id": "22099131", "scheme": "TW-CR", "schemeName": tw.TW_CR_SCHEME_NAME}
    ]
    assert rd["jurisdiction"] == {"name": "Taiwan", "code": "TW"}
    assert rd["foundingDate"] == "1987-02-21"
    assert rd["entityType"]["type"] == "registeredEntity"
    assert "Company limited by shares (股份有限公司)" in rd["entityType"]["details"]
    assert entity["statementDate"] == "2026-08-21"  # the register's last approved change
    # Only the Chinese name the register published; GLEIF's English name is
    # on GLEIF's own statement.
    assert "alternateNames" not in rd or all("Taiwan Semiconductor" not in n for n in rd["alternateNames"])


def test_a_foreign_companys_registration_takes_its_home_jurisdiction() -> None:
    entity = _map("formosa_marine_foreign_branch")[0]
    assert entity["recordDetails"]["jurisdiction"] == {"name": "Liberia", "code": "LR"}
    assert "賴比瑞亞商" in entity["recordDetails"]["entityType"]["details"]
    assert finding_gcis_taiwan(_bundle("formosa_marine_foreign_branch")).startswith(
        "Foreign company registered in Taiwan"
    )


def test_every_status_in_the_platforms_code_table_is_classified() -> None:
    # The 33 descriptions of /od/cmpStatusCodeData (read 29 Oct 2026) and the
    # one foreign registrations carry. A new wording reads as unknown.
    table = [
        "核准設立", "核准設立，但已命令解散", "重整", "解散", "撤銷", "破產", "合併解散",
        "撤回認許", "廢止", "廢止認許", "解散已清算完結", "撤銷已清算完結", "廢止已清算完結",
        "撤回認許已清算完結", "撤銷認許已清算完結", "廢止認許已清算完結", "撤銷認許", "分割解散",
        "終止破產", "中止破產", "塗銷破產", "破產程序終結(終止)", "破產程序終結(終止)清算中",
        "破產已清算完結", "接管", "撤銷無需清算", "撤銷許可", "廢止許可", "撤銷許可已清算完結",
        "廢止許可已清算完結", "清理", "撤銷公司設立", "清理完結",
    ]
    assert len(table) == 33
    assert set(TW_STATUS_LIVENESS) == set(table) | {"核准登記"}


def test_a_terminal_status_dates_the_dissolution() -> None:
    bundle = _bundle("ktg_limited")
    bundle["company"] = dict(bundle["company"], Company_Status_Desc="解散已清算完結", Revoke_App_Date="1130515")
    with provenance.recording():
        entity = list(map_gcis_taiwan(bundle))[0]
    assert entity["recordDetails"]["dissolutionDate"] == "2024-05-15"


# ---------------------------------------------------------------------------
# Mapping: directors, supervisors and holdings (decisions A, B, C)
# ---------------------------------------------------------------------------


def test_a_natural_person_director_holds_a_seat_and_a_shareholding() -> None:
    statements = _map("tsmc")
    (rel,) = _rels_of(statements, "魏哲家")
    types = [i["type"] for i in rel["recordDetails"]["interests"]]
    assert types == ["boardChair", "shareholding"]
    share = rel["recordDetails"]["interests"][1]
    assert share["share"] == {"exact": round(7452349 * 100 / 25932370067, 4)}
    assert "7,452,349 shares of 25,932,370,067 issued" in share["details"]
    assert "publishes no list of shareholders" in share["details"]
    # The percentage is OpenCheck's arithmetic, said on the share it computed.
    (note,) = rel["annotations"]
    assert note["motivation"] == "transformation"
    assert note["statementPointerTarget"] == "/recordDetails/interests/1/share"


def test_an_independent_director_with_no_shares_has_only_the_seat() -> None:
    (rel,) = _rels_of(_map("tsmc"), "彼得‧邦菲爵士")
    assert [i["type"] for i in rel["recordDetails"]["interests"]] == ["boardMember"]
    assert "Independent director (獨立董事)" in rel["recordDetails"]["interests"][0]["details"]


def test_the_national_development_fund_holds_its_seat_and_six_per_cent() -> None:
    statements = _map("tsmc")
    fund = next(s for s in _by_type(statements, "entity") if _name(s) == "行政院國家發展基金管理會")
    # Decision E: never classified as the state by its name.
    assert fund["recordDetails"]["entityType"]["type"] == "legalEntity"
    (rel,) = _rels_of(statements, "行政院國家發展基金管理會")
    interests = rel["recordDetails"]["interests"]
    assert [i["type"] for i in interests] == ["boardMember", "shareholding"]
    assert interests[1]["share"] == {"exact": 6.377}
    assert "葉俊顯" in interests[0]["details"]


def test_a_legal_person_holds_every_seat_and_one_shareholding() -> None:
    statements = _map("visionbay_juristic")
    (rel,) = _rels_of(statements, "鴻海精密工業股份有限公司")
    interests = rel["recordDetails"]["interests"]
    # Four seats (chair, two directors, a supervisor) and ONE holding — the
    # register repeats 1,310,000,000 on all four rows.
    assert [i["type"] for i in interests] == [
        "boardChair", "boardMember", "boardMember", "otherInfluenceOrControl", "shareholding",
    ]
    assert interests[-1]["share"] == {"exact": 100.0}
    assert interests[3][OFFICER_POSITION_KEY] == "Supervisor (監察人)"


def test_a_representative_names_the_legal_person_and_holds_nothing() -> None:
    statements = _map("visionbay_juristic")
    (rel,) = _rels_of(statements, "黃英士")
    (interest,) = rel["recordDetails"]["interests"]
    assert interest["type"] == "otherInfluenceOrControl"
    assert interest[OFFICER_POSITION_KEY] == (
        "Representative of 鴻海精密工業股份有限公司: Chair of the board (董事長)"
    )
    assert "share" not in interest


def test_supervisors_and_representatives_are_officers_never_board_members() -> None:
    for position, (interest_type, words, officer) in TW_POSITIONS.items():
        if position in ("監察人", "在中華民國境內負責人/代表人", "訴訟代理人及非訴訟代理人"):
            assert interest_type == "otherInfluenceOrControl" and officer, position
    (rel,) = _rels_of(_map("formosa_marine_foreign_branch"), "呂學修")
    (interest,) = rel["recordDetails"]["interests"]
    assert interest[OFFICER_POSITION_KEY].startswith("Representative in Taiwan")


def test_a_limited_companys_holding_is_a_capital_contribution() -> None:
    (rel,) = _rels_of(_map("ktg_limited"), "林俊豪")
    share = rel["recordDetails"]["interests"][1]
    assert share["share"] == {"exact": 60.0}
    assert share["details"].startswith("Capital contribution of NT$60,000 of NT$100,000 total capital")


def test_no_percentage_when_the_holdings_exceed_the_issued_shares() -> None:
    bundle = _bundle("skyworth_over_100")
    assert tw_percentages(bundle["company"], bundle["directors"]) is None
    statements = _map("skyworth_over_100")
    shares = [
        i for s in _by_type(statements, "relationship")
        for i in s["recordDetails"]["interests"] if i["type"] == "shareholding"
    ]
    assert shares and all("share" not in i for i in shares)
    notes = [a["description"] for s in _by_type(statements, "relationship") for a in s.get("annotations", [])]
    assert any("more than the" in n for n in notes)


def test_no_percentage_for_a_foreign_branch_with_no_capital() -> None:
    bundle = _bundle("formosa_marine_foreign_branch")
    assert tw_percentages(bundle["company"], bundle["directors"]) is None


def test_a_foreign_legal_person_takes_its_prefixs_jurisdiction() -> None:
    statements = _map("orsted_holding")
    holder = next(s for s in _by_type(statements, "entity") if "Orsted" in _name(s))
    assert holder["recordDetails"]["jurisdiction"] == {"name": "Denmark", "code": "DK"}
    assert tw_foreign_prefix("英屬開曼群島商六方科技股份有限公司")[1] == "KY"  # not 英商 → GB


@pytest.mark.parametrize(
    "fixture",
    ["tsmc", "visionbay_juristic", "orsted_holding", "yujung_limited", "ktg_limited",
     "formosa_marine_foreign_branch", "skyworth_over_100"],
)
def test_every_mapping_is_well_formed(fixture: str) -> None:
    statements = _map(fixture)
    for stmt in statements:
        assert not ann.validate_annotations(stmt), stmt.get("annotations")
        for interest in (stmt["recordDetails"].get("interests") or []):
            # A commercial register: the flag is never set.
            assert "beneficialOwnershipOrControl" not in interest
            share = (interest.get("share") or {}).get("exact")
            assert share is None or 0 <= share <= 100
    # Only the subject carries an identifier, and never the LEI.
    identified = [s for s in statements if s["recordDetails"].get("identifiers")]
    assert identified == [statements[0]]
    # A natural person appears once, however many rows name them.
    people = [_name(s) for s in _by_type(statements, "person")]
    assert len(people) == len(set(people))


def test_a_stub_or_not_found_bundle_maps_to_nothing() -> None:
    assert list(map_gcis_taiwan({"tw_ubn": "82969793", "company": None, "is_stub": True})) == []
    assert list(map_gcis_taiwan({"tw_ubn": "82969793", "company": None, "is_stub": False, "not_found": True})) == []


# ---------------------------------------------------------------------------
# The finding sentence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        ("tsmc", "Active company limited by shares, set up 21 February 1987, 10 directors, "
                 "directors and supervisors hold 6.5% between them."),
        ("yujung_limited", "Active limited company, set up 30 September 1999, 1 director, "
                           "directors and supervisors hold 100% between them."),
        ("skyworth_over_100", "Active company limited by shares, set up 4 October 2016, "
                              "2 directors, directors' and supervisors' holdings filed."),
    ],
)
def test_finding(fixture: str, expected: str) -> None:
    assert finding_gcis_taiwan(_bundle(fixture)) == expected


@pytest.mark.parametrize(
    "fixture",
    ["tsmc", "visionbay_juristic", "orsted_holding", "yujung_limited", "ktg_limited",
     "formosa_marine_foreign_branch", "skyworth_over_100"],
)
def test_the_finding_never_claims_ownership(fixture: str) -> None:
    sentence = finding_gcis_taiwan(_bundle(fixture)) or ""
    assert len(sentence) <= MAX_FINDING_CHARS
    lowered = sentence.lower()
    for word in ("owner", "owns", "owned", "beneficial", "shareholder"):
        assert word not in lowered, (word, sentence)
