"""Tests for the Wikirate adapter's REST API path (Wikirate 2.0).

Fixtures are real responses from ``data.wikirate.org/rest/v1`` captured on
2 Nov 2026 for BP plc. (company 637): the company found by LEI (ISIN list cut
to three), its three latest researched answers, and the 401 / 404 bodies the
API returns for a bad token and a missing ``Accept-Profile`` header. Totals
come back in ``Content-Range`` with HTTP 206, as the live API sends them.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

import opencheck.sources.wikirate as wikirate_mod
from opencheck import degradation
from opencheck.bods import map_wikirate
from opencheck.sources.wikirate import WikirateAdapter

REST = "https://data.wikirate.org/rest/v1"
FIXTURES = Path(__file__).parent / "fixtures" / "wikirate"
LEI = "213800LH1BZH3DI6G760"
QID = "Q152057"
PK = "pk-test-0000"
TOKEN = "wr_test_token_0000"


def _fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def rest(monkeypatch):
    """REST credentials set, legacy key unset, nothing cached."""
    monkeypatch.setattr(
        wikirate_mod, "_rest_credentials", lambda: ("https://data.wikirate.org", PK, TOKEN)
    )
    monkeypatch.setattr(wikirate_mod, "_api_key", lambda: None)


@pytest.fixture
def adapter(rest) -> WikirateAdapter:
    a = WikirateAdapter()
    a._cache.get_payload = lambda *args, **kwargs: None  # type: ignore[method-assign]
    a._cache.put = lambda *args, **kwargs: None  # type: ignore[method-assign]
    return a


def _count(total: int) -> httpx.Response:
    return httpx.Response(
        206,
        json=[{"id": 1}],
        headers={"content-range": f"0-0/{total}"},
    )


def _mock_bp(respx_mock, *, total: int = 2295, answers=None, company=None) -> dict:
    routes = {}
    routes["company"] = respx_mock.get(
        f"{REST}/v1_companies",
        params={"company_identifiers->>legal_entity_identifier": f"eq.{LEI}"},
    ).mock(
        return_value=httpx.Response(
            200, json=company if company is not None else _fixture("rest_company_lei_bp.json")
        )
    )
    routes["count"] = respx_mock.get(
        f"{REST}/v1_answers", params={"select": "id", "limit": "1"}
    ).mock(return_value=_count(total))
    routes["answers"] = respx_mock.get(
        f"{REST}/v1_answers", params={"metric_type": "eq.researched"}
    ).mock(
        return_value=httpx.Response(
            200, json=answers if answers is not None else _fixture("rest_answers_637_bp.json")
        )
    )
    return routes


# ----------------------------------------------------------------------
# Choosing the API
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("choice", "rest", "legacy", "expected"),
    [
        ("auto", True, True, "rest"),
        ("auto", False, True, "legacy"),
        ("auto", True, False, "rest"),
        ("auto", False, False, None),
        ("legacy", True, True, "legacy"),
        ("legacy", True, False, None),
        ("rest", False, True, None),
        ("rest", True, True, "rest"),
        (" REST ", True, False, "rest"),
    ],
)
def test_mode(monkeypatch, choice, rest, legacy, expected):
    class _S:
        wikirate_backend = choice

    monkeypatch.setattr(wikirate_mod, "get_settings", lambda: _S())
    monkeypatch.setattr(
        wikirate_mod, "_rest_credentials", lambda: ("u", "k", "t") if rest else None
    )
    monkeypatch.setattr(wikirate_mod, "_api_key", lambda: "key" if legacy else None)
    assert wikirate_mod._mode() == expected


def test_rest_credentials_need_both_values(monkeypatch):
    class _S:
        wikirate_api_url = "https://data.wikirate.org/"
        wikirate_publishable_key = "pk"
        wikirate_api_token = None

    monkeypatch.setattr(wikirate_mod, "get_settings", lambda: _S())
    assert wikirate_mod._rest_credentials() is None
    _S.wikirate_api_token = "wr_x"
    assert wikirate_mod._rest_credentials() == ("https://data.wikirate.org", "pk", "wr_x")


def test_the_token_settings_are_scrubbed_from_errors():
    from opencheck.config import Settings

    fields = Settings.model_fields
    assert fields["wikirate_api_token"].alias == "WIKIRATE_API_TOKEN"
    assert fields["wikirate_publishable_key"].alias == "WIKIRATE_PUBLISHABLE_KEY"
    # secret_scrub redacts every setting whose name ends in key/token/...
    from opencheck.secret_scrub import _SECRET_FIELD

    assert _SECRET_FIELD.search("wikirate_api_token")
    assert _SECRET_FIELD.search("wikirate_publishable_key")


@pytest.mark.parametrize(
    ("header", "total"), [("0-0/2295", 2295), ("*/0", 0), ("*/*", None), (None, None)]
)
def test_content_range_total(header, total):
    assert wikirate_mod._content_range_total(header) == total


# ----------------------------------------------------------------------
# The bundle, from real responses
# ----------------------------------------------------------------------


@respx.mock
async def test_bp_by_lei_builds_the_bundle(adapter, respx_mock):
    routes = _mock_bp(respx_mock)
    bundle = await adapter.fetch_by_lei(LEI, qid=QID)

    assert bundle["api"] == "rest"
    assert bundle["card_id"] == 637
    assert bundle["name"] == "BP plc."
    assert bundle["matched_by"] == "lei"
    assert bundle["total_answers"] == 2295
    assert bundle["headquarters"] == "United Kingdom"
    assert bundle["wikirate_url"] == "https://wikirate.org/company/637"
    assert bundle["platform_url"] == "https://wikirate.org/company/637"
    # The legacy names, so the BODS mapper and hit builder read both APIs.
    assert bundle["identifiers"]["legal_entity_identifier"] == LEI
    assert bundle["identifiers"]["wikidata_id"] == QID
    assert bundle["identifiers"]["sec_central_index_key"] == "313807"
    assert bundle["identifiers"]["international_securities_identification_number"][0] == (
        "DE0008618737"
    )
    first = bundle["latest_answers"][0]
    assert first["metric_designer"] == "Corporate Research Project of Good Jobs First"
    assert first["year"] == 2025
    assert first["value"] == 50043991  # value_numeric, not the text
    assert first["answer_url"] is None
    assert bundle["answers_other_licence"] == 0

    # The three headers on every request, and the count asked for.
    for route in routes.values():
        request = route.calls.last.request
        assert request.headers["apikey"] == PK
        assert request.headers["x-api-token"] == TOKEN
        assert request.headers["accept-profile"] == "api"
    assert routes["count"].calls.last.request.headers["prefer"] == "count=exact"
    answers_request = routes["answers"].calls.last.request
    assert answers_request.url.params["order"] == "year.desc,id.asc"
    assert answers_request.url.params["company_id"] == "eq.637"


@respx.mock
async def test_no_request_reaches_the_legacy_host(adapter, respx_mock):
    legacy = respx_mock.route(host="legacy.wikirate.org").mock(
        return_value=httpx.Response(500)
    )
    _mock_bp(respx_mock)
    assert await adapter.fetch_by_lei(LEI) is not None
    assert not legacy.called


@respx.mock
async def test_the_bundle_maps_to_the_same_bods_as_legacy(adapter, respx_mock):
    _mock_bp(respx_mock)
    bundle = await adapter.fetch_by_lei(LEI)
    statements = list(map_wikirate(bundle))
    assert len(statements) == 1
    schemes = {i["scheme"]: i["id"] for i in statements[0]["recordDetails"]["identifiers"]}
    assert schemes["XI-LEI"] == LEI
    assert schemes["WIKIDATA"] == QID
    assert schemes["US-SEC-CIK"] == "313807"
    assert statements[0]["source"]["url"] == "https://wikirate.org/company/637"


@respx.mock
async def test_falls_back_to_wikidata(adapter, respx_mock):
    respx_mock.get(
        f"{REST}/v1_companies",
        params={"company_identifiers->>legal_entity_identifier": f"eq.{LEI}"},
    ).mock(return_value=httpx.Response(200, json=[]))
    wd = respx_mock.get(
        f"{REST}/v1_companies", params={"company_identifiers->>wikidata": f"eq.{QID}"}
    ).mock(return_value=httpx.Response(200, json=_fixture("rest_company_lei_bp.json")))
    respx_mock.get(f"{REST}/v1_answers", params={"select": "id"}).mock(
        return_value=_count(2295)
    )
    respx_mock.get(f"{REST}/v1_answers", params={"metric_type": "eq.researched"}).mock(
        return_value=httpx.Response(200, json=[])
    )
    bundle = await adapter.fetch_by_lei(LEI, qid=QID)
    assert wd.called
    assert bundle["matched_by"] == "wikidata_qid"


@respx.mock
async def test_no_match_is_none_and_records_nothing(adapter, respx_mock):
    respx_mock.get(f"{REST}/v1_companies").mock(return_value=httpx.Response(200, json=[]))
    degradation.begin()
    assert await adapter.fetch_by_lei(LEI, qid=QID) is None
    assert degradation.collect() == []


@respx.mock
async def test_an_answer_under_another_licence_is_left_out_and_counted(adapter, respx_mock):
    answers = _fixture("rest_answers_637_bp.json")
    answers[1]["license"] = {"id": 9, "abbreviation": "CC BY-NC 4.0"}
    _mock_bp(respx_mock, answers=answers)
    bundle = await adapter.fetch_by_lei(LEI)
    assert len(bundle["latest_answers"]) == 2
    assert bundle["answers_other_licence"] == 1


@respx.mock
async def test_card_path_reads_the_company_by_id(adapter, respx_mock):
    by_id = respx_mock.get(f"{REST}/v1_companies", params={"id": "eq.637"}).mock(
        return_value=httpx.Response(200, json=_fixture("rest_company_lei_bp.json"))
    )
    respx_mock.get(f"{REST}/v1_answers", params={"select": "id"}).mock(
        return_value=_count(2295)
    )
    respx_mock.get(f"{REST}/v1_answers", params={"metric_type": "eq.researched"}).mock(
        return_value=httpx.Response(200, json=_fixture("rest_answers_637_bp.json"))
    )
    bundle = await adapter.fetch("637")
    assert by_id.called
    assert bundle["matched_by"] == "card"
    assert bundle["total_answers"] == 2295


@respx.mock
async def test_an_unknown_card_is_a_stub_without_a_degradation(adapter, respx_mock):
    respx_mock.get(f"{REST}/v1_companies").mock(return_value=httpx.Response(200, json=[]))
    degradation.begin()
    assert (await adapter.fetch("999999"))["is_stub"] is True
    assert (await adapter.fetch("not-a-number"))["is_stub"] is True
    assert degradation.collect() == []


# ----------------------------------------------------------------------
# Failures say so
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("response", "words"),
    [
        (httpx.Response(401, json=_fixture("rest_401.json")), "HTTP 401"),
        (httpx.Response(404, json=_fixture("rest_404_no_profile.json")), "HTTP 404"),
        (httpx.Response(503, json={"code": "57014"}), "HTTP 503"),
        (
            httpx.Response(200, text="<!DOCTYPE html>", headers={"content-type": "text/html"}),
            "text/html instead of JSON",
        ),
        (httpx.Response(200, json={"message": "?"}), "a JSON object where rows were expected"),
    ],
)
@respx.mock
async def test_a_failed_lookup_is_a_degradation_not_no_record(
    adapter, respx_mock, response, words
):
    respx_mock.get(f"{REST}/v1_companies").mock(return_value=response)
    degradation.begin()
    assert await adapter.fetch_by_lei(LEI, qid=QID) is None
    recorded = degradation.unanswered_fetch("wikirate")
    degradation.collect()
    assert recorded is not None
    assert recorded.detail.startswith("Wikirate's REST API did not answer")
    assert words in recorded.detail
    assert TOKEN not in recorded.detail and PK not in recorded.detail
    assert LEI not in recorded.detail


@respx.mock
async def test_a_count_without_a_total_is_a_read_failure(adapter, respx_mock):
    _mock_bp(respx_mock)
    respx_mock.get(f"{REST}/v1_answers", params={"select": "id", "limit": "1"}).mock(
        return_value=httpx.Response(200, json=[{"id": 1}], headers={"content-range": "0-0/*"})
    )
    degradation.begin()
    bundle = await adapter.fetch_by_lei(LEI)
    recorded = degradation.collect()
    assert bundle["total_answers"] == 0
    assert [(d.check, d.source_id) for d in recorded] == [
        (degradation.CHECK_SOURCE_READ, "wikirate")
    ]


@respx.mock
async def test_a_failure_is_never_cached(rest, respx_mock):
    a = WikirateAdapter()
    stored: list[str] = []
    a._cache.get_payload = lambda *args, **kwargs: None  # type: ignore[method-assign]
    a._cache.put = lambda key, payload: stored.append(key)  # type: ignore[method-assign]
    respx_mock.get(f"{REST}/v1_companies").mock(return_value=httpx.Response(503))
    assert await a.fetch_by_lei(LEI) is None
    assert stored == []


@respx.mock
async def test_a_success_is_cached_under_the_rest_namespace(rest, respx_mock):
    a = WikirateAdapter()
    stored: dict[str, object] = {}
    a._cache.get_payload = lambda *args, **kwargs: None  # type: ignore[method-assign]
    a._cache.put = lambda key, payload: stored.__setitem__(key, payload)  # type: ignore[method-assign]
    _mock_bp(respx_mock)
    assert await a.fetch_by_lei(LEI) is not None
    assert stored and all(k.startswith("wikirate-rest/") for k in stored)
    # No credential in a cache key.
    assert not any(TOKEN in k or PK in k for k in stored)
