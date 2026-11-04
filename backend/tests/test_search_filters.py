"""Search filters (the advanced-search-filters ticket).

Pins the decisions of 3 Nov 2026: a source that cannot apply an active filter
is left out and named; the jurisdiction filter is exact (GLEIF's matches each
part of a code, so ``PA`` also returns Pennsylvania); legal forms are scoped
to a jurisdiction; the GLEIF parameter is ``entity.legalForm``, never
``entity.legalForm.id`` (HTTP 400); and the website's copies of the data and
the words agree with the backend's.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import search_filters as sf
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.gleif import GleifAdapter

ROOT = Path(__file__).resolve().parents[2]
_API = "https://api.gleif.org/api/v1"


@pytest.fixture
def live(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Any:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _record(lei: str, name: str, jurisdiction: str, form: str = "8888", **extra: str) -> dict[str, Any]:
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "jurisdiction": jurisdiction,
                "legalAddress": {"country": jurisdiction.split("-")[0]},
                "legalForm": {"id": form},
                "status": extra.get("status", "ACTIVE"),
                "category": extra.get("category", "GENERAL"),
            },
            "registration": {"status": extra.get("registration", "ISSUED")},
        },
    }


# --- the generated data ------------------------------------------------------


def test_the_website_copy_of_the_jurisdictions_is_the_backends() -> None:
    backend = (ROOT / "backend/opencheck/data/gleif_jurisdictions.json").read_text()
    frontend = (ROOT / "frontend/src/lib/gleifJurisdictions.json").read_text()
    assert json.loads(backend) == json.loads(frontend), (
        "re-run scripts/build_search_filter_data.py — it writes both copies"
    )


def test_the_clash_table_is_derived_from_the_accepted_jurisdictions() -> None:
    codes = sf.jurisdictions()
    derived: dict[str, list[str]] = {}
    for code in codes:
        if "-" in code:
            parent, suffix = code.split("-", 1)
            if suffix in codes and suffix != parent:
                derived.setdefault(suffix, []).append(code)
    clash = sf.clashes()
    assert clash == {k: sorted(v) for k, v in derived.items()}
    # The cases measured live on 3 Nov 2026.
    assert clash["DE"] == ["US-DE"]
    assert clash["PA"] == ["US-PA"]
    assert clash["KY"] == ["US-KY"]
    assert clash["SK"] == ["CA-SK"]
    assert clash["AZ"] == ["AE-AZ", "US-AZ"]
    assert clash["SH"] == ["AE-SH"]
    assert "GB" not in clash and "US" not in clash and "FR" not in clash


def test_the_elf_list_is_active_forms_with_abbreviations() -> None:
    idx = sf.elf_index()
    assert len(idx) >= 3000
    assert "8888" not in idx and "9999" not in idx
    assert "GmbH" in idx["2HBR"]["abbreviations"]
    assert idx["HZEH"]["subdivision"] == "US-DE"
    assert idx["2HBR"]["country"] == "DE" and idx["2HBR"]["subdivision"] is None


@pytest.mark.parametrize(
    ("code", "abbr"),
    [
        ("2HBR", "GmbH"),
        ("8Z6G", "KG"),  # not "GmbH & Co. KG"
        ("63KS", "UG"),  # not "GmbH UG", parenthesised tail dropped
        ("QZ3L", "e.V."),
        ("H0PO", "Ltd"),  # not the Welsh "cyf"
        ("Z0EY", "LLP"),  # not the spelled-out form beside it
        ("HZEH", None),  # US forms carry no abbreviation
    ],
)
def test_the_displayed_abbreviation(code: str, abbr: str | None) -> None:
    assert sf.display_abbreviation(sf.elf_index()[code]) == abbr


# --- parsing -----------------------------------------------------------------


def test_parse_normalises_and_validates() -> None:
    f = sf.parse_filters(
        jurisdiction="us-de",
        legal_form="hzeh",
        entity_status="active",
        registration_status="issued, lapsed",
        category="fund",
    )
    assert f.jurisdiction == "US-DE"
    assert f.legal_form == ("HZEH",)
    assert f.registration_status == ("ISSUED", "LAPSED")
    assert f.active() == ["jurisdiction", "legal_form", "entity_status", "registration_status", "category"]
    assert not sf.parse_filters()


def test_no_elf_code_means_both_placeholders() -> None:
    assert sf.parse_filters(legal_form="no-elf-code").legal_form == ("8888", "9999")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"jurisdiction": "QQ"},
        {"legal_form": "ZZZZ"},
        {"jurisdiction": "FR", "legal_form": "2HBR"},  # a German form
        {"entity_status": "DISSOLVED"},
        {"registration_status": "EXPIRED"},
        {"category": "TRUST"},
        {"nationality": "GBR"},
        {"birth_year": "1700"},
        {"birth_year": "nineteen"},
    ],
)
def test_parse_refuses_bad_values(kwargs: dict[str, str]) -> None:
    with pytest.raises(sf.FilterError):
        sf.parse_filters(**kwargs)


def test_jurisdiction_matching_is_exact_or_own_subdivision() -> None:
    assert sf.jurisdiction_matches("US-DE", "US")
    assert sf.jurisdiction_matches("US-DE", "US-DE")
    assert sf.jurisdiction_matches("DE", "DE")
    assert not sf.jurisdiction_matches("US-DE", "DE")
    assert not sf.jurisdiction_matches("US-PA", "PA")
    assert not sf.jurisdiction_matches(None, "DE")


def test_when_the_scan_is_needed() -> None:
    assert sf.needs_exact_jurisdiction(sf.parse_filters(jurisdiction="PA"))
    assert sf.needs_exact_jurisdiction(sf.parse_filters(jurisdiction="DE", legal_form="no-elf-code"))
    assert not sf.needs_exact_jurisdiction(sf.parse_filters(jurisdiction="DE", legal_form="2HBR"))
    assert not sf.needs_exact_jurisdiction(sf.parse_filters(jurisdiction="GB"))
    assert not sf.needs_exact_jurisdiction(sf.parse_filters(jurisdiction="US-DE"))


def test_the_gleif_parameter_is_legal_form_not_legal_form_id() -> None:
    params = dict(sf.gleif_filter_params(sf.parse_filters(jurisdiction="DE", legal_form="2HBR,6QQB")))
    assert params == {"filter[entity.jurisdiction]": "DE", "filter[entity.legalForm]": "2HBR,6QQB"}
    assert not any(".id]" in k for k in params)


# --- legal forms ---------------------------------------------------------------


def test_forms_for_a_country_most_used_first() -> None:
    payload = sf.forms_for("de", counts={"2HBR": 109_065, "6QQB": 3_742, "8Z6G": 22_768})
    codes = [f["code"] for f in payload["forms"]]
    assert codes[:3] == ["2HBR", "8Z6G", "6QQB"]
    assert len(codes) == 31
    assert payload["counts_available"] and not payload["has_subdivision_forms"]


def test_us_forms_wait_for_a_state() -> None:
    country = sf.forms_for("US")
    assert country["has_subdivision_forms"]
    assert "HZEH" not in {f["code"] for f in country["forms"]}
    delaware = sf.forms_for("US-DE")
    assert "HZEH" in {f["code"] for f in delaware["forms"]}
    assert not delaware["counts_available"]


def test_resolve_legal_form() -> None:
    assert sf.resolve_legal_form("GmbH", "DE") == (
        ["2HBR"],
        [{"code": "2HBR", "jurisdiction": "DE", "name": "Gesellschaft mit beschränkter Haftung", "abbreviation": "GmbH"}],
    )
    codes, candidates = sf.resolve_legal_form("GmbH", None)
    assert codes == [] and {"AT", "CH", "DE", "LI"} <= {c["jurisdiction"] for c in candidates}
    codes, candidates = sf.resolve_legal_form("Ltd", "GB")  # three UK forms list "Ltd"
    assert codes == [] and len(candidates) >= 2
    assert sf.resolve_legal_form("2HBR", None) == (["2HBR"], [])


# --- which source runs ------------------------------------------------------------


def test_filter_plan() -> None:
    ch = REGISTRY["companies_house"]
    gleif = REGISTRY["gleif"]
    gb = sf.parse_filters(jurisdiction="GB")
    assert sf.filter_plan(ch, "entity", gb).run  # every CH record is GB
    fr = sf.parse_filters(jurisdiction="FR")
    plan = sf.filter_plan(ch, "entity", fr)
    assert not plan.run and plan.reason == "outside_jurisdiction"
    form = sf.parse_filters(jurisdiction="GB", legal_form="H0PO")
    plan = sf.filter_plan(ch, "entity", form)
    assert not plan.run and plan.reason == "cannot_apply" and plan.unsupported == ["legal_form"]
    assert sf.filter_plan(gleif, "entity", form).filtered
    # Entity filters do not touch a person search, and the reverse.
    assert sf.filter_plan(ch, "person", form) == sf.FilterPlan(run=True, filtered=False)
    plan = sf.filter_plan(ch, "person", sf.parse_filters(birth_year=1960))
    assert not plan.run and plan.unsupported == ["birth_year"]
    assert sf.filter_plan(REGISTRY["opensanctions"], "person", sf.parse_filters(birth_year=1960)).filtered


def test_a_subnational_register_is_not_taken_for_another_state() -> None:
    dc = REGISTRY["dlcp_dc"]  # country "US"
    assert sf.filter_plan(dc, "entity", sf.parse_filters(jurisdiction="US")).run
    assert not sf.filter_plan(dc, "entity", sf.parse_filters(jurisdiction="US-DE")).run


# --- the GLEIF read ------------------------------------------------------------------


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(str(url)).query)


async def test_a_clashing_jurisdiction_is_scanned_and_made_exact(live: Any, httpx_mock: HTTPXMock) -> None:
    rows = [_record(f"PA{i:018d}", f"Panama Holdings {i}", "PA") for i in range(4)] + [
        _record(f"US{i:018d}", f"Pennsylvania Holdings {i}", "US-PA") for i in range(6)
    ]
    httpx_mock.add_response(
        url=re.compile(r".*page%5Bnumber%5D=1$|.*page\[number\]=1$"),
        json={"data": rows, "meta": {"pagination": {"lastPage": 1}}},
    )
    hits = await GleifAdapter().search_filtered(
        "holdings", SearchKind.ENTITY, sf.parse_filters(jurisdiction="PA")
    )
    assert [h.name for h in hits] == [f"Panama Holdings {i}" for i in range(4)]
    request = httpx_mock.get_requests()[0]
    q = _query(request.url)
    assert q["filter[entity.jurisdiction]"] == ["PA"]
    assert q["page[size]"] == [str(sf.EXACT_SCAN_PAGE)]


async def test_the_scan_stops_at_its_limit(live: Any, httpx_mock: HTTPXMock) -> None:
    pennsylvania = [_record(f"US{i:018d}", "Pennsylvania", "US-PA") for i in range(3)]
    httpx_mock.add_response(
        url=re.compile(r".*"),
        json={"data": pennsylvania, "meta": {"pagination": {"lastPage": 99}}},
        is_reusable=True,
    )
    hits = await GleifAdapter().search_filtered(
        "holdings", SearchKind.ENTITY, sf.parse_filters(jurisdiction="PA")
    )
    assert hits == []
    assert len(httpx_mock.get_requests()) == sf.EXACT_SCAN_MAX_PAGES


async def test_an_unclashing_filter_is_one_page_of_ten_and_still_checked(
    live: Any, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(
        url=re.compile(r".*"),
        json={
            "data": [
                _record("GB000000000000000001", "Fine Ltd", "GB", "H0PO"),
                _record("GB000000000000000002", "Lapsed Ltd", "GB", "H0PO", registration="LAPSED"),
            ],
            "meta": {"pagination": {"lastPage": 1}},
        },
    )
    hits = await GleifAdapter().search_filtered(
        "ltd",
        SearchKind.ENTITY,
        sf.parse_filters(jurisdiction="GB", legal_form="H0PO", registration_status="ISSUED"),
    )
    assert [h.name for h in hits] == ["Fine Ltd"]
    q = _query(httpx_mock.get_requests()[0].url)
    assert q["page[size]"] == ["10"]
    assert q["filter[entity.legalForm]"] == ["H0PO"]
    assert q["filter[registration.status]"] == ["ISSUED"]


# --- people ----------------------------------------------------------------------------


async def test_opensanctions_person_filters_use_its_own_data(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = REGISTRY["opensanctions"]
    monkeypatch.setattr(type(adapter), "info", property(lambda self: SimpleNamespace(live_available=True)))

    async def fake_get(self: Any, path: str, cache_key: str) -> dict[str, Any]:
        assert "limit=50" in path
        return {
            "results": [
                {"id": "a", "caption": "Ivan One", "schema": "Person", "properties": {"birthDate": ["1952-10-07"], "nationality": ["ru"]}},
                {"id": "b", "caption": "Ivan Two", "schema": "Person", "properties": {"birthDate": ["1952"], "citizenship": ["ua"]}},
                {"id": "c", "caption": "Ivan Three", "schema": "Person", "properties": {"nationality": ["ru"]}},
            ]
        }

    monkeypatch.setattr(type(adapter), "_get", fake_get)
    both = await adapter.search_filtered("Ivan", SearchKind.PERSON, sf.parse_filters(birth_year=1952, nationality="RU"))
    assert [h.name for h in both] == ["Ivan One"]
    year = await adapter.search_filtered("Ivan", SearchKind.PERSON, sf.parse_filters(birth_year=1952))
    assert [h.name for h in year] == ["Ivan One", "Ivan Two"]  # no birth date is no match


async def test_wikidata_fails_closed_when_the_check_does_not_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = REGISTRY["wikidata"]
    from opencheck.sources.base import SourceHit

    async def fake_search(self: Any, query: str, kind: SearchKind) -> list[SourceHit]:
        return [SourceHit(source_id="wikidata", hit_id="Q7747", kind=kind, name="Vladimir Putin", summary="", is_stub=False)]

    async def no_answer(self: Any, query: str, *, cache_key: str) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(type(adapter), "search", fake_search)
    monkeypatch.setattr(type(adapter), "_sparql", no_answer)
    with pytest.raises(RuntimeError):
        await adapter.search_filtered("Putin", SearchKind.PERSON, sf.parse_filters(birth_year=1952))


# --- the routes and the MCP tool --------------------------------------------------------------


def test_search_refuses_a_bad_filter() -> None:
    from opencheck.app import app

    with TestClient(app) as client:
        r = client.get("/search", params={"q": "holdings", "jurisdiction": "QQ"})
    assert r.status_code == 400
    assert "jurisdiction" in r.json()["detail"]


def test_search_names_the_sources_left_out(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.app import app
    from opencheck.sources.base import SourceHit

    async def fake_filtered(self: Any, query: str, kind: SearchKind, filters: Any) -> list[SourceHit]:
        return [SourceHit(source_id="gleif", hit_id="X" * 20, kind=kind, name="Holdings GmbH", summary="DE", is_stub=False)]

    monkeypatch.setattr(GleifAdapter, "search_filtered", fake_filtered)
    with TestClient(app) as client:
        r = client.get("/search", params={"q": "holdings", "jurisdiction": "DE", "legal_form": "2HBR"})
    body = r.json()
    assert r.status_code == 200
    assert body["filters"]["applied"] == {"jurisdiction": "DE", "legal_form": ["2HBR"]}
    assert body["filters"]["applied_by"] == ["gleif"]
    left = {row["source_id"]: row for row in body["filters"]["left_out"]}
    assert left["companies_house"]["reason"] == "outside_jurisdiction"
    assert left["opencorporates"]["reason"] == "cannot_apply"
    assert [h["name"] for h in body["hits"]] == ["Holdings GmbH"]


def test_search_without_filters_has_no_filters_block(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.routers import search as search_mod

    async def fake_run(q: str, kind: SearchKind) -> tuple[dict[str, Any], dict[str, str]]:
        return {}, {}

    monkeypatch.setattr(search_mod, "_run_adapters", fake_run)
    from opencheck.app import app

    with TestClient(app) as client:
        r = client.get("/search", params={"q": "holdings"})
    assert r.status_code == 200 and r.json()["filters"] is None


def test_elf_forms_route() -> None:
    from opencheck.app import app

    with TestClient(app) as client:
        ok = client.get("/elf-forms", params={"jurisdiction": "DE"})
        bad = client.get("/elf-forms", params={"jurisdiction": "QQ"})
    assert ok.status_code == 200
    body = ok.json()
    assert body["jurisdiction_name"] == "Germany" and len(body["forms"]) == 31
    assert body["counts_available"] is False  # no mirror in the suite
    assert {"code", "name", "abbreviation", "abbreviations", "names", "issued_leis"} <= set(body["forms"][0])
    assert bad.status_code == 400


async def test_mcp_resolves_an_abbreviation_or_lists_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.mcp import server
    from opencheck.routers import search as search_mod

    seen: list[Any] = []

    async def fake_impl(q: str, kind: SearchKind, filters: Any = None) -> Any:
        seen.append(filters)
        return SimpleNamespace(query=q, kind=kind, hits=[], filters={"applied": filters.to_dict(), "applied_by": ["gleif"], "left_out": [{"source_id": "kvk", "reason": "cannot_apply", "filters": ["legal_form"]}]})

    monkeypatch.setattr(search_mod, "_search_impl", fake_impl)
    out = await server.opencheck_search("holdings", jurisdiction="DE", legal_form="GmbH")
    assert seen[-1].legal_form == ("2HBR",)
    assert out["filters"]["applied_by"] == ["gleif"] and "kvk" in out["hint"]

    ambiguous = await server.opencheck_search("holdings", legal_form="GmbH")
    assert "error" in ambiguous and {"AT", "DE"} <= {c["jurisdiction"] for c in ambiguous["candidates"]}

    bad = await server.opencheck_search("holdings", jurisdiction="QQ")
    assert "error" in bad


# --- the website mirrors the words -------------------------------------------------------------


def _ts_record(source: str, name: str) -> dict[str, str]:
    block = re.search(rf"export const {name}[^=]*=\s*\{{(.*?)\}};", source, re.S)
    assert block, f"{name} not found in searchFilters.ts"
    return dict(re.findall(r'(\w+):\s*"([^"]+)"', block.group(1)))


def test_the_website_uses_the_same_words_and_values() -> None:
    ts = (ROOT / "frontend/src/lib/searchFilters.ts").read_text()
    assert _ts_record(ts, "ENTITY_STATUS_WORDS") == sf.ENTITY_STATUSES
    assert _ts_record(ts, "REGISTRATION_STATUS_WORDS") == sf.REGISTRATION_STATUSES
    assert _ts_record(ts, "CATEGORY_WORDS") == sf.CATEGORIES
    assert re.search(rf"EXACT_SCAN_PAGE\s*=\s*{sf.EXACT_SCAN_PAGE}\b", ts)
    assert re.search(rf"EXACT_SCAN_MAX_PAGES\s*=\s*{sf.EXACT_SCAN_MAX_PAGES}\b", ts)
    assert '"filter[entity.legalForm]"' in ts
    assert '"filter[entity.legalForm.id]"' not in ts
