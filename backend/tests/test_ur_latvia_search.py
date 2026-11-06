"""ur_latvia name search (the "ur_latvia name search stubs" ticket, 5 Nov 2026).

data.gov.lv dropped CKAN's ``datastore_search_sql`` action (HTTP 400 "Action
name not known: datastore_search_sql") by 3 Nov 2026, and the adapter fell back
to its stub without a word, so ``/search?q=Latvenergo`` carried no Latvian
register candidate while the weekly sweep (which exercises ``fetch``) read ok.
The old test mocked the action the portal no longer serves; these pin the
request the portal does serve, and that a failed search is an error.

``fixtures/ur_latvia/search_latvenergo.json`` is the portal's real answer to
:func:`search_url("Latvenergo")` on 5 Nov 2026 (34 rows, AS "Latvenergo"
eighth in CKAN's own order).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from opencheck.cache import Cache
from opencheck import search_rank
from opencheck.config import get_settings
from opencheck.mcp import shaping
from opencheck.routers import search as search_router
from opencheck.sources import ur_latvia as lv
from opencheck.sources.base import SearchKind, SourceHit

FIXTURE = Path(__file__).parent / "fixtures" / "ur_latvia" / "search_latvenergo.json"
LATVENERGO = "40003032949"

SQL_REFUSAL = {
    "help": "https://data.gov.lv/dati/api/3/action/help_show?name=datastore_search_sql",
    "success": False,
    "error": {"message": "Action name not known: datastore_search_sql", "__type": "Bad request"},
}


def _answer() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def adapter(tmp_path) -> lv.UrLatviaAdapter:
    a = lv.UrLatviaAdapter()
    a._cache = Cache(root=tmp_path)
    return a


# ---------------------------------------------------------------------------
# The request shape
# ---------------------------------------------------------------------------


def test_the_search_asks_datastore_search_with_a_name_scoped_q():
    url = urlsplit(lv.search_url('AS "Latvenergo"'))
    params = parse_qs(url.query)
    assert url.netloc == "data.gov.lv"
    assert url.path == "/dati/api/3/action/datastore_search"
    assert params["resource_id"] == [lv._RES_BUSINESS]
    assert json.loads(params["q"][0]) == {"name": 'AS "Latvenergo"'}
    assert params["limit"] == ["100"]
    assert params["fields"][0].split(",")[:2] == ["regcode", "name"]
    assert "sql" not in params
    assert "datastore_search_sql" not in lv.search_url("x")


def test_the_adapter_no_longer_names_the_dropped_action():
    source = Path(lv.__file__).read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "datastore_search_sql?" not in code


async def test_search_sends_exactly_search_url(live, adapter, httpx_mock):
    httpx_mock.add_response(url=lv.search_url("Latvenergo"), json=_answer())
    hits = await adapter.search("  Latvenergo ", SearchKind.ENTITY)
    assert hits
    [request] = httpx_mock.get_requests()
    assert str(request.url) == lv.search_url("Latvenergo")


# ---------------------------------------------------------------------------
# Ranking the page CKAN returns
# ---------------------------------------------------------------------------


async def test_the_company_itself_comes_before_its_branches_and_union_locals(live, adapter, httpx_mock):
    httpx_mock.add_response(url=lv.search_url("Latvenergo"), json=_answer())
    hits = await adapter.search("Latvenergo", SearchKind.ENTITY)
    assert hits[0].hit_id == LATVENERGO
    assert hits[0].name == 'Akciju sabiedrība "Latvenergo"'
    assert hits[0].summary.startswith(f"LV {LATVENERGO} · active")
    assert hits[0].identifiers == {"lv_regcode": LATVENERGO}
    assert not hits[0].is_stub
    assert len(hits) == lv._SEARCH_LIMIT
    # CKAN's own order put a branch first.
    assert str(_answer()["result"]["records"][0]["regcode"]) != LATVENERGO


def test_rank_live_before_closed_and_legal_persons_before_branches():
    rows = [
        {"regcode": 1, "name": 'SIA "Beta" filiāle "Alfa"', "name_in_quotes": "Alfa", "type": "FIL", "closed": " "},
        {"regcode": 2, "name": 'SIA "Alfa"', "name_in_quotes": "Alfa", "type": "SIA", "closed": "L"},
        {"regcode": 3, "name": 'Biedrība "Alfa draugi"', "name_in_quotes": "Alfa draugi", "type": "BDR", "closed": " "},
        {"regcode": 4, "name": 'SIA "Alfa"', "name_in_quotes": "Alfa", "type": "SIA", "closed": " ", "terminated": None},
    ]
    # Exact bare name first: the live company, its live branch, the closed
    # company; then the partial match.
    assert [r["regcode"] for r in lv.rank_records("Alfa", rows)] == [4, 1, 2, 3]


def test_the_bare_name_ranks_where_the_legal_form_would_hide_it():
    rows = [
        {"regcode": 1, "name": '"Air Baltic Training", SIA', "name_in_quotes": '"Air Baltic Training",', "type": "SIA", "closed": " "},
        {"regcode": 2, "name": "Air Baltic Corporation AS", "name_in_quotes": "Air Baltic Corporation", "type": "AS", "closed": " "},
    ]
    assert lv.rank_records("Air Baltic Corporation", rows)[0]["regcode"] == 2


async def test_a_blank_query_asks_nothing(live, adapter):
    assert await adapter.search("   ", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# A failed search is an error, never the stub
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(400, json=SQL_REFUSAL),
        httpx.Response(503, text="Service Unavailable"),
        httpx.Response(200, text="<html>maintenance</html>"),
        httpx.Response(200, json={"success": False, "error": {"message": "x"}}),
        httpx.Response(200, json={"success": True, "result": {}}),
    ],
    ids=["dropped-action", "5xx", "html", "success-false", "no-records"],
)
async def test_a_failed_search_raises_and_caches_nothing(live, adapter, httpx_mock, response):
    httpx_mock.add_callback(lambda request: response, is_reusable=True)
    with pytest.raises(lv.UrLatviaSearchError) as err:
        await adapter.search("Latvenergo", SearchKind.ENTITY)
    assert "data.gov.lv" in str(err.value)
    assert "http" not in str(err.value).lower().replace("http ", "")  # no URL
    assert not adapter._cache.has(f"{lv._CACHE_NS}/search-v2/{lv._slug('Latvenergo')}")


async def test_an_empty_answer_is_no_match_not_an_error(live, adapter, httpx_mock):
    httpx_mock.add_response(
        url=lv.search_url("Nonexistent"),
        json={"success": True, "result": {"records": [], "total": 0}},
    )
    assert await adapter.search("Nonexistent", SearchKind.ENTITY) == []


async def test_search_route_reports_the_register_as_not_answering(live, httpx_mock, monkeypatch, tmp_path):
    a = lv.UrLatviaAdapter()
    a._cache = Cache(root=tmp_path)
    monkeypatch.setattr(
        search_router, "_plan", lambda kind, filters: ([("ur_latvia", a, False)], None)
    )
    httpx_mock.add_response(status_code=400, json=SQL_REFUSAL)
    results, errors = await search_router._run_adapters("Latvenergo", SearchKind.ENTITY)
    assert results == {"ur_latvia": []}
    assert "ur_latvia" in errors
    assert "UrLatviaSearchError" in errors["ur_latvia"]
    assert "stub" not in json.dumps(errors).lower()


async def test_offline_without_a_cache_is_still_the_labelled_stub(adapter, monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    try:
        [hit] = await adapter.search("Latvenergo", SearchKind.ENTITY)
    finally:
        get_settings.cache_clear()
    assert hit.is_stub


# ---------------------------------------------------------------------------
# Cross-source ranking reads the register's bare name (6 Nov 2026 follow-up)
# ---------------------------------------------------------------------------
#
# Production /search?q=Latvenergo on 6 Nov 2026 put AS "Latvenergo" tenth of
# 33: its full name 'Akciju sabiedrība "Latvenergo"' was "same_name" while
# four other sources' "Latvenergo" records were "exact". The hits below are
# those records' names and order as served that day.


def _hit(source_id: str, hit_id: str, name: str, raw: dict | None = None, **kw) -> SourceHit:
    return SourceHit(
        source_id=source_id, hit_id=hit_id, kind=SearchKind.ENTITY, name=name,
        summary=kw.pop("summary", ""), raw=raw or {}, is_stub=False, **kw,
    )


def _latvenergo_hits() -> list[SourceHit]:
    [rec] = [r for r in _answer()["result"]["records"] if str(r["regcode"]) == LATVENERGO]
    return [
        _hit("opensanctions", "gem-own-e100000", "Latvenergo"),
        _hit("openaleph", "oa-1", "Latvenergo"),
        _hit("wikidata", "Q3736450", "Latvenergo"),
        _hit("openaleph", "oa-2", "AS “Latvenergo”"),
        _hit("climatetrace", "E100000001565", "Latvenergo AS"),
        _hit("gleif", "213800DJRB539Q1EMW75", 'Akciju sabiedrība "Latvenergo"'),
        _hit("ur_latvia", LATVENERGO, rec["name"], raw=rec,
             summary=f"LV {LATVENERGO} · active · Akciju sabiedrība (Joint-stock company)"),
    ]


def test_the_register_hit_matches_exactly_on_its_bare_name():
    reg = _latvenergo_hits()[-1]
    assert search_rank.bare_names_of_hit(reg) == ["Latvenergo"]
    tier, matched_former = search_rank.best_match("Latvenergo", reg)
    assert search_rank.MATCH_TIERS[tier] == "exact"
    assert matched_former is None  # the current name, never a former one


def test_the_register_hit_joins_the_exact_matches_in_search():
    ranked = search_rank.rank_hits("Latvenergo", _latvenergo_hits())
    position = [h.source_id for h in ranked].index("ur_latvia")
    exact = [
        h for h in ranked
        if search_rank.MATCH_TIERS[search_rank.best_match("Latvenergo", h)[0]] == "exact"
    ]
    assert ranked[position] in exact
    assert position < [h.source_id for h in ranked].index("climatetrace")


def test_the_mcp_candidate_says_exact_and_names_no_former_name():
    payload = SimpleNamespace(query="Latvenergo", kind=SearchKind.ENTITY, hits=_latvenergo_hits(), filters=None)
    [cand] = [c for c in shaping.shape_search(payload)["candidates"] if c["source"] == "ur_latvia"]
    assert cand["match"] == "exact"
    assert "matched_former_name" not in cand
    assert cand["name"] == 'Akciju sabiedrība "Latvenergo"'


@pytest.mark.parametrize(
    "source_id,raw",
    [
        ("gleif", {"name_in_quotes": "Latvenergo"}),   # another source's field is not read
        ("ur_latvia", {"name_in_quotes": "   "}),
        ("ur_latvia", {}),
        ("ur_latvia", {"name_in_quotes": 'Akciju sabiedrība "Latvenergo"'}),  # same as the name
    ],
)
def test_no_bare_name_unless_the_register_publishes_a_different_one(source_id, raw):
    hit = _hit(source_id, "x", 'Akciju sabiedrība "Latvenergo"', raw=raw)
    assert search_rank.bare_names_of_hit(hit) == []


def test_the_bare_name_is_tidied_of_the_full_names_punctuation():
    assert search_rank.clean_bare_name(' "Air Baltic Training", ') == "Air Baltic Training"
