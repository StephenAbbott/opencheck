"""Related-party person screening through one batched OpenSanctions /match.

The batched-/match ticket (10 Nov 2026): ``cross_check`` sent one
``GET /search`` per related person; it now sends every related person as a
FollowTheMoney ``Person`` example in one ``POST /match/default``. What these
tests pin:

* one request for all the people of a lookup (chunks of 100, OpenSanctions'
  limit), entities still searched by name;
* the request: JSON body, ``cutoff=0`` (OpenSanctions' score orders the
  candidates and never filters them), ``logic-v2``, the risk topics;
* OpenSanctions' ``score`` / ``match`` decide nothing — the 0.88 similarity,
  single-token, birth-year and patronymic gates still do;
* a failed batch, or one refused query inside it, is a degraded source for
  exactly those names, never a clean screen;
* each answer is cached on its own, so a repeat lookup sends nothing.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.cross_check import NameScreen, assess_cross_source_names
from opencheck.risk import DEGRADED_RATE_LIMITED, DEGRADED_UPSTREAM_ERROR
from opencheck.sources import REGISTRY, SearchKind, SourceHit
from opencheck.sources.opensanctions import (
    _MATCH_PARAMS,
    _TOPIC_PARAMS,
    MATCH_BATCH_SIZE,
    OpenSanctionsAdapter,
    person_match_example,
)

_API = "https://api.opensanctions.org"
_MATCH_URL = f"{_API}/match/default?{_MATCH_PARAMS}"


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _NoPolitician:
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []


@pytest.fixture
def adapter(monkeypatch) -> OpenSanctionsAdapter:
    a = OpenSanctionsAdapter()
    monkeypatch.setitem(REGISTRY, "opensanctions", a)
    monkeypatch.setitem(REGISTRY, "everypolitician", _NoPolitician())
    return a


def _person(sid: str, name: str, *, birth: str | None = None,
            nationality: str | None = None) -> dict[str, Any]:
    rd: dict[str, Any] = {
        "personType": "knownPerson",
        "names": [{"type": "legal", "fullName": name}],
    }
    if birth:
        rd["birthDate"] = birth
    if nationality:
        rd["nationalities"] = [{"name": nationality, "code": nationality}]
    return {"statementId": sid, "recordType": "person", "recordDetails": rd}


def _entity(sid: str, name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": name},
    }


def _result(rid: str, caption: str, topics: list[str], *, score: float = 0.99,
            birth: str | None = None) -> dict[str, Any]:
    props: dict[str, Any] = {"name": [caption], "topics": topics}
    if birth:
        props["birthDate"] = [birth]
    # /match puts topics under ``properties`` only — no top-level key.
    return {
        "id": rid,
        "schema": "Person",
        "caption": caption,
        "properties": props,
        "datasets": ["us_ofac_sdn"],
        "referents": [],
        "score": score,
        "match": score >= 0.7,
        "explanations": {},
    }


def _bodies(httpx_mock: HTTPXMock) -> list[dict[str, Any]]:
    return [
        json.loads(r.content)
        for r in httpx_mock.get_requests()
        if r.method == "POST"
    ]


# ---------------------------------------------------------------------
# The query
# ---------------------------------------------------------------------


def test_example_carries_name_birth_year_and_alpha2_nationalities() -> None:
    ex = person_match_example("  Vladimir   Putin ", 1952, ("RU", "Russia", "gb"))
    assert ex == {
        "schema": "Person",
        "properties": {
            "name": ["Vladimir Putin"],
            "birthDate": ["1952"],
            "nationality": ["gb", "ru"],
        },
    }
    assert person_match_example("") is None
    # A name Lucene would choke on goes as JSON, untouched.
    assert person_match_example('Moshe "Mo" Cohen')["properties"]["name"] == [
        'Moshe "Mo" Cohen'
    ]


def test_params_return_every_scored_candidate() -> None:
    assert "cutoff=0.0" in _MATCH_PARAMS
    assert "algorithm=logic-v2" in _MATCH_PARAMS
    assert _MATCH_PARAMS.endswith(_TOPIC_PARAMS)


# ---------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------


async def test_one_request_for_many_people_and_hits_shaped_like_search(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(
        url=_MATCH_URL,
        method="POST",
        match_headers={"Authorization": "ApiKey test-key"},
        json={
            "responses": {
                "a": {"status": 200, "results": [_result("NK-a", "Ann Example", ["sanction"])]},
                "b": {"status": 200, "results": []},
            },
            "limit": 10,
        },
    )
    out = await adapter.match_persons(
        {"a": {"name": "Ann Example", "birth_year": 1960}, "b": {"name": "Bob Example"}}
    )
    assert [b["queries"].keys() for b in _bodies(httpx_mock)] == [{"a", "b"}]
    hits = out["a"]
    assert isinstance(hits, list) and hits[0].hit_id == "NK-a"
    assert hits[0].kind == SearchKind.PERSON
    assert hits[0].identifiers["opensanctions_id"] == "NK-a"
    assert out["b"] == []

    # Each answer is cached on its own: the same people again send nothing.
    again = await adapter.match_persons(
        {"x": {"name": "Ann Example", "birth_year": 1960}, "y": {"name": "Bob Example"}}
    )
    assert len(_bodies(httpx_mock)) == 1
    assert again["x"][0].hit_id == "NK-a" and again["y"] == []


async def test_more_than_a_batch_is_chunked(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    def reply(request):
        import httpx

        queries = json.loads(request.content)["queries"]
        assert len(queries) <= MATCH_BATCH_SIZE
        return httpx.Response(
            200, json={"responses": {k: {"status": 200, "results": []} for k in queries}}
        )

    httpx_mock.add_callback(reply, url=_MATCH_URL, method="POST", is_reusable=True)
    n = MATCH_BATCH_SIZE + 3
    out = await adapter.match_persons(
        {str(i): {"name": f"Person Number{i} Example"} for i in range(n)}
    )
    assert len(_bodies(httpx_mock)) == 2
    assert all(v == [] for v in out.values()) and len(out) == n


async def test_a_refused_query_fails_that_name_only(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    import httpx

    httpx_mock.add_response(
        url=_MATCH_URL,
        method="POST",
        json={"responses": {"a": {"status": 200, "results": []}, "b": {"status": 429}}},
    )
    out = await adapter.match_persons({"a": {"name": "Ann Example"}, "b": {"name": "Bob Example"}})
    assert out["a"] == []
    assert isinstance(out["b"], httpx.HTTPStatusError)
    assert out["b"].response.status_code == 429


async def test_a_failed_request_fails_every_name_in_it(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    import httpx

    httpx_mock.add_response(url=_MATCH_URL, method="POST", status_code=503)
    out = await adapter.match_persons({"a": {"name": "Ann Example"}, "b": {"name": "Bob Example"}})
    assert all(isinstance(v, httpx.HTTPStatusError) for v in out.values())


# ---------------------------------------------------------------------
# cross_check through the batch
# ---------------------------------------------------------------------


async def test_people_go_in_one_batch_and_entities_by_name(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    def reply(request):
        import httpx

        queries = json.loads(request.content)["queries"]
        by_name = {"Ann Example": [_result("NK-ann", "Ann Example", ["sanction"], birth="1960")]}
        return httpx.Response(200, json={"responses": {
            k: {"status": 200, "results": by_name.get(q["properties"]["name"][0], [])}
            for k, q in queries.items()
        }})

    httpx_mock.add_callback(reply, url=_MATCH_URL, method="POST")
    httpx_mock.add_response(
        url=f"{_API}/search/default?q=Acme%20Holdings%20Ltd&schema=LegalEntity&limit=10&{_TOPIC_PARAMS}",
        json={"results": []},
    )
    bods = [
        _person("p1", "Ann Example", birth="1960-02-01", nationality="GB"),
        _person("p2", "Bob Example"),
        _person("p3", "Cleo Example"),
        _entity("e1", "Acme Holdings Ltd"),
    ]
    screen = NameScreen()
    degraded: list = []
    signals = await assess_cross_source_names(bods, degraded=degraded, screen=screen)

    bodies = _bodies(httpx_mock)
    assert len(bodies) == 1
    names = sorted(q["properties"]["name"][0] for q in bodies[0]["queries"].values())
    assert names == ["Ann Example", "Bob Example", "Cleo Example"]
    ann = next(q for q in bodies[0]["queries"].values() if q["properties"]["name"] == ["Ann Example"])
    assert ann["properties"]["birthDate"] == ["1960"] and ann["properties"]["nationality"] == ["gb"]
    assert [r.method for r in httpx_mock.get_requests()].count("GET") == 1

    assert screen.names_screened == 4
    assert degraded == []
    [sig] = signals
    assert sig.code == "RELATED_SANCTIONED"
    assert sig.evidence["subject_statement_id"] == "p1"
    # Birth year agrees on both sides → corroborated, by OpenCheck's ladder.
    assert sig.evidence["corroboration"] == ["birth_year"]
    assert sig.confidence == "high"


async def test_opensanctions_match_flag_decides_nothing(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    """Three candidates OpenSanctions calls a match at 0.99; OpenCheck's own
    gates drop all three, and the one it keeps is graded by its own ladder."""
    httpx_mock.add_response(
        url=_MATCH_URL,
        method="POST",
        json={
            "responses": {
                "0": {
                    "status": 200,
                    "results": [
                        # Below 0.88 similarity.
                        _result("NK-far", "Michaela Gordonova Example", ["sanction"]),
                        # Birth years conflict.
                        _result("NK-old", "Michael Gordon", ["sanction"], birth="1930"),
                        # Kept — but name only, so never above medium.
                        _result("NK-ok", "Michael Gordon", ["role.pep"], score=0.71),
                    ],
                }
            }
        },
    )
    signals = await assess_cross_source_names([_person("p1", "Michael Gordon", birth="1975")])
    assert [(s.hit_id, s.code) for s in signals] == [("NK-ok", "RELATED_PEP")]
    assert signals[0].confidence == "medium"
    assert signals[0].evidence["name_match_only"] is True


async def test_single_token_person_is_still_gated(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(
        url=_MATCH_URL,
        method="POST",
        json={"responses": {"0": {"status": 200, "results": [_result("NK-i", "Ivanov", ["sanction"])]}}},
    )
    assert await assess_cross_source_names([_person("p1", "Ivanov")]) == []


@pytest.mark.parametrize(
    ("reply", "reason"),
    [
        ({"status_code": 429}, DEGRADED_RATE_LIMITED),
        ({"status_code": 500}, DEGRADED_UPSTREAM_ERROR),
        ({"json": {"responses": {"0": {"status": 200, "results": []}, "1": {"status": 429}}}},
         DEGRADED_RATE_LIMITED),
    ],
)
async def test_a_failed_batch_is_degraded_never_clean(
    adapter: OpenSanctionsAdapter, httpx_mock: HTTPXMock, reply, reason
) -> None:
    httpx_mock.add_response(url=_MATCH_URL, method="POST", **reply)
    degraded: list = []
    signals = await assess_cross_source_names(
        [_person("p1", "Ann Example"), _person("p2", "Bob Example")], degraded=degraded
    )
    assert signals == []
    [record] = [d for d in degraded if d.source_id == "opensanctions"]
    assert record.check == "cross_source_names"
    assert record.reason == reason
    failed = 1 if "json" in reply else 2
    assert record.detail.startswith(f"Search failed for {failed} of 2 ")
