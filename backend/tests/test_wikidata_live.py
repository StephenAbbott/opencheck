"""Live Wikidata adapter tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.wikidata import WikidataAdapter

_SPARQL_RE = re.compile(r"^https://query\.wikidata\.org/sparql.*")


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------


async def test_search_returns_wbsearchentities_hits(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=(
            "https://www.wikidata.org/w/api.php"
            "?action=wbsearchentities&search=putin&language=en"
            "&format=json&type=item&limit=10"
        ),
        json={
            "search": [
                {
                    "id": "Q7747",
                    "label": "Vladimir Putin",
                    "description": "president of Russia (1999–2008, 2012–)",
                    "match": {"type": "label"},
                },
                {
                    "id": "Q34020",
                    "label": "Vladimir Putin",
                    "description": "Russian actor (b. 1971)",
                },
            ]
        },
    )

    adapter = WikidataAdapter()
    hits = await adapter.search("putin", SearchKind.PERSON)

    assert len(hits) == 2
    first = hits[0]
    assert first.is_stub is False
    assert first.hit_id == "Q7747"
    assert first.name == "Vladimir Putin"
    assert first.identifiers["wikidata_qid"] == "Q7747"
    assert "president" in first.summary


async def test_search_stub_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = WikidataAdapter()
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True
    assert hits[0].hit_id == "Q0"


# ---------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------


async def test_fetch_summarises_person_bindings(httpx_mock: HTTPXMock) -> None:
    # Simulate a SPARQL response for a Person with two citizenships and
    # two positions — we expect the summariser to dedupe and collapse.
    httpx_mock.add_response(
        url=_SPARQL_RE,
        json={
            "head": {"vars": []},
            "results": {
                "bindings": [
                    {
                        "label": {"type": "literal", "value": "Vladimir Putin"},
                        "description": {
                            "type": "literal",
                            "value": "President of Russia",
                        },
                        "instance": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q5",
                        },
                        "instanceLabel": {"type": "literal", "value": "human"},
                        "dob": {"type": "literal", "value": "1952-10-07T00:00:00Z"},
                        "citizenship": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q15180",
                        },
                        "citizenshipLabel": {
                            "type": "literal",
                            "value": "Soviet Union",
                        },
                        "position": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q123028",
                        },
                        "positionLabel": {
                            "type": "literal",
                            "value": "President of Russia",
                        },
                        "positionStart": {
                            "type": "literal",
                            "value": "2012-05-07T00:00:00Z",
                        },
                    },
                    {
                        "label": {"type": "literal", "value": "Vladimir Putin"},
                        "description": {
                            "type": "literal",
                            "value": "President of Russia",
                        },
                        "citizenship": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q159",
                        },
                        "citizenshipLabel": {
                            "type": "literal",
                            "value": "Russia",
                        },
                        "position": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q899139",
                        },
                        "positionLabel": {
                            "type": "literal",
                            "value": "Prime Minister of Russia",
                        },
                    },
                ]
            },
        },
    )

    adapter = WikidataAdapter()
    bundle = await adapter.fetch("Q7747")

    summary = bundle["summary"]
    assert summary["qid"] == "Q7747"
    assert summary["is_person"] is True
    assert summary["is_entity"] is False
    assert summary["label"] == "Vladimir Putin"
    assert summary["dob"] == "1952-10-07T00:00:00Z"
    citizenship_qids = {c["qid"] for c in summary["citizenships"]}
    assert citizenship_qids == {"Q15180", "Q159"}
    position_qids = {p["qid"] for p in summary["positions"]}
    assert position_qids == {"Q123028", "Q899139"}


async def test_fetch_picks_up_lei_and_jurisdiction(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=_SPARQL_RE,
        json={
            "head": {"vars": []},
            "results": {
                "bindings": [
                    {
                        "label": {"type": "literal", "value": "BP p.l.c."},
                        "description": {
                            "type": "literal",
                            "value": "British multinational oil and gas company",
                        },
                        "instance": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q891723",
                        },
                        "instanceLabel": {
                            "type": "literal",
                            "value": "public company",
                        },
                        "lei": {
                            "type": "literal",
                            "value": "213800LBDB8WB3QGVN21",
                        },
                        "country": {
                            "type": "uri",
                            "value": "http://www.wikidata.org/entity/Q145",
                        },
                        "countryLabel": {
                            "type": "literal",
                            "value": "United Kingdom",
                        },
                        "inception": {
                            "type": "literal",
                            "value": "1909-04-14T00:00:00Z",
                        },
                    }
                ]
            },
        },
    )

    adapter = WikidataAdapter()
    bundle = await adapter.fetch("Q152057")
    summary = bundle["summary"]

    assert summary["is_person"] is False
    assert summary["is_entity"] is True
    assert summary["identifiers"]["lei"] == "213800LBDB8WB3QGVN21"
    assert summary["country"] == {"qid": "Q145", "label": "United Kingdom"}
    assert summary["inception"] == "1909-04-14T00:00:00Z"


async def test_fetch_rejects_non_qid() -> None:
    """A malformed QID must not reach SPARQL — defensive against injection."""
    adapter = WikidataAdapter()
    bundle = await adapter.fetch("not-a-qid; DROP TABLE")
    assert bundle["bindings"] == []
    assert bundle["summary"] == {}


async def test_fetch_stub_when_allow_live_false(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = WikidataAdapter()
    bundle = await adapter.fetch("Q7747")
    assert bundle["is_stub"] is True
