"""opencheck_export_bods hands back a link, not the graph (the export-by-link ticket).

Anthropic's "Code execution with MCP" (Nov 2025): a large tool result passes
through the model's context whole. A group's BODS graph runs to thousands of
statements, so by default the tool now answers with counts and a
``download_url`` naming the held run; ``GET /export?run=`` serves exactly that
run or answers 409, and never starts a fresh check.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from opencheck import lookup_replay
from opencheck.app import app
from opencheck.mcp import server as mcp_server
from opencheck.routers.lookup import LookupResponse
from opencheck.sources import SearchKind

LEI = "213800LBDB8WB3QGVN21"
RUN = "2026-11-26T09:00:00+00:00"

_BODS: list[dict[str, Any]] = [
    {
        "statementId": "e1",
        "recordId": "e1",
        "recordType": "entity",
        "recordStatus": "new",
        "statementDate": "2026-11-26",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "Test Holdings Ltd",
            "identifiers": [{"id": LEI, "scheme": "XI-LEI"}],
        },
    },
    {
        "statementId": "p1",
        "recordId": "p1",
        "recordType": "person",
        "recordStatus": "new",
        "statementDate": "2026-11-26",
        "recordDetails": {
            "isComponent": False,
            "personType": "knownPerson",
            "names": [{"type": "legal", "fullName": "Jane Doe"}],
        },
    },
    {
        "statementId": "r1",
        "recordId": "r1",
        "recordType": "relationship",
        "recordStatus": "new",
        "statementDate": "2026-11-26",
        "recordDetails": {
            "isComponent": False,
            "subject": "e1",
            "interestedParty": "p1",
            "interests": [{"type": "shareholding", "share": {"exact": 100}}],
        },
    },
]

_RESPONSE = LookupResponse(
    lei=LEI,
    legal_name="Test Holdings Ltd",
    jurisdiction="GB",
    query="Test Holdings Ltd",
    kind=SearchKind.ENTITY,
    hits=[],
    errors={},
    cross_source_links=[],
    risk_signals=[],
    bods=_BODS,
    bods_issues=[],
    license_notices=[],
    derived_identifiers={},
)


def _hold_run(lei: str = LEI, deepen_top: int = 5, run: str = RUN) -> None:
    lookup_replay._REPLAY_CACHE[lookup_replay._replay_key(lei, deepen_top)] = (
        lookup_replay._ReplayEntry(stored=time.monotonic(), fetched_at=run, events=[])
    )


@pytest.fixture
def mcp_lookup(monkeypatch):
    monkeypatch.setattr(
        "opencheck.routers.lookup._lookup_impl", AsyncMock(return_value=_RESPONSE)
    )


async def test_the_default_answer_is_counts_and_a_link_not_the_graph(mcp_lookup) -> None:
    _hold_run()
    out = await mcp_server.opencheck_export_bods(lei=LEI)
    assert out["delivery"] == "link"
    assert "statements" not in out and "records" not in out
    assert out["statement_count"] == 3
    assert out["statement_counts"] == {"entity": 1, "person": 1, "relationship": 1}
    assert out["run_completed_at"] == RUN
    assert out["link_expires_at"]
    q = parse_qs(urlparse(out["download_url"]).query)
    assert q == {"lei": [LEI], "format": ["json"], "deepen_top": ["5"], "run": [RUN]}
    assert "license_notices" in out and "integrated_ownership" in out


async def test_a_senzing_link_counts_the_records(mcp_lookup) -> None:
    _hold_run()
    out = await mcp_server.opencheck_export_bods(lei=LEI, format="senzing")
    assert out["record_count"] == 2
    assert parse_qs(urlparse(out["download_url"]).query)["format"] == ["senzing"]


async def test_with_no_held_run_there_is_no_link_and_it_says_why(mcp_lookup) -> None:
    out = await mcp_server.opencheck_export_bods(lei=LEI)
    assert out["download_url"] is None
    assert "delivery='inline'" in out["download_unavailable"]


async def test_inline_filters_by_record_type_and_says_what_it_cut(mcp_lookup) -> None:
    out = await mcp_server.opencheck_export_bods(
        lei=LEI, delivery="inline", record_types=["entity", "person"], max_statements=1
    )
    assert [s["statementId"] for s in out["statements"]] == ["e1"]
    assert out["total"] == 2 and out["truncated"] is True
    assert out["statement_count"] == 3


@pytest.mark.parametrize(
    "kwargs",
    [
        {"delivery": "stream"},
        {"record_types": ["ownership"]},
        {"max_statements": 0},
        {"max_statements": mcp_server.INLINE_HARD_MAX + 1},
    ],
)
async def test_bad_arguments_are_refused(kwargs) -> None:
    out = await mcp_server.opencheck_export_bods(lei=LEI, **kwargs)
    assert "error" in out


@pytest.fixture
def client():
    with patch(
        "opencheck.routers.export._lookup_impl", new=AsyncMock(return_value=_RESPONSE)
    ):
        with TestClient(app) as c:
            yield c


def test_the_link_serves_the_held_run(client) -> None:
    _hold_run()
    r = client.get("/export", params={"lei": LEI, "format": "json", "run": RUN})
    assert r.status_code == 200
    assert [s["statementId"] for s in r.json()] == ["e1", "p1", "r1"]


def test_a_link_to_a_run_no_longer_held_is_409_and_runs_nothing(client) -> None:
    from opencheck.routers import export as export_router

    r = client.get("/export", params={"lei": LEI, "format": "json", "run": RUN})
    assert r.status_code == 409
    assert "15 minutes" in r.json()["detail"]
    export_router._lookup_impl.assert_not_awaited()


def test_a_link_to_a_replaced_run_is_409(client) -> None:
    _hold_run(run="2026-11-26T09:20:00+00:00")
    r = client.get("/export", params={"lei": LEI, "format": "json", "run": RUN})
    assert r.status_code == 409


def test_run_needs_an_lei(client) -> None:
    r = client.get("/export", params={"q": "Test", "format": "json", "run": RUN})
    assert r.status_code == 400
    # Plain prose: the detail is read as a JSON error, where reStructuredText
    # backticks reach the reader as literal characters (26 Nov 2026).
    assert r.json()["detail"] == (
        "The run parameter names a held run of an LEI check. "
        "Give an lei and no saved_report_id."
    )
    assert "`" not in r.json()["detail"]


def test_run_beside_a_saved_report_is_refused_in_the_same_words(client) -> None:
    from opencheck.routers.export import RUN_NEEDS_LEI

    r = client.get(
        "/export",
        params={"lei": LEI, "saved_report_id": "A" * 22, "format": "json", "run": RUN},
    )
    assert r.status_code == 400
    assert r.json()["detail"] == RUN_NEEDS_LEI


# The subject's GLEIF record twice, as the Shell run of 26 Nov 2026 had it:
# once from the lookup (with its listing) and again, re-stated, from the
# subsidiaries merge. Same recordId, different statementId.
_REPEATED_SUBJECT = {
    **_BODS[0],
    "statementId": "e1-again",
    "recordDetails": {**_BODS[0]["recordDetails"], "name": "TEST HOLDINGS LTD"},
}
_RESPONSE_WITH_REPEAT = _RESPONSE.model_copy(
    update={"bods": [*_BODS, _REPEATED_SUBJECT]}
)


@pytest.fixture
def mcp_lookup_with_repeat(monkeypatch):
    monkeypatch.setattr(
        "opencheck.routers.lookup._lookup_impl",
        AsyncMock(return_value=_RESPONSE_WITH_REPEAT),
    )


@pytest.mark.parametrize("format", ["json", "jsonl"])
async def test_inline_and_link_give_one_statement_per_record(
    mcp_lookup_with_repeat, format
) -> None:
    _hold_run()
    link = await mcp_server.opencheck_export_bods(lei=LEI, format=format)
    inline = await mcp_server.opencheck_export_bods(
        lei=LEI, format=format, delivery="inline"
    )
    assert link["statement_count"] == inline["statement_count"] == 3
    assert inline["total"] == 3 and inline["truncated"] is False
    statements = inline["statements"]
    if format == "jsonl":
        import json

        statements = [json.loads(line) for line in statements.splitlines()]
    # The first reading is kept, as GET /export keeps it.
    assert [s["statementId"] for s in statements] == ["e1", "p1", "r1"]


async def test_inline_senzing_counts_the_same_records_as_the_link(
    mcp_lookup_with_repeat,
) -> None:
    _hold_run()
    link = await mcp_server.opencheck_export_bods(lei=LEI, format="senzing")
    inline = await mcp_server.opencheck_export_bods(
        lei=LEI, format="senzing", delivery="inline"
    )
    assert link["record_count"] == inline["record_count"] == inline["total"]
