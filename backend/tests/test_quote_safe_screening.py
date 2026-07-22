"""Quote-safe name screening (the בע"מ bug).

Related-party names containing an unbalanced ASCII double quote — the
gershayim in Israeli company names (``בע"מ`` = Ltd) is routinely written
as ``"`` — deterministically broke both name screens: OpenSanctions'
Lucene parser rejects the query with HTTP 400 (``token_mgr_error:
Lexical error … <EOF>``) and the ICIJ Offshore Leaks reconciliation
endpoint answers a bare HTTP 500. Diagnosed live 2026-07-22 on Unilever
PLC (LEI 549300MKFYEKVRWML317): 3 of 25 OpenSanctions probes and a whole
10-name ICIJ batch failed on every lookup, always for the same three
Israeli subsidiaries.

Two defences, both covered here:

* :func:`opencheck.http.sanitize_name_query` strips Lucene syntax
  characters (``"`` and ``\\``) from every outgoing name query — applied
  in the OpenSanctions and EveryPolitician adapters and the ICIJ batcher.
* The ICIJ batcher retries a deterministically-rejected batch one name at
  a time, so a poison name only loses itself instead of taking up to nine
  clean names down with it. No per-name retry on 429 (throttled), 404
  (service moved), timeouts or network errors — those are service-level.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.http import sanitize_name_query
from opencheck.icij_check import _RECONCILE_URL, assess_icij_names
from opencheck.risk import (
    DEGRADED_RATE_LIMITED,
    DEGRADED_TIMEOUT,
    DEGRADED_UPSTREAM_ERROR,
    OFFSHORE_LEAKS,
    DegradedSource,
)
from opencheck.sources import SearchKind
from opencheck.sources.everypolitician import EveryPoliticianAdapter
from opencheck.sources.opensanctions import OpenSanctionsAdapter

#: A real shape from the Unilever bundle — one ASCII gershayim, unbalanced.
_HEBREW_LTD = 'יוניליוור ישראל מזון בע"מ'


# ---------------------------------------------------------------------
# sanitize_name_query
# ---------------------------------------------------------------------


def test_sanitize_identity_on_clean_names() -> None:
    assert sanitize_name_query("UNILEVER PLC") == "UNILEVER PLC"
    assert sanitize_name_query("PAULA'S CHOICE EUROPE B.V.") == "PAULA'S CHOICE EUROPE B.V."


def test_sanitize_strips_unbalanced_hebrew_gershayim_quote() -> None:
    assert sanitize_name_query(_HEBREW_LTD) == "יוניליוור ישראל מזון בע מ"


def test_sanitize_strips_balanced_quotes_too() -> None:
    # Balanced quotes are phrase-query syntax upstream — never intended
    # when screening a name.
    assert sanitize_name_query('ACME "REAL" HOLDINGS LTD') == "ACME REAL HOLDINGS LTD"


def test_sanitize_strips_backslash() -> None:
    assert sanitize_name_query("ACME\\CORP") == "ACME CORP"


def test_sanitize_collapses_whitespace() -> None:
    assert sanitize_name_query('  ACME  "  CORP ') == "ACME CORP"


def test_sanitize_empty_and_quote_only_inputs() -> None:
    assert sanitize_name_query("") == ""
    assert sanitize_name_query('"') == ""
    assert sanitize_name_query(' "" \\ ') == ""


# ---------------------------------------------------------------------
# Adapter search paths (OpenSanctions + EveryPolitician share the same
# Lucene-backed /search API)
# ---------------------------------------------------------------------


@pytest.fixture
def _live(monkeypatch: pytest.MonkeyPatch, tmp_path) -> Any:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _query_param(request: httpx.Request) -> str:
    return parse_qs(request.url.query.decode())["q"][0]


async def test_opensanctions_search_sends_sanitised_query(
    _live: Any, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(json={"results": []})
    hits = await OpenSanctionsAdapter().search(_HEBREW_LTD, SearchKind.ENTITY)
    assert hits == []
    (request,) = httpx_mock.get_requests()
    assert _query_param(request) == "יוניליוור ישראל מזון בע מ"
    assert '"' not in str(request.url)
    assert "%22" not in str(request.url)


async def test_opensanctions_quote_only_query_skips_http(
    _live: Any, httpx_mock: HTTPXMock
) -> None:
    hits = await OpenSanctionsAdapter().search('"', SearchKind.ENTITY)
    assert hits == []
    assert httpx_mock.get_requests() == []


async def test_everypolitician_search_sends_sanitised_query(
    _live: Any, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(json={"results": []})
    hits = await EveryPoliticianAdapter().search('ד"ר יעקב כהן', SearchKind.PERSON)
    assert hits == []
    (request,) = httpx_mock.get_requests()
    assert _query_param(request) == "ד ר יעקב כהן"
    assert "%22" not in str(request.url)


async def test_everypolitician_quote_only_query_skips_http(
    _live: Any, httpx_mock: HTTPXMock
) -> None:
    hits = await EveryPoliticianAdapter().search('""', SearchKind.PERSON)
    assert hits == []
    assert httpx_mock.get_requests() == []


# ---------------------------------------------------------------------
# ICIJ — sanitised queries + per-name fallback
# ---------------------------------------------------------------------


@pytest.fixture
def _live_icij(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _entity(sid: str, name: str) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordType": "entity",
        "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": name},
    }


def _posted_queries(request: httpx.Request) -> dict[str, Any]:
    return json.loads(parse_qs(request.content.decode())["queries"][0])


async def test_icij_queries_are_sanitised(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(url=_RECONCILE_URL, method="POST", json={})
    await assess_icij_names([_entity("e1", _HEBREW_LTD)])
    (request,) = httpx_mock.get_requests()
    queries = _posted_queries(request)
    assert queries["q0"]["query"] == "יוניליוור ישראל מזון בע מ"


async def test_icij_quote_only_names_are_skipped_without_http(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    signals = await assess_icij_names([_entity("e1", '"')])
    assert signals == []
    assert httpx_mock.get_requests() == []


async def test_icij_per_name_fallback_only_loses_the_poison_name(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    """A 500 on the batch triggers one-name-at-a-time retries: the clean
    name is screened (and can still match), only the poison name is
    counted as unscreened."""
    clean_match = {
        "id": "12345",
        "name": "CLEAN HOLDINGS BVI",
        "score": 95,
        "match": True,
        "description": "Panama Papers · British Virgin Islands",
    }

    def route(request: httpx.Request) -> httpx.Response:
        queries = _posted_queries(request)
        if len(queries) > 1:  # the batch → deterministic rejection
            return httpx.Response(500, json={"code": 500, "message": "Server Error"})
        (query,) = queries.values()
        if "POISON" in query["query"]:
            return httpx.Response(500, json={"code": 500, "message": "Server Error"})
        return httpx.Response(201, json={"q0": {"result": [clean_match]}})

    httpx_mock.add_callback(route, url=_RECONCILE_URL, method="POST", is_reusable=True)

    degraded: list[DegradedSource] = []
    signals = await assess_icij_names(
        [_entity("e1", "CLEAN HOLDINGS BVI"), _entity("e2", "POISON LLC")],
        degraded=degraded,
    )

    # 1 batch POST + 2 per-name retries.
    assert len(httpx_mock.get_requests()) == 3
    # The clean name was screened and matched.
    assert [s.code for s in signals] == [OFFSHORE_LEAKS]
    assert signals[0].evidence["subject_statement_id"] == "e1"
    # Only the poison name counts as unscreened.
    (record,) = degraded
    assert record.reason == DEGRADED_UPSTREAM_ERROR
    assert "1 of 1 reconciliation batch(es) failed" in record.detail
    assert "1 of 2 name(s)" in record.detail


async def test_icij_fallback_full_recovery_is_not_degraded(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    """If every name of a failed batch succeeds individually, nothing was
    skipped — no degradation record."""

    def route(request: httpx.Request) -> httpx.Response:
        queries = _posted_queries(request)
        if len(queries) > 1:
            return httpx.Response(500, json={"code": 500, "message": "Server Error"})
        return httpx.Response(201, json={"q0": {"result": []}})

    httpx_mock.add_callback(route, url=_RECONCILE_URL, method="POST", is_reusable=True)

    degraded: list[DegradedSource] = []
    signals = await assess_icij_names(
        [_entity("e1", "ACME ONE"), _entity("e2", "ACME TWO")], degraded=degraded
    )
    assert signals == []
    assert degraded == []
    assert len(httpx_mock.get_requests()) == 3


async def test_icij_no_per_name_fallback_on_429(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    """Throttled → retrying name-by-name would make it worse."""
    httpx_mock.add_response(url=_RECONCILE_URL, method="POST", status_code=429)
    degraded: list[DegradedSource] = []
    await assess_icij_names(
        [_entity("e1", "ACME ONE"), _entity("e2", "ACME TWO")], degraded=degraded
    )
    assert len(httpx_mock.get_requests()) == 1
    assert degraded[0].reason == DEGRADED_RATE_LIMITED
    assert "2 of 2 name(s)" in degraded[0].detail


async def test_icij_no_per_name_fallback_on_404(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    """404 = the service moved (again) — every per-name retry would 404 too."""
    httpx_mock.add_response(url=_RECONCILE_URL, method="POST", status_code=404)
    degraded: list[DegradedSource] = []
    await assess_icij_names(
        [_entity("e1", "ACME ONE"), _entity("e2", "ACME TWO")], degraded=degraded
    )
    assert len(httpx_mock.get_requests()) == 1
    assert degraded[0].reason == DEGRADED_UPSTREAM_ERROR


async def test_icij_no_per_name_fallback_on_timeout(
    _live_icij: Any, httpx_mock: HTTPXMock
) -> None:
    """Timeouts are service-level; ten more requests only add latency."""
    httpx_mock.add_exception(
        httpx.ConnectTimeout("slow"), url=_RECONCILE_URL, is_reusable=True
    )
    degraded: list[DegradedSource] = []
    await assess_icij_names(
        [_entity("e1", "ACME ONE"), _entity("e2", "ACME TWO")], degraded=degraded
    )
    assert len(httpx_mock.get_requests()) == 1
    assert degraded[0].reason == DEGRADED_TIMEOUT
    assert "2 of 2 name(s)" in degraded[0].detail


# ---------------------------------------------------------------------
# The A/S bug (diagnosed live 2026-07-22 on HORNSEA 1 LIMITED,
# LEI 2138002S3XGZ38WN5Q72): ``/`` opens a Lucene REGEX, so Danish and
# Norwegian company names ending ``A/S`` are an unterminated regular
# expression to the same parsers the gershayim broke. Hornsea 1's
# Ørsted parent chain carries eleven ``A/S`` entities — 8 of 25
# OpenSanctions probes and all 3 ICIJ batches failed on every lookup.
# ---------------------------------------------------------------------


def test_sanitize_strips_slash_the_hornsea_bug() -> None:
    # Real names from the Hornsea 1 curated bundle.
    assert sanitize_name_query("ØRSTED A/S") == "ØRSTED A S"
    assert sanitize_name_query("ØRSTED WIND POWER A/S") == "ØRSTED WIND POWER A S"
    assert sanitize_name_query("INEOS E&P A/S") == "INEOS E&P A S"  # single & kept


def test_sanitize_strips_remaining_lucene_syntax() -> None:
    # Grouping / range / boost / fuzzy / wildcard / field syntax.
    assert sanitize_name_query("ORSTED POWER (UK) LIMITED") == "ORSTED POWER UK LIMITED"
    assert sanitize_name_query("ACME [HOLDINGS] {X}") == "ACME HOLDINGS X"
    assert sanitize_name_query("ACME^2 ~1 *? :Y") == "ACME 2 1 Y"
    # Boolean operator pairs go; single & and | are not syntax.
    assert sanitize_name_query("SMITH && JONES || CO") == "SMITH JONES CO"
    # Leading - / + are NOT / MUST operators; mid-word hyphens are names.
    assert sanitize_name_query("-BAD +HALLE") == "BAD HALLE"
    assert sanitize_name_query("ANNE-MARIE SMITH") == "ANNE-MARIE SMITH"


def test_sanitize_still_identity_on_clean_names_post_extension() -> None:
    for name in (
        "UNILEVER PLC",
        "PAULA'S CHOICE EUROPE B.V.",
        "ENECOGEN V.O.F.",
        "2W PERMIAN SOLAR, LLC",
        "ANNE-MARIE SMITH",
    ):
        assert sanitize_name_query(name) == name
