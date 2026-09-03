"""Phase 164 — batch / portfolio screening (``opencheck.batch``,
``GET /batch-stream``, MCP ``opencheck_batch_lookup``).

The batch is a thin loop over the single-LEI pipeline, and the tests pin
the properties that make it safe to be one:

1. **parity** — a batch row for LEI X is ``shape_batch_row`` of the very
   ``LookupResponse`` a single lookup of X returns;
2. **tolerant paste** — separators, checksum rejection, dedupe, the cap
   with the overflow counted;
3. **concurrency** — twenty pipelines never exceed two in flight;
4. **a failed row is a row** — a throttle refusal (503) arrives as
   ``row_failed`` with ``degraded: true`` and the batch still completes;
5. **the bot gate** — a declared bot gets the same 403 as /lookup-stream;
6. **the tier** — the route is on the heavy budget, not the lookup one.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Iterator

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from opencheck import batch as _batch
from opencheck import identifiers
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.mcp import TOOL_NAMES
from opencheck.mcp import server as mcp_server
from opencheck.mcp.shaping import shape_batch_row
from opencheck.ratelimit import limiter
from opencheck.routers.lookup import LookupResponse
from opencheck.sources import SearchKind, SourceHit

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def _with_check_digits(base18: str) -> str:
    """Append the ISO 17442 check digits to an 18-character stem."""
    assert len(base18) == 18
    for cd in range(2, 99):
        cand = f"{base18}{cd:02d}"
        if identifiers.lei_check_digits_ok(cand):
            return cand
    raise AssertionError("no check digits")  # pragma: no cover


def _leis(n: int) -> list[str]:
    return [_with_check_digits(f"2138000000000000{i:02d}") for i in range(n)]


def _hit(source_id: str, *, stub: bool = False) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id="x",
        kind=SearchKind.ENTITY,
        name="Northwind",
        summary="",
        is_stub=stub,
    )


def _resp(lei: str, **over) -> LookupResponse:
    fields = dict(
        query=lei,
        kind=SearchKind.ENTITY,
        hits=[_hit("gleif"), _hit("companies_house"), _hit("wikidata", stub=True)],
        errors={"opensanctions": "timeout"},
        cross_source_links=[],
        risk_signals=[
            {"code": "SANCTIONED", "kind": "risk", "severity": 3, "summary": "a", "source_id": "opensanctions"},
            {"code": "NON_EU_JURISDICTION", "kind": "context", "severity": 1, "summary": "b", "source_id": "gleif"},
            {"code": "NON_EU_JURISDICTION", "kind": "context", "severity": 1, "summary": "b", "source_id": "companies_house"},
        ],
        bods=[],
        bods_issues=[],
        license_notices=[],
        degraded_sources=[{"source_id": "opensanctions", "check": "related_party", "reason": "timeout"}],
        verdict="The company itself is on a sanctions list.",
        subject_profile={
            "register_status": {"liveness": "live", "since": "2001-01-01", "raw": "active", "source_id": "companies_house", "sources": ["companies_house"]},
        },
        lei=lei,
        legal_name="Northwind Logistics Ltd",
        jurisdiction="GB",
        derived_identifiers={},
        sources_applicable=["companies_house", "opensanctions", "wikidata"],
    )
    fields.update(over)
    return LookupResponse(**fields)


# ---- 1. parity ------------------------------------------------------------


def test_row_is_the_single_lookup_reduced() -> None:
    lei = _leis(1)[0]
    row = shape_batch_row(_resp(lei))
    assert row["lei"] == lei
    assert row["legal_name"] == "Northwind Logistics Ltd"
    assert row["jurisdiction"] == "GB"
    assert row["verdict"] == "The company itself is on a sanctions list."
    assert row["register_status"] == {
        "liveness": "live", "since": "2001-01-01", "raw": "active", "source_id": "companies_house",
    }
    # Kind split from the same helper the single report uses; identical
    # context rows merge (Phase 153) so the count is 1, not 2.
    assert row["risk_count"] == 1 and row["risk_codes"] == ["SANCTIONED"]
    assert row["context_count"] == 1 and row["context_codes"] == ["NON_EU_JURISDICTION"]
    # Phase 156: the GLEIF anchor is counted in both coverage figures.
    assert row["coverage"]["applicable"] == 4
    assert row["coverage"]["applicable_ids"][0] == "gleif"
    assert row["coverage"]["answered"] == 2  # gleif + companies_house (wikidata was a stub)
    assert row["degraded"] is True
    assert row["degraded_sources"] == ["opensanctions"]
    assert row["report_url"] == f"/?lei={lei}"


@pytest.mark.asyncio
async def test_batch_row_equals_single_lookup_for_same_lei(monkeypatch) -> None:
    a, b = _leis(2)

    async def _fake(lei, deepen_top=5, refresh=False):
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    rows = {}
    async for event, payload in _batch.run_batch([a, b], concurrency=2):
        if event == "row_done":
            rows[payload["lei"]] = payload
    assert set(rows) == {a, b}
    assert rows[a] == shape_batch_row(await _fake(a))
    assert rows[b] == shape_batch_row(await _fake(b))


# ---- 2. tolerant paste ----------------------------------------------------


@pytest.fixture
def checksums_on(monkeypatch) -> Iterator[None]:
    monkeypatch.setenv("OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED", "1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_parse_accepts_every_separator_and_case(checksums_on) -> None:
    a, b, c, d = _leis(4)
    text = f"{a.lower()}\n{b},{c};\t {d}  \r\n"
    parsed = _batch.parse_lei_list(text)
    assert parsed.leis == [a, b, c, d]
    assert parsed.rejected == [] and parsed.overflow == 0


def test_parse_rejects_in_place_with_reasons(checksums_on) -> None:
    a = _leis(1)[0]
    bad_digits = a[:18] + ("00" if a[18:] != "00" else "01")
    parsed = _batch.parse_lei_list(f"{a}\nTOO-SHORT\n{a}\n{bad_digits}\nabcdefghijklmnopqr!!")
    assert parsed.leis == [a]
    reasons = {r.token: r.reason for r in parsed.rejected}
    assert "characters" in reasons["TOO-SHORT"]
    assert reasons[a] == "duplicate"  # the second occurrence
    assert "check digits" in reasons[bad_digits]
    assert "not an LEI" in reasons["abcdefghijklmnopqr!!"]


def test_parse_caps_at_twenty_and_counts_the_overflow() -> None:
    leis = _leis(27)
    parsed = _batch.parse_lei_list(" ".join(leis))
    assert len(parsed.leis) == _batch.MAX_ROWS == 20
    assert parsed.leis == leis[:20]
    assert parsed.overflow == 7
    assert parsed.cap == 20


def test_parse_empty_paste_is_empty() -> None:
    parsed = _batch.parse_lei_list("  \n,;\t")
    assert parsed.leis == [] and parsed.rejected == []


# ---- 3. concurrency --------------------------------------------------------


@pytest.mark.asyncio
async def test_never_more_than_the_cap_in_flight(monkeypatch) -> None:
    in_flight = 0
    peak = 0

    async def _fake(lei, deepen_top=5, refresh=False):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    events = [e async for e in _batch.run_batch(_leis(20), concurrency=2)]
    assert peak == 2
    assert sum(1 for e, _ in events if e == "row_done") == 20
    assert events[-1] == ("batch_done", {"requested": 20, "done": 20, "failed": 0})


def test_concurrency_default_is_two_and_env_overridable(monkeypatch) -> None:
    monkeypatch.delenv("OPENCHECK_BATCH_CONCURRENCY", raising=False)
    get_settings.cache_clear()
    assert _batch.batch_concurrency() == 2
    monkeypatch.setenv("OPENCHECK_BATCH_CONCURRENCY", "3")
    get_settings.cache_clear()
    assert _batch.batch_concurrency() == 3
    get_settings.cache_clear()


# ---- 4. a failed row is a row ---------------------------------------------


@pytest.mark.asyncio
async def test_rate_limited_row_is_marked_degraded_and_batch_completes(monkeypatch) -> None:
    a, b, c = _leis(3)

    async def _fake(lei, deepen_top=5, refresh=False):
        if lei == b:
            raise HTTPException(503, "GLEIF is rate-limiting OpenCheck's shared connection")
        if lei == c:
            raise RuntimeError("adapter fan-out died")
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    events = [e async for e in _batch.run_batch([a, b, c], concurrency=2)]
    by_lei = {p["lei"]: (e, p) for e, p in events if e != "batch_done"}
    assert by_lei[a][0] == "row_done"
    e, p = by_lei[b]
    assert e == "row_failed" and p["degraded"] is True and p["status"] == 503
    assert p["retryable"] is True and "rate-limiting" in p["reason"]
    e, p = by_lei[c]
    assert e == "row_failed" and p["degraded"] is True and p["retryable"] is False
    assert "RuntimeError" in p["reason"]
    assert events[-1] == ("batch_done", {"requested": 3, "done": 1, "failed": 2})


# ---- 5 + 6. the route: bot gate, tier, wire shape --------------------------


@pytest.fixture
def client(monkeypatch, tmp_path: Path) -> Iterator[TestClient]:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", raising=False)
    get_settings.cache_clear()
    yield TestClient(app)
    get_settings.cache_clear()


@pytest.mark.parametrize(
    "ua", ["Mozilla/5.0 (compatible; Googlebot/2.1)", "python-httpx/0.27.0", ""],
    ids=["googlebot", "httpx", "empty"],
)
def test_declared_bots_get_the_lookup_stream_refusal(client: TestClient, ua: str) -> None:
    r = client.get("/batch-stream", params={"leis": _leis(1)[0]}, headers={"User-Agent": ua})
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert "/lookup?lei=" in detail and "/entity/" in detail and "robots" in detail
    assert "opencheck_batch_lookup" in detail


def test_gate_shares_the_lookup_stream_setting(client: TestClient, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", "0")
    get_settings.cache_clear()

    async def _fake(lei, deepen_top=5, refresh=False):
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    r = client.get(
        "/batch-stream", params={"leis": _leis(1)[0]},
        headers={"User-Agent": "python-httpx/0.27.0"},
    )
    assert r.status_code == 200


def test_no_valid_leis_is_422_with_the_rejections(client: TestClient) -> None:
    r = client.get(
        "/batch-stream", params={"leis": "nope, also-nope"},
        headers={"User-Agent": _BROWSER_UA},
    )
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["message"] == "No valid LEIs to screen."
    assert [d["token"] for d in detail["rejected"]] == ["nope", "also-nope"]


def _sse(body: str) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    event = None
    for line in body.splitlines():
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:") and event:
            out.append((event, json.loads(line[5:].strip())))
            event = None
    return out


def test_stream_shape_start_rows_done(client: TestClient, monkeypatch) -> None:
    a, b = _leis(2)
    extra = _leis(3)[2]

    async def _fake(lei, deepen_top=5, refresh=False):
        if lei == b:
            raise HTTPException(404, "No GLEIF record found")
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    r = client.get(
        "/batch-stream", params={"leis": f"{a},{b} {a} junk"},
        headers={"User-Agent": _BROWSER_UA},
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    events = _sse(r.text)
    assert events[0][0] == "batch_start"
    start = events[0][1]
    assert start["accepted"] == [a, b]
    assert [x["reason"] for x in start["rejected"]] == ["duplicate", "4 characters — an LEI has 20"]
    assert start["cap"] == 20 and start["concurrency"] == 2 and start["overflow"] == 0
    kinds = {p.get("lei"): e for e, p in events[1:-1]}
    assert kinds == {a: "row_done", b: "row_failed"}
    assert events[-1] == ("batch_done", {"requested": 2, "done": 1, "failed": 1})
    assert extra not in kinds


@pytest.fixture
def limited_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[TestClient]:
    """Limiter ON with the heavy tier at 1/minute and lookup at 5/minute."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_DEFAULT", "50/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_LOOKUP", "5/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_HEAVY", "1/minute")
    get_settings.cache_clear()
    limiter.reset()
    limiter.enabled = True
    try:
        yield TestClient(app)
    finally:
        limiter.enabled = False
        limiter.reset()
        get_settings.cache_clear()


def test_route_is_on_the_heavy_tier(limited_client: TestClient, monkeypatch) -> None:
    async def _fake(lei, deepen_top=5, refresh=False):
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    hdr = {"User-Agent": _BROWSER_UA}
    lei = _leis(1)[0]
    assert limited_client.get("/batch-stream", params={"leis": lei}, headers=hdr).status_code == 200
    r = limited_client.get("/batch-stream", params={"leis": lei}, headers=hdr)
    # Second call within the minute: the HEAVY budget (1/min) is spent while
    # the lookup budget (5/min) is not — so a 429 here proves the tier.
    assert r.status_code == 429
    assert "Rate limit exceeded" in r.json()["detail"]


# ---- MCP tool --------------------------------------------------------------


def test_tool_is_declared() -> None:
    assert "opencheck_batch_lookup" in TOOL_NAMES


def test_robots_disallows_the_batch_stream(client: TestClient) -> None:
    """The 403 points bots at robots.txt, so robots.txt must actually say it."""
    r = client.get("/robots.txt")
    assert r.status_code == 200
    assert "Disallow: /batch-stream" in r.text


@pytest.mark.asyncio
async def test_mcp_tool_returns_rows_in_paste_order_with_failures_apart(monkeypatch) -> None:
    a, b, c = _leis(3)

    async def _fake(lei, deepen_top=5, refresh=False):
        if lei == b:
            raise HTTPException(503, "rate-limited")
        await asyncio.sleep(0.02 if lei == a else 0)
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    out = await mcp_server.opencheck_batch_lookup(leis=[c, a, b, a, "bad"])
    assert out["accepted"] == [c, a, b]
    assert [r["token"] for r in out["rejected"]] == [a, "bad"]
    assert [r["lei"] for r in out["rows"]] == [c, a]  # paste order despite a finishing last
    assert [r["lei"] for r in out["failed"]] == [b]
    assert out["failed"][0]["degraded"] is True
    assert out["counts"] == {"requested": 3, "done": 2, "failed": 1, "degraded": 3}
    assert out["cap"] == 20


@pytest.mark.asyncio
async def test_mcp_tool_with_nothing_valid() -> None:
    out = await mcp_server.opencheck_batch_lookup(leis=["nope"])
    assert out["rows"] == [] and out["failed"] == [] and out["accepted"] == []
    assert out["counts"]["requested"] == 0
