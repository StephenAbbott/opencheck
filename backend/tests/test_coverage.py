"""Phase 241 — one coverage definition, and every source error a degradation.

The Opus 5.5 check found three surfaces with three denominators: the MCP
summary said "11 of 11 sources returned data" for Shell where the batch row
said 13 applied, the batch ``answered`` counted only sources with a record,
and OpenAleph's ``ReadTimeout`` after returning a result (Scottish Mortgage)
left ``degraded_sources`` empty.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import degradation
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.coverage import coverage_sentence, report_coverage, source_coverage
from opencheck.mcp.shaping import shape_batch_row, shape_lookup
from opencheck.risk import DegradedSource
from opencheck.routers.lookup import LookupResponse
from opencheck.sources import REGISTRY, SearchKind, SourceHit
from opencheck.verdict import build_verdict

LEI = "213800LH1BZH3DI6G760"


def _hit(source_id: str, *, stub: bool = False, raw: dict | None = None) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=f"{source_id}-1",
        kind=SearchKind.ENTITY,
        name="Northwind Logistics Ltd",
        summary="",
        identifiers={},
        raw=raw or {},
        is_stub=stub,
    )


def _resp(**over) -> LookupResponse:
    fields = dict(
        query=LEI,
        kind=SearchKind.ENTITY,
        # Shell's shape, shrunk: thirteen sources in the real run, five here.
        hits=[_hit("gleif"), _hit("companies_house"), _hit("openaleph")],
        errors={"openaleph": "ReadTimeout: read timed out", "kvk": "HTTPStatusError: HTTP 429"},
        cross_source_links=[],
        risk_signals=[],
        bods=[],
        bods_issues=[],
        license_notices=[],
        degraded_sources=[],
        lei=LEI,
        legal_name="Northwind Logistics Ltd",
        jurisdiction="GB",
        derived_identifiers={},
        sources_applicable=["companies_house", "openaleph", "wikidata", "ted_eu", "kvk"],
    )
    fields.update(over)
    return LookupResponse(**fields)


# ---- the one definition ----------------------------------------------------


def test_every_applicable_source_is_in_exactly_one_bucket() -> None:
    r = _resp()
    cov = source_coverage(r.hits, r.errors, r.sources_applicable)
    assert cov["applicable_ids"] == ["gleif", "companies_house", "openaleph", "wikidata", "ted_eu", "kvk"]
    assert cov["with_data_ids"] == ["gleif", "companies_house", "openaleph"]
    assert cov["no_record_ids"] == ["wikidata", "ted_eu"]
    assert cov["failed_ids"] == ["kvk"]
    # A result and an error: answered, with data, and partial.
    assert cov["partial_ids"] == ["openaleph"]
    assert cov["answered_ids"] == ["gleif", "companies_house", "openaleph", "wikidata", "ted_eu"]
    buckets = cov["with_data_ids"] + cov["no_record_ids"] + cov["failed_ids"]
    assert sorted(buckets) == sorted(cov["applicable_ids"])
    assert (cov["applicable"], cov["answered"], cov["with_data"], cov["failed"]) == (6, 5, 3, 1)


def test_a_stub_or_a_coverage_note_is_not_a_record() -> None:
    hits = [
        _hit("gleif"),
        _hit("wikidata", stub=True),
        _hit("inpi", raw={"not_found": True, "coverage_note": "not in the RNE"}),
    ]
    cov = source_coverage(hits, {}, ["wikidata", "inpi"])
    assert cov["with_data_ids"] == ["gleif"]
    assert cov["no_record_ids"] == ["wikidata", "inpi"]


def test_a_source_never_announced_is_still_counted_and_named() -> None:
    cov = source_coverage([_hit("gleif"), _hit("sec_edgar")], {"eiti": "x"}, [])
    assert cov["applicable_ids"] == ["gleif", "sec_edgar", "eiti"]
    assert cov["failed_ids"] == ["eiti"]


def test_the_anchor_is_counted_only_when_the_lookup_resolved() -> None:
    assert source_coverage([], {}, ["kvk"], anchored=False)["applicable_ids"] == ["kvk"]
    assert source_coverage([], {}, ["kvk"])["applicable_ids"] == ["gleif", "kvk"]


def test_the_sentence_counts_answered_against_applicable() -> None:
    r = _resp()
    cov = source_coverage(r.hits, r.errors, r.sources_applicable)
    assert coverage_sentence(cov) == (
        "5 of 6 sources answered (3 with records, 2 with no record); 1 did not answer"
    )
    assert coverage_sentence(source_coverage([_hit("gleif")], {}, [])) == "1 of 1 source answered"


# ---- the surfaces agree ----------------------------------------------------


def test_mcp_summary_uses_the_applicable_denominator() -> None:
    out = shape_lookup(_resp())
    assert "5 of 6 sources answered (3 with records, 2 with no record); 1 did not answer" in out["summary"]
    assert "returned data" not in out["summary"]
    assert out["counts"]["sources_applicable"] == 6
    assert out["counts"]["sources_answered"] == 5
    assert out["counts"]["sources_with_data"] == 3
    # Every applicable source has a row, the no-record ones included.
    rows = {s["id"]: s for s in out["sources"]}
    assert set(rows) == {"gleif", "companies_house", "openaleph", "wikidata", "ted_eu", "kvk"}
    assert rows["wikidata"]["no_record"] is True and rows["wikidata"]["answered"] is True
    assert rows["kvk"]["answered"] is False and "error" in rows["kvk"]
    assert rows["openaleph"]["found"] is True and rows["openaleph"]["answered"] is True
    assert out["coverage"]["no_record_ids"] == ["wikidata", "ted_eu"]


def test_batch_row_and_mcp_summary_report_the_same_figures() -> None:
    r = _resp()
    row = shape_batch_row(r)
    counts = shape_lookup(r)["counts"]
    assert row["coverage"]["applicable"] == counts["sources_applicable"]
    assert row["coverage"]["answered"] == counts["sources_answered"]
    assert row["coverage"]["with_data"] == counts["sources_with_data"]


def test_the_report_names_every_applicable_source() -> None:
    from opencheck.reporting.html_report import _sources_found as html_sources
    from opencheck.reporting.markdown_report import _sources_found as md_sources

    report = _resp().model_dump()
    assert report_coverage(report)["applicable"] == 6
    md = "\n".join(md_sources(report))
    html = html_sources(report)
    for text in (md, html):
        assert "5 of 6 sources answered" in text
        assert "Answered with no record" in text and "Did not answer" in text
    no_record = REGISTRY["wikidata"].info.name
    assert no_record in md and no_record in html
    assert REGISTRY["kvk"].info.name in md


# ---- every source error is a degradation -----------------------------------


def test_source_errors_become_degradations_worded_by_kind() -> None:
    degraded: list[DegradedSource] = []
    degradation.add_source_errors(
        degraded,
        {"openaleph": "ReadTimeout: read timed out", "kvk": "HTTPStatusError: HTTP 429 Too Many Requests"},
        with_data={"openaleph"},
    )
    by_id = {d.source_id: d for d in degraded}
    assert by_id["openaleph"].check == degradation.CHECK_SOURCE_READ
    assert by_id["openaleph"].reason == degradation.REASON_TIMEOUT
    assert by_id["kvk"].check == degradation.CHECK_SOURCE_FETCH
    assert by_id["kvk"].reason == degradation.REASON_RATE_LIMITED


def test_an_adapter_recorded_degradation_is_not_repeated() -> None:
    degraded = [
        DegradedSource("jar_lithuania", degradation.CHECK_SOURCE_FETCH, [], "HTTP 403", "upstream_error"),
        DegradedSource("opensanctions", "cross_source_names", [], "x", "timeout"),
    ]
    degradation.add_source_errors(
        degraded, {"jar_lithuania": "boom", "opensanctions": "boom"}, with_data=set()
    )
    assert [(d.source_id, d.check) for d in degraded] == [
        ("jar_lithuania", "source_fetch"),
        ("opensanctions", "cross_source_names"),
        # A derived screen that failed is a different gap from the source
        # itself not answering, so the source record is still added.
        ("opensanctions", "source_fetch"),
    ]


def test_the_verdict_says_a_partial_answer_is_partial() -> None:
    sentence = build_verdict(
        [],
        [{"source_id": "openaleph", "check": "source_read", "reason": "timeout"}],
    )
    assert sentence == "No risk signals surfaced, but one source answered only in part."


# ---- end to end through the pipeline ----------------------------------------


@pytest.fixture
def client(monkeypatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{LEI}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "statementId": "e-subject",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Bundle Co P.L.C.",
                    "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                    "identifiers": [{"id": LEI, "scheme": "XI-LEI"}, {"id": "12345678", "scheme": "GB-COH"}],
                },
            }
        )
        + "\n"
    )
    yield TestClient(app)
    get_settings.cache_clear()


def test_a_source_that_failed_is_in_degraded_sources(client, monkeypatch) -> None:
    ch = REGISTRY["companies_house"]

    async def hung(*_a, **_kw):
        await asyncio.sleep(30)

    monkeypatch.setattr(ch, "fetch", hung)
    monkeypatch.setattr(type(ch), "lookup_timeout_s", 0.05)
    body = client.get("/lookup", params={"lei": LEI}).json()
    assert "companies_house" in body["errors"]
    rows = [d for d in body["degraded_sources"] if d["source_id"] == "companies_house"]
    assert rows and rows[0]["check"] == "source_fetch" and rows[0]["reason"] == "timeout"
    assert "did not answer" in (body["verdict"] or "")


def test_a_source_whose_read_failed_is_a_partial_answer(client, monkeypatch) -> None:
    import opencheck.routers.lookup as lookup_mod

    real = lookup_mod._safe_deepen

    async def failing(source_id, hit_id):
        if source_id == "gleif":
            raise TimeoutError("ReadTimeout")
        return await real(source_id, hit_id)

    monkeypatch.setattr(lookup_mod, "_safe_deepen", failing)
    body = client.get("/lookup", params={"lei": LEI}).json()
    assert any(h["source_id"] == "gleif" for h in body["hits"])
    rows = [d for d in body["degraded_sources"] if d["source_id"] == "gleif"]
    assert rows and rows[0]["check"] == "source_read"
    assert rows[0]["detail"] == degradation.SOURCE_READ_DETAIL
