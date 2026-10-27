"""A source that degrades instead of raising is a source that did not answer.

The KvK request-pacing follow-up (27 Oct 2026). When KvK refused a request (a
429, or the Phase 412 throttle declining to send), the adapter recorded a
``source_fetch`` degradation and returned a stub, as Phase 324 asked. The
pipeline then emitted ``source_completed`` with no hits and no error, so the
report listed KvK under "Answered with no record", counted it as answered,
and gave it no card — and no Try again button — while the degraded-sources
notice above said it had been rate-limited. The same was true of every adapter
that degrades this way.

``_source_outcome`` now reads a recorded ``source_fetch`` failure for a source
that produced nothing as an error, in the run and in the per-source retry.
"""

from __future__ import annotations

import pytest

from opencheck import coverage, degradation
from opencheck.sources import REGISTRY
from tests.test_lookup_pipeline import _stream_body, _stream_events
from tests.test_phase331_lookup_source import LEI, _BUNDLE, _retry, client  # noqa: F401

SOURCE = "companies_house"
DETAIL = "Companies House did not answer (HTTP 429)"


def _degrading_fetch(*, reason: str, stub: bool = True):
    """A fetch that records a ``source_fetch`` degradation, as KvK's does."""

    async def _fetch(local_id: str, **_kw) -> dict:
        degradation.record(SOURCE, DETAIL, reason=reason)
        return {"is_stub": True} if stub else dict(_BUNDLE)

    return _fetch


# ---------------------------------------------------------------------------
# The retry
# ---------------------------------------------------------------------------


def test_a_degraded_stub_is_an_error_on_retry(client, monkeypatch) -> None:
    monkeypatch.setattr(
        REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=degradation.REASON_RATE_LIMITED)
    )
    body = _retry(client)
    assert body["hits"] == []
    assert body["error"] == DETAIL
    assert body["error_type"] == "fetch_error"
    assert body["source_liveness"][SOURCE]["liveness"] == "stub"


def test_a_timeout_keeps_its_type(client, monkeypatch) -> None:
    monkeypatch.setattr(
        REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=degradation.REASON_TIMEOUT)
    )
    assert _retry(client)["error_type"] == "timeout"


def test_a_missing_credential_is_not_a_failed_request(client, monkeypatch) -> None:
    """``not_configured`` stays as it was: retrying changes nothing."""
    monkeypatch.setattr(
        REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=degradation.REASON_NOT_CONFIGURED)
    )
    body = _retry(client)
    assert body["hits"] == [] and body["error"] is None


def test_a_card_with_a_degradation_keeps_its_card(client, monkeypatch) -> None:
    """A source that answered with a record and noted a problem (a note card,
    a partial read) is not turned into an error: only an empty answer is."""
    monkeypatch.setattr(
        REGISTRY[SOURCE],
        "fetch",
        _degrading_fetch(reason=degradation.REASON_RATE_LIMITED, stub=False),
    )
    body = _retry(client)
    assert [h["source_id"] for h in body["hits"]] == [SOURCE]
    assert body["error"] is None


def test_a_plain_stub_is_still_no_record(client, monkeypatch) -> None:
    async def _stub(local_id: str, **_kw) -> dict:
        return {"is_stub": True}

    monkeypatch.setattr(REGISTRY[SOURCE], "fetch", _stub)
    body = _retry(client)
    assert body["hits"] == [] and body["error"] is None


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def test_the_run_reports_it_as_not_answering(client, monkeypatch) -> None:
    monkeypatch.setattr(
        REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=degradation.REASON_RATE_LIMITED)
    )
    run = client.get("/lookup", params={"lei": LEI, "refresh": "true"}).json()

    assert run["errors"].get(SOURCE) == DETAIL
    assert not [h for h in run["hits"] if h["source_id"] == SOURCE]
    # One degradation, the adapter's own — add_source_errors does not repeat it.
    mine = [d for d in run["degraded_sources"] if d["source_id"] == SOURCE]
    assert [(d["check"], d["reason"], d["detail"]) for d in mine] == [
        (degradation.CHECK_SOURCE_FETCH, degradation.REASON_RATE_LIMITED, DETAIL)
    ]
    cov = coverage.source_coverage(run["hits"], run["errors"], run["sources_applicable"])
    assert SOURCE in cov["failed_ids"]
    assert SOURCE not in cov["no_record_ids"]
    assert SOURCE not in cov["answered_ids"]


def test_the_stream_says_source_error_not_source_completed(client, monkeypatch) -> None:
    monkeypatch.setattr(
        REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=degradation.REASON_RATE_LIMITED)
    )
    events = _stream_events(_stream_body(client, LEI))
    mine = [(e, p) for e, p in events if isinstance(p, dict) and p.get("source_id") == SOURCE]
    kinds = [e for e, _ in mine]
    assert "source_error" in kinds
    assert "source_completed" not in kinds
    [(_, err)] = [(e, p) for e, p in mine if e == "source_error"]
    assert err["error"] == DETAIL and err["error_type"] == "fetch_error"


@pytest.mark.parametrize(
    "reason",
    [degradation.REASON_RATE_LIMITED, degradation.REASON_NOT_CONFIGURED],
    ids=["rate-limited", "not-configured"],
)
def test_run_and_retry_agree(client, monkeypatch, reason) -> None:
    monkeypatch.setattr(REGISTRY[SOURCE], "fetch", _degrading_fetch(reason=reason))
    run = client.get("/lookup", params={"lei": LEI, "refresh": "true"}).json()
    retry = _retry(client)
    assert run["errors"].get(SOURCE) == retry["error"]


# ---------------------------------------------------------------------------
# The helper
# ---------------------------------------------------------------------------


def test_unanswered_fetch_outside_a_scope_is_none() -> None:
    assert degradation.unanswered_fetch(SOURCE) is None


def test_unanswered_fetch_reads_only_fetch_failures() -> None:
    with degradation.recording():
        degradation.record(SOURCE, "read failed", check=degradation.CHECK_SOURCE_READ)
        degradation.record("other", DETAIL, reason=degradation.REASON_TIMEOUT)
        assert degradation.unanswered_fetch(SOURCE) is None
        found = degradation.unanswered_fetch("other")
        assert found is not None and found.detail == DETAIL

