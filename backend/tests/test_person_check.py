"""Tests for the BackgroundCheck /person-check endpoint (spike)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.routers.person_check import (
    STRONG_MATCH_THRESHOLD,
    _score_hit,
)
from opencheck.sources import REGISTRY, SearchKind, SourceHit


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _hit(
    source_id: str,
    name: str,
    *,
    hit_id: str = "h1",
    raw: dict | None = None,
    is_stub: bool = False,
) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=SearchKind.PERSON,
        name=name,
        summary="",
        identifiers={},
        raw=raw or {},
        is_stub=is_stub,
    )


# ---------------------------------------------------------------------
# Endpoint shape (stub mode — no live keys in tests)
# ---------------------------------------------------------------------


def test_person_check_returns_expected_shape(client: TestClient) -> None:
    r = client.get("/person-check", params={"name": "Jane Example"})
    assert r.status_code == 200
    body = r.json()
    assert body["query"] == "Jane Example"
    assert body["birth_year"] is None
    assert isinstance(body["matches"], list)
    assert isinstance(body["risk_signals"], list)
    assert len(body["caveats"]) == 2

    source_ids = {s["source_id"] for s in body["sources"]}
    # Every person-capable registry adapter must be accounted for.
    expected = {
        sid
        for sid, adapter in REGISTRY.items()
        if SearchKind.PERSON in adapter.info.supports
    }
    assert source_ids == expected
    # Attribution/licence must ride along for every checked source.
    for s in body["sources"]:
        assert s["license"]
        assert s["attribution"]


def test_person_check_requires_name(client: TestClient) -> None:
    assert client.get("/person-check").status_code == 422
    assert client.get("/person-check", params={"name": "x"}).status_code == 422


def test_stub_hits_never_produce_risk_signals(client: TestClient) -> None:
    # In test/offline mode adapters return stub hits; assess_hit skips
    # stubs, so no signal may claim a real person is a PEP off fictional
    # stub data.
    r = client.get("/person-check", params={"name": "Vladimir Putin"})
    assert r.status_code == 200
    assert r.json()["risk_signals"] == []


# ---------------------------------------------------------------------
# Match scoring
# ---------------------------------------------------------------------


def test_exact_name_is_strong_match() -> None:
    m = _score_hit(_hit("opensanctions", "Jane Example"), "Jane Example", None)
    assert m.name_score == 1.0
    assert m.strong


def test_dissimilar_name_is_weak_match() -> None:
    m = _score_hit(
        _hit("opensanctions", "John Entirely Different"), "Jane Example", None
    )
    assert m.name_score < STRONG_MATCH_THRESHOLD
    assert not m.strong


def test_birth_year_mismatch_blocks_strong_match() -> None:
    raw = {"properties": {"birthDate": ["1950-01-01"]}}
    m = _score_hit(_hit("opensanctions", "Jane Example", raw=raw), "Jane Example", 1980)
    assert m.name_score == 1.0
    assert not m.birth_year_compatible
    assert not m.strong


def test_birth_year_match_corroborates() -> None:
    raw = {"properties": {"birthDate": ["1980-06-01"]}}
    m = _score_hit(_hit("opensanctions", "Jane Example", raw=raw), "Jane Example", 1980)
    assert m.birth_year_compatible
    assert m.strong


# ---------------------------------------------------------------------
# Signal gating + evidence (patched adapters)
# ---------------------------------------------------------------------


def test_signals_only_from_strong_matches_and_carry_match_evidence(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    strong = _hit(
        "everypolitician",
        "Jane Example",
        hit_id="ep-strong",
        raw={"properties": {"position": ["Member of Parliament"]}},
    )
    weak = _hit(
        "everypolitician",
        "Janet Exampleton-Smythe of Somewhere",
        hit_id="ep-weak",
    )

    async def fake_run_adapters(q, kind):
        return {"everypolitician": [strong, weak]}, {}

    monkeypatch.setattr(
        "opencheck.routers.person_check._run_adapters", fake_run_adapters
    )

    r = client.get(
        "/person-check", params={"name": "Jane Example", "birth_year": 1980}
    )
    assert r.status_code == 200
    body = r.json()

    # Both hits returned, strong first.
    assert [m["hit"]["hit_id"] for m in body["matches"]] == ["ep-strong", "ep-weak"]
    assert body["matches"][0]["strong"] is True
    assert body["matches"][1]["strong"] is False
    assert body["weak_match_count"] == 1

    # Exactly one PEP signal — from the strong hit only — with the
    # match block in its evidence.
    assert len(body["risk_signals"]) == 1
    sig = body["risk_signals"][0]
    assert sig["code"] == "PEP"
    assert sig["hit_id"] == "ep-strong"
    match_ev = sig["evidence"]["match"]
    assert match_ev["query_name"] == "Jane Example"
    assert match_ev["name_score"] == 1.0
    assert match_ev["birth_year_checked"] is True


def test_source_errors_are_reported_not_silent(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_run_adapters(q, kind):
        return {"opensanctions": []}, {"opensanctions": "TimeoutError: boom"}

    monkeypatch.setattr(
        "opencheck.routers.person_check._run_adapters", fake_run_adapters
    )

    r = client.get("/person-check", params={"name": "Jane Example"})
    assert r.status_code == 200
    by_id = {s["source_id"]: s for s in r.json()["sources"]}
    assert by_id["opensanctions"]["error"] == "TimeoutError: boom"
