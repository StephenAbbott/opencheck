"""Integration tests for the /narrative endpoint.

The real Anthropic call and the real lookup pipeline are both mocked: we assert
the endpoint's wiring (flag/key gating, packet build, validated response shape),
not the model. The narrative *content* is covered offline by the eval harness.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.narrative.summarise import NarrativeResult
from opencheck.narrative.validate import ValidationResult
from opencheck.routers.lookup import LookupResponse
from opencheck.sources.base import SearchKind


@pytest.fixture(autouse=True)
def _clear_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _fake_lookup_response() -> LookupResponse:
    return LookupResponse(
        query="2138000000000000A001",
        kind=SearchKind.ENTITY,
        hits=[],
        errors={},
        cross_source_links=[],
        risk_signals=[],
        bods=[
            {
                "statementId": "ent-1",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Northwind Logistics Ltd",
                    "jurisdiction": {"name": "United Kingdom"},
                    "identifiers": [{"scheme": "XI-LEI", "id": "2138000000000000A001"}],
                },
                "source": {"description": "UK Companies House", "type": ["officialRegister"]},
            }
        ],
        bods_issues=[],
        license_notices=[],
        lei="2138000000000000A001",
        legal_name="Northwind Logistics Ltd",
        jurisdiction="GB",
        derived_identifiers={},
    )


async def _fake_lookup(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
    return _fake_lookup_response()


def _fake_summarise(packet, *, api_key, model, temperature=0.0):
    validation = ValidationResult(
        ok=True,
        valid_claims=[{"id": "c1", "text": "X is registered.", "fact_ids": ["f1"],
                       "confidence": "high"}],
        summary="Northwind Logistics Ltd is a registered entity.",
        overall_confidence="high",
    )
    return NarrativeResult(
        summary=validation.summary,
        claims=validation.valid_claims,
        limitations=[],
        overall_confidence="high",
        model=model,
        validation=validation,
    )


def test_narrative_endpoint_returns_grounded_summary(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("OPENCHECK_NARRATIVE_ENABLED", "true")
    get_settings.cache_clear()
    monkeypatch.setattr("opencheck.routers.narrative.lookup", _fake_lookup)
    monkeypatch.setattr("opencheck.routers.narrative.summarise", _fake_summarise)

    client = TestClient(app)
    r = client.get("/narrative", params={"lei": "2138000000000000A001"})
    assert r.status_code == 200
    body = r.json()
    assert body["subject_name"] == "Northwind Logistics Ltd"
    assert body["summary"]
    assert body["claims"] and body["claims"][0]["fact_ids"] == ["f1"]
    assert body["validation_ok"] is True
    # The packet is returned so the UI can resolve cited ids to evidence.
    assert "facts" in body["packet"]
    assert body["prompt_version"]


def test_narrative_endpoint_503_without_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")  # don't pick up a dev .env key
    monkeypatch.setenv("OPENCHECK_NARRATIVE_ENABLED", "true")
    get_settings.cache_clear()
    client = TestClient(app)
    r = client.get("/narrative", params={"lei": "2138000000000000A001"})
    assert r.status_code == 503


def test_narrative_endpoint_404_when_disabled(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.setenv("OPENCHECK_NARRATIVE_ENABLED", "false")
    get_settings.cache_clear()
    client = TestClient(app)
    r = client.get("/narrative", params={"lei": "2138000000000000A001"})
    assert r.status_code == 404
