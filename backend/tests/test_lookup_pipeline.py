"""Tests for the unified lookup pipeline driving /lookup and /lookup-stream.

Until Phase 47 the two endpoints were hand-synchronised copies; these tests
pin the single-pipeline contract: both endpoints draw from
``_lookup_pipeline()`` and therefore cannot diverge.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers.lookup import (
    _RA_DERIVERS,
    _REGISTRY_SOURCES,
    _LookupCtx,
    _build_derived,
)
from opencheck.sources import REGISTRY


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _seed_bundle(tmp_path: Path, lei: str) -> None:
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "statementId": "e-subject",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Bundle Co P.L.C.",
                    "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                    "identifiers": [
                        {"id": lei, "scheme": "XI-LEI"},
                        {"id": "12345678", "scheme": "GB-COH"},
                    ],
                },
            }
        )
        + "\n"
    )


def _stream_body(client: TestClient, lei: str) -> str:
    with client.stream("GET", "/lookup-stream", params={"lei": lei}) as r:
        assert r.status_code == 200
        return "".join(chunk for chunk in r.iter_text())


def _stream_events(body: str) -> list[tuple[str, dict]]:
    """Parse SSE frames into (event, payload) pairs."""
    events: list[tuple[str, dict]] = []
    current_event: str | None = None
    for line in body.splitlines():
        if line.startswith("event: "):
            current_event = line[len("event: "):]
        elif line.startswith("data: ") and current_event:
            events.append((current_event, json.loads(line[len("data: "):])))
            current_event = None
    return events


# ---------------------------------------------------------------------------
# Stream error handling (mirrors the /lookup HTTP errors)
# ---------------------------------------------------------------------------


def test_stream_rejects_invalid_lei(client: TestClient) -> None:
    events = _stream_events(_stream_body(client, "not-an-lei"))
    assert events, "expected at least one SSE event"
    name, payload = events[0]
    assert name == "error"
    assert "20-character" in payload["detail"]
    assert payload["status"] == 400


def test_stream_unknown_lei_emits_404_error(client: TestClient) -> None:
    events = _stream_events(_stream_body(client, "ZZZZ00000000000000ZZ"))
    error_events = [p for n, p in events if n == "error"]
    assert error_events and "No GLEIF record" in error_events[0]["detail"]
    assert error_events[0]["status"] == 404


# ---------------------------------------------------------------------------
# Parity: /lookup and /lookup-stream see the same pipeline output
# ---------------------------------------------------------------------------


def test_sync_and_stream_agree_on_offline_bundle(
    client: TestClient, tmp_path: Path
) -> None:
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)

    sync = client.get("/lookup", params={"lei": lei}).json()
    events = _stream_events(_stream_body(client, lei))

    by_name: dict[str, list[dict]] = {}
    for name, payload in events:
        by_name.setdefault(name, []).append(payload)

    # gleif_done carries the same anchor metadata as the sync response.
    gleif_done = by_name["gleif_done"][0]
    assert gleif_done["legal_name"] == sync["legal_name"] == "Bundle Co P.L.C."
    assert gleif_done["jurisdiction"] == sync["jurisdiction"] == "GB"
    assert (
        gleif_done["derived_identifiers"]
        == sync["derived_identifiers"]
    )
    assert sync["derived_identifiers"]["gb_coh"] == "12345678"

    # Identical hit sets (source_id, hit_id) in both views.
    stream_hits = {(p["source_id"], p["hit_id"]) for p in by_name.get("hit", [])}
    sync_hits = {(h["source_id"], h["hit_id"]) for h in sync["hits"]}
    assert stream_hits == sync_hits
    assert ("gleif", lei) in sync_hits

    # Stream terminates with done carrying the same issue list.
    done = by_name["done"][0]
    assert done["lei"] == lei
    assert done["bods_issues"] == sync["bods_issues"]

    # Risk signals match.
    stream_signals = by_name["risk_signals"][0]["signals"]
    assert {s["code"] for s in stream_signals} == {
        s["code"] for s in sync["risk_signals"]
    }


def test_stream_announces_applicable_sources(client: TestClient, tmp_path: Path) -> None:
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    events = _stream_events(_stream_body(client, lei))
    applicable = [p for n, p in events if n == "sources_applicable"][0]["source_ids"]
    # GB bundle → Companies House dispatched, plus the LEI-keyed sources.
    assert "companies_house" in applicable
    started = {p["source_id"] for n, p in events if n == "source_started"}
    completed = {p["source_id"] for n, p in events if n == "source_completed"}
    # Every announced source starts and completes (sec_edgar is deferred).
    for sid in applicable:
        if sid != "sec_edgar":
            assert sid in started
            assert sid in completed or any(
                p["source_id"] == sid for n, p in events if n == "source_error"
            )


# ---------------------------------------------------------------------------
# Single-place wiring guarantees (replaces the old "two-place rule")
# ---------------------------------------------------------------------------


def test_registry_sources_exist_in_registry() -> None:
    """Every dispatch spec points at a registered adapter."""
    for spec in _REGISTRY_SOURCES:
        assert spec.source_id in REGISTRY, spec.source_id
        assert spec.derived_keys, spec.source_id


def test_every_ra_derived_key_has_a_dispatch_spec() -> None:
    """An RA-code deriver without a dispatch entry would silently never
    fire — exactly the class of bug the old duplicated blocks produced."""
    dispatchable = {k for spec in _REGISTRY_SOURCES for k in spec.derived_keys}
    for _codes, key, _norm in _RA_DERIVERS:
        assert key in dispatchable, f"derived key {key!r} has no dispatch spec"


def test_build_derived_maps_ra_codes() -> None:
    from opencheck.sources.ariregister import EE_RA_CODE
    from opencheck.sources.brreg import NO_RA_CODE
    from opencheck.sources.cvr_denmark import DK_CVR_RA_CODE

    cases = [
        ("RA000585", "GB", "00102498", "gb_coh", "00102498"),
        (DK_CVR_RA_CODE, "DK", "12345678", "dk_cvr", "12345678"),
        (EE_RA_CODE, "EE", "1234567", "ee_registry_code", "01234567"),
        (NO_RA_CODE, "NO", "923 609 016", "no_orgnr", "923609016"),
    ]
    for ra_code, jur, registered_as, key, expected in cases:
        ctx = _LookupCtx(lei="X" * 20)
        ctx.jurisdiction = jur
        ctx.registered_as = registered_as
        _build_derived(ctx, ra_code)
        assert ctx.derived.get(key) == expected, (ra_code, key, ctx.derived)


def test_build_derived_skips_malformed_local_id() -> None:
    """A ValueError from a normaliser skips the source instead of crashing."""
    from opencheck.sources.bolagsverket import BV_RA_CODE

    ctx = _LookupCtx(lei="X" * 20)
    ctx.jurisdiction = "SE"
    ctx.registered_as = "not-a-number"
    _build_derived(ctx, BV_RA_CODE)  # Bolagsverket — strict normaliser
    assert "se_org_number" not in ctx.derived
    assert ctx.derived["lei"] == "X" * 20
