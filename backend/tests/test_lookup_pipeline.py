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
    # Pin the climatetrace in-memory indexes to empty so pipeline runs are
    # deterministic and never download GEM data mid-test.
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
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
# Replay cache + per-source retry (/lookup-source)
# ---------------------------------------------------------------------------


def test_completed_lookup_is_served_from_replay_cache(
    client: TestClient, tmp_path: Path, monkeypatch
) -> None:
    """The second identical lookup replays cached events — the pipeline
    runs exactly once."""
    from opencheck.routers import lookup as lookup_mod

    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)

    calls = {"n": 0}
    real_pipeline = lookup_mod._lookup_pipeline

    async def counting_pipeline(*args, **kwargs):
        calls["n"] += 1
        async for ev in real_pipeline(*args, **kwargs):
            yield ev

    monkeypatch.setattr(lookup_mod, "_lookup_pipeline", counting_pipeline)

    first = client.get("/lookup", params={"lei": lei}).json()
    second = client.get("/lookup", params={"lei": lei}).json()
    assert calls["n"] == 1
    assert second == first

    # refresh=true bypasses the cache and re-runs the pipeline.
    client.get("/lookup", params={"lei": lei, "refresh": "true"})
    assert calls["n"] == 2


def test_failed_lookup_is_not_cached(client: TestClient, monkeypatch) -> None:
    """Runs that abort before "done" (e.g. unknown LEI) must not be cached."""
    from opencheck.routers import lookup as lookup_mod

    r1 = client.get("/lookup", params={"lei": "ZZZZ00000000000000ZZ"})
    assert r1.status_code == 404
    assert lookup_mod._REPLAY_CACHE == {}


def test_lookup_source_retries_one_source(
    client: TestClient, tmp_path: Path
) -> None:
    """Per-source retry re-runs just the requested source and reports
    stub results as zero hits without an error."""
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)

    r = client.get(
        "/lookup-source", params={"lei": lei, "source_id": "companies_house"}
    )
    assert r.status_code == 200
    body = r.json()
    assert body["lei"] == lei
    assert body["source_id"] == "companies_house"
    assert body["error"] is None
    assert body["hits"] == []  # offline → CH stub → no hit, no error


def test_lookup_source_rejects_inapplicable_source(
    client: TestClient, tmp_path: Path
) -> None:
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)  # GB bundle — no Danish CVR identifier
    r = client.get("/lookup-source", params={"lei": lei, "source_id": "cvr_denmark"})
    assert r.status_code == 404
    assert "not applicable" in r.json()["detail"]


def test_lookup_source_rejects_invalid_lei(client: TestClient) -> None:
    r = client.get("/lookup-source", params={"lei": "nope", "source_id": "kvk"})
    assert r.status_code == 400


def test_lookup_source_invalidates_replay_cache(
    client: TestClient, tmp_path: Path
) -> None:
    from opencheck.routers import lookup as lookup_mod

    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    client.get("/lookup", params={"lei": lei})
    assert lookup_mod._REPLAY_CACHE  # completed run cached
    client.get("/lookup-source", params={"lei": lei, "source_id": "companies_house"})
    assert lookup_mod._REPLAY_CACHE == {}


# ---------------------------------------------------------------------------
# Per-source wall-clock budgets
# ---------------------------------------------------------------------------


def test_hung_source_is_cut_off_by_its_time_budget(
    client: TestClient, tmp_path: Path, monkeypatch
) -> None:
    """A source that never returns is cancelled at its budget and reported
    as a timeout — the lookup itself still completes."""
    import asyncio as _asyncio

    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)  # GB → companies_house dispatched

    ch_adapter = REGISTRY["companies_house"]

    async def hung_fetch(*_a, **_kw):
        await _asyncio.sleep(30)

    monkeypatch.setattr(ch_adapter, "fetch", hung_fetch)
    monkeypatch.setattr(type(ch_adapter), "lookup_timeout_s", 0.05)

    r = client.get("/lookup", params={"lei": lei})
    assert r.status_code == 200
    body = r.json()
    assert "companies_house" in body["errors"]
    assert "time budget" in body["errors"]["companies_house"]
    # The rest of the lookup is unaffected.
    assert any(h["source_id"] == "gleif" for h in body["hits"])


def test_timeout_emits_timeout_error_type_on_stream(
    client: TestClient, tmp_path: Path, monkeypatch
) -> None:
    import asyncio as _asyncio

    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    ch_adapter = REGISTRY["companies_house"]

    async def hung_fetch(*_a, **_kw):
        await _asyncio.sleep(30)

    monkeypatch.setattr(ch_adapter, "fetch", hung_fetch)
    monkeypatch.setattr(type(ch_adapter), "lookup_timeout_s", 0.05)

    events = _stream_events(_stream_body(client, lei))
    timeout_errors = [
        p for n, p in events
        if n == "source_error" and p["source_id"] == "companies_house"
    ]
    assert timeout_errors and timeout_errors[0]["error_type"] == "timeout"


def test_every_adapter_declares_a_sane_budget() -> None:
    """Budgets must exist and be positive; slow-by-design adapters may
    exceed the default but nothing should be unbounded."""
    for sid, adapter in REGISTRY.items():
        budget = adapter.lookup_timeout_s
        assert 0 < budget <= 120, f"{sid} has unreasonable budget {budget}"


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
    for deriver in _RA_DERIVERS:
        assert deriver.derived_key in dispatchable, (
            f"derived key {deriver.derived_key!r} has no dispatch spec"
        )


def test_dispatch_specs_come_from_adapter_declarations() -> None:
    """The dispatch table is built from the adapters' own lookup specs —
    one spec per registry adapter that declares lookup keys."""
    declaring = {
        sid for sid, a in REGISTRY.items() if a.lookup_keys()
    }
    assert {s.source_id for s in _REGISTRY_SOURCES} == declaring
    for spec in _REGISTRY_SOURCES:
        adapter = REGISTRY[spec.source_id]
        assert spec.derived_keys == adapter.lookup_keys()
        assert spec.pass_legal_name == adapter.lookup_pass_legal_name
        assert callable(spec.build)


def test_missing_hit_builder_fails_fast() -> None:
    """An adapter declaring lookup keys without a _bh_<id>() builder must
    blow up at import/collection time, not silently at runtime."""
    from unittest.mock import patch

    from opencheck.routers import lookup as lookup_mod
    from opencheck.sources.base import SourceAdapter, SourceInfo
    from opencheck.sources import SearchKind

    class GhostAdapter(SourceAdapter):
        id = "ghost_register"
        lookup_dispatch_keys = ("ghost_id",)

        @property
        def info(self) -> SourceInfo:  # pragma: no cover - never called
            raise NotImplementedError

        async def search(self, query: str, kind: SearchKind):  # pragma: no cover
            return []

        async def fetch(self, hit_id: str):  # pragma: no cover
            return {}

    fake_registry = dict(REGISTRY)
    fake_registry["ghost_register"] = GhostAdapter()
    with patch.object(lookup_mod, "REGISTRY", fake_registry):
        with pytest.raises(RuntimeError, match="_bh_ghost_register"):
            lookup_mod._collect_registry_sources()


def test_mapper_convention_covers_all_dispatch_sources() -> None:
    """Every lookup-dispatched adapter has a map_<id>() BODS mapper
    reachable by the naming convention (no hand-maintained mapper dict)."""
    from opencheck.routers.lookup import _mapper_for

    for spec in _REGISTRY_SOURCES:
        assert callable(_mapper_for(spec.source_id)), (
            f"no map_{spec.source_id}() exported from opencheck.bods"
        )
    # And the LEI-keyed specials used by the pipeline.
    for sid in ("gleif", "wikidata", "opencorporates", "opensanctions",
                "openaleph", "climatetrace", "sec_edgar"):
        assert callable(_mapper_for(sid)), sid


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
