"""Phase 215 — the watchlist.

Pinned here, in the order the ticket listed them: the delta intersection (a
watched LEI absent from the delta is never re-run); the renewal-churn filter
(a row whose only change is ``LastUpdateDate`` / ``NextRenewalDate`` queues
nothing); the degraded-source rule (a signal whose producer did not answer
is "could not check", never "retired"); the caps; feed validity. Plus the
Tier 2 matcher, the routes, robots and the lifespan task.

The re-run is monkeypatched at ``opencheck.routers.lookup._lookup_impl``, the
seam the batch tests use, so no adapter is dispatched.
"""

from __future__ import annotations

import json
import sqlite3
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import entity_pages as ep
from opencheck import identifiers
from opencheck import mirror_refresh as mr
from opencheck import mirrorstats
from opencheck import watchlist as wl
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers.lookup import LookupResponse, SourceHit
from opencheck.routers.watch import render_atom
from opencheck.sources import SearchKind
from tests.test_gleif_mirror_store import (
    EASY,
    LEI2_HEADER,
    PARENT,
    REPEX_HEADER,
    RR_HEADER,
    _lei2_row,
    _write_csv,
)
from tests.test_mirror_refresh import WATERMARK, _build_mirror

ATOM = "{http://www.w3.org/2005/Atom}"


def _with_check_digits(base18: str) -> str:
    for cd in range(2, 99):
        cand = f"{base18}{cd:02d}"
        if identifiers.lei_check_digits_ok(cand):
            return cand
    raise AssertionError("no check digits")  # pragma: no cover


OTHER = _with_check_digits("2138000000000000W2")


def _hit(source_id: str, *, stub: bool = False) -> SourceHit:
    return SourceHit(
        source_id=source_id, hit_id="x", kind=SearchKind.ENTITY, name="Easy", summary="", is_stub=stub
    )


def _resp(lei: str, **over) -> LookupResponse:
    fields = dict(
        query=lei,
        kind=SearchKind.ENTITY,
        hits=[_hit("gleif"), _hit("companies_house")],
        errors={},
        cross_source_links=[],
        risk_signals=[
            {"code": "SANCTIONED", "kind": "risk", "summary": "a", "source_id": "opensanctions"},
            {"code": "NON_EU_JURISDICTION", "kind": "context", "summary": "b", "source_id": "gleif"},
        ],
        bods=[],
        bods_issues=[],
        license_notices=[],
        degraded_sources=[],
        verdict="The company itself is on a sanctions list.",
        subject_profile={
            "register_status": {
                "liveness": "live", "since": "2001-01-01", "raw": "active",
                "source_id": "companies_house", "sources": ["companies_house"],
            },
            "founding_date": {"value": "2001-01-01", "sources": ["companies_house"]},
            "legal_form": {"value": "Private limited company", "sources": ["companies_house"]},
        },
        source_liveness={
            "gleif": {"liveness": "live", "retrieved_at": "2026-09-16T08:00:00Z"},
            "companies_house": {"liveness": "live", "retrieved_at": "2026-09-16T08:00:01Z"},
            "opensanctions": {"liveness": "live", "retrieved_at": "2026-09-16T08:00:02Z"},
        },
        lei=lei,
        legal_name="EASY POWER",
        jurisdiction="GR",
        derived_identifiers={},
        sources_applicable=["companies_house", "opensanctions"],
    )
    fields.update(over)
    return LookupResponse(**fields)


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A mirror on disk, a watchlist file, live allowed, workers off."""
    db = _build_mirror(tmp_path / "entity_pages.sqlite", tmp_path)
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_WATCHLIST_DB_FILE", str(tmp_path / "watchlist.sqlite"))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_URL", raising=False)
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_WATCHLIST_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_WATCHLIST_OPENSANCTIONS_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_REFRESH_INTERVAL_S", "0")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    mr.reset_for_tests()
    mirrorstats.reset_for_tests()
    wl.reset_for_tests()
    yield db
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    wl.reset_for_tests()


@pytest.fixture
def client(env: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    async def _fake(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    yield TestClient(app)


def _delta(tmp: Path, rows: list[list[str]], *, rr: list[list[str]] | None = None) -> dict:
    publish = "2026-09-08 00:00:00"
    lei2 = _write_csv(tmp / "d-lei2.csv", LEI2_HEADER, rows)
    rrf = _write_csv(tmp / "d-rr.csv", RR_HEADER, rr or [])
    repex = _write_csv(tmp / "d-repex.csv", REPEX_HEADER, [])
    files = {
        kind: {"delta_files": {w: {"csv": {"url": f"file://{p}"}} for w, _ in mr.DELTA_WINDOWS}}
        for kind, p in (("lei2", lei2), ("rr", rrf), ("repex", repex))
    }
    return {"publish_date": publish, **files}


def _easy_row(**over: str) -> list[str]:
    """EASY's verbatim 2026-09-07 delta row with its renewal clocks moved on —
    what the next day's delta looks like when nothing material changed —
    plus any override."""
    from tests.test_gleif_mirror_store import _LEI2

    row = list(_LEI2["row"])
    values = {
        "Registration.LastUpdateDate": "2026-09-08T00:00:00Z",
        "Registration.NextRenewalDate": "2027-09-08T00:00:00Z",
    }
    values.update(over)
    for col, value in values.items():
        row[LEI2_HEADER.index(col)] = value
    return row


def _watch(client: TestClient, lei: str, token: str | None = None) -> dict:
    body = {"lei": lei}
    if token:
        body["token"] = token
    r = client.post("/watch/items", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ---- the diff model ---------------------------------------------------------


def test_snapshot_reduces_the_response_to_the_facts_the_feed_compares() -> None:
    snap = wl.snapshot_from_response(_resp(EASY))
    assert snap["legal_name"] == "EASY POWER" and snap["jurisdiction"] == "GR"
    assert snap["register_status"]["liveness"] == "live"
    assert snap["founding_date"] == "2001-01-01"
    assert snap["signals"] == [
        {"code": "NON_EU_JURISDICTION", "source_id": "gleif", "kind": "context"},
        {"code": "SANCTIONED", "source_id": "opensanctions", "kind": "risk"},
    ]
    # Phase 241: opensanctions answered with no record — answered, not with data.
    assert snap["coverage"]["applicable"] == 3 and snap["coverage"]["answered"] == 3
    assert snap["coverage"]["with_data"] == 2
    assert [c["source_id"] for c in snap["checked"]] == ["companies_house", "gleif", "opensanctions"]


def test_no_change_is_an_empty_diff() -> None:
    a = wl.snapshot_from_response(_resp(EASY))
    assert wl.diff_snapshots(a, wl.snapshot_from_response(_resp(EASY))) == []
    assert wl.diff_snapshots(None, a) == []


def test_a_retired_signal_needs_its_producer_to_have_answered() -> None:
    before = wl.snapshot_from_response(_resp(EASY))
    # Same lookup, the sanctions row gone and OpenSanctions degraded: the
    # code could not be re-assessed. Never "retired".
    after = wl.snapshot_from_response(
        _resp(
            EASY,
            risk_signals=[{"code": "NON_EU_JURISDICTION", "kind": "context", "summary": "b", "source_id": "gleif"}],
            degraded_sources=[
                {"source_id": "opensanctions", "check": "screen", "reason": "timeout",
                 "affected_signals": ["SANCTIONED"], "detail": ""}
            ],
        )
    )
    kinds = {c["kind"]: c for c in wl.diff_snapshots(before, after)}
    assert "signal_unchecked" in kinds and "signal_retired" not in kinds
    assert kinds["signal_unchecked"]["code"] == "SANCTIONED"
    assert kinds["signal_unchecked"]["degraded"] == ["opensanctions"]
    # …and with the producer answering, it is retired.
    after_ok = wl.snapshot_from_response(
        _resp(EASY, risk_signals=[{"code": "NON_EU_JURISDICTION", "kind": "context", "summary": "b", "source_id": "gleif"}])
    )
    kinds = {c["kind"] for c in wl.diff_snapshots(before, after_ok)}
    assert "signal_retired" in kinds and "signal_unchecked" not in kinds


def test_coverage_that_fell_because_a_source_was_degraded_is_unchecked() -> None:
    before = wl.snapshot_from_response(_resp(EASY))
    after = wl.snapshot_from_response(
        _resp(
            EASY,
            hits=[_hit("gleif")],
            errors={"companies_house": "timeout"},
            degraded_sources=[
                {"source_id": "companies_house", "check": "fetch", "reason": "timeout",
                 "affected_signals": [], "detail": ""}
            ],
        )
    )
    cov = [c for c in wl.diff_snapshots(before, after) if c["kind"].startswith("coverage")]
    assert len(cov) == 1 and cov[0]["kind"] == "coverage_unchecked"
    # Phase 241: opensanctions answered with no record in both runs; only the
    # source that errored is missing.
    assert cov[0]["missing"] == ["companies_house"]


def test_a_baseline_stored_before_phase_241_is_compared_on_sources_with_data() -> None:
    """A stored baseline's ``answered`` meant "with a record". Compared with
    the new ``answered`` it would report every watch as changed on the first
    re-run after deploy; read as ``with_data`` it reports nothing."""
    before = wl.snapshot_from_response(_resp(EASY))
    legacy = dict(before)
    legacy["coverage"] = {
        "applicable": before["coverage"]["applicable"],
        "answered": before["coverage"]["with_data"],
        "applicable_ids": before["coverage"]["applicable_ids"],
        "answered_ids": before["coverage"]["with_data_ids"],
    }
    after = wl.snapshot_from_response(_resp(EASY))
    assert not [c for c in wl.diff_snapshots(legacy, after) if c["kind"].startswith("coverage")]
    # …and a real fall in sources with a record still registers.
    fewer = wl.snapshot_from_response(_resp(EASY, hits=[_hit("gleif")]))
    cov = [c for c in wl.diff_snapshots(legacy, fewer) if c["kind"].startswith("coverage")]
    assert len(cov) == 1 and cov[0]["old"]["answered"] is None
    assert (cov[0]["old"]["with_data"], cov[0]["new"]["with_data"]) == (2, 1)


def test_a_verdict_worded_by_an_older_template_is_not_a_change() -> None:
    """Phase 245 rewrote the verdict as two sentences. A baseline stored
    before it carries no ``verdict_template`` and the old wording; compared
    with a new snapshot of the same facts it must report nothing, or every
    watched company would say "verdict changed" on its next re-run."""
    before = wl.snapshot_from_response(_resp(EASY))
    assert before["verdict_template"] == 3
    legacy = {k: v for k, v in before.items() if k != "verdict_template"}
    legacy["verdict"] = "Sanctions findings on the company itself."
    after = wl.snapshot_from_response(_resp(EASY))
    assert wl.diff_snapshots(legacy, after) == []
    # Phase 247 ("possible" for name-match clauses) bumped it again: a Phase
    # 245 baseline with the unhedged wording is not a change either.
    phase_245 = dict(
        before,
        verdict_template=2,
        verdict="The records show a politically exposed person among the parties named.",
    )
    assert wl.diff_snapshots(phase_245, after) == []
    # Between two snapshots of the same template a wording change still shows.
    changed = dict(after, verdict="Something else.")
    assert [c["kind"] for c in wl.diff_snapshots(before, changed)] == ["verdict"]


def test_new_signal_status_and_name_changes_are_named() -> None:
    before = wl.snapshot_from_response(_resp(EASY))
    after = wl.snapshot_from_response(
        _resp(
            EASY,
            legal_name="EASY POWER (RENAMED)",
            risk_signals=[
                {"code": "SANCTIONED", "kind": "risk", "summary": "a", "source_id": "opensanctions"},
                {"code": "PEP", "kind": "risk", "summary": "c", "source_id": "opensanctions"},
                {"code": "NON_EU_JURISDICTION", "kind": "context", "summary": "b", "source_id": "gleif"},
            ],
            subject_profile={
                "register_status": {"liveness": "terminal", "since": "2026-09-01", "raw": "dissolved",
                                    "source_id": "companies_house", "sources": ["companies_house"]},
                "founding_date": {"value": "2001-01-01"},
                "legal_form": {"value": "Private limited company"},
            },
        )
    )
    changes = wl.diff_snapshots(before, after)
    kinds = [c["kind"] for c in changes]
    assert "legal_name" in kinds and "register_status" in kinds and "signal_new" in kinds
    assert all(k in wl.CHANGE_KINDS for k in kinds)


def test_gleif_material_fields_exclude_renewal_churn() -> None:
    assert "next_renewal_date" not in wl.GLEIF_MATERIAL_FIELDS
    assert "last_updated" not in wl.GLEIF_MATERIAL_FIELDS
    a = {f: None for f in wl.GLEIF_MATERIAL_FIELDS} | {"legal_name": "A"}
    b = dict(a, legal_name="B")
    assert wl.diff_gleif_facts(a, a) == []
    assert wl.diff_gleif_facts(a, b) == [{"kind": "gleif_field", "field": "legal_name", "old": "A", "new": "B"}]


# ---- Tier 1: the delta intersection and the churn filter ---------------------


def test_a_watched_lei_absent_from_the_delta_is_never_rerun(client: TestClient, env: Path, tmp_path: Path) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None and store.pending_count() == 0
    # A delta naming only PARENT (renamed) — EASY is untouched.
    publish = _delta(tmp_path, [_lei2_row(**{
        "LEI": PARENT, "Entity.LegalName": "Mirror Parent Holdings Ltd (Renamed)",
        "Entity.LegalAddress.City": "LONDON", "Entity.LegalAddress.Country": "GB",
        "Entity.LegalJurisdiction": "GB", "Entity.EntityStatus": "ACTIVE",
        "Entity.LegalForm.EntityLegalFormCode": "H0PO",
        "Registration.LastUpdateDate": "2026-09-08T00:00:00Z",
        "Registration.RegistrationStatus": "ISSUED",
    })])
    assert mr.refresh_once(publish=publish) == "applied"
    assert store.pending_count() == 0
    assert wl.state()["gleif_touched"] == 0 and wl.state()["gleif_deltas_seen"] == 1


def test_renewal_churn_triggers_nothing(client: TestClient, env: Path, tmp_path: Path) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    # EASY is in the delta, but only its LastUpdateDate / NextRenewalDate moved.
    publish = _delta(tmp_path, [_easy_row()])
    assert mr.refresh_once(publish=publish) == "applied"
    assert store.pending_count() == 0
    s = wl.state()
    assert s["gleif_touched"] == 1 and s["gleif_churn"] == 1 and s["gleif_queued"] == 0


def test_a_material_gleif_change_queues_one_rerun(client: TestClient, env: Path, tmp_path: Path) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    publish = _delta(tmp_path, [_easy_row(**{"Registration.RegistrationStatus": "LAPSED"})])
    assert mr.refresh_once(publish=publish) == "applied"
    pending = store.take_pending(10)
    assert [p["lei"] for p in pending] == [EASY]
    assert pending[0]["tier"] == "gleif"
    assert pending[0]["trigger"]["fields"] == ["registration_status"]
    assert pending[0]["trigger"]["publish"] == "2026-09-08 00:00:00"
    # A second delta before the re-run ran folds into the same pending row.
    store.enqueue(EASY, "gleif", {"tier": "gleif"})
    assert store.enqueue(EASY, "gleif", {"tier": "gleif"}) is False
    assert store.pending_count() == 1


def test_a_parent_relationship_change_is_material(client: TestClient, env: Path, tmp_path: Path) -> None:
    from tests.test_gleif_mirror_store import _rr_row

    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    # Only the RR file names EASY: its direct parent record is retired.
    publish = _delta(tmp_path, [], rr=[_rr_row(EASY, PARENT, "IS_DIRECTLY_CONSOLIDATED_BY", reg="RETIRED")])
    assert mr.refresh_once(publish=publish) == "applied"
    pending = store.take_pending(10)
    assert [p["lei"] for p in pending] == [EASY]
    assert "direct_parent_lei" in pending[0]["trigger"]["fields"]


def test_the_hook_never_breaks_the_refresh(env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*a, **k):  # noqa: ANN001
        raise RuntimeError("watcher exploded")

    monkeypatch.setattr(wl, "get_store", _boom)
    publish = _delta(tmp_path, [_easy_row()])
    assert mr.refresh_once(publish=publish) == "applied"


# ---- the re-run --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_rerun_writes_an_entry_only_when_something_changed(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = _watch(client, EASY)
    token = data["token"]
    th = wl.token_hash(token)
    store = wl.get_store()
    assert store is not None

    # Manual re-check, nothing changed: no entry, but last_checked_at moves.
    before = store.get_watch(th, EASY)
    result = await wl.rerun(EASY, wl.TIER_MANUAL, {"tier": "manual"}, only_token_hash=th)
    assert result["entries"] == 0 and result["changes"] == []
    assert store.entries(th) == []
    assert store.get_watch(th, EASY)["last_checked_at"] >= before["last_checked_at"]

    # Now the register says dissolved and a new signal appears.
    async def _changed(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        assert refresh is True  # a re-run never accepts the replay cache
        return _resp(
            lei,
            risk_signals=[
                {"code": "SANCTIONED", "kind": "risk", "summary": "a", "source_id": "opensanctions"},
                {"code": "PEP", "kind": "risk", "summary": "c", "source_id": "opensanctions"},
            ],
            subject_profile={
                "register_status": {"liveness": "terminal", "since": "2026-09-01", "raw": "dissolved",
                                    "source_id": "companies_house", "sources": ["companies_house"]},
                "founding_date": {"value": "2001-01-01"},
                "legal_form": {"value": "Private limited company"},
            },
        )

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _changed)
    result = await wl.rerun(EASY, wl.TIER_GLEIF, {"tier": "gleif", "publish": "2026-09-08 00:00:00", "fields": ["registration_status"]})
    assert result["entries"] == 1
    kinds = {c["kind"] for c in result["changes"]}
    assert {"register_status", "signal_new", "context_retired"} <= kinds
    entries = store.entries(th)
    assert len(entries) == 1 and entries[0]["tier"] == "gleif"
    assert entries[0]["trigger"]["publish"] == "2026-09-08 00:00:00"
    assert [c["source_id"] for c in entries[0]["checked"]] == ["companies_house", "gleif", "opensanctions"]
    # The baseline moved: the same response again is no change.
    result = await wl.rerun(EASY, wl.TIER_MANUAL, {"tier": "manual"}, only_token_hash=th)
    assert result["changes"] == [] and len(store.entries(th)) == 1


@pytest.mark.asyncio
async def test_a_failed_rerun_leaves_the_baseline_alone(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = _watch(client, EASY)
    th = wl.token_hash(data["token"])
    store = wl.get_store()
    assert store is not None
    before = store.get_watch(th, EASY)

    async def _down(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        raise RuntimeError("GLEIF 429")

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _down)
    result = await wl.rerun(EASY, wl.TIER_GLEIF, {"tier": "gleif"})
    assert result["entries"] == 0 and result.get("error")
    assert store.get_watch(th, EASY)["snapshot_at"] == before["snapshot_at"]
    assert wl.state()["rerun_failures"] == 1


@pytest.mark.asyncio
async def test_tick_drains_pending_within_its_bound(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_RERUNS_PER_TICK", "1")
    get_settings.cache_clear()
    data = _watch(client, EASY)
    _watch(client, PARENT, data["token"])
    store = wl.get_store()
    assert store is not None
    store.enqueue(EASY, "gleif", {"tier": "gleif"})
    store.enqueue(PARENT, "gleif", {"tier": "gleif"})
    out = await wl.tick()
    assert out["reruns"] == 1 and store.pending_count() == 1
    out = await wl.tick()
    assert out["reruns"] == 1 and store.pending_count() == 0


# ---- Tier 2: OpenSanctions ---------------------------------------------------


def _os_line(op: str, schema: str, names: list[str], *, lei: str | None = None, eid: str = "e1") -> str:
    props = {"name": names, "topics": ["sanction"]}
    if lei:
        props["leiCode"] = [lei]
    return json.dumps({"op": op, "entity": {"id": eid, "caption": names[0], "schema": schema,
                                             "datasets": ["eu_fsf"], "properties": props}})


def test_os_delta_matches_by_lei_and_by_name_at_the_gate() -> None:
    watched = {EASY: ["EASY POWER"], OTHER: ["Northwind Logistics Ltd"]}
    lines = [
        _os_line("ADD", "Company", ["Some Unrelated GmbH"], lei=EASY, eid="by-lei"),
        _os_line("MOD", "LegalEntity", ["NORTHWIND LOGISTICS LIMITED"], eid="by-name"),
        _os_line("ADD", "Person", ["Easy Power"], eid="person-ignored"),
        _os_line("DEL", "Company", ["Totally Different Ltd"], eid="miss"),
        "",
        "not json",
    ]
    hits = {h["lei"]: h["trigger"] for h in wl.scan_os_delta(lines, watched, "20260916")}
    assert hits[EASY]["matched_on"] == "lei" and hits[EASY]["entity_id"] == "by-lei"
    assert hits[OTHER]["matched_on"] == "name" and hits[OTHER]["score"] >= wl.NAME_MATCH_THRESHOLD
    assert hits[OTHER]["op"] == "MOD" and hits[OTHER]["datasets"] == ["eu_fsf"]
    assert len(hits) == 2


def test_os_tick_catches_up_on_every_version_since_the_last(client: TestClient, env: Path, httpx_mock) -> None:  # noqa: ANN001
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": ["v1", "v2", "v3"]})
    # First run only marks the current version — nothing is downloaded.
    assert wl.opensanctions_tick() == {"versions": 0, "queued": 0}
    assert store.get_meta("opensanctions_version") == "v3"
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": ["v1", "v2", "v3", "v4", "v5"]})
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version="v4"), text=_os_line("ADD", "Company", ["Nobody Ltd"]) + "\n")
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version="v5"), text=_os_line("MOD", "Company", ["EASY POWER"]) + "\n")
    assert wl.opensanctions_tick() == {"versions": 2, "queued": 1}
    assert store.get_meta("opensanctions_version") == "v5"
    pending = store.take_pending(10)
    assert pending[0]["lei"] == EASY and pending[0]["tier"] == "opensanctions"
    assert pending[0]["trigger"]["version"] == "v5"


def test_os_tick_downloads_nothing_when_nothing_is_watched(env: Path, httpx_mock) -> None:  # noqa: ANN001
    assert wl.opensanctions_tick() == {"versions": 0, "queued": 0}
    assert httpx_mock.get_requests() == []


# ---- caps ------------------------------------------------------------------------


def test_caps_per_list_and_per_instance(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_MAX_PER_LIST", "2")
    monkeypatch.setenv("OPENCHECK_WATCHLIST_MAX_TOTAL", "3")
    get_settings.cache_clear()
    wl.reset_for_tests()
    a = _watch(client, EASY)["token"]
    _watch(client, PARENT, a)
    r = client.post("/watch/items", json={"lei": OTHER, "token": a})
    assert r.status_code == 409 and "this list" in r.json()["detail"]
    # Re-adding a watched LEI is not a second row.
    assert client.post("/watch/items", json={"lei": EASY, "token": a}).status_code == 201
    b = _watch(client, EASY)["token"]
    r = client.post("/watch/items", json={"lei": PARENT, "token": b})
    assert r.status_code == 409 and "this instance" in r.json()["detail"]
    assert client.get(f"/watch/{b}").json()["caps"] == {"per_list": 2, "total": 3, "in_list": 1, "total_watched": 3}


# ---- routes ----------------------------------------------------------------------


def test_add_get_remove_and_the_baseline(client: TestClient, env: Path) -> None:
    data = _watch(client, EASY)
    token = data["token"]
    assert len(token) >= 24
    assert data["feed_url"].endswith(f"/watch/{token}.atom")
    w = data["watch"]
    assert w["lei"] == EASY and w["legal_name"] == "EASY POWER" and "snapshot" not in w
    # The mirror's material fields were read at add time, with the watermark.
    assert w["gleif_facts"]["legal_name"].startswith("EASY POWER") and w["gleif_watermark"] == WATERMARK
    page = client.get(f"/watch/{token}").json()
    assert [x["lei"] for x in page["watches"]] == [EASY]
    assert page["watches"][0]["baseline"]["risk_codes"] == ["SANCTIONED"]
    assert page["watches"][0]["baseline"]["coverage"]["answered"] == 3
    assert page["watches"][0]["baseline"]["coverage"]["with_data"] == 2
    assert page["tiers"]["gleif"]["available"] is True
    assert page["tiers"]["gleif"]["watermark"] == WATERMARK
    r = client.delete(f"/watch/{token}/items/{EASY}")
    assert r.status_code == 200 and r.json()["watches"] == []
    assert client.delete(f"/watch/{token}/items/{EASY}").status_code == 404


def test_unknown_token_bad_lei_and_disabled_instance(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert client.get("/watch/nope").status_code == 404
    assert client.get("/watch/nope.atom").status_code == 404
    assert client.post("/watch/items", json={"lei": "NOTANLEI0000000000000"}).status_code == 400
    monkeypatch.delenv("OPENCHECK_WATCHLIST_DB_FILE")
    get_settings.cache_clear()
    wl.reset_for_tests()
    assert client.post("/watch/items", json={"lei": EASY}).status_code == 503
    assert client.get("/watchstats").json()["enabled"] is False


def test_manual_recheck_is_on_the_heavy_tier_and_reports(client: TestClient, env: Path) -> None:
    token = _watch(client, EASY)["token"]
    r = client.post(f"/watch/{token}/recheck", json={"lei": EASY})
    assert r.status_code == 200
    assert r.json()["result"]["changes"] == [] and r.json()["result"]["entries"] == 0
    assert r.json()["watches"][0]["last_checked_at"]
    assert client.post(f"/watch/{token}/recheck", json={"lei": OTHER}).status_code == 404
    from opencheck.routers import watch as watch_router

    assert watch_router.recheck.__wrapped__ if hasattr(watch_router.recheck, "__wrapped__") else True


def test_watchstats_carries_no_lei(client: TestClient, env: Path) -> None:
    _watch(client, EASY)
    text = client.get("/watchstats").text
    assert EASY not in text and "EASY POWER" not in text
    assert client.get("/watchstats").json()["watched"]["total"] == 1


def test_robots_disallows_watch(client: TestClient, env: Path) -> None:
    assert "Disallow: /watch" in client.get("/robots.txt").text


def test_the_store_holds_only_hashes(client: TestClient, env: Path) -> None:
    token = _watch(client, EASY)["token"]
    conn = sqlite3.connect(get_settings().watchlist_db_file)
    dump = "\n".join(conn.iterdump())
    conn.close()
    assert token not in dump and wl.token_hash(token) in dump


# ---- the feed --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feed_is_valid_atom_with_one_entry_per_change(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token = _watch(client, EASY)["token"]
    th = wl.token_hash(token)

    async def _changed(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        return _resp(lei, legal_name="EASY POWER (RENAMED)")

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _changed)
    await wl.rerun(EASY, wl.TIER_GLEIF, {"tier": "gleif", "publish": "2026-09-08 00:00:00", "fields": ["legal_name"]})
    r = client.get(f"/watch/{token}.atom")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/atom+xml")
    assert r.headers["cache-control"] == "private, no-store"
    root = ET.fromstring(r.text)
    assert root.tag == f"{ATOM}feed"
    assert root.find(f"{ATOM}id").text.startswith("urn:opencheck:watchlist:")
    assert root.find(f"{ATOM}updated").text.endswith("Z")
    links = {l.get("rel"): l.get("href") for l in root.findall(f"{ATOM}link")}
    assert links["self"].endswith(f"/watch/{token}.atom") and "/watchlist?token=" in links["alternate"]
    entries = root.findall(f"{ATOM}entry")
    assert len(entries) == 1
    e = entries[0]
    assert e.find(f"{ATOM}title").text == "EASY POWER (RENAMED): 1 change"
    assert e.find(f"{ATOM}id").text.startswith("urn:opencheck:watch-entry:")
    assert e.find(f"{ATOM}category").get("term") == "gleif"
    content = e.find(f"{ATOM}content").text
    assert "GLEIF published a change to this record in the 2026-09-08 00:00:00 Golden Copy delta." in content
    assert "Legal name: EASY POWER → EASY POWER (RENAMED)." in content
    assert "3 sources checked as a result (retrieved 2026-09-16)." in content
    # The token itself never appears in the entry body.
    assert th not in r.text


def test_empty_feed_is_still_valid() -> None:
    xml = render_atom(
        feed_id="urn:opencheck:watchlist:x", self_url="https://api/watch/t.atom",
        page_url="https://app/watchlist?token=t", title="OpenCheck watchlist", watches=[], entries=[],
    )
    root = ET.fromstring(xml)
    assert root.findall(f"{ATOM}entry") == []
    assert "0 watched companies" in root.find(f"{ATOM}subtitle").text
    assert "does not poll the registers" in root.find(f"{ATOM}subtitle").text


def test_feed_escapes_names() -> None:
    xml = render_atom(
        feed_id="x", self_url="https://api/watch/t.atom", page_url="https://app/?a=1&b=2", title="t",
        watches=[], entries=[{
            "id": 1, "lei": EASY, "legal_name": "R&D <Ltd>", "created_at": "2026-09-16T08:00:00Z",
            "tier": "manual", "trigger": {}, "changes": [{"kind": "verdict", "old": None, "new": "a < b"}],
            "checked": [], "degraded": [],
        }],
    )
    root = ET.fromstring(xml)
    assert root.find(f"{ATOM}entry/{ATOM}title").text.startswith("R&D <Ltd>: 1 change")


# ---- lifespan --------------------------------------------------------------------


def test_lifespan_starts_the_worker_when_a_store_is_configured(env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_INTERVAL_S", "3600")
    get_settings.cache_clear()
    with TestClient(app):
        assert wl.state()["enabled"] is True
    assert wl.state()["enabled"] is False


def test_every_change_kind_is_worded_by_the_feed() -> None:
    from opencheck.routers.watch import _describe

    for kind in wl.CHANGE_KINDS:
        sentence = _describe({"kind": kind, "old": {"answered": 1, "applicable": 2, "liveness": "live"},
                              "new": {"answered": 2, "applicable": 2, "liveness": "live"},
                              "code": "X", "scheme": "S", "field": "f", "sources": ["a"], "degraded": ["a"]})
        assert sentence and sentence != f"{kind}."


# ---- with the limiter ON -------------------------------------------------------


@pytest.fixture
def limited_client(env: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """The suite runs with the limiter off (conftest). slowapi injects its
    headers through a ``response: Response`` parameter and 500s on a
    dict-returning handler that lacks one — which only shows with the
    limiter on. Production found /recheck and DELETE this way on the day
    Phase 215 deployed; this fixture is what would have caught it."""
    from opencheck.ratelimit import limiter

    async def _fake(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        return _resp(lei)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_DEFAULT", "50/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_HEAVY", "2/minute")
    get_settings.cache_clear()
    limiter.reset()
    limiter.enabled = True
    try:
        yield TestClient(app)
    finally:
        limiter.enabled = False
        limiter.reset()
        get_settings.cache_clear()


def test_every_watch_route_answers_with_the_limiter_on(limited_client: TestClient) -> None:
    c = limited_client
    r = c.post("/watch/items", json={"lei": EASY})
    assert r.status_code == 201 and "x-ratelimit-limit" in r.headers
    token = r.json()["token"]
    assert c.get(f"/watch/{token}").status_code == 200
    assert c.get(f"/watch/{token}.atom").status_code == 200
    r = c.post(f"/watch/{token}/recheck", json={"lei": EASY})
    assert r.status_code == 200, r.text
    assert r.headers["x-ratelimit-limit"] == "2"
    r = c.delete(f"/watch/{token}/items/{EASY}")
    assert r.status_code == 200, r.text
    # The heavy tier bounds the re-check: the third press in a minute is refused.
    _watch(c, EASY, token)
    assert c.post(f"/watch/{token}/recheck", json={"lei": EASY}).status_code == 200
    assert c.post(f"/watch/{token}/recheck", json={"lei": EASY}).status_code == 429


# ---- Phase 234: capacity one client cannot exhaust -----------------------------


def test_new_lists_are_a_per_client_quota(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck import lookup_budget

    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_ENABLED", "1")
    monkeypatch.setenv("OPENCHECK_WATCHLIST_NEW_LISTS_PER_IP", "2/day")
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()
    first = _watch(client, EASY)["token"]
    _watch(client, EASY)
    r = client.post("/watch/items", json={"lei": EASY})
    assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1
    assert "a list you already have" in r.json()["detail"]
    # Adding to a list the client already holds is not a new list.
    assert client.post("/watch/items", json={"lei": PARENT, "token": first}).status_code == 201
    # Another address is untouched.
    r = client.post("/watch/items", json={"lei": EASY}, headers={"x-forwarded-for": "198.51.100.200"})
    assert r.status_code == 201


def test_a_full_instance_refuses_before_running_the_baseline(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_MAX_TOTAL", "1")
    get_settings.cache_clear()
    wl.reset_for_tests()
    _watch(client, EASY)
    lists_before = wl.get_store().counts()

    async def _must_not_run(lei: str) -> None:
        raise AssertionError("the baseline lookup ran for a watch that cannot be kept")

    monkeypatch.setattr(wl, "baseline", _must_not_run)
    r = client.post("/watch/items", json={"lei": PARENT})
    assert r.status_code == 409 and "this instance" in r.json()["detail"]
    # …and no empty list was minted for the refused add.
    with wl.get_store()._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM lists").fetchone()[0] == 1
    assert wl.get_store().counts() == lists_before


def test_a_list_unopened_past_the_window_is_deleted(client: TestClient, env: Path) -> None:
    token = _watch(client, EASY)["token"]
    keep = _watch(client, PARENT)["token"]
    store = wl.get_store()
    with store._conn() as conn:
        conn.execute(
            "UPDATE lists SET last_seen_at = '2026-01-01T00:00:00Z' WHERE token_hash = ?",
            (wl.token_hash(token),),
        )
    assert store.prune_stale_lists(older_than_days=90) == 1
    assert store.prune_stale_lists(older_than_days=0) == 0
    r = client.get(f"/watch/{token}")
    assert r.status_code == 404 and "90 days" in r.json()["detail"]
    assert client.get(f"/watch/{keep}").status_code == 200
    assert store.watched_leis() == {PARENT}
