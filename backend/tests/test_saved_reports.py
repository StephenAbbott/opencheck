"""Phase 216 — saved reports: the record of exactly what OpenCheck showed.

Pins the contract in ``opencheck/saved_reports.py``: a save copies the
server's own held run (never a client payload) and refuses a run it no longer
holds; the content hash covers the exact bytes served and is re-checked on
read; retention, extend and delete; narratives only when this server wrote
them from the same run; dispositions frozen at save time and live sheets kept
in the store; noindex, robots, the bot gate, and every route with the limiter
on.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import saved_reports as sr
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.dispositions import (
    ClaimDisposition,
    DispositionRecord,
    load_dispositions,
    save_dispositions,
)
from opencheck.routers import lookup as lookup_mod
from opencheck.routers import narrative as narrative_mod
from opencheck.sources import SearchKind, SourceHit

LEI = "213800LH1BZH3DI6G760"
RUN_AT = "2026-09-16T14:02:00+00:00"
NARRATIVE_RUN = "0123456789abcdef"
RETIRED = "RETIRED_SIGNAL_CODE_FROM_2026"
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/128 Safari/537.36"


@pytest.fixture(autouse=True)
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    db = tmp_path / "saved_reports.sqlite"
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_DB_FILE", str(db))
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_PRUNE_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    sr.reset_for_tests()
    lookup_mod._REPLAY_CACHE.clear()
    narrative_mod._NARRATIVE_CACHE.clear()
    yield db
    lookup_mod._REPLAY_CACHE.clear()
    narrative_mod._NARRATIVE_CACHE.clear()
    sr.reset_for_tests()
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app, headers={"User-Agent": BROWSER_UA})


FROZEN_KNOWABILITY: dict = {
    "code": "GB", "name": "United Kingdom",
    "sentence": "PSC register frozen sentence from the day of the run.",
    "sentences": ["PSC register frozen sentence from the day of the run."],
    "stated_absence": False, "review_status": "verified", "last_verified": "2026-09-16",
    "access": "public", "fields": {}, "opencheck_reads": [], "sources": [],
}
FROZEN_KY: dict = {
    **FROZEN_KNOWABILITY, "code": "KY", "name": "Cayman Islands",
    "sentence": "Cayman frozen sentence from the day of the run.",
    "sentences": ["Cayman frozen sentence from the day of the run."],
    "review_status": "unverified", "last_verified": None, "access": "legitimate_interest",
}


def _events(run_at: str = RUN_AT, code: str = RETIRED) -> list[tuple[str, object]]:
    hit = SourceHit(
        source_id="gleif", hit_id=LEI, kind=SearchKind.ENTITY, name="BP P.L.C.", summary="GB · LEI",
        is_stub=False,
    )
    os_hit = SourceHit(
        source_id="opensanctions", hit_id="NK-1", kind=SearchKind.ENTITY, name="BP P.L.C.", summary="match",
        is_stub=False,
    )
    return [
        ("gleif_done", {"lei": LEI, "legal_name": "BP P.L.C.", "jurisdiction": "GB",
                        "derived_identifiers": {"gb_coh": "00102498"}}),
        ("sources_applicable", {"source_ids": ["companies_house", "opensanctions"]}),
        ("source_started", {"source_id": "gleif"}),
        ("hit", hit),
        ("source_completed", {"source_id": "gleif"}),
        ("hit", os_hit),
        ("deepen_result", {"source_id": "gleif", "bods": [{"statementId": "s1", "recordType": "entity"}]}),
        # Phases 224/226: the dated statements, frozen with the run.
        ("knowability", {**FROZEN_KNOWABILITY, "as_of": "2026-09-16"}),
        ("knowability_chain", {
            "subject": "GB", "codes": ["GB", "KY"], "as_of": "2026-09-16",
            "statements": [FROZEN_KNOWABILITY, FROZEN_KY],
        }),
        ("risk_signals", {
            "signals": [{"code": code, "kind": "risk", "confidence": "high", "summary": "retired",
                         "source_id": "gleif", "hit_id": LEI, "evidence": {}}],
            "degraded_sources": [], "verdict": "One finding.",
            "source_liveness": {"gleif": {"liveness": "live", "retrieved_at": "2026-09-16T14:01:59Z"}},
        }),
        ("done", {"lei": LEI, "bods_issues": [], "license_notices": [], "run_completed_at": run_at}),
    ]


def _hold_run(run_at: str = RUN_AT, *, age_s: float = 0.0, code: str = RETIRED) -> None:
    lookup_mod._REPLAY_CACHE[f"{LEI}:5"] = lookup_mod._ReplayEntry(
        stored=time.monotonic() - age_s, fetched_at=run_at, events=_events(run_at, code)
    )


def _hold_narrative(run_at: str = RUN_AT, run_id: str = NARRATIVE_RUN) -> None:
    narrative_mod._NARRATIVE_CACHE[run_id] = narrative_mod.HeldNarrative(
        stored=time.monotonic(), lei=LEI, deepen_top=5, run_completed_at=run_at,
        narrative={"lei": LEI, "run_id": run_id, "summary": "BP is a company.",
                   "claims": [{"id": "c1", "text": "BP is a company."}]},
    )


def _save(client: TestClient, **body) -> dict:
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT, **body})
    assert r.status_code == 201, r.text
    return r.json()


# ---- the fold and the run's name ------------------------------------------------


def _seed_bundle(tmp_path: Path) -> None:
    target = tmp_path / "data" / "cache" / "bods_data" / "gleif" / f"{LEI}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({
        "statementId": "e-subject", "recordType": "entity",
        "recordDetails": {"name": "Bundle Co P.L.C.", "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                          "identifiers": [{"id": LEI, "scheme": "XI-LEI"}, {"id": "12345678", "scheme": "GB-COH"}]},
    }) + "\n")


def test_fold_of_the_held_events_is_the_lookup_response(client: TestClient, tmp_path: Path) -> None:
    """``fold_lookup_events`` was factored out of ``_lookup_impl``: folding the
    replay cache's events gives back exactly the /lookup response."""
    _seed_bundle(tmp_path)
    live = client.get("/lookup", params={"lei": LEI}).json()
    entry = lookup_mod.replay_entry(LEI)
    assert entry is not None
    folded = lookup_mod.fold_lookup_events(LEI, entry.events).model_dump(mode="json")
    assert folded == live
    # And from the JSON round trip a saved report stores.
    again = lookup_mod.fold_lookup_events(LEI, sr.deserialise_events(sr.serialise_events(entry.events)))
    assert again.model_dump(mode="json") == live


def test_a_live_run_names_itself_and_fetched_at_stays_replay_only(client: TestClient, tmp_path: Path) -> None:
    _seed_bundle(tmp_path)
    first = client.get("/lookup", params={"lei": LEI}).json()
    assert first["replayed"] is False and first["fetched_at"] is None
    datetime.fromisoformat(first["run_completed_at"])
    second = client.get("/lookup", params={"lei": LEI}).json()
    # The replay repeats the same name — it is the same run.
    assert second["run_completed_at"] == first["run_completed_at"] == second["fetched_at"]
    with client.stream("GET", "/lookup-stream", params={"lei": LEI}) as r:
        body = "".join(r.iter_text())
    done = [line for line in body.splitlines() if line.startswith("data: ") and "bods_issues" in line][-1]
    assert json.loads(done[6:])["run_completed_at"] == first["run_completed_at"]


def test_replay_entry_respects_the_window() -> None:
    _hold_run(age_s=lookup_mod._REPLAY_TTL_SECONDS + 1)
    assert lookup_mod.replay_entry(LEI) is None
    _hold_run()
    assert lookup_mod.replay_entry(LEI) is not None


# ---- saving ---------------------------------------------------------------------


def test_save_then_read_a_real_run_end_to_end(client: TestClient, tmp_path: Path) -> None:
    _seed_bundle(tmp_path)
    live = client.get("/lookup", params={"lei": LEI}).json()
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": live["run_completed_at"]})
    assert r.status_code == 201, r.text
    assert r.headers["x-robots-tag"].startswith("noindex")
    saved = r.json()
    assert len(saved["report_id"]) == 22 and saved["manage_token"]
    assert saved["url"].endswith(f"/report/{saved['report_id']}")

    got = client.get(f"/saved-reports/{saved['report_id']}")
    assert got.status_code == 200 and got.headers["x-robots-tag"].startswith("noindex")
    body = got.json()
    assert "manage_token" not in body
    payload = body["payload"]
    assert payload["schema"] == sr.SCHEMA and payload["report_id"] == saved["report_id"]
    assert payload["run_completed_at"] == live["run_completed_at"]
    folded = lookup_mod.fold_lookup_events(LEI, sr.deserialise_events(payload["events"]))
    assert folded.model_dump(mode="json") == live


def test_the_json_download_is_the_hashed_bytes(client: TestClient) -> None:
    _hold_run()
    saved = _save(client)
    r = client.get(f"/saved-reports/{saved['report_id']}.json")
    assert r.status_code == 200
    digest = hashlib.sha256(r.content).hexdigest()
    assert digest == saved["content_hash"] == r.headers["x-opencheck-content-sha256"]
    assert r.content == sr.canonical_bytes(json.loads(r.content))
    assert "attachment" in r.headers["content-disposition"]


def test_a_run_the_server_no_longer_holds_is_not_saved(client: TestClient) -> None:
    body = {"lei": LEI, "run_completed_at": RUN_AT}
    r = client.post("/saved-reports", json=body)  # nothing held
    assert r.status_code == 409 and r.headers["x-opencheck-refusal"] == "run_not_held"
    assert "Run the check again" in r.json()["detail"]
    _hold_run("2026-09-16T13:00:00+00:00")  # a different run
    assert client.post("/saved-reports", json=body).status_code == 409
    _hold_run(age_s=lookup_mod._REPLAY_TTL_SECONDS + 5)  # aged out
    assert client.post("/saved-reports", json=body).status_code == 409
    assert sr.get_store().count() == 0


def test_a_per_source_retry_clears_the_run_so_it_cannot_be_saved(client: TestClient) -> None:
    _hold_run()
    lookup_mod._invalidate_replay(LEI)
    assert client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT}).status_code == 409


def test_a_client_payload_is_refused(client: TestClient) -> None:
    _hold_run()
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT,
                                            "events": [{"event": "risk_signals", "data": {}}]})
    assert r.status_code == 422
    assert sr.get_store().count() == 0


def test_a_retired_signal_code_is_saved_verbatim(client: TestClient) -> None:
    """The fixture Phase B renders: a code the engine no longer emits must be
    carried, not dropped."""
    _hold_run(code=RETIRED)
    saved = _save(client)
    payload = client.get(f"/saved-reports/{saved['report_id']}").json()["payload"]
    signals = [e for e in payload["events"] if e["event"] == "risk_signals"][0]["data"]["signals"]
    assert [s["code"] for s in signals] == [RETIRED]
    folded = lookup_mod.fold_lookup_events(LEI, sr.deserialise_events(payload["events"]))
    assert [s["code"] for s in folded.risk_signals] == [RETIRED]


def test_payload_carries_scope_licensing_and_clocks(client: TestClient) -> None:
    _hold_run()
    saved = _save(client)
    p = client.get(f"/saved-reports/{saved['report_id']}").json()["payload"]
    assert p["scope"]["excluded"] == ["background", "subsidiaries", "history", "esg", "securities", "nz_associations"]
    # One CC-BY-NC source (OpenSanctions) makes the whole saved report
    # non-commercial — assessed at save time, frozen with it.
    assert p["licensing"]["commercial_use"] == "no"
    assert [s["source_id"] for s in p["licensing"]["per_source"]] == ["gleif", "opensanctions"]
    assert p["saved_at"] == saved["saved_at"] and p["run_completed_at"] == RUN_AT
    assert p["narrative"] is None and p["dispositions"] is None
    # deepen_result is kept for the exports even though the stream skips it.
    assert any(e["event"] == "deepen_result" for e in p["events"])


# ---- narrative and dispositions ---------------------------------------------------


def test_a_narrative_is_saved_only_when_this_server_wrote_it_from_this_run(client: TestClient) -> None:
    _hold_run()
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT, "narrative_run_id": NARRATIVE_RUN})
    assert r.status_code == 409 and r.headers["x-opencheck-refusal"] == "narrative_not_held"

    _hold_narrative(run_at="2026-09-16T09:00:00+00:00")  # written from another run
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT, "narrative_run_id": NARRATIVE_RUN})
    assert r.status_code == 409

    _hold_narrative()
    saved = _save(client, narrative_run_id=NARRATIVE_RUN)
    p = client.get(f"/saved-reports/{saved['report_id']}").json()["payload"]
    assert p["narrative"]["summary"] == "BP is a company."


def test_the_narrative_route_holds_what_it_generates(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.routers.narrative import NarrativeResponse, held_narrative
    from opencheck.routers.narrative import _hold_narrative as hold

    resp = NarrativeResponse(lei=LEI, subject_name="BP", summary="s", model="m", prompt_version="p",
                             run_id=NARRATIVE_RUN, packet={})
    hold(resp, deepen_top=5, run_completed_at=RUN_AT)
    held = held_narrative(NARRATIVE_RUN)
    assert held is not None and held.run_completed_at == RUN_AT and held.narrative["summary"] == "s"
    monkeypatch.setattr(narrative_mod, "_NARRATIVE_CACHE",
                        {NARRATIVE_RUN: held._replace(stored=time.monotonic() - 10_000)})
    assert held_narrative(NARRATIVE_RUN) is None


def _sheet(status: str) -> DispositionRecord:
    return DispositionRecord(lei=LEI, run_id=NARRATIVE_RUN,
                             dispositions=[ClaimDisposition(claim_id="c1", status=status)])


def test_dispositions_are_frozen_at_save_time(client: TestClient) -> None:
    _hold_run()
    _hold_narrative()
    save_dispositions(_sheet("accepted"))
    saved = _save(client, narrative_run_id=NARRATIVE_RUN)
    save_dispositions(_sheet("disputed"))  # the analyst changes their mind later
    p = client.get(f"/saved-reports/{saved['report_id']}").json()["payload"]
    assert p["dispositions"]["dispositions"][0]["status"] == "accepted"
    assert load_dispositions(LEI, NARRATIVE_RUN).dispositions[0].status == "disputed"


def test_live_dispositions_live_in_the_store_not_on_the_filesystem(env: Path, tmp_path: Path) -> None:
    save_dispositions(_sheet("needs_review"))
    assert not (tmp_path / "data" / "dispositions").exists()
    with sqlite3.connect(env) as conn:
        assert conn.execute("SELECT COUNT(*) FROM dispositions").fetchone()[0] == 1
    assert load_dispositions(LEI, NARRATIVE_RUN).dispositions[0].status == "needs_review"


def test_a_legacy_disposition_file_is_still_read(tmp_path: Path) -> None:
    legacy = tmp_path / "data" / "dispositions" / LEI / "fedcba9876543210.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(DispositionRecord(lei=LEI, run_id="fedcba9876543210").model_dump_json())
    assert load_dispositions(LEI, "fedcba9876543210") is not None


def test_without_the_store_dispositions_stay_on_the_filesystem(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("OPENCHECK_SAVED_REPORTS_DB_FILE")
    get_settings.cache_clear()
    sr.reset_for_tests()
    save_dispositions(_sheet("accepted"))
    assert (tmp_path / "data" / "dispositions" / LEI / f"{NARRATIVE_RUN}.json").is_file()


# ---- integrity -------------------------------------------------------------------


def test_canonical_bytes_do_not_depend_on_key_order() -> None:
    assert sr.canonical_bytes({"b": 1, "a": [1, {"d": 2, "c": "é"}]}) == sr.canonical_bytes(
        {"a": [1, {"c": "é", "d": 2}], "b": 1}
    )


def test_a_tampered_report_is_refused(client: TestClient, env: Path) -> None:
    _hold_run()
    saved = _save(client)
    with sqlite3.connect(env) as conn:
        blob = conn.execute("SELECT payload_gz FROM reports").fetchone()[0]
        doctored = gzip.decompress(blob).replace(RETIRED.encode(), b"NOTHING_TO_SEE_HERE_AT_ALL____")
        conn.execute("UPDATE reports SET payload_gz = ?", (gzip.compress(doctored),))
    for path in (f"/saved-reports/{saved['report_id']}", f"/saved-reports/{saved['report_id']}.json"):
        r = client.get(path)
        assert r.status_code == 500 and r.headers["x-opencheck-refusal"] == "integrity"


# ---- retention, extend, delete ------------------------------------------------------


def test_an_expired_report_is_gone_and_pruned(client: TestClient) -> None:
    store = sr.get_store()
    _hold_run()
    old = sr.save_from_replay(store, lei=LEI, run_completed_at=RUN_AT, now=datetime.now(UTC) - timedelta(days=91))
    r = client.get(f"/saved-reports/{old['report_id']}")
    assert r.status_code == 410 and r.headers["x-opencheck-refusal"] == "expired"
    assert client.get(f"/saved-reports/{old['report_id']}").status_code == 404  # deleted on read

    sr.save_from_replay(store, lei=LEI, run_completed_at=RUN_AT, now=datetime.now(UTC) - timedelta(days=100))
    fresh = sr.save_from_replay(store, lei=LEI, run_completed_at=RUN_AT)
    assert store.prune_expired() == 1
    assert client.get(f"/saved-reports/{fresh['report_id']}").status_code == 200


def test_retention_is_ninety_days_by_default(client: TestClient) -> None:
    _hold_run()
    saved = _save(client)
    days = datetime.fromisoformat(saved["expires_at"].replace("Z", "+00:00")) - datetime.fromisoformat(
        saved["saved_at"].replace("Z", "+00:00"))
    assert days == timedelta(days=90)


def test_extend_and_delete_need_the_manage_token(client: TestClient) -> None:
    store = sr.get_store()
    _hold_run()
    saved = sr.save_from_replay(store, lei=LEI, run_completed_at=RUN_AT, now=datetime.now(UTC) - timedelta(days=60))
    rid, token = saved["report_id"], saved["manage_token"]
    assert client.post(f"/saved-reports/{rid}/extend").status_code == 403
    assert client.post(f"/saved-reports/{rid}/extend", headers={sr_header(): "wrong"}).status_code == 403
    r = client.post(f"/saved-reports/{rid}/extend", headers={sr_header(): token})
    assert r.status_code == 200 and r.json()["extended_at"]
    assert r.json()["expires_at"] > saved["expires_at"]
    # Extending never changes the hashed record.
    assert r.json()["content_hash"] == saved["content_hash"]

    assert client.delete(f"/saved-reports/{rid}", headers={sr_header(): "wrong"}).status_code == 403
    assert client.delete(f"/saved-reports/{rid}", headers={sr_header(): token}).status_code == 200
    assert client.get(f"/saved-reports/{rid}").status_code == 404


def sr_header() -> str:
    from opencheck.routers.saved_reports import MANAGE_HEADER

    return MANAGE_HEADER


def test_the_manage_token_is_stored_only_as_a_hash(client: TestClient, env: Path) -> None:
    _hold_run()
    saved = _save(client)
    raw = env.read_bytes() + (env.with_name(env.name + "-wal").read_bytes() if env.with_name(env.name + "-wal").exists() else b"")
    assert saved["manage_token"].encode() not in raw


def test_unknown_and_malformed_ids_are_404(client: TestClient) -> None:
    assert client.get("/saved-reports/AAAAAAAAAAAAAAAAAAAAAA").status_code == 404
    assert client.get("/saved-reports/../etc").status_code == 404
    assert client.get("/saved-reports/short.json").status_code == 404


def test_the_instance_cap(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_MAX_TOTAL", "1")
    get_settings.cache_clear()
    _hold_run()
    _save(client)
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT})
    assert r.status_code == 507 and r.headers["x-opencheck-refusal"] == "cap_exceeded"


def test_disabled_instance_answers_503(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENCHECK_SAVED_REPORTS_DB_FILE")
    get_settings.cache_clear()
    sr.reset_for_tests()
    assert client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT}).status_code == 503
    assert client.get("/saved-reports/AAAAAAAAAAAAAAAAAAAAAA").status_code == 503


# ---- crawlers ----------------------------------------------------------------------


def test_robots_disallows_saved_reports(client: TestClient) -> None:
    body = client.get("/robots.txt").text
    assert "Disallow: /saved-reports" in body


def test_declared_bots_cannot_save(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", raising=False)
    get_settings.cache_clear()
    _hold_run()
    r = client.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT},
                    headers={"User-Agent": "python-httpx/0.27.0"})
    assert r.status_code == 403 and "robots.txt" in r.json()["detail"]


# ---- with the limiter ON --------------------------------------------------------------


@pytest.fixture
def limited_client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """The suite runs with the limiter off. slowapi 500s a dict-returning
    handler without ``response: Response`` only when it is on (PR #280)."""
    from opencheck.ratelimit import limiter

    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_DEFAULT", "50/minute")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_HEAVY", "2/minute")
    get_settings.cache_clear()
    limiter.reset()
    limiter.enabled = True
    try:
        yield TestClient(app, headers={"User-Agent": BROWSER_UA})
    finally:
        limiter.enabled = False
        limiter.reset()
        get_settings.cache_clear()


def test_every_saved_report_route_answers_with_the_limiter_on(limited_client: TestClient) -> None:
    c = limited_client
    _hold_run()
    r = c.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT})
    assert r.status_code == 201, r.text
    assert r.headers["x-ratelimit-limit"] == "2"  # save is on the heavy tier
    rid, token = r.json()["report_id"], r.json()["manage_token"]
    assert c.get(f"/saved-reports/{rid}").status_code == 200
    assert c.get(f"/saved-reports/{rid}.json").status_code == 200
    assert c.post(f"/saved-reports/{rid}/extend", headers={sr_header(): token}).status_code == 200
    assert c.delete(f"/saved-reports/{rid}", headers={sr_header(): token}).status_code == 200
    assert c.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT}).status_code == 201
    assert c.post("/saved-reports", json={"lei": LEI, "run_completed_at": RUN_AT}).status_code == 429


# ---- Phase 217: what a saved source drawer reads ------------------------------------


def test_deepen_result_carries_what_a_source_drawer_renders(client: TestClient, tmp_path: Path) -> None:
    """A saved report's source drawers render from the stored ``deepen_result``
    rather than calling /deepen for today's record, so the event carries the
    rest of the /deepen answer — everything but the raw record."""
    _seed_bundle(tmp_path)
    live = client.get("/lookup", params={"lei": LEI}).json()
    saved = _save(client, run_completed_at=live["run_completed_at"])
    events = client.get(f"/saved-reports/{saved['report_id']}").json()["payload"]["events"]
    deepened = [e["data"] for e in events if e["event"] == "deepen_result"]
    assert deepened, "the offline GLEIF bundle is deepened"
    for d in deepened:
        assert {"source_id", "hit_id", "bods", "bods_issues", "risk_signals", "license", "license_notice"} <= set(d)
        assert "raw" not in d
    gleif = next(d for d in deepened if d["source_id"] == "gleif")
    assert gleif["license"] == "CC0-1.0"
    # The stream still never sends it.
    with client.stream("GET", "/lookup-stream", params={"lei": LEI}) as r:
        assert "event: deepen_result" not in "".join(r.iter_text())
