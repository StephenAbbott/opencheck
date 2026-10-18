"""pKYC stage D — the review record (``opencheck/watchlist_decisions.py``):
a decision on a log entry, append-only, with a saved report pinned as the
evidence on request.

Stephen's decisions on the stage D ticket (18 Oct 2026): three closed statuses
(reviewed, no action / re-checked / escalated); a decision is never set
automatically; append-only, the history kept; the note follows stage B's
rationale rule; evidence is opt-in — the entry's own held run, or a fresh
re-check saved in the same action — and a refused save writes no decision; no
reviewer identity; a Markdown record per list that reads no clock.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import saved_reports as sr
from opencheck import watchlist as wl
from opencheck import watchlist_decisions as dec
from opencheck.config import get_settings
from opencheck.routers.lookup import LookupResponse
from tests.test_gleif_mirror_store import EASY, PARENT
from tests.test_watchlist import _resp, _watch, client, env  # noqa: F401 - fixtures

ATOM = "{http://www.w3.org/2005/Atom}"
NOW = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "watchlist.ts"
DOC = Path(__file__).resolve().parents[2] / "docs" / "watchlist.md"
RUN_AT = "2026-10-18T11:58:00+00:00"


# ---- the module ----------------------------------------------------------------


def test_the_three_statuses_their_words_and_the_validators() -> None:
    assert dec.STATUSES == ("reviewed_no_action", "rechecked", "escalated")
    assert set(dec.STATUS_LABELS) == set(dec.STATUS_MEANINGS) == set(dec.STATUSES)
    assert dec.validate_status("escalated") == "escalated"
    with pytest.raises(dec.DecisionError) as e:
        dec.validate_status("closed")
    assert e.value.field == "status"
    assert dec.validate_note(None) is None and dec.validate_note("   ") is None
    assert dec.validate_note("  Checked the  parent filing. ") == "Checked the parent filing."
    with pytest.raises(dec.DecisionError) as e:
        dec.validate_note("mail me at a@b.co")
    assert e.value.field == "note" and "note" in e.value.message and "rationale" not in e.value.message
    assert dec.validate_evidence(None) is None and dec.validate_evidence("") is None
    assert dec.validate_evidence("entry_run") == "entry_run"
    with pytest.raises(dec.DecisionError):
        dec.validate_evidence("pdf")


def test_run_still_held_follows_the_replay_window() -> None:
    assert dec.run_still_held({"run_completed_at": "2026-10-18T11:50:00+00:00"}, now=NOW) is True
    assert dec.run_still_held({"run_completed_at": "2026-10-18T11:40:00+00:00"}, now=NOW) is False
    assert dec.run_still_held({"run_completed_at": None}, now=NOW) is False
    assert dec.run_still_held({}, now=NOW) is False


def test_the_sentence_and_the_needs_review_rule() -> None:
    d = {"status": "reviewed_no_action", "decided_at": "2026-10-18T12:00:00+00:00"}
    assert dec.decision_sentence(d) == "Decision: reviewed, no action — 2026-10-18."
    d2 = {
        "status": "escalated", "decided_at": "2026-10-18T12:00:00+00:00", "note": "Sent to compliance.",
        "saved_report_id": "r1", "evidence_kind": "entry_run", "report": {"available": False},
    }
    assert dec.decision_sentence(d2) == (
        "Decision: escalated — 2026-10-18; note: Sent to compliance.; evidence: saved report r1 "
        "(the re-run that produced this entry) — the pinned saved report has since expired."
    )
    assert dec.is_undecided_material({"materiality": "recheck", "decision": None}) is True
    assert dec.is_undecided_material({"materiality": "escalate", "decision": d}) is False
    assert dec.is_undecided_material({"materiality": "note", "decision": None}) is False


# ---- the store -------------------------------------------------------------------


def test_decisions_append_and_the_entry_carries_its_run(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    th = wl.token_hash(token)
    eid = store.add_entry(
        th, EASY, tier=wl.TIER_MANUAL, trigger={"tier": "manual"},
        changes=[{"kind": "signal_new", "code": "SANCTIONED"}], checked=[], degraded=[],
        legal_name="EASY POWER", run_completed_at=RUN_AT, deepen_top=5,
    )
    entry = store.entry(th, eid)
    assert entry and entry["run_completed_at"] == RUN_AT and entry["deepen_top"] == 5
    assert store.entry("other-hash", eid) is None
    first = store.add_decision(th, eid, lei=EASY, status="escalated", note=None, saved_report_id=None, evidence_kind=None, now=NOW)
    second = store.add_decision(th, eid, lei=EASY, status="reviewed_no_action", note="False alarm.", saved_report_id=None, evidence_kind=None)
    rows = store.decisions(th)
    assert [r["id"] for r in rows] == [first["id"], second["id"]]
    assert rows[0]["status"] == "escalated" and rows[0]["decided_at"] == "2026-10-18T12:00:00+00:00"
    counts = store.decision_counts()
    assert counts == {
        "decisions_by_status": {"reviewed_no_action": 1, "rechecked": 0, "escalated": 1},
        "decisions_with_evidence": 0,
        "material_undecided": 0,
    }


def test_a_rerun_records_the_run_it_came_from(client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    seen: dict[str, Any] = {}

    async def _fake(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        seen["deepen_top"] = deepen_top
        return _resp(lei, risk_signals=[], run_completed_at=RUN_AT)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    r = client.post(f"/watch/{token}/recheck", json={"lei": EASY})
    assert r.status_code == 200, r.text
    assert seen["deepen_top"] == wl.RERUN_DEEPEN_TOP == 5
    assert r.json()["result"]["run_completed_at"] == RUN_AT
    entry = r.json()["entries"][0]
    assert entry["run_completed_at"] == RUN_AT and entry["deepen_top"] == 5
    assert entry["decision"] is None and entry["decision_count"] == 0 and entry["decision_history"] == []


# ---- the route -----------------------------------------------------------------


def _entry(client: TestClient, token: str, *, run_at: str | None = RUN_AT, materiality: str = "recheck") -> int:
    store = wl.get_store()
    assert store is not None
    th = wl.token_hash(token)
    change = {"kind": "signal_new", "code": "SANCTIONED"} if materiality != "note" else {"kind": "jurisdiction", "old": "GR", "new": "GR"}
    return store.add_entry(
        th, EASY, tier=wl.TIER_MANUAL, trigger={"tier": "manual"}, changes=[change], checked=[], degraded=[],
        legal_name="EASY POWER", run_completed_at=run_at, deepen_top=5,
    )


def _enable_saved_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sr.SavedReportsStore:
    monkeypatch.setenv("OPENCHECK_SAVED_REPORTS_DB_FILE", str(tmp_path / "saved.sqlite"))
    get_settings.cache_clear()
    sr.reset_for_tests()
    store = sr.get_store()
    assert store is not None
    return store


def _stub_save(monkeypatch: pytest.MonkeyPatch, calls: list[dict[str, Any]]) -> None:
    """Stand in for ``save_from_replay``: record what was asked and insert a
    real report so the pinned block can be read back."""

    def _save(store: sr.SavedReportsStore, *, lei: str, run_completed_at: str, deepen_top: int = 5, **_: Any) -> dict[str, Any]:
        calls.append({"lei": lei, "run_completed_at": run_completed_at, "deepen_top": deepen_top})
        rid = sr.new_report_id()
        meta = store.insert(
            report_id=rid, lei=lei, legal_name="EASY POWER",
            data=sr.canonical_bytes({"schema": sr.SCHEMA, "lei": lei, "events": []}),
            saved_at=NOW, manage_token="m" * 32,
        )
        return {**sr._public_meta(meta), "manage_token": "m" * 32}

    monkeypatch.setattr(sr, "save_from_replay", _save)


def test_a_plain_decision_is_recorded_and_shown_on_the_entry_the_feed_and_the_record(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    eid = _entry(client, token)
    page = client.get(f"/watch/{token}").json()
    assert page["decisions"] == {"undecided_material": 1, "total": 0, "evidence_available": False}
    assert page["entries"][0]["run_held"] is False  # the clock has moved past RUN_AT

    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "note": "To compliance."})
    assert r.status_code == 201, r.text
    body = r.json()
    d = body["decision"]
    assert d["status"] == "escalated" and d["note"] == "To compliance." and d["saved_report_id"] is None
    assert d["evidence_kind"] is None and d["report"] is None and d["entry_id"] == eid
    assert body["decisions"] == {"undecided_material": 0, "total": 1, "evidence_available": False}
    entry = body["entries"][0]
    assert entry["decision"]["status"] == "escalated" and entry["decision_count"] == 1
    assert body["watches"][0]["review_history"][0]["status"] == "escalated"

    # Append-only: a change of mind is a second row, the latest is current.
    r2 = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "reviewed_no_action"})
    assert r2.status_code == 201
    entry = r2.json()["entries"][0]
    assert entry["decision"]["status"] == "reviewed_no_action" and entry["decision_count"] == 2
    assert [h["status"] for h in entry["decision_history"]] == ["escalated", "reviewed_no_action"]

    root = ET.fromstring(client.get(f"/watch/{token}.atom").text)
    atom_entry = root.find(f"{ATOM}entry")
    assert atom_entry is not None
    content = atom_entry.findtext(f"{ATOM}content") or ""
    assert content.rstrip().endswith("Decision: reviewed, no action — 2026-10-18. Replaces 1 earlier decision on this entry.") or re.search(
        r"Decision: reviewed, no action — \d{4}-\d{2}-\d{2}\. Replaces 1 earlier decision on this entry\.$", content.rstrip()
    )
    cats = [(c.get("scheme"), c.get("term"), c.get("label")) for c in atom_entry.findall(f"{ATOM}category")]
    assert (dec.ATOM_SCHEME, "reviewed_no_action", "Reviewed, no action") in cats

    md = client.get(f"/watch/{token}/record.md")
    assert md.status_code == 200 and md.headers["content-type"].startswith("text/markdown")
    assert "# Review record" in md.text and "2 decisions on 1 entry" in md.text
    assert "**Escalated**" in md.text and "**Reviewed, no action**" in md.text
    assert "note: To compliance." in md.text and f"LEI {EASY}" in md.text
    assert md.text == client.get(f"/watch/{token}/record.md").text  # byte-identical on a second read

    stats = client.get("/watchstats").json()["watched"]
    assert stats["decisions_by_status"] == {"reviewed_no_action": 1, "rechecked": 0, "escalated": 1}
    assert stats["decisions_with_evidence"] == 0 and stats["material_undecided"] == 0
    assert "To compliance" not in json.dumps(stats) and EASY not in json.dumps(stats)


def test_validation_refusals_write_nothing(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    eid = _entry(client, token)
    assert client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "closed"}).status_code == 422
    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "note": "a@b.co"})
    assert r.status_code == 422 and r.json()["detail"]["field"] == "note"
    assert client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "evidence": "pdf"}).status_code == 422
    assert client.post(f"/watch/{token}/entries/{eid + 99}/decisions", json={"status": "escalated"}).status_code == 404
    # A fresh re-check as evidence goes through the heavy-tier route.
    assert client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "evidence": "recheck"}).status_code == 400
    assert client.post(f"/watch/{token}/entries/{eid}/decisions/recheck", json={"status": "escalated"}).status_code == 400
    # Another list's token cannot decide on this entry.
    other = _watch(client, PARENT)["token"]
    assert client.post(f"/watch/{other}/entries/{eid}/decisions", json={"status": "escalated"}).status_code == 404
    store = wl.get_store()
    assert store is not None and store.decisions(wl.token_hash(token)) == []


def test_entry_run_evidence_saves_the_held_run_and_a_lost_run_is_a_409_with_no_decision(
    client: TestClient, env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    token = _watch(client, EASY)["token"]
    eid = _entry(client, token)
    th = wl.token_hash(token)
    store = wl.get_store()
    assert store is not None
    # Saved reports off: evidence cannot be kept, the page says so, nothing is written.
    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "evidence": "entry_run"})
    assert r.status_code == 503 and store.decisions(th) == []

    _enable_saved_reports(tmp_path, monkeypatch)
    assert client.get(f"/watch/{token}").json()["decisions"]["evidence_available"] is True
    # The real save: the replay cache holds nothing for this run → 409, no decision.
    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "evidence": "entry_run"})
    assert r.status_code == 409 and r.headers["X-OpenCheck-Refusal"] == "run_not_held"
    assert r.json()["detail"] == dec.EVIDENCE_WORDS["run_not_held"]
    assert store.decisions(th) == []
    # An entry written before stage D has no run to save.
    old = _entry(client, token, run_at=None)
    r = client.post(f"/watch/{token}/entries/{old}/decisions", json={"status": "escalated", "evidence": "entry_run"})
    assert r.status_code == 409 and store.decisions(th) == []

    calls: list[dict[str, Any]] = []
    _stub_save(monkeypatch, calls)
    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "reviewed_no_action", "evidence": "entry_run"})
    assert r.status_code == 201, r.text
    assert calls == [{"lei": EASY, "run_completed_at": RUN_AT, "deepen_top": 5}]
    d = r.json()["decision"]
    assert d["evidence_kind"] == "entry_run" and d["saved_report_id"]
    assert d["report"]["available"] is True and d["report"]["report_path"] == f"/report/{d['saved_report_id']}"
    assert d["report"]["content_hash"] and "manage_token" not in json.dumps(d)
    # The status is the reader's, never set by the evidence.
    assert d["status"] == "reviewed_no_action"
    md = client.get(f"/watch/{token}/record.md").text
    assert f"saved report `{d['saved_report_id']}`" in md and f"sha256 `{d['report']['content_hash']}`" in md
    assert client.get("/watchstats").json()["watched"]["decisions_with_evidence"] == 1
    # The quota is checked before the save and spent after it.
    def _refuse() -> None:
        raise sr.ClientQuotaError("quota", 60)

    monkeypatch.setattr(sr, "check_client_quota", _refuse)
    r = client.post(f"/watch/{token}/entries/{eid}/decisions", json={"status": "escalated", "evidence": "entry_run"})
    assert r.status_code == 429 and len(store.decisions(th)) == 1 and len(calls) == 1


def test_recheck_evidence_reruns_then_saves_the_fresh_run(
    client: TestClient, env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    token = _watch(client, EASY)["token"]
    eid = _entry(client, token)
    th = wl.token_hash(token)
    store = wl.get_store()
    assert store is not None
    _enable_saved_reports(tmp_path, monkeypatch)
    fresh = "2026-10-18T12:30:00+00:00"
    runs: list[bool] = []

    async def _fake(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        runs.append(refresh)
        return _resp(lei, risk_signals=[], run_completed_at=fresh)

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake)
    calls: list[dict[str, Any]] = []
    _stub_save(monkeypatch, calls)
    r = client.post(f"/watch/{token}/entries/{eid}/decisions/recheck", json={"status": "rechecked", "evidence": "recheck"})
    assert r.status_code == 201, r.text
    assert runs == [True]  # a deliberate re-run bypasses the replay cache
    assert calls == [{"lei": EASY, "run_completed_at": fresh, "deepen_top": 5}]
    d = r.json()["decision"]
    assert d["entry_id"] == eid and d["evidence_kind"] == "recheck" and d["report"]["available"] is True
    # The re-run wrote its own entry (the signal went away) with its run; the decision sits on the entry asked about.
    entries = r.json()["entries"]
    assert entries[0]["id"] != eid and entries[0]["run_completed_at"] == fresh and entries[0]["decision"] is None
    assert entries[1]["id"] == eid and entries[1]["decision"]["status"] == "rechecked"

    # A re-run that fails records no decision.
    async def _boom(lei: str, deepen_top: int = 5, refresh: bool = False) -> LookupResponse:
        raise RuntimeError("down")

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _boom)
    r = client.post(f"/watch/{token}/entries/{eid}/decisions/recheck", json={"status": "escalated", "evidence": "recheck"})
    assert r.status_code == 503 and len(store.decisions(th)) == 1


def test_a_list_notice_takes_a_decision_but_no_evidence(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    th = wl.token_hash(token)
    with store._conn() as conn:
        conn.execute("UPDATE lists SET last_seen_at = '2026-06-01T00:00:00Z' WHERE token_hash = ?", (th,))
    store.expire_stale_lists(stale_days=90, grace_days=30, now=NOW)
    note = store.entries(th)[0]
    assert note["tier"] == "review"
    r = client.post(f"/watch/{token}/entries/{note['id']}/decisions", json={"status": "reviewed_no_action", "evidence": "entry_run"})
    assert r.status_code == 422 and r.json()["detail"]["field"] == "evidence"
    r = client.post(f"/watch/{token}/entries/{note['id']}/decisions", json={"status": "reviewed_no_action"})
    assert r.status_code == 201
    md = client.get(f"/watch/{token}/record.md").text
    assert "## This list" in md


def test_the_record_of_an_undecided_list_says_so(client: TestClient, env: Path) -> None:  # noqa: F811
    token = _watch(client, EASY)["token"]
    md = client.get(f"/watch/{token}/record.md").text
    assert "0 decisions on 0 entries" in md and "No decisions have been recorded on this list." in md
    assert client.get("/watch/nope/record.md").status_code == 404


# ---- parity: the frontend and the doc carry the same words -----------------------


def _ts_dict(ts: str, name: str) -> dict[str, str]:
    block = re.search(rf"export const {name}[^{{]*\{{(.*?)\}};", ts, re.S)
    assert block, f"lib/watchlist.ts must export {name}"
    return dict(re.findall(r'(\w+):\s*"((?:[^"\\]|\\.)*)"', block.group(1)))


def test_frontend_mirrors_the_decision_words() -> None:
    ts = FRONTEND.read_text(encoding="utf-8")
    assert _ts_dict(ts, "DECISION_LABELS") == dec.STATUS_LABELS
    assert _ts_dict(ts, "DECISION_MEANINGS") == dec.STATUS_MEANINGS
    words = _ts_dict(ts, "EVIDENCE_WORDS")
    assert words == dec.EVIDENCE_WORDS
    statuses = re.search(r"export const DECISION_STATUSES[^=]*=\s*\[(.*?)\]", ts, re.S)
    assert statuses and re.findall(r'"(\w+)"', statuses.group(1)) == list(dec.STATUSES)


def test_the_doc_states_the_rules() -> None:
    text = " ".join(DOC.read_text(encoding="utf-8").split())
    for phrase in (
        "change → review → decision",
        "Reviewed, no action",
        "Re-checked",
        "Escalated",
        "Append-only",
        "record.md",
        "never automatic",
    ):
        assert phrase in text, phrase
