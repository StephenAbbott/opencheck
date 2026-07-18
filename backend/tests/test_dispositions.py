"""Analyst control plane: run identity, disposition persistence, API, and PDF-HTML rendering.

Covers the acceptance criteria of the analyst-control-plane spec:
run_id determinism, disposition round-trip (API + store) surviving restarts,
the gap-citation validator rule, and claims/dispositions/gaps rendering in the
report HTML that feeds the tagged PDF.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.dispositions import (
    ClaimDisposition,
    DispositionRecord,
    compute_run_id,
    load_dispositions,
    save_dispositions,
    validate_keys,
)
from opencheck.narrative.packet import EvidencePacket
from opencheck.narrative.validate import validate_narrative
from opencheck.reporting.html_report import build_report_html

LEI = "2138000000000000A001"
RUN_ID = "0123456789abcdef"


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# run_id
# ---------------------------------------------------------------------------


def test_run_id_is_deterministic_and_order_insensitive():
    a = compute_run_id(LEI, "v1", "claude-sonnet-4-6", "Summary.", ["claim b", "claim a"])
    b = compute_run_id(LEI, "v1", "claude-sonnet-4-6", "Summary.", ["claim a", "claim b"])
    assert a == b
    assert len(a) == 16 and all(c in "0123456789abcdef" for c in a)


def test_run_id_changes_when_the_narrative_changes():
    base = compute_run_id(LEI, "v1", "m", "Summary.", ["claim a"])
    assert compute_run_id(LEI, "v1", "m", "Different summary.", ["claim a"]) != base
    assert compute_run_id(LEI, "v2", "m", "Summary.", ["claim a"]) != base
    assert compute_run_id(LEI, "v1", "m", "Summary.", ["claim a", "claim b"]) != base


def test_run_id_field_separator_is_unambiguous():
    # "ab" + "c" must not collide with "a" + "bc".
    assert compute_run_id(LEI, "ab", "c", "s", []) != compute_run_id(LEI, "a", "bc", "s", [])


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------


def test_validate_keys_rejects_path_unsafe_input():
    with pytest.raises(ValueError):
        validate_keys("../../../etc/passwd", RUN_ID)
    with pytest.raises(ValueError):
        validate_keys(LEI, "../escape")
    with pytest.raises(ValueError):
        validate_keys(LEI, "ABCDEF0123456789")  # uppercase hex is not a run_id
    validate_keys(LEI, RUN_ID)  # well-formed passes


def test_save_and_load_round_trip(data_root):
    rec = DispositionRecord(
        lei=LEI,
        run_id=RUN_ID,
        prompt_version="v1",
        model="m",
        dispositions=[
            ClaimDisposition(claim_id="c1", status="accepted"),
            ClaimDisposition(claim_id="c2", status="disputed", comment="check filing date"),
        ],
    )
    saved = save_dispositions(rec)
    assert saved.updated_at is not None
    assert all(d.decided_at is not None for d in saved.dispositions)

    loaded = load_dispositions(LEI, RUN_ID)
    assert loaded is not None
    assert {d.claim_id: d.status for d in loaded.dispositions} == {
        "c1": "accepted",
        "c2": "disputed",
    }
    assert loaded.dispositions[1].comment == "check filing date"
    # Stored under data/dispositions/<LEI>/<run_id>.json (auditable on disk).
    assert (data_root / "dispositions" / LEI / f"{RUN_ID}.json").is_file()


def test_unchanged_claims_keep_their_decided_at(data_root):
    rec = DispositionRecord(
        lei=LEI,
        run_id=RUN_ID,
        dispositions=[ClaimDisposition(claim_id="c1", status="accepted")],
    )
    first = save_dispositions(rec)
    t1 = first.dispositions[0].decided_at
    time.sleep(0.01)

    # Re-save unchanged → decided_at preserved; change status → re-stamped.
    second = save_dispositions(rec)
    assert second.dispositions[0].decided_at == t1

    changed = DispositionRecord(
        lei=LEI,
        run_id=RUN_ID,
        dispositions=[ClaimDisposition(claim_id="c1", status="disputed")],
    )
    third = save_dispositions(changed)
    assert third.dispositions[0].decided_at != t1


def test_load_missing_returns_none(data_root):
    assert load_dispositions(LEI, "ffffffffffffffff") is None


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------


def test_dispositions_api_round_trip(data_root):
    client = TestClient(app)
    r = client.put(
        "/narrative/dispositions",
        json={
            "lei": LEI.lower(),  # normalised server-side
            "run_id": RUN_ID,
            "prompt_version": "v1",
            "model": "m",
            "dispositions": [
                {"claim_id": "c1", "status": "accepted"},
                {"claim_id": "c2", "status": "needs_review", "comment": "unclear share band"},
            ],
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["lei"] == LEI
    assert body["updated_at"] and body["dispositions"][0]["decided_at"]

    r2 = client.get("/narrative/dispositions", params={"lei": LEI, "run_id": RUN_ID})
    assert r2.status_code == 200
    assert {d["claim_id"] for d in r2.json()["dispositions"]} == {"c1", "c2"}


def test_dispositions_api_validation(data_root):
    client = TestClient(app)
    r = client.put(
        "/narrative/dispositions",
        json={"lei": LEI, "run_id": "../escape", "dispositions": []},
    )
    assert r.status_code == 400

    r2 = client.get("/narrative/dispositions", params={"lei": LEI, "run_id": "not-hex"})
    assert r2.status_code == 400

    r3 = client.get(
        "/narrative/dispositions", params={"lei": LEI, "run_id": "ffffffffffffffff"}
    )
    assert r3.status_code == 404

    # An invalid status is rejected by the request model.
    r4 = client.put(
        "/narrative/dispositions",
        json={
            "lei": LEI,
            "run_id": RUN_ID,
            "dispositions": [{"claim_id": "c1", "status": "maybe"}],
        },
    )
    assert r4.status_code == 422


# ---------------------------------------------------------------------------
# gap-citation validator rule
# ---------------------------------------------------------------------------


def _packet_with_gap() -> EvidencePacket:
    return EvidencePacket(
        subject_name="Northwind Logistics Ltd",
        lei=LEI,
        facts=[{"id": "f1", "statement": "X is registered.", "source_name": "UK Companies House"}],
        gaps=["OpenSanctions could not be queried (timeout)."],
    )


def test_uncited_gap_is_surfaced_but_does_not_invalidate():
    packet = _packet_with_gap()
    result = validate_narrative(
        packet,
        {"summary": "S.", "claims": [{"id": "c1", "text": "X.", "fact_ids": ["f1"]}]},
    )
    assert result.ok is True  # ok tracks ungrounded claims only
    assert result.uncited_gaps == ["g1"]
    assert any("g1" in i for i in result.issues)


def test_cited_gap_produces_no_issue():
    packet = _packet_with_gap()
    result = validate_narrative(
        packet,
        {
            "summary": "S.",
            "claims": [
                {"id": "c1", "text": "X.", "fact_ids": ["f1"]},
                {"id": "c2", "text": "Sanctions could not be checked.", "fact_ids": ["g1"]},
            ],
        },
    )
    assert result.uncited_gaps == []


# ---------------------------------------------------------------------------
# report HTML (feeds the tagged PDF)
# ---------------------------------------------------------------------------


def _narrative_dict() -> dict:
    return {
        "summary": "Northwind Logistics Ltd is a registered entity.",
        "overall_confidence": "high",
        "model": "claude-sonnet-4-6",
        "prompt_version": "v1",
        "run_id": RUN_ID,
        "generated_at": "2026-07-18T10:00:00+00:00",
        "claims": [
            {"id": "c1", "text": "X is registered.", "fact_ids": ["f1"]},
            {"id": "c2", "text": "No owner was disclosed.", "fact_ids": ["g1"]},
        ],
        "packet": {
            "facts": [{"id": "f1", "source_name": "UK Companies House"}],
            "risks": [],
            "gaps": [{"id": "g1", "statement": "No beneficial owner was disclosed."}],
        },
    }


def _report_dict() -> dict:
    return {"lei": LEI, "bods": [], "hits": [], "risk_signals": []}


def test_report_html_renders_claims_dispositions_and_gaps():
    dispositions = {
        "lei": LEI,
        "run_id": RUN_ID,
        "updated_at": "2026-07-18T11:00:00+00:00",
        "dispositions": [
            {"claim_id": "c1", "status": "accepted", "decided_at": "2026-07-18T10:30:00+00:00"},
            {
                "claim_id": "c2",
                "status": "disputed",
                "comment": "Verify against the register extract.",
                "decided_at": "2026-07-18T10:31:00+00:00",
            },
        ],
    }
    html = build_report_html(
        _report_dict(), narrative=_narrative_dict(), dispositions=dispositions
    )
    assert "Claims and analyst dispositions" in html
    assert "Accepted" in html and "Disputed" in html
    assert "Analyst note: Verify against the register extract." in html
    assert "1 accepted" in html and "1 disputed" in html
    # Gaps are always rendered from the packet, disposition-independent.
    assert "Not verified in this check" in html
    assert "No beneficial owner was disclosed." in html
    # Run metadata makes the PDF self-describing as an audit artefact.
    assert f"run {RUN_ID}" in html
    assert "dispositions updated 2026-07-18" in html


def test_report_html_without_dispositions_still_lists_claims_and_gaps():
    html = build_report_html(_report_dict(), narrative=_narrative_dict())
    assert "Claims and analyst dispositions" in html
    assert "X is registered." in html
    assert "Not verified in this check" in html
    assert "Accepted" not in html  # no disposition badges without a record


def test_report_html_undecided_tally():
    dispositions = {
        "lei": LEI,
        "run_id": RUN_ID,
        "dispositions": [{"claim_id": "c1", "status": "accepted"}],
    }
    html = build_report_html(
        _report_dict(), narrative=_narrative_dict(), dispositions=dispositions
    )
    assert "1 undecided" in html
