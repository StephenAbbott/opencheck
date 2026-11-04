"""The discrepancy note (the "Discrepancy report export for registers" ticket).

The golden shape is the one the plan was started for: a UK company Companies
House records as dissolved while GLEIF's record still reads ACTIVE — the
BANK SADERAT PLC shape on the findings golden set. Every test is named for a
rule Stephen set on 3 Nov 2026: only the shown ``RECORD_CONSISTENCY``
disagreements qualify; one note per register holding a disputed value, GLEIF
included; describe and never allege, with the knowability wording guard; the
route from the jurisdictions table; the saved-report footer only when saved;
blank reporter lines; a saved report exports the dates of its own day.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from opencheck import discrepancy_note as dn
from opencheck.bods import liveness
from opencheck.consistency import assess_consistency, build_signal
from opencheck.knowability import BANNED_VOCABULARY
from opencheck.routers import export as export_mod
from opencheck.routers import lookup as lookup_mod
from tests.test_consistency import LEI, _stmt
from tests.test_saved_reports import (  # noqa: F401 — fixtures
    RUN_AT,
    _events,
    _save,
    client,
    env,
)
from tests.test_saved_reports import LEI as SAVED_LEI

CH_URL = "https://find-and-update.company-information.service.gov.uk/company/04366849"
NOW = datetime(2026, 11, 3, 14, 0, tzinfo=UTC)


def _golden(lei: str = LEI) -> dict:
    """Companies House dissolved (since 3 Apr 2019) vs GLEIF ACTIVE, each
    statement dated and retrieved on its own day, each carrying the
    register's own status word as production statements do — the note
    quotes that word, never OpenCheck's liveness class (the Phase 337 rule)."""
    ch = _stmt("companies_house", lei=lei, number=("GB-COH", "04366849"), name="BANK SADERAT PLC")
    liveness.apply_register_status(ch, source_label="companies_house", liveness=liveness.TERMINAL,
                                   raw="dissolved", since="2019-04-03")
    gl = _stmt("gleif", lei=lei, name="BANK SADERAT PLC",
               number=None if lei else ("GB-COH", "04366849"))
    liveness.apply_register_status(gl, source_label="gleif", liveness=liveness.LIVE, raw="ACTIVE")
    ch["statementDate"] = "2026-10-30"
    ch["source"]["retrievedAt"] = "2026-11-02T10:15:00Z"
    ch["source"]["url"] = CH_URL
    gl["statementDate"] = "2026-11-01"
    gl["source"]["retrievedAt"] = "2026-11-02T10:15:03Z"
    stmts = [ch, gl]
    sig = build_signal(
        assess_consistency(stmts), stmts, subject=lei,
        subject_statement_ids={s["statementId"] for s in stmts}, degraded=None,
    )
    assert sig is not None
    return {"lei": lei, "legal_name": "BANK SADERAT PLC", "bods": stmts, "risk_signals": [sig]}


# ---- qualifying findings ----------------------------------------------------


def test_only_shown_disagreements_qualify() -> None:
    report = _golden()
    items = dn.qualifying_items(report)
    assert [i["field"] for i in items] == ["liveness"]
    assert dn.qualifies(report)


def test_agreement_alone_is_no_note() -> None:
    stmts = [_stmt("companies_house", status=liveness.LIVE), _stmt("gleif", status=liveness.LIVE)]
    sig = build_signal(assess_consistency(stmts), stmts, subject=LEI,
                       subject_statement_ids={s["statementId"] for s in stmts}, degraded=None)
    report = {"lei": LEI, "bods": stmts, "risk_signals": [sig]}
    assert dn.qualifying_items(report) == []
    assert dn.build_notes(report) == []
    assert dn.build_discrepancy_note(report) is None


def test_no_signal_at_all_is_no_note() -> None:
    assert dn.build_notes({"lei": LEI, "bods": [], "risk_signals": []}) == []


def test_a_counted_but_unshown_field_never_reaches_a_note() -> None:
    """The gate's items are what the signal carries; a founding-date clash is
    counted on /consistencystats and not in the evidence, so not here."""
    report = _golden()
    for item in report["risk_signals"][0]["evidence"]["items"]:
        assert item["field"] in ("liveness", "jurisdiction")


# ---- one note per register, GLEIF included -------------------------------


def test_one_note_per_register_holding_a_disputed_value_gleif_included() -> None:
    notes = dn.build_notes(_golden(), now=NOW)
    assert [n.source_id for n in notes] == ["companies_house", "gleif"]
    ch, gl = notes
    assert ch.register_name == "UK Companies House" and gl.register_name == "GLEIF"
    (d,) = ch.discrepancies
    assert d.field == "liveness" and d.field_noun == "register status"
    assert d.register.source_id == "companies_house" and d.other.source_id == "gleif"
    assert d.register.value == "dissolved (since 3 Apr 2019)" and d.other.value == "ACTIVE"
    # The same disagreement, seen from GLEIF's side.
    (g,) = gl.discrepancies
    assert g.register.source_id == "gleif" and g.other.source_id == "companies_house"


def test_a_non_register_side_is_quoted_never_addressed() -> None:
    # Brreg vs OpenAleph on jurisdiction — a pair the gate admits for that field.
    stmts = [
        _stmt("brreg", jurisdiction=("Norway", "NO"), number=("NO-BRC", "923609016")),
        _stmt("openaleph", jurisdiction=("Sweden", "SE"), number=("NO-BRC", "923609016")),
    ]
    sig = build_signal(assess_consistency(stmts), stmts, subject=LEI,
                       subject_statement_ids={s["statementId"] for s in stmts}, degraded=None)
    assert sig is not None and sig["evidence"]["disagreed_fields"] == ["jurisdiction"]
    notes = dn.build_notes({"lei": LEI, "bods": stmts, "risk_signals": [sig]})
    assert [n.source_id for n in notes] == ["brreg"]
    (d,) = notes[0].discrepancies
    assert d.field == "jurisdiction" and d.other.source_id == "openaleph"
    assert notes[0].jurisdiction == "NO" and notes[0].route.url  # the Altinn form


def test_every_official_register_is_an_addressee_and_nothing_else_is() -> None:
    from opencheck.bods.statements import OFFICIAL_REGISTER_SOURCES

    assert dn.ADDRESSEE_SOURCES == OFFICIAL_REGISTER_SOURCES | {"gleif"}
    assert "openaleph" not in dn.ADDRESSEE_SOURCES and "opencorporates" not in dn.ADDRESSEE_SOURCES


# ---- what each side carries -------------------------------------------------


def test_each_side_carries_dates_licence_and_record_link_from_the_frozen_bods() -> None:
    ch, gl = dn.build_notes(_golden(), now=NOW)
    reg, other = ch.discrepancies[0].register, ch.discrepancies[0].other
    assert reg.statement_date == "2026-10-30" and reg.retrieved_at == "2026-11-02T10:15:00Z"
    assert reg.record_url == CH_URL and reg.licence == "OGL-3.0"
    assert other.statement_date == "2026-11-01" and other.licence == "CC0-1.0"
    assert ch.identifiers == [{"scheme": "GB-COH", "id": "04366849"}]
    assert ch.record_url == CH_URL
    assert gl.record_url == f"https://search.gleif.org/#/record/{LEI}"
    assert ch.lei == LEI and ch.subject_name == "BANK SADERAT PLC"


def test_the_route_comes_from_the_jurisdictions_table_and_gleifs_from_its_challenge_facility() -> None:
    ch, gl = dn.build_notes(_golden(), now=NOW)
    assert ch.jurisdiction == "GB"
    assert ch.route.url == "https://find-and-update.company-information.service.gov.uk/report-a-discrepancy/discrepancy-category"
    assert ch.route.company_register_url == "https://www.gov.uk/contact-companies-house"
    assert ch.route.note and "30A" in ch.route.note
    assert gl.jurisdiction is None and gl.route.url == dn.GLEIF_CHALLENGE["url"]


def test_a_register_with_no_recorded_route_says_so() -> None:
    route = dn.route_for("zefix", "CH")
    assert route.url is None
    md = dn.render_markdown([
        dn.Note(source_id="zefix", register_name="Zefix", jurisdiction="CH", subject_name="X", lei=LEI,
                identifiers=[], record_url=None, discrepancies=[], route=route, generated_at=NOW.isoformat())
    ])
    assert dn.NO_ROUTE_SENTENCE in md


def test_routes_are_recorded_for_the_five_first_registers() -> None:
    from opencheck.knowability import get_jurisdiction

    for code in ("GB", "EE", "LV", "DK", "NO"):
        j = get_jurisdiction(code)
        assert j is not None and j.bo_register.discrepancy_route, code
        assert j.bo_register.discrepancy_note, code


# ---- wording ---------------------------------------------------------------


def test_the_fixed_sentences_describe_and_never_allege() -> None:
    for text in dn.FIXED_TEXT:
        assert dn.wording_issues(text) == [], text
    md = dn.render_markdown(dn.build_notes(_golden(), now=NOW))
    assert dn.wording_issues(md) == []
    # The honest-framing sentence is on every note, verbatim.
    assert md.count(dn.SCOPE_SENTENCE) == 2
    assert "not a beneficial-ownership discrepancy report" in dn.SCOPE_SENTENCE
    assert "does not say which is right" in dn.PURPOSE_SENTENCE


def test_wording_issues_reads_the_knowability_list() -> None:
    assert dn.wording_issues("a risky shell with hidden owners") == sorted({"risky", "shell", "hidden"} & set(BANNED_VOCABULARY))
    assert dn.wording_issues("") == []


# ---- rendering ---------------------------------------------------------------


def test_markdown_carries_the_subject_the_sides_the_route_and_blank_reporter_lines() -> None:
    md = dn.render_markdown(dn.build_notes(_golden(), now=NOW))
    assert md.startswith(f"# {dn.TITLE}\n\n**To:** UK Companies House")
    assert f"- LEI: {LEI}" in md and "- GB-COH: 04366849" in md
    assert "UK Companies House records *dissolved (since 3 Apr 2019)*" in md
    assert "GLEIF records *ACTIVE*" in md
    assert "Stated 30 Oct 2026; retrieved by OpenCheck 2 Nov 2026 10:15 UTC; licence UK Open Government Licence v3.0 (OGL-3.0)." in md
    assert "Reporting route: https://find-and-update.company-information.service.gov.uk/report-a-discrepancy/discrepancy-category" in md
    for label in dn.REPORTER_FIELDS:
        assert f"- {label}: ____" in md
    # Two notes, one per register, separated by a rule.
    assert md.count("**To:** ") == 2 and "\n\n---\n\n# " in md


def test_an_unsaved_run_says_how_to_get_a_verifiable_link() -> None:
    md = dn.render_markdown(dn.build_notes(_golden(), now=NOW))
    assert dn.NO_SAVED_SENTENCE in md
    assert "Generated by OpenCheck 3 Nov 2026 14:00 UTC." in md
    assert "SHA-256" not in md.replace(dn.NO_SAVED_SENTENCE, "")


def test_a_saved_run_prints_the_link_and_hash_and_reads_no_clock() -> None:
    saved = {
        "report_url": "https://opencheck.world/report/abc", "content_hash": "f" * 64,
        "run_completed_at": "2026-10-20T09:00:00+00:00", "saved_at": "2026-10-20T09:05:00+00:00",
    }
    md = dn.render_markdown(dn.build_notes(_golden(), saved=saved, now=NOW))
    assert "OpenCheck saved report: https://opencheck.world/report/abc" in md
    assert f"SHA-256 of the saved record: `{'f' * 64}`" in md
    assert "Run completed 20 Oct 2026 09:00 UTC; saved 20 Oct 2026 09:05 UTC." in md
    assert dn.NO_SAVED_SENTENCE not in md and "Generated by OpenCheck" not in md


def test_html_escapes_and_breaks_pages_between_notes() -> None:
    report = _golden()
    report["legal_name"] = "A <B> & C"
    html = dn.render_html(dn.build_notes(report, now=NOW))
    assert "A &lt;B&gt; &amp; C" in html and "<B>" not in html
    assert html.count('<section class="note">') == 2
    assert ".note + .note { page-break-before: always; }" in html


def test_pdf_renders_through_the_report_toolchain() -> None:
    pytest.importorskip("weasyprint")
    out = dn.build_discrepancy_note(_golden(), fmt="pdf", now=NOW)
    assert isinstance(out, bytes) and out.startswith(b"%PDF")


def test_summary_names_addressee_fields_and_route() -> None:
    s = dn.summary(dn.build_notes(_golden(), now=NOW))
    assert [n["to"] for n in s["notes"]] == ["companies_house", "gleif"]
    assert s["notes"][0]["fields"] == ["liveness"] and s["notes"][0]["jurisdiction"] == "GB"
    assert s["notes"][1]["route"] == dn.GLEIF_CHALLENGE["url"]


# ---- the route ---------------------------------------------------------------


def _fake_lookup_factory(report: dict):
    from opencheck.routers.lookup import LookupResponse
    from opencheck.sources.base import SearchKind

    async def fake(lei, deepen_top=5, refresh=False):
        return LookupResponse(
            query=lei, kind=SearchKind.ENTITY, hits=[], errors={}, cross_source_links=[],
            risk_signals=report["risk_signals"], bods=report["bods"], bods_issues=[], license_notices=[],
            lei=lei, legal_name=report.get("legal_name"), jurisdiction="GB", derived_identifiers={},
        )

    return fake


def test_route_streams_markdown_for_a_qualifying_run(monkeypatch) -> None:
    from opencheck.app import app

    monkeypatch.setattr(export_mod, "_lookup_impl", _fake_lookup_factory(_golden()))
    r = TestClient(app).post("/export/discrepancy-note", json={"lei": LEI})
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/markdown")
    assert r.headers["content-disposition"].startswith('attachment; filename="opencheck-discrepancy-note-bank-saderat-plc-')
    assert r.text.startswith(f"# {dn.TITLE}")
    assert "**To:** UK Companies House" in r.text and "**To:** GLEIF" in r.text


def test_route_is_404_when_nothing_qualifies(monkeypatch) -> None:
    from opencheck.app import app

    stmts = [_stmt("companies_house", status=liveness.LIVE), _stmt("gleif", status=liveness.LIVE)]
    sig = build_signal(assess_consistency(stmts), stmts, subject=LEI,
                       subject_statement_ids={s["statementId"] for s in stmts}, degraded=None)
    monkeypatch.setattr(export_mod, "_lookup_impl", _fake_lookup_factory({"bods": stmts, "risk_signals": [sig]}))
    r = TestClient(app).post("/export/discrepancy-note", json={"lei": LEI})
    assert r.status_code == 404
    assert "no disagreement" in r.json()["detail"]


def test_route_needs_a_subject() -> None:
    from opencheck.app import app

    r = TestClient(app).post("/export/discrepancy-note", json={})
    assert r.status_code == 400


def test_route_pdf(monkeypatch) -> None:
    pytest.importorskip("weasyprint")
    from opencheck.app import app

    monkeypatch.setattr(export_mod, "_lookup_impl", _fake_lookup_factory(_golden()))
    r = TestClient(app).post("/export/discrepancy-note", json={"lei": LEI, "format": "pdf"})
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/pdf" and r.content.startswith(b"%PDF")
    assert r.headers["content-disposition"].endswith('.pdf"')


def test_route_takes_a_register_number_for_a_company_with_no_lei(monkeypatch) -> None:
    from opencheck.app import app
    from opencheck.routers.lookup import RegisterLookupResponse
    from opencheck.sources.base import SearchKind

    report = _golden(lei=None)  # type: ignore[arg-type]
    seen = {}

    async def fake_register(scheme, ident, deepen_top=5, refresh=False, name=""):
        seen.update(scheme=scheme, ident=ident)
        return RegisterLookupResponse(
            query=f"{scheme}:{ident}", kind=SearchKind.ENTITY, hits=[], errors={}, cross_source_links=[],
            risk_signals=report["risk_signals"], bods=report["bods"], bods_issues=[], license_notices=[],
            lei=None, scheme=scheme, id=ident, legal_name="BANK SADERAT PLC", derived_identifiers={},
        )

    monkeypatch.setattr(lookup_mod, "_register_lookup_impl", fake_register)
    r = TestClient(app).post("/export/discrepancy-note", json={"scheme": "GB-COH", "id": "04366849"})
    assert r.status_code == 200, r.text
    assert seen == {"scheme": "GB-COH", "ident": "04366849"}
    assert "- LEI:" not in r.text and "- GB-COH: 04366849" in r.text


# ---- a saved report ---------------------------------------------------------


def _hold_golden_run() -> dict:
    """The saved-report fixtures' run, with the golden disagreement in it."""
    report = _golden(lei=SAVED_LEI)
    events = []
    for name, data in _events():
        if name == "deepen_result":
            data = {"source_id": "gleif", "bods": report["bods"]}
        if name == "risk_signals":
            data = {**data, "signals": report["risk_signals"]}
        events.append((name, data))
    lookup_mod._REPLAY_CACHE[f"{SAVED_LEI}:5"] = lookup_mod._ReplayEntry(
        stored=time.monotonic(), fetched_at=RUN_AT, events=events
    )
    return report


def test_a_saved_report_exports_its_own_note_with_the_hash_and_never_runs_the_check(
    client: TestClient, monkeypatch
) -> None:
    _hold_golden_run()
    meta = _save(client)

    async def boom(*a, **k):  # pragma: no cover — reaching it is the failure
        raise AssertionError("a saved-report export ran the check again")

    monkeypatch.setattr(export_mod, "_lookup_impl", boom)
    r = client.post("/export/discrepancy-note", json={"lei": SAVED_LEI, "saved_report_id": meta["report_id"]})
    assert r.status_code == 200, r.text
    assert meta["content_hash"] in r.text and meta["report_id"] in r.text
    assert dn.NO_SAVED_SENTENCE not in r.text
    assert r.headers["content-disposition"].endswith('-saved-20260916.md"') or "-saved-" in r.headers["content-disposition"]


# ---- the MCP tool ------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_tool_returns_markdown_and_a_routing_summary(monkeypatch) -> None:
    from opencheck.mcp import TOOL_NAMES
    from opencheck.mcp import server as mcp_server
    from opencheck.mcp.guard import TOOL_TIERS

    assert "opencheck_discrepancy_note" in TOOL_NAMES and "opencheck_discrepancy_note" in TOOL_TIERS
    monkeypatch.setattr(lookup_mod, "_lookup_impl", _fake_lookup_factory(_golden()))
    out = await mcp_server.opencheck_discrepancy_note(lei=LEI)
    assert out["subject"] == LEI and out["saved_report"] is None
    assert [n["to"] for n in out["notes"]] == ["companies_house", "gleif"]
    assert out["markdown"].startswith(f"# {dn.TITLE}")


@pytest.mark.asyncio
async def test_mcp_tool_says_when_nothing_qualifies(monkeypatch) -> None:
    from opencheck.mcp import server as mcp_server

    stmts = [_stmt("companies_house", status=liveness.LIVE), _stmt("gleif", status=liveness.LIVE)]
    sig = build_signal(assess_consistency(stmts), stmts, subject=LEI,
                       subject_statement_ids={s["statementId"] for s in stmts}, degraded=None)
    monkeypatch.setattr(lookup_mod, "_lookup_impl", _fake_lookup_factory({"bods": stmts, "risk_signals": [sig]}))
    out = await mcp_server.opencheck_discrepancy_note(lei=LEI)
    assert out["status"] == 404 and "markdown" not in out
    assert await mcp_server.opencheck_discrepancy_note() == {"error": "Give an LEI, or a register scheme and id.", "status": 400}


# ---- the frontend copy ------------------------------------------------------


def test_the_frontend_qualifies_on_the_same_rule() -> None:
    """``lib/discrepancyNote.ts`` decides whether the menu item is enabled;
    this side decides whether the route answers. Both read the shown
    ``disagree`` rows of RECORD_CONSISTENCY and nothing else."""
    from pathlib import Path

    from opencheck.consistency import DISAGREE

    ts = (Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "discrepancyNote.ts").read_text()
    assert 'code === "RECORD_CONSISTENCY"' in ts
    assert f'r.relation === "{DISAGREE}"' in ts
