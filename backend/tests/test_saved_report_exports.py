"""Phase 218 — reports and downloads rendered from a saved report.

A saved report's PDF, Markdown and format downloads are built from the saved
events, never from a new run: they say they are a saved report on the first
page and on every page's footer, print the SHA-256, carry the licence
assessment made when it was saved, and read nothing from today's clock — so
the same saved report renders the same report whatever the engine does later.
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.og_image import ui_date
from opencheck.reporting import build_report_html, build_report_markdown
from opencheck.reporting.html_report import _saved_when
from opencheck.routers import export as export_mod
from tests.test_saved_reports import (  # noqa: F401 — fixtures
    LEI,
    NARRATIVE_RUN,
    RUN_AT,
    _hold_narrative,
    _hold_run,
    _save,
    client,
    env,
)

OTHER_LEI = "5493001KJTIIGC8Y1R12"


def _saved(client: TestClient, **body) -> dict:
    _hold_run()
    return _save(client, **body)


@pytest.fixture
def no_new_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any attempt to run the check again fails the test."""

    async def boom(*a, **k):  # pragma: no cover — reaching it is the failure
        raise AssertionError("a saved-report export ran the check again")

    monkeypatch.setattr(export_mod, "_lookup_impl", boom)
    monkeypatch.setattr(export_mod, "_build_report", boom)


def test_markdown_from_a_saved_report_says_so_and_prints_the_hash(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.post("/export/markdown", json={"lei": LEI, "saved_report_id": meta["report_id"]})
    assert r.status_code == 200, r.text
    md = r.text
    assert md.startswith("# OpenCheck saved report — BP P.L.C.")
    assert "## Saved report" in md
    assert meta["content_hash"] in md
    assert meta["report_id"] in md
    assert f"/saved-reports/{meta['report_id']}.json" in md
    assert "Nothing in this report has been re-checked since it was saved." in md
    assert "(as assessed when the report was saved)" in md
    day = _saved_when(meta["saved_at"])[0]  # the save's date, never today's
    assert f"Rendered from saved report {meta['report_id']}, saved {day}." in md
    assert "BP P.L.C." in md
    stamp = meta["saved_at"][:10].replace("-", "")
    assert r.headers["content-disposition"].endswith(f'-saved-{stamp}.md"')


def test_markdown_is_the_same_whatever_happens_after_the_save(
    client: TestClient, no_new_runs, monkeypatch: pytest.MonkeyPatch
) -> None:
    meta = _saved(client)
    body = {"lei": LEI, "saved_report_id": meta["report_id"]}
    first = client.post("/export/markdown", json=body).text

    # A source's licence changes and the clock moves on: the record does not.
    import opencheck.licensing as licensing

    def changed(ids):
        raise AssertionError("a saved report re-assessed its licence")

    monkeypatch.setattr(licensing, "assess", changed)
    monkeypatch.setattr(export_mod, "assess_licensing", changed)
    second = client.post("/export/markdown", json=body).text
    assert first == second


def test_the_saved_narrative_and_frozen_sign_off_are_used(client: TestClient, no_new_runs) -> None:
    _hold_run()
    _hold_narrative()
    meta = _save(client, narrative_run_id=NARRATIVE_RUN)
    md = client.post("/export/markdown", json={"lei": LEI, "saved_report_id": meta["report_id"]}).text
    assert "BP is a company." in md


def test_posting_a_narrative_with_a_saved_report_is_refused(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.post(
        "/export/markdown",
        json={"lei": LEI, "saved_report_id": meta["report_id"], "narrative": {"summary": "invented"}},
    )
    assert r.status_code == 400
    assert "carries its own summary" in r.json()["detail"]


def test_a_saved_report_for_another_company_is_refused(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.post("/export/markdown", json={"lei": OTHER_LEI, "saved_report_id": meta["report_id"]})
    assert r.status_code == 400
    assert LEI in r.json()["detail"]


def test_an_unknown_saved_report_is_404_with_its_refusal_code(client: TestClient, no_new_runs) -> None:
    r = client.post("/export/markdown", json={"lei": LEI, "saved_report_id": "A" * 22})
    assert r.status_code == 404
    assert r.headers["x-opencheck-refusal"] == "not_found"


def test_pdf_route_passes_the_saved_block_to_the_renderer(
    client: TestClient, no_new_runs, monkeypatch: pytest.MonkeyPatch
) -> None:
    meta = _saved(client)
    seen: dict = {}

    def fake_pdf(report, *, narrative=None, dispositions=None, saved=None):
        seen.update(report=report, saved=saved, narrative=narrative)
        return b"%PDF-1.7 fake"

    monkeypatch.setattr(export_mod, "build_report_pdf", fake_pdf)
    r = client.post("/export/pdf", json={"lei": LEI, "saved_report_id": meta["report_id"]})
    assert r.status_code == 200, r.text
    assert seen["saved"]["content_hash"] == meta["content_hash"]
    assert seen["saved"]["saved_at"] == meta["saved_at"]
    assert seen["saved"]["run_completed_at"] == RUN_AT
    assert seen["saved"]["licensing"]["commercial_use"] == "no"  # OpenSanctions was in the run
    assert seen["report"]["legal_name"] == "BP P.L.C."


def test_the_pdf_html_footer_names_the_report_and_hash_on_every_page(client: TestClient) -> None:
    meta = _saved(client)
    saved = {
        "report_id": meta["report_id"],
        "content_hash": meta["content_hash"],
        "saved_at": meta["saved_at"],
        "run_completed_at": RUN_AT,
        "report_url": f"https://opencheck.world/report/{meta['report_id']}",
        "json_url": f"https://api.opencheck.world/saved-reports/{meta['report_id']}.json",
        "licensing": {"headline": "Frozen headline", "color": "red"},
    }
    html = build_report_html({"lei": LEI, "legal_name": "BP P.L.C.", "bods": [], "hits": []}, saved=saved)
    assert f'@bottom-left {{ content: "Saved report {meta["report_id"]}"' in html
    assert f'@bottom-center {{ content: "SHA-256 {meta["content_hash"]}"' in html
    assert "<title>OpenCheck saved report — BP P.L.C.</title>" in html
    assert "Frozen headline" in html
    assert "This is a saved report, not a live one" in html


def test_a_malformed_saved_block_never_reaches_the_page_css() -> None:
    saved = {"report_id": 'x"; } body { display:none', "content_hash": "zz", "saved_at": RUN_AT}
    html = build_report_html({"lei": LEI, "bods": [], "hits": []}, saved=saved)
    assert "display:none" not in html.split("</style>")[0]


def test_live_reports_are_unchanged() -> None:
    report = {"lei": LEI, "legal_name": "BP P.L.C.", "bods": [], "hits": []}
    html = build_report_html(report)
    md = build_report_markdown(report)
    assert "Saved report" not in html and "Saved report" not in md
    assert "This is a point-in-time snapshot" in html


def test_format_export_from_a_saved_report(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.get("/export", params={"saved_report_id": meta["report_id"], "format": "json"})
    assert r.status_code == 200, r.text
    assert r.json() == [{"statementId": "s1", "recordType": "entity"}]
    stamp = meta["saved_at"][:10].replace("-", "")
    assert r.headers["content-disposition"].endswith(f'-saved-{stamp}.json"')


def test_zip_from_a_saved_report_names_it_and_is_byte_stable(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    params = {"saved_report_id": meta["report_id"], "format": "zip"}
    first = client.get("/export", params=params)
    assert first.status_code == 200, first.text
    second = client.get("/export", params=params)
    assert first.content == second.content
    zf = zipfile.ZipFile(io.BytesIO(first.content))
    manifest = json.loads(next(zf.read(n) for n in zf.namelist() if n.endswith("manifest.json")))
    assert manifest["saved_report"]["content_hash"] == meta["content_hash"]
    assert manifest["generated_at"] == meta["saved_at"]
    assert manifest["licensing"]["commercial_use"] == "no"
    assert manifest["sources_consulted"] == ["companies_house", "opensanctions"]
    licences = next(zf.read(n) for n in zf.namelist() if n.endswith("LICENSES.md")).decode()
    assert "Non-commercial" in licences or "non-commercial" in licences


def test_subsidiaries_cannot_be_added_to_a_saved_export(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.get(
        "/export", params={"saved_report_id": meta["report_id"], "format": "json", "subsidiaries": "true"}
    )
    assert r.status_code == 400


def test_format_export_lei_must_match_the_saved_report(client: TestClient, no_new_runs) -> None:
    meta = _saved(client)
    r = client.get("/export", params={"saved_report_id": meta["report_id"], "lei": OTHER_LEI, "format": "json"})
    assert r.status_code == 400


def test_the_saved_report_renders_a_real_pdf_when_weasyprint_is_present(client: TestClient, tmp_path: Path) -> None:
    pytest.importorskip("weasyprint")
    from opencheck.reporting import PdfUnavailable

    meta = _saved(client)
    r = client.post("/export/pdf", json={"lei": LEI, "saved_report_id": meta["report_id"]})
    if r.status_code == 503:
        pytest.skip("PDF toolchain unavailable")
    assert r.status_code == 200, r.text
    assert r.content.startswith(b"%PDF")
    _ = PdfUnavailable


# ---- the share card and share page (Phase 218) --------------------------------


def test_the_saved_share_page_previews_the_record_and_redirects_to_it(client: TestClient) -> None:
    meta = _saved(client)
    rid = meta["report_id"]
    r = client.get(f"/share/saved/{rid}")
    assert r.status_code == 200, r.text
    page = r.text
    assert r.headers["x-robots-tag"] == "noindex, nofollow"
    assert f'content="0;url=https://opencheck.world/report/{rid}"' in page
    assert f"/og/saved/{rid}.png" in page
    day = ui_date(meta["saved_at"])
    assert f"Saved report · {day} · 1 risk signal found in that check · not re-checked since" in page
    assert "BP P.L.C. — saved report — OpenCheck" in page
    assert f"saved {day}" in page  # alt text


def test_the_saved_card_renders_and_says_saved(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck import og_image
    from opencheck.routers import share as share_mod

    meta = _saved(client)
    seen: dict = {}
    real = og_image.render_share_card

    def spy(name, lei, signals, *, saved_at=None):
        seen.update(name=name, lei=lei, signals=signals, saved_at=saved_at)
        return real(name, lei, signals, saved_at=saved_at)

    monkeypatch.setattr(share_mod, "render_share_card", spy)
    r = client.get(f"/og/saved/{meta['report_id']}.png")
    assert r.status_code == 200, r.text
    assert r.content.startswith(b"\x89PNG")
    assert r.headers["x-robots-tag"] == "noindex, nofollow"
    assert seen["saved_at"] == meta["saved_at"]
    assert seen["lei"] == LEI and seen["name"] == "BP P.L.C."
    assert [s["code"] for s in seen["signals"]] == ["RETIRED_SIGNAL_CODE_FROM_2026"]


def test_a_saved_card_does_not_quote_todays_source_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck import og_image

    drawn: list[str] = []
    real_text = og_image.ImageDraw.ImageDraw.text

    def spy(self, xy, text, *a, **k):
        drawn.append(str(text))
        return real_text(self, xy, text, *a, **k)

    monkeypatch.setattr(og_image.ImageDraw.ImageDraw, "text", spy)
    og_image.render_share_card("BP P.L.C.", LEI, [], saved_at=RUN_AT)
    assert "in the check that was saved" in drawn
    assert not any("open sources" in t for t in drawn)
    assert "Saved report · " in drawn


def test_a_deleted_saved_report_has_no_card_or_share_page(client: TestClient) -> None:
    meta = _saved(client)
    rid = meta["report_id"]
    assert client.get(f"/og/saved/{rid}.png").status_code == 200
    d = client.delete(f"/saved-reports/{rid}", headers={"X-OpenCheck-Manage-Token": meta["manage_token"]})
    assert d.status_code == 200
    assert client.get(f"/og/saved/{rid}.png").status_code == 404
    assert client.get(f"/share/saved/{rid}").status_code == 404


def test_ui_date_is_the_page_format() -> None:
    assert ui_date("2026-09-16T00:30:00+01:00") == "15 Sept 2026"
    assert ui_date("2026-06-01T12:00:00Z") == "1 June 2026"


# ---- the MCP save tool (Phase 218) --------------------------------------------


async def test_the_mcp_save_tool_saves_the_run_it_just_ran(monkeypatch: pytest.MonkeyPatch) -> None:
    import hashlib

    from opencheck import saved_reports as sr
    from opencheck.mcp import server as mcp_server
    from opencheck.routers import lookup as lookup_mod
    from tests.test_saved_reports import _events

    calls: list[tuple[str, int]] = []

    async def fake_lookup(*, lei: str, deepen_top: int = 5, refresh: bool = False):
        calls.append((lei, deepen_top))
        _hold_run()
        return lookup_mod.fold_lookup_events(lei, _events())

    monkeypatch.setattr(lookup_mod, "_lookup_impl", fake_lookup)
    out = await mcp_server.opencheck_save_report(LEI)
    assert "error" not in out, out
    assert calls == [(LEI, 5)]
    assert out["url"].endswith(f"/report/{out['report_id']}")
    assert out["json_url"].endswith(f"/saved-reports/{out['report_id']}.json")
    assert out["run_completed_at"] == RUN_AT
    assert out["manage_token"]
    assert "Non-commercial" in out["licensing"] or "non-commercial" in out["licensing"].lower()
    _, data = sr.load_report(sr.get_store(), out["report_id"])
    assert hashlib.sha256(data).hexdigest() == out["content_hash"]


async def test_the_mcp_save_tool_says_when_saving_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck import saved_reports as sr
    from opencheck.mcp import server as mcp_server

    monkeypatch.setattr(sr, "get_store", lambda: None)
    out = await mcp_server.opencheck_save_report(LEI)
    assert out["status"] == 503


# ----------------------------------------------------------------------
# Phase 226 — "What can be known" in the exports, from the frozen payload
# ----------------------------------------------------------------------
def test_markdown_carries_the_frozen_knowability_paragraphs(client: TestClient, no_new_runs, monkeypatch) -> None:
    """The section is read from the saved ``knowability`` / ``knowability_chain``
    events — the sentences true on the day of the run — never re-rendered
    from today's table: ``statement_for`` is made to explode."""
    import opencheck.knowability as know

    monkeypatch.setattr(know, "statement_for", lambda *a, **k: (_ for _ in ()).throw(AssertionError("re-rendered")))
    meta = _saved(client)
    md = client.post("/export/markdown", json={"lei": LEI, "saved_report_id": meta["report_id"]}).text
    assert "## What can be known" in md
    assert "**United Kingdom** (checked 2026-09-16) — PSC register frozen sentence from the day of the run." in md
    assert "**Cayman Islands** (unverified draft) — Cayman frozen sentence from the day of the run." in md
    assert "facts OpenCheck held as of 2026-09-16" in md
    # Subject first, chain after; the subject is not repeated from the chain.
    assert md.count("frozen sentence from the day of the run") == 2
    assert md.index("United Kingdom** (checked") < md.index("Cayman Islands** (unverified")


def test_pdf_html_carries_the_same_section(client: TestClient, no_new_runs, monkeypatch) -> None:
    from opencheck.reporting import html_report

    import asyncio

    from opencheck.routers.saved_reports import open_for_export

    meta = _saved(client)
    saved = asyncio.run(open_for_export(meta["report_id"]))
    html = html_report.build_report_html(saved.response.model_dump(), saved=saved.saved)
    assert '<h2 id="know">What can be known</h2>' in html
    assert "PSC register frozen sentence from the day of the run." in html
    assert "Cayman frozen sentence from the day of the run." in html
    # It sits between "What each source found" and the diagrams/licensing.
    assert html.index('id="src"') < html.index('id="know"') < html.index("Licensing")


def test_a_payload_without_the_knowability_events_has_no_section() -> None:
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    report = {"lei": LEI, "legal_name": "Old Co", "hits": [], "bods": [], "risk_signals": [],
              "errors": {}, "license_notices": [], "cross_source_links": [], "bods_issues": []}
    assert "What can be known" not in build_report_markdown(report)
    assert "What can be known" not in build_report_html(report)

