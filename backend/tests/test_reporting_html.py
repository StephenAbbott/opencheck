"""Offline tests for the report HTML builder (no WeasyPrint needed).

A WeasyPrint render test is included but skips automatically until the optional
PDF toolchain is installed (added with the Render image in a later step).
"""

from __future__ import annotations

import pytest

from opencheck.reporting import build_report_html


def _report() -> dict:
    return {
        "lei": "2138000000000000A001",
        "legal_name": "Northwind Logistics Ltd",
        "jurisdiction": "GB",
        "derived_identifiers": {"company_number": "08123456"},
        "hits": [
            {"source_id": "companies_house", "is_stub": False, "name": "Northwind Logistics Ltd",
             "summary": "Active private company, incorporated 2016-04-06."},
            {"source_id": "gleif", "is_stub": False, "name": "Northwind Logistics Ltd",
             "summary": "LEI issued; no parent reported."},
            {"source_id": "kvk", "is_stub": True, "name": "stub", "summary": "stub"},
        ],
        "risk_signals": [],
        "license_notices": [],
        "bods": [
            {"statementId": "ent-1", "recordType": "entity",
             "recordDetails": {"name": "Northwind Logistics Ltd",
                               "identifiers": [{"scheme": "XI-LEI", "id": "2138000000000000A001"}]},
             "source": {"description": "UK Companies House"}},
            {"statementId": "per-1", "recordType": "person",
             "recordDetails": {"names": [{"fullName": "Jane Eleanor Smith"}]}},
            {"statementId": "rel-1", "recordType": "relationship",
             "recordDetails": {"interestedParty": "per-1", "subject": "ent-1",
                               "interests": [{"type": "shareholding",
                                              "details": "ownership of shares",
                                              "share": {"exclusiveMinimum": 75, "maximum": 100},
                                              "startDate": "2016-04-06"}]},
             "source": {"description": "UK Companies House"}},
        ],
    }


def test_html_has_all_sections_and_accessibility_basics():
    html = build_report_html(_report())
    assert html.startswith("<!DOCTYPE html>")
    assert 'lang="en"' in html
    assert "<title>OpenCheck due-diligence report — Northwind Logistics Ltd</title>" in html
    # All seven elements present.
    assert "<h1>Northwind Logistics Ltd</h1>" in html       # 1 title
    assert "Identifiers" in html and "2138000000000000A001" in html  # 2 ids
    # 6 — live-check URL
    assert "Run a live check" in html
    assert "opencheck.world/?lei=2138000000000000A001" in html
    assert "Risk signals" in html                            # 4 risk
    assert "What each source found" in html                  # 4 sources
    assert "Ownership &amp; control structure" in html       # 5 diagrams
    assert "Licensing &amp; attribution" in html             # 7 licensing
    # Table semantics for screen readers.
    assert 'th scope="row"' in html and 'th scope="col"' in html


def test_summary_included_only_when_narrative_present():
    html_without = build_report_html(_report())
    assert 'id="sum"' not in html_without
    narrative = {
        "summary": "Northwind Logistics Ltd is an identity-confirmed entity.",
        "overall_confidence": "high",
        "model": "claude-sonnet-4-6",
        "prompt_version": "2026-06-14-v3",
        "packet": {"facts": [{"source_name": "UK Companies House"}]},
    }
    html_with = build_report_html(_report(), narrative=narrative)
    assert 'id="sum"' in html_with
    assert "identity-confirmed entity" in html_with
    assert "high confidence" in html_with
    assert "UK Companies House" in html_with  # evidence chip


def test_no_risk_signals_states_checks_applied():
    html = build_report_html(_report())
    assert "No risk signals were raised" in html
    # The "returned clear" list must describe the checks the engine actually
    # runs as risk checks: non-EU/EEA became a context observation in
    # Phase 111 and the EU high-risk third-country list joined the screen.
    assert "non-EU/EEA" not in html
    assert "EU high-risk third-country" in html


def test_signal_labels_come_from_curated_map():
    report = _report()
    report["risk_signals"] = [
        {"code": "OPAQUE_OWNERSHIP", "confidence": "high",
         "summary": "Withheld.", "source_id": ""},
        {"code": "SOME_FUTURE_CODE", "confidence": "low",
         "summary": "Unknown.", "source_id": ""},
    ]
    html = build_report_html(report)
    assert "Opaque ownership" in html          # curated label, not "Opaque Ownership"
    assert "Opaque Ownership" not in html
    assert "Some Future Code" in html          # unknown codes fall back to title case


def test_context_signal_never_renders_under_risk_heading():
    report = _report()
    report["risk_signals"] = [
        {"code": "SANCTIONED", "confidence": "high",
         "summary": "Listed on a sanctions list.", "source_id": ""},
        {"code": "NON_EU_JURISDICTION", "confidence": "low", "kind": "context",
         "summary": "Ownership chain reaches outside the EU/EEA.", "source_id": ""},
    ]
    html = build_report_html(report)
    assert "Structural context" in html
    assert "Not risk findings" in html
    # The context signal is marked with the context class; the risk one is not.
    assert 'class="signal low context"' in html
    assert 'class="signal high context"' not in html
    # Ordering: the risk finding renders before the Structural context
    # heading, the context observation after it.
    ctx_at = html.index("Structural context")
    assert html.index("Sanctioned") < ctx_at
    assert html.index("Non-EU jurisdiction") > ctx_at


def test_context_only_report_still_reads_as_no_risk_signals():
    report = _report()
    report["risk_signals"] = [
        {"code": "GLEIF_REPORTING_EXCEPTION", "confidence": "high", "kind": "context",
         "summary": "No accounting-consolidation parent is reported to GLEIF.",
         "source_id": ""},
    ]
    html = build_report_html(report)
    assert "No risk signals were raised" in html
    assert "Structural context" in html
    assert "No parent in GLEIF (exempt)" in html


def test_diagram_and_text_equivalent_present():
    html = build_report_html(_report())
    assert "<figure>" in html and "<svg" in html
    assert "Jane Eleanor Smith" in html
    assert "Relationships in Figure 1" in html  # text-equivalent table caption


def test_stub_hits_excluded_from_sources():
    html = build_report_html(_report())
    # The stubbed kvk hit must not appear.
    assert "stub" not in html


def test_captioned_table_split_across_page_does_not_crash():
    """Regression: a captioned table that splits right after its caption used to
    raise WeasyPrint's "Table wrapper without a table". `caption{break-after:avoid}`
    in the report CSS prevents the orphaned-caption split."""
    pytest.importorskip("weasyprint")
    from weasyprint import HTML

    from opencheck.reporting.html_report import _CSS

    rows = "".join(
        f"<tr><td>Person {i}</td><td>Interest {i}</td><td>Subject</td></tr>" for i in range(8)
    )
    # The tall spacer pushes the captioned table to the bottom of page 1 so it
    # must split immediately after the caption.
    html = (
        f'<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>t</title>'
        f"<style>{_CSS}</style></head><body>"
        '<div style="height:235mm">tall</div>'
        "<table><caption>caption</caption>"
        '<thead><tr><th scope="col">P</th><th scope="col">I</th><th scope="col">S</th></tr></thead>'
        f"<tbody>{rows}</tbody></table></body></html>"
    )
    pdf = HTML(string=html).write_pdf(pdf_variant="pdf/ua-1")
    assert pdf[:4] == b"%PDF"


def test_weasyprint_render_is_tagged():
    pytest.importorskip("weasyprint")
    pikepdf = pytest.importorskip("pikepdf")
    from opencheck.reporting import build_report_pdf

    pdf_bytes = build_report_pdf(_report())
    assert pdf_bytes[:4] == b"%PDF"
    import io

    with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
        assert str(pdf.Root.get("/Lang")) == "en"
        assert "/StructTreeRoot" in pdf.Root
        mark = pdf.Root.get("/MarkInfo")
        assert mark is not None and bool(mark.get("/Marked"))
