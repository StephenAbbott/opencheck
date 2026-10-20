"""Phase 339 — integrated ownership along the ownership path, per source.

The case table ``frontend/src/lib/integratedOwnershipCases.json`` is the
contract: this file and ``integratedOwnership.test.ts`` run every case and
must both give ``expect``. Then the parts only the backend has — the frozen
lookup event, the BODS annotation, the report tables, the MCP block — and the
rule that matters most: the figure is context, never a signal, so no risk
module may import it.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from opencheck import integrated_ownership as io
from opencheck.ownership_math import Edge, figure_text, integrated_ownership

_BACKEND = Path(__file__).resolve().parents[1]
_CASES = _BACKEND.parent / "frontend" / "src" / "lib" / "integratedOwnershipCases.json"

pytestmark = pytest.mark.skipif(
    not _CASES.is_file(), reason="frontend sources not present (backend-only checkout)"
)


def _project(payload: dict) -> list[dict]:
    return [
        {
            "party": h["party"],
            "status": h["status"],
            "kind": h["kind"],
            "figures": [
                {"source_id": f["source_id"], "text": f["text"], "route": io.route_words(f)}
                for f in h["figures"]
            ],
        }
        for h in payload["holders"]
    ]


def _cases() -> list[dict]:
    return json.loads(_CASES.read_text(encoding="utf-8"))["cases"]


@pytest.mark.parametrize("case", _cases(), ids=lambda c: c["name"])
def test_the_shared_case_table(case: dict) -> None:
    got = io.compute(case["statements"], case["subject_ids"], case["as_of"], case.get("canon"))
    assert _project(got) == case["expect"]


def test_the_case_table_covers_the_ticket_rules() -> None:
    names = " | ".join(c["name"] for c in _cases())
    for rule in ("voting rights", "ended", "roles", "GLEIF and MEIP", "never added",
                 "rounding", "at least", "band"):
        assert rule in names, rule


# --- the arithmetic -----------------------------------------------------------


def test_a_share_returning_to_a_single_owner_is_given_back():
    # T-Rank example 3.4 seen from C, which sits on the cycle T → B → C → T:
    # T holds 4% of itself, so C's 50% is 50 / 96 of the shares held outside.
    edges = [
        Edge("X", "A", 0.5, 0.5), Edge("A", "B", 0.8, 0.8), Edge("B", "C", 0.8, 0.8),
        Edge("C", "T", 0.5, 0.5), Edge("T", "B", 0.1, 0.1),
    ]
    assert figure_text(integrated_ownership(edges, {"C"}, "T").as_dict()) == "52.08%"
    assert figure_text(integrated_ownership(edges, {"B"}, "T").as_dict()) == "41.67%"
    # and every row of the table keeps its ratio: A holds 80% of B.
    assert figure_text(integrated_ownership(edges, {"A"}, "T").as_dict()) == "33.33%"


def test_the_payload_is_json_and_carries_the_method_and_credit():
    case = _cases()[0]
    payload = io.compute(case["statements"], case["subject_ids"], case["as_of"])
    json.dumps(payload)
    assert payload["method"] == "integrated_ownership"
    assert payload["as_of"] == case["as_of"]
    assert "T-Rank" in payload["credit"]
    assert "never added" in payload["description"]


def test_a_holder_line_names_every_source_and_the_route():
    case = next(c for c in _cases() if c["name"] == "two sources are listed, never added")
    payload = io.compute(case["statements"], case["subject_ids"], case["as_of"], case.get("canon"))
    (h,) = payload["holders"]
    assert io.holder_line(h) == (
        "Ministry of Finance — 67% (OpenSanctions, directly); 50.1% (Wikidata, directly)"
    )


# --- the lookup event and the frozen copy -----------------------------------


_OPENCHECK = {"publisher": {"name": "OpenCheck"}}


def _ours(stmt: dict, source_id: str | None = None) -> dict:
    """*stmt* as OpenCheck publishes it (the shared case table carries no
    ``publicationDetails``; the frontend parity tests read it as it is)."""
    out = {**stmt, "publicationDetails": _OPENCHECK}
    if source_id:
        out["source"] = {"description": source_id.upper(), "opencheckSourceId": source_id}
    return out


def _lookup_events(payload: dict) -> list[tuple[str, dict]]:
    case = next(c for c in _cases() if c["name"] == "a direct and an indirect stake add")
    return [
        ("gleif_done",
         {"legal_name": "Target Co", "jurisdiction": "GB", "derived_identifiers": {}}),
        ("deepen_result",
         {"source_id": "opensanctions", "bods": [_ours(s) for s in case["statements"]]}),
        ("integrated_ownership", payload),
        ("done", {"bods_issues": [], "license_notices": []}),
    ]


def _frozen() -> dict:
    case = next(c for c in _cases() if c["name"] == "a direct and an indirect stake add")
    payload = io.compute(case["statements"], ["T"], "2026-01-02")
    payload["subject_ids"] = ["T"]
    return payload


def test_the_fold_keeps_the_frozen_payload_and_annotates_the_subject():
    from opencheck.lookup_replay import fold_lookup_events

    frozen = _frozen()
    resp = fold_lookup_events("213800LH1BZH3DI6G760", _lookup_events(frozen))
    assert resp.integrated_ownership == frozen
    (subject,) = [s for s in resp.bods if s.get("statementId") == "T"]
    (ann,) = [a for a in subject.get("annotations") or [] if a["motivation"] == "commenting"]
    assert ann["statementPointerTarget"] == "/recordDetails"
    assert ann["url"] == io.METHOD_URL
    assert ann["creationDate"] == "2026-01-02"
    assert "State — 34% (OpenSanctions, directly and through Holding A)" in ann["description"]
    assert "T-Rank" in ann["description"]


def test_the_annotation_never_targets_the_statement_root():
    """libcove's ``annotation_statement_pointer_target_invalid`` rejects
    ``"/"``; a statement-wide annotation targets ``/recordDetails`` (the
    Phase 255 rule). Found by the BODS quality sweep of 19 Oct 2026."""
    payload = _frozen()
    out = io.apply_to_bods(
        [_ours({"statementId": "T", "recordType": "entity", "recordDetails": {}})], payload
    )
    pointers = [a["statementPointerTarget"] for a in out[0]["annotations"]]
    assert pointers and "/" not in pointers
    for pointer in pointers:
        node = out[0]
        for part in pointer.lstrip("/").split("/"):
            assert part in node, f"{pointer} does not resolve in the statement"
            node = node[part]


def _subject(statement_id: str, *, publisher: str, source_id: str, description: str) -> dict:
    return {
        "statementId": statement_id,
        "recordType": "entity",
        "recordDetails": {"name": "Target Co"},
        "publicationDetails": {"publisher": {"name": publisher}},
        "source": {"description": description, "opencheckSourceId": source_id},
    }


def _commented(out: list[dict]) -> list[str]:
    return [
        s["statementId"] for s in out
        if any(a.get("motivation") == "commenting" for a in s.get("annotations") or [])
    ]


def test_the_annotation_goes_on_opencheck_s_own_gleif_statement():
    """Stephen, 20 Oct 2026 (option c): never on Open Ownership's verbatim
    GLEIF statement, which is described "GLEIF" too, and not on another
    OpenCheck statement when OpenCheck's GLEIF one is there."""
    payload = {**_frozen(), "subject_ids": ["OO", "CH", "GL"]}
    bods = [
        {**_subject("OO", publisher="Open Ownership", source_id="", description="GLEIF"),
         "source": {"description": "GLEIF"}},
        _subject("CH", publisher="OpenCheck", source_id="companies_house",
                 description="UK Companies House"),
        _subject("GL", publisher="OpenCheck", source_id="gleif", description="GLEIF"),
    ]
    out = io.apply_to_bods(bods, payload)
    assert _commented(out) == ["GL"]
    assert out[0] is bods[0]  # Open Ownership's statement is untouched


def test_without_a_gleif_statement_the_note_goes_on_opencheck_s_register_record():
    """A register-anchored run (Phase 290) has no GLEIF anchor."""
    payload = {**_frozen(), "subject_ids": ["CH"]}
    bods = [_subject("CH", publisher="OpenCheck", source_id="companies_house",
                     description="UK Companies House")]
    assert _commented(io.apply_to_bods(bods, payload)) == ["CH"]


def test_another_publisher_s_statement_is_never_annotated():
    payload = {**_frozen(), "subject_ids": ["OO"]}
    bods = [{**_subject("OO", publisher="Open Ownership", source_id="", description="GLEIF"),
             "source": {"description": "GLEIF"}}]
    assert io.apply_to_bods(bods, payload) == bods


def test_no_holder_means_no_annotation():
    assert io.apply_to_bods([{"statementId": "T", "recordType": "entity"}], None) == [
        {"statementId": "T", "recordType": "entity"}
    ]
    empty = {"holders": [], "subject_ids": ["T"]}
    assert io.apply_to_bods([{"statementId": "T", "recordType": "entity"}], empty)[0].get(
        "annotations"
    ) is None


def test_the_annotation_never_mutates_the_saved_statements():
    bods = [{"statementId": "T", "recordType": "entity"}]
    io.apply_to_bods(bods, _frozen())
    assert "annotations" not in bods[0]


def test_the_lookup_pipeline_yields_the_event(monkeypatch, tmp_path):
    """The offline-bundle lookup carries the field, with today's ``as_of``."""
    from fastapi.testclient import TestClient

    from opencheck.app import app
    from opencheck.bods.lifecycle import today_iso
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    lei = "213800LH1BZH3DI6G760"
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({
        "statementId": "e-subject", "recordType": "entity",
        "recordDetails": {"name": "Bundle Co", "identifiers": [{"id": lei, "scheme": "XI-LEI"}]},
    }) + "\n")
    try:
        body = TestClient(app).get("/lookup", params={"lei": lei}).json()
    finally:
        get_settings.cache_clear()
    io_payload = body["integrated_ownership"]
    assert io_payload["method"] == "integrated_ownership"
    assert io_payload["as_of"] == today_iso()
    assert io_payload["holders"] == []


# --- reports and MCP ----------------------------------------------------------


def _report() -> dict:
    return {"lei": "213800LH1BZH3DI6G760", "legal_name": "Target Co", "bods": [],
            "integrated_ownership": _frozen()}


def test_the_html_report_prints_the_frozen_table():
    from opencheck.reporting.html_report import build_report_html

    html = build_report_html(_report())
    assert '<h2 id="iown">Integrated ownership</h2>' in html
    assert (
        "<td>State</td><td>34%</td><td>OpenSanctions</td>"
        "<td>directly and through Holding A</td>"
    ) in html
    assert "2026-01-02" in html and "T-Rank" in html


def test_the_markdown_report_prints_the_frozen_table():
    from opencheck.reporting.markdown_report import build_report_markdown

    md = build_report_markdown(_report())
    assert "## Integrated ownership" in md
    assert "| State | 34% | OpenSanctions | directly and through Holding A |" in md


def test_reports_without_the_payload_have_no_section():
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    report = {"lei": "213800LH1BZH3DI6G760", "legal_name": "Target Co", "bods": []}
    assert 'id="iown"' not in build_report_html(report)
    assert "## Integrated ownership" not in build_report_markdown(report)


def test_the_mcp_block_is_context_and_lists_each_figure():
    from types import SimpleNamespace

    from opencheck.mcp.shaping import _integrated_ownership

    block = _integrated_ownership(SimpleNamespace(integrated_ownership=_frozen()))
    assert block is not None
    assert block["method"] == "Integrated ownership"
    assert "not a risk finding" in block["hint"]
    state = next(h for h in block["holders"] if h["name"] == "State")
    assert state["figures"] == [{
        "source": "OpenSanctions", "text": "34%", "minimum": 34.0, "maximum": 34.0,
        "incomplete": False, "route": "directly and through Holding A",
    }]
    assert _integrated_ownership(SimpleNamespace(integrated_ownership=None)) is None


# --- never a signal -----------------------------------------------------------


_RISK_SIDE = sorted(
    [_BACKEND / "opencheck" / m for m in ("risk.py", "verdict.py", "signalstats.py")]
    + list((_BACKEND / "opencheck" / "narrative").glob("*.py"))
)


@pytest.mark.parametrize("path", _RISK_SIDE, ids=lambda p: p.name)
def test_no_risk_module_reads_the_figure(path: Path) -> None:
    """The risk engine, the verdict, ``/signalstats`` and the narrative never
    import the module, so the figure cannot become a signal, a verdict word or
    a sentence of the summary. (Phase 338's state figure calls the shared
    arithmetic in ``ownership_math`` — that is the math, not this payload.)"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names = [node.module or ""] + [a.name for a in node.names]
        elif isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        else:
            continue
        assert not any(n.split(".")[-1] == "integrated_ownership" for n in names), path.name
