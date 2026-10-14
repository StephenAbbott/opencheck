"""Phase 334 — institutional shareholders from SEC Form 13F, read.

Every index here is built by the **real builder** (``scripts/build_form13f_index.py``)
from zips in the SEC's exact shape (the helpers of ``test_build_form13f_index``),
and every ISIN table by the real ``isin_index.build_index`` — so the reader is
tested against what the builder writes, not against what it expects.

The rule these tests exist for (Stephen, 14 Oct 2026): **a 13F filer is an
investment manager with discretion, not a beneficial owner.** The payload
never becomes BODS, never reaches a risk signal, the verdict, the related-party
screens or the AI narrative, is never totalled, and says so in its own words.
"""

from __future__ import annotations

import ast
import datetime as dt
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from opencheck import form13f, isin_index
from opencheck.config import get_settings
from tests.test_build_form13f_index import MOODYS, DataSetBuilder, _bystanders, builder

MOODYS_LEI = "549300GCEDD8YCF5WU84"
MOODYS_CIK = "1059556"
TODAY = date(2026, 10, 14)
PACKAGE = Path(form13f.__file__).resolve().parent


# ---------------------------------------------------------------------------
# Fixtures: an ISIN table and a 13F index, both built by their real builders
# ---------------------------------------------------------------------------


def _isin(cusip: str) -> str:
    """A US ISIN for a CUSIP, with its check digit."""
    body = "US" + cusip
    digits = "".join(str(int(c, 36)) for c in body)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch) * (2 if i % 2 == 0 else 1)
        total += d // 10 + d % 10
    return body + str((10 - total % 10) % 10)


def test_the_isin_helper_matches_moodys_published_isin() -> None:
    assert _isin(MOODYS) == "US6153691059"


@pytest.fixture
def isin_table(tmp_path, monkeypatch):
    """Build an ISIN table from ``rows`` [(lei, isin)] and point the reader at it."""

    def _build(rows: list[tuple[str, str]]) -> Path:
        csv_path = tmp_path / "isin.csv"
        csv_path.write_text("LEI,ISIN\n" + "".join(f"{lei},{isin}\n" for lei, isin in rows))
        out = tmp_path / "isin.sqlite"
        isin_index.build_index(
            csv_path, out, file_name="isin-lei-test.zip",
            uploaded_at=dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        monkeypatch.setenv("OPENCHECK_ISIN_INDEX_DB_FILE", str(out))
        get_settings.cache_clear()
        isin_index.reset_connection()
        return out

    yield _build
    isin_index.reset_connection()
    get_settings.cache_clear()


@pytest.fixture
def index(tmp_path, monkeypatch):
    """Build a 13F index from a DataSetBuilder (+ frames) and point the reader at it."""

    def _build(ds: DataSetBuilder, frames: dict[str, list[dict[str, Any]]] | None = None) -> Path:
        zip_path = ds.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
        out = tmp_path / "form13f.sqlite"
        builder.build_index(
            [zip_path], out, min_filings=5, frames=frames or {},
            data_sets=[builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))],
        )
        monkeypatch.setenv("OPENCHECK_FORM13F_DB_FILE", str(out))
        get_settings.cache_clear()
        form13f.reset_connection()
        return out

    yield _build
    form13f.reset_connection()
    get_settings.cache_clear()


MOODYS_FRAME = {"CY2026Q2I": [{
    "cik": 1059556, "end": "2026-06-30", "val": 173_200_000,
    "accn": "0001628280-26-049398", "entityName": "Moody’s Corporation",
}]}


def _moodys() -> DataSetBuilder:
    ds = DataSetBuilder()
    ds.filing("0000950123-26-008611", "1067983", "Berkshire Hathaway Inc", included=14)
    for part in (20_000_000, 4_000_000, 669_778):
        ds.holding("0000950123-26-008611", part, discretion="DFND")
    ds.filing("0001647251-26-000004", "1647251", "TCI FUND MANAGEMENT LTD")
    ds.holding("0001647251-26-000004", 14_334_027)
    ds.filing("0000080255-26-000017", "80255", "PRICE T ROWE ASSOCIATES INC /MD/")
    ds.holding("0000080255-26-000017", 1_545_468, 699_974)
    _bystanders(ds, 30)
    return ds


def _payload(**kw) -> dict[str, Any] | None:
    kw.setdefault("issuer_cik", MOODYS_CIK)
    kw.setdefault("today", TODAY)
    return form13f.build_payload(MOODYS_LEI, kw.pop("bods", []), **kw)


# ---------------------------------------------------------------------------
# Reading Moody's
# ---------------------------------------------------------------------------


def test_moodys_through_its_gleif_isin(index, isin_table) -> None:
    index(_moodys(), MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS)), (MOODYS_LEI, "US615369AB12")])
    p = _payload()
    assert p["status"] == "found" and p["period"] == "2026-06-30"
    (sec,) = p["securities"]
    assert sec["cusip"] == MOODYS and sec["matched_via"] == ["gleif_isin"]
    first = sec["positions"][0]
    assert (first["manager"], first["shares"], first["percent"]) == (
        "Berkshire Hathaway Inc", 24_669_778, 14.24,
    )
    assert sec["shares_outstanding"]["shares"] == 173_200_000
    assert sec["shares_outstanding"]["basis"] == (
        "Of 173.2 million shares outstanding on Moody’s Corporation's 10-Q/10-K cover, "
        "30 June 2026 — computed by OpenCheck."
    )
    assert "Berkshire Hathaway Inc with 24,669,778 shares, 14.24%" in p["line"]
    assert p["attribution"] == form13f.ATTRIBUTION and p["caveat"] == form13f.CAVEAT


def test_twenty_shown_the_rest_counted_never_totalled(index, isin_table) -> None:
    index(_moodys(), MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    (sec,) = _payload()["securities"]
    assert len(sec["positions"]) == form13f.TOP == 20
    assert sec["managers_reported"] == 33
    assert sec["managers_more"] == 13
    assert not any("total" in k for k in sec)


def test_a_value_in_thousands_shows_its_shares_and_not_its_value(index, isin_table) -> None:
    index(_moodys(), MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    (sec,) = _payload()["securities"]
    rowe = next(p for p in sec["positions"] if p["manager"].startswith("PRICE T ROWE"))
    assert rowe["shares"] == 1_545_468
    assert rowe["value"] is None and rowe["value_note"] == form13f.THOUSANDS_NOTE
    assert ("PRICE T ROWE ASSOCIATES INC /MD/", "1,545,468", "not shown (reported in thousands)",
            "0.89%") in form13f.report_rows(sec)


def test_a_cusip_from_the_companys_own_13g_filings(index, isin_table) -> None:
    """Chevron's case: GLEIF holds no ISIN for it; SEC EDGAR's statement
    carries the CUSIP read off the filings."""
    index(_moodys(), MOODYS_FRAME)
    isin_table([("5493000C01ZX7D35SD85", "US0846707026")])
    edgar = {
        "statementId": "x", "recordType": "entity",
        "source": {"opencheckSourceId": "sec_edgar", "description": "SEC EDGAR"},
        "recordDetails": {"identifiers": [
            {"id": MOODYS_CIK, "scheme": "US-SEC-CIK"}, {"id": MOODYS, "scheme": "CUSIP"},
        ]},
    }
    p = _payload(bods=[edgar])
    assert p["securities"][0]["matched_via"] == ["sec_edgar"]
    other_source = {**edgar, "source": {"opencheckSourceId": "opensanctions"}}
    assert _payload(bods=[other_source]) is None


# ---------------------------------------------------------------------------
# The percentage's gates
# ---------------------------------------------------------------------------


def test_no_percentage_without_the_issuer_or_outside_the_window(index, isin_table) -> None:
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    index(_moodys(), MOODYS_FRAME)
    no_cik = _payload(issuer_cik=None)["securities"][0]
    assert no_cik["shares_outstanding"] is None
    assert all(p["percent"] is None for p in no_cik["positions"])
    far = {"CY2026Q2I": [{**MOODYS_FRAME["CY2026Q2I"][0], "end": "2025-12-31"}]}
    index(_moodys(), far)
    assert _payload()["securities"][0]["shares_outstanding"] is None


def test_two_classes_get_no_percentage(index, isin_table) -> None:
    ds = _moodys()
    for i in range(12):
        acc = f"0000{5000 + i}-26-000001"
        ds.filing(acc, str(5000 + i), f"Class B Holder {i}")
        ds.holding(acc, 50 + i, cusip="615369204", title="CL B")
    index(ds, MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS)), (MOODYS_LEI, _isin("615369204"))])
    p = _payload()
    assert len(p["securities"]) == 2
    assert all(s["shares_outstanding"] is None for s in p["securities"])
    assert "1 more of the company's securities is listed" in p["line"]


def test_an_adr_is_labelled_and_never_gets_a_percentage(index, isin_table) -> None:
    ds = DataSetBuilder()
    for i in range(12):
        acc = f"0000{6000 + i}-26-000001"
        ds.filing(acc, str(6000 + i), f"ADR Holder {i}")
        ds.holding(acc, 1000 + i, round((1000 + i) * 47.94), cusip="670100205",
                   issuer="NOVO-NORDISK A S", title="ADR")
    index(ds, {"CY2026Q2I": [{"cik": 353278, "end": "2026-06-30", "val": 4_000_000_000}]})
    isin_table([("549300DAQ1CVT6CXN342", _isin("670100205"))])
    p = form13f.build_payload("549300DAQ1CVT6CXN342", [], issuer_cik="353278", today=TODAY)
    (sec,) = p["securities"]
    assert sec["is_adr"] and sec["adr_note"] == form13f.ADR_NOTE
    assert sec["shares_outstanding"] is None
    assert "American depositary shares" in form13f.security_heading(sec)


def test_a_position_larger_than_the_shares_outstanding_is_left_out(index, isin_table) -> None:
    ds = _moodys()
    ds.filing("0000777777-26-000001", "777777", "Impossible Adviser LLC")
    ds.holding("0000777777-26-000001", 200_000_000)
    index(ds, MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    (sec,) = _payload()["securities"]
    assert "Impossible Adviser LLC" not in [p["manager"] for p in sec["positions"]]
    assert sec["left_out"] >= 1


def test_debt_and_thinly_reported_securities_are_counted_not_listed(index, isin_table) -> None:
    ds = _moodys()
    ds.filing("0000888888-26-000001", "888888", "Bond Writer LLC")
    ds.holding("0000888888-26-000001", 5000, 4990, cusip="615369AB1", title="CORPORATE BONDS")
    ds.filing("0000888889-26-000001", "888889", "Preferred Writer LLC")
    ds.holding("0000888889-26-000001", 700, 14000, cusip="615369303", title="PFD")
    index(ds, MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(c)) for c in (MOODYS, "615369AB1", "615369303")])
    p = _payload()
    assert [s["cusip"] for s in p["securities"]] == [MOODYS]
    assert p["securities_not_listed"] == 2
    assert p["not_listed_note"].startswith("2 more of the company's securities appear")
    assert form13f.is_equity_cusip("615369105") and not form13f.is_equity_cusip("615369AB1")


# ---------------------------------------------------------------------------
# Absence and age
# ---------------------------------------------------------------------------


def test_no_index_or_no_match_is_no_payload(index, isin_table) -> None:
    assert _payload() is None  # conftest points at a file that does not exist
    index(_moodys(), MOODYS_FRAME)
    isin_table([("5493000C01ZX7D35SD85", "US0846707026")])
    assert _payload() is None
    assert form13f.build_payload("GB-COH:00102498", [], today=TODAY) is None


def test_an_old_index_says_so_and_lists_nothing(index, isin_table) -> None:
    index(_moodys(), MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    p = _payload(today=date(2027, 3, 1))
    assert p["status"] == "out_of_date" and p["securities"] == []
    assert "too old to show" in p["line"]


def test_an_unknown_schema_is_not_read(index, isin_table, tmp_path) -> None:
    path = index(_moodys(), MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(MOODYS))])
    conn = sqlite3.connect(path)
    conn.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    form13f.reset_connection()
    assert _payload() is None
    assert form13f.status()["available"] is False


def test_isins_with_prefix_reads_only_the_run_it_needs(isin_table, monkeypatch) -> None:
    monkeypatch.setattr(isin_index, "CHUNK", 3)
    lei = "5493001KJTIIGC8Y1R12"
    rows = [(lei, f"{cc}{n:09d}{d}") for cc in ("CA", "GB", "US", "XS") for n in range(4) for d in "0"]
    isin_table(rows)
    got = isin_index.isins_with_prefix(lei, "US")
    assert got == [f"US{n:09d}0" for n in range(4)]
    assert isin_index.isins_with_prefix(lei, "US", limit=2) == got[:2]
    assert isin_index.isins_with_prefix(lei, "JP") == []
    assert isin_index.isins_with_prefix("XXXXXXXXXXXXXXXXXXXX", "US") == []


# ---------------------------------------------------------------------------
# Never ownership — the structural rules
# ---------------------------------------------------------------------------

#: Modules that decide or describe risk, ownership or the verdict. None may
#: import form13f: the payload is shown beside the report, never read by it.
NEVER_IMPORT = (
    "risk.py", "cross_check.py", "icij_check.py", "openaleph_check.py", "verdict.py",
    "pep_merge.py", "related_targets.py", "degradation.py", "coverage.py",
    "subject_profile.py", "subject_identity.py", "reconcile.py", "watchlist.py",
)


def _imports_form13f(path: Path) -> bool:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").endswith("form13f") or any(a.name == "form13f" for a in node.names):
                return True
        elif isinstance(node, ast.Import) and any(a.name.endswith("form13f") for a in node.names):
            return True
    return False


@pytest.mark.parametrize("name", NEVER_IMPORT)
def test_no_risk_or_verdict_module_reads_form13f(name: str) -> None:
    assert not _imports_form13f(PACKAGE / name), f"{name} imports form13f"


def test_no_bods_narrative_or_source_module_reads_form13f() -> None:
    for folder in ("bods", "narrative", "sources", "timeline"):
        for path in (PACKAGE / folder).rglob("*.py"):
            assert not _imports_form13f(path), f"{path.relative_to(PACKAGE)} imports form13f"


def _all_payloads(index, isin_table) -> list[dict[str, Any]]:
    ds = _moodys()
    for i in range(12):
        acc = f"0000{6100 + i}-26-000001"
        ds.filing(acc, str(6100 + i), f"Depositary Holder {i}")
        ds.holding(acc, 900 + i, round((900 + i) * 30.0), cusip="615369402", title="SPON ADS")
    ds.filing("0000888888-26-000001", "888888", "Bond Writer LLC")
    ds.holding("0000888888-26-000001", 5000, 4990, cusip="615369AB1")
    index(ds, MOODYS_FRAME)
    isin_table([(MOODYS_LEI, _isin(c)) for c in (MOODYS, "615369402", "615369AB1")])
    return [_payload(), _payload(today=date(2027, 6, 1)), _payload(issuer_cik=None)]


def test_no_sentence_reads_a_position_as_ownership(index, isin_table) -> None:
    payloads = _all_payloads(index, isin_table)
    found = [s for p in payloads for s in form13f.sentences(p)]
    assert len(found) > 10
    for sentence in found:
        low = sentence.lower()
        for word in form13f.BANNED_VOCABULARY:
            assert word not in low, f"{word!r} in: {sentence}"
    # The one sentence allowed to name what 13F is not, pinned verbatim.
    assert "not a record of beneficial ownership" in form13f.DISCLAIMER


def _fold(payload: dict[str, Any] | None):
    from opencheck.routers.lookup import fold_lookup_events

    subject = {
        "statementId": "e-subject", "recordId": "e-subject", "recordType": "entity",
        "recordDetails": {"name": "MOODY'S CORPORATION", "identifiers": [{"id": MOODYS_LEI, "scheme": "XI-LEI"}]},
    }
    events: list[tuple[str, dict]] = [
        ("gleif_done", {"lei": MOODYS_LEI, "legal_name": "MOODY'S CORPORATION",
                        "jurisdiction": "US-DE", "derived_identifiers": {}}),
        ("deepen_result", {"bods": [subject]}),
    ]
    if payload is not None:
        events.append(("form13f", payload))
    events.append(("done", {"bods_issues": [], "license_notices": []}))
    return fold_lookup_events(MOODYS_LEI, events)


def test_the_event_is_folded_as_recorded_and_touches_no_bods(index, isin_table) -> None:
    payload = _all_payloads(index, isin_table)[0]
    with_it, without = _fold(payload), _fold(None)
    assert with_it.form13f == payload and without.form13f is None
    assert with_it.bods == without.bods
    assert with_it.risk_signals == without.risk_signals == []


def test_mcp_carries_it_apart_and_the_summary_never_names_a_manager(index, isin_table) -> None:
    from opencheck.mcp.shaping import shape_lookup

    shaped = shape_lookup(_fold(_all_payloads(index, isin_table)[0]))
    field = shaped["institutional_shareholders"]
    assert field["status"] == "found"
    first = field["securities"][0]
    assert first["positions"][0]["manager"] == "Berkshire Hathaway Inc"
    assert first["positions"][0]["percent_computed"] == 14.24
    assert len(first["positions"]) == 10
    assert "Berkshire" not in shaped["summary"] and "13F" not in shaped["summary"]
    assert "Berkshire" not in str(shaped["risk_signals"])
    assert shape_lookup(_fold(None))["institutional_shareholders"] is None


def test_reports_carry_the_section_and_drop_it_when_absent(index, isin_table) -> None:
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    report = _fold(_all_payloads(index, isin_table)[0]).model_dump()
    md = build_report_markdown(report)
    assert "## Institutional shareholders (Form 13F)" in md
    assert "| Berkshire Hathaway Inc | 24,669,778 |" in md
    assert form13f.DISCLAIMER in md and form13f.ATTRIBUTION in md
    html = build_report_html(report)
    assert "Institutional shareholders (Form 13F)" in html and "14.24%" in html
    bare = _fold(None).model_dump()
    assert "Form 13F" not in build_report_markdown(bare)
    assert "Form 13F" not in build_report_html(bare)


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------


def _pipeline_client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    import opencheck.sources.climatetrace as _ct
    from opencheck.app import app
    from tests.test_lookup_pipeline import _seed_bundle

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    return TestClient(app), lei


def _signals_and_bods(events: list[tuple[str, dict]]):
    risk = dict(events)["risk_signals"]
    bods = [b for n, p in events if n == "deepen_result" for b in p["bods"]]
    return risk["signals"], risk.get("verdict"), risk["degraded_sources"], bods


def test_pipeline_emits_the_event_and_nothing_else_reads_it(monkeypatch, tmp_path) -> None:
    """A payload naming a manager no screen should ever see changes neither
    the signals, the verdict, the degraded sources nor the statements."""
    from tests.test_lookup_pipeline import _stream_body, _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)
    seen: dict[str, Any] = {}
    payload = {
        "status": "found", "period": "2026-06-30", "line": "x.", "notes": [],
        "securities": [{"cusip": MOODYS, "positions": [
            {"manager": "BANK SADERAT PLC", "shares": 1, "percent": 99.0},
        ]}],
    }

    def _fake(got_lei, bods, *, issuer_cik=None, legal_name=None, today=None):
        seen.update(lei=got_lei, issuer_cik=issuer_cik)
        return payload

    try:
        baseline = _stream_events(_stream_body(client, lei))
        monkeypatch.setattr(form13f, "build_payload", _fake)
        with client.stream(
            "GET", "/lookup-stream", params={"lei": lei, "refresh": "true"}
        ) as r:
            events = _stream_events("".join(r.iter_text()))
        names = [n for n, _ in events]
        assert names.count("form13f") == 1
        assert names.index("form13f") < names.index("subject_profile") < names.index("risk_signals")
        assert dict(events)["form13f"] == payload and seen["lei"] == lei
        assert _signals_and_bods(events) == _signals_and_bods(baseline)
        assert "form13f" not in [n for n, _ in baseline]

        sync = client.get("/lookup", params={"lei": lei, "refresh": "true"}).json()
        assert sync["form13f"] == payload
        assert "BANK SADERAT" not in str(sync["risk_signals"]) + str(sync["bods"]) + str(sync["verdict"])
    finally:
        get_settings.cache_clear()


def test_a_cik_resolved_from_the_name_reaches_the_read(monkeypatch, tmp_path) -> None:
    """Phase 335: most US issuers get their CIK from the name fallback, not
    OpenCorporates. The read must be handed it, or the percentage never shows
    (Moody's in production, 14 Oct 2026). It stays out of the response's
    derived identifiers."""
    import json as _json

    from opencheck.sources import REGISTRY
    from tests.test_lookup_pipeline import _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    stmt = _json.loads(target.read_text())
    stmt["recordDetails"]["jurisdiction"] = {"name": "Delaware", "code": "US-DE"}
    stmt["recordDetails"]["identifiers"] = [{"id": lei, "scheme": "XI-LEI"}]
    target.write_text(_json.dumps(stmt) + "\n")

    adapter = REGISTRY["sec_edgar"]
    cls = type(adapter)
    live_info = adapter.info.model_copy(update={"live_available": True})
    monkeypatch.setattr(cls, "info", property(lambda self: live_info))

    async def _resolve(self, name):
        return "1059556"

    async def _fetch(self, hit_id):
        raise RuntimeError("not reached for this test")

    monkeypatch.setattr(cls, "resolve_cik", _resolve)
    monkeypatch.setattr(cls, "fetch", _fetch)
    seen: dict[str, Any] = {}

    def _fake(got_lei, bods, *, issuer_cik=None, legal_name=None, today=None):
        seen["issuer_cik"] = issuer_cik
        return None

    monkeypatch.setattr(form13f, "build_payload", _fake)
    try:
        with client.stream(
            "GET", "/lookup-stream", params={"lei": lei, "refresh": "true"}
        ) as r:
            events = _stream_events("".join(r.iter_text()))
        assert seen["issuer_cik"] == "1059556"
        assert "sec_edgar" in [p.get("source_id") for n, p in events if n == "hit"]
        sync = client.get("/lookup", params={"lei": lei}).json()
        assert "edgar_cik" not in (sync.get("derived_identifiers") or {})
    finally:
        get_settings.cache_clear()


def test_a_failing_read_never_fails_the_lookup(monkeypatch, tmp_path) -> None:
    from tests.test_lookup_pipeline import _stream_body, _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)

    def _boom(*a, **k):
        raise sqlite3.DatabaseError("disk image is malformed")

    monkeypatch.setattr(form13f, "build_payload", _boom)
    try:
        events = _stream_events(_stream_body(client, lei))
        names = [n for n, _ in events]
        assert "form13f" not in names and "done" in names
        assert not any("form13f" in str(d) for d in dict(events)["risk_signals"]["degraded_sources"])
    finally:
        get_settings.cache_clear()


def test_warm_index_without_a_url_reports_the_state() -> None:
    assert form13f.warm_index() == {
        "form13f": f"absent: {form13f.db_path()} (no URL)"
    }


def test_signalstats_says_whether_the_index_is_loaded(index, isin_table) -> None:
    from opencheck import signalstats

    assert signalstats.stats()["form13f"] == {"available": False}
    index(_moodys(), MOODYS_FRAME)
    got = signalstats.stats()["form13f"]
    assert got["available"] is True and got["period"] == "2026-06-30"
    assert not any("/" in str(v) for v in got.values() if v and "T" not in str(v))
