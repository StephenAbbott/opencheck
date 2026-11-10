"""Tests for the ESTMA payments source: index builder, adapter, mapper, hit
builder, finding and the committed crosswalk.

The index is built from a small CSV in the exact shape of Statistics
Canada's SDMX-CSV (``scripts.build_estma_index.HEADER``), with the traps the
real file has: a payment filed once by payee and once by project, numbered
sub-lines of one payment type, an entity filed in another's report with
blank values, and two reports for one year.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods import map_estma, validate_shape
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_estma
from opencheck.licensing import classify
from opencheck.routers.lookup import _bh_estma, _build_result_hit, _LookupCtx
from opencheck.sources import REGISTRY, estma
from scripts import build_estma_index as build

SUNCOR_LEI = "894500XO86FKXQGPYV71"
COSP_LEI = "549300COSP00000000X1"
STUB_LEI = "549300STUB00000000X2"
TWO_IDS_LEI = "549300TWO000000000X3"
RETIRED_LEI = "549300RETIRED00000X4"

CUT = "2026-11-05"


def _row(
    entity: str,
    year: str,
    *,
    kind: str = "PAYEE: Payee",
    payee: str = "PY000001: PROVINCE OF ALBERTA",
    country: str = "CA: Canada",
    ptype: str = "ROYALT: Royalties",
    value: str = "100.0",
    sub: str = "_Z: Not applicable",
    report_type: str = "CON: Consolidated Report",
    start: str | None = None,
    end: str | None = None,
    link: str = "https://example.com/suncor-estma",
    parent: str = "",
    unit: str = "CAD: Canadian Dollar",
) -> list[str]:
    return [
        "DATAFLOW", "STC:DF_ESTMA(1.0)", "I", "A: Annual", "NA: North America",
        country, entity, "CA: Canada", payee, sub, kind, ptype, year, value,
        start or f"{year}-01-01", end or f"{year}-12-31", report_type, "", unit,
        "0: Units", link, parent, "_N: See report for additional report notes.",
    ]


SUNCOR = "E405435: SUNCOR ENERGY INC."
COSP = "E984046: CANADIAN OIL SANDS PARTNERSHIP"

ROWS = [
    # Suncor 2025: two payees; Norway's fees filed as two numbered sub-lines.
    _row(SUNCOR, "2025", value="100.0"),
    _row(SUNCOR, "2025", ptype="TAXES: Taxes", value="50.0"),
    _row(SUNCOR, "2025", payee="PY000002: GOVERNMENT OF NORWAY", country="NO: Norway",
         ptype="FEES: Fees", value="6.0", sub="F_1: Fees_1"),
    _row(SUNCOR, "2025", payee="PY000002: GOVERNMENT OF NORWAY", country="NO: Norway",
         ptype="FEES: Fees", value="4.0", sub="F_2: Fees_2"),
    # The same payments again, by project. Never added to the payee view.
    _row(SUNCOR, "2025", kind="PROJECT: Project", payee="PJ000001: OIL SANDS", value="150.0"),
    _row(SUNCOR, "2025", kind="PROJECT: Project", payee="PJ000002: NORWAY",
         country="NO: Norway", ptype="FEES: Fees", value="10.0"),
    # Suncor 2024.
    _row(SUNCOR, "2024", value="80.0", link="https://example.com/suncor-2024"),
    # Canadian Oil Sands Partnership: filed in Suncor's report, blank values.
    _row(COSP, "2025", value="", parent=SUNCOR),
    _row(COSP, "2025", kind="PROJECT: Project", payee="PJ000001: OIL SANDS", value="", parent=SUNCOR),
    # A company with a full-year report and a stub for one year.
    _row("E111111: STUB MINING LTD.", "2025", value="20.0", report_type="IND: Individual Report",
         start="2024-04-01", end="2025-03-31", link="https://example.com/stub-full"),
    _row("E111111: STUB MINING LTD.", "2025", value="5.0", report_type="IND_ST: Individual Report (Stub)",
         start="2025-04-01", end="2025-12-31", link="https://example.com/stub-stub"),
    # One company, two ESTMA IDs (renamed).
    _row("E198001: MINTO EXPLORATIONS LTD.", "2021", value="7.0", link="https://example.com/minto-1"),
    _row("E964086: MINTO METALS CORP.", "2022", value="9.0", link="https://example.com/minto-2"),
    # A retired LEI in the crosswalk is not read.
    _row("E222222: RETIRED CO.", "2025", value="1.0", link="https://example.com/retired"),
    # Suffixed code, as filed.
    _row("E425485_1: FOSTERVILLE GOLD MINE PTY LTD. (1)", "2025", value="", parent=SUNCOR),
]

CROSSWALK = [
    ("E405435", "SUNCOR ENERGY INC.", "yes", SUNCOR_LEI, "accepted", "exact name, one live LEI"),
    ("E984046", "CANADIAN OIL SANDS PARTNERSHIP", "no (in parent report)", COSP_LEI, "accepted",
     "exact name, one live LEI"),
    ("E111111", "STUB MINING LTD.", "yes", STUB_LEI, "accepted", "exact name, one live LEI"),
    ("E198001", "MINTO EXPLORATIONS LTD.", "yes", TWO_IDS_LEI, "accepted", "checked by Stephen"),
    ("E964086", "MINTO METALS CORP.", "yes", TWO_IDS_LEI, "accepted", "checked by Stephen"),
    ("E222222", "RETIRED CO.", "yes", RETIRED_LEI, "retired LEI", "exact name, one live LEI"),
]


def _write_csv(path: Path, rows: list[list[str]], header: list[str] | None = None) -> Path:
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header or build.HEADER)
        w.writerows(rows)
    return path


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("ESTMA_DB_FILE", None)
    else:
        os.environ["ESTMA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    estma.reset_connection()


@pytest.fixture()
def crosswalk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "crosswalk.csv"
    fields = ["estma_id", "estma_name", "reports_payments", "lei", "decision", "method", "gleif_name", "note"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for eid, name, reports, lei, decision, method in CROSSWALK:
            w.writerow({
                "estma_id": eid, "estma_name": name, "reports_payments": reports, "lei": lei,
                "decision": decision, "method": method, "gleif_name": name.title(), "note": "",
            })
    monkeypatch.setattr(estma, "CROSSWALK_PATH", path)
    estma.reset_connection()
    yield path
    estma.reset_connection()


@pytest.fixture()
def index(tmp_path: Path, crosswalk: Path):
    out = tmp_path / "estma.sqlite"
    stats = build.build(_write_csv(tmp_path / "estma.csv", ROWS), out, CUT, min_rows=1, min_entities=1)
    _point_at(out)
    yield out, stats
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path, crosswalk: Path):
    _point_at(tmp_path / "absent.sqlite")
    yield
    _point_at(None)


def _ctx(lei: str, name: str = "") -> _LookupCtx:
    return _LookupCtx(
        lei=lei, legal_name=name, jurisdiction="CA", registered_as="",
        derived={}, ocid=None, spglobal=None, qid=None,
    )


async def _bundle(lei: str) -> dict[str, Any] | None:
    return await REGISTRY["estma"].fetch_by_lei(lei)


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_split_code_reads_sdmx_code_and_label():
    assert build.split_code("E405435: SUNCOR ENERGY INC.") == ("E405435", "SUNCOR ENERGY INC.")
    assert build.split_code("_Z") == ("_Z", "")
    assert build.split_code("") == ("", "")


def test_builder_counts_and_sums_numbered_sub_lines(index):
    _, stats = index
    assert stats["rows"] == len(ROWS)
    # Suncor, the stub company, two Minto IDs, the retired company; the two
    # entities filed in Suncor's report report nothing themselves.
    assert stats["reporting_entities"] == 5
    assert stats["entities"] == 7
    import sqlite3

    conn = sqlite3.connect(index[0])
    fees = conn.execute(
        "SELECT amount FROM payment WHERE kind='payee' AND code='PY000002'"
    ).fetchall()
    assert fees == [(10.0,)]  # F_1 + F_2, one line
    meta = dict(conn.execute("SELECT key, value FROM meta"))
    assert meta["cut"] == CUT and meta["schema_version"] == build.SCHEMA_VERSION


def test_builder_refuses_a_changed_header(tmp_path):
    bad = _write_csv(tmp_path / "bad.csv", ROWS, header=build.HEADER[:-1])
    with pytest.raises(build.RefusedInput):
        build.build(bad, tmp_path / "out.sqlite", CUT, min_rows=1, min_entities=1)
    assert not (tmp_path / "out.sqlite").exists()


def test_builder_refuses_a_currency_it_would_have_to_convert(tmp_path):
    rows = [_row(SUNCOR, "2025", unit="USD: US Dollar")]
    with pytest.raises(build.RefusedInput, match="only CAD"):
        build.build(_write_csv(tmp_path / "usd.csv", rows), tmp_path / "o.sqlite", CUT,
                    min_rows=1, min_entities=1)


def test_builder_refuses_a_short_file(tmp_path):
    with pytest.raises(build.RefusedInput, match="not the ESTMA dataset"):
        build.build(_write_csv(tmp_path / "s.csv", ROWS), tmp_path / "o.sqlite", CUT)


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


def test_info_is_esg_and_commercial_with_attribution(index):
    info = REGISTRY["estma"].info
    assert info.category == "esg"
    assert info.country is None  # not a register for knowability
    assert info.live_available is True
    terms = classify(info.license)
    assert terms.commercial_use == "yes" and terms.attribution_required and not terms.share_alike


def test_no_index_means_not_covered(no_index):
    adapter = REGISTRY["estma"]
    assert adapter.covers_lei(SUNCOR_LEI) is False
    assert adapter.info.live_available is False


def test_covers_only_accepted_crosswalk_rows(index):
    adapter = REGISTRY["estma"]
    assert adapter.covers_lei(SUNCOR_LEI)
    assert not adapter.covers_lei(RETIRED_LEI)
    assert not adapter.covers_lei("5493000000000000ZZ99")


async def test_payee_total_never_adds_the_project_view(index):
    with provenance.recording() as rec:
        bundle = await _bundle(SUNCOR_LEI)
    assert rec.resolve().liveness == "snapshot"
    assert rec.resolve().source_as_of.date().isoformat() == CUT
    entity = bundle["entities"][0]
    latest = entity["reports"][0]
    assert latest["year"] == "2025"
    assert latest["payee_total_cad"] == 160.0  # 100 + 50 + 10, not 320
    assert latest["payee_count"] == 2 and latest["project_count"] == 2
    assert [c["code"] for c in latest["by_country"]] == ["CA", "NO"]
    assert [t["code"] for t in latest["by_payment_type"]] == ["ROYALT", "TAXES", "FEES"]
    assert bundle["identifiers"] == {"ca_estma_id": "E405435"}
    assert [r["year"] for r in entity["reports"]] == ["2025", "2024"]


async def test_reporting_entity_names_the_entities_it_reports_for(index):
    bundle = await _bundle(SUNCOR_LEI)
    includes = bundle["entities"][0]["includes"]
    assert includes["year"] == "2025"
    assert [e["estma_id"] for e in includes["entities"]] == ["E984046", "E425485_1"]


async def test_included_entity_carries_no_amounts(index):
    bundle = await _bundle(COSP_LEI)
    report = bundle["entities"][0]["reports"][0]
    assert report["included_in"] == {"estma_id": "E405435", "name": "SUNCOR ENERGY INC."}
    assert "payee_total_cad" not in report


async def test_two_reports_for_one_year_stay_apart(index):
    bundle = await _bundle(STUB_LEI)
    reports = bundle["entities"][0]["reports"]
    assert len(reports) == 2
    assert sorted(r["payee_total_cad"] for r in reports) == [5.0, 20.0]


async def test_one_lei_two_estma_ids(index):
    bundle = await _bundle(TWO_IDS_LEI)
    assert [e["estma_id"] for e in bundle["entities"]] == ["E964086", "E198001"]
    assert bundle["entities"][0]["match"]["basis"] == estma.MATCH_BASIS["checked by Stephen"]


async def test_fetch_miss_is_a_stub(index):
    assert await _bundle("5493000000000000ZZ99") is None
    stub = await REGISTRY["estma"].fetch("5493000000000000ZZ99")
    assert stub["is_stub"] is True


# ---------------------------------------------------------------------------
# Mapper, hit builder, finding
# ---------------------------------------------------------------------------


async def test_mapper_one_entity_no_relationships_lei_annotated(index):
    bundle = await _bundle(SUNCOR_LEI)
    with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
        statements = list(map_estma(bundle))
    assert len(statements) == 1
    assert validate_shape(statements) == []
    rd = statements[0]["recordDetails"]
    assert rd["identifiers"] == [{
        "id": "E405435", "scheme": "CA-ESTMA",
        "schemeName": "ESTMA entity ID — Natural Resources Canada",
    }]
    assert "jurisdiction" not in rd
    assert statements[0]["statementDate"] == CUT
    notes = statements[0]["annotations"]
    assert any(a["motivation"] == "identifying" and SUNCOR_LEI in a["description"] for a in notes)
    assert all(s["recordType"] == "entity" for s in statements)


async def test_mapper_says_a_reporting_arrangement_is_not_ownership(index):
    bundle = await _bundle(COSP_LEI)
    with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
        statements = list(map_estma(bundle))
    note = next(a for a in statements[0]["annotations"] if a["motivation"] == "commenting")
    assert "not a statement of ownership or control" in note["description"]
    assert [s["recordType"] for s in statements] == ["entity"]


def test_mapper_ignores_stubs():
    assert list(map_estma({"is_stub": True})) == []
    assert list(map_estma({})) == []


async def test_hit_builder_asserts_the_estma_id_not_the_lei(index):
    bundle = await _bundle(SUNCOR_LEI)
    hit = _bh_estma(bundle, _ctx(SUNCOR_LEI, "Suncor Energy Inc."))
    assert hit.identifiers == {"ca_estma_id": "E405435"}
    assert "lei" not in hit.identifiers
    assert hit.summary == "ESTMA E405435 · 2024–2025"
    assert _build_result_hit("estma", bundle, _ctx(SUNCOR_LEI)) is not None
    assert _build_result_hit("estma", None, _ctx(SUNCOR_LEI)) is None


async def test_finding_counts_payees_and_countries(index):
    bundle = await _bundle(SUNCOR_LEI)
    assert finding_estma(bundle) == (
        "Reported payments to 2 government payees in 2 countries for 2025, "
        "across 2 projects; its report also covers 2 other entities."
    )


async def test_finding_for_an_included_entity_names_the_report(index):
    bundle = await _bundle(COSP_LEI)
    assert finding_estma(bundle) == "Payments for 2025 filed in the report of SUNCOR ENERGY INC."


async def test_finding_says_two_reports_without_adding_them(index):
    bundle = await _bundle(STUB_LEI)
    sentence = finding_estma(bundle)
    assert sentence == (
        "Reported payments to 1 government payee in 1 country for 2025, in 2 reports for that year."
    )
    assert "25" not in sentence.replace("2025", "")
    assert len(sentence) <= MAX_FINDING_CHARS


def test_finding_degrades():
    assert finding_estma({}) is None
    assert finding_estma({"is_stub": True}) is None
    assert finding_estma({"entities": [{"reports": []}]}) is None


# ---------------------------------------------------------------------------
# The committed crosswalk
# ---------------------------------------------------------------------------


def test_committed_crosswalk_is_well_formed_and_carries_the_reviewed_rows():
    path = Path(estma.__file__).resolve().parent.parent / "data" / "estma_lei_crosswalk.csv"
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    accepted = [r for r in rows if r["decision"] == "accepted"]
    assert len(accepted) >= 700
    for r in accepted:
        assert len(r["lei"]) == 20, r
        assert r["estma_id"].startswith("E"), r
        assert r["method"] in estma.MATCH_BASIS, r["method"]
    by_id = {r["estma_id"]: r for r in rows}
    # The five rows Stephen settled by hand on 10 Nov 2026.
    stephen = {r["estma_id"]: r["lei"] for r in accepted if r["method"] == "checked by Stephen"}
    assert set(stephen.values()) >= {
        "5493000FX50GEWAF3U70", "529900OOJMT6X29RYO98", "5299008S2TTH1BS4UF53",
        "549300Q1NUF1HR60FR27", "5493008RFUSBWG5XLV10",
    }
    assert by_id["E198001"]["lei"] == "5493008RFUSBWG5XLV10"
    # A retired LEI is recorded, never read.
    assert all(r["decision"] != "accepted" for r in rows if r["lei_status"] == "RETIRED")
