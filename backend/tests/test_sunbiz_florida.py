"""The Florida wiring (30 Oct 2026) — the Division of Corporations (Sunbiz),
read from an index of its quarterly bulk files.

Every index here is produced by the **real builder**
(``scripts/build_sunbiz_florida_index.py``) from zips in the Division's
published shape — ten ``cordataN.txt`` members of 1,440-character fixed-width
records and one ``corevt.txt`` of 662-character events, latin-1, CRLF — and
read back through the **real adapter** (the JAR / ONRC rule). The records are
composed field by field at the official layout's offsets; every person in them
is invented.
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import pytest

from opencheck import degradation, provenance
from opencheck.bods.mapper import map_sunbiz_florida
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_sunbiz_florida
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY, sunbiz_florida
from opencheck.sources.base import SearchKind
from opencheck.sources.schemas.sunbiz_florida import SunbizFloridaBundle
from opencheck.sources.sunbiz_florida import (
    FL_RA_CODE,
    clean_fei,
    name_check,
    normalise_doc,
    status_reading,
    title_label,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_sunbiz_florida_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_sunbiz_florida_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = (2026, 10, 1)
EVENTS_DAY = (2026, 10, 2)
LEI = "254900U0350DLRN7AR49"

ALTA = "L22000385605"       # active LLC, two people and a corporate manager
RENAMED = "P10000012345"    # active corporation renamed twice
DISSOLVED = "L16000216990"  # inactive LLC an LEI names, administratively dissolved
FOREIGN = "F16000002690"    # inactive Delaware corporation, named by its FEI only
MORE6 = "N21000010490"      # active not-for-profit with more than six officers
MERGED = "P20000000002"     # active corporation with two merger events
DROPPED = "L10000000001"    # inactive and named by no LEI: not kept
ABSENT = "L99999999999"


# ---------------------------------------------------------------------------
# Composing records at the official layout's offsets
# ---------------------------------------------------------------------------


class _Row:
    def __init__(self, width: int) -> None:
        self.chars = [" "] * width

    def put(self, offset: int, width: int, value: str) -> _Row:
        text = value[:width].ljust(width)
        self.chars[offset:offset + width] = list(text)
        return self

    def line(self) -> str:
        return "".join(self.chars).rstrip() + "\r\n"


def _officer(row: _Row, i: int, title: str, kind: str, surname: str, given: str = "",
             middle: str = "", city: str = "MIAMI", state: str = "FL") -> None:
    base = 668 + i * 128
    row.put(base, 4, title).put(base + 4, 1, kind)
    if kind == "P":
        row.put(base + 5, 20, surname).put(base + 25, 14, given).put(base + 39, 8, middle)
    else:
        row.put(base + 5, 42, surname)
    # A street address and a ZIP, which must never reach the index (D4).
    row.put(base + 47, 42, "1 SECRET STREET").put(base + 89, 28, city)
    row.put(base + 117, 2, state).put(base + 119, 9, "331310000")


def _record(doc: str, name: str, status: str, ftype: str, *, filed: str = "09012022",
            fei: str = "", more6: str = "N", last_tx: str = "", sc: str = "FL",
            reports: tuple[tuple[str, str], ...] = (), officers=(),
            agent: tuple[str, str] | None = ("REGISTERED AGENTS INC", "C")) -> str:
    row = _Row(1440)
    row.put(0, 12, doc).put(12, 192, name).put(204, 1, status).put(205, 15, ftype)
    row.put(220, 42, "100 BISCAYNE BLVD").put(304, 28, "MIAMI").put(332, 2, "FL")
    row.put(334, 10, "33131").put(344, 2, "US")
    row.put(346, 42, "PO BOX 1").put(430, 28, "MIAMI").put(458, 2, "FL")
    row.put(460, 10, "33101").put(470, 2, "US")
    row.put(472, 8, filed).put(480, 14, fei).put(494, 1, more6).put(495, 8, last_tx)
    row.put(503, 2, sc)
    for (year, day), (y0, d0) in zip(reports, ((505, 510), (518, 523), (531, 536)), strict=False):
        row.put(y0, 4, year).put(d0, 8, day)
    if agent:
        row.put(544, 42, agent[0]).put(586, 1, agent[1])
        row.put(587, 42, "2 AGENT STREET").put(629, 28, "TALLAHASSEE").put(657, 2, "FL")
        row.put(659, 9, "323010000")
    for i, officer in enumerate(officers):
        _officer(row, i, *officer)
    return row.line()


def _event(doc: str, seq: int, code: str, *, effective: str = "", filed: str = "",
           other: str = "", name: str = "", description: str = "") -> str:
    row = _Row(662)
    row.put(0, 12, doc).put(12, 5, str(seq).rjust(5, "0")).put(17, 20, code)
    row.put(37, 40, description or code).put(77, 8, effective).put(85, 8, filed)
    row.put(198, 12, other).put(210, 192, name)
    return row.line()


def _records() -> list[str]:
    return [
        _record(ALTA, "ALTA NEW RIVER LLC", "A", "FLAL", fei="920243279", last_tx="05152026",
                reports=(("2026", "04232026"), ("2026", "04232026"), ("2025", "03112025")),
                officers=[("AP", "P", "DOE", "JANE", "Q"), ("AP", "P", "ROE", "RICHARD"),
                          ("MGR", "C", "ALTA MANAGEMENT LLC")]),
        _record(RENAMED, "NEWNAME HOLDINGS INC", "A", "DOMP", filed="01152010",
                officers=[("P, C", "P", "SMITH", "ANNA")]),
        _record(DISSOLVED, "FLIP THAT HOUSE LLC", "I", "FLAL", filed="11302016",
                last_tx="09222017", officers=[("MGR", "P", "BLOGGS", "JOE")]),
        _record(FOREIGN, "COMMERCE INVESTORS GP, INC.", "I", "FORP", filed="06132016",
                fei="65-0686540", sc="DE", last_tx="04012024",
                reports=(("2023", "04242023"),),
                officers=[("PST", "P", "OBRIEN", "MARTIN", "E")]),
        _record(MORE6, "COMPANION OF THE POOR, USA, INC.", "A", "DOMNP", more6="Y",
                filed="09032021", fei="APPLIED FOR",
                officers=[(t, "P", f"PERSON{i}", "ALEX") for i, t in
                          enumerate(("P, C", "S", "D", "D", "Dire", "AVP"))]),
        _record(MERGED, "SURVIVOR CORP", "A", "DOMP", filed="03072000"),
        _record(DROPPED, "LONG GONE LLC", "I", "FLAL", filed="01012010"),
    ]


def _events() -> list[str]:
    return [
        _event(RENAMED, 1, "CORAPNC", effective="06012015", name="MIDDLE NAME INC"),
        _event(RENAMED, 2, "CORAPAMD", effective="06012016"),  # not kept
        _event(RENAMED, 3, "CORAPNC", filed="07012020", name="NEWNAME HOLDINGS INC"),
        _event(DISSOLVED, 1, "CORAMADMAR", effective="09222017", filed="09222017"),
        _event(FOREIGN, 1, "CORAPWITH", filed="04012024"),
        _event(MERGED, 1, "CORAPMER", effective="12312021", other=ALTA),
        _event(MERGED, 2, "CORAPMER", effective="06302022", other=DROPPED),
        _event(DROPPED, 1, "CORAMADMAR", effective="09232011"),  # its record is not kept
    ]


def _zip_cordata(path: Path, records: list[str], *, members: int = 10) -> Path:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for m in range(members):
            body = "".join(r for r in records if r[11] == str(m) or (m == 0 and not r[11].isdigit()))
            if m == 0:
                body += ("X" * 1441) + "\r\n"  # one malformed row
            info = zipfile.ZipInfo(f"cordata{m}.txt", date_time=(*CUT, 3, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, body.encode("latin-1"))
    return path


def _zip_events(path: Path, events: list[str], *, day=EVENTS_DAY) -> Path:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo("corevt.txt", date_time=(*day, 3, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        zf.writestr(info, "".join(events).encode("latin-1"))
    return path


def _build(tmp_path: Path, *, members: int = 10, events_day=EVENTS_DAY,
           min_rows: dict[str, int] | None = None, name: str = "sunbiz_florida.sqlite"):
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    cordata = _zip_cordata(src / "cordata.zip", _records(), members=members)
    corevent = _zip_events(src / "corevent.zip", _events(), day=events_day)
    out = tmp_path / "data" / name
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(
        cordata, corevent, out,
        lei_docs={DISSOLVED}, lei_feis={"650686540"},
        min_rows={} if min_rows is None else min_rows,
    )
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("SUNBIZ_FLORIDA_DB_FILE", None)
    else:
        os.environ["SUNBIZ_FLORIDA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    sunbiz_florida.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    out, stats = _build(tmp_path)
    _point_at(out)
    yield out, stats
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    _point_at(None)
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    _point_at(None)


async def _fetch(doc: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["sunbiz_florida"].fetch(doc, legal_name=legal_name)


def _by_type(statements) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {"entity": [], "person": [], "relationship": []}
    for s in statements:
        out[s["recordType"]].append(s)
    return out


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_keeps_active_and_lei_named_records_and_records_its_cut(index):
    out, stats = index
    assert stats["records"] == 7 and stats["active"] == 4 and stats["malformed"] == 1
    assert stats["kept"] == 6 and stats["kept_inactive_lei"] == 2
    assert stats["cut"] == "2026-10-01"
    con = sqlite3.connect(out)
    docs = {r[0] for r in con.execute("SELECT doc FROM entity")}
    assert DROPPED not in docs and {DISSOLVED, FOREIGN, ALTA} <= docs
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["cut"] == "2026-10-01" and meta["events_cut"] == "2026-10-02"
    assert meta["schema_version"] == sunbiz_florida.INDEX_SCHEMA_VERSION
    # Only the kinds the adapter reads, for kept records only.
    codes = sorted(r[0] for r in con.execute("SELECT code FROM event"))
    assert "CORAPAMD" not in codes and codes.count("CORAMADMAR") == 1
    con.close()


def test_no_officer_or_agent_street_or_zip_reaches_the_index(index):
    out, _ = index
    blob = Path(out).read_bytes()
    assert b"SECRET STREET" not in blob and b"AGENT STREET" not in blob
    assert b"331310000" not in blob and b"323010000" not in blob


def test_a_repeated_annual_report_is_kept_once(index):
    out, _ = index
    con = sqlite3.connect(out)
    (reports,) = con.execute("SELECT reports FROM entity WHERE doc = ?", (ALTA,)).fetchone()
    con.close()
    assert reports == "2026:2026-04-23;2025:2025-03-11"


def test_the_builder_refuses_a_missing_member_and_far_apart_files(tmp_path):
    with pytest.raises(builder.RefusedInput, match="9 cordata members"):
        _build(tmp_path, members=9)
    with pytest.raises(builder.RefusedInput, match="not one quarter"):
        _build(tmp_path, events_day=(2026, 6, 1))
    assert not (tmp_path / "data" / "sunbiz_florida.sqlite").exists()


def test_the_builder_refuses_a_short_file_and_writes_nothing(tmp_path):
    with pytest.raises(builder.RefusedInput, match="not the register"):
        _build(tmp_path, min_rows=builder.MIN_ROWS)
    assert not (tmp_path / "data" / "sunbiz_florida.sqlite").exists()
    assert not (tmp_path / "data" / "sunbiz_florida.sqlite.building").exists()


# ---------------------------------------------------------------------------
# Small readers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("L22000385605", "L22000385605"),
        (" m70748 ", "M70748"),
        ("59-0896138", "FEI:590896138"),
        ("FEI:590896138", "FEI:590896138"),
    ],
)
def test_a_document_number_and_an_ein_normalise(raw, want):
    assert normalise_doc(raw) == want


@pytest.mark.parametrize("raw", ["", "L2200038560", "590896138X", "ABC-DEF"])
def test_anything_else_is_not_a_document_number(raw):
    with pytest.raises(ValueError):
        normalise_doc(raw)


def test_an_fei_is_nine_digits_or_nothing():
    assert clean_fei("65-0686540") == "650686540"
    assert clean_fei("APPLIED FOR") is None and clean_fei("000000000") is None


@pytest.mark.parametrize(
    ("title", "words"),
    [
        ("MGR", "manager"), ("AMBR", "authorized member"), ("PSD", "president, secretary, director"),
        ("VPT", "vice president, treasurer"), ("P, C", "president, chair"),
        ("AVP", "assistant vice president"), ("Dire", "director"), ("Proj", "Proj"), ("", "officer"),
    ],
)
def test_officer_titles_are_read_in_words_or_shown_as_filed(title, words):
    assert title_label(title) == words


def test_status_reads_the_latest_terminal_event_unless_reversed():
    dissolved = [{"seq": 1, "code": "CORAMADMAR", "effective": "2017-09-22"}]
    assert status_reading("I", "FLAL", dissolved) == {
        "class": "terminal", "since": "2017-09-22", "event": "CORAMADMAR",
        "words": "administratively dissolved for not filing an annual report",
    }
    reinstated = [*dissolved, {"seq": 2, "code": "CORAPREIN", "filed": "2018-01-02"}]
    assert status_reading("I", "FLAL", reinstated)["words"] == "inactive"
    assert status_reading("I", "FLAL", reinstated)["since"] is None
    assert status_reading("A", "FLAL", dissolved)["class"] == "live"
    foreign = status_reading("I", "FORP", [])
    assert foreign["class"] == "inactive_foreign" and foreign["words"] == "inactive in Florida"


def test_the_name_gate_accepts_a_recorded_rename_and_refuses_a_stranger():
    events = [{"seq": 1, "code": "CORAPNC", "name": "MIDDLE NAME INC"}]
    assert name_check("Middle Name, Inc.", "NEWNAME HOLDINGS INC", events) == "agrees"
    assert name_check("Original Widgets Inc", "NEWNAME HOLDINGS INC", events) == "renamed"
    assert name_check("Original Widgets Inc", "NEWNAME HOLDINGS INC", []) == "differs"
    assert name_check("", "ANYTHING", []) == "agrees"


def test_the_schema_refuses_an_officer_street_address():
    with pytest.raises(ValueError):
        SunbizFloridaBundle.model_validate(
            {"doc": ALTA, "officers": [{"name": "JANE DOE", "addr1": "1 SECRET STREET"}]}
        )


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["sunbiz_florida"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({FL_RA_CODE})
    assert deriver.derived_key == "us_fl_doc"
    assert deriver.normalise("l22000385605") == ALTA
    assert adapter.lookup_pass_legal_name is True


async def test_an_active_record_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch(ALTA, legal_name="Alta New River LLC")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["name"] == "ALTA NEW RIVER LLC" and bundle["status_class"] == "live"
    assert bundle["file_date"] == "2022-09-01" and bundle["fei"] == "920243279"
    assert [o["name"] for o in bundle["officers"]] == ["JANE Q DOE", "RICHARD ROE", "ALTA MANAGEMENT LLC"]
    assert bundle["officers"][0]["city"] == "MIAMI"
    assert bundle["registered_agent"] == {
        "name": "REGISTERED AGENTS INC", "type": "C", "city": "TALLAHASSEE", "state": "FL",
    }
    assert bundle["annual_reports"][0] == {"year": "2026", "filed": "2026-04-23"}
    assert bundle["name_check"] == "agrees" and bundle["matched_via"] == "document_number"
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == "2026-10-01"
    assert resolved.retrieved_at is not None


async def test_a_dissolved_record_an_lei_names_is_dated_by_its_event(index):
    bundle = await _fetch(DISSOLVED, legal_name="Flip That House LLC")
    assert bundle["status_class"] == "terminal"
    assert bundle["status_since"] == "2017-09-22" and bundle["status_event"] == "CORAMADMAR"


async def test_an_lei_that_filed_the_ein_is_matched_on_the_fei(index):
    bundle = await _fetch("65-0686540", legal_name="Commerce Investors GP, Inc.")
    assert bundle["doc"] == FOREIGN and bundle["matched_via"] == "fei"
    assert bundle["status_class"] == "inactive_foreign"
    assert bundle["status_words"] == "withdrawn from Florida"


async def test_a_renamed_entity_keeps_its_former_names_and_passes_the_gate(index):
    bundle = await _fetch(RENAMED, legal_name="Original Widgets Inc")
    assert bundle["name_check"] == "renamed" and not bundle.get("not_found")
    assert bundle["former_names"] == [{"name": "MIDDLE NAME INC", "since": "2015-06-01"}]


async def test_a_number_for_a_differently_named_company_is_a_note_card(index):
    degradation.begin()
    bundle = await _fetch(ALTA, legal_name="Totally Different Bakery Inc")
    assert degradation.collect() == []
    assert bundle["not_found"] is True and bundle["name_mismatch"] is True
    assert bundle["registered_name"] == "ALTA NEW RIVER LLC"
    assert "officers" in bundle and bundle["officers"] == []


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    degradation.begin()
    with provenance.recording() as rec:
        bundle = await _fetch(ABSENT, legal_name="GHOST LLC")
    assert degradation.collect() == []
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert "not in the Florida Division of Corporations' quarterly data" in bundle["coverage_note"]
    assert bundle["cut"] == "2026-10-01"
    assert not rec.observations
    assert (await _fetch(DROPPED))["not_found"] is True


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["sunbiz_florida"]
    assert sunbiz_florida.index_available() is False
    assert adapter.covers_lei(LEI) is False
    assert (await _fetch(ALTA))["is_stub"] is True
    assert await adapter.search("ALTA", SearchKind.ENTITY) == []


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    out, _ = _build(tmp_path)
    con = sqlite3.connect(out)
    con.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    con.commit()
    con.close()
    _point_at(out)
    try:
        assert sunbiz_florida.index_available() is False
        assert (await _fetch(ALTA))["is_stub"] is True
    finally:
        _point_at(None)


async def test_name_search_finds_the_record_with_an_exact_name_first(index):
    hits = await REGISTRY["sunbiz_florida"].search("Alta New River LLC", SearchKind.ENTITY)
    assert hits[0].hit_id == ALTA
    assert hits[0].identifiers == {"us_fl_doc": ALTA}
    assert hits[0].summary == "US-FL L22000385605 · Florida limited liability company"
    dissolved = await REGISTRY["sunbiz_florida"].search("flip that house", SearchKind.ENTITY)
    assert dissolved[0].summary.endswith("· inactive")


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_the_company_and_its_officers_as_managing_officials(index):
    with provenance.recording():
        bundle = await _fetch(ALTA, legal_name="Alta New River LLC")
        statements = list(map_sunbiz_florida(bundle))
    by = _by_type(statements)
    (company,) = [e for e in by["entity"] if e["recordDetails"]["name"] == "ALTA NEW RIVER LLC"]
    rd = company["recordDetails"]
    assert {(i["scheme"], i["id"]) for i in rd["identifiers"]} == {
        ("US-FL", ALTA), ("US-EIN", "92-0243279"),
    }
    assert rd["jurisdiction"] == {"name": "United States", "code": "US"}
    assert rd["foundingDate"] == "2022-09-01"
    assert [p["recordDetails"]["names"][0]["fullName"] for p in by["person"]] == ["JANE Q DOE", "RICHARD ROE"]
    jane = by["person"][0]["recordDetails"]["names"][0]
    assert jane["familyName"] == "DOE" and jane["givenName"] == "JANE"
    corporate = [e for e in by["entity"] if e["recordDetails"]["name"] == "ALTA MANAGEMENT LLC"]
    assert corporate and "jurisdiction" not in corporate[0]["recordDetails"]
    assert len(by["relationship"]) == 3
    for rel in by["relationship"]:
        (interest,) = rel["recordDetails"]["interests"]
        assert interest["type"] == "seniorManagingOfficial"
        assert "beneficialOwnershipOrControl" not in interest
        assert rel["statementDate"] == "2026-04-23"
    assert by["relationship"][0]["recordDetails"]["interests"][0]["details"].startswith(
        "Authorized person (filed as “AP”)"
    )
    # Nobody's street address reaches BODS.
    text = repr(statements)
    assert "SECRET STREET" not in text and "AGENT STREET" not in text


async def test_a_dissolution_reaches_the_status_with_its_date(index):
    with provenance.recording():
        statements = list(map_sunbiz_florida(await _fetch(DISSOLVED)))
    company = statements[0]
    assert company["recordDetails"]["dissolutionDate"] == "2017-09-22"


async def test_a_foreign_filing_takes_its_home_jurisdiction_and_no_founding_date(index):
    with provenance.recording():
        statements = list(map_sunbiz_florida(await _fetch(FOREIGN)))
    rd = statements[0]["recordDetails"]
    assert rd["jurisdiction"] == {"name": "United States", "code": "US"}
    assert "foundingDate" not in rd and "dissolutionDate" not in rd
    notes = " ".join(a.get("description", "") for a in statements[0].get("annotations", []))
    assert "says nothing about the entity in its home jurisdiction" in notes


async def test_former_names_are_alternate_names_and_a_rename_is_annotated(index):
    with provenance.recording():
        statements = list(map_sunbiz_florida(await _fetch(RENAMED, legal_name="Original Widgets Inc")))
    rd = statements[0]["recordDetails"]
    assert rd["alternateNames"] == ["MIDDLE NAME INC"]
    notes = " ".join(a.get("description", "") for a in statements[0].get("annotations", []))
    assert "GLEIF names this record “Original Widgets Inc”" in notes


async def test_a_merger_partner_is_named_never_related(index):
    with provenance.recording():
        statements = list(map_sunbiz_florida(await _fetch(MERGED)))
    by = _by_type(statements)
    assert by["relationship"] == []
    partner = [e for e in by["entity"] if e["recordDetails"]["name"] == "ALTA NEW RIVER LLC"]
    assert partner, "the partner the index holds is an entity statement"
    notes = " ".join(a.get("description", "") for a in statements[0].get("annotations", []))
    assert DROPPED in notes and "record outside the index" in notes


async def test_more_than_six_officers_is_said_on_the_record(index):
    with provenance.recording():
        bundle = await _fetch(MORE6)
        statements = list(map_sunbiz_florida(bundle))
    assert bundle["fei"] is None
    notes = " ".join(a.get("description", "") for a in statements[0].get("annotations", []))
    assert "more than six officers" in notes


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_sunbiz_florida({"is_stub": True})) == []
    assert list(map_sunbiz_florida({"is_stub": False, "not_found": True, "doc": ALTA})) == []


# ---------------------------------------------------------------------------
# The sentence, the card and dispatch
# ---------------------------------------------------------------------------


async def test_the_finding_sentence(index):
    s = finding_sunbiz_florida(await _fetch(ALTA))
    assert s == (
        "Active, filed 1 September 2022, Florida limited liability company, "
        "3 officers listed, last annual report 2026."
    )
    assert len(s) <= MAX_FINDING_CHARS
    s = finding_sunbiz_florida(await _fetch(DISSOLVED))
    assert s.startswith(
        "Administratively dissolved for not filing an annual report on 22 September 2017"
    )
    assert "1 officer listed" in s
    s = finding_sunbiz_florida(await _fetch(MORE6))
    assert "more than six officers (list incomplete)" in s
    assert finding_sunbiz_florida(await _fetch(ABSENT)).startswith("Not in the Florida")
    assert "differently named company" in finding_sunbiz_florida(
        await _fetch(ALTA, legal_name="Totally Different Bakery Inc")
    )
    assert finding_sunbiz_florida({"is_stub": True}) is None


async def test_the_hit_builder_asserts_the_number_and_the_ein_only_for_a_record(index):
    from opencheck.routers.hit_builders import _bh_sunbiz_florida

    ctx = _LookupCtx(lei=LEI, legal_name="ALTA NEW RIVER LLC")
    hit = _bh_sunbiz_florida(await _fetch(ALTA), ALTA, ctx)
    assert hit.identifiers == {"us_fl_doc": ALTA, "us_ein": "92-0243279"}
    assert hit.summary == "US-FL L22000385605 · Florida limited liability company"
    hit = _bh_sunbiz_florida(await _fetch(DISSOLVED), DISSOLVED, ctx)
    assert hit.summary.endswith("administratively dissolved for not filing an annual report")
    hit = _bh_sunbiz_florida(await _fetch(ABSENT), ABSENT, ctx)
    assert hit.identifiers == {} and hit.raw["coverage_note"]


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="sunbiz_florida")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_on_the_ra_code_when_the_index_is_present(index):
    ctx = _LookupCtx(lei=LEI, jurisdiction="US-FL", registered_as=ALTA)
    _build_derived(ctx, FL_RA_CODE)
    assert ctx.derived.get("us_fl_doc") == ALTA
    assert _dispatched(ctx) == {"sunbiz_florida"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei=LEI, jurisdiction="US-FL", registered_as=ALTA)
    _build_derived(ctx, FL_RA_CODE)
    assert _dispatched(ctx) == set()


async def test_the_note_card_survives_the_pipelines_stub_drop(index):
    bundle = await _fetch(ABSENT, "GHOST LLC")
    ctx = _LookupCtx(lei=LEI, legal_name="GHOST LLC", jurisdiction="US-FL", registered_as=ABSENT)
    _build_derived(ctx, FL_RA_CODE)
    hit = _build_result_hit("sunbiz_florida", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {}
    assert hit.finding and hit.finding.startswith("Not in the Florida")


# ---------------------------------------------------------------------------
# Reading GLEIF's registeredAs values (the first workflow run, 31 Oct 2026)
# ---------------------------------------------------------------------------


def _fake_gleif(per_status: dict[str, list[str]], total: int):
    """A fetch that answers like GLEIF: 400 for a status outside its enum."""
    import re as _re

    def fetch(url: str) -> dict:
        m = _re.search(r"registration\.status%5D=([A-Z_]+)", url)
        if m is None:
            return {"meta": {"pagination": {"total": total, "lastPage": 1}}, "data": []}
        status = m.group(1)
        if status not in builder.GLEIF_REGISTRATION_STATUSES:
            raise AssertionError(f"GLEIF answers 400 for {status}")
        values = per_status.get(status, [])
        return {
            "meta": {"pagination": {"total": len(values), "lastPage": 1}},
            "data": [{"attributes": {"entity": {"registeredAs": v}}} for v in values],
        }

    return fetch


def test_only_statuses_gleif_accepts_are_asked_for():
    # GLEIF's own enum, from its 400 message: TRANSFERRED / CANCELLED / PENDING_VALIDATION are not in it.
    assert set(builder.GLEIF_REGISTRATION_STATUSES) == {
        "ISSUED", "LAPSED", "ANNULLED", "PENDING_TRANSFER", "PENDING_ARCHIVAL",
        "DUPLICATE", "RETIRED", "MERGED",
    }
    docs, feis = builder.gleif_registered_as(
        fetch=_fake_gleif({"ISSUED": [ALTA, "59-0896138"], "LAPSED": [DISSOLVED]}, total=3)
    )
    assert docs == {ALTA, DISSOLVED} and feis == {"590896138"}


def test_statuses_that_do_not_add_up_to_the_total_are_refused():
    with pytest.raises(builder.RefusedInput, match="a status is missing"):
        builder.gleif_registered_as(fetch=_fake_gleif({"ISSUED": [ALTA]}, total=2))
