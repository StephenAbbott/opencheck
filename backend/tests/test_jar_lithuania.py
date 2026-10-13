"""Phase 325 — Lithuania's Register of Legal Entities, read from an index.

Every index in this file is produced by the **real builder**
(``scripts/build_jar_lithuania_index.py``) from CSVs in the register's exact
published shape, and read back through the **real adapter**. A fixture that
hand-writes the index would encode what the adapter expects rather than what
the builder writes — the ONRC lesson (Phase 211), where every adapter test
passed against an index no builder produced.

The rows are modelled on the 12 Oct 2026 files: pipe-delimited, doubled-quote
escaping, one cut date on every row, and the legal form's name in the name
column wherever the register withholds a sole proprietorship's or
partnership's name.
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods import liveness as bods_liveness
from opencheck.bods.mapper import map_jar_lithuania
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_jar_lithuania
from opencheck.routers.lookup import _LookupCtx, _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, jar_lithuania
from opencheck.sources.base import SearchKind

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_jar_lithuania_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_jar_lithuania_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-10-12"
IGNITIS = "301844044"      # AB, operating
SOLE = "304420944"         # IĮ — the register withholds the name
LIQUIDATING = "302486707"  # UAB in liquidation (status 7)
PARTICIPANT = "110870933"  # UAB taking part in a reorganisation (status 2)
BUDGET = "188603472"       # a budget institution → stateBody
STATE_ENT = "110079374"    # a state enterprise → registeredEntity
GONE = "110000215"         # deregistered UAB
GONE_SOLE = "300557319"    # deregistered IĮ, name withheld
ABSENT = "116957320"       # in neither file (GLEIF's one miss on 12 Oct 2026)

_REG = "|".join(builder.REGISTERED_HEADER)
_DEREG = "|".join(builder.DEREGISTERED_HEADER)


def _registered_rows(cut: str = CUT) -> list[str]:
    return [
        f'{IGNITIS}|"AB ""Ignitis grupė"""|Vilnius, Laisvės pr. 10, LT-04215|2008-08-28|320|Akcinė bendrovė|0|Teisinis stat neįregistruotas|2020-07-28|{cut}',
        f"{SOLE}|Individuali įmonė|Kaunas, Laisvės al. 1, LT-44309|2016-12-01|810|Individuali įmonė|0|Teisinis stat neįregistruotas|2016-12-01|{cut}",
        f'{LIQUIDATING}|"UAB ""Senas sandėlis"""|Klaipėda, Taikos pr. 5, LT-91111|2011-03-01|310|Uždaroji akcinė bendrovė|7|Likviduojamas|2025-03-04|{cut}',
        f'{PARTICIPANT}|"UAB ""Jungtinė"""|Vilnius, Gedimino pr. 1, LT-01103|1992-05-05|310|Uždaroji akcinė bendrovė|2|Dalyvaujantis reorganizavime|2026-01-15|{cut}',
        f"{BUDGET}|Vilniaus lopšelis-darželis „Saulutė“|Vilnius, Žirmūnų g. 2, LT-09215|1995-01-01|950|Biudžetinė įstaiga|0|Teisinis stat neįregistruotas|1995-01-01|{cut}",
        f"{STATE_ENT}|Valstybės įmonė Registrų centras|Vilnius, Studentų g. 39, LT-08106|1997-01-01|110|Valstybės įmonė|0|Teisinis stat neįregistruotas|1997-01-01|{cut}",
    ]


INVERTED = "120083595"     # deregistered before its registration date

def _deregistered_rows(cut: str = CUT) -> list[str]:
    return [
        f"{INVERTED}|Valstybės įmonė „Senoji“|Vilnius, Senoji g. 9, LT-01100|1993-07-13|110|Valstybės įmonė|1992-09-17|{cut}",
        f'{GONE}|"UAB ""Praeitis"""|Vilnius, Senoji g. 1, LT-01100|1993-02-02|310|Uždaroji akcinė bendrovė|2019-05-02|{cut}',
        f"{GONE_SOLE}|Individuali įmonė|Šiauliai, Vilniaus g. 3, LT-76000|2005-06-06|810|Individuali įmonė|2014-11-20|{cut}",
    ]


def _write(path: Path, header: str, rows: list[str]) -> Path:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


def _build(tmp_path: Path, *, registered: list[str] | None = None,
           deregistered: list[str] | None = None, name: str = "jar_lithuania.sqlite") -> tuple[Path, dict]:
    src = tmp_path / "src"
    src.mkdir(exist_ok=True)
    reg = _write(src / builder.REGISTERED_FILE, _REG, _registered_rows() if registered is None else registered)
    der = _write(src / builder.DEREGISTERED_FILE, _DEREG, _deregistered_rows() if deregistered is None else deregistered)
    out = tmp_path / "data" / name
    meta = builder.build_index(reg, der, out, min_rows={})
    return out, meta


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("JAR_LITHUANIA_DB_FILE", None)
    else:
        os.environ["JAR_LITHUANIA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    jar_lithuania.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    """An index of the fixture rows, built by the real builder."""
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    """No index anywhere — neither named nor at the data-root default."""
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    _point_at(None)
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    _point_at(None)


async def _fetch(code: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["jar_lithuania"].fetch(code, legal_name=legal_name)


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_and_the_adapter_agree_on_the_schema() -> None:
    assert builder.SCHEMA_VERSION == jar_lithuania.INDEX_SCHEMA_VERSION


def test_the_builder_indexes_both_files(index) -> None:
    out, meta = index
    assert meta["cut"] == CUT
    assert meta["registered"] == "6"
    assert meta["deregistered"] == "3"
    assert meta["name_withheld"] == "2"
    assert meta["schema_version"] == builder.SCHEMA_VERSION
    # built_at is a moment, to the second, in UTC (Phase 318).
    assert provenance.parse_moment(meta["built_at"]) is not None
    assert "T" in meta["built_at"] and meta["built_at"].endswith("+00:00")


def test_a_withheld_name_is_stored_as_no_name(index) -> None:
    out, _ = index
    conn = sqlite3.connect(out)
    names = dict(conn.execute("SELECT code, name FROM entity"))
    conn.close()
    assert names[SOLE] is None and names[GONE_SOLE] is None
    # Doubled quotes are unescaped, never kept.
    assert names[IGNITIS] == 'AB "Ignitis grupė"'


def _refused(tmp_path: Path, **rows) -> str:
    with pytest.raises(builder.BuildRefused) as exc:
        _build(tmp_path, **rows)
    assert not (tmp_path / "data" / "jar_lithuania.sqlite").exists(), "a refusal must write nothing"
    return str(exc.value)


def test_the_site_shell_in_place_of_a_file_is_refused(tmp_path: Path) -> None:
    """A wrong file name answers HTTP 200 with the site's HTML shell."""
    src = tmp_path / "src"
    src.mkdir()
    (src / builder.REGISTERED_FILE).write_text("<!doctype html><html><head>", encoding="utf-8")
    _write(src / builder.DEREGISTERED_FILE, _DEREG, _deregistered_rows())
    with pytest.raises(builder.BuildRefused, match="header"):
        builder.build_index(
            src / builder.REGISTERED_FILE, src / builder.DEREGISTERED_FILE,
            tmp_path / "data" / "x.sqlite", min_rows={},
        )


def test_a_stray_quote_that_merges_rows_is_refused(tmp_path: Path) -> None:
    """The ONRC trap: a field-initial quote with no partner swallows the next
    line. The quote must *start* the field, or csv never enters quoted mode
    and the fixture proves nothing."""
    rows = _registered_rows()
    rows[2] = rows[2].replace('"UAB ""Senas sandėlis"""', '"UAB Senas sandėlis')
    assert "merging or splitting rows" in _refused(tmp_path, registered=rows)


def test_two_cut_dates_are_refused(tmp_path: Path) -> None:
    assert "one cut date" in _refused(tmp_path, deregistered=_deregistered_rows("2026-10-11"))


def test_a_code_that_is_not_nine_digits_is_refused(tmp_path: Path) -> None:
    rows = _registered_rows()
    rows[0] = rows[0].replace(IGNITIS, "30184404", 1)
    assert "nine digits" in _refused(tmp_path, registered=rows)


def test_a_repeated_code_is_refused(tmp_path: Path) -> None:
    rows = _registered_rows()
    assert "appears twice" in _refused(tmp_path, registered=rows + [rows[0]])


def test_a_short_file_is_refused(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    reg = _write(src / builder.REGISTERED_FILE, _REG, _registered_rows())
    der = _write(src / builder.DEREGISTERED_FILE, _DEREG, _deregistered_rows())
    with pytest.raises(builder.BuildRefused, match="fewer than"):
        builder.build_index(reg, der, tmp_path / "data" / "x.sqlite")


def test_the_builder_cli_from_a_directory(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    _write(src / builder.REGISTERED_FILE, _REG, _registered_rows())
    _write(src / builder.DEREGISTERED_FILE, _DEREG, _deregistered_rows())
    out = tmp_path / "out" / "jar.sqlite"
    assert builder.main(["--from-dir", str(src), "--out", str(out), "--gzip", "--min-rows", "0"]) == 0
    assert out.exists() and out.with_name("jar.sqlite.gz").exists()
    # A missing input is a refusal with an exit code, not a traceback.
    assert builder.main(["--from-dir", str(tmp_path / "nowhere"), "--out", str(out), "--min-rows", "0"]) == 2


# ---------------------------------------------------------------------------
# The adapter, reading the real builder's index
# ---------------------------------------------------------------------------


async def test_a_registered_company(index) -> None:
    with provenance.recording() as recorder:
        bundle = await _fetch(IGNITIS, "Ignitis grupe")
    assert bundle["is_stub"] is False
    assert bundle["name"] == 'AB "Ignitis grupė"'
    assert bundle["name_withheld"] is False
    assert bundle["address"] == "Vilnius, Laisvės pr. 10, LT-04215"
    assert bundle["legal_form"] == "Akcinė bendrovė"
    assert bundle["registered_on"] == "2008-08-28"
    assert bundle["status_code"] == "0"
    assert bundle["deregistered"] is False
    assert bundle["cut"] == CUT
    prov = recorder.resolve(is_stub=False)
    assert prov.liveness == "snapshot"
    # The register's clock and ours, apart (Phase 314).
    assert prov.source_as_of is not None and prov.source_as_of.date().isoformat() == CUT
    assert prov.retrieved_at is not None


async def test_a_withheld_name_falls_back_to_gleif_and_says_so(index) -> None:
    bundle = await _fetch(SOLE, "IĮ \"4 BIZ\"")
    assert bundle["name"] == 'IĮ "4 BIZ"'
    assert bundle["register_name"] is None
    assert bundle["name_withheld"] is True


async def test_a_deregistered_company(index) -> None:
    bundle = await _fetch(GONE)
    assert bundle["deregistered"] is True
    assert bundle["deregistered_on"] == "2019-05-02"
    assert bundle["status"] is None and bundle["status_code"] is None


async def test_a_code_outside_the_index_is_a_note_card_not_a_stub(index) -> None:
    with provenance.recording() as recorder:
        bundle = await _fetch(ABSENT, "GENIRA GROUP HOLDINGS OÜ VILNIUS OFFICE")
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"] == jar_lithuania.COVERAGE_NOT_IN_INDEX
    # A miss is not a read of data: nothing declared.
    assert not recorder.observations


async def test_a_miss_is_answered_once_the_index_holds_the_code(tmp_path: Path) -> None:
    """Nothing is cached between builds: the miss belongs to one index."""
    first, _ = _build(tmp_path, name="first.sqlite")
    _point_at(first)
    try:
        assert (await _fetch(ABSENT))["not_found"] is True
        rows = _registered_rows() + [
            f'{ABSENT}|"UAB ""Nauja"""|Vilnius, Naujoji g. 1, LT-01101|2026-10-09|310|Uždaroji akcinė bendrovė|0|Teisinis stat neįregistruotas|2026-10-09|{CUT}'
        ]
        second, _ = _build(tmp_path, registered=rows, name="second.sqlite")
        _point_at(second)
        bundle = await _fetch(ABSENT)
        assert bundle.get("not_found") is None and bundle["name"] == 'UAB "Nauja"'
    finally:
        _point_at(None)


async def test_no_index_is_a_stub_and_the_source_is_not_announced(no_index) -> None:
    adapter = REGISTRY["jar_lithuania"]
    assert adapter.covers_lei("5493005RZJHJT5PNHY10") is False
    assert adapter.info.live_available is False
    bundle = await _fetch(IGNITIS, "Ignitis grupe")
    assert bundle["is_stub"] is True
    # Never the pre-325 shape: a non-stub card carrying only GLEIF's name.
    assert "register_unavailable" not in bundle


async def test_an_index_of_another_schema_is_ignored(tmp_path: Path) -> None:
    out, _ = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert jar_lithuania.index_available() is False
    finally:
        _point_at(None)


async def test_search_reads_the_index(index) -> None:
    hits = await REGISTRY["jar_lithuania"].search("Praeitis", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [GONE]
    assert hits[0].summary.split(" · ")[-1] == "deregistered"
    liquidating = await REGISTRY["jar_lithuania"].search("Senas", SearchKind.ENTITY)
    assert liquidating[0].summary.endswith("in liquidation")
    # A withheld name is not searchable as the form's name.
    assert await REGISTRY["jar_lithuania"].search("Individuali", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# The pipeline seams
# ---------------------------------------------------------------------------


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="jar_lithuania")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index) -> None:
    ctx = _LookupCtx(lei="5493005RZJHJT5PNHY10", jurisdiction="LT", registered_as=IGNITIS)
    _build_derived(ctx, "RA000430")
    assert _dispatched(ctx) == {"jar_lithuania"}


def test_not_dispatched_without_an_index(no_index) -> None:
    ctx = _LookupCtx(lei="5493005RZJHJT5PNHY10", jurisdiction="LT", registered_as=IGNITIS)
    _build_derived(ctx, "RA000430")
    assert _dispatched(ctx) == set()


async def test_the_note_card_survives_the_pipelines_stub_drop(index) -> None:
    bundle = await _fetch(ABSENT, "GENIRA GROUP HOLDINGS OÜ VILNIUS OFFICE")
    ctx = _LookupCtx(lei="254900JHKNIMTW39IY50", legal_name="GENIRA", jurisdiction="LT", registered_as=ABSENT)
    _build_derived(ctx, "RA000430")
    hit = _build_result_hit("jar_lithuania", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {}, "the register published no identifier for us"
    assert hit.finding and "Not in the Register of Legal Entities" in hit.finding


async def test_a_record_asserts_the_code_it_was_published_under(index) -> None:
    bundle = await _fetch(GONE)
    ctx = _LookupCtx(lei="X", legal_name="Praeitis", jurisdiction="LT", registered_as=GONE)
    _build_derived(ctx, "RA000430")
    hit = _build_result_hit("jar_lithuania", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {"lt_code": GONE}
    assert hit.summary == f"LT-JAR {GONE} · deregistered"


# ---------------------------------------------------------------------------
# BODS
# ---------------------------------------------------------------------------


async def _statement(code: str, legal_name: str = "") -> dict[str, Any]:
    statements = list(map_jar_lithuania(await _fetch(code, legal_name)))
    assert len(statements) == 1
    return statements[0]


def _status_note(statement: dict[str, Any]) -> str:
    notes = [a["description"] for a in statement.get("annotations", []) if a.get("motivation") == "commenting"]
    return " ".join(notes)


async def test_an_operating_company_maps_as_live(index) -> None:
    s = await _statement(IGNITIS)
    rd = s["recordDetails"]
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Akcinė bendrovė (public limited company)"}
    assert rd["foundingDate"] == "2008-08-28"
    assert "dissolutionDate" not in rd
    assert rd["identifiers"][0] == {
        "id": IGNITIS, "scheme": "LT-JAR",
        "schemeName": "Juridinių Asmenų Registras (Lithuanian Register of Legal Entities)",
    }
    assert rd["addresses"][0]["country"]["code"] == "LT"
    assert s["statementDate"] == CUT
    assert bods_liveness.read_register_status(s)["liveness"] == "live"


async def test_a_liquidation_maps_as_pending_with_its_date(index) -> None:
    s = await _statement(LIQUIDATING)
    assert bods_liveness.read_register_status(s)["liveness"] == "pending"
    assert "2025-03-04" in _status_note(s)
    assert "in liquidation" in _status_note(s)
    assert "dissolutionDate" not in s["recordDetails"]


async def test_an_unclear_status_writes_nothing(index) -> None:
    """Taking part in someone else's reorganisation does not say this entity
    ends — unknown, so no status annotation rather than a guess."""
    s = await _statement(PARTICIPANT)
    assert bods_liveness.read_register_status(s) is None


async def test_a_deregistered_company_maps_as_terminal_and_dissolved(index) -> None:
    s = await _statement(GONE)
    assert s["recordDetails"]["dissolutionDate"] == "2019-05-02"
    assert bods_liveness.read_register_status(s)["liveness"] == "terminal"


async def test_a_deregistration_before_registration_keeps_the_deregistration(index) -> None:
    """libcove: ``statement_entity_dissolution_before_founding_date``. The
    register does this for 273 entities; neither date is invented or lost."""
    s = await _statement(INVERTED)
    rd = s["recordDetails"]
    assert rd["dissolutionDate"] == "1992-09-17"
    assert "foundingDate" not in rd
    [note] = [a for a in s["annotations"] if a["motivation"] == "transformation"]
    assert note["statementPointerTarget"] == "/recordDetails/dissolutionDate"
    assert "1993-07-13" in note["description"] and "1992-09-17" in note["description"]


async def test_a_budget_institution_is_a_state_body_and_an_soe_is_not(index) -> None:
    assert (await _statement(BUDGET))["recordDetails"]["entityType"]["type"] == "stateBody"
    assert (await _statement(STATE_ENT))["recordDetails"]["entityType"]["type"] == "registeredEntity"


async def test_a_withheld_name_is_annotated_where_it_came_from(index) -> None:
    s = await _statement(SOLE, 'IĮ "4 BIZ"')
    assert s["recordDetails"]["name"] == 'IĮ "4 BIZ"'
    [note] = [a for a in s["annotations"] if a["motivation"] == "transformation"]
    assert note["statementPointerTarget"] == "/recordDetails/name"
    assert "GLEIF" in note["description"]
    # With no GLEIF name either, the form and code — and it says it is not a name.
    s = await _statement(GONE_SOLE)
    assert s["recordDetails"]["name"] == f"Individuali įmonė {GONE_SOLE}"
    assert any("not a name" in a["description"] for a in s["annotations"])


async def test_a_miss_maps_to_nothing(index) -> None:
    assert list(map_jar_lithuania(await _fetch(ABSENT))) == []


async def test_statements_pass_libcovebods(index) -> None:
    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import validate_bods_statements

    statements = []
    for code in (IGNITIS, SOLE, LIQUIDATING, PARTICIPANT, BUDGET, STATE_ENT, GONE, GONE_SOLE, INVERTED):
        statements.extend(map_jar_lithuania(await _fetch(code, "Name from GLEIF")))
    report = validate_bods_statements(statements)
    assert report["json_errors"] == []
    assert report["additional_errors"] == []


# ---------------------------------------------------------------------------
# The finding sentence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code,legal_name,expected",
    [
        (IGNITIS, "", "On the register since 2008-08-28, Akcinė bendrovė (public limited company)."),
        (LIQUIDATING, "", "In liquidation since 2025-03-04, Uždaroji akcinė bendrovė (private limited company)."),
        (GONE, "", "Deregistered on 2019-05-02, Uždaroji akcinė bendrovė (private limited company)."),
        (
            SOLE, "IĮ 4 BIZ",
            "On the register since 2016-12-01, Individuali įmonė (sole proprietorship), "
            "the open data publishes no name for this legal form.",
        ),
        (
            ABSENT, "",
            "Not in the Register of Legal Entities' open data as indexed, among "
            "either registered or deregistered entities.",
        ),
    ],
)
async def test_finding(index, code, legal_name, expected) -> None:
    sentence = finding_jar_lithuania(await _fetch(code, legal_name))
    assert sentence == expected
    assert len(sentence) <= MAX_FINDING_CHARS


# ---------------------------------------------------------------------------
# Boot download
# ---------------------------------------------------------------------------


def test_warm_index_without_a_url(no_index, monkeypatch) -> None:
    monkeypatch.setenv("JAR_LITHUANIA_DB_URL", "")
    get_settings.cache_clear()
    assert jar_lithuania.warm_index()["jar_lithuania"].startswith("absent")


def test_warm_index_never_raises(no_index, monkeypatch) -> None:
    import opencheck.entity_pages as entity_pages

    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(entity_pages, "download_db", boom)
    assert jar_lithuania.warm_index()["jar_lithuania"].startswith("failed")


def test_warm_index_downloads_to_the_path_the_reader_reads(no_index, monkeypatch, tmp_path: Path) -> None:
    """The ONRC lesson: downloader and reader once disagreed about the path
    and a good index sat unread. One ``db_path()`` serves both."""
    import opencheck.entity_pages as entity_pages

    built, _ = _build(tmp_path, name="asset.sqlite")
    written: list[Path] = []

    def fake_download(url, path, *, last_modified=None):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(built.read_bytes())
        written.append(Path(path))
        return built.stat().st_size, 0.1

    monkeypatch.setattr(entity_pages, "download_db", fake_download)
    outcome = jar_lithuania.warm_index()["jar_lithuania"]
    assert outcome.startswith("downloaded")
    assert written == [jar_lithuania.db_path()]
    assert jar_lithuania.index_available() is True
