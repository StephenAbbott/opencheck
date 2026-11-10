"""The Israel wiring (10 Nov 2026) — the Corporations Authority's registers of
companies and partnerships, read from an index of data.gov.il's datastore.

Every index here is produced by the **real builder**
(``scripts/build_ica_israel_index.py``) from records in the datastore's exact
published shape (Hebrew field names, ``~`` for the Hebrew quotation mark,
day-first dates, codes as numbers, a postcode as a number) and read back
through the **real adapter** — the JAR / ONRC rule.
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.mapper import map_ica_israel
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_ica_israel
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY, ica_israel
from opencheck.sources.base import SearchKind
from opencheck.sources.ica_israel import IL_ICA_RA_CODE, normalise_il_number, status_class

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_ica_israel_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_ica_israel_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-11-09"
PAYPAL = "516478872"   # live private company, English name, report 2026
BEZEQ = "520031931"    # live public company, report year 2002 (not said)
STRUCK = "510000045"   # struck off, no address
VIOLATING = "513000001"  # live, violating, strike-off proceedings
FOREIGN = "560000001"  # foreign private company, Cayman address
GOV = "520000002"      # government company
PARTNER = "540000001"  # Israeli limited partnership
ABSENT = "580000000"   # an association's number: on another register


def _company(number: str, name: str, **over: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "_id": 1, "מספר חברה": int(number), "שם חברה": name, "שם באנגלית": "",
        "סוג תאגיד": "ישראלית חברה פרטית", "סטטוס חברה": "פעילה", "תאור חברה": "",
        "מטרת החברה": "לעסוק בכל עיסוק חוקי", "תאריך התאגדות": "18/10/2021",
        "חברה ממשלתית": "לא", "מגבלות": "מוגבלת", "מפרה": "",
        "שנה אחרונה של דוח שנתי (שהוגש)": None, "שם עיר": "", "שם רחוב": "",
        "מספר בית": "", "מיקוד": None, "ת.ד.": "", "מדינה": "",
        "אצל": "ישראל ישראלי", "תת סטטוס": "", "קוד סטטוס חברה": 0,
        "קוד סוג חברה": 1, "קוד סיווג חברה": 51, "קוד מטרת החברה": 1,
        "קוד מגבלה": 1, "קוד חברה מפרה": None, "קוד ישוב": None, "קוד רחוב": None,
        "קוד מדינה": None,
    }
    rec.update(over)
    return rec


def _companies() -> list[dict[str, Any]]:
    return [
        _company(
            PAYPAL, "פייפאל ישראל שירותי תשלום  בע~מ",
            **{"שם באנגלית": "PAYPAL ISRAEL PAYMENT SERVICES  LTD",
               "שנה אחרונה של דוח שנתי (שהוגש)": 2026, "שם עיר": "תל אביב - יפו",
               "שם רחוב": "אלון יגאל", "מספר בית": "98", "מיקוד": 6789141,
               "מדינה": "ישראל"},
        ),
        _company(
            BEZEQ, "בזק החברה הישראלית לתקשורת בע~מ",
            **{"סוג תאגיד": "ישראלית חברה ציבורית", "תאריך התאגדות": "21/08/1980",
               "שנה אחרונה של דוח שנתי (שהוגש)": 2002},
        ),
        _company(
            STRUCK, "פלנת טרוסט אנד אגנסו בעמ",
            **{"סטטוס חברה": "מחוקה", "קוד סטטוס חברה": 19, "תאריך התאגדות": "01/05/1935"},
        ),
        _company(
            VIOLATING, "מפירה בע~מ",
            **{"מפרה": "מפרה", "קוד חברה מפרה": 18, "תת סטטוס": "בהליכי מחיקה",
               "שנה אחרונה של דוח שנתי (שהוגש)": 2019},
        ),
        _company(
            FOREIGN, "OFFSHORE HOLDINGS LIMITED",
            **{"סוג תאגיד": "חוץ חברה פרטית", "קוד סוג חברה": 2, "מדינה": "איי קיימן",
               "שם רחוב": "PO BOX 309 UGLAND HOUSE"},
        ),
        _company(GOV, "חברת ממשלה בע~מ", **{"חברה ממשלתית": "כן"}),
        # malformed: an eight-digit number
        _company("51000004", "שבור"),
    ]


def _partnerships() -> list[dict[str, Any]]:
    return [
        {
            "_id": 1, "מספר שותפות": int(PARTNER), "שם שותפות": "קרן השקעות (שותפות מוגבלת)",
            "שם באנגלית": "INVESTMENT FUND LP", "סוג תאגיד": "ישראלית מוגבלת",
            "סטטוס תאגיד": "פעילה", "תאריך התאגדות": "02/03/2015", "ישוב": "הרצליה",
            "רחוב": "המנופים", "מספר בית": "8", "מיקוד": 4672553, "ת.ד": "", "מדינה": "ישראל",
            "אצל": "", "קוד סטטוס שותפות": 0, "קוד סוג חברה": 1, "קוד סיווג חברה": 1,
            "קוד ישוב": None, "קוד רחוב": "", "קוד מדינה": None,
        }
    ]


def _build(tmp_path: Path, *, companies=None, partnerships=None, cut: str = CUT) -> tuple[Path, dict]:
    out = tmp_path / "data" / "ica_israel.sqlite"
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(
        {
            "company": _companies() if companies is None else companies,
            "partnership": _partnerships() if partnerships is None else partnerships,
        },
        out, cut, min_rows={},
    )
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("ICA_ISRAEL_DB_FILE", None)
    else:
        os.environ["ICA_ISRAEL_DB_FILE"] = str(path)
    get_settings.cache_clear()
    ica_israel.reset_connection()


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


async def _fetch(number: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["ica_israel"].fetch(number, legal_name=legal_name)


async def _map(number: str) -> list[dict[str, Any]]:
    with provenance.recording():
        bundle = await _fetch(number)
        return list(map_ica_israel(bundle))


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_the_datastore_shape_and_records_its_cut(index):
    out, stats = index
    assert stats["company_rows"] == 6 and stats["company_malformed"] == 1
    assert stats["partnership_rows"] == 1
    conn = sqlite3.connect(out)
    meta = dict(conn.execute("SELECT key, value FROM meta"))
    assert meta["cut"] == CUT and meta["schema_version"] == ica_israel.INDEX_SCHEMA_VERSION
    assert meta["built_at"].endswith("Z") and "T" in meta["built_at"]


def test_the_builder_writes_the_quotation_mark_and_drops_the_care_of_field(index):
    out, _ = index
    conn = sqlite3.connect(out)
    name, name_en, postcode = conn.execute(
        "SELECT name_he, name_en, postcode FROM entity WHERE number = ?", (PAYPAL,)
    ).fetchone()
    assert name == 'פייפאל ישראל שירותי תשלום בע"מ', "~ is the Hebrew quotation mark"
    assert name_en == "PAYPAL ISRAEL PAYMENT SERVICES LTD"
    assert postcode == "6789141"
    columns = {row[1] for row in conn.execute("PRAGMA table_info(entity)")}
    assert not columns & {"care_of", "purpose", "description"}
    dump = "\n".join(conn.iterdump())
    assert "ישראל ישראלי" not in dump, "the care-of name never reaches the index"


def test_the_builder_refuses_a_changed_field_set(tmp_path):
    rows = _companies()
    for row in rows:
        row.pop("מפרה")
    with pytest.raises(builder.RefusedInput, match="fields missing"):
        _build(tmp_path, companies=rows)
    assert not (tmp_path / "data" / "ica_israel.sqlite").exists()
    assert not (tmp_path / "data" / "ica_israel.sqlite.building").exists()


def test_the_builder_refuses_a_short_harvest_and_a_bad_cut(tmp_path):
    out = tmp_path / "x.sqlite"
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build({"company": _companies(), "partnership": _partnerships()}, out, CUT)
    with pytest.raises(builder.RefusedInput, match="YYYY-MM-DD"):
        builder.build({"company": [], "partnership": []}, out, "10/11/2026", min_rows={})
    assert not out.exists()


def test_a_harvest_that_fails_midway_leaves_nothing_behind(tmp_path):
    def broken():
        yield _companies()[0]
        raise builder.RefusedInput("read 1 rows, the API reported 733,463")

    out = tmp_path / "x.sqlite"
    with pytest.raises(builder.RefusedInput, match="API reported"):
        builder.build({"company": broken(), "partnership": []}, out, CUT, min_rows={})
    assert not out.exists() and not out.with_name("x.sqlite.building").exists()


def test_the_harvest_refuses_a_short_read(monkeypatch):
    pages = iter([{"total": 5, "records": [{"_id": 1}, {"_id": 2}]}])
    monkeypatch.setattr(builder, "_get", lambda action, params: next(pages))
    with pytest.raises(builder.RefusedInput, match="read 2 rows"):
        list(builder.harvest("res"))


# ---------------------------------------------------------------------------
# Numbers and statuses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["516478872", "51-647887-2", " 516478872 "])
def test_every_spelling_of_a_number_is_the_digits(raw):
    assert normalise_il_number(raw) == PAYPAL


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["ica_israel"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({IL_ICA_RA_CODE})
    assert deriver.derived_key == "il_roc"
    assert adapter.lookup_pass_legal_name is True
    assert adapter.info.country == "IL" and adapter.info.is_national_register


@pytest.mark.parametrize(
    ("kind", "code", "sub", "expected"),
    [
        ("company", 0, "", "live"),
        ("company", 0, "בהליכי מחיקה", "pending"),
        ("company", 15, "", "pending"),
        ("company", 19, "", "terminal"),
        ("company", 35, "", "terminal"),
        ("partnership", 80, "", "terminal"),
        ("partnership", 59, "", "terminal"),
        ("company", 999, "", "unknown"),
        ("company", None, "", "unknown"),
    ],
)
def test_status_classes(kind, code, sub, expected):
    assert status_class(kind, code, sub) == expected


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


async def test_a_company_in_the_index_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch(PAYPAL, legal_name="PayPal")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["name"] == 'פייפאל ישראל שירותי תשלום בע"מ'
    assert bundle["kind"] == "company" and bundle["status_class"] == "live"
    assert bundle["incorporated"] == "2021-10-18"
    assert bundle["government"] is False and bundle["last_report_year"] == 2026
    assert bundle["address"]["city"] == "תל אביב - יפו"
    assert bundle["cut"] == CUT
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT
    assert resolved.retrieved_at is not None


async def test_a_partnership_is_read_from_the_same_index(index):
    bundle = await _fetch(PARTNER)
    assert bundle["kind"] == "partnership"
    assert bundle["link"] == ica_israel.PARTNERSHIPS_DATASET_URL
    assert bundle["violating"] is None and bundle["last_report_year"] is None


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    from opencheck import degradation

    with degradation.recording() as degraded:
        bundle = await _fetch(ABSENT, legal_name="An Association")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == ica_israel.COVERAGE_NOT_IN_INDEX
    assert list(degraded) == []
    assert list(map_ica_israel(bundle)) == []


async def test_a_malformed_number_is_a_stub(index):
    bundle = await _fetch("12345")
    assert bundle["is_stub"] is True


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["ica_israel"]
    assert adapter.covers_lei("25490039WF5R6PKDL402") is False
    assert (await _fetch(PAYPAL))["is_stub"] is True
    assert await adapter.search("פייפאל", SearchKind.ENTITY) == []


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    out, _ = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ica_israel.index_available() is False
    finally:
        _point_at(None)


async def test_name_search_ranks_an_exact_name_first(index):
    hits = await REGISTRY["ica_israel"].search("investment fund lp", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [PARTNER]
    hits = await REGISTRY["ica_israel"].search('בע"מ', SearchKind.ENTITY)
    assert hits and all(h.identifiers == {"il_roc": h.hit_id} for h in hits)
    hits = await REGISTRY["ica_israel"].search("בע~מ", SearchKind.ENTITY)
    assert hits, "a query spelled with the file's ~ still finds the name"


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_one_entity_on_the_scheme_gleif_uses(index):
    (stmt,) = await _map(PAYPAL)
    rd = stmt["recordDetails"]
    assert rd["identifiers"] == [{
        "id": PAYPAL, "scheme": "IL-ROC",
        "schemeName": "Registrar of Companies (Israel Corporations Authority)",
    }]
    from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID

    assert _GLEIF_RA_TO_ORG_ID[IL_ICA_RA_CODE][0] == "IL-ROC", "GLEIF and the register corroborate"
    assert rd["jurisdiction"] == {"name": "Israel", "code": "IL"}
    assert rd["alternateNames"] == ["PAYPAL ISRAEL PAYMENT SERVICES LTD"]
    assert rd["foundingDate"] == "2021-10-18"
    assert rd["entityType"] == {"type": "registeredEntity", "details": "Israeli private company (ישראלית חברה פרטית)"}
    (address,) = rd["addresses"]
    assert address["country"]["code"] == "IL"
    assert stmt["source"]["type"] == ["officialRegister"]
    notes = [a["description"] for a in stmt.get("annotations", [])]
    assert any("2026" in n for n in notes)


async def test_a_struck_off_company_is_terminal_with_no_invented_date(index):
    (stmt,) = await _map(STRUCK)
    assert "dissolutionDate" not in stmt["recordDetails"]
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "מחוקה" in notes and "struck off" in notes


async def test_the_violating_mark_is_a_note_never_a_signal(index):
    (stmt,) = await _map(VIOLATING)
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "violating company" in notes and "362A" in notes
    assert "strike-off proceedings under way" in notes
    bundle = await _fetch(VIOLATING)
    finding = finding_ica_israel(bundle)
    assert finding and "violating company" in finding
    import ast

    for module in ("risk", "verdict"):
        src = (Path(__file__).resolve().parents[1] / "opencheck" / f"{module}.py").read_text()
        names = {
            n.module for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ImportFrom) and n.module
        }
        assert not any("ica_israel" in name for name in names)


async def test_a_foreign_company_has_no_jurisdiction_and_says_why(index):
    (stmt,) = await _map(FOREIGN)
    rd = stmt["recordDetails"]
    assert "jurisdiction" not in rd
    (address,) = rd["addresses"]
    assert address["country"]["code"] == "KY"
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "home jurisdiction" in notes


async def test_a_government_company_stays_a_registered_entity(index):
    (stmt,) = await _map(GOV)
    et = stmt["recordDetails"]["entityType"]
    assert et["type"] == "registeredEntity"
    assert "government company" in et["details"]


async def test_a_partnership_maps_with_its_form(index):
    (stmt,) = await _map(PARTNER)
    assert "Israeli limited partnership" in stmt["recordDetails"]["entityType"]["details"]
    assert stmt["source"]["url"] == ica_israel.PARTNERSHIPS_DATASET_URL


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_ica_israel({"is_stub": True, "number": PAYPAL})) == []
    assert list(map_ica_israel({"is_stub": False, "not_found": True, "number": PAYPAL})) == []


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


async def test_the_finding_sentences(index):
    paypal = finding_ica_israel(await _fetch(PAYPAL))
    assert paypal == "Registered since 2021-10-18, Israeli private company, last annual report filed for 2026."
    bezeq = finding_ica_israel(await _fetch(BEZEQ))
    assert "2002" not in bezeq, "a public company's register year is not said"
    struck = finding_ica_israel(await _fetch(STRUCK))
    assert struck.startswith("Struck off")
    absent = finding_ica_israel(await _fetch(ABSENT))
    assert absent.startswith("Not in the Corporations Authority")
    for number in (PAYPAL, BEZEQ, STRUCK, VIOLATING, FOREIGN, GOV, PARTNER):
        assert len(finding_ica_israel(await _fetch(number)) or "") <= MAX_FINDING_CHARS


async def test_the_hit_builder_asserts_the_number_only_when_the_register_published_it(index):
    ctx = _LookupCtx(lei="25490039WF5R6PKDL402", legal_name="PayPal", jurisdiction="IL", registered_as=PAYPAL)
    _build_derived(ctx, IL_ICA_RA_CODE)
    hit = _build_result_hit("ica_israel", await _fetch(PAYPAL), ctx)
    assert hit is not None
    assert hit.identifiers == {"il_roc": PAYPAL} and hit.summary == f"IL {PAYPAL}"
    ctx = _LookupCtx(lei="25490039WF5R6PKDL402", legal_name="Old", jurisdiction="IL", registered_as=STRUCK)
    _build_derived(ctx, IL_ICA_RA_CODE)
    struck = _build_result_hit("ica_israel", await _fetch(STRUCK), ctx)
    assert struck is not None and struck.summary == f"IL {STRUCK} · struck off"
    ghost = _build_result_hit("ica_israel", await _fetch(ABSENT, "Ghost"), ctx)
    assert ghost is not None and ghost.identifiers == {}


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="ica_israel")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="25490039WF5R6PKDL402", jurisdiction="IL", registered_as=PAYPAL)
    _build_derived(ctx, IL_ICA_RA_CODE)
    assert ctx.derived.get("il_roc") == PAYPAL
    assert _dispatched(ctx) == {"ica_israel"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="25490039WF5R6PKDL402", jurisdiction="IL", registered_as=PAYPAL)
    _build_derived(ctx, IL_ICA_RA_CODE)
    assert _dispatched(ctx) == set()
