"""The Slovenia wiring (12 Nov 2026) — the Slovenian Business Register (AJPES
PRS), read from an index of the open CSV on podatki.gov.si.

Every index here is produced by the **real builder**
(``scripts/build_si_prs_index.py``) from a file in the published shape —
UTF-16LE with a BOM, CRLF, every field quoted, the header's stray spaces —
and read back through the **real adapter** (the JAR / ONRC rule). The names
and addresses of natural persons below are invented.
"""

from __future__ import annotations

import ast
import codecs
import importlib.util
import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.mapper import map_si_prs
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_si_prs
from opencheck.routers.lookup import _build_derived, _build_result_hit, _dispatch, _LookupCtx
from opencheck.sources import REGISTRY, si_prs
from opencheck.sources.base import SearchKind
from opencheck.sources.si_prs import (
    SI_PRS_RA_CODE,
    form_is_legal_person,
    form_label,
    normalise_maticna,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_si_prs_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_si_prs_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-11-01"
KRKA = "5043611000"     # public limited company, court-registered
DOO = "6123456000"      # private limited company, AJPES-registered
SP = "7000001000"       # sole trader: street dropped, out of search
ROOM = "7000002000"     # registered room renter: a natural person too
ODD = "7000003000"      # a form nobody foresaw: read as a natural person
ABSENT = "5319200023"   # a fund's number GLEIF files: not in the file

#: The published header, stray spaces included.
RAW_HEADER = [
    "Matična številka", "Popolno ime", "HSEID", "Pravnoorganizacijska oblika",
    "Registrski organ", "Ulica", "Hišna št ", "Hišna št  dodatek", "Naselje",
    "Poštna št ", "Pošta", "Država",
]


def _row(number: str, name: str, form: str, organ: str, street: str = "Glavna ulica",
         house: str = "1", suffix: str = "", settlement: str = "Ljubljana",
         postcode: str = "1000", post: str = "Ljubljana") -> list[str]:
    return [number, name, "12345678", form, organ, street, house, suffix, settlement,
            postcode, post, "SLOVENIJA"]


def _rows() -> list[list[str]]:
    return [
        _row(KRKA, "KRKA, tovarna zdravil, d.d., Novo mesto", "Delniška družba d.d.",
             "Okrožno sodišče v Novem mestu", "Šmarješka cesta", "006", "",
             "Novo mesto", "8501", "Novo mesto"),
        _row(DOO, "PRIMER d.o.o.", "Družba z omejeno odgovornostjo d.o.o.",
             "AJPES, izpostava Ljubljana", "Dunajska cesta", "5", "A"),
        _row(SP, "JANEZ NOVAK S.P.", "Samostojni podjetnik posameznik s.p.",
             "AJPES, izpostava Celje", "Domača ulica", "12", "", "Šentjur", "3230", "Šentjur"),
        _row(ROOM, "MARIJA KOVAČ", "Registrirani sobodajalec", "AJPES, izpostava Koper",
             "Morska pot", "3", "", "Piran", "6330", "Piran"),
        _row(ODD, "NEKAJ NOVEGA", "Nova oblika", "AJPES, izpostava Kranj", "Skrita ulica", "9"),
        # malformed: a nine-digit number, a short row, and a repeat
        _row("504361100", "SHORT NUMBER", "Delniška družba d.d.", "x"),
        [DOO, "SHORT ROW"],
        _row(DOO, "PRIMER d.o.o. (again)", "Družba z omejeno odgovornostjo d.o.o.", "x"),
    ]


def _write_csv(path: Path, rows: list[list[str]], *, header: list[str] | None = None) -> Path:
    def line(cells: list[str]) -> str:
        return ",".join('"' + c.replace('"', '""') + '"' for c in cells) + "\r\n"

    text = line(header or RAW_HEADER) + "".join(line(r) for r in rows)
    path.write_bytes(codecs.BOM_UTF16_LE + text.encode("utf-16-le"))
    return path


def _build(tmp_path: Path, rows: list[list[str]] | None = None, cut: str = CUT) -> tuple[Path, dict]:
    csv_path = _write_csv(tmp_path / "opsiprs.csv", _rows() if rows is None else rows)
    out = tmp_path / "data" / "si_prs.sqlite"
    out.parent.mkdir(exist_ok=True)
    stats = builder.build(builder.read_rows(csv_path), out, cut, min_rows=0)
    return out, stats


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("SI_PRS_DB_FILE", None)
    else:
        os.environ["SI_PRS_DB_FILE"] = str(path)
    get_settings.cache_clear()
    si_prs.reset_connection()


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
    return await REGISTRY["si_prs"].fetch(number, legal_name=legal_name)


async def _map(number: str) -> list[dict[str, Any]]:
    with provenance.recording():
        bundle = await _fetch(number)
        return list(map_si_prs(bundle))


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_reads_the_utf16_file_and_records_its_cut(index):
    out, stats = index
    assert stats["rows"] == 5 and stats["malformed"] == 2 and stats["repeated"] == 1
    assert stats["legal_persons"] == 2 and stats["natural_persons"] == 3
    meta = dict(sqlite3.connect(out).execute("SELECT key, value FROM meta"))
    assert meta["cut"] == CUT and meta["schema_version"] == si_prs.INDEX_SCHEMA_VERSION
    assert meta["built_at"].endswith("Z") and "T" in meta["built_at"]


def test_a_natural_persons_street_and_the_house_id_never_reach_the_index(index):
    out, _ = index
    conn = sqlite3.connect(out)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(entity)")}
    assert "hseid" not in {c.lower() for c in columns}
    dump = "\n".join(conn.iterdump())
    for street in ("Domača ulica", "Morska pot", "Skrita ulica"):
        assert street not in dump
    assert "12345678" not in dump, "HSEID is never stored"
    assert "Dunajska cesta" in dump, "a company keeps its registered street"
    row = conn.execute("SELECT settlement, postcode FROM entity WHERE number = ?", (SP,)).fetchone()
    assert row == ("Šentjur", "3230")


def test_the_builder_refuses_an_error_page_a_changed_header_and_a_short_file(tmp_path):
    page = tmp_path / "opsiprs.csv"
    page.write_bytes(b"<html><title>407 Proxy Authentication Required</title></html>")
    with pytest.raises(builder.RefusedInput, match="not UTF-16"):
        list(builder.read_rows(page))
    changed = _write_csv(tmp_path / "changed.csv", _rows(), header=RAW_HEADER[:-1] + ["Drzava"])
    with pytest.raises(builder.RefusedInput, match="header"):
        list(builder.read_rows(changed))
    out = tmp_path / "x.sqlite"
    csv_path = _write_csv(tmp_path / "ok.csv", _rows())
    with pytest.raises(builder.RefusedInput, match="not the register"):
        builder.build(builder.read_rows(csv_path), out, CUT)
    with pytest.raises(builder.RefusedInput, match="YYYY-MM-DD"):
        builder.build(builder.read_rows(csv_path), out, "01.11.2026", min_rows=0)
    assert not out.exists() and not out.with_name("x.sqlite.building").exists()


def test_the_builders_copies_match_the_adapter():
    """The builder runs under a bare python3, so it copies three things."""
    assert builder.INDEX_SCHEMA_VERSION == si_prs.INDEX_SCHEMA_VERSION
    assert builder.CSV_URL == si_prs.CSV_URL
    assert builder._NATURAL_PERSON_MARKERS.pattern == si_prs._NATURAL_PERSON_MARKERS.pattern
    assert builder._LEGAL_PERSON_MARKERS.pattern == si_prs._LEGAL_PERSON_MARKERS.pattern
    imports = {
        n.module for n in ast.walk(ast.parse(_SCRIPT.read_text())) if isinstance(n, ast.ImportFrom)
    }
    assert not any((m or "").startswith("opencheck") for m in imports)


# ---------------------------------------------------------------------------
# Numbers and forms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("raw", "want"), [
    ("5043611000", KRKA), (" 5043611000 ", KRKA), ("5043611", KRKA), ("504361100", ""), ("", ""),
])
def test_a_number_is_ten_digits(raw, want):
    assert normalise_maticna(raw) == want


@pytest.mark.parametrize(("form", "legal", "gloss"), [
    ("Samostojni podjetnik posameznik s.p.", False, "sole trader"),
    ("Registrirani sobodajalec", False, "registered room renter"),
    ("Nosilec dopolnilne dejavnosti na kmetiji", False, None),
    ("Družba z omejeno odgovornostjo d.o.o.", True, "private limited company"),
    ("Delniška družba d.d.", True, "public limited company"),
    ("Komanditna delniška družba k.d.d.", True, "partnership limited by shares"),
    ("Komanditna družba k.d.", True, "limited partnership"),
    ("Družba z neomejeno odgovornostjo d.n.o.", True, "general partnership"),
    ("Kmetijska zadruga z.o.o.", True, "cooperative"),
    ("Javni zavod", True, "institute"),
    ("Nova oblika", False, None),
])
def test_forms(form, legal, gloss):
    assert form_is_legal_person(form) is legal
    assert form_label(form) == gloss


def test_the_adapter_declares_its_lookup_wiring():
    adapter = REGISTRY["si_prs"]
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({SI_PRS_RA_CODE})
    assert deriver.derived_key == "si_maticna"
    assert adapter.lookup_pass_legal_name is True
    assert adapter.info.country == "SI" and adapter.info.is_national_register


# ---------------------------------------------------------------------------
# The adapter over the index
# ---------------------------------------------------------------------------


async def test_a_unit_in_the_index_is_a_dated_snapshot(index):
    with provenance.recording() as rec:
        bundle = await _fetch(KRKA, legal_name="Krka")
    assert bundle["is_stub"] is False and not bundle.get("not_found")
    assert bundle["name"] == "KRKA, tovarna zdravil, d.d., Novo mesto"
    assert bundle["legal_person"] is True
    assert bundle["address"]["street"] == "Šmarješka cesta"
    assert bundle["cut"] == CUT
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of.date().isoformat() == CUT
    assert resolved.retrieved_at is not None


async def test_a_sole_trader_joins_without_a_street(index):
    bundle = await _fetch(SP)
    assert bundle["legal_person"] is False
    assert "street" not in bundle["address"] and "house" not in bundle["address"]
    assert bundle["address"]["settlement"] == "Šentjur"


async def test_a_number_outside_the_index_is_a_note_card_not_a_degradation(index):
    from opencheck import degradation

    with degradation.recording() as degraded:
        bundle = await _fetch(ABSENT, legal_name="A Fund")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert bundle["coverage_note"] == si_prs.COVERAGE_NOT_IN_INDEX
    assert list(degraded) == []
    assert list(map_si_prs(bundle)) == []


async def test_a_malformed_number_is_a_stub(index):
    assert (await _fetch("12345"))["is_stub"] is True


async def test_without_an_index_nothing_is_announced(no_index):
    adapter = REGISTRY["si_prs"]
    assert adapter.covers_lei("549300H9RLRTRTLRUZ73") is False
    assert (await _fetch(KRKA))["is_stub"] is True
    assert await adapter.search("krka", SearchKind.ENTITY) == []


async def test_a_wrong_schema_version_is_no_index(tmp_path):
    out, _ = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert si_prs.index_available() is False
    finally:
        _point_at(None)


async def test_name_search_covers_legal_persons_only(index):
    adapter = REGISTRY["si_prs"]
    hits = await adapter.search("krka", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [KRKA]
    assert hits[0].identifiers == {"si_maticna": KRKA}
    assert await adapter.search("novak", SearchKind.ENTITY) == []
    assert await adapter.search("kovač", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# The mapper
# ---------------------------------------------------------------------------


async def test_the_mapper_draws_one_entity_on_the_scheme_gleif_uses(index):
    (stmt,) = await _map(KRKA)
    rd = stmt["recordDetails"]
    assert rd["identifiers"] == [{
        "id": KRKA, "scheme": "SI-PRS",
        "schemeName": "Slovenian Business Register (Poslovni register Slovenije, AJPES)",
    }]
    from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID

    assert _GLEIF_RA_TO_ORG_ID[SI_PRS_RA_CODE][0] == "SI-PRS", "GLEIF and the register corroborate"
    assert rd["jurisdiction"] == {"name": "Slovenia", "code": "SI"}
    assert "foundingDate" not in rd and "dissolutionDate" not in rd
    assert rd["entityType"] == {
        "type": "registeredEntity", "details": "Public limited company (Delniška družba d.d.)",
    }
    (address,) = rd["addresses"]
    assert address["address"] == "Šmarješka cesta 006, 8501 Novo mesto"
    assert address["country"]["code"] == "SI"
    assert stmt["source"]["type"] == ["officialRegister"]
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "Okrožno sodišče v Novem mestu" in notes
    assert "no status" in notes


async def test_a_sole_traders_address_is_its_settlement_and_says_why(index):
    (stmt,) = await _map(SP)
    (address,) = stmt["recordDetails"]["addresses"]
    assert address["address"] == "3230 Šentjur"
    assert "Sole trader" in stmt["recordDetails"]["entityType"]["details"]
    targets = {a["statementPointerTarget"] for a in stmt.get("annotations", [])}
    assert "/recordDetails/addresses" in targets


async def test_an_ajpes_branch_is_named_in_english_and_slovene(index):
    (stmt,) = await _map(DOO)
    notes = " ".join(a["description"] for a in stmt.get("annotations", []))
    assert "AJPES's Ljubljana branch office (AJPES, izpostava Ljubljana)" in notes
    (address,) = stmt["recordDetails"]["addresses"]
    assert address["address"] == "Dunajska cesta 5 A, 1000 Ljubljana"


def test_a_note_card_and_a_stub_map_to_nothing():
    assert list(map_si_prs({"is_stub": True, "number": KRKA})) == []
    assert list(map_si_prs({"is_stub": False, "not_found": True, "number": KRKA})) == []


# ---------------------------------------------------------------------------
# The finding and the hit
# ---------------------------------------------------------------------------


async def test_the_finding_sentences(index):
    krka = finding_si_prs(await _fetch(KRKA))
    assert krka.startswith("On the register, public limited company, no status published")
    assert "active" not in krka.lower()
    absent = finding_si_prs(await _fetch(ABSENT))
    assert absent.startswith("Not in the Slovenian Business Register")
    for number in (KRKA, DOO, SP, ROOM, ODD):
        assert len(finding_si_prs(await _fetch(number)) or "") <= MAX_FINDING_CHARS


async def test_the_hit_builder_asserts_the_number_only_when_the_register_published_it(index):
    ctx = _LookupCtx(lei="549300H9RLRTRTLRUZ73", legal_name="Krka", jurisdiction="SI", registered_as=KRKA)
    _build_derived(ctx, SI_PRS_RA_CODE)
    hit = _build_result_hit("si_prs", await _fetch(KRKA), ctx)
    assert hit is not None
    assert hit.identifiers == {"si_maticna": KRKA} and hit.summary == f"SI {KRKA}"
    ghost = _build_result_hit("si_prs", await _fetch(ABSENT, "Ghost"), ctx)
    assert ghost is not None and ghost.identifiers == {}


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="si_prs")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_only_when_the_index_is_present(index):
    ctx = _LookupCtx(lei="549300H9RLRTRTLRUZ73", jurisdiction="SI", registered_as=KRKA)
    _build_derived(ctx, SI_PRS_RA_CODE)
    assert ctx.derived.get("si_maticna") == KRKA
    assert _dispatched(ctx) == {"si_prs"}


def test_not_dispatched_without_an_index(no_index):
    ctx = _LookupCtx(lei="549300H9RLRTRTLRUZ73", jurisdiction="SI", registered_as=KRKA)
    _build_derived(ctx, SI_PRS_RA_CODE)
    assert _dispatched(ctx) == set()
