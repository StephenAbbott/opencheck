"""Moldova — ASP State Register of Legal Entities (weekly dataset.gov.md export).

The fixture workbook is written with openpyxl in the shape of the real
14 September 2026 export: a ``=CONCATENATE`` title row carrying the snapshot
date, the header on row 2, IDNOs as number cells, dates as Excel serials, and
every text cell a shared string. The traps it pins are the ones the real file
contains: liquidated rows and sole traders to leave out, a founder filed with
its IDNO and address in the name, a stake filed against two people, a group
placeholder instead of a founder, and a corporate founder that can only be
identified by its name.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from opencheck import degradation, provenance
from opencheck.bods import map_asp_moldova
from opencheck.bods.annotations import resolve_pointer
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_asp_moldova
from opencheck.licensing import classify
from opencheck.routers.hit_builders import _bh_asp_moldova, _LookupCtx
from opencheck.sources import asp_moldova
from opencheck.sources.asp_moldova import (
    COVERAGE_NOT_IN_EXPORT,
    MD_RA_CODES,
    AspMoldovaAdapter,
    build_index,
    fold,
    is_entity_name,
    latest_resource,
    name_key,
    normalise_idno,
    parse_directors,
    parse_founders,
    resolve_founder,
)

openpyxl = pytest.importorskip("openpyxl")


def _idno(first_twelve: str) -> str:
    return first_twelve + asp_moldova.idno_check_digit(first_twelve)


KAUFLAND = "1016600004811"
GRAWE_CD = "1005600012810"
CARATEST = "1008603003829"
BANK = "1002600003778"
LIQUIDATING = _idno("100460001398")
STATE_ENT = _idno("100360202353")
JOINT = _idno("101060001482")
LIQUIDATED = "1017600018068"
SOLE_TRADER = _idno("101760500397")
TWIN_A = _idno("102460001111")
TWIN_B = _idno("102460002222")
USES_TWIN = _idno("102460003333")

# ---------------------------------------------------------------------------
# Identifier grammar and names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [KAUFLAND, f" {BANK} ", "1003600062730"])
def test_normalise_idno_accepts_real_idnos(raw: str) -> None:
    assert normalise_idno(raw) == raw.strip()


@pytest.mark.parametrize(
    "raw",
    [
        "16479",            # what GLEIF files for Mogo Loans SRL — not an IDNO
        "1016600004812",    # Kaufland with the check digit off by one
        "10166000048110",   # fourteen digits
        "",
    ],
)
def test_normalise_idno_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_idno(raw)


def test_ra_codes_cover_every_moldovan_authority() -> None:
    assert MD_RA_CODES == {"RA000451", "RA000950", "RA000951"}


def test_fold_treats_cedilla_and_comma_below_alike() -> None:
    assert fold("gospodăria ţărănească") == fold("GOSPODĂRIA ȚĂRĂNEASCĂ") == "GOSPODARIA TARANEASCA"


def test_name_key_drops_form_words_and_keeps_order() -> None:
    a = name_key('"GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL" S.R.L.')
    b = name_key('Societatea cu Răspundere Limitată "GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL"')
    assert a == b == "GRAWE CONSULTING DEVELOPMENT INTERNATIONAL"
    assert name_key("ALFA BETA SRL") != name_key("BETA ALFA SRL")


@pytest.mark.parametrize(
    "name,entity",
    [
        ('"MAN IMOBIL GRUP" S.R.L.', True),
        ("KAUFLAND ROMANIA SCS", True),
        ("CONSILIUL MUNICIPAL CHIȘINĂU", True),
        ("SOCIETATE CU RĂSPUNDERE LIMITATĂ SIGMA VENMER", True),
        ("FEDOROV BORIS", False),
        # Apostrophes and a surname that is also a form abbreviation are the
        # false positives an earlier marker set produced on the real file.
        ("D'ALONZO FRANCO", False),
        ("SAS IOAN-VASILE", False),
    ],
)
def test_is_entity_name(name: str, entity: bool) -> None:
    assert is_entity_name(name) is entity


# ---------------------------------------------------------------------------
# Directors and founders as the export writes them
# ---------------------------------------------------------------------------


def test_parse_directors_types_each_role() -> None:
    rows = parse_directors(
        "POPA ION [Administrator], ZAK DAVID [Comitet], "
        "MOLDAUDITING [Datele lipsesc], RUSU ANA [Lichidator], X Y [Rol nou]"
    )
    assert [(r["name"], r["interest_type"]) for r in rows] == [
        ("POPA ION", "seniorManagingOfficial"),
        ("ZAK DAVID", "boardMember"),
        ("MOLDAUDITING", "unknownInterest"),
        ("RUSU ANA", "controlByLegalFramework"),
        # A role the table does not know stays unknown rather than defaulting
        # to management.
        ("X Y", "unknownInterest"),
    ]


def test_parse_founders_shares_use_a_decimal_comma() -> None:
    rows = parse_founders("FEDOROV BORIS (40,00%), FEDOROVA NADEJDA (30,50%), \"TENCOM\" S.A. (29,50%)")
    assert [(r["name"], r["kind"], r["share_pct"]) for r in rows] == [
        ("FEDOROV BORIS", "person", 40.0),
        ("FEDOROVA NADEJDA", "person", 30.5),
        ('"TENCOM" S.A.', "entity", 29.5),
    ]


def test_parse_founders_without_shares() -> None:
    rows = parse_founders("CANCELARIA DE STAT, POPESCU ANA")
    assert [(r["name"], r["kind"], r["share_pct"], r["share_text"]) for r in rows] == [
        ("CANCELARIA DE STAT", "entity", None, None),
        ("POPESCU ANA", "person", None, None),
    ]


def test_parse_founders_joint_stake_keeps_the_filed_text_only() -> None:
    rows = parse_founders("BECU IGOR, BECU LUDMILA (25,00%), BECU ANA (75,00%)")
    assert [(r["name"], r["joint"], r["share_pct"], r["share_text"]) for r in rows] == [
        ("BECU IGOR", True, None, "25,00%"),
        ("BECU LUDMILA", True, None, "25,00%"),
        ("BECU ANA", False, 75.0, "75,00%"),
    ]


def test_parse_founders_does_not_split_one_person_with_several_given_names() -> None:
    rows = parse_founders("BOURDIER ERIC, MARCEL, LOUIS (100,00%)")
    assert len(rows) == 1
    assert rows[0]["name"] == "BOURDIER ERIC, MARCEL, LOUIS"
    assert rows[0]["share_pct"] == 100.0


def test_parse_founders_skips_group_placeholders() -> None:
    assert parse_founders("TOTAL 71 MEMBRI") == []
    assert [r["name"] for r in parse_founders("MEMBRII ASOCIAȚIEI -PERSOANE FIZICE, POPA ION")] == ["POPA ION"]


def test_parse_founders_embedded_idno_and_address() -> None:
    raw = (
        "ORGANIZAȚIA DE CREDITARE NEBANCARĂ ”DINAR-CAPITAL” SRL, IDNO 1011600007976\n"
        "MD-2028, STRADA SCHINOASA-DEAL 51/6, MUN. CHIȘINĂU. (100,00%)"
    )
    (row,) = parse_founders(raw)
    assert row["name"] == "ORGANIZAȚIA DE CREDITARE NEBANCARĂ ”DINAR-CAPITAL” SRL"
    assert row["embedded_idno"] == "1011600007976"
    assert row["kind"] == "entity"
    assert row["share_pct"] == 100.0


def test_parse_founders_state() -> None:
    (row,) = parse_founders("STATUL (100,00%)")
    assert row["kind"] == "state"


def test_resolve_founder_rules() -> None:
    one = [(GRAWE_CD, 'Societatea cu Răspundere Limitată "GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL"')]
    founder = '"GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL" S.R.L.'
    assert resolve_founder(founder, one, own_idno=CARATEST) == one[0]
    # A company is never its own founder.
    assert resolve_founder(founder, one, own_idno=GRAWE_CD) is None
    # Two companies with the same folded name: refuse rather than pick one.
    assert resolve_founder(founder, one + [(TWIN_A, one[0][1])], own_idno=CARATEST) is None
    # Nothing distinctive left once the form words are gone.
    assert resolve_founder("S.R.L. ABC", [(TWIN_A, "ABC SRL")], own_idno=CARATEST) is None


# ---------------------------------------------------------------------------
# The fixture workbook and index
# ---------------------------------------------------------------------------

_HEADER = [
    "IDNO/ Cod fiscal",
    "Data înregistrării",
    "Denumirea completă",
    "Forma org./jurid.",
    "Adresa",
    "Codul unităţii administrativ-teritoriale (CUATM)",
    "Lista conducătorilor (cu indicarea rolurilor)",
    "Lista fondatorilor (cu indicarea cotei părţi în capitalul social %)",
    "Genuri de activitate nelicentiate",
    "Genuri de activitate licentiate",
    "Data lichidării",
]

SRL = "Societate cu răspundere limitată"


def _rows() -> list[list[Any]]:
    return [
        [int(KAUFLAND), date(2016, 2, 12), 'Societatea cu Răspundere Limitată KAUFLAND', SRL,
         "MD-2001, CHIȘINĂU, str. Exemplu, 1", 120,
         "MUNTEANU ELENA [Administrator], HOESSL MARCO VOLKER [Administrator]",
         "KAUFLAND ROMANIA SCS (100,00%)", "47110", "", None],
        [int(GRAWE_CD), date(2005, 3, 17),
         'Societatea cu Răspundere Limitată "GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL"', SRL,
         "MD-2012, CHIȘINĂU", 120, "CARABAN ELENA [Administrator]", "", "70220", "", None],
        [int(CARATEST), date(2008, 8, 18), 'S.R.L. "CARATEST"', SRL, "MD-2044, CHIȘINĂU", 120,
         "BAKARJI SVETLANA [Administrator]",
         '"GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL" S.R.L. (60,00%), BAKARJI SVETLANA (40,00%)',
         "71200", "", None],
        [int(BANK), date(2001, 5, 21), 'Banca Comercială "MOLDOVA-AGROINDBANK" S.A.',
         "Societate pe acţiuni de tip deschis", "MD-2006, CHIȘINĂU", 120,
         "SONIC ALEXANDRU [Comitet], STOIANOV MACAR [Comitet]", "", "64190", "Activitatea financiară", None],
        [int(LIQUIDATING), date(1998, 1, 5), 'S.R.L. "IN LICHIDARE"', SRL, "", 300,
         "RUSU ANA [Lichidator]",
         "ORGANIZAȚIA DE CREDITARE NEBANCARĂ ”DINAR-CAPITAL” SRL, IDNO 1011600007976\nMD-2028, CHIȘINĂU. (100,00%)",
         "", "", None],
        [int(STATE_ENT), date(1992, 1, 2), 'ÎNTREPRINDEREA DE STAT "RUBIN"', "Întreprindere de stat",
         "", 4301, "CUHTIȚCHI NICOLAI [Administrator]", "STATUL (100,00%)", "", "", None],
        [int(JOINT), date(2010, 6, 1), 'S.R.L. "BECU"', SRL, "", 300,
         "BECU IGOR [Administrator]",
         "BECU IGOR, BECU LUDMILA (25,00%), BECU ANA (75,00%)", "", "", None],
        # Left out: liquidated, a sole trader, and a legacy short fiscal code.
        [int(LIQUIDATED), date(2017, 5, 1), '"JET4U" S.R.L.', SRL, "", 120,
         "ȚURCAN AUREL [Administrator]", "ȚURCAN AUREL (100,00%)", "", "", date(2023, 1, 1)],
        [int(SOLE_TRADER), date(2017, 7, 13), 'Întreprinzător Individual "MOGOREANU GHEORGHE"',
         "Întreprindere individuală", "", 300, "MOGOREANU GHEORGHE [Administrator]",
         "MOGOREANU GHEORGHE", "", "", None],
        [213976, date(1992, 1, 3), 'SRL "CERCETARE"', SRL, "", 140, "FEDOROV BORIS [Administrator]",
         "FEDOROV BORIS (100,00%)", "", "", None],
        # Two companies whose names fold alike: a founder naming them resolves to neither.
        [int(TWIN_A), date(2020, 1, 1), 'S.R.L. "GEMENI"', SRL, "", 120, "A B [Administrator]",
         "TOTAL 2 MEMBRI", "", "", None],
        [int(TWIN_B), date(2021, 1, 1), 'SOCIETATEA CU RĂSPUNDERE LIMITATĂ GEMENI', SRL, "", 120,
         "C D [Administrator]", "", "", "", None],
        [int(USES_TWIN), date(2022, 1, 1), 'S.R.L. "FOLOSESTE"', SRL, "", 120, "E F [Administrator]",
         '"GEMENI" S.R.L. (100,00%)', "", "", None],
    ]


def _write_workbook(path: Path, *, title_row: int = 1, header_row: int = 2) -> Path:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Company"
    ws.cell(
        row=title_row,
        column=4,
        value=(
            "Date din Registrul de stat al unităţilor de drept privind\n"
            "întreprinderile înregistrate în Republica Moldova\n (starea pînă la 14.09.2026)"
        ),
    )
    for col, header in enumerate(_HEADER, start=1):
        ws.cell(row=header_row, column=col, value=header)
    for offset, row in enumerate(_rows(), start=header_row + 1):
        for col, value in enumerate(row, start=1):
            if value not in (None, ""):
                ws.cell(row=offset, column=col, value=value)
    wb.create_sheet("Clasificare nelicenţiate").append(["ID", "Denumire", "Cod CAEM"])
    wb.save(path)
    return path


@pytest.fixture()
def workbook(tmp_path: Path) -> Path:
    return _write_workbook(tmp_path / "company_2026.09.14.xlsx")


@pytest.fixture()
def built(workbook: Path, tmp_path: Path) -> tuple[Path, dict[str, str]]:
    out = tmp_path / "asp_moldova.sqlite"
    meta = build_index(workbook, out)
    return out, meta


@pytest.fixture()
def index(built: tuple[Path, dict[str, str]]):
    """The built index, pointed at by the setting, with downloads off."""
    path, _ = built
    get_settings.cache_clear()
    os.environ["ASP_MOLDOVA_DB_FILE"] = str(path)
    os.environ["ASP_MOLDOVA_SYNC"] = "false"
    asp_moldova.reset_connection()
    yield path
    os.environ.pop("ASP_MOLDOVA_DB_FILE", None)
    os.environ.pop("ASP_MOLDOVA_SYNC", None)
    get_settings.cache_clear()
    asp_moldova.reset_connection()


def test_build_index_counts_and_exclusions(built: tuple[Path, dict[str, str]]) -> None:
    path, meta = built
    assert meta["companies"] == "10"
    assert meta["excluded_liquidated"] == "1"
    assert meta["excluded_sole_traders_and_farms"] == "1"
    assert meta["excluded_no_valid_idno"] == "1"
    assert meta["founders_skipped_collective"] == "1"
    # Read from the title row, since the fixture passes no snapshot date.
    assert meta["snapshot_date"] == "2026-09-14"
    conn = sqlite3.connect(path)
    ids = {r[0] for r in conn.execute("SELECT idno FROM company")}
    assert LIQUIDATED not in ids and SOLE_TRADER not in ids
    assert conn.execute("SELECT registered_on FROM company WHERE idno = ?", (KAUFLAND,)).fetchone()[0] == "2016-02-12"


def test_build_index_resolves_founders_with_the_name_check(built: tuple[Path, dict[str, str]]) -> None:
    path, meta = built
    conn = sqlite3.connect(path)
    resolved = dict(
        conn.execute(
            "SELECT idno || ':' || name, resolved_idno || ':' || resolution FROM founder "
            "WHERE resolved_idno IS NOT NULL"
        ).fetchall()
    )
    assert resolved == {
        f'{CARATEST}:"GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL" S.R.L.': f"{GRAWE_CD}:name_match",
        # The register's own text names the number; kept though that founder
        # is not in the index.
        f"{LIQUIDATING}:ORGANIZAȚIA DE CREDITARE NEBANCARĂ ”DINAR-CAPITAL” SRL": "1011600007976:embedded_idno",
    }
    assert meta["founders_resolved"] == "2"
    # KAUFLAND ROMANIA SCS is foreign and "GEMENI" is ambiguous: neither resolves.
    assert conn.execute(
        "SELECT resolved_idno FROM founder WHERE idno = ?", (USES_TWIN,)
    ).fetchone()[0] is None


def test_build_index_finds_the_header_wherever_it_is(tmp_path: Path) -> None:
    """The non-commercial export puts its header on row 5; find it by text."""
    path = _write_workbook(tmp_path / "late_header.xlsx", title_row=1, header_row=5)
    meta = build_index(path, tmp_path / "out.sqlite")
    assert meta["companies"] == "10"


def test_build_index_fails_loudly_on_a_changed_export(tmp_path: Path) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Company"
    ws.append(["IDNO", "Denumirea"])  # no directors, founders or liquidation columns
    ws.append([int(KAUFLAND), "KAUFLAND"])
    wb.save(tmp_path / "changed.xlsx")
    with pytest.raises(asp_moldova.IndexBuildError):
        build_index(tmp_path / "changed.xlsx", tmp_path / "out.sqlite")
    assert not (tmp_path / "out.sqlite").exists()


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


async def test_fetch_returns_the_record_and_records_the_snapshot(index: Path) -> None:
    with provenance.recording() as observed:
        bundle = await AspMoldovaAdapter().fetch(KAUFLAND, legal_name='"Kaufland" S.R.L.')
    assert bundle["is_stub"] is False
    assert bundle["company"]["name"] == "Societatea cu Răspundere Limitată KAUFLAND"
    assert [o["name"] for o in bundle["officers"]] == ["MUNTEANU ELENA", "HOESSL MARCO VOLKER"]
    assert bundle["founders"][0]["name"] == "KAUFLAND ROMANIA SCS"
    assert bundle["snapshot_date"] == "2026-09-14"
    resolved = observed.resolve()
    assert resolved.liveness == "snapshot"
    assert resolved.retrieved_at.date() == date(2026, 9, 14)


async def test_fetch_not_in_export_is_an_answer_not_a_stub(index: Path) -> None:
    with degradation.recording() as degraded:
        bundle = await AspMoldovaAdapter().fetch(LIQUIDATED)
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"] == COVERAGE_NOT_IN_EXPORT
    assert degraded == []
    assert list(map_asp_moldova(bundle)) == []
    hit = _bh_asp_moldova(bundle, LIQUIDATED, _LookupCtx(lei="X" * 20, legal_name="JET4U"))
    assert hit.identifiers == {}
    assert "liquidated" in (hit.finding or "")


async def test_fetch_rejects_a_non_idno(index: Path) -> None:
    bundle = await AspMoldovaAdapter().fetch("16479")
    assert bundle["is_stub"] is True


async def test_fetch_without_an_index_offline_is_a_quiet_stub(tmp_path: Path) -> None:
    get_settings.cache_clear()
    os.environ["ASP_MOLDOVA_DB_FILE"] = str(tmp_path / "absent.sqlite")
    asp_moldova.reset_connection()
    try:
        with degradation.recording() as degraded:
            bundle = await AspMoldovaAdapter().fetch(KAUFLAND)
        assert bundle["is_stub"] is True
        assert degraded == []
    finally:
        os.environ.pop("ASP_MOLDOVA_DB_FILE", None)
        get_settings.cache_clear()
        asp_moldova.reset_connection()


async def test_fetch_without_an_index_live_degrades(tmp_path: Path, monkeypatch) -> None:
    """Live, no index, and the build does not land in time: say so."""
    get_settings.cache_clear()
    os.environ["ASP_MOLDOVA_DB_FILE"] = str(tmp_path / "absent.sqlite")
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    asp_moldova.reset_connection()
    monkeypatch.setattr(asp_moldova, "_start_background_sync", lambda: None)
    try:
        with degradation.recording() as degraded:
            bundle = await AspMoldovaAdapter().fetch(KAUFLAND)
        assert bundle["is_stub"] is True
        assert [d.source_id for d in degraded] == ["asp_moldova"]
        # Counts and sources only — never the company looked up.
        assert KAUFLAND not in degraded[0].detail
    finally:
        os.environ.pop("ASP_MOLDOVA_DB_FILE", None)
        os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
        get_settings.cache_clear()
        asp_moldova.reset_connection()


async def test_search_reads_the_index(index: Path) -> None:
    from opencheck.sources.base import SearchKind

    hits = await AspMoldovaAdapter().search("caratest", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [CARATEST]
    assert hits[0].identifiers == {"md_idno": CARATEST}


def test_info_declares_the_register(index: Path) -> None:
    info = AspMoldovaAdapter().info
    assert info.is_national_register is True
    assert info.country == "MD"
    assert info.requires_api_key is False
    assert info.license == "DATASET.GOV.MD-Reuse"


def test_licence_is_classified_as_commercial_reuse() -> None:
    terms = classify("DATASET.GOV.MD-Reuse")
    assert terms.commercial_use == "yes"
    assert terms.color == "green"


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------


async def _statements(idno: str) -> list[dict[str, Any]]:
    return list(map_asp_moldova(await AspMoldovaAdapter().fetch(idno)))


def _relationships(statements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == "relationship"]


async def test_mapper_entity_and_interests(index: Path) -> None:
    statements = await _statements(CARATEST)
    entity = statements[0]
    details = entity["recordDetails"]
    assert details["identifiers"][0] == {
        "id": CARATEST,
        "scheme": "MD-IDNO",
        "schemeName": "IDNO — State Register of Legal Entities (Moldova)",
    }
    assert details["jurisdiction"] == {"name": "Moldova", "code": "MD"}
    assert details["foundingDate"] == "2008-08-18"

    rels = _relationships(statements)
    assert all(
        i["beneficialOwnershipOrControl"] is False
        for r in rels
        for i in r["recordDetails"]["interests"]
    )
    types = sorted(i["type"] for r in rels for i in r["recordDetails"]["interests"])
    assert types == ["seniorManagingOfficial", "shareholding", "shareholding"]
    shares = sorted(
        i["share"]["exact"] for r in rels for i in r["recordDetails"]["interests"] if "share" in i
    )
    assert shares == [40.0, 60.0]


async def test_mapper_one_person_for_director_and_founder(index: Path) -> None:
    statements = await _statements(CARATEST)
    people = [s for s in statements if s["recordType"] == "person"]
    assert len(people) == 1  # BAKARJI SVETLANA, administrator and 40% founder
    rels_to_her = [
        r for r in _relationships(statements)
        if r["recordDetails"]["interestedParty"] == people[0]["statementId"]
    ]
    assert len(rels_to_her) == 2


async def test_mapper_name_matched_founder_is_annotated(index: Path) -> None:
    statements = await _statements(CARATEST)
    founder = next(
        s for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["identifiers"]
        and s["recordDetails"]["identifiers"][0]["id"] == GRAWE_CD
    )
    assert founder["recordDetails"]["entityType"]["type"] == "registeredEntity"
    (annotation,) = founder["annotations"]
    assert annotation["motivation"] == "transformation"
    assert resolve_pointer(founder, annotation["statementPointerTarget"]) == GRAWE_CD


async def test_mapper_embedded_idno_is_not_annotated(index: Path) -> None:
    statements = await _statements(LIQUIDATING)
    founder = next(
        s for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["identifiers"]
        and s["recordDetails"]["identifiers"][0]["id"] == "1011600007976"
    )
    assert "annotations" not in founder


async def test_mapper_liquidator_makes_the_company_pending(index: Path) -> None:
    statements = await _statements(LIQUIDATING)
    rel = _relationships(statements)[0]
    assert rel["recordDetails"]["interests"][0]["type"] == "controlByLegalFramework"
    assert any(
        "in a terminal process" in (a.get("description") or "") and "Lichidator" in a["description"]
        for a in statements[0].get("annotations", [])
    )


async def test_mapper_state_founder_and_joint_stake(index: Path) -> None:
    state = await _statements(STATE_ENT)
    assert any(
        s["recordType"] == "entity" and s["recordDetails"]["entityType"]["type"] == "state"
        for s in state
    )
    joint = await _statements(JOINT)
    interests = [i for r in _relationships(joint) for i in r["recordDetails"]["interests"]]
    jointly = [i for i in interests if "jointly" in (i.get("details") or "")]
    assert len(jointly) == 2 and all("share" not in i for i in jointly)
    assert sum(1 for s in joint if s["recordType"] == "person") == 3


async def test_mapper_unknown_foreign_founder_is_a_legal_entity_without_identifier(index: Path) -> None:
    statements = await _statements(KAUFLAND)
    founder = next(
        s for s in statements
        if s["recordType"] == "entity" and s["recordDetails"]["name"] == "KAUFLAND ROMANIA SCS"
    )
    assert founder["recordDetails"]["entityType"]["type"] == "legalEntity"
    assert founder["recordDetails"]["identifiers"] == []
    assert "jurisdiction" not in founder["recordDetails"]


# ---------------------------------------------------------------------------
# Finding, hit builder, and the wiring into GLEIF and the frontier
# ---------------------------------------------------------------------------


async def test_finding_sentences(index: Path) -> None:
    adapter = AspMoldovaAdapter()
    kaufland = finding_asp_moldova(await adapter.fetch(KAUFLAND))
    assert kaufland == (
        "Registered 2016-02-12, societate cu răspundere limitată, 2 directors on file, "
        "founded by KAUFLAND ROMANIA SCS (100%)."
    )
    assert len(kaufland) <= MAX_FINDING_CHARS
    liquidating = finding_asp_moldova(await adapter.fetch(LIQUIDATING))
    assert liquidating.startswith("Lichidator on file")
    bank = finding_asp_moldova(await adapter.fetch(BANK))
    assert "founder" not in bank


async def test_hit_builder_asserts_the_idno(index: Path) -> None:
    bundle = await AspMoldovaAdapter().fetch(KAUFLAND)
    hit = _bh_asp_moldova(bundle, KAUFLAND, _LookupCtx(lei="X" * 20))
    assert hit.identifiers == {"md_idno": KAUFLAND}
    assert hit.summary == f"MD-IDNO {KAUFLAND}"


def test_build_derived_maps_every_moldovan_authority() -> None:
    from opencheck.routers.lookup import _build_derived

    for ra in sorted(MD_RA_CODES):
        ctx = _LookupCtx(lei="X" * 20)
        ctx.jurisdiction = "MD"
        ctx.registered_as = KAUFLAND
        _build_derived(ctx, ra)
        assert ctx.derived.get("md_idno") == KAUFLAND


def test_md_idno_is_a_register_hop() -> None:
    """A founder carrying MD-IDNO can be expanded in FullCheck."""
    from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID
    from opencheck.register_hops import hop_for

    assert {_GLEIF_RA_TO_ORG_ID[ra][0] for ra in MD_RA_CODES} == {"MD-IDNO"}
    hop = hop_for("MD-IDNO")
    assert hop is not None and hop.source_id == "asp_moldova"


# ---------------------------------------------------------------------------
# Keeping the index current
# ---------------------------------------------------------------------------


def _package(*resources: dict[str, Any]) -> dict[str, Any]:
    return {"success": True, "result": {"resources": list(resources)}}


def test_latest_resource_reads_the_date_from_the_name_not_the_list_order() -> None:
    package = _package(
        {"name": "Informații la data de 14.09.2026", "format": "XLSX",
         "url": "https://x/company_2026.09.14.xlsx", "created": "2026-09-14T05:52:15"},
        {"name": "Date la data de 07.12.2020", "format": "XLSX",
         "url": "https://x/company.xlsx", "created": "2020-12-07T00:00:00"},
        {"name": "Informații la data de 24.08.2026", "format": "XLSX",
         "url": "https://x/company_2026.08.24.xlsx", "created": "2026-08-28T05:53:20"},
        {"name": "Descriere", "format": "PDF", "url": "https://x/readme.pdf", "created": "2026-09-15"},
    )
    assert latest_resource(package) == {
        "url": "https://x/company_2026.09.14.xlsx",
        "snapshot_date": "2026-09-14",
        "name": "Informații la data de 14.09.2026",
    }


def test_latest_resource_falls_back_to_the_file_name() -> None:
    package = _package(
        {"name": "Fișier", "format": "XLSX", "url": "https://x/company_2026.09.07.xlsx", "created": "2026-09-08"},
    )
    assert latest_resource(package)["snapshot_date"] == "2026-09-07"


def test_sync_downloads_and_builds_when_stale(workbook: Path, tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "synced.sqlite"
    get_settings.cache_clear()
    os.environ["ASP_MOLDOVA_DB_FILE"] = str(target)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    asp_moldova.reset_connection()
    asp_moldova._STATE["last_check"] = None
    calls: list[str] = []

    def fake_download(url: str, dest: Path) -> int:
        calls.append(url)
        dest.write_bytes(workbook.read_bytes())
        return dest.stat().st_size

    monkeypatch.setattr(
        asp_moldova,
        "latest_resource",
        lambda package=None: {"url": "https://x/company_2026.09.14.xlsx", "snapshot_date": "2026-09-14", "name": "n"},
    )
    monkeypatch.setattr(asp_moldova, "_download", fake_download)
    monkeypatch.setattr(asp_moldova, "snapshot_age_days", lambda meta: 1 if meta else None)
    try:
        first = asp_moldova.sync_index()
        assert first["asp_moldova"] == "built"
        assert first["companies"] == "10"
        assert not target.with_name(target.name + ".xlsx").exists()
        # Fresh now: the second call keeps the file and asks nobody.
        assert asp_moldova.sync_index()["asp_moldova"] == "kept"
        assert calls == ["https://x/company_2026.09.14.xlsx"]
        assert asp_moldova.index_available()
    finally:
        os.environ.pop("ASP_MOLDOVA_DB_FILE", None)
        os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
        get_settings.cache_clear()
        asp_moldova.reset_connection()


def test_sync_failure_keeps_the_old_index(built: tuple[Path, dict[str, str]], monkeypatch) -> None:
    path, _ = built
    get_settings.cache_clear()
    os.environ["ASP_MOLDOVA_DB_FILE"] = str(path)
    os.environ["OPENCHECK_ALLOW_LIVE"] = "true"
    asp_moldova.reset_connection()
    asp_moldova._STATE["last_check"] = None

    def boom(package=None):
        raise OSError("portal unreachable")

    monkeypatch.setattr(asp_moldova, "latest_resource", boom)
    monkeypatch.setattr(asp_moldova, "snapshot_age_days", lambda meta: 30)
    try:
        outcome = asp_moldova.sync_index()
        assert outcome["asp_moldova"] == "failed"
        assert "portal unreachable" in outcome["error"]
        assert asp_moldova.index_available()
    finally:
        os.environ.pop("ASP_MOLDOVA_DB_FILE", None)
        os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
        get_settings.cache_clear()
        asp_moldova.reset_connection()
