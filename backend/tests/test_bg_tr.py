"""Bulgaria's Commercial Register (the Bulgaria Commercial Register ticket,
18 Nov 2026): the index builder, the adapter, the schema and the mapper.

The fixture files copy the register's XML layout (``Message/Body/Deeds/Deed/
SubDeed/<field>``) with invented companies and people. Every ЕИК is valid;
every hash is made up.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from opencheck.bods import map_bg_tr
from opencheck.bods.mapper import _GLEIF_RA_TO_ORG_ID, gleif_registration_scheme
from opencheck.config import get_settings
from opencheck.findings import finding_bg_tr
from opencheck.sources import REGISTRY, bg_tr
from opencheck.sources.schemas.bg_tr import BgTrBundle

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_bg_tr_index.py"
_spec = importlib.util.spec_from_file_location("build_bg_tr_index", _SCRIPT)
assert _spec and _spec.loader
builder = importlib.util.module_from_spec(_spec)
sys.modules["build_bg_tr_index"] = builder
_spec.loader.exec_module(builder)

OOD = "200000015"       # two partners, a manager who is also the beneficial owner
EOOD = "200000027"      # a company sole owner, deleted on day two
PARENT = "123456786"    # the sole owner of EOOD
ASSOC = "831642181"     # an association: a board and a code-less beneficial owner

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64

_PERSON = (
    '<{tag} CompetentAuthorityForRegistration="" RegistrationNumber="" ForeignRegisterCode="" '
    'ForeignLegalFormCode="" CountryCode="{cc}" LegalForm="" Position="" IsForeignTraderText="0">'
    "<Indent>{indent}</Indent><Name>{name}</Name><IndentType>{itype}</IndentType>"
    "<CountryName>{cname}</CountryName></{tag}>"
)


def _person(name: str, indent: str, *, tag: str = "Person", cc: str = "BGR",
            itype: str = "EGN", cname: str = "БЪЛГАРИЯ") -> str:
    return _PERSON.format(tag=tag, name=name, indent=indent, cc=cc, itype=itype, cname=cname)


def _field(tag: str, entry: str, body: str = "", *, op: str = "Add", attrs: str = "") -> str:
    day = f"{entry[:4]}-{entry[4:6]}-{entry[6:8]}T{entry[8:10]}:{entry[10:12]}:{entry[12:14]}"
    # A list's Erase names no RecordID (as the register writes it).
    return (f'<{tag} FieldOperation="{op}" FieldEntryNumber="{entry}" FieldEntryDate="{day}" '
            + (f'RecordID="{int(entry[-8:]) + len(tag)}" ' if op == "Add" else "")
            + f'{attrs}>{body}</{tag}>')


def _deed(uic: str, name: str, form: str, fields: str, status: str = "N") -> str:
    return (f'<Deed DeedStatus="{status}" CompanyName="{name}" UIC="{uic}" LegalForm="{form}">'
            f'<SubDeed SubUIC="0001" SubUICType="MainCircumstances" SubDeedStatus="A">{fields}</SubDeed>'
            "</Deed>")


def _message(created: str, msg_id: str, deeds: str) -> str:
    return (
        '<?xml version="1.0"?><Message><Header><MessageDetails>'
        f"<CreateDate>{created}</CreateDate><ID>{msg_id}</ID></MessageDetails></Header>"
        f"<Body><Deeds>{deeds}</Deeds></Body></Message>"
    )


DAY1 = "20210104"
DAY2 = "20210105"

_OWNER = (
    "<ActualOwner>" + _person("Иван Петров Иванов", HASH_A)
    + "<Address><CountryCode>BGR</CountryCode><Settlement>СОФИЯ</Settlement>"
    "<Street>Улица на собственика</Street><StreetNumber>7</StreetNumber></Address>"
    "<OwnedRights>Пряко притежаване на дялово участие 50 на сто</OwnedRights>"
    "<CountryOfResidence><CountryCode>BGR</CountryCode><Country>БЪЛГАРИЯ</Country></CountryOfResidence>"
    "</ActualOwner>"
)

FILE1 = _message("2021-01-05T00:13:13+02:00", "msg-1", "".join([
    _deed(OOD, "ДВАМА СЪДРУЖНИЦИ", "OOD", "".join([
        _field("UIC", DAY1 + "080000", attrs='IsNew="true"'),
        _field("Company", DAY1 + "080000", "ДВАМА СЪДРУЖНИЦИ"),
        _field("Transliteration", DAY1 + "080000", "DVAMA SADRUZHNITSI"),
        _field("LegalForm", DAY1 + "080000", attrs='Code="OOD" Text="Дружество с ограничена отговорност"'),
        _field("Seat", DAY1 + "080000",
               "<Address><CountryCode>BGR</CountryCode><Country>БЪЛГАРИЯ</Country>"
               "<Settlement>СОФИЯ</Settlement><Street>бул. Витоша</Street><StreetNumber>1</StreetNumber></Address>"),
        _field("Funds", DAY1 + "080000", "100", attrs='euro="0"'),
        _field("Partners", DAY1 + "080000",
               '<Partner share="50">' + _person("Иван Петров Иванов", HASH_A, tag="Subject") + "</Partner>"
               '<Partner share="50">' + _person("Мария Георгиева", "751231", tag="Subject",
                                                  itype="BirthDate", cc="GRC", cname="ГЪРЦИЯ") + "</Partner>"),
        _field("Managers", DAY1 + "080000",
               "<Manager>" + _person("Иван Петров Иванов", HASH_A) + "</Manager>"),
        _field("Procurators", DAY1 + "080000",
               "<Procurator>" + _person("Стоян Стоянов", HASH_B) + "</Procurator>"),
        _field("ActualOwners", DAY1 + "090000", _OWNER),
    ])),
    _deed(EOOD, "ЕДИН СОБСТВЕНИК", "EOOD", "".join([
        _field("Company", DAY1 + "100000", "ЕДИН СОБСТВЕНИК"),
        _field("SoleCapitalOwner", DAY1 + "100000",
               '<Subject CountryCode="BGR" LegalForm="" RegistrationNumber="" ForeignRegisterCode="">'
               f"<Indent>{PARENT}</Indent><Name>МАЙКА АД</Name><IndentType>UIC</IndentType></Subject>"),
        _field("Managers", DAY1 + "100000",
               "<Manager>" + _person("Петър Петров", HASH_C) + "</Manager>"),
    ])),
]))

FILE2 = _message("2021-01-06T00:10:00+02:00", "msg-2", "".join([
    _deed(OOD, "ДВАМА СЪДРУЖНИЦИ", "OOD", _field("Procurators", DAY2 + "080000", op="Erase")),
    _deed(EOOD, "ЕДИН СОБСТВЕНИК", "EOOD", _field("AddemptionOfTrader", DAY2 + "110000")),
    _deed(ASSOC, "СДРУЖЕНИЕ ПРИМЕР", "ASSOC", "".join([
        _field("Company", DAY2 + "120000", "СДРУЖЕНИЕ ПРИМЕР"),
        _field("BoardOfManagers", DAY2 + "120000",
               "<Member>" + _person("Елена Иванова", HASH_B) + "</Member>"),
        _field("ActualOwners", DAY2 + "120000",
               "<ActualOwner>" + _person("Елена Иванова", HASH_B)
               + "<OwnedRights>Косвено притежаване на повече от 25 на сто</OwnedRights></ActualOwner>"),
    ])),
]))


def _write(directory: Path, name: str, text: str) -> None:
    with gzip.open(directory / name, "wt", encoding="utf-8") as fh:
        fh.write(text)


def _build(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    raw = tmp_path / "raw"
    raw.mkdir()
    # Named against their order: the build reads the header's CreateDate.
    _write(raw, "b.xml.gz", FILE1)
    _write(raw, "a.xml.gz", FILE2)
    _write(raw, "c.xml.gz", FILE2)  # the same message republished: read once
    out = tmp_path / "data" / "bg_tr.sqlite"
    out.parent.mkdir()
    return out, builder.build_index(raw, out, min_files=0)


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("BG_TR_DB_FILE", None)
    else:
        os.environ["BG_TR_DB_FILE"] = str(path)
    get_settings.cache_clear()
    bg_tr.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    previous = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty")
    _point_at(None)
    yield
    if previous is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous
    _point_at(None)


async def _fetch(eik: str, **kwargs):
    return await REGISTRY["bg_tr"].fetch(eik, **kwargs)


# --------------------------------------------------------------------------
# Identifiers and wiring
# --------------------------------------------------------------------------


def test_the_check_digit() -> None:
    for code in (OOD, EOOD, PARENT, ASSOC, "000694959"):
        assert bg_tr.is_valid_eik(code)
    assert not bg_tr.is_valid_eik("200000016")
    assert bg_tr.normalise_eik("BG 200 000 015") == OOD


def test_the_schema_version_is_the_builders() -> None:
    assert bg_tr.INDEX_SCHEMA_VERSION == builder.SCHEMA_VERSION


def test_gleif_files_the_eik_under_three_authorities_and_never_the_fsc_codes() -> None:
    assert {"RA000065", "RA000730", "RA000731"} == bg_tr.BG_RA_CODES
    for ra in bg_tr.BG_RA_CODES:
        assert _GLEIF_RA_TO_ORG_ID[ra][0] == "BG-EIK"
        assert gleif_registration_scheme(ra, "BG", None, OOD)[0] == "BG-EIK"
    assert "RA000066" not in bg_tr.BG_RA_CODES
    assert (_GLEIF_RA_TO_ORG_ID.get("RA000066") or ("",))[0] != "BG-EIK"


# --------------------------------------------------------------------------
# The builder
# --------------------------------------------------------------------------


def test_the_build_replays_in_header_order_and_reads_a_republished_message_once(index) -> None:
    out, meta = index
    assert meta["files"] == "2"
    assert meta["companies"] == "3"
    assert meta["with_beneficial_owners"] == "2"
    assert meta["cut"] == "2021-01-05"
    assert meta["first_entry"] == "2021-01-04"


def test_an_erase_clears_a_list(index) -> None:
    out, _meta = index
    data = json.loads(sqlite3.connect(out).execute(
        "SELECT data FROM company WHERE uic = ?", (OOD,)).fetchone()[0])
    assert "Procurators" not in data
    assert "Managers" in data


def test_the_index_holds_no_personal_number_hash_address_or_birth_date(index) -> None:
    out, _meta = index
    dump = "\n".join(row[0] for row in sqlite3.connect(out).execute("SELECT data FROM company"))
    for forbidden in (HASH_A, HASH_B, HASH_C, "751231", "Улица на собственика", '"Indent"',
                      '"address"', "BirthDate"):
        assert forbidden not in dump


def test_a_person_key_comes_from_a_hash_never_from_a_birth_date() -> None:
    assert builder.person_key(OOD, HASH_A) is not None
    assert builder.person_key(OOD, HASH_A) != builder.person_key(EOOD, HASH_A)
    assert builder.person_key(OOD, "751231") is None


def test_a_sole_traders_seat_keeps_the_place_only(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _write(raw, "et.xml.gz", _message("2021-01-05T00:00:00+02:00", "et", _deed(
        "200000039", "ЕТ ТЪРГОВЕЦ", "ET", _field("Seat", DAY1 + "080000",
            "<Address><CountryCode>BGR</CountryCode><Settlement>ВАРНА</Settlement>"
            "<Street>Домашна улица</Street><StreetNumber>3</StreetNumber></Address>"))))
    out = tmp_path / "et.sqlite"
    builder.build_index(raw, out, min_files=0)
    data = sqlite3.connect(out).execute("SELECT data FROM company").fetchone()[0]
    assert "ВАРНА" in data
    assert "Домашна" not in data


def test_the_build_refuses_a_page_that_is_not_a_register_message(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "x.xml").write_text("<html><body>Not found</body></html>")
    with pytest.raises(builder.BuildRefused):
        builder.build_index(raw, tmp_path / "x.sqlite", min_files=0)
    assert not (tmp_path / "x.sqlite").exists()


def test_percentages_are_read_exactly_or_as_a_lower_bound() -> None:
    assert builder.parse_percent("Пряко притежаване 50 на сто") == {"exact": 50.0}
    assert builder.parse_percent("повече от 25 на сто") == {"more_than": 25.0}
    assert builder.parse_percent("25 % и 30 %") is None


# --------------------------------------------------------------------------
# The adapter
# --------------------------------------------------------------------------


async def test_fetch_reads_the_index(index) -> None:
    bundle = await _fetch(OOD)
    assert bundle["name"] == "ДВАМА СЪДРУЖНИЦИ"
    assert bundle["legal_form_code"] == "OOD"
    assert bundle["registered_on"] == "2021-01-04"
    assert bundle["status_class"] == "live"
    assert bundle["cut"] == "2021-01-05"
    assert {p["field"] for p in bundle["parties"]} == {"Partners", "Managers"}
    assert len(bundle["beneficial_owners"]) == 1
    assert "Витоша" in bundle["seat_text"]


async def test_a_deleted_company_is_terminal(index) -> None:
    bundle = await _fetch(EOOD)
    assert bundle["status_class"] == "terminal"
    assert bundle["status_since"] == "2021-01-05"


async def test_a_code_the_index_lacks_is_a_note_card(index) -> None:
    bundle = await _fetch("000694959")
    assert bundle["not_found"] is True
    assert bundle["is_stub"] is False
    assert list(map_bg_tr(bundle)) == []
    assert "since 1 January 2021" in finding_bg_tr(bundle)


async def test_a_malformed_code_is_a_stub(index) -> None:
    bundle = await _fetch("12345")
    assert bundle["is_stub"] is True


async def test_no_index_is_a_stub_and_not_covered(no_index) -> None:
    assert not REGISTRY["bg_tr"].covers_lei("5299000PCY1EP8QJFV48")
    bundle = await _fetch(OOD)
    assert bundle["is_stub"] is True


async def test_search_by_name_and_by_code(index) -> None:
    from opencheck.sources import SearchKind

    by_name = await REGISTRY["bg_tr"].search("съдружници", SearchKind.ENTITY)
    assert [h.hit_id for h in by_name] == [OOD]
    assert by_name[0].identifiers == {"bg_eik": OOD}
    by_latin = await REGISTRY["bg_tr"].search("sadruzhnitsi", SearchKind.ENTITY)
    assert [h.hit_id for h in by_latin] == [OOD]
    by_code = await REGISTRY["bg_tr"].search(EOOD, SearchKind.ENTITY)
    assert [h.hit_id for h in by_code] == [EOOD]


def test_the_schema_refuses_personal_data() -> None:
    for key in ("indent", "birth_date", "address", "passport", "hash", "egn"):
        with pytest.raises(ValueError):
            BgTrBundle(eik=OOD, beneficial_owners=[{"name": "X", key: "1"}])
        with pytest.raises(ValueError):
            BgTrBundle(eik=OOD, parties=[{"name": "X", key: "1"}])


# --------------------------------------------------------------------------
# The mapper
# --------------------------------------------------------------------------


def _by_type(statements: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for s in statements:
        out.setdefault(s["recordType"], []).append(s)
    return out


def _interests(statements: list[dict], party_name: str) -> list[dict]:
    names = {s["recordId"]: (s["recordDetails"].get("names") or [{}])[0].get("fullName")
             or s["recordDetails"].get("name") for s in statements if s["recordType"] != "relationship"}
    out = []
    for s in statements:
        if s["recordType"] == "relationship":
            ref = s["recordDetails"]["interestedParty"]
            if names.get(ref) == party_name:
                out += s["recordDetails"]["interests"]
    return out


async def test_the_company_statement(index) -> None:
    statements = list(map_bg_tr(await _fetch(OOD)))
    company = statements[0]["recordDetails"]
    assert company["identifiers"][0] == {
        "id": OOD, "scheme": "BG-EIK", "schemeName": company["identifiers"][0]["schemeName"]}
    assert company["jurisdiction"]["code"] == "BG"
    assert company["foundingDate"] == "2021-01-04"
    assert "DVAMA SADRUZHNITSI" in company["alternateNames"]
    notes = [a["description"] for a in statements[0].get("annotations") or []]
    assert any("since 1 January 2021" in n for n in notes)


async def test_one_person_on_two_lists_is_one_party_with_no_address_beyond_residence(index) -> None:
    statements = list(map_bg_tr(await _fetch(OOD)))
    people = _by_type(statements)["person"]
    names = sorted(p["recordDetails"]["names"][0]["fullName"] for p in people)
    assert names == ["Иван Петров Иванов", "Мария Георгиева"]
    ivan = next(p for p in people if p["recordDetails"]["names"][0]["fullName"] == "Иван Петров Иванов")
    assert ivan["recordDetails"]["nationalities"][0]["code"] == "BG"
    assert ivan["recordDetails"]["addresses"] == [
        {"type": "residence", "country": ivan["recordDetails"]["addresses"][0]["country"]}]
    assert "address" not in json.dumps(ivan["recordDetails"]["addresses"][0]["country"])
    assert "birthDate" not in ivan["recordDetails"]
    maria = next(p for p in people if p is not ivan)
    assert maria["recordDetails"]["nationalities"][0]["code"] == "GR"
    assert "751231" not in json.dumps(statements)


async def test_partner_shares_reconcile_to_the_capital_and_say_so(index) -> None:
    statements = list(map_bg_tr(await _fetch(OOD)))
    held = [i for i in _interests(statements, "Мария Георгиева") if i["type"] == "shareholding"]
    assert held[0]["share"] == {"exact": 50.0}
    rel = next(s for s in statements if s["recordType"] == "relationship"
               and s["recordDetails"]["interests"][0].get("share") == {"exact": 50.0})
    assert any(a["motivation"] == "transformation" for a in rel.get("annotations") or [])


async def test_the_beneficial_owner_asserts_ownership_with_its_extent(index) -> None:
    statements = list(map_bg_tr(await _fetch(OOD)))
    ivan = _interests(statements, "Иван Петров Иванов")
    bo = [i for i in ivan if i.get("beneficialOwnershipOrControl")]
    assert len(bo) == 1
    assert bo[0]["type"] == "shareholding"
    assert bo[0]["share"] == {"exact": 50.0}
    assert {i["type"] for i in ivan} == {"shareholding", "seniorManagingOfficial"}


async def test_a_lower_bound_is_an_exclusive_minimum_and_an_association_holds_no_shares(index) -> None:
    statements = list(map_bg_tr(await _fetch(ASSOC)))
    elena = _interests(statements, "Елена Иванова")
    bo = [i for i in elena if i.get("beneficialOwnershipOrControl")]
    assert bo[0]["share"] == {"exclusiveMinimum": 25.0}
    assert {i["type"] for i in elena if not i.get("beneficialOwnershipOrControl")} == {"boardMember"}


async def test_a_company_sole_owner_is_an_entity_with_its_eik_and_all_the_shares(index) -> None:
    statements = list(map_bg_tr(await _fetch(EOOD)))
    entities = _by_type(statements)["entity"]
    parent = next(e for e in entities if e["recordDetails"]["name"] == "МАЙКА АД")
    assert parent["recordDetails"]["identifiers"][0]["id"] == PARENT
    held = _interests(statements, "МАЙКА АД")
    assert held == [held[0]]
    assert held[0]["type"] == "shareholding" and held[0]["share"] == {"exact": 100.0}
    assert statements[0]["recordDetails"].get("dissolutionDate") == "2021-01-05"


def test_a_procurator_is_marked_as_an_officer() -> None:
    from opencheck.bods.officer_roles import officer_position

    bundle = {
        "eik": OOD, "name": "X", "legal_form_code": "OOD", "status_class": "live",
        "parties": [{"name": "Стоян Стоянов", "kind": "person", "field": "Procurators"}],
    }
    rel = [s for s in map_bg_tr(bundle) if s["recordType"] == "relationship"][0]
    interest = rel["recordDetails"]["interests"][0]
    assert interest["type"] == "otherInfluenceOrControl"
    assert officer_position(interest) == "Procurator"


def test_a_senior_manager_named_for_want_of_an_owner_is_no_owner() -> None:
    bundle = {
        "eik": OOD, "name": "X", "legal_form_code": "OOD", "status_class": "live",
        "beneficial_owners": [{"name": "Y Z", "kind": "person",
                               "rights": [{"code": "SENIOR_MANAGEMENT_POSITION"}]}],
    }
    rel = [s for s in map_bg_tr(bundle) if s["recordType"] == "relationship"][0]
    interest = rel["recordDetails"]["interests"][0]
    assert interest["type"] == "seniorManagingOfficial"
    assert interest["beneficialOwnershipOrControl"] is False


# --------------------------------------------------------------------------
# The finding
# --------------------------------------------------------------------------


async def test_the_findings(index) -> None:
    live = finding_bg_tr(await _fetch(OOD))
    assert live.startswith("Registered on 2021-01-04")
    assert "1 beneficial owner filed" in live
    deleted = finding_bg_tr(await _fetch(EOOD))
    assert deleted.startswith("Deleted from the register on 2021-01-05")
    assert "no beneficial owners in the open data since 2021" in deleted


# --------------------------------------------------------------------------
# The disclosure status
# --------------------------------------------------------------------------


def test_no_owner_in_the_files_is_none_on_record_since_2021_never_not_public() -> None:
    from opencheck.disclosure import BG_SINCE_2021, bundle_hints

    hints = bundle_hints("bg_tr", {"eik": OOD, "beneficial_owners": [], "parties": []})
    assert {(h["value"], h["covers"]) for h in hints} == {
        ("none_filed", "beneficial_owners"), ("none_filed", "shareholders")}
    assert all(h["details"] == [BG_SINCE_2021] for h in hints)
    named = bundle_hints("bg_tr", {"beneficial_owners": [
        {"name": "Y", "rights": [{"code": "SENIOR_MANAGEMENT_POSITION"}]}],
        "parties": [{"name": "Z", "field": "SoleCapitalOwner"}]})
    assert [h["value"] for h in named] == ["none_exist"]
    assert bundle_hints("bg_tr", {"not_found": True}) == []
