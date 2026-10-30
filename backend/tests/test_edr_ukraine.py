"""Tests for the Ukraine ЄДР adapter and BODS mapper.

The adapter queries a local SQLite index (built by
scripts/build_edr_ukraine_index.py from the weekly data.gov.ua XML export).
Tests write a small index of the fixture rows to a temporary file and point
``EDR_UKRAINE_DB_FILE`` at it (the ``jar_lithuania`` fixture shape), so no
network access is needed and the module-level shared connection is exercised
exactly as production exercises it.

The fixture strings are **verbatim rows from the 8 September 2026 export** —
not invented shapes. Several of them exist to pin a specific hard-won reading:
the indirect-percentage parenthetical, the hryvnia founder holding, the
corporate founder's bare EDRPOU, and the four flavours of filed absence.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from opencheck.config import get_settings
from opencheck.sources import edr_ukraine

from opencheck.bods.mapper import map_edr_ukraine
from opencheck.sources.base import SearchKind
from opencheck.sources.edr_ukraine import (
    ABSENCE_BODS_REASON,
    UA_RA_CODES,
    EdrUkraineAdapter,
    classify_absence,
    normalise_edrpou,
    parse_beneficiary,
    parse_capital,
    parse_founder,
    parse_role_holder,
)

# --- verbatim strings from the real export ---------------------------------

BO_DIRECT = (
    "Пелих Марина Аркадіївна; громадянство: Україна; "
    "Прямий вирішальний вплив; відсоток частки - 100"
)
BO_INDIRECT = (
    "ВАРЧЕНКО   АНДРІЙ  ВІТАЛІЙОВИЧ; громадянство: Україна; "
    "Непрямий вирішальний вплив; відсоток частки (непрямий вплив) - 0.84; "
    "інший характер та міра впливу: ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ"
)
BO_BOTH = (
    "Васадзе Таріел Шакрович; громадянство: Україна; "
    "Прямий та непрямий вирішальний вплив; відсоток частки - 35; "
    "відсоток частки (непрямий вплив) - 65"
)
BO_ABSENT_NO_PERSON = (
    "причина відсутності: Відсутні фізичні особи, які відповідають статусу "
    "кінцевого бенефіціарного власника юридичної особи"
)
BO_ABSENT_STATUTE = "причина відсутності: не передбачено законодавством"
BO_ABSENT_STRUCTURE = (
    "причина відсутності: Причина відсутності кінцевого бенефіціарного "
    "власника юридичної особи зазначена у структурі власності"
)
BO_ABSENT_BLANK = "причина відсутності: -"

FOUNDER_PERSON = (
    "ПЕЛИХ МАРИНА АРКАДІЇВНА; громадянство: Україна; "
    "розмір частки - 134493517,83 грн."
)
FOUNDER_ENTITY = "КОЗЕЛЕЦЬКА РАЙОННА РАДА; 22824090; розмір частки - 0,00 грн."
SIGNER = (
    "КОВАЛЬЧУК АНДРІЙ ЛЕОНІДОВИЧ; (згідно статуту) - керівник"
)
MEMBER_CHAIR = "СТАРЧЕНКО  ВАЛЕРІЙ ВАСИЛЬОВИЧ; Голова Організації, Голова Правління"
MEMBER_PLAIN = "Болюх   Андрій  Васильович; Член Ради Об'єднання"


# --- parser tests ----------------------------------------------------------


def test_ra_codes_are_a_set_of_three():
    # All three carry the EDRPOU in registeredAs; keying on the register's own
    # code alone loses 15% of Ukrainian LEIs.
    assert UA_RA_CODES == {"RA000567", "RA001026", "RA001027"}


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("12345678", "12345678"),
        ("1234567", "01234567"),  # short codes are zero-padded to match GLEIF
        (" 00013480 ", "00013480"),
        ("ЄДР", ""),
        ("", ""),
    ],
)
def test_normalise_edrpou(raw, expected):
    assert normalise_edrpou(raw) == expected


def test_parse_beneficiary_direct():
    p = parse_beneficiary(BO_DIRECT)
    assert p["kind"] == "named"
    assert p["name"] == "Пелих Марина Аркадіївна"
    assert p["citizenship"] == "Україна"
    assert p["influence"] == "direct"
    assert p["pct_direct"] == 100.0
    assert p["pct_indirect"] is None


def test_parse_beneficiary_indirect_percentage_is_not_read_as_direct():
    """The two percentage fields share the prefix 'відсоток частки'.

    A naive direct-percentage pattern matches the indirect field's prefix and
    silently reports an indirect holding as a direct one.
    """
    p = parse_beneficiary(BO_INDIRECT)
    assert p["influence"] == "indirect"
    assert p["pct_indirect"] == 0.84
    assert p["pct_direct"] is None
    assert p["note"].startswith("ТОВАРИСТВО")


def test_parse_beneficiary_both_directions():
    p = parse_beneficiary(BO_BOTH)
    assert p["influence"] == "both"
    assert p["pct_direct"] == 35.0
    assert p["pct_indirect"] == 65.0


def test_parse_beneficiary_absence():
    p = parse_beneficiary(BO_ABSENT_NO_PERSON)
    assert p["kind"] == "absence"
    assert "Відсутні фізичні особи" in p["reason_text"]


def test_parse_beneficiary_unparsed_is_not_coerced():
    # The one malformed record in the whole register must stay visible rather
    # than become a person named "громадянство: Україна".
    assert parse_beneficiary("громадянство: Україна")["kind"] == "unparsed"


@pytest.mark.parametrize(
    "text,expected",
    [
        (BO_ABSENT_NO_PERSON, "no_qualifying_person"),
        (BO_ABSENT_STATUTE, "statutory_exemption"),
        (BO_ABSENT_STRUCTURE, "structure_filed"),
        (BO_ABSENT_BLANK, "blank"),
    ],
)
def test_classify_absence(text, expected):
    reason = parse_beneficiary(text)["reason_text"]
    assert classify_absence(reason) == expected


def test_absence_boilerplate_does_not_swallow_the_category():
    """'відсутності' is part of the field LABEL, not the reason.

    Classifying without stripping it claims every reason as
    'no_qualifying_person' — including the ones that point at the separately
    filed ownership structure.
    """
    reason = parse_beneficiary(BO_ABSENT_STRUCTURE)["reason_text"]
    assert "відсутності" in reason.lower()
    assert classify_absence(reason) == "structure_filed"


def test_every_absence_code_maps_to_a_bods_reason():
    codes = {c for c, _ in [(k, v) for k, v in ABSENCE_BODS_REASON.items()]}
    assert "noBeneficialOwners" in ABSENCE_BODS_REASON.values()
    for code in ("no_qualifying_person", "structure_filed", "state_or_municipal",
                 "statutory_exemption", "legal_form_exemption", "not_stated",
                 "blank", "other"):
        assert code in codes


def test_parse_founder_person_and_entity():
    person = parse_founder(FOUNDER_PERSON)
    assert person["name"] == "ПЕЛИХ МАРИНА АРКАДІЇВНА"
    assert person["code"] is None
    assert person["amount_uah"] == 134493517.83
    entity = parse_founder(FOUNDER_ENTITY)
    assert entity["code"] == "22824090"
    assert entity["amount_uah"] == 0.0


def test_parse_role_holder():
    assert parse_role_holder(SIGNER)["name"] == "КОВАЛЬЧУК АНДРІЙ ЛЕОНІДОВИЧ"
    assert "керівник" in parse_role_holder(SIGNER)["role"]
    assert parse_role_holder(MEMBER_CHAIR)["role"].startswith("Голова")


def test_parse_capital():
    assert parse_capital("134 493 517,83 грн.") == 134493517.83
    assert parse_capital("") is None


# --- adapter tests ---------------------------------------------------------


def _make_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE entity (edrpou TEXT PRIMARY KEY, name TEXT, short_name TEXT,
          opf TEXT, stan TEXT, active INTEGER, capital REAL, registration TEXT);
        CREATE TABLE beneficiary (edrpou TEXT, seq INTEGER, kind TEXT, name TEXT,
          citizenship TEXT, influence TEXT, pct_direct REAL, pct_indirect REAL,
          note TEXT, reason_text TEXT, raw TEXT);
        CREATE TABLE founder (edrpou TEXT, seq INTEGER, name TEXT, code TEXT,
          citizenship TEXT, amount_uah REAL);
        CREATE TABLE signer (edrpou TEXT, seq INTEGER, name TEXT, role TEXT);
        CREATE TABLE member (edrpou TEXT, seq INTEGER, name TEXT, role TEXT);
        CREATE TABLE executive_power (edrpou TEXT, seq INTEGER, name TEXT, code TEXT);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany(
        "INSERT INTO meta VALUES (?,?)",
        [
            ("schema_version", edr_ukraine.INDEX_SCHEMA_VERSION),
            ("export_date", "2026-09-08"),
            ("built_at", "2026-09-14T10:00:00Z"),
            ("tier", "graph"),
        ],
    )
    conn.execute(
        "INSERT INTO entity VALUES (?,?,?,?,?,?,?,?)",
        ("35265987", 'ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ "КИЇВ-АРТ"',
         'ТОВ "КИЇВ-АРТ"', "Товариство з обмеженою відповідальністю",
         "зареєстровано", 1, 134493517.83, "Дата: 12.07.2007"),
    )
    b = parse_beneficiary(BO_DIRECT)
    conn.execute(
        "INSERT INTO beneficiary VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("35265987", 0, "named", b["name"], b["citizenship"], b["influence"],
         b["pct_direct"], b["pct_indirect"], b["note"], None, BO_DIRECT),
    )
    f = parse_founder(FOUNDER_PERSON)
    conn.execute(
        "INSERT INTO founder VALUES (?,?,?,?,?,?)",
        ("35265987", 0, f["name"], f["code"], f["citizenship"], f["amount_uah"]),
    )
    s = parse_role_holder(SIGNER)
    conn.execute(
        "INSERT INTO signer VALUES (?,?,?,?)",
        ("35265987", 0, s["name"], s["role"]),
    )
    conn.commit()
    return conn


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("EDR_UKRAINE_DB_FILE", None)
    else:
        os.environ["EDR_UKRAINE_DB_FILE"] = str(path)
    get_settings.cache_clear()
    edr_ukraine.reset_connection()


@pytest.fixture
def adapter():
    """No index: the adapter answers stubs and covers nothing."""
    _point_at(None)
    yield EdrUkraineAdapter()
    _point_at(None)


@pytest.fixture
def live_adapter(tmp_path: Path):
    """An index of the fixture rows, on disk, found through the settings."""
    path = tmp_path / "edr_ukraine.sqlite"
    _make_db(path).close()
    _point_at(path)
    yield EdrUkraineAdapter()
    _point_at(None)


def test_requires_no_api_key(adapter):
    assert not adapter.info.requires_api_key
    assert adapter.info.country == "UA"


@pytest.mark.asyncio
async def test_fetch_full_bundle(live_adapter):
    bundle = await live_adapter.fetch("35265987", legal_name="KYIV-ART")
    assert bundle["is_stub"] is False
    assert bundle["edrpou"] == "35265987"
    assert len(bundle["beneficiaries"]) == 1
    assert len(bundle["founders"]) == 1
    assert len(bundle["signers"]) == 1
    assert bundle["executive_power"] is None


@pytest.mark.asyncio
async def test_fetch_stub_when_no_db(adapter):
    bundle = await adapter.fetch("35265987", legal_name="KYIV-ART")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_unknown_code_is_a_note_card_not_a_stub(live_adapter):
    """The index answered and holds no row: ``not_found`` with the coverage
    note, ``is_stub`` False (a stub is dropped by the pipeline before the
    card is built — the JAR / INPI / ONRC shape)."""
    bundle = await live_adapter.fetch("00000001", legal_name="NOBODY LLC")
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"] == edr_ukraine.COVERAGE_NOT_IN_INDEX
    assert bundle["export_date"] == "2026-09-08"
    assert bundle["entity"] is None and bundle["beneficiaries"] == []
    assert list(map_edr_ukraine(bundle)) == []


@pytest.mark.asyncio
async def test_fetch_blank_code_is_a_stub(live_adapter):
    assert (await live_adapter.fetch(""))["is_stub"] is True


@pytest.mark.asyncio
async def test_search_returns_nothing_without_db(adapter):
    assert await adapter.search("КИЇВ-АРТ", SearchKind.ENTITY) == []


def test_dispatch_is_gated_on_the_index(adapter):
    assert adapter.covers_lei("254900UI956MSQXOL287") is False
    assert adapter.info.live_available is False


def test_dispatch_opens_with_the_index(live_adapter):
    assert live_adapter.covers_lei("254900UI956MSQXOL287") is True
    assert live_adapter.info.live_available is True


def test_an_index_of_another_schema_is_ignored(tmp_path: Path):
    path = tmp_path / "edr_ukraine.sqlite"
    conn = _make_db(path)
    conn.execute("UPDATE meta SET value = '0' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(path)
    try:
        assert edr_ukraine.index_available() is False
    finally:
        _point_at(None)


def test_the_adapter_declares_the_lookup_wiring():
    """Three authorities file the EDRPOU; the deriver names all three."""
    (deriver,) = EdrUkraineAdapter.lookup_derivers
    assert deriver.ra_codes == UA_RA_CODES
    assert deriver.derived_key == "ua_edrpou"
    assert deriver.normalise("35265987") == "35265987"
    assert deriver.normalise("1234567") == "01234567"
    assert EdrUkraineAdapter.lookup_pass_legal_name is True


@pytest.mark.asyncio
async def test_search_finds_by_name_with_db(live_adapter):
    hits = await live_adapter.search("КИЇВ-АРТ", SearchKind.ENTITY)
    assert hits and hits[0].is_stub is False
    assert hits[0].identifiers["ua_edrpou"] == "35265987"


# --- mapper tests ----------------------------------------------------------


def _bundle(**over):
    base = {
        "source_id": "edr_ukraine",
        "edrpou": "35265987",
        "name": 'ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ "КИЇВ-АРТ"',
        "entity": {
            "edrpou": "35265987",
            "name": 'ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ "КИЇВ-АРТ"',
            "short_name": 'ТОВ "КИЇВ-АРТ"',
            "opf": "Товариство з обмеженою відповідальністю",
            "stan": "зареєстровано",
            "active": 1,
            "capital": 134493517.83,
        },
        "beneficiaries": [],
        "founders": [],
        "signers": [],
        "members": [],
        "executive_power": None,
        "link": "https://data.gov.ua/",
        "is_stub": False,
    }
    base.update(over)
    return base


def _by_type(statements):
    out = {"entity": [], "person": [], "relationship": []}
    for s in statements:
        out[s["recordType"]].append(s)
    return out


def _ben_row(raw, seq=0):
    p = parse_beneficiary(raw)
    return {"seq": seq, **p}


def test_mapper_skips_stub():
    assert list(map_edr_ukraine(_bundle(is_stub=True))) == []


def test_mapper_emits_company_with_ua_identifier():
    stmts = _by_type(list(map_edr_ukraine(_bundle())))
    assert len(stmts["entity"]) == 1
    rd = stmts["entity"][0]["recordDetails"]
    assert rd["jurisdiction"] == {"name": "Ukraine", "code": "UA"}
    assert rd["identifiers"][0]["scheme"] == "UA-EDR"
    assert rd["identifiers"][0]["id"] == "35265987"


def test_beneficial_owner_is_asserted_as_such():
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_DIRECT)])))
    )
    assert len(stmts["person"]) == 1
    rel = stmts["relationship"][0]
    interest = rel["recordDetails"]["interests"][0]
    assert interest["beneficialOwnershipOrControl"] is True
    assert interest["share"]["exact"] == 100.0
    assert interest["directOrIndirect"] == "direct"


def test_both_directions_yield_two_interests():
    """A record declaring direct AND indirect influence must not be averaged."""
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_BOTH)])))
    )
    interests = stmts["relationship"][0]["recordDetails"]["interests"]
    assert len(interests) == 2
    assert {i["directOrIndirect"] for i in interests} == {"direct", "indirect"}
    assert {i["share"]["exact"] for i in interests} == {35.0, 65.0}


def test_absence_emits_unspecified_party_with_the_registers_own_words():
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_ABSENT_NO_PERSON)])))
    )
    assert stmts["person"] == []
    rel = stmts["relationship"][0]
    party = rel["recordDetails"]["interestedParty"]
    assert party["reason"] == "noBeneficialOwners"
    assert "Відсутні фізичні особи" in party["description"]


def test_absence_carries_no_interest():
    """Unlike the CH PSC statements, no Ukrainian absence reason asserts that a
    beneficial owner exists — so none of them may carry an interest."""
    for raw in (BO_ABSENT_NO_PERSON, BO_ABSENT_STATUTE, BO_ABSENT_BLANK,
                BO_ABSENT_STRUCTURE):
        stmts = _by_type(list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(raw)]))))
        rel = stmts["relationship"][0]
        assert rel["recordDetails"]["interests"] == [], raw


def test_an_exemption_is_not_reported_as_a_finding_of_no_owner():
    """'Not required to file' and 'we looked and there is nobody' are different
    claims, and BODS v0.4 has a distinct code for each. An earlier draft of this
    mapper collapsed the exemptions onto noBeneficialOwners."""
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_ABSENT_STATUTE)])))
    )
    party = stmts["relationship"][0]["recordDetails"]["interestedParty"]
    assert party["reason"] == "subjectExemptFromDisclosure"
    # The codelist carries the kind; the description carries the registrar's
    # own words, which are finer grained than any codelist.
    assert party["description"] == "не передбачено законодавством"


def test_a_finding_of_no_owner_stays_a_finding():
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_ABSENT_NO_PERSON)])))
    )
    assert (
        stmts["relationship"][0]["recordDetails"]["interestedParty"]["reason"]
        == "noBeneficialOwners"
    )


def test_a_blank_reason_is_unknown_not_an_assertion_of_absence():
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(beneficiaries=[_ben_row(BO_ABSENT_BLANK)])))
    )
    assert (
        stmts["relationship"][0]["recordDetails"]["interestedParty"]["reason"]
        == "unknown"
    )


def test_every_mapped_reason_is_in_the_bods_v04_codelist():
    """Pins the whole mapping against the real v0.4 enum.

    `unknownUnknown` is not a BODS value and an earlier draft used it; the
    fixture-only tests passed and the schema validator caught it on live data.
    """
    valid = {
        "noBeneficialOwners",
        "subjectUnableToConfirmOrIdentifyBeneficialOwner",
        "interestedPartyHasNotProvidedInformation",
        "subjectExemptFromDisclosure",
        "interestedPartyExemptFromDisclosure",
        "unknown",
        "informationUnknownToPublisher",
    }
    assert set(ABSENCE_BODS_REASON.values()) <= valid


def test_founder_share_is_derived_only_when_it_reconciles():
    f = parse_founder(FOUNDER_PERSON)
    row = {"seq": 0, **f}
    stmts = _by_type(list(map_edr_ukraine(_bundle(founders=[row]))))
    interest = stmts["relationship"][0]["recordDetails"]["interests"][0]
    # Sole founder, holding == declared capital → 100 %.
    assert interest["type"] == "shareholding"
    assert interest["share"]["exact"] == 100.0
    # and it is NOT a beneficial ownership claim
    assert "beneficialOwnershipOrControl" not in interest


def test_founder_share_is_withheld_when_it_does_not_reconcile():
    f = parse_founder(FOUNDER_PERSON)
    row = {"seq": 0, **f}
    bundle = _bundle(founders=[row])
    bundle["entity"]["capital"] = 999_999_999.0  # holdings no longer add up
    stmts = _by_type(list(map_edr_ukraine(bundle)))
    interest = stmts["relationship"][0]["recordDetails"]["interests"][0]
    assert interest["type"] == "unknownInterest"
    assert "share" not in interest
    assert "UAH" in interest["details"]


def test_derived_share_is_annotated_as_opencheck_arithmetic():
    f = parse_founder(FOUNDER_PERSON)
    stmts = _by_type(list(map_edr_ukraine(_bundle(founders=[{"seq": 0, **f}]))))
    rel = stmts["relationship"][0]
    annotations = rel.get("annotations") or []
    assert annotations, "a derived percentage must say it was derived"
    assert "derived by OpenCheck" in json.dumps(annotations, ensure_ascii=False)


def test_corporate_founder_is_an_entity_party():
    f = parse_founder(FOUNDER_ENTITY)
    stmts = _by_type(list(map_edr_ukraine(_bundle(founders=[{"seq": 0, **f}]))))
    # company + the founding council
    assert len(stmts["entity"]) == 2
    assert stmts["person"] == []


def test_signer_is_a_senior_managing_official_not_a_shareholder():
    s = parse_role_holder(SIGNER)
    stmts = _by_type(list(map_edr_ukraine(_bundle(signers=[{"seq": 0, **s}]))))
    interest = stmts["relationship"][0]["recordDetails"]["interests"][0]
    assert interest["type"] == "seniorManagingOfficial"
    assert "beneficialOwnershipOrControl" not in interest


def test_member_chair_and_plain_member_are_distinguished():
    chair = parse_role_holder(MEMBER_CHAIR)
    plain = parse_role_holder(MEMBER_PLAIN)
    stmts = _by_type(
        list(map_edr_ukraine(_bundle(members=[{"seq": 0, **chair}, {"seq": 1, **plain}])))
    )
    types = [r["recordDetails"]["interests"][0]["type"] for r in stmts["relationship"]]
    assert types == ["boardChair", "boardMember"]


def test_state_control_uses_the_bods_soe_shape():
    """BODS: an SOE's entity statement MUST be the subject of a relationship
    connecting it to an entity typed 'state' or 'stateBody'."""
    bundle = _bundle(
        executive_power={"name": "МІНІСТЕРСТВО ФІНАНСІВ УКРАЇНИ", "code": "00013480"}
    )
    stmts = _by_type(list(map_edr_ukraine(bundle)))
    state = [
        e for e in stmts["entity"]
        if e["recordDetails"]["entityType"]["type"] == "stateBody"
    ]
    assert len(state) == 1
    # jurisdiction is required on a state node — it says WHICH state
    assert state[0]["recordDetails"]["jurisdiction"]["code"] == "UA"
    rel = stmts["relationship"][0]
    assert rel["recordDetails"]["subject"] == stmts["entity"][0]["recordId"]
    assert rel["recordDetails"]["interestedParty"] == state[0]["recordId"]
    interest = rel["recordDetails"]["interests"][0]
    assert interest["type"] == "controlByLegalFramework"
    # The register does not distinguish "belongs to" from "holds >=25%", and
    # publishes no percentage — so none may be asserted.
    assert "share" not in interest


def test_state_control_absent_when_the_register_names_no_authority():
    stmts = _by_type(list(map_edr_ukraine(_bundle())))
    assert not [
        e for e in stmts["entity"]
        if e["recordDetails"]["entityType"]["type"] == "stateBody"
    ]


# --- the EDR wiring (29 Oct 2026): placeholders, the sentence, the card ----


@pytest.mark.parametrize(
    "name",
    [
        "ФІЗИЧНІ ОСОБИ",
        "НЕВІДОМІ ОСОБИ",
        "ПРИВАТНІ ОСОБИ В КІЛЬКОСТІ 112 ОСІБ",
        "ГРОМАДЯНИ УКРАЇНИ",
        "АКЦІОНЕРИ",
        "ЮРИДИЧНІ ТА ФІЗИЧНІ ОСОБИ",
        "ІНШІ АКЦІОНЕРИ",
        "НЕВИЗНАЧЕНА ФІЗИЧНА ОСОБА",
        "НЕВИЗНАЧЕНА ОСОБА",
        "ЗАГАЛЬНА КІЛЬКІСТЬ АКЦІОНЕРІВ 1520",
        "ВСЬОГО АКЦІОНЕРІВ",
        "ТРИ ФІЗИЧНІ ОСОБИ",
        "ФІЗИЧНИХ ОСІБ",
        "ФІЗ ОСОБИ",
        "[ЗАСНОВНИК]",
        "ІНФОРМАЦІЯ ПРО ЗАСНОВНИКІВ ВІДСУТНЯ",
        "ЗГІДНО РЕЄСТРУ АКЦІОНЕРІВ",
        "ТРУДОВИЙ КОЛЕКТИВ",
    ],
)
def test_a_placeholder_founder_is_an_unspecified_party_never_a_person(name: str):
    """"ФІЗИЧНІ ОСОБИ" names a class of people, not a person: no person
    statement reaches the graph or the related-party screens; the holding is
    an unspecified party carrying the register's words (the GEM shape)."""
    from opencheck.bods.mappers.ukraine import founder_is_placeholder

    assert founder_is_placeholder(name)
    assert not founder_is_placeholder("ПЕЛИХ МАРИНА АРКАДІЇВНА")
    founders = [
        {"seq": 0, "name": name, "code": None, "citizenship": None, "amount_uah": 1000.0},
    ]
    out = _by_type(map_edr_ukraine(_bundle(founders=founders)))
    assert out["person"] == []
    (rel,) = out["relationship"]
    party = rel["recordDetails"]["interestedParty"]
    assert party["reason"] == "informationUnknownToPublisher"
    assert party["description"] == name
    (interest,) = rel["recordDetails"]["interests"]
    assert interest["type"] == "unknownInterest"
    assert "beneficialOwnershipOrControl" not in interest
    assert "1,000.00 UAH" in interest["details"]


@pytest.mark.parametrize(
    "name",
    [
        "КЛАРІКС КЕПІТАЛ ЛІМІТЕД",
        "КОМПАНІЯ ЕКСТРА ВЕНЧУР ЛТД (EXTRA VENTURE LTD)",
        "ПРИВАТНА АКЦІОНЕРНА КОМПАНІЯ «БІЗ КОРП. ЛТД»",
        "КВІНТЕР ЛП LP 21052",
        "STELLAR STAR LTD",
        "ACME HOLDINGS B.V.",
    ],
)
def test_a_codeless_founder_named_by_its_legal_form_is_an_entity_with_no_jurisdiction(name: str):
    """A foreign company founder carries no EDRPOU and the register types
    nothing; its legal-form words make it an entity, with no identifier and
    no assumed country (Phase 295), never a person on the related-party
    screens. Measured on the 27 Oct 2026 index: 17,212 of 1,029,088
    codeless founder rows; none shaped like a person's three names."""
    from opencheck.bods.mappers.ukraine import founder_is_organisation

    assert founder_is_organisation(name)
    for person in (
        "ПЕЛИХ МАРИНА АРКАДІЇВНА",
        "ТРОФІМОВА РАДА ВОЛОДИМИРІВНА",  # a given name, not a council
        "СПІЛКА ОЛЕГ ВІКТОРОВИЧ",  # a surname, not a union
    ):
        assert not founder_is_organisation(person), person
    founders = [{"seq": 0, "name": name, "code": None, "amount_uah": 500.0}]
    out = _by_type(map_edr_ukraine(_bundle(founders=founders)))
    assert out["person"] == []
    (subject, party) = out["entity"]
    assert party["recordDetails"]["name"] == name
    assert "jurisdiction" not in party["recordDetails"]
    assert not party["recordDetails"].get("identifiers")
    (rel,) = out["relationship"]
    assert rel["recordDetails"]["interestedParty"] == party["recordId"]


def test_the_finding_sentence_counts_in_one_voice():
    from opencheck.findings import MAX_FINDING_CHARS, finding_edr_ukraine

    b = parse_beneficiary(BO_DIRECT)
    bundle = _bundle(
        beneficiaries=[{"seq": 0, "kind": "named", **b}],
        founders=[{"seq": 0, "name": "ПЕЛИХ МАРИНА АРКАДІЇВНА", "code": None, "amount_uah": 1.0}],
        signers=[{"seq": 0, "name": "ПЕЛИХ МАРИНА АРКАДІЇВНА", "role": "керівник"}],
    )
    sentence = finding_edr_ukraine(bundle)
    assert sentence == (
        "On the register, limited liability company (ТОВ), 1 beneficial owner "
        "declared, 1 founder on file, 1 signatory."
    )
    assert len(sentence) <= MAX_FINDING_CHARS

    absent = _bundle(
        beneficiaries=[{"seq": 0, "kind": "absence", "reason_text": "засновником є держава", "raw": ""}],
    )
    assert "no beneficial owner declared: state or municipal founder" in finding_edr_ukraine(absent)
    assert "0 founders on file" in finding_edr_ukraine(absent)

    terminated = _bundle(entity={**_bundle()["entity"], "stan": "припинено"})
    assert finding_edr_ukraine(terminated).startswith("Terminated, ")

    assert finding_edr_ukraine({"is_stub": True}) is None
    assert finding_edr_ukraine({"is_stub": False, "not_found": True}).startswith(
        "Not in OpenCheck's index"
    )


@pytest.mark.asyncio
async def test_the_hit_builder_asserts_the_code_only_when_the_register_did(live_adapter):
    from opencheck.routers.hit_builders import _bh_edr_ukraine

    class Ctx:
        legal_name = "KYIV-ART"

    found = await live_adapter.fetch("35265987", legal_name="KYIV-ART")
    hit = _bh_edr_ukraine(found, "35265987", Ctx())
    assert hit.identifiers == {"ua_edrpou": "35265987"}
    assert hit.summary == "UA-EDR 35265987"
    assert hit.finding and "1 beneficial owner declared" in hit.finding
    assert hit.name.startswith("ТОВАРИСТВО")

    class Other:
        legal_name = "NOBODY LLC"

    missing = await live_adapter.fetch("00000001", legal_name="NOBODY LLC")
    card = _bh_edr_ukraine(missing, "00000001", Other())
    assert card.identifiers == {}
    assert card.name == "NOBODY LLC"
    assert "not in the index" in card.summary
    assert card.raw["coverage_note"] == edr_ukraine.COVERAGE_NOT_IN_INDEX


# --- the role tail (post-deploy test, 30 Oct 2026) --------------------------


@pytest.mark.parametrize(
    "raw, name, role",
    [
        ("КОМАРОВ ОЛЕКСАНДР ВАЛЕРІЙОВИЧ - керівник", "КОМАРОВ ОЛЕКСАНДР ВАЛЕРІЙОВИЧ", "керівник"),
        ("ОЛІЙНИК ОКСАНА ВІКТОРІВНА; (Повноваження: x) - представник",
         "ОЛІЙНИК ОКСАНА ВІКТОРІВНА", "(Повноваження: x) - представник"),
        (SIGNER, "КОВАЛЬЧУК АНДРІЙ ЛЕОНІДОВИЧ", "(згідно статуту) - керівник"),
        # A dash that is not a role word is left alone.
        ("ІВАНОВ ІВАН - ПЕТРОВ ПЕТРО", "ІВАНОВ ІВАН - ПЕТРОВ ПЕТРО", None),
    ],
)
def test_a_trailing_role_word_never_stays_on_the_name(raw, name, role):
    parsed = parse_role_holder(raw)
    assert parsed["name"] == name
    assert parsed["role"] == role


def test_an_index_row_built_before_the_tail_rule_is_read_right():
    """The 27 Oct 2026 asset holds 398,929 signer rows with the tail on the
    name; the read-time strip means no rebuild is needed to say the person's
    name correctly."""
    from opencheck.sources.edr_ukraine import strip_role_tail

    row = {"edrpou": "21673832", "seq": 0, "name": "КОМАРОВ ОЛЕКСАНДР ВАЛЕРІЙОВИЧ - керівник", "role": None}
    fixed = strip_role_tail(row)
    assert fixed["name"] == "КОМАРОВ ОЛЕКСАНДР ВАЛЕРІЙОВИЧ"
    assert fixed["role"] == "керівник"
    assert row["name"].endswith("керівник")  # the input is not mutated
    clean = {"edrpou": "x", "seq": 1, "name": "ОЛІЙНИК ОКСАНА ВІКТОРІВНА", "role": "x - представник"}
    assert strip_role_tail(clean) is clean
