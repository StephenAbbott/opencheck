"""Tests for the Ukraine ЄДР adapter and BODS mapper.

The adapter queries a local SQLite index (built by
scripts/build_edr_ukraine_index.py from the weekly data.gov.ua XML export).
Tests inject an in-memory DB via ``adapter._db`` so no filesystem or network
access is needed.

The fixture strings are **verbatim rows from the 8 September 2026 export** —
not invented shapes. Several of them exist to pin a specific hard-won reading:
the indirect-percentage parenthetical, the hryvnia founder holding, the
corporate founder's bare EDRPOU, and the four flavours of filed absence.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

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


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
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
        """
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


@pytest.fixture
def adapter():
    return EdrUkraineAdapter()


@pytest.fixture
def live_adapter():
    a = EdrUkraineAdapter()
    a._db = _make_db()  # inject in-memory DB (bypasses settings/filesystem)
    return a


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
async def test_fetch_stub_when_unknown_code(live_adapter):
    assert (await live_adapter.fetch("00000000"))["is_stub"] is True


@pytest.mark.asyncio
async def test_search_returns_stub_without_db(adapter):
    hits = await adapter.search("КИЇВ-АРТ", SearchKind.ENTITY)
    assert hits and hits[0].is_stub is True


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
    assert rel["recordDetails"]["subject"] == stmts["entity"][0]["statementId"]
    assert rel["recordDetails"]["interestedParty"] == state[0]["statementId"]
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
