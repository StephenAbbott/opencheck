"""A person's other companies on the Estonian register, keyed on the
register's pseudonymous ``isikukood_hash`` (the cross-company person index).

Stephen's decisions (16 Oct 2026): hash only — never a name search; company,
role, since and share, no addresses; listed, never drawn as graph nodes.
Built on the ``test_ariregister_index`` fixture, with Veronika Oškina (one
node across 007 Agent & Partners OÜ's lists) also put on Eesti Energia's
supervisory board, so she has another company to find.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods.mappers.estonia import ee_person_role_label
from opencheck.sources import ariregister
from tests.test_ariregister_index import (
    AGENT,
    EESTI,
    H_KALEV,
    H_VERONIKA,
    HAUDE,
    _build,
    _off_card,
    _off_cards,
    _point_at,
)

NOBODY = "00000000-0000-5000-8000-000000000000"


def _with_veronika_on_eesti_board() -> list[dict]:
    cards = _off_cards()
    for company in cards:
        if company["ariregistri_kood"] == int(EESTI):
            company["kaardivalised_isikud"].append(
                _off_card(kirje_id=45, isiku_roll="N", isiku_roll_tekstina="Nõukogu liige",
                          eesnimi="Veronika", nimi_arinimi="Oškina", isikukood_hash=H_VERONIKA,
                          osaluse_suurus=None, kontrolli_allikas=None, kontrolli_allikas_tekstina=None,
                          kontrolli_allika_kpv=None, algus_kpv="01.06.2025"),
            )
    return cards


@pytest.fixture()
def person_index(tmp_path: Path):
    out, meta = _build(tmp_path, off_cards=_with_veronika_on_eesti_board())
    _point_at(out)
    yield out
    _point_at(None)


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)


def test_the_builder_indexes_the_hash_on_all_four_people_tables(person_index) -> None:
    conn = sqlite3.connect(person_index)
    indexes = {name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    assert {"officer_person", "shareholder_person", "beneficial_owner_person", "off_card_person_person"} <= indexes
    plan = " ".join(str(r) for r in conn.execute(
        "EXPLAIN QUERY PLAN SELECT code FROM shareholder WHERE isikukood_hash = ?", (b"x" * 16,)
    ))
    assert "shareholder_person" in plan


def test_every_list_a_person_is_on_in_every_company(person_index) -> None:
    found = ariregister.companies_for_hash(H_VERONIKA)
    assert found is not None
    assert found["name"] == "Veronika Oškina"
    assert found["companies"] == 2
    lists = {(r["code"], r["list"]) for r in found["rows"]}
    assert lists == {
        (AGENT, "registry_card"), (AGENT, "shareholders"), (AGENT, "beneficial_owners"), (AGENT, "off_card"),
        (EESTI, "off_card"),
    }
    board = next(r for r in found["rows"] if r["code"] == EESTI)
    assert board["role"] == "N" and board["since"] == "2025-06-01"
    assert board["name"] == "Eesti Energia AS"
    holding = next(r for r in found["rows"] if r["code"] == AGENT and r["list"] == "shareholders")
    assert holding["share_size"] and holding["percent"] is not None
    # The register's fields, never an address or a personal code.
    for row in found["rows"]:
        assert set(row) == {
            "code", "name", "legal_form", "status_code", "status_text", "list", "role",
            "role_text", "since", "percent", "share_size", "currency",
        }


def test_a_person_with_one_company_answers_that_company(person_index) -> None:
    found = ariregister.companies_for_hash(H_KALEV)
    assert found is not None and found["companies"] == 1
    assert {r["code"] for r in found["rows"]} == {HAUDE}


def test_a_hash_with_no_rows_is_an_empty_list_not_a_miss(person_index) -> None:
    found = ariregister.companies_for_hash(NOBODY)
    assert found == {"name": None, "rows": [], "total_rows": 0, "companies": 0, "truncated": False}


def test_a_value_that_is_not_a_uuid_is_refused(person_index) -> None:
    assert ariregister.parse_person_hash("38001085718") is None  # a personal code's shape
    assert ariregister.companies_for_hash("38001085718") is None
    assert ariregister.companies_for_hash("Veronika Oškina") is None


def test_without_an_index_there_is_no_answer(tmp_path: Path) -> None:
    _point_at(tmp_path / "absent.sqlite")
    try:
        assert ariregister.companies_for_hash(H_VERONIKA) is None
    finally:
        _point_at(None)


def test_the_limit_cuts_rows_and_says_so(person_index) -> None:
    found = ariregister.companies_for_hash(H_VERONIKA, limit=2)
    assert found is not None
    assert len(found["rows"]) == 2 and found["total_rows"] == 5 and found["truncated"]


def test_an_index_built_before_the_hash_indexes_still_answers(tmp_path: Path) -> None:
    out, _meta = _build(tmp_path, off_cards=_with_veronika_on_eesti_board())
    conn = sqlite3.connect(out)
    for name in ("officer_person", "shareholder_person", "beneficial_owner_person", "off_card_person_person"):
        conn.execute(f"DROP INDEX {name}")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        found = ariregister.companies_for_hash(H_VERONIKA)
        assert found is not None and found["companies"] == 2
    finally:
        _point_at(None)


def test_roles_read_in_the_mappers_words() -> None:
    assert ee_person_role_label("registry_card", "JUHL") == "Board member (juhatuse liige)"
    assert ee_person_role_label("shareholders", None) == "Shareholder (osanik)"
    assert ee_person_role_label("beneficial_owners", "K") == "Beneficial owner — indirect participation"
    assert ee_person_role_label("off_card", "N") == "Supervisory board member (nõukogu liige)"
    assert ee_person_role_label("off_card", "S").startswith("Shareholder of a public limited company")
    assert ee_person_role_label("off_card", "A") == "Founder (asutaja)"
    assert ee_person_role_label("off_card", "J") == "Auditor (revident)"
    # A code no table knows reads as the register's own text.
    assert ee_person_role_label("registry_card", "ZZZ", "Uus roll") == "Uus roll"


# --- the route ---------------------------------------------------------------


def test_the_route_lists_the_persons_companies(person_index, client: TestClient) -> None:
    r = client.get("/person-appointments", params={"scheme": "EE-ARIREGISTER-HASH", "id": H_VERONIKA})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scheme"] == "EE-ARIREGISTER-HASH" and body["source_id"] == "ariregister"
    assert body["officer_id"] == H_VERONIKA and body["name"] == "Veronika Oškina"
    assert body["company_count"] == 2 and body["total_results"] == 5
    assert body["as_of"] == "2026-10-12"
    assert body["bods"] == [], "listed, never drawn"
    assert "pseudonymous" in body["caveat"] and "current rows only" in body["caveat"]
    board = next(a for a in body["appointments"] if a["company_number"] == EESTI)
    assert board["role"] == "Supervisory board member (nõukogu liige)"
    assert board["register_list"] == "off_card" and board["appointed_on"] == "2025-06-01"
    assert board["company_status"] == "Entered in the register"
    # No address and no personal code anywhere in the answer.
    assert not any("address" in key for a in body["appointments"] for key in a)
    assert not re.search(r"(?<!\d)\d{11}(?!\d)", r.text)


def test_the_route_accepts_an_upper_case_uuid(person_index, client: TestClient) -> None:
    r = client.get("/person-appointments", params={"scheme": "EE-ARIREGISTER-HASH", "id": H_VERONIKA.upper()})
    assert r.status_code == 200 and r.json()["officer_id"] == H_VERONIKA


def test_the_route_answers_an_empty_list_for_a_hash_on_no_list(person_index, client: TestClient) -> None:
    r = client.get("/person-appointments", params={"scheme": "EE-ARIREGISTER-HASH", "id": NOBODY})
    assert r.status_code == 200
    assert r.json()["appointments"] == [] and r.json()["company_count"] == 0


@pytest.mark.parametrize("params", [
    {"scheme": "EE-ARIREGISTER-HASH", "id": "Veronika Oškina"},  # never a name search
    {"scheme": "EE-ARIREGISTER-HASH", "id": "38001085718"},
    {"scheme": "EE-ARIREGISTER-HASH"},
    {"scheme": "NAME", "id": "Veronika Oškina"},
    {"id": H_VERONIKA},
    {},
])
def test_the_route_refuses_anything_but_a_register_key(person_index, client: TestClient, params) -> None:
    r = client.get("/person-appointments", params=params)
    assert r.status_code == 400


def test_without_an_index_the_route_says_so(tmp_path: Path, client: TestClient) -> None:
    _point_at(tmp_path / "absent.sqlite")
    try:
        r = client.get("/person-appointments", params={"scheme": "EE-ARIREGISTER-HASH", "id": H_VERONIKA})
    finally:
        _point_at(None)
    assert r.status_code == 200 and r.json()["is_stub"] is True


# --- MCP -----------------------------------------------------------------------


async def test_the_mcp_tool_lists_the_persons_companies(person_index) -> None:
    from opencheck.mcp.server import opencheck_person_positions

    out = await opencheck_person_positions(scheme="ee-ariregister-hash", id=H_VERONIKA)
    assert out["company_count"] == 2 and "bods" not in out
    assert {a["company_number"] for a in out["appointments"]} == {AGENT, EESTI}


async def test_the_mcp_tool_refuses_a_name(person_index) -> None:
    from opencheck.mcp.server import opencheck_person_positions

    assert "error" in await opencheck_person_positions(scheme="EE-ARIREGISTER-HASH", id="Veronika Oškina")
    assert "error" in await opencheck_person_positions(scheme="NAME", id="Veronika Oškina")


def test_the_mcp_tool_spends_the_lookup_tier() -> None:
    from opencheck.mcp.guard import _LOOKUP, TOOL_TIERS

    assert TOOL_TIERS["opencheck_person_positions"] is _LOOKUP


# --- the page path: one company's same-named people are one node ---------------

_PAGE_ONE_PERSON = """<html><body>
<h2>Nordic Foods 1 OÜ (17441866)</h2>
<div class="row mt-4"><div class="col-md-4 text-muted">Registry code</div><div class="col font-weight-bold">17441866</div></div>
<div class="row mt-4"><div class="col-md-4 text-muted">Status</div><div class="col font-weight-bold">Entered into the register</div></div>
<table>
<tr><th>Name</th><th>Personal identification code</th><th>Role</th><th>Start - end</th></tr>
<tr><td>Eva-Maria Kaerma</td><td>49301230014</td><td>Management board member</td><td>20.02.2026</td></tr>
<tr><td>Jaan Tamm</td><td>38001010014</td><td>Management board member</td><td>20.02.2026</td></tr>
</table>
<table>
<tr><th>Participation</th><th>Contribution</th><th>Name</th><th>Code</th><th>Start - End</th></tr>
<tr><td>60.00%</td><td>1500.00 EUR</td><td>Omanikukonto: EVA MARIA KAERMA</td><td>49301230014</td><td>20.02.2026</td></tr>
<tr><td>40.00%</td><td>1000.00 EUR</td><td>Haudemaja OÜ</td><td>16765745</td><td>20.02.2026</td></tr>
</table>
<table>
<tr><th>Name</th><th>Personal identification code / date of birth</th><th>Manner of exercising control</th><th>Start - end</th></tr>
<tr><td>Eva–Maria  Kaerma</td><td>49301230014</td><td>Direct ownership</td><td>20.02.2026</td></tr>
</table>
</body></html>"""

_PAGE_URL = "https://ariregister.rik.ee/eng/company/17441866/company_print_json"


async def _page_statements(tmp_path: Path, html: str) -> list[dict]:
    import respx
    from httpx import Response

    from opencheck import provenance
    from opencheck.bods.mapper import map_ariregister
    from opencheck.sources.ariregister import AriregisterAdapter

    _point_at(tmp_path / "absent.sqlite")
    try:
        with respx.mock:
            respx.get(_PAGE_URL).mock(return_value=Response(
                200, text=html, headers={"content-type": "text/html; charset=utf-8"}))
            with provenance.recording() as rec:
                bundle = await AriregisterAdapter().fetch("17441866", legal_name="Nordic Foods 1 OÜ")
            assert bundle["tier"] == "page"
            with provenance.mapping_provenance(rec.resolve(is_stub=False)):
                return list(map_ariregister(bundle))
    finally:
        _point_at(None)


async def test_the_page_draws_one_person_across_three_lists(tmp_path: Path) -> None:
    statements = await _page_statements(tmp_path, _PAGE_ONE_PERSON)
    people = [s for s in statements if s["recordType"] == "person"]
    names = sorted(p["recordDetails"]["names"][0]["fullName"] for p in people)
    assert len(people) == 2, names  # Eva-Maria Kaerma once, Jaan Tamm once
    eva = next(p for p in people if "aerma" in p["recordDetails"]["names"][0]["fullName"].lower())
    # The node carries the registry card's spelling, not the account-prefixed one.
    assert eva["recordDetails"]["names"][0]["fullName"] == "Eva-Maria Kaerma"
    edges = [
        s for s in statements if s["recordType"] == "relationship"
        and s["recordDetails"]["interestedParty"] == eva["recordId"]
    ]
    assert len(edges) == 3, "board member, shareholder and beneficial owner — one node"
    notes = [a for a in eva.get("annotations", []) if a["motivation"] == "commenting"]
    assert len(notes) == 1
    assert "the registry card, the shareholder list and the beneficial-owner list" in notes[0]["description"]
    assert "publishes no person key" in notes[0]["description"]
    jaan = next(p for p in people if p is not eva)
    assert not [a for a in jaan.get("annotations", []) if a["motivation"] == "commenting"], \
        "a person on one list joined nothing and says nothing"
    # Never a person key or a personal code on the page path.
    text = str(statements)
    assert "EE-ARIREGISTER-HASH" not in text and "49301230014" not in text


async def test_different_names_on_the_page_stay_apart(tmp_path: Path) -> None:
    html = _PAGE_ONE_PERSON.replace("Eva–Maria  Kaerma", "Anne-Liis Theisen")
    statements = await _page_statements(tmp_path, html)
    people = [s for s in statements if s["recordType"] == "person"]
    assert len(people) == 3


def test_the_name_key_sets_aside_case_accents_hyphens_spacing_and_the_account_prefix() -> None:
    from opencheck.bods.mappers.estonia import ee_name_key

    assert ee_name_key("Eva-Maria", "Kaerma") == ee_name_key(None, "Omanikukonto: EVA MARIA  KAERMA")
    assert ee_name_key("Jüri", "Õun") == ee_name_key("JURI", "oun")
    assert ee_name_key("Lihtkonto:", "Mari Mets") == "mari mets"
    assert ee_name_key("Mari", "Mets") != ee_name_key("Mari", "Metsa")
