"""The Estonian register's general data (üldandmed) in the index, the BODS,
the finding and the History tab (the üldandmed phase).

Stephen's decisions (16 Oct 2026): a company obliged to file beneficial
owners with none on file is said in the finding and counted at build time,
not raised as a signal; a merger or division partner the index holds gets an
entity statement plus the register's sentence as an annotation; every part of
the ticket is in (capital, activity, suspensions, the registrar, shares
without nominal value, a foreign branch's parent, the last annual report,
dated status changes). Built on the ``test_ariregister_index`` fixture, whose
Eesti Energia rows are as published on 16 Oct 2026.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck import provenance
from opencheck.bods.mapper import map_ariregister
from opencheck.findings import finding_ariregister
from opencheck.sources import ariregister
from opencheck.sources.ariregister import AriregisterAdapter
from opencheck.timeline.ariregister import general_events
from opencheck.timeline.model import ChangeType, Tier
from tests.test_ariregister_index import (
    EESTI,
    HAUDE,
    HOLD,
    STATE,
    TOOTSI_TEXT,
    _build,
    _csv_rows,
    _general,
    _point_at,
    _yld,
    builder,
)

BRANCH = "11223507"


def _branch_csv() -> list[str]:
    rows = _csv_rows()
    rows.append(";".join([
        "AAS BTA Baltic Insurance Company Eesti filiaal", BRANCH, "Välismaa äriühingu filiaal", "",
        "", "R", "Registrisse kantud", "21.02.2006", "", "Lõõtsa tn 2b", "0387",
        "Lasnamäe linnaosa, Tallinn, Harju maakond", "11415", "3426818", "",
        "Harju maakond, Tallinn, Lasnamäe linnaosa, Lõõtsa tn 2b",
        f"https://ariregister.rik.ee/est/company/{BRANCH}",
    ]))
    return rows


def _rich_general() -> list[dict[str, Any]]:
    """The shared fixture plus: a merger on Haudemaja naming the holding (a
    code the index holds) without the word "registrikood", a suspension on
    the holding, a bankruptcy ruling that names a person by personal code,
    and the Latvian insurer's Estonian branch."""
    general = _general()
    by_code = {str(g["ariregistri_kood"]).zfill(8): g for g in general}
    by_code[HAUDE]["yldandmed"]["oigusjargsused"] = [{
        "kande_nr": 9, "tyyp": "YHIN", "tyyp_tekstina": "Ühinemine tekstina",
        "sisu": f'Ühendav ühing Haudemaja OÜ ja ühendatav ühing Holding "Põhjala" OÜ, {HOLD}, ühinevad.',
        "algus_kpv": "01.06.2026", "lopp_kpv": None,
    }]
    by_code[HOLD]["yldandmed"]["ettevotja_tegevuse_aeg"] = [{
        "tyyp": "TP", "tyyp_tekstina": "Tegevuse peatamine",
        "sisu": "Ettevõtja tegevus on peatatud 21.07.2025 - 27.07.2035",
        "periood_algus_kpv": "21.07.2025", "periood_lopp_kpv": "27.07.2035", "periood": "Y",
        "algus_kpv": "21.07.2025", "lopp_kpv": None,
    }]
    by_code[HOLD]["yldandmed"]["seisundi_muutused"].append({
        "kande_nr": 4, "tyyp": "PANKV", "tyyp_tekstina": "Pankroti väljakuulutamine",
        "sisu": "Harju Maakohtu määrusega kuulutati välja Mari Mets (isikukood: 48001010000) pankrot.",
        "algus_kpv": "02.03.2026", "lopp_kpv": None,
    })
    general.append({"ariregistri_kood": int(BRANCH), "nimi": "AAS BTA Baltic Insurance Company Eesti filiaal",
                    "yldandmed": _yld(
        oiguslik_vorm="FIL", esitab_kasusaajad=False, lahknevusteade_puudumisest=None,
        valismaa_ariyhing=[{
            "arinimi": [{"sisu": "AAS BTA Baltic Insurance Company", "algus_kpv": "14.07.2015", "lopp_kpv": None}],
            "asukoht": [{"aadress_riik": "LVA", "aadress_riik_tekstina": "Läti", "aadress": "Sporta 11, Riia",
                         "algus_kpv": "30.05.2018", "lopp_kpv": None}],
            "sarnane_eesti_oigvorm": [{"sisu": "AS", "sisu_tekstina": "Aktsiaselts", "algus_kpv": "21.02.2006", "lopp_kpv": None}],
            "register": [{"sisu": "Läti Vabariigi Ettevõtte Registris", "algus_kpv": "21.02.2006", "lopp_kpv": None}],
            "registreerimisnumber": [{"sisu": "40103840140", "algus_kpv": "14.07.2015", "lopp_kpv": None}],
        }],
    )})
    return general


@pytest.fixture()
def index(tmp_path: Path):
    out, meta = _build(tmp_path, csv_rows=_branch_csv(), general=_rich_general())
    _point_at(out)
    yield out, meta
    _point_at(None)


async def _statements(code: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with provenance.recording() as rec:
        bundle = await AriregisterAdapter().fetch(code)
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        return bundle, list(map_ariregister(bundle))


def _notes(stmt: dict[str, Any]) -> list[str]:
    return [a["description"] for a in stmt.get("annotations") or [] if a["motivation"] == "commenting"]


# --- the builder ------------------------------------------------------------------


def test_the_general_table_holds_the_fields_and_nothing_personal(index) -> None:
    out, meta = index
    assert meta["general"] == "6"
    conn = sqlite3.connect(out)
    row = conn.execute("SELECT * FROM general WHERE code = ?", (EESTI,)).fetchone()
    columns = [r[1] for r in conn.execute("PRAGMA table_info(general)")]
    rec = dict(zip(columns, row))
    assert rec["capital"] == "846645750.00" and rec["capital_since"] == "2025-07-09"
    assert rec["activity"] == "35151" and rec["activity_nace"] == "35.15"
    assert rec["last_report_end"] == "2025-12-31" and rec["last_report_employees"] == "793"
    assert rec["reports_filed"] == 2 and rec["bo_obliged"] == 1 and rec["bo_absence_reported"] == 0
    extra = json.loads(rec["extra"])
    assert extra["succession"][0]["codes"] == ["16053613"]
    assert extra["predecessor_notes"] == ["Eesti Energoremont (reg. nr. 01005614) baasil."]
    # Contact details are in the file and never in the index.
    dump = "\n".join(conn.iterdump())
    assert "info@enefit.com" not in dump
    assert conn.execute("SELECT text FROM vocab WHERE kind = 'emtak' AND code = '35151'").fetchone() == ("Elektrienergia müük",)


def test_a_personal_code_in_a_ruling_is_removed_and_a_company_number_kept(index) -> None:
    out, _meta = index
    dump = "\n".join(sqlite3.connect(out).iterdump())
    assert "48001010000" not in dump
    assert builder.PERSONAL_CODE_REMOVED in dump
    assert "40103840140" in dump  # the Latvian insurer's registration number


def test_an_unlabelled_eight_digit_number_is_a_partner_only_when_the_index_holds_it() -> None:
    vocab: dict[tuple[str, str], str] = {}
    y = _yld(oigusjargsused=[{"tyyp": "YHIN", "sisu": f"A ühendab B, {HOLD}, ja C, 99999999.",
                              "algus_kpv": "01.06.2026", "lopp_kpv": None}])
    row = builder._general_values(HAUDE, y, vocab, {HOLD, HAUDE})
    extra = json.loads(row[-1])
    assert extra["succession"][0]["codes"] == [HOLD]


def test_the_general_file_must_name_only_known_companies(tmp_path: Path) -> None:
    general = _general() + [{"ariregistri_kood": 11111111, "nimi": "Ghost OÜ", "yldandmed": _yld()}]
    with pytest.raises(builder.BuildRefused, match="is not in"):
        _build(tmp_path, general=general)


def test_an_index_built_before_the_general_data_reads_none(tmp_path: Path) -> None:
    out, _meta = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("DROP TABLE general")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ariregister.general_record(EESTI) is None
        assert ariregister.entity_record(EESTI)["general"] is None
    finally:
        _point_at(None)


# --- the BODS ---------------------------------------------------------------------


async def test_eesti_energia_carries_its_capital_activity_and_division(index) -> None:
    bundle, statements = await _statements(EESTI)
    subject = next(s for s in statements if s["recordType"] == "entity"
                   and s["recordDetails"]["name"] == "Eesti Energia AS")
    notes = _notes(subject)
    assert any("Share capital 846,645,750.00 EUR, as entered on 2025-07-09" in n for n in notes)
    assert any(n.startswith("Main activity (EMTAK 35151, NACE 35.15): Elektrienergia müük") for n in notes)
    assert any("reference to predecessors" in n and "01005614" in n for n in notes)
    assert any(n == ("Latest annual report in the register's open data: the financial year ending "
                     "2025-12-31, reporting 793 employees; 2 annual reports listed in all.") for n in notes)
    assert any(n.startswith("Division (jagunemine), entered on 2020-09-15, in the register's words:")
               and "16053613" in n for n in notes)
    # Tootsi Windpark OÜ is not in the fixture index: said, never drawn.
    names = {s["recordDetails"].get("name") for s in statements if s["recordType"] == "entity"}
    assert "Tootsi Windpark OÜ" not in names


async def test_a_merger_partner_the_index_holds_is_a_statement_and_never_a_relationship(index) -> None:
    _bundle, statements = await _statements(HAUDE)
    holding = [s for s in statements if s["recordType"] == "entity"
               and s["recordDetails"].get("name") == 'Holding "Põhjala" OÜ']
    assert len(holding) == 1
    assert [(i["scheme"], i["id"]) for i in holding[0]["recordDetails"]["identifiers"]] == [
        ("EE-ARIREGISTER", HOLD)
    ]
    assert any("Named in a merger entry on the registry card of Haudemaja OÜ (entered on 2026-06-01)"
               in n for n in _notes(holding[0]))
    # The holding is Haudemaja's shareholder in the fixture: that edge is a
    # shareholding; the merger adds no relationship of its own.
    rels = [s for s in statements if s["recordType"] == "relationship"
            and s["recordDetails"]["interestedParty"] == holding[0]["recordId"]]
    assert all(i["type"] != "otherInfluenceOrControl" for r in rels for i in r["recordDetails"]["interests"])


async def test_a_branch_draws_its_foreign_parent_with_its_jurisdiction(index) -> None:
    _bundle, statements = await _statements(BRANCH)
    parent = next(s for s in statements if s["recordType"] == "entity"
                  and s["recordDetails"].get("name") == "AAS BTA Baltic Insurance Company")
    assert parent["recordDetails"]["jurisdiction"]["code"] == "LV"
    assert any("whose Estonian branch is AAS BTA Baltic Insurance Company Eesti filiaal" in n
               for n in _notes(parent))
    rel = next(s for s in statements if s["recordType"] == "relationship"
               and s["recordDetails"]["interestedParty"] == parent["recordId"])
    (interest,) = rel["recordDetails"]["interests"]
    assert interest["type"] == "otherInfluenceOrControl" and interest["directOrIndirect"] == "direct"
    assert "not a separate legal person" in interest["details"]
    assert "beneficialOwnershipOrControl" not in interest


# --- the finding --------------------------------------------------------------------


async def test_the_finding_says_whether_beneficial_owners_are_due(index) -> None:
    state, _ = await _statements(STATE)
    assert "beneficial owners not required" in finding_ariregister(state)
    haude, _ = await _statements(HAUDE)
    assert "beneficial owners due, none published" in finding_ariregister(haude)


async def test_the_finding_says_a_suspension_in_force(index) -> None:
    hold, _ = await _statements(HOLD)
    assert "activity suspended until 27 July 2035" in finding_ariregister(hold)


async def test_the_share_registrar_is_said_where_there_is_room_and_always_in_the_bods(index) -> None:
    eesti, statements = await _statements(EESTI)
    # Eesti Energia's own sentence is at the cap already; the note is in the BODS.
    subject = next(s for s in statements if s["recordType"] == "entity"
                   and s["recordDetails"]["name"] == "Eesti Energia AS")
    assert any("kept by a registrar on the registry card (Nasdaq CSD)" in n for n in _notes(subject))
    # With no shareholder in the open data, the count says where they are.
    short = {**eesti, "off_card_persons": []}
    assert "shareholders kept at Nasdaq CSD" in finding_ariregister(short)


# --- the History tab --------------------------------------------------------------


def test_status_changes_and_succession_are_dated_notable_events(index) -> None:
    hold = general_events(HOLD, ariregister.general_record(HOLD))
    assert [(e.change_type, e.value_new, e.event_date) for e in hold] == [
        (ChangeType.STATUS_CHANGED, "Decision to dissolve (lõpetamine)", "2026-03-01"),
        (ChangeType.STATUS_CHANGED, "Bankruptcy declared (pankroti väljakuulutamine)", "2026-03-02"),
    ]
    assert all(e.tier is Tier.IDENTITY_STATUS for e in hold)
    eesti = general_events(EESTI, ariregister.general_record(EESTI))
    (division,) = eesti
    assert division.change_type is ChangeType.SUCCESSION and division.event_date == "2020-09-15"
    assert division.counterparty == "registry code 16053613"
    haude = general_events(HAUDE, ariregister.general_record(HAUDE))
    assert haude[0].counterparty == 'Holding "Põhjala" OÜ'


def test_no_general_data_is_no_events() -> None:
    assert general_events(EESTI, None) == []


def test_the_division_text_is_kept_whole(index) -> None:
    rec = ariregister.general_record(EESTI)
    assert rec["succession"][0]["text"] == " ".join(TOOTSI_TEXT.split())
