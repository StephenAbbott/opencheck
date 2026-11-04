"""Tests for the Latvian (Register of Enterprises) Time Machine emitter.

Rows are in the CKAN datastore's shape as read from data.gov.lv on 3 Nov
2026 — the day the tables were measured and found to carry current rows
only, with no end date on any people table.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.sources import ur_latvia as lv
from opencheck.timeline import (
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
    assemble_timeline,
    ur_latvia_change_events,
)

REG = "40003229495"

_DATA: dict[str, Any] = {
    "regcode": REG,
    "entity": {
        "regcode": 40003229495, "name": 'SIA "GAISMA"', "regtype": "K",
        "regtype_text": "Komercreģistrs", "type": "SIA", "registered": "1995-02-26T00:00:00",
        "terminated": "2026-04-10T00:00:00", "closed": "L",
    },
    "historical_names": [
        {"regcode": 40003229495, "date_to": "2013-05-13T00:00:00", "name": 'Sabiedrība ar ierobežotu atbildību "RTS UG"'},
        {"regcode": 40003229495, "date_to": "2019-01-07T00:00:00", "name": 'SIA "RTS"'},
    ],
    "beneficial_owners": [
        {"id": 64582, "legal_entity_registration_number": 40003229495, "forename": "Argods", "surname": "Lūsiņš",
         "latvian_identity_number_masked": "090859-*****", "birth_date": "", "nationality": "LV", "residence": "LV",
         "registered_on": "2019-05-02T00:00:00", "last_modified_at": "2019-05-02T08:41:39"},
    ],
    "officers": [
        {"id": 108, "at_legal_entity_registration_number": 40003229495, "entity_type": "NATURAL_PERSON",
         "position": "CHAIR_OF_BOARD", "governing_body": "EXECUTIVE_BOARD", "name": "Liberte Inese",
         "rights_of_representation_type": "INDIVIDUALLY", "registered_on": "2005-08-19T00:00:00",
         "last_modified_at": "2014-11-04T17:07:09"},
        {"id": 109, "at_legal_entity_registration_number": 40003229495, "entity_type": "NATURAL_PERSON",
         "position": "", "governing_body": "", "name": "", "registered_on": "2005-08-19T00:00:00"},
    ],
    "members": [
        {"id": 3, "at_legal_entity_registration_number": 40003229495, "entity_type": "NATURAL_PERSON",
         "name": "Kadaks Didzis", "number_of_shares": 100, "share_nominal_value": 10, "share_currency": "LVL",
         "date_from": "2008-12-30T00:00:00", "registered_on": "2009-01-05T00:00:00"},
        {"id": 4, "at_legal_entity_registration_number": 40003229495, "entity_type": "LEGAL_ENTITY",
         "name": 'SIA "TURĒTĀJS"', "legal_entity_registration_number": 40003111111, "number_of_shares": 0,
         "date_from": None, "registered_on": "2010-03-01T00:00:00"},
    ],
    "stockholders": [
        {"id": 1, "at_legal_entity_registration_number": 40003229495, "entity_type": "DEPOSITORY", "name": "",
         "number_of_shares": 0, "stock_type": "DEMATERIALISED_STOCKS", "depository_registration_number": 40003242879,
         "depository_name": "Nasdaq CSD SE", "date_from": "2023-07-03T00:00:00", "registered_on": "2023-07-03T00:00:00"},
        {"id": 2, "at_legal_entity_registration_number": 40003229495, "entity_type": "LEGAL_ENTITY", "name": "AS \"HOLDINGS\"",
         "number_of_shares": 1200, "share_nominal_value": 1.4, "share_currency": "EUR", "votes": 1200,
         "stock_type": "REGISTERED_STOCKS", "date_from": "2021-06-01T00:00:00", "registered_on": "2021-06-03T00:00:00"},
    ],
    "liquidations": [
        {"id": 900338087, "legal_entity_registration_number": 40003229495, "liquidation_type": "LIQUIDATION",
         "liquidation_type_text": "Likvidācijas process", "date_from": "2026-01-02T00:00:00",
         "grounds_for_liquidation": "Sabiedrības vienīgā dalībnieka 20.10.2025.lēmums Nr.2/2025",
         "registered_on": "2026-01-02T19:25:18"},
    ],
    "reorganizations": [
        {"id": 90018562, "reorganization_type": "DIVISION", "reorganization_type_text": "Sadalīta sašķelšanas ceļā",
         "source_entity_regcode": 40003229495, "final_entity_regcode": 45403028766, "registered": "2018-11-02T00:00:00"},
        {"id": 90018500, "reorganization_type": "ACQUISITION", "reorganization_type_text": "Apvienošanas ceļā pievienota pie",
         "source_entity_regcode": 40003999999, "final_entity_regcode": 40003229495, "registered": "2012-02-01T00:00:00"},
    ],
    "insolvency": [
        {"proceeding_id": 90000033, "debtor_registration_number": 40003229495, "proceeding_started_on": "2008-01-29T00:00:00",
         "proceeding_ended_on": "2008-05-21T00:00:00", "proceeding_form": "INSOLVENCY", "proceeding_type": "NATIONAL",
         "proceeding_resolution_type": "M06", "proceeding_resolution_name": "pabeigta bankrota procedūra",
         "court_case_initial_number": "C29179808", "court_name": "Rīgas pilsētas Latgales priekšpilsētas tiesa"},
    ],
    "suspensions": [
        {"id": 900338085, "legal_entity_registration_number": 40003229495, "suspension_code": "ECONOMIC_ACTIVITY_SUSPENDED",
         "suspension_code_text": "Saimnieciskās darbības apturēšana", "date_from": "2025-11-02T00:00:00",
         "ground_for": "Valsts ieņēmumu dienesta 28.10.2025. lēmums Nr. 32.1/17.22.1/35743"},
    ],
    "securing_measures": [
        {"id": 91407392, "legal_entity_registration_number": 40003229495, "securing_measure_type": "PROHIBITION",
         "securing_measure_type_text": "Aizliegums", "type_details": "Piemērots kapitāla daļām atsavināšanas aizliegums",
         "date_from": "2024-11-02T00:00:00", "case_number": "43055/089/2024-NOS",
         "institution_name": "Zvērināta tiesu izpildītāja Dita Voronova "},
    ],
    "unavailable": [],
}


def _by(events, change_type):
    return [e for e in events if e.change_type is change_type]


def test_every_event_is_latvian_and_dated_by_the_register():
    events = ur_latvia_change_events(_DATA)
    assert events
    assert all(e.source_id == "ur_latvia" for e in events)
    assert all(e.subject_id == REG for e in events)
    assert all(e.event_date for e in events)
    assert all(e.date_confidence is DateConfidence.HIGH for e in events)
    assert {e.date_basis for e in events} == {DateBasis.EFFECTIVE, DateBasis.RECORDED}


def test_people_are_additions_only_and_never_removed():
    events = ur_latvia_change_events(_DATA)
    assert _by(events, ChangeType.OWNER_REMOVED) == []
    assert _by(events, ChangeType.OFFICER_RESIGNED) == []
    assert all(e.interest_end_date is None for e in events)


def test_shareholders_and_stockholders_are_tier_one_dated_by_date_from():
    events = ur_latvia_change_events(_DATA)
    added = _by(events, ChangeType.OWNER_ADDED)
    member = next(e for e in added if "Kadaks Didzis" in e.counterparty)
    assert member.tier is Tier.OWNERSHIP_CONTROL
    assert member.record_type is RecordType.RELATIONSHIP
    assert member.event_date == "2008-12-30"          # date_from, not registered_on
    assert member.interest_start_date == "2008-12-30"
    assert member.date_basis is DateBasis.EFFECTIVE
    assert member.counterparty == "Kadaks Didzis — shareholder (dalībnieks) (100 shares of 10.00 LVL)"
    stock = next(e for e in added if "HOLDINGS" in e.counterparty)
    assert stock.counterparty == 'AS "HOLDINGS" — stockholder (akcionārs) (1,200 shares of 1.40 EUR)'
    assert stock.event_date == "2021-06-01"
    # No date_from: the entry date, said as such.
    holder = next(e for e in added if "TURĒTĀJS" in e.counterparty)
    assert holder.event_date == "2010-03-01"
    assert holder.date_basis is DateBasis.RECORDED
    assert "(" not in holder.counterparty.split("(dalībnieks)")[-1]   # zero shares states no holding


def test_beneficial_owners_are_dated_by_the_entry_day_as_recorded():
    events = ur_latvia_change_events(_DATA)
    bo = next(e for e in _by(events, ChangeType.OWNER_ADDED) if "beneficial owner" in e.counterparty)
    assert bo.counterparty == "Argods Lūsiņš — beneficial owner (LV)"
    assert bo.event_date == "2019-05-02"
    assert bo.date_basis is DateBasis.RECORDED
    assert bo.tier is Tier.OWNERSHIP_CONTROL


def test_officers_are_the_board_stream():
    events = ur_latvia_change_events(_DATA)
    officers = _by(events, ChangeType.OFFICER_APPOINTED)
    assert len(officers) == 1   # the unnamed row is not an event
    officer = officers[0]
    assert officer.tier is Tier.BOARD_CHANGE
    assert officer.counterparty == (
        "Liberte Inese — chair of the board (valdes priekšsēdētājs), management board (valde); represents individually"
    )
    assert officer.event_date == "2005-08-19"
    assert officer.date_basis is DateBasis.RECORDED
    assert not any("Liberte" in (e.counterparty or "") for e in _by(events, ChangeType.OWNER_ADDED))


def test_name_history_chains_each_former_name_to_the_next():
    events = ur_latvia_change_events(_DATA)
    names = sorted((e.event_date, e.value_old, e.value_new) for e in _by(events, ChangeType.LEGAL_NAME_CHANGE))
    assert names == [
        ("2013-05-13", 'Sabiedrība ar ierobežotu atbildību "RTS UG"', 'SIA "RTS"'),
        ("2019-01-07", 'SIA "RTS"', 'SIA "GAISMA"'),
    ]
    assert all(e.tier is Tier.IDENTITY_STATUS for e in _by(events, ChangeType.LEGAL_NAME_CHANGE))


def test_status_events_carry_the_registers_words_and_grounds():
    events = ur_latvia_change_events(_DATA)
    status = _by(events, ChangeType.STATUS_CHANGED)
    by_date = {e.event_date: e for e in status}
    assert by_date["2026-04-10"].value_new == "Terminated (izbeigta)"
    assert by_date["2026-04-10"].raw_field == "closed L"
    assert by_date["2026-01-02"].value_new == "Liquidation process (likvidācijas process)"
    assert "20.10.2025" in by_date["2026-01-02"].raw_field
    assert by_date["2025-11-02"].value_new == "Economic activity suspended (saimnieciskās darbības apturēšana)"
    assert by_date["2008-01-29"].value_new == "Insolvency proceedings (maksātnespējas process) opened"
    assert by_date["2008-05-21"].value_new == "Insolvency proceedings (maksātnespējas process) ended: pabeigta bankrota procedūra"
    assert "C29179808" in by_date["2008-05-21"].raw_field
    assert all(e.tier is Tier.IDENTITY_STATUS for e in status)
    # The registration itself is administrative, as a CH incorporation filing is.
    registered = next(e for e in events if e.raw_change_type == "registered")
    assert registered.tier is Tier.ADMIN_NOISE and registered.change_type is None
    assert registered.event_date == "1995-02-26"


def test_reorganisations_name_the_other_company_on_either_side():
    events = ur_latvia_change_events(_DATA)
    succ = {e.event_date: e for e in _by(events, ChangeType.SUCCESSION)}
    assert succ["2018-11-02"].value_new == "Divided by splitting (sadalīta sašķelšanas ceļā) — into registration number 45403028766"
    assert succ["2018-11-02"].counterparty == "registration number 45403028766"
    assert succ["2012-02-01"].value_new == "Merged by acquisition (apvienošanas ceļā pievienota) — from registration number 40003999999"


def test_securing_measures_and_depository_rows_are_administrative():
    events = ur_latvia_change_events(_DATA)
    admin = [e for e in events if e.tier is Tier.ADMIN_NOISE and e.change_type is None]
    texts = {e.value_new for e in admin}
    assert any(t.startswith("Prohibition (aizliegums): Piemērots kapitāla") for t in texts)
    assert any("dematerialised form at Nasdaq CSD SE" in t for t in texts)
    prohibition = next(e for e in admin if e.raw_change_type == "securing measure PROHIBITION")
    assert prohibition.raw_field == "43055/089/2024-NOS, Zvērināta tiesu izpildītāja Dita Voronova"


def test_empty_data():
    assert ur_latvia_change_events(None) == []
    assert ur_latvia_change_events({}) == []
    assert ur_latvia_change_events({"regcode": REG, "entity": {}}) == []


def test_assembler_integrates_latvian_events():
    tl = assemble_timeline(lei="X", extra_events=ur_latvia_change_events(_DATA))
    assert any(entry.sources == ["ur_latvia"] for entry in tl.notable)
    assert any(e.tier is Tier.BOARD_CHANGE for e in tl.events)


# --------------------------------------------------------------------------- #
# The adapter's history read
# --------------------------------------------------------------------------- #

_NAME_FILE = (
    "regcode;date_to;name\n"
    '40003229495;2013-05-13;Sabiedrība ar ierobežotu atbildību "RTS UG"\n'
    "40003229495;2019-01-07;SIA \"RTS\"\n"
    "40003481588;2013-05-13;Another company\n"
)


def _ckan(records: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(200, json={"success": True, "result": {"records": records, "total": len(records)}})


@pytest.fixture
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _adapter(tmp_path) -> lv.UrLatviaAdapter:
    adapter = lv.UrLatviaAdapter()
    adapter._cache = Cache(root=tmp_path)   # a response cache of this test's own
    return adapter


def test_name_history_file_is_parsed_into_the_ckan_row_shape():
    rows = lv._parse_name_history(_NAME_FILE)
    assert rows[0] == {"regcode": 40003229495, "date_to": "2013-05-13T00:00:00", "name": 'Sabiedrība ar ierobežotu atbildību "RTS UG"'}
    assert len(rows) == 3


async def test_fetch_timeline_data_reads_the_six_event_tables_and_says_which_did_not_answer(live, httpx_mock, tmp_path):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        calls.append(url)
        if url.startswith(lv._NAME_HISTORY_URL):
            return httpx.Response(200, text=_NAME_FILE)
        rid = request.url.params.get("resource_id")
        filters = json.loads(request.url.params.get("filters") or "{}")
        if rid == lv._RES_BUSINESS:
            return _ckan([_DATA["entity"]])
        if rid == lv._RES_SECURING:
            return httpx.Response(503, text="gateway timeout")
        if rid == lv._RES_REORGANIZATIONS:
            field = next(iter(filters))
            return _ckan([r for r in _DATA["reorganizations"] if str(r[field]) == str(filters[field])])
        table = {
            lv._RES_BOWNERS: "beneficial_owners", lv._RES_OFFICERS: "officers", lv._RES_MEMBERS: "members",
            lv._RES_STOCKHOLDERS: "stockholders", lv._RES_LIQUIDATIONS: "liquidations",
            lv._RES_INSOLVENCY: "insolvency", lv._RES_SUSPENSIONS: "suspensions",
        }.get(rid)
        assert table, rid
        return _ckan(_DATA[table])

    httpx_mock.add_callback(handler, is_reusable=True)
    adapter = _adapter(tmp_path)
    data = await adapter.fetch_timeline_data(REG)
    assert data
    assert data["regcode"] == REG
    # The names come from the daily raw file, the entity's rows only.
    assert [n["name"] for n in data["historical_names"]] == [
        'Sabiedrība ar ierobežotu atbildību "RTS UG"', 'SIA "RTS"',
    ]
    assert not any(lv._RES_HIST_NAMES in u for u in calls), "CKAN's stale copy was read"
    assert len(data["reorganizations"]) == 2
    assert [r["id"] for r in data["liquidations"]] == [900338087]
    assert data["securing_measures"] == []
    assert data["unavailable"] == ["securing_measures"]
    events = ur_latvia_change_events(data)
    assert _by(events, ChangeType.SUCCESSION) and _by(events, ChangeType.LEGAL_NAME_CHANGE)


async def test_a_failed_name_file_falls_back_to_the_ckan_copy(live, httpx_mock, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith(lv._NAME_HISTORY_URL):
            return httpx.Response(500)
        rid = request.url.params.get("resource_id")
        if rid == lv._RES_BUSINESS:
            return _ckan([_DATA["entity"]])
        if rid == lv._RES_HIST_NAMES:
            return _ckan(_DATA["historical_names"][:1])
        return _ckan([])

    httpx_mock.add_callback(handler, is_reusable=True)
    payload = await _adapter(tmp_path).fetch(REG)
    assert [n["name"] for n in payload["historical_names"]] == ['Sabiedrība ar ierobežotu atbildību "RTS UG"']
