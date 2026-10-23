"""Ended ownership from the e-Business Register's history (the full-history
phase).

The SOAP responses here use the field names RIK's ``detailandmed_v2`` and
``tegelikudKasusaajad_v2`` actually return (read from Bolt Technology OÜ's
and Eesti Energia AS's history on 16 Oct 2026), with invented people and
invented codes. They are joined to the index fixture of
``test_ariregister_index`` — Haudemaja OÜ (current shareholders Kalev Kask
and Holding "Põhjala" OÜ, Kalev on the card) and the holding (Lars Jensen,
with a birth date).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

from opencheck import degradation, integrated_ownership, provenance
from opencheck.bods.mapper import map_ariregister
from opencheck.bods import validator
from opencheck.config import get_settings
from opencheck.findings import finding_ariregister
from opencheck.risk import former_party_ids
from opencheck.sources import REGISTRY, ariregister, ariregister_history
from opencheck.sources.ariregister_history import (
    JOIN_FORMER,
    JOIN_NAME_BIRTH,
    JOIN_REGISTER,
    JOIN_ROW,
    history_from_responses,
)
from opencheck.sources.schemas import validate_raw
from opencheck.sources.schemas.ariregister import AriregisterBundle
from tests.test_ariregister_index import CUT, H_KALEV, H_LARS, HAUDE, HOLD, _build, _point_at

KALEV_CODE = "38001010001"   # invented personal codes, the real shape
VANA_CODE = "37001010002"
LT_CODE = "38703150123"      # a Lithuanian asmens kodas is 11 digits too


def _item(**fields: str) -> str:
    return "<item>" + "".join(f"<{k}>{v}</{k}>" for k, v in fields.items()) + "</item>"


def _envelope(op: str, body: str) -> str:
    return (
        '<?xml version="1.0"?><SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">'
        f'<SOAP-ENV:Body><{op}Response xmlns="http://arireg.x-road.eu/producer/"><keha>{body}</keha>'
        f"</{op}Response></SOAP-ENV:Body></SOAP-ENV:Envelope>"
    )


def _haude_detail() -> str:
    on_card = _item(
        kirje_id="71", kaardi_piirkond="5", kaardi_nr="1", kaardi_tyyp="R", kande_nr="1",
        isiku_tyyp="F", isiku_roll="JUHL", isiku_roll_tekstina="Management board member",
        eesnimi="Kalev", nimi_arinimi="Kask", isikukood_registrikood=KALEV_CODE,
        algus_kpv="2010-03-10Z",
    )
    off_card = "".join([
        # Kalev's current holding — kirje 61 is the index's row.
        _item(kirje_id="61", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Kalev", nimi_arinimi="Kask", isikukood_registrikood=KALEV_CODE,
              osaluse_protsent="60.000000", osaluse_suurus="1500.000000", osaluse_valuuta="EUR",
              osaluse_omandiliik="L", algus_kpv="2018-05-01Z"),
        # …and his earlier, smaller holding, re-registered when it grew.
        _item(kirje_id="9001", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Kalev", nimi_arinimi="Kask", isikukood_registrikood=KALEV_CODE,
              osaluse_protsent="30.000000", osaluse_suurus="750.000000", osaluse_valuuta="EUR",
              osaluse_omandiliik="L", algus_kpv="2010-03-10Z", lopp_kpv="2018-05-01Z"),
        # A former-only holder with two rows: one node, two interests.
        _item(kirje_id="9002", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Vana", nimi_arinimi="Omanik", isikukood_registrikood=VANA_CODE,
              osaluse_protsent="40.000000", osaluse_suurus="1000.000000", osaluse_valuuta="EUR",
              osaluse_omandiliik="L", algus_kpv="2010-03-10Z", lopp_kpv="2014-01-15Z"),
        _item(kirje_id="9003", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Vana", nimi_arinimi="Omanik", isikukood_registrikood=VANA_CODE,
              osaluse_protsent="10.000000", osaluse_suurus="250.000000", osaluse_valuuta="EUR",
              osaluse_omandiliik="L", algus_kpv="2014-01-15Z", lopp_kpv="2016-06-30Z"),
        # No code and no birth date: the row on its own.
        _item(kirje_id="9004", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Ainar", nimi_arinimi="Teadmata", osaluse_protsent="5.000000",
              algus_kpv="2011-01-01Z", lopp_kpv="2012-01-01Z"),
        # A foreign person with a foreign personal code: grouped on it, then dropped.
        _item(kirje_id="9005", isiku_tyyp="F", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              eesnimi="Jonas", nimi_arinimi="Petraitis", valis_kood=LT_CODE, valis_kood_riik="LTU",
              valis_kood_riik_tekstina="Lithuania", synniaeg="1987-03-15Z", aadress_riik="LTU",
              osaluse_protsent="15.000000", algus_kpv="2012-01-01Z", lopp_kpv="2015-12-31Z"),
        # A former corporate shareholder: its registry code is a register number and stays.
        _item(kirje_id="9006", isiku_tyyp="J", isiku_roll="O", isiku_roll_tekstina="Shareholder",
              nimi_arinimi="Vana Holding OÜ", isikukood_registrikood="10000001",
              osaluse_protsent="25.000000", algus_kpv="2010-03-10Z", lopp_kpv="2013-03-01Z"),
        # An ended supervisory-board row: officers stay serving-only.
        _item(kirje_id="9007", isiku_tyyp="F", isiku_roll="N", isiku_roll_tekstina="Member of the supervisory board",
              eesnimi="Endine", nimi_arinimi="Noukogu", isikukood_registrikood="49001010003",
              algus_kpv="2010-03-10Z", lopp_kpv="2012-03-10Z"),
    ])
    entries = (
        '<registrikaardid><item><kaardi_piirkond>5</kaardi_piirkond><kaardi_nr>1</kaardi_nr>'
        "<kaardi_tyyp>R</kaardi_tyyp><kanded>"
        + _item(kande_nr="1", kande_kpv="2010-03-10Z", kandeliik="1")
        + _item(kande_nr="2", kande_kpv="2018-05-02Z", kandeliik="2")
        + "</kanded></item></registrikaardid>"
    )
    return _envelope(
        "detailandmed_v2",
        "<ettevotjad><item><ariregistri_kood>16765745</ariregistri_kood><nimi>Haudemaja OÜ</nimi>"
        f"<isikuandmed><kaardile_kantud_isikud>{on_card}</kaardile_kantud_isikud>"
        f"<kaardivalised_isikud>{off_card}</kaardivalised_isikud></isikuandmed>{entries}"
        "</item></ettevotjad>",
    )


def _haude_bo() -> str:
    def owner(**f: str) -> str:
        return "<kasusaaja>" + "".join(f"<{k}>{v}</{k}>" for k, v in f.items()) + "</kasusaaja>"
    return _envelope("tegelikudKasusaajad_v2", "<kasusaajad>" + "".join([
        # The former shareholder was also a beneficial owner: the same node.
        owner(kirje_id="9101", eesnimi="Vana", nimi="Omanik", isikukood=VANA_CODE,
              aadress_riik="EST", kontrolli_teostamise_viis="O",
              kontrolli_teostamise_viis_tekstina="Direct participation",
              algus_kpv="2018-01-01Z", lopp_kpv="2016-06-30Z", lopetamise_liik="K",
              lahknevusteade_esitatud="false"),
        owner(kirje_id="9102", eesnimi="Teine", nimi="Kasusaaja", isikukood="48502020004",
              aadress_riik="EST", kontrolli_teostamise_viis="J",
              kontrolli_teostamise_viis_tekstina="Senior managing official",
              algus_kpv="2019-01-01Z", lopp_kpv="2021-12-31Z", lahknevusteade_esitatud="true"),
    ]) + "</kasusaajad>")


@pytest.fixture()
def index(tmp_path: Path):
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


def _history(code: str = HAUDE) -> dict[str, Any]:
    return history_from_responses(_haude_detail(), _haude_bo(), ariregister.entity_record(code))


async def _statements(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.recording() as rec:
        provenance.record_snapshot(retrieved_at=None, source_as_of=None, detail="test")
        with provenance.mapping_provenance(rec.resolve(is_stub=False)):
            return list(map_ariregister(bundle))


async def _bundle_with_history(code: str = HAUDE) -> dict[str, Any]:
    bundle = await REGISTRY["ariregister"].fetch(code)
    bundle["history"] = _history(code)
    return bundle


# ---------------------------------------------------------------------------
# The adapter's reading of the history
# ---------------------------------------------------------------------------


def test_only_ended_shareholdings_and_owners_are_kept(index) -> None:
    h = _history()
    kirjes = {r["kirje_id"] for r in h["former_shareholders"]}
    assert kirjes == {"9001", "9002", "9003", "9004", "9005", "9006"}, "current rows and officers left out"
    assert {r["kirje_id"] for r in h["former_beneficial_owners"]} == {"9101", "9102"}
    assert [e["kande_nr"] for e in h["card_entries"]] == ["1", "2"]
    assert h["card_entries"][1]["kande_kpv"] == "2018-05-02"


def test_no_personal_code_survives_estonian_or_foreign(index) -> None:
    h = _history()
    text = json.dumps(h)
    for code in (KALEV_CODE, VANA_CODE, LT_CODE, "48502020004", "49001010003"):
        assert code not in text
    natural = [r for r in h["former_shareholders"] if r.get("isiku_tyyp") != "J"]
    assert not any(re.search(r"(?<!\d)\d{11}(?!\d)", json.dumps(r)) for r in natural)
    jonas = next(r for r in natural if r["nimi_arinimi"] == "Petraitis")
    assert "valis_kood" not in jonas and jonas["valis_kood_riik"] == "LTU", "the country stays"
    corp = next(r for r in h["former_shareholders"] if r.get("isiku_tyyp") == "J")
    assert corp["isikukood_registrikood"] == "10000001", "a legal person's registry code is not personal"
    # And the bundle schema refuses one if it ever got through.
    validate_raw("ariregister", AriregisterBundle, {"registry_code": HAUDE, "history": h})


def test_each_ended_row_is_identified_without_its_code(index) -> None:
    rows = {r["kirje_id"]: r for r in _history()["former_shareholders"]}
    assert rows["9001"]["history_join"] == JOIN_REGISTER
    assert rows["9001"]["isikukood_hash"] == H_KALEV, "the current row's hash, via its kirje_id"
    assert rows["9002"]["history_join"] == rows["9003"]["history_join"] == JOIN_FORMER
    assert rows["9002"]["history_key"] == rows["9003"]["history_key"] == "hist-9002", "one former person"
    assert rows["9004"]["history_join"] == JOIN_ROW and rows["9004"]["history_key"] == "hist-9004"
    assert rows["9005"]["history_join"] == JOIN_FORMER
    owners = {r["kirje_id"]: r for r in _history()["former_beneficial_owners"]}
    assert owners["9101"]["history_key"] == "hist-9002", "the former shareholder's own key"


def test_name_and_birth_date_join_a_person_the_index_gives_a_birth_date(index) -> None:
    detail = _envelope("detailandmed_v2", "<ettevotjad><item><isikuandmed><kaardivalised_isikud>" + _item(
        kirje_id="9201", isiku_tyyp="F", isiku_roll="O", eesnimi="Lars", nimi_arinimi="Jensen",
        synniaeg="1987-01-20Z", osaluse_protsent="10.000000", algus_kpv="2016-01-01Z", lopp_kpv="2017-01-01Z",
    ) + "</kaardivalised_isikud></isikuandmed></item></ettevotjad>")
    h = history_from_responses(detail, None, ariregister.entity_record(HOLD))
    (row,) = h["former_shareholders"]
    assert row["history_join"] == JOIN_NAME_BIRTH and row["isikukood_hash"] == H_LARS


# ---------------------------------------------------------------------------
# The graph
# ---------------------------------------------------------------------------


async def test_one_closed_record_per_party_each_row_an_interest(index) -> None:
    statements = await _statements(await _bundle_with_history())
    assert validator.validate_shape(statements) == []
    hist = [s for s in statements if s["recordType"] == "relationship" and "-hist-" not in s["recordId"]
            and any("history" in a.get("description", "") for a in s.get("annotations", []))]
    # Kalev (earlier holding), Vana, Ainar, Jonas, Vana Holding OÜ; Vana and Teine as former owners.
    assert len(hist) == 7
    assert all(s["recordStatus"] == "closed" for s in hist), "every interest ended → closed (Phase 317)"
    by_note = {s["annotations"][-1]["description"].split(",")[0]: s for s in hist}
    vana = next(s for s in hist if "2 register entries" in s["annotations"][-1]["description"])
    assert [i["share"]["exact"] for i in vana["recordDetails"]["interests"]] == [40.0, 10.0]
    assert vana["statementDate"] == "2016-06-30", "dated by the register's last end date, not the cut"
    assert "Earlier holdings of a current shareholder" in by_note
    assert {s["statementDate"] for s in statements if s not in hist and s["recordType"] == "relationship"} == {CUT}


async def test_a_current_holders_earlier_holdings_sit_on_the_same_node(index) -> None:
    statements = await _statements(await _bundle_with_history())
    kalev_ids = {s["recordId"] for s in statements if s["recordType"] == "person"
                 and s["recordDetails"]["names"][0]["fullName"] == "Kalev Kask"}
    assert len(kalev_ids) == 1, "the register's own person record joins him — one node"
    earlier = next(s for s in statements if s["recordType"] == "relationship"
                   and "Earlier holdings of a current shareholder" in str(s.get("annotations")))
    assert earlier["recordDetails"]["interestedParty"] in kalev_ids
    assert "by the register's own person record" in earlier["annotations"][-1]["description"]


async def test_former_people_are_their_own_nodes_and_officers_are_not_drawn(index) -> None:
    statements = await _statements(await _bundle_with_history())
    people = {s["recordDetails"]["names"][0]["fullName"]: s for s in statements if s["recordType"] == "person"}
    assert {"Vana Omanik", "Ainar Teadmata", "Jonas Petraitis", "Teine Kasusaaja"} <= set(people)
    assert "Endine Noukogu" not in people, "an ended supervisory-board row is an officer: not mapped"
    assert people["Jonas Petraitis"]["recordDetails"]["nationalities"] == [{"name": "Lithuania", "code": "LT"}]
    assert people["Vana Omanik"]["statementDate"] == "2016-06-30"
    vana_rels = [s for s in statements if s["recordType"] == "relationship"
                 and s["recordDetails"]["interestedParty"] == people["Vana Omanik"]["recordId"]]
    assert len(vana_rels) == 2, "a former shareholding and a former BO entry, one person"
    ids = former_party_ids(statements)
    assert people["Vana Omanik"]["statementId"] in ids
    assert people["Kalev Kask"]["statementId"] not in ids, "still a current holder"


async def test_former_bo_interest_keeps_the_register_wording_and_flag(index) -> None:
    statements = await _statements(await _bundle_with_history())
    teine = next(s for s in statements if s["recordType"] == "relationship"
                 and "A former beneficial owner" in str(s.get("annotations")) and "2019-01-01" in json.dumps(s))
    (interest,) = teine["recordDetails"]["interests"]
    # A J row: a board member named because no owner was identified — on
    # the BO list, not a beneficial owner (20 Oct 2026).
    assert interest["type"] == "seniorManagingOfficial" and interest["beneficialOwnershipOrControl"] is False
    assert interest["endDate"] == "2021-12-31"


async def test_integrated_figures_ignore_ended_holdings(index) -> None:
    plain = await _statements(await REGISTRY["ariregister"].fetch(HAUDE))
    with_history = await _statements(await _bundle_with_history())
    before = integrated_ownership.for_lookup(f"EE-ARIREGISTER:{HAUDE}", plain, CUT)
    after = integrated_ownership.for_lookup(f"EE-ARIREGISTER:{HAUDE}", with_history, CUT)
    figures = lambda p: {  # noqa: E731
        h["name"]: h["figures"] for h in integrated_ownership.with_figures(p) if h.get("figures")
    }
    assert figures(before) == figures(after)
    formerly = {h["name"] for h in after.get("holders", []) if h.get("status") == "formerly"}
    assert {"Vana Omanik", "Vana Holding OÜ"} <= formerly


# ---------------------------------------------------------------------------
# Reading it: offline, no credentials, a failure, the cache
# ---------------------------------------------------------------------------


@pytest.fixture()
def live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.delenv("ARIREGISTER_USERNAME", raising=False)
    monkeypatch.delenv("ARIREGISTER_PASSWORD", raising=False)
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


def _read(adapter: Any) -> tuple[dict[str, Any], list[Any]]:
    async def go() -> tuple[dict[str, Any], list[Any]]:
        with degradation.recording() as found:
            h = await ariregister_history.read_history(adapter, HAUDE, ariregister.entity_record(HAUDE))
        return h, list(found)
    return asyncio.run(go())


class _Adapter:
    def __init__(self, data: dict[str, Any] | None) -> None:
        self.data, self.calls = data, 0

    async def fetch_timeline_data(self, code: str, **_kw: Any) -> dict[str, Any] | None:
        self.calls += 1
        return self.data


def test_offline_reads_nothing_and_says_nothing(index, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    adapter = _Adapter(None)
    h, found = _read(adapter)
    get_settings.cache_clear()
    assert h["read"] is False and h["reason"] == "offline" and found == [] and adapter.calls == 0


def test_no_credentials_is_said_not_silent(index, live) -> None:
    adapter = _Adapter({"detail_xml": _haude_detail(), "bo_xml": _haude_bo()})
    h, found = _read(adapter)
    assert h["read"] is False and adapter.calls == 0
    (d,) = found
    assert d.check == "source_read" and d.reason == "not_configured"
    assert "former shareholders" in d.detail
    sentence = finding_ariregister({"status": "Entered into the register", "history": h,
                                    "shareholders": [{}], "officers": [{}], "beneficial_owners": []})
    assert "ownership history not read" in sentence


def test_a_failure_is_said_and_never_cached(index, live) -> None:
    live.setenv("ARIREGISTER_USERNAME", "u")
    live.setenv("ARIREGISTER_PASSWORD", "p")
    get_settings.cache_clear()
    adapter = _Adapter(None)
    h, found = _read(adapter)
    assert h["read"] is False and [d.reason for d in found] == ["upstream_error"]
    _read(adapter)
    assert adapter.calls == 2, "a failure is never remembered (Phase 262)"


def test_a_read_is_cached_for_the_day_stripped(index, live) -> None:
    live.setenv("ARIREGISTER_USERNAME", "u")
    live.setenv("ARIREGISTER_PASSWORD", "p")
    get_settings.cache_clear()
    adapter = _Adapter({"detail_xml": _haude_detail(), "bo_xml": _haude_bo()})
    first, found = _read(adapter)
    second, _ = _read(adapter)
    assert first["read"] and second == first and adapter.calls == 1 and found == []
    (path,) = list(Path(os.environ["OPENCHECK_DATA_ROOT"]).glob("**/history-v2/*.json"))
    text = path.read_text()
    assert KALEV_CODE not in text and VANA_CODE not in text, "only the stripped history reaches the disk"


def test_a_missing_bo_history_shows_holdings_and_says_so(index, live) -> None:
    live.setenv("ARIREGISTER_USERNAME", "u")
    live.setenv("ARIREGISTER_PASSWORD", "p")
    get_settings.cache_clear()
    adapter = _Adapter({"detail_xml": _haude_detail(), "bo_xml": None})
    h, found = _read(adapter)
    assert h["read"] and h["former_shareholders"] and h["former_beneficial_owners"] == []
    assert [d.reason for d in found] == ["upstream_error"] and "beneficial-owner history" in found[0].detail
    _read(adapter)
    assert adapter.calls == 2, "a partial read is not cached either"


async def test_the_finding_counts_ended_records(index) -> None:
    bundle = await _bundle_with_history()
    # The fixture's deletion warning (the rulings phase) and its "beneficial
    # owners due, none published" take the room the history clause needs
    # here; this test is about the history.
    sentence = finding_ariregister({**bundle, "rulings": [], "general": {}})
    assert "7 ended ownership records" in sentence, sentence
