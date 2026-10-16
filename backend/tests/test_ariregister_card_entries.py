"""Registry-card entries as the History tab's Estonian event spine (the
card-entry phase, Part 2 of the full-history ticket).

The registrikaardid open-data file holds every entry on every card — number,
date, entry type — and needs no credentials. Read on 16 Oct 2026 it held
exactly the entries the credentialed SOAP card log returns for Eesti Energia
AS (57) and Bolt Technology OÜ (50). The fixture is Eesti Energia's first and
last entries as published.

The decision taken (the ticket's suggestion): every entry is a row in the
administrative stream; the status, name and succession entries are notable
rows only when the register's own dated history was not read — then the card
is the only Estonian history there is.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, ariregister
from opencheck.timeline import service
from opencheck.timeline.ariregister import (
    CARD_ENTRY_LABELS,
    card_entry_events,
    card_entry_label,
)
from opencheck.timeline.model import ChangeType, DateBasis, Tier
from tests.test_ariregister_index import (
    CUT,
    EESTI,
    HOLD,
    STATE,
    _build,
    _cards,
    _point_at,
    builder,
)


@pytest.fixture()
def index(tmp_path: Path):
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


# --- the builder and the reader ---------------------------------------------


def test_a_company_reads_its_entries_in_the_registers_order(index) -> None:
    entries = ariregister.card_entries(EESTI)
    assert [(e["entry_no"], e["day"], e["entry_type"]) for e in entries] == [
        (1, "1998-03-31", "1"), (2, "1998-06-25", "10"),
        (56, "2026-07-15", "33"), (57, "2026-10-02", "12"),
    ]
    assert entries[0]["entry_text"] == "Asutamiskanne"
    assert {e["card_type"] for e in entries} == {"R"}


def test_every_card_of_a_company_is_read(index) -> None:
    entries = ariregister.card_entries(HOLD)
    assert [(e["card_type"], e["card_no"], e["entry_no"]) for e in entries] == [
        ("R", "1", 1), ("R", "1", 2), ("K", "2", 1),
    ]


def test_an_entry_type_without_wording_reads_as_none(index) -> None:
    (entry,) = ariregister.card_entries(STATE)
    assert entry["entry_type"] == "9999" and entry["entry_text"] is None


def test_a_company_with_no_card_is_an_empty_list(index) -> None:
    assert ariregister.card_entries("16765745") == []  # Haudemaja: no card in the fixture


def test_an_index_built_before_the_cards_were_read_answers_none(tmp_path: Path) -> None:
    out, _meta = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("DROP TABLE card_entry")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ariregister.card_entries(EESTI) is None
    finally:
        _point_at(None)


def test_without_an_index_there_is_nothing(tmp_path: Path) -> None:
    _point_at(tmp_path / "absent.sqlite")
    try:
        assert ariregister.card_entries(EESTI) is None
    finally:
        _point_at(None)


def _with_entry(**changes: Any) -> list[dict[str, Any]]:
    cards = _cards()
    cards[0]["registrikaardid"][0]["kanded"][0].update(changes)
    return cards


@pytest.mark.parametrize("changes, match", [
    ({"kpv": ""}, "no number or date"),
    ({"kande_nr": None}, "no number or date"),
    ({"kandeliik": "1;2"}, "separator"),
])
def test_the_builder_refuses_an_entry_it_cannot_pack(tmp_path: Path, changes, match) -> None:
    with pytest.raises(builder.BuildRefused, match=match):
        _build(tmp_path, cards=_with_entry(**changes))


def test_the_builder_refuses_a_card_for_an_unknown_company(tmp_path: Path) -> None:
    cards = _cards() + [{"ariregistri_kood": 11111111, "nimi": "Ghost OÜ", "registrikaardid": []}]
    with pytest.raises(builder.BuildRefused, match="is not in"):
        _build(tmp_path, cards=cards)


def test_the_builder_refuses_a_short_cards_file(tmp_path: Path) -> None:
    from tests.test_ariregister_index import _write_files

    src = tmp_path / "src"
    _write_files(src)
    with pytest.raises(builder.BuildRefused, match="entries, fewer than"):
        builder.build_index(src, tmp_path / "x.sqlite", cut_override=CUT,
                            min_rows={builder.CARDS_FILE: 1_000})


def test_the_reader_and_the_builder_agree_on_the_packing() -> None:
    assert builder.CARD_ENTRY_FIELDS == ariregister.CARD_ENTRY_FIELDS


# --- the events ---------------------------------------------------------------


def _entries(code: str) -> list[dict[str, Any]]:
    out = ariregister.card_entries(code)
    assert out is not None
    return out


def test_with_the_history_read_every_entry_is_administrative(index) -> None:
    events = card_entry_events(HOLD, _entries(HOLD), history_read=True)
    assert len(events) == 3
    assert all(e.tier is Tier.ADMIN_NOISE and e.change_type is None for e in events)
    assert all(e.date_basis is DateBasis.EFFECTIVE for e in events)
    assert [e.raw_change_type for e in events] == [
        "Founding entry (asutamiskanne)",
        "Change of business name (ärinime muutmise kanne)",
        "Commercial pledge entered (kommertspandi avamiskanne)",
    ]
    assert events[2].raw_field == "card K 2 · entry 1"
    assert events[1].event_date == "2026-03-01"


def test_without_the_history_a_name_change_entry_is_a_notable_row(index) -> None:
    events = card_entry_events(HOLD, _entries(HOLD), history_read=False)
    notable = [e for e in events if e.is_notable]
    assert len(notable) == 1
    assert notable[0].change_type is ChangeType.LEGAL_NAME_CHANGE
    assert notable[0].tier is Tier.IDENTITY_STATUS
    assert notable[0].value_new == "Change of business name (ärinime muutmise kanne)"
    # Founding and pledge entries stay administrative either way.
    assert sum(e.tier is Tier.ADMIN_NOISE for e in events) == 2


def test_an_entry_type_without_wording_says_so() -> None:
    assert card_entry_label("9999") == "Register entry (type 9999, no description published)"
    assert card_entry_label("88", "Uus kanne") == "Uus kanne"
    assert card_entry_label("12") == CARD_ENTRY_LABELS["12"]


def test_every_notable_entry_type_has_an_english_label() -> None:
    from opencheck.timeline.ariregister import CARD_ENTRY_NOTABLE

    assert set(CARD_ENTRY_NOTABLE) <= set(CARD_ENTRY_LABELS)


# --- the History service --------------------------------------------------------


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("ARIREGISTER_USERNAME", raising=False)
    monkeypatch.delenv("ARIREGISTER_PASSWORD", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _estonian_registration(monkeypatch, code: str) -> None:
    async def registration(_client, _lei):
        return (None, None, code, None, None, "Holding Põhjala OÜ")

    async def modifications(_client, _lei):
        return [], []

    monkeypatch.setattr(service, "_gleif_registration", registration)
    monkeypatch.setattr(service, "_gleif_modifications", modifications)


async def test_without_credentials_the_card_is_the_estonian_history(index, live, monkeypatch) -> None:
    get_settings().ariregister_username = None  # belt and braces for a .env on the machine
    get_settings().ariregister_password = None
    _estonian_registration(monkeypatch, HOLD)
    timeline = await service.fetch_timeline("529900HOLDING0000000")
    ee = [e for e in timeline.events if e.source_id == "ariregister"]
    assert len(ee) == 3
    names = [n for n in timeline.notable if n.change_type is ChangeType.LEGAL_NAME_CHANGE]
    assert len(names) == 1 and names[0].date == "2026-03-01"
    assert timeline.registry_numbers.get("ariregister") == HOLD


async def test_with_the_history_read_the_card_entries_stay_administrative(index, live, monkeypatch) -> None:
    settings = get_settings()
    settings.ariregister_username = "user"
    settings.ariregister_password = "pass"
    _estonian_registration(monkeypatch, HOLD)

    async def soap(self, code):
        return {"registry_code": code, "detail_xml": "<x/>", "bo_xml": None}

    monkeypatch.setattr(type(REGISTRY["ariregister"]), "fetch_timeline_data", soap)
    timeline = await service.fetch_timeline("529900HOLDING0000000")
    card = [e for e in timeline.events if e.raw_field and e.raw_field.startswith("card ")]
    assert len(card) == 3 and all(e.tier is Tier.ADMIN_NOISE for e in card)
    assert not [n for n in timeline.notable if n.change_type is ChangeType.LEGAL_NAME_CHANGE]


async def test_a_company_the_index_lacks_adds_nothing(index, live, monkeypatch) -> None:
    get_settings().ariregister_username = None
    get_settings().ariregister_password = None
    _estonian_registration(monkeypatch, "99999999")
    timeline = await service.fetch_timeline("529900ABSENT00000000")
    assert not [e for e in timeline.events if e.source_id == "ariregister"]


def test_a_known_entry_fixture_for_eesti_energia_reads_as_published(index) -> None:
    events = card_entry_events(EESTI, _entries(EESTI), history_read=True)
    assert events[0].raw_change_type == "Founding entry (asutamiskanne)"
    assert events[1].raw_change_type == "Change entry, not published (mitteavaldatav muutmiskanne)"
    assert events[-1].event_date == "2026-10-02"
