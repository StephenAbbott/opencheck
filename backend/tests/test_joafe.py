"""JOAFE association notices on the History tab (the JOAFE History ticket).

Fixtures in ``tests/fixtures/joafe/`` are real answers of DILA's
``jo_associations`` dataset (10–11 Nov 2026). Where a seat is a member's home,
the person's name and street lines are replaced by placeholders in the
fixture; the shape is the portal's.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import httpx
import pytest
import respx

from opencheck.config import get_settings
from opencheck.sources import joafe, rna_index
from opencheck.timeline import service
from opencheck.timeline.joafe import CORRECTED, joafe_change_events
from opencheck.timeline.model import ChangeType, DateBasis, DateConfidence, Tier

FIXTURES = Path(__file__).parent / "fixtures" / "joafe"


@pytest.fixture(autouse=True)
def fresh_cache(tmp_path: Path, monkeypatch) -> None:
    """Each test its own cache: a cached answer must not leak across tests."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))


def _raw(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))["results"]


def _rows(name: str) -> list[dict]:
    return [joafe._reduce(r) for r in _raw(name)]


# --- the reader: what a row may carry ------------------------------------------


@pytest.mark.parametrize("name", sorted(p.name for p in FIXTURES.glob("*.json")))
def test_a_reduced_row_carries_only_allowlisted_keys(name: str) -> None:
    for row in _rows(name):
        assert set(row) <= joafe.ROW_KEYS


def test_a_members_home_never_reaches_a_row_or_an_event() -> None:
    raw = _raw("chez_seat_W731007118.json")
    assert "chez" in raw[0]["contenu"]  # the fixture does carry the trap
    rows = [joafe._reduce(r) for r in raw]
    events = joafe_change_events(rows)
    dumped = json.dumps(rows, ensure_ascii=False) + json.dumps(
        [e.__dict__ for e in events], default=str, ensure_ascii=False
    )
    for street in ("chez", "Exemple", "Appartement", "Lamarque", "Maison des Associations", "chemin"):
        assert street not in dumped
    seat = [e for e in events if e.change_type is ChangeType.ADDRESS_CHANGE]
    assert len(seat) == 1
    assert seat[0].value_old == "Albertville (73200)"
    assert seat[0].value_new == "Albertville (73200)"
    assert seat[0].tier is Tier.IDENTITY_STATUS


def test_no_purpose_or_founders_text_is_kept() -> None:
    rows = _rows("annulled_W062020449.json")
    dumped = json.dumps(rows, ensure_ascii=False)
    assert "insertion sociale" not in dumped  # the creation's objet
    assert "objet" not in dumped


def test_where_clause_asks_for_both_keys() -> None:
    assert joafe.where_clause("425138393", ["W922001074"]) == (
        'dca_siren="425138393" or numero_rna in ("W922001074")'
    )
    assert joafe.where_clause("425138393", []) == 'dca_siren="425138393"'


# --- the emitter ---------------------------------------------------------------


def test_transparency_international_france() -> None:
    events = joafe_change_events(_rows("tif_425138393.json"))
    notable = [e for e in events if e.tier is Tier.IDENTITY_STATUS]
    moves = [(e.event_date, e.value_old, e.value_new) for e in notable
             if e.change_type is ChangeType.ADDRESS_CHANGE]
    assert moves == [
        ("2011-05-12", "Levallois-Perret (92300)", "Neuilly-sur-Seine (92200)"),
        ("2013-01-29", "Neuilly-sur-Seine (92200)", "Paris (75010)"),
    ]
    names = [e for e in notable if e.change_type is ChangeType.LEGAL_NAME_CHANGE]
    assert len(names) == 1
    assert names[0].event_date == "2014-09-24"
    assert names[0].value_old == "TRANSPARENCE - INTERNATIONAL (FRANCE)."
    assert names[0].value_new == "TRANSPARENCY INTERNATIONAL FRANCE."
    assert names[0].date_basis is DateBasis.EFFECTIVE
    assert names[0].subject_id == "W922001074"
    assert names[0].raw_payload_ref == "201400401419"

    accounts = [e for e in events if e.raw_field == "accounts deposit"]
    assert len(accounts) == 6
    assert all(e.tier is Tier.ADMIN_NOISE and e.subject_id == "425138393" for e in accounts)
    assert accounts[0].raw_change_type.endswith("year ending 31 Dec 2020")
    assert all(e.date_basis is DateBasis.RECORDED for e in accounts)
    assert {e.source_id for e in events} == {"joafe"}


def test_a_rectification_replaces_the_notice_it_names() -> None:
    events = joafe_change_events(_rows("rectified_W832005930.json"))
    # The initial (2475 of 13 Oct 2026) is gone; the rectification stands. It
    # names neither a new name nor a moved seat (both repeated as they were).
    assert [e.raw_payload_ref for e in events] == ["202600452548"]
    assert events[0].raw_change_type.endswith(f"({CORRECTED})")
    assert events[0].tier is Tier.ADMIN_NOISE
    assert events[0].change_type is None


def test_a_cancellation_removes_the_notice_and_says_so() -> None:
    events = joafe_change_events(_rows("annulled_W062020449.json"))
    created = [e for e in events if e.change_type is ChangeType.STATUS_CHANGED]
    assert [e.value_new for e in created] == ["Declared"]  # the dissolution is withdrawn
    withdrawn = [e for e in events if "withdrawn" in e.raw_change_type]
    assert len(withdrawn) == 1
    assert withdrawn[0].raw_change_type == "Notice of 23 Jun 2026 withdrawn (annulation)"
    assert withdrawn[0].tier is Tier.ADMIN_NOISE
    assert withdrawn[0].event_date == "2026-10-06"
    assert withdrawn[0].date_confidence is DateConfidence.HIGH


def test_a_later_accounts_version_replaces_the_earlier() -> None:
    base = {"id": "1_31122024", "source": "dca", "siren": "425138393",
            "accounts_period_end": "2024-12-31", "published": "2025-08-05", "accounts_version": 0}
    later = {**base, "id": "1_31122024_1", "published": "2025-09-01", "accounts_version": 1}
    events = joafe_change_events([base, later])
    assert [e.raw_payload_ref for e in events] == ["1_31122024_1"]
    assert events[0].raw_change_type.endswith(f"({CORRECTED})")


def test_no_rows_no_events() -> None:
    assert joafe_change_events(None) == []
    assert joafe_change_events([]) == []


# --- the RNA index's SIREN join -----------------------------------------------


@pytest.fixture()
def rna_db(tmp_path: Path):
    path = tmp_path / "rna.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
        "CREATE TABLE association (id TEXT, siren TEXT);"
        "CREATE INDEX association_siren ON association(siren);"
    )
    conn.execute("INSERT INTO meta VALUES ('schema_version', ?)", (rna_index.INDEX_SCHEMA_VERSION,))
    conn.executemany("INSERT INTO association VALUES (?, ?)", [
        ("W922001074", "425138393"), ("W751000002", "111111111"), ("W751000001", "111111111"),
    ])
    conn.commit()
    conn.close()
    os.environ["RNA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    rna_index.reset_connection()
    yield path
    os.environ.pop("RNA_DB_FILE", None)
    get_settings.cache_clear()
    rna_index.reset_connection()


def test_rnas_for_siren(rna_db: Path) -> None:
    assert rna_index.rnas_for_siren("425138393") == ["W922001074"]
    assert rna_index.rnas_for_siren("111111111") == ["W751000001", "W751000002"]
    assert rna_index.rnas_for_siren("999999999") == []


def test_rnas_for_siren_without_an_index(tmp_path: Path) -> None:
    os.environ["RNA_DB_FILE"] = str(tmp_path / "absent.sqlite")
    get_settings.cache_clear()
    rna_index.reset_connection()
    try:
        assert rna_index.rnas_for_siren("425138393") is None
    finally:
        os.environ.pop("RNA_DB_FILE", None)
        get_settings.cache_clear()
        rna_index.reset_connection()


# --- the live read -------------------------------------------------------------


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _answer() -> dict:
    raw = _raw("tif_425138393.json")
    return {"total_count": len(raw), "results": raw}


async def test_notices_reads_once_and_caches_a_day(live, monkeypatch) -> None:
    monkeypatch.setattr(rna_index, "rnas_for_siren", lambda s: ["W922001074"])
    with respx.mock(assert_all_called=True) as mock:
        route = mock.get(joafe.RECORDS_URL).mock(return_value=httpx.Response(200, json=_answer()))
        first = await joafe.notices("425138393")
        second = await joafe.notices("425138393")
    assert route.call_count == 1
    assert first == second
    assert first["status"] == "read" and first["rna_status"] == "read"
    assert first["rnas"] == ["W922001074"]
    assert len(first["rows"]) == 9
    assert "numero_rna in" in route.calls[0].request.url.params["where"]
    # Nothing the allowlist leaves out reaches the cache.
    assert "contenu" not in json.dumps(first)
    assert "rue" not in json.dumps(first)


@pytest.mark.parametrize("answer", [
    httpx.Response(500, text="oops"),
    httpx.Response(200, text="not json"),
    httpx.ConnectError("reset"),
])
async def test_a_failure_is_said_and_never_cached(live, monkeypatch, answer) -> None:
    monkeypatch.setattr(rna_index, "rnas_for_siren", lambda s: [])
    with respx.mock() as mock:
        route = mock.get(joafe.RECORDS_URL)
        if isinstance(answer, Exception):
            route.mock(side_effect=answer)
        else:
            route.mock(return_value=answer)
        failed = await joafe.notices("425138393")
        route.mock(return_value=httpx.Response(200, json={"total_count": 0, "results": []}))
        again = await joafe.notices("425138393")
    assert failed["status"] == "unavailable"
    assert failed["reason"] == "upstream_error"
    assert again["status"] == "read" and again["rows"] == []


async def test_without_the_index_the_answer_is_accounts_only_and_not_cached(live, monkeypatch) -> None:
    monkeypatch.setattr(rna_index, "rnas_for_siren", lambda s: None)
    with respx.mock() as mock:
        route = mock.get(joafe.RECORDS_URL).mock(
            return_value=httpx.Response(200, json={"total_count": 0, "results": []}))
        first = await joafe.notices("425138393")
        await joafe.notices("425138393")
    assert first["rna_status"] == "no_index"
    assert route.call_count == 2
    assert route.calls[0].request.url.params["where"] == 'dca_siren="425138393"'


async def test_offline_nothing_is_asked() -> None:
    answer = await joafe.notices("425138393")
    assert answer == {"status": "unavailable", "reason": "not_configured", "siren": "425138393"}


def test_not_a_siren_is_refused() -> None:
    with pytest.raises(ValueError):
        import asyncio

        asyncio.run(joafe.notices("42513"))


# --- the History service -------------------------------------------------------


def test_registration_numbers_reads_a_french_siren_from_every_authority() -> None:
    for ra, filed in (("RA000189", "425138393"), ("RA000192", "425 138 393"), ("RA001129", "425138393")):
        payload = {"data": {"attributes": {"entity": {
            "registeredAs": filed, "registeredAt": {"id": ra},
            "jurisdiction": "FR", "legalName": {"name": "TRANSPARENCY INTERNATIONAL FRANCE"},
        }}}}
        assert service._registration_numbers(payload)[8] == "425138393"
    other = {"data": {"attributes": {"entity": {"registeredAs": "425138393", "registeredAt": {"id": "RA000190"}}}}}
    assert service._registration_numbers(other)[8] is None


def _french_registration(monkeypatch) -> None:
    async def registration(_client, _lei):
        return (None,) * 7 + ("TRANSPARENCY INTERNATIONAL FRANCE", "425138393")

    async def modifications(_client, _lei):
        return [], []

    monkeypatch.setattr(service, "_gleif_registration", registration)
    monkeypatch.setattr(service, "_gleif_modifications", modifications)

    # The same SIREN keys the BODACC notices; answer them as read and empty.
    from opencheck import bodacc

    async def notices_for(_siren, **_k):
        return {"notices": [], "truncated": False}

    monkeypatch.setattr(bodacc, "notices_for", notices_for)


async def test_the_history_service_reads_the_joafe(live, monkeypatch) -> None:
    _french_registration(monkeypatch)
    rows = _rows("tif_425138393.json")

    async def notices(siren):
        assert siren == "425138393"
        return {"status": "read", "siren": siren, "rnas": ["W922001074"], "rna_status": "read",
                "rows": rows, "truncated": False}

    monkeypatch.setattr(joafe, "notices", notices)
    timeline = await service.fetch_timeline("969500AEH12X8M5XEO53")
    fr = [e for e in timeline.events if e.source_id == "joafe"]
    assert len(fr) == 9
    assert timeline.registry_numbers["joafe"] == "W922001074"
    assert timeline.registers_unavailable == {}
    notable = {e.change_type for e in timeline.notable}
    assert {ChangeType.LEGAL_NAME_CHANGE, ChangeType.ADDRESS_CHANGE} <= notable


async def test_a_joafe_that_did_not_answer_is_said(live, monkeypatch) -> None:
    _french_registration(monkeypatch)

    async def notices(siren):
        return {"status": "unavailable", "reason": "timeout", "siren": siren}

    monkeypatch.setattr(joafe, "notices", notices)
    timeline = await service.fetch_timeline("969500AEH12X8M5XEO53")
    assert timeline.registers_unavailable == {"joafe": "timeout"}
    assert "joafe" not in timeline.registry_numbers
