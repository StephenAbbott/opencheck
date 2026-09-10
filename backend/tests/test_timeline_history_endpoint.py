"""Tests for the /history (Time Machine) service + endpoint.

GLEIF and Companies House HTTP are mocked with respx; the route function is
called directly so the app lifespan (warm-ups) is not involved. Morrisons shapes
(LEI 213800IN6LSRGTZSOS29 / company 00358949).
"""

from __future__ import annotations

import re

import pytest
import respx
from fastapi import HTTPException
from httpx import ConnectTimeout, Response

from opencheck.config import get_settings
from opencheck.gleif_throttle import GleifRateLimitedError
from opencheck.routers.history import history
from opencheck.timeline import service as tl_service

_LEI = "213800IN6LSRGTZSOS29"
_LEI_PREFIX = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/"
_RR_PREFIX = "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"

_GLEIF_RECORD = {
    "data": {
        "attributes": {
            "entity": {
                "legalName": {"name": "WM MORRISON SUPERMARKETS LIMITED"},
                "registeredAs": "00358949",
                "registeredAt": {"id": "RA000585"},
                "jurisdiction": "GB",
            }
        }
    }
}


def _mod(attrs: dict) -> dict:
    return {"type": "field-modifications", "id": "x", "attributes": attrs}


_GLEIF_MODS = {
    "data": [
        _mod({
            "lei": _LEI, "recordType": "LEI", "modificationType": "UPDATE",
            "field": _LEI_PREFIX + "lei:Entity/lei:LegalName",
            "date": "2021-12-09T16:00:00Z",
            "valueOld": "WM MORRISON SUPERMARKETS P L C",
            "valueNew": "WM MORRISON SUPERMARKETS LIMITED",
        }),
        _mod({
            "lei": _LEI, "recordType": "LEI", "modificationType": "UPDATE",
            "field": _LEI_PREFIX + "lei:Registration/lei:NextRenewalDate",
            "date": "2025-11-20T00:00:00Z",
            "valueOld": "2026-01-11T00:00:00Z", "valueNew": "2027-01-11T00:00:00Z",
        }),
        _mod({
            "lei": _LEI, "recordType": "RR", "modificationType": "INITIAL",
            "field": _RR_PREFIX + "rr:Relationship/rr:RelationshipType",
            "date": "2023-11-25T00:00:00Z", "valueOld": None,
            "valueNew": "IS_DIRECTLY_CONSOLIDATED_BY",
            "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "endNode": "549300RKU7UEPSC42U63"},
        }),
        _mod({
            "lei": _LEI, "recordType": "RR", "modificationType": "INITIAL",
            "field": _RR_PREFIX
            + "rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:StartDate",
            "date": "2023-11-25T00:00:00Z", "valueOld": None,
            "valueNew": "2021-11-01T00:00:00Z",
            "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "endNode": "549300RKU7UEPSC42U63"},
        }),
    ],
    "meta": {"pagination": {"lastPage": 1}},
}

_CH_FILINGS = {
    "items": [
        {"category": "change-of-name", "type": "CONNOT", "date": "2022-01-05",
         "action_date": "2021-12-01", "links": {"self": "/company/00358949/filing-history/a"}},
        {"category": "persons-with-significant-control", "type": "PSC02",
         "date": "2021-11-15", "links": {"self": "/company/00358949/filing-history/b"}},
        {"category": "confirmation-statement", "type": "CS01", "date": "2022-03-01",
         "links": {"self": "/company/00358949/filing-history/c"}},
    ],
    "total_count": 3,
}

# Phase 198: the board stream's real source. Two officers — one still serving,
# one who resigned — and the register's officer id on each, which is the whole
# reason the stream moved here from the filing history.
_CH_OFFICERS = {
    "items": [
        {
            "name": "BENNETT, Kelly Brian",
            "officer_role": "director",
            "appointed_on": "2019-01-01",
            "links": {"officer": {"appointments": "/officers/kb-1/appointments"}},
        },
        {
            "name": "PRIOR, Jane",
            "officer_role": "director",
            "appointed_on": "1998-04-01",
            "resigned_on": "2004-09-30",
            "links": {"officer": {"appointments": "/officers/jp-2/appointments"}},
        },
    ],
    "active_count": 1,
    "resigned_count": 1,
    "total_results": 2,
}


def _mock_live():
    respx.get(f"https://api.gleif.org/api/v1/lei-records/{_LEI}").mock(
        return_value=Response(200, json=_GLEIF_RECORD)
    )
    respx.get(url__regex=rf"https://api\.gleif\.org/api/v1/lei-records/{_LEI}/field-modifications").mock(
        return_value=Response(200, json=_GLEIF_MODS)
    )
    respx.get(url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/filing-history").mock(
        return_value=Response(200, json=_CH_FILINGS)
    )
    respx.get(url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/officers").mock(
        return_value=Response(200, json=_CH_OFFICERS)
    )


@pytest.mark.asyncio
async def test_history_invalid_lei_returns_400():
    with pytest.raises(HTTPException) as exc:
        await history(request=None, response=None, lei="not-a-lei", include_noise=False)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_history_stub_mode_is_unavailable(monkeypatch):
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()
    assert resp.available is False
    assert resp.notable == []


@pytest.mark.asyncio
async def test_history_live_merges_gleif_and_companies_house(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    # The dedicated history key is used (separate from the lookup adapter's key).
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.available is True
    assert resp.company_number == "00358949"
    assert set(resp.sources) == {"gleif", "companies_house"}

    # Name change corroborated across both sources, CH (effective) date wins.
    name = [e for e in resp.notable if e.change_type == "LEGAL_NAME_CHANGE"]
    assert len(name) == 1
    assert name[0].sources == ["companies_house", "gleif"]
    assert name[0].date == "2021-12-01"
    assert name[0].date_basis == "effective"

    # GLEIF corporate-parent add carries the period start + parent LEI.
    gleif_owner = [e for e in resp.notable
                   if e.change_type == "OWNER_ADDED" and "gleif" in e.sources]
    assert len(gleif_owner) == 1
    assert gleif_owner[0].interest_start_date == "2021-11-01"
    assert gleif_owner[0].counterparty == "549300RKU7UEPSC42U63"

    # CH PSC add is a separate ownership entry.
    ch_owner = [e for e in resp.notable
                if e.change_type == "OWNER_ADDED" and "companies_house" in e.sources]
    assert len(ch_owner) == 1

    # include_noise exposed the Tier-3 rows (NextRenewalDate, CS01).
    assert any(ev.tier == 3 for ev in resp.events)
    assert resp.notable_count == len(resp.notable)


@pytest.mark.asyncio
async def test_history_degrades_to_gleif_only_without_ch_key(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_HISTORY_API_KEY", raising=False)
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()  # CH route present but should never be called
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.available is True
    assert resp.sources == ["gleif"]  # Companies House not contacted
    assert any(e.change_type == "LEGAL_NAME_CHANGE" for e in resp.notable)


# ---------------------------------------------------------------------------
# Phase 146 — GLEIF refusing must not read as "checked, no history"
# ---------------------------------------------------------------------------

_RECORD_URL = f"https://api.gleif.org/api/v1/lei-records/{_LEI}"
_MODS_URL_RE = rf"https://api\.gleif\.org/api/v1/lei-records/{_LEI}/field-modifications"


def _refusals():
    """The three shapes GLEIF saturation actually produces."""
    return [
        Response(429),
        GleifRateLimitedError("budget exhausted"),
        ConnectTimeout("timed out"),
    ]


def _mock_refusal(url_or_regex, outcome, *, regex=False):
    route = (
        respx.get(url__regex=url_or_regex) if regex else respx.get(url_or_regex)
    )
    if isinstance(outcome, BaseException):
        route.mock(side_effect=outcome)
    else:
        route.mock(return_value=outcome)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    _refusals(),
    ids=["429-handed-back", "throttle-refused-to-send", "network"],
)
async def test_gleif_refusal_is_declared_not_an_empty_timeline(monkeypatch, tmp_path, outcome):
    """The silent failure the Phase 145 pattern is being extended to cover.

    Both GLEIF calls refused, nothing cached: the response must say the
    history could not be checked AND that the registry sources could not even
    be attempted — not return `available: false` as if it had looked.
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_refusal(_RECORD_URL, outcome)
        _mock_refusal(_MODS_URL_RE, outcome, regex=True)
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.gleif_record_available is False
    assert resp.gleif_events_available is False
    assert resp.registry_sources_blocked is True
    assert resp.company_number_basis is None
    assert resp.company_number is None
    assert resp.notable == [] and resp.events == []


@pytest.mark.asyncio
async def test_change_log_refusal_alone_keeps_the_registry_sources(monkeypatch, tmp_path):
    """Record fine, change log rate-limited: Companies House still runs, and
    only the GLEIF half is declared missing."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        respx.get(_RECORD_URL).mock(return_value=Response(200, json=_GLEIF_RECORD))
        _mock_refusal(_MODS_URL_RE, Response(429), regex=True)
        respx.get(
            url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/filing-history"
        ).mock(return_value=Response(200, json=_CH_FILINGS))
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.gleif_record_available is True
    assert resp.gleif_events_available is False
    assert resp.registry_sources_blocked is False
    assert resp.company_number == "00358949"
    assert resp.company_number_basis == "live"
    assert resp.sources == ["companies_house"]  # CH ran; GLEIF contributed nothing


@pytest.mark.asyncio
async def test_cached_gleif_record_keeps_the_registry_sources_alive(monkeypatch, tmp_path):
    """The compounding half of the bug: without the registry number the CH /
    NZ / EE / DK branches are never even attempted. The GLEIF adapter's cached
    record supplies it (the Golden Copy snapshot cannot — it holds no
    registeredAs), and the response says the number came from cache."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    tl_service._cache.put(f"gleif/lei/{_LEI}", _GLEIF_RECORD)
    with respx.mock:
        _mock_refusal(_RECORD_URL, Response(429))
        _mock_refusal(_MODS_URL_RE, Response(429), regex=True)
        respx.get(
            url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/filing-history"
        ).mock(return_value=Response(200, json=_CH_FILINGS))
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.gleif_record_available is False
    assert resp.company_number == "00358949"
    assert resp.company_number_basis == "cached"
    # Attempted, not blocked — and the Companies House history actually landed.
    assert resp.registry_sources_blocked is False
    assert resp.sources == ["companies_house"]
    assert resp.available is True


@pytest.mark.asyncio
async def test_healthy_fetch_declares_everything_available(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()
    assert resp.gleif_record_available is True
    assert resp.gleif_events_available is True
    assert resp.registry_sources_blocked is False
    assert resp.company_number_basis == "live"


# --------------------------------------------------------------------------- #
# Phase 190 — registry_numbers: how each register addresses this company
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_registry_numbers_carry_the_number_each_register_uses(monkeypatch):
    """The History tab links a dated row back to the record that published it.

    A GLEIF row is addressed by the LEI the caller already has; every other
    register needs its own number, and until Phase 190 the service derived
    those, used them to decide which history calls to make, and dropped them —
    so a Danish or Estonian row could be shown and not sourced.
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.registry_numbers == {"companies_house": "00358949"}
    # GLEIF is deliberately absent: it addresses the company by the LEI, which
    # every caller of this endpoint already holds. A key here would be a second
    # copy of `lei` that could go stale against it.
    assert "gleif" not in resp.registry_numbers
    # It agrees with the field that has always been there.
    assert resp.registry_numbers["companies_house"] == resp.company_number


@pytest.mark.asyncio
async def test_registry_numbers_are_read_per_registration_authority(monkeypatch):
    """A Danish company yields the CVR number under `cvr_denmark`, not the CH key.

    The four national numbers all come out of one GLEIF field, `registeredAs`,
    and are told apart only by `registeredAt`. Keying on the wrong authority is
    the failure that would put a Danish number behind a Companies House link.
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("CVR_DENMARK_API_KEY", raising=False)
    get_settings.cache_clear()
    danish = {
        "data": {
            "attributes": {
                "entity": {
                    "legalName": {"name": "A/S DANSK EKSEMPEL"},
                    "registeredAs": "12345678",
                    "registeredAt": {"id": "RA000170"},
                    "jurisdiction": "DK",
                }
            }
        }
    }
    with respx.mock:
        respx.get(f"https://api.gleif.org/api/v1/lei-records/{_LEI}").mock(
            return_value=Response(200, json=danish)
        )
        respx.get(
            url__regex=rf"https://api\.gleif\.org/api/v1/lei-records/{_LEI}/field-modifications"
        ).mock(return_value=Response(200, json=_GLEIF_MODS))
        resp = await history(request=None, response=None, lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.registry_numbers == {"cvr_denmark": "12345678"}
    assert resp.company_number is None
    # No CVR key is set, so Danish history was never fetched. The number says
    # the register knows the company, and `sources` says who actually answered
    # — two different statements, and conflating them is how an unchecked
    # register would read as a checked one.
    assert "cvr_denmark" not in resp.sources


# ---------------------------------------------------------------------------
# Phase 198 — the board stream comes from the officers list, and its rows
# carry an identity, not just a name
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_board_rows_come_from_the_officers_list_and_identify_the_person(
    monkeypatch,
):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.officers_available is True
    board = [ev for ev in resp.events if ev.tier == 4]
    # One appointment each, and one resignation — three dated ends of two
    # appointments, which is what the register actually records.
    assert {(ev.change_type, ev.counterparty, ev.event_date) for ev in board} == {
        ("OFFICER_APPOINTED", "BENNETT, Kelly Brian", "2019-01-01"),
        ("OFFICER_APPOINTED", "PRIOR, Jane", "1998-04-01"),
        ("OFFICER_RESIGNED", "PRIOR, Jane", "2004-09-30"),
    }
    # The part a filing-history row could never carry.
    assert all(ev.party_id for ev in board)
    assert all(ev.party_statement_id for ev in board)
    # Both of Jane Prior's rows point at one person, not two.
    prior = {ev.party_statement_id for ev in board if ev.counterparty == "PRIOR, Jane"}
    assert len(prior) == 1


@pytest.mark.asyncio
async def test_officer_filings_no_longer_double_the_board(monkeypatch):
    """The filing history still carries officer filings and they are still
    typed — they just do not appear on the board stream, which now has a
    better record of the same events."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    filings = dict(_CH_FILINGS)
    filings["items"] = _CH_FILINGS["items"] + [
        {
            "category": "officers", "type": "AP01", "date": "2019-01-14",
            "description_values": {"officer_name": "Mr Kelly Brian Bennett"},
            "links": {"self": "/company/00358949/filing-history/d"},
        }
    ]
    with respx.mock:
        _mock_live()
        respx.get(
            url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/filing-history"
        ).mock(return_value=Response(200, json=filings))
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    ap01 = [ev for ev in resp.events if ev.raw_change_type == "AP01"]
    assert len(ap01) == 1
    assert ap01[0].tier == 3
    assert ap01[0].change_type == "OFFICER_APPOINTED"  # typed, just not Tier 4
    assert ap01[0].party_statement_id is None  # a filing identifies nobody
    # Kelly Bennett appears once on the board, from the officers list.
    board_bennett = [
        ev for ev in resp.events if ev.tier == 4 and ev.counterparty == "BENNETT, Kelly Brian"
    ]
    assert len(board_bennett) == 1


@pytest.mark.asyncio
async def test_an_unread_officers_list_says_so_rather_than_showing_an_empty_board(
    monkeypatch,
):
    """No key means the officers list was never asked for. An empty board
    stream then means "not checked", and the flag is how the tab knows."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_HISTORY_API_KEY", raising=False)
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.officers_available is False
    assert not [ev for ev in resp.events if ev.tier == 4]


@pytest.mark.asyncio
async def test_the_officers_list_refusing_does_not_sink_the_filing_history(
    monkeypatch,
):
    """Two independent reads of one register. Phase 194's history survives
    Phase 198's officers call failing."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        respx.get(
            url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/officers"
        ).mock(side_effect=ConnectTimeout("timed out"))
        resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.officers_available is False
    assert any(e.change_type == "LEGAL_NAME_CHANGE" for e in resp.notable)
    assert "companies_house" in resp.sources
