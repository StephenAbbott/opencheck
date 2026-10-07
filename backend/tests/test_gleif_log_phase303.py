"""Phase 303: GLEIF's field-modification log — the module against a mocked
GLEIF API. Shapes are the ones the live API served on 7 Oct 2026: newest
first, pages of up to 200, inconsistent XPath strings, relationship changes
under the child LEI with ``context``."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from opencheck import gleif_log as gl
from opencheck.gleif_throttle import GleifRateLimitedError

LEI = "254900RT9QQBQZVH8O89"
SINCE = datetime(2026, 9, 1, tzinfo=UTC)
REG = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Registration/lei:RegistrationStatus"


def _mod(date: str, field: str = REG, old: str | None = "ISSUED", new: str | None = "LAPSED", *,
         kind: str = "UPDATE", record: str = "LEI", context: dict | None = None) -> dict:
    return {"lei": LEI, "recordType": record, "modificationType": kind, "field": field,
            "date": date, "valueOld": old, "valueNew": new, "context": context}


# ---- labels -----------------------------------------------------------------


def test_labels_read_the_trailing_elements_of_any_xpath_shape() -> None:
    assert gl.label(REG) == "registration status"
    assert gl.label("/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Entity/lei:HeadquartersAddress/lei:City") == "headquarters address city"
    assert gl.label("lei:Entity/lei:RegistrationAuthority/lei:RegistrationAuthorityEntityID") == "register number"
    assert gl.label("/lei:Entity/lei:LegalEntityEvents/lei:LegalEntityEvent/lei:LegalEntityEventStatus") == "legal entity event status"  # the container is not repeated
    rr = gl.label(
        "RelationshipRecords/rr:RelationshipRecord/rr:Relationship/rr:RelationshipStatus", "RR",
        {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY", "endNode": "21380068P1DRHMJ8KU70"},
    )
    assert rr == "direct parent relationship (21380068P1DRHMJ8KU70): relationship status"
    assert gl.label("", None) == "field"


def test_renewal_clocks_and_validation_references_are_noise_even_truncated() -> None:
    raw = [
        _mod("2026-09-20T00:00:00Z", "a/rr:RelationshipRecords/rr:RelationshipRecord/rr:Registration/rr:LastUpdateDate", record="RR"),
        _mod("2026-09-20T00:00:00Z", "/lei:Registration/lei:NextRenewalDate"),
        _mod("2026-09-20T00:00:00Z", "RelationshipRecords/rr:Registration/rr:ValidationReference", record="RR"),
        _mod("2026-09-20T00:00:00Z"),
    ]
    log = gl.summarise(raw, SINCE)
    assert [i["label"] for i in log["items"]] == ["registration status"]


# ---- summarise / after ----------------------------------------------------------


def test_summarise_keeps_lines_after_the_cutoff_newest_first() -> None:
    raw = [
        _mod("2026-09-01T00:00:00Z", old="A", new="B"),  # the baseline's own publish: already in it
        _mod("2026-09-16T00:00:00Z"),
        _mod("2026-09-10T00:00:00Z", "/lei:Entity/lei:LegalName", "Old Ltd", "New Ltd"),
        _mod("2026-08-01T00:00:00Z", old="X", new="Y"),
    ]
    log = gl.summarise(raw, SINCE)
    assert log["available"] is True and log["since"] == "2026-09-01T00:00:00Z"
    assert [(i["date"], i["label"]) for i in log["items"]] == [
        ("2026-09-16", "registration status"), ("2026-09-10", "legal name"),
    ]
    assert log["more"] == 0


def test_summarise_caps_the_lines_and_counts_the_rest() -> None:
    raw = [_mod(f"2026-09-{d:02d}T00:00:00Z", old=str(d), new=str(d + 1)) for d in range(2, 30)] * 2
    log = gl.summarise(raw, SINCE)
    assert len(log["items"]) == gl.MAX_ITEMS and log["more"] == len(raw) - gl.MAX_ITEMS


def test_relationship_lines_carry_their_context() -> None:
    raw = [_mod("2026-09-20T00:00:00Z", "rr:RelationshipRecord/rr:Relationship/rr:RelationshipStatus",
                "ACTIVE", "INACTIVE", record="RR",
                context={"relationshipType": "IS_ULTIMATELY_CONSOLIDATED_BY", "endNode": "TOP"})]
    item = gl.summarise(raw, SINCE)["items"][0]
    assert item["record"] == "RR" and item["relationship"] == {"type": "IS_ULTIMATELY_CONSOLIDATED_BY", "end_node": "TOP"}
    assert item["label"].startswith("ultimate parent relationship (TOP)")


def test_after_gives_each_list_its_own_share() -> None:
    raw = [_mod("2026-09-16T00:00:00Z"), _mod("2026-09-05T00:00:00Z", "/lei:Entity/lei:LegalName", "A", "B")]
    log = gl.summarise(raw, SINCE)
    later = gl.after(log, datetime(2026, 9, 10, tzinfo=UTC))
    assert [i["date"] for i in later["items"]] == ["2026-09-16"] and later["since"] == "2026-09-10T00:00:00Z"
    assert gl.after(log, None) is log
    off = gl.unavailable("GLEIF's API was rate-limited", SINCE)
    assert gl.after(off, datetime(2026, 9, 10, tzinfo=UTC)) is off
    assert gl.after(None, SINCE) is None


# ---- fetch_since against a mocked API ----------------------------------------


@pytest.fixture
def live(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gl, "get_settings", lambda: SimpleNamespace(allow_live=True))


def _serve(monkeypatch: pytest.MonkeyPatch, handler) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    monkeypatch.setattr(gl, "build_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(wrapped)))
    return seen


def _page(rows: list[dict], last_page: int) -> httpx.Response:
    return httpx.Response(200, json={"data": [{"attributes": r} for r in rows],
                                     "meta": {"pagination": {"lastPage": last_page}}})


async def test_one_page_is_one_request_newest_first(monkeypatch: pytest.MonkeyPatch, live: None) -> None:
    seen = _serve(monkeypatch, lambda r: _page([_mod("2026-09-16T00:00:00Z"), _mod("2026-08-01T00:00:00Z")], 4))
    log = await gl.fetch_since(LEI, SINCE)
    assert len(seen) == 1  # the oldest line on page 1 is already past the cut-off
    params = seen[0].url.params
    assert params["sort"] == "-date" and params["page[size]"] == "200" and params["page[number]"] == "1"
    assert seen[0].url.path.endswith(f"/lei-records/{LEI}/field-modifications")
    assert [i["date"] for i in log["items"]] == ["2026-09-16"]


async def test_pages_until_the_cutoff_and_no_further(monkeypatch: pytest.MonkeyPatch, live: None) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        page = int(r.url.params["page[number]"])
        if page == 1:
            return _page([_mod("2026-09-20T00:00:00Z")] * 200, 9)
        return _page([_mod("2026-09-15T00:00:00Z"), _mod("2026-08-30T00:00:00Z")], 9)

    seen = _serve(monkeypatch, handler)
    log = await gl.fetch_since(LEI, SINCE)
    assert len(seen) == 2
    assert len(log["items"]) == gl.MAX_ITEMS and log["more"] == 201 - gl.MAX_ITEMS


async def test_stops_at_the_last_page_and_at_max_pages(monkeypatch: pytest.MonkeyPatch, live: None) -> None:
    seen = _serve(monkeypatch, lambda r: _page([_mod("2026-09-20T00:00:00Z")], 1))
    await gl.fetch_since(LEI, SINCE)
    assert len(seen) == 1
    seen = _serve(monkeypatch, lambda r: _page([_mod("2026-09-20T00:00:00Z")] * 200, 99))
    await gl.fetch_since(LEI, SINCE)
    assert len(seen) == gl.MAX_PAGES


@pytest.mark.parametrize(
    ("handler", "reason"),
    [
        (lambda r: httpx.Response(429), "GLEIF's API answered HTTP 429"),
        (lambda r: httpx.Response(503), "GLEIF's API answered HTTP 503"),
        (lambda r: (_ for _ in ()).throw(GleifRateLimitedError("budget")), "GLEIF's API was rate-limited"),
        (lambda r: (_ for _ in ()).throw(httpx.ConnectError("down")), "GLEIF's API could not be reached"),
        (lambda r: httpx.Response(200, text="<html>"), "GLEIF's API could not be reached"),
    ],
)
async def test_every_failure_is_returned_not_raised(monkeypatch: pytest.MonkeyPatch, live: None, handler, reason: str) -> None:
    _serve(monkeypatch, handler)
    log = await gl.fetch_since(LEI, SINCE)
    assert log == {"available": False, "reason": reason, "since": "2026-09-01T00:00:00Z", "items": [], "more": 0}


async def test_no_request_when_live_calls_are_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gl, "get_settings", lambda: SimpleNamespace(allow_live=False))
    seen = _serve(monkeypatch, lambda r: _page([], 1))
    log = await gl.fetch_since(LEI, SINCE)
    assert seen == [] and log["available"] is False and log["reason"] == "live calls are switched off"


def test_the_client_is_the_throttled_one() -> None:
    """The 50/min GLEIF budget lives in build_client's transport; the module
    must not open its own client."""
    from opencheck import http

    assert gl.build_client is http.build_client
