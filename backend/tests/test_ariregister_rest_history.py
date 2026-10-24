"""The ownership history's people from RIK's REST API (the REST-history phase).

``GET /company/{code}`` with ``onlyValid=false`` returns the rows SOAP
``detailandmed_v2`` returns with ``ainult_kehtivad=0``, as JSON, under the
same ``recordId``. Stephen's decision (17 Oct 2026): the ownership graph's
history reads REST; the History tab keeps SOAP, which names a corporate party
as filed at the time, and takes REST only without SOAP credentials. The
beneficial-owner history stays on SOAP.

Fixtures in ``fixtures/ariregister_rest/history/`` are real REST and SOAP
answers for Eesti Energia AS (10421629) and Bolt Operations OÜ (14532901),
read on 17 Oct 2026 within a minute of each other, with every personal
identification code replaced by the same fake in both (so the joins still
join), people's addresses below country, e-mails and birth dates removed.
"""

from __future__ import annotations

import asyncio
import copy
import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods.mapper import map_ariregister
from opencheck.config import get_settings
from opencheck.sources import REGISTRY
from opencheck.sources import ariregister
from opencheck.sources import ariregister_history as h
from opencheck.sources import ariregister_rest as rik
from opencheck.sources.ariregister_rest_company import bundle_from_rest
from opencheck.timeline.ariregister import ariregister_change_events
from opencheck.timeline.model import ChangeType

F = Path(__file__).parent / "fixtures" / "ariregister_rest" / "history"
EE = "10421629"
BOLT = "14532901"
KEY = "rest-key-1234567890-secret"
_PERSONAL = re.compile(r"(?<!\d)[1-6]\d{10}(?!\d)")


def _rest(code: str) -> dict[str, Any]:
    return json.loads((F / f"rest_{code}.json").read_text(encoding="utf-8"))


def _soap(code: str) -> str:
    return (F / f"soap_detail_{code}.xml").read_text(encoding="utf-8")


def _bo(code: str) -> str:
    return (F / f"soap_bo_{code}.xml").read_text(encoding="utf-8")


def _codes(code: str) -> set[str]:
    """The (fake) personal codes the fixtures carry — the join keys."""
    return set(_PERSONAL.findall((F / f"rest_{code}.json").read_text(encoding="utf-8")))


def _record(code: str) -> dict[str, Any]:
    """An index record for the company: every current row with a hash, as
    the index holds them, so the register join is exercised."""
    rows = [r for r in h.rest_person_rows(_rest(code)) if not r.get("lopp_kpv")]
    return {"shareholders": [
        {"kirje_id": r["kirje_id"], "isikukood_hash": f"hash-{r['kirje_id']}"} for r in rows
        if (r.get("isiku_tyyp") or "F") == "F"
    ]}


# --------------------------------------------------------------------------- #
# The rows: REST and SOAP name the same rows
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("code", [EE, BOLT])
def test_rest_and_soap_carry_the_same_rows(code: str) -> None:
    soap_root = h._parse(_soap(code))
    soap = h._rows(soap_root, "kaardile_kantud_isikud") + h._rows(soap_root, "kaardivalised_isikud")
    rest = {r["kirje_id"]: r for r in h.rest_person_rows(_rest(code), keep_codes=True)}
    assert {r["kirje_id"] for r in soap} == set(rest)
    for row in soap:
        twin = rest[row["kirje_id"]]
        for field in ("isiku_tyyp", "isiku_roll", "isiku_roll_tekstina", "isikukood_registrikood", "eesnimi"):
            assert row.get(field) == twin.get(field), (row["kirje_id"], field)
        if row.get("isiku_roll") in h.SHAREHOLDER_ROLES:
            # A founder's contribution currency is on SOAP's row only; the
            # history reads shareholdings, not founders.
            for field in ("osaluse_protsent", "osaluse_suurus", "osaluse_valuuta"):
                assert row.get(field) == twin.get(field), (row["kirje_id"], field)
        for field in ("algus_kpv", "lopp_kpv", "kontrolli_allika_kpv"):
            assert h._day(row.get(field)) == h._day(twin.get(field)), (row["kirje_id"], field)


def test_eesti_energia_counts_match_the_evaluation() -> None:
    rows = h.rest_person_rows(_rest(EE))
    assert len(rows) == 163 and sum(1 for r in rows if r.get("lopp_kpv")) == 147


def test_without_keep_codes_no_natural_persons_code_is_copied() -> None:
    for code in (EE, BOLT):
        for row in h.rest_person_rows(_rest(code)):
            if (row.get("isiku_tyyp") or "F") == "F":
                assert "isikukood_registrikood" not in row and "valis_kood" not in row


def test_card_entries_read_the_same_from_rest() -> None:
    for code in (EE, BOLT):
        soap = h.history_from_responses(_soap(code), _bo(code), {})["card_entries"]
        rest = h.rest_card_entries(_rest(code))
        assert soap == rest


# --------------------------------------------------------------------------- #
# The history and the graph
# --------------------------------------------------------------------------- #


def _comparable(history: dict[str, Any]) -> list[dict[str, Any]]:
    """The ended rows without what REST words differently: a legal
    person's name (today's, not as filed) and a person's address country
    (each service fills it on different rows)."""
    out = []
    for row in history["former_shareholders"]:
        row = {k: v for k, v in row.items() if k not in ("aadress_riik", "aadress_riik_tekstina")}
        if row.get("isiku_tyyp") == "J":
            row.pop("nimi_arinimi", None)
        out.append(row)
    return sorted(out, key=lambda r: r["kirje_id"])


@pytest.mark.parametrize("code", [EE, BOLT])
def test_the_history_is_the_same_from_either_service(code: str) -> None:
    record = _record(code)
    soap = h.history_from_responses(_soap(code), _bo(code), record)
    rest = h.history_from_rest(_rest(code), _bo(code), record)
    assert _comparable(soap) == _comparable(rest)
    assert soap["former_beneficial_owners"] == rest["former_beneficial_owners"]
    assert len(rest["former_shareholders"]) == {EE: 13, BOLT: 6}[code]


@pytest.mark.parametrize("code", [EE, BOLT])
def test_no_personal_code_leaves_the_rest_history(code: str) -> None:
    text = json.dumps(h.history_from_rest(_rest(code), _bo(code), _record(code)))
    assert not (_codes(code) & set(_PERSONAL.findall(text)))


def _statements(code: str, history: dict[str, Any]) -> dict[str, dict[str, Any]]:
    bundle = bundle_from_rest(_rest(code), code)
    bundle["history"] = history
    with provenance.recording() as rec:
        provenance.record_live("test")
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        statements = list(map_ariregister(bundle))
    out = {}
    for s in statements:
        s = copy.deepcopy(s)
        s.pop("source", None)
        s.pop("publicationDetails", None)
        out[s["recordId"]] = s
    return out


@pytest.mark.parametrize("code", [EE, BOLT])
def test_the_graph_is_the_same_apart_from_a_legal_persons_spelling(code: str) -> None:
    soap = _statements(code, h.history_from_responses(_soap(code), _bo(code), {}))
    rest = _statements(code, h.history_from_rest(_rest(code), _bo(code), {}))
    assert set(soap) == set(rest)
    for sid, statement in soap.items():
        twin = rest[sid]
        if statement == twin:
            continue
        # The one difference allowed: an entity named by the register today.
        assert statement["recordType"] == "entity"
        a = copy.deepcopy(statement)
        b = copy.deepcopy(twin)
        a["recordDetails"].pop("name")
        b["recordDetails"].pop("name")
        # The statementId is content-addressed, so it follows the name.
        a.pop("statementId")
        b.pop("statementId")
        assert a == b
        assert statement["recordDetails"]["name"].casefold().split(":")[-1].strip() == \
            twin["recordDetails"]["name"].casefold()


# --------------------------------------------------------------------------- #
# The History tab: SOAP when it can, REST without the credentials
# --------------------------------------------------------------------------- #


def test_the_history_tab_reads_the_same_people_and_dates_from_rest() -> None:
    soap = ariregister_change_events({"registry_code": EE, "detail_xml": _soap(EE), "bo_xml": _bo(EE)})
    rest = ariregister_change_events({"registry_code": EE, "detail_rest": _rest(EE), "bo_xml": _bo(EE)})
    assert len(soap) == len(rest)

    def people(events: list[Any]) -> list[tuple[Any, ...]]:
        return sorted(
            (e.change_type.value, e.counterparty, e.event_date) for e in events
            if e.change_type in (ChangeType.OWNER_ADDED, ChangeType.OWNER_REMOVED)
            and " — " in (e.counterparty or "") and "ministeerium" not in (e.counterparty or "").casefold()
        )

    assert people(soap) == people(rest)

    def names(events: list[Any]) -> list[tuple[Any, ...]]:
        return [(e.value_old, e.value_new, e.event_date) for e in events if e.change_type is ChangeType.LEGAL_NAME_CHANGE]

    assert names(soap) == names(rest) == [("Eesti Energia Aktsiaselts", "Eesti Energia AS", "2022-05-27")]


def test_a_deletion_reads_as_soap_has_it() -> None:
    body = json.loads((F.parent / "rest_company_11336737.json").read_text(encoding="utf-8"))
    soap = (F / "soap_detail_11336737.xml").read_text(encoding="utf-8")

    def statuses(events: list[Any]) -> list[tuple[Any, ...]]:
        return [(e.value_old, e.value_new, e.event_date) for e in events if e.change_type is ChangeType.STATUS_CHANGED]

    a = statuses(ariregister_change_events({"registry_code": "11336737", "detail_xml": soap}))
    b = statuses(ariregister_change_events({"registry_code": "11336737", "detail_rest": body}))
    assert a == b == [("Entered into the register", "Deleted", "2017-01-10")]


# --------------------------------------------------------------------------- #
# fetch_timeline_data and read_history
# --------------------------------------------------------------------------- #


@pytest.fixture()
def live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.setenv("ARIREGISTER_QUEUE_WAIT_S", "5")
    monkeypatch.setenv("ARIREGISTER_REST_API_KEY", KEY)
    get_settings.cache_clear()
    rik.reset_for_tests()
    yield monkeypatch
    get_settings.cache_clear()


def _mock(router: respx.MockRouter, code: str = EE) -> tuple[respx.Route, respx.Route]:
    router.get(f"{rik.REST_BASE}/keyLogin/").mock(
        return_value=httpx.Response(200, json={"access_token": "T", "token_type": "bearer"})
    )
    company = router.get(f"{rik.REST_BASE}/company/{code}").mock(
        return_value=httpx.Response(200, json=_rest(code))
    )

    def soap_answer(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        return httpx.Response(200, text=_bo(code) if "tegelikudKasusaajad" in body else _soap(code))

    soap = router.post("https://ariregxmlv6.rik.ee/").mock(side_effect=soap_answer)
    return company, soap


def _credentials(live: Any) -> None:
    live.setenv("ARIREGISTER_USERNAME", "u")
    live.setenv("ARIREGISTER_PASSWORD", "p")
    get_settings.cache_clear()


def test_the_graph_reads_rest_and_asks_soap_only_for_the_owners(live) -> None:
    _credentials(live)
    with respx.mock(assert_all_called=False) as router:
        company, soap = _mock(router)
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(EE, card_source="rest"))
    assert data["detail_rest"] and data["detail_xml"] is None and data["bo_xml"]
    assert company.call_count == 1 and soap.call_count == 1
    params = company.calls[0].request.url.params
    assert dict(params) == ariregister.HISTORY_QUERY
    assert "tegelikudKasusaajad" in soap.calls[0].request.content.decode()


def test_the_history_tab_keeps_soap_when_it_can(live) -> None:
    _credentials(live)
    with respx.mock(assert_all_called=False) as router:
        company, soap = _mock(router)
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(EE))
    assert data["detail_xml"] and data["detail_rest"] is None
    assert company.call_count == 0 and soap.call_count == 2


def test_without_soap_credentials_the_history_tab_reads_rest(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        company, soap = _mock(router)
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(EE))
    assert data["detail_rest"] and soap.call_count == 0
    assert data["unavailable"] == {"bo": h.BO_NOT_CONFIGURED}


def test_without_the_key_the_graph_reads_soap(live) -> None:
    live.setenv("ARIREGISTER_REST_API_KEY", "")
    _credentials(live)
    with respx.mock(assert_all_called=False) as router:
        company, soap = _mock(router)
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(EE, card_source="rest"))
    assert data["detail_xml"] and company.call_count == 0


def test_a_rest_failure_is_said_and_nothing_is_cached(live) -> None:
    _credentials(live)
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{rik.REST_BASE}/keyLogin/").mock(
            return_value=httpx.Response(200, json={"access_token": "T", "token_type": "bearer"})
        )
        router.get(f"{rik.REST_BASE}/company/{EE}").mock(return_value=httpx.Response(503))
        router.post("https://ariregxmlv6.rik.ee/").mock(return_value=httpx.Response(200, text=_bo(EE)))
        with degradation.recording() as degraded:
            history = asyncio.run(h.read_history(REGISTRY["ariregister"], EE, _record(EE)))
    assert history["read"] is False
    assert [d.check for d in degraded] == [degradation.CHECK_SOURCE_READ]
    assert not list(Path(live_root()).glob("**/history-v2/*.json"))


def live_root() -> str:
    import os

    return os.environ["OPENCHECK_DATA_ROOT"]


def test_rest_only_reads_the_holdings_and_says_the_owners_were_not(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        _mock(router)
        with degradation.recording() as degraded:
            history = asyncio.run(h.read_history(REGISTRY["ariregister"], EE, _record(EE)))
    assert history["read"] is True and history["bo_read"] is False
    assert len(history["former_shareholders"]) == 13
    assert [d.reason for d in degraded] == [degradation.REASON_NOT_CONFIGURED]
    assert "no open-data API credentials" in degraded[0].detail


def test_a_rest_read_is_cached_stripped(live) -> None:
    _credentials(live)
    with respx.mock(assert_all_called=False) as router:
        company, _ = _mock(router)
        first = asyncio.run(h.read_history(REGISTRY["ariregister"], EE, _record(EE)))
        second = asyncio.run(h.read_history(REGISTRY["ariregister"], EE, _record(EE)))
    assert first == second and company.call_count == 1
    (path,) = list(Path(live_root()).glob("**/history-v2/*.json"))
    assert not (_codes(EE) & set(_PERSONAL.findall(path.read_text(encoding="utf-8"))))


def test_a_natural_persons_rows_join_by_code_and_lose_it() -> None:
    """The real fixtures' ended holders are companies; this is the join for
    a person, in REST's shape: an ended holding joins the same person's
    current row (whose ``recordId`` the index holds) by the personal code,
    which is then dropped; a former-only person is keyed by a row."""
    code_now, code_gone = "38001010001", "37001010002"

    def row(rid: int, code: str, first: str, end: str | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {
            "recordId": rid, "startDate": "2010-01-01", "sharePercentage": 50.0, "shareSize": 1250.0,
            "shareCurrency": "EUR",
            "person": {"type": "F", "role": "O", "roleAsText": "Shareholder", "firstName": first,
                       "surnameLegalEntityName": "Tamm", "identityCode": code},
        }
        if end:
            out["endDate"] = end
        return out

    body = {"relatedPersons": {"shareholders": [
        row(61, code_now, "Kalev"), row(9001, code_now, "Kalev", "2018-05-01"),
        row(9002, code_gone, "Vana", "2015-01-01"), row(9003, code_gone, "Vana", "2016-01-01"),
    ]}}
    record = {"shareholders": [{"kirje_id": "61", "isikukood_hash": "hash-kalev"}]}
    history = h.history_from_rest(body, None, record)
    by_id = {r["kirje_id"]: r for r in history["former_shareholders"]}
    assert by_id["9001"]["isikukood_hash"] == "hash-kalev" and by_id["9001"]["history_join"] == h.JOIN_REGISTER
    assert by_id["9002"]["history_key"] == by_id["9003"]["history_key"] == "hist-9002"
    assert by_id["9002"]["history_join"] == h.JOIN_FORMER
    assert by_id["9001"]["osaluse_protsent"] == "50.000000"
    text = json.dumps(history)
    assert code_now not in text and code_gone not in text
