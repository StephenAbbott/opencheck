"""The shared client for RIK's credentialed services (ariregister_rest.py).

The contract partner behind OpenCheck's REST key and SOAP credentials may run
**one query at a time** and **50,000 answers a day**, across both services and
every OpenCheck user. These tests pin the slot, the budget, the token, the
outcome vocabulary and the personal-data check.

Fixtures in ``tests/fixtures/ariregister_rest/`` are real answers read on
17 Oct 2026 from Stephen's machine with the live key: the token is replaced,
list fields of the company answer are cut to two rows, and nothing in them is
personal (the company's own address and history).
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.config import get_settings
from opencheck.sources import ariregister_rest as rik

FIXTURES = Path(__file__).parent / "fixtures" / "ariregister_rest"
KEY = "rest-key-1234567890-secret"
BASE = rik.REST_BASE
CODE = "10421629"


def _fixture(name: str) -> Any:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture()
def configured(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.setenv("ARIREGISTER_REST_API_KEY", KEY)
    monkeypatch.setenv("ARIREGISTER_QUEUE_WAIT_S", "5")
    get_settings.cache_clear()
    rik.reset_for_tests()
    yield monkeypatch
    get_settings.cache_clear()


def _settings(monkeypatch, **env: str) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()


def _login_route(router: respx.MockRouter, token: str = "TOKEN-1") -> respx.Route:
    return router.get(f"{BASE}/keyLogin/").mock(
        return_value=httpx.Response(200, json={"access_token": token, "token_type": "bearer"})
    )


def _company_path(code: str = CODE) -> str:
    return f"/company/{code}/generalData"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


def test_no_key_is_not_configured_and_sends_nothing(monkeypatch) -> None:
    _settings(monkeypatch, OPENCHECK_ALLOW_LIVE="true", ARIREGISTER_REST_API_KEY="")
    with respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=BASE)
        with pytest.raises(rik.RikNotConfigured):
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert route.call_count == 0
    assert rik.rest_configured() is False
    get_settings.cache_clear()


def test_offline_is_not_configured(configured) -> None:
    _settings(configured, OPENCHECK_ALLOW_LIVE="false")
    with pytest.raises(rik.RikNotConfigured):
        asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert rik.rest_configured() is False


def test_an_unknown_service_is_a_programming_error(configured) -> None:
    with pytest.raises(ValueError):
        asyncio.run(rik.get_json(_company_path(), service="everything"))
    with pytest.raises(ValueError):
        rik.record_outcome("company", "fine")


# --------------------------------------------------------------------------- #
# The token and the headers
# --------------------------------------------------------------------------- #


def test_login_sends_the_key_as_a_header_and_the_token_is_reused(configured) -> None:
    with respx.mock() as router:
        login = _login_route(router)
        data = router.get(f"{BASE}{_company_path()}").mock(
            return_value=httpx.Response(200, json=_fixture("general_data_10421629"))
        )

        async def go() -> list[Any]:
            return [await rik.get_json(_company_path(), service="general_data") for _ in range(3)]

        bodies = asyncio.run(go())

    assert login.call_count == 1, "the token is cached for the process"
    req = login.calls[0].request
    assert req.headers["apiKey"] == KEY
    assert KEY not in str(req.url), "the key never goes in a URL (a query parameter is a 401)"
    assert data.call_count == 3
    for call in data.calls:
        h = call.request.headers
        assert h["Authorization"] == "Bearer TOKEN-1"
        assert h["acceptLanguage"] == "eng"  # "en" is a 422
        assert h["apiVersion"] == "V1"
        assert "apiKey" not in h, "the key is sent to /keyLogin/ only"
        assert h["User-Agent"].startswith("OpenCheck/")
    assert bodies[0]["legalEntityName"] == "Eesti Energia AS"
    s = rik.stats()
    assert s["sent_today"] == 4 and s["token_logins_today"] == 1
    assert s["outcomes"] == {"general_data|answered": 3, "login|answered": 1}


def test_an_expired_token_logs_in_again_once(configured) -> None:
    with respx.mock() as router:
        login = router.get(f"{BASE}/keyLogin/").mock(
            side_effect=[
                httpx.Response(200, json={"access_token": "OLD", "token_type": "bearer"}),
                httpx.Response(200, json={"access_token": "NEW", "token_type": "bearer"}),
            ]
        )
        data = router.get(f"{BASE}{_company_path()}").mock(
            side_effect=[
                httpx.Response(401, json=_fixture("bad_token")),
                httpx.Response(200, json=_fixture("general_data_10421629")),
            ]
        )
        body = asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert body["registryCode"] == 10421629
    assert login.call_count == 2
    assert data.calls[0].request.headers["Authorization"] == "Bearer OLD"
    assert data.calls[1].request.headers["Authorization"] == "Bearer NEW"
    assert rik.stats()["outcomes"]["general_data|unauthorized"] == 1


def test_a_second_401_is_the_key_refused_not_no_record(configured) -> None:
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(
            return_value=httpx.Response(401, json=_fixture("bad_token"))
        )
        with pytest.raises(rik.RikUnavailable) as caught:
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert caught.value.cause == "unauthorized"
    assert caught.value.reason == degradation.REASON_UPSTREAM_ERROR
    assert rik._token is None, "a refused token is not kept"


def test_an_unknown_key_is_refused_even_though_rik_answers_404(configured) -> None:
    """``GET /keyLogin/`` with a key RIK does not know answers 404
    ``{"detail": "API Key not found"}`` — never to be read as "no such company"."""
    with respx.mock(assert_all_called=False) as router:
        router.get(f"{BASE}/keyLogin/").mock(
            return_value=httpx.Response(404, json=_fixture("login_bad_key"))
        )
        data = router.get(f"{BASE}{_company_path()}")
        with pytest.raises(rik.RikUnavailable) as caught:
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert caught.value.cause == "unauthorized"
    assert data.call_count == 0
    assert rik.stats()["outcomes"] == {"login|unauthorized": 1}


# --------------------------------------------------------------------------- #
# Answers: one definitive "no record", everything else a degradation
# --------------------------------------------------------------------------- #


def test_4043_is_the_one_definitive_no_record(configured) -> None:
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path('99999990')}").mock(
            return_value=httpx.Response(404, json=_fixture("not_found"))
        )
        with pytest.raises(rik.RikNotFound):
            asyncio.run(rik.get_json(_company_path("99999990"), service="general_data"))
    assert rik.stats()["outcomes"]["general_data|not_found"] == 1


@pytest.mark.parametrize(
    ("response", "cause", "reason"),
    [
        (httpx.Response(404, json={"detail": "Not Found"}), "client_error", "upstream_error"),
        (httpx.Response(403, json=_fixture("inactive_service")), "forbidden", "upstream_error"),
        (httpx.Response(422, json=_fixture("bad_language")), "client_error", "upstream_error"),
        (httpx.Response(429, text="slow down"), "rate_limited", "rate_limited"),
        (httpx.Response(503, text="<html>maintenance</html>"), "server_error", "upstream_error"),
        (httpx.Response(200, text="An error has occurred."), "bad_response", "upstream_error"),
        (httpx.Response(200, json="a string"), "bad_response", "upstream_error"),
    ],
)
def test_failures_are_unavailable_never_not_found(configured, response, cause, reason) -> None:
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(return_value=response)
        with pytest.raises(rik.RikUnavailable) as caught:
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert not isinstance(caught.value, rik.RikNotFound)
    assert caught.value.cause == cause and caught.value.reason == reason
    assert caught.value.detail == rik.DETAILS[cause]


@pytest.mark.parametrize(
    ("exc", "cause", "reason"),
    [
        (httpx.ReadTimeout("slow"), "timeout", "timeout"),
        (httpx.ConnectError("refused"), "network_error", "upstream_error"),
    ],
)
def test_transport_failures_are_unavailable(configured, exc, cause, reason) -> None:
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(side_effect=exc)
        with pytest.raises(rik.RikUnavailable) as caught:
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    assert caught.value.cause == cause and caught.value.reason == reason
    assert rik.stats()["outcomes"][f"general_data|{cause}"] == 1


def test_a_failure_becomes_a_degraded_source_in_the_closed_vocabulary(configured) -> None:
    with degradation.recording() as found:
        rik.record_degradation(rik.RikUnavailable("budget"))
        rik.record_degradation(rik.RikUnavailable("timeout"), check=degradation.CHECK_SOURCE_READ)
    a, b = found
    assert (a.source_id, a.check, a.reason) == ("ariregister", "source_fetch", "rate_limited")
    assert (b.check, b.reason) == ("source_read", "timeout")
    assert a.detail == rik.DETAILS["budget"]


def test_the_client_never_touches_the_response_cache(configured, monkeypatch) -> None:
    """Phase 262: a failure is never cached as "no record" — and the client
    caches nothing at all; caching a successful, stripped answer is the
    caller's decision."""
    from opencheck import cache as cache_mod

    def refuse(*_a, **_k):
        raise AssertionError("ariregister_rest must not write the cache")

    monkeypatch.setattr(cache_mod.Cache, "put", refuse)
    monkeypatch.setattr(cache_mod.Cache, "put_absent", refuse)
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(
            side_effect=[
                httpx.Response(503),
                httpx.Response(404, json=_fixture("not_found")),
                httpx.Response(200, json=_fixture("general_data_10421629")),
            ]
        )

        async def go() -> None:
            for expected in (rik.RikUnavailable, rik.RikNotFound):
                with pytest.raises(expected):
                    await rik.get_json(_company_path(), service="general_data")
            await rik.get_json(_company_path(), service="general_data")

        asyncio.run(go())


# --------------------------------------------------------------------------- #
# The daily budget
# --------------------------------------------------------------------------- #


def test_a_spent_budget_refuses_before_sending(configured) -> None:
    _settings(configured, ARIREGISTER_DAILY_BUDGET="3")
    with respx.mock() as router:
        _login_route(router)
        data = router.get(f"{BASE}{_company_path()}").mock(
            return_value=httpx.Response(200, json=_fixture("general_data_10421629"))
        )

        async def go() -> rik.RikUnavailable:
            await rik.get_json(_company_path(), service="general_data")  # login + 1
            await rik.get_json(_company_path(), service="general_data")  # 3
            with pytest.raises(rik.RikUnavailable) as caught:
                await rik.get_json(_company_path(), service="general_data")
            return caught.value

        refused = asyncio.run(go())
    assert data.call_count == 2, "the refused call was never sent"
    assert refused.cause == "budget" and refused.reason == "rate_limited"
    s = rik.stats()
    assert s["sent_today"] == 3 and s["remaining_today"] == 0
    assert s["refused"] == {"general_data|budget": 1}


def test_the_budget_is_capped_at_the_contract(configured) -> None:
    _settings(configured, ARIREGISTER_DAILY_BUDGET="900000")
    assert rik.daily_budget() == rik.CONTRACT_DAILY_ANSWERS == 50_000


def test_the_budget_starts_again_on_the_registers_next_day(configured, monkeypatch) -> None:
    _settings(configured, ARIREGISTER_DAILY_BUDGET="1")
    day = {"today": date(2026, 10, 17)}
    monkeypatch.setattr(rik, "_register_day", lambda now=None: day["today"])
    rik.reset_for_tests()
    with rik.totals.lock:
        rik.totals.sent = 1
    with pytest.raises(rik.RikUnavailable):
        rik._charge("company")
    day["today"] = date(2026, 10, 18)
    rik._charge("company")
    s = rik.stats()
    assert s["register_day"] == "2026-10-18" and s["sent_today"] == 1 and s["refused"] == {}


def test_the_register_day_is_tallinns() -> None:
    from datetime import datetime, timezone

    # 22:30 UTC on 16 Oct is 01:30 on 17 Oct in Tallinn (UTC+3 in summer time).
    assert rik._register_day(datetime(2026, 10, 16, 22, 30, tzinfo=timezone.utc)) == date(2026, 10, 17)


# --------------------------------------------------------------------------- #
# One query at a time
# --------------------------------------------------------------------------- #


class _InFlight:
    """A respx side effect that counts simultaneous requests."""

    def __init__(self, body: Any, delay: float = 0.02) -> None:
        self.body, self.delay, self.now, self.peak = body, delay, 0, 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.now -= 1
        return httpx.Response(200, json=self.body)


def test_concurrent_callers_are_serialised(configured) -> None:
    counter = _InFlight(_fixture("general_data_10421629"))
    with respx.mock() as router:
        router.get(f"{BASE}/keyLogin/").mock(side_effect=_InFlight({"access_token": "T", "token_type": "bearer"}))
        router.get(url__regex=rf"{BASE}/company/\d+/generalData").mock(side_effect=counter)

        async def go() -> list[Any]:
            return await asyncio.gather(
                *(rik.get_json(f"/company/{1000 + i}/generalData", service="general_data") for i in range(6))
            )

        bodies = asyncio.run(go())
    assert len(bodies) == 6
    assert counter.peak == 1, "the contract allows one simultaneous query"
    assert rik.stats()["token_logins_today"] == 1, "the first caller logs in; the rest reuse it"


def test_soap_and_rest_share_the_one_slot(configured) -> None:
    """The REST key and the SOAP credentials are one contract partner."""
    from opencheck.sources import REGISTRY

    _settings(configured, ARIREGISTER_USERNAME="user", ARIREGISTER_PASSWORD="pass")
    shared = _InFlight(None)

    async def soap(request: httpx.Request) -> httpx.Response:
        await shared(request)
        return httpx.Response(200, text="<xml/>")

    async def rest(request: httpx.Request) -> httpx.Response:
        await shared(request)
        return httpx.Response(200, json=_fixture("general_data_10421629"))

    with respx.mock() as router:
        _login_route(router)
        router.post("https://ariregxmlv6.rik.ee/").mock(side_effect=soap)
        router.get(f"{BASE}{_company_path()}").mock(side_effect=rest)

        async def go() -> tuple[Any, ...]:
            return await asyncio.gather(
                REGISTRY["ariregister"].fetch_timeline_data(CODE),
                rik.get_json(_company_path(), service="general_data"),
                REGISTRY["ariregister"].fetch_timeline_data(CODE),
            )

        timeline, body, _ = asyncio.run(go())
    assert shared.peak == 1
    assert timeline["detail_xml"] == "<xml/>" and timeline["unavailable"] == {}
    assert body["legalEntityName"] == "Eesti Energia AS"
    outcomes = rik.stats()["outcomes"]
    assert outcomes["soap_detail|answered"] == 2 and outcomes["soap_beneficial_owners|answered"] == 2


def test_a_caller_that_cannot_get_the_slot_is_refused_as_busy(configured) -> None:
    _settings(configured, ARIREGISTER_QUEUE_WAIT_S="0.05")
    slow = _InFlight(_fixture("general_data_10421629"), delay=0.5)
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(side_effect=slow)

        async def go() -> list[Any]:
            first = asyncio.create_task(rik.get_json(_company_path(), service="general_data"))
            await asyncio.sleep(0.01)
            second = asyncio.create_task(rik.get_json(_company_path(), service="company"))
            return await asyncio.gather(first, second, return_exceptions=True)

        first, second = asyncio.run(go())
    assert first["registryCode"] == 10421629
    assert isinstance(second, rik.RikUnavailable) and second.cause == "busy"
    assert second.reason == "rate_limited"
    s = rik.stats()
    assert s["refused"] == {"company|busy": 1}
    assert s["queue"]["waiting"] == 0 and s["queue"]["peak_waiting_today"] >= 1


def test_the_slot_is_per_event_loop(configured) -> None:
    """A lock that has been waited on binds to its loop (the ny_dos rule):
    a second ``asyncio.run`` must get its own."""
    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(
            side_effect=_InFlight(_fixture("general_data_10421629"))
        )

        async def contended() -> None:
            await asyncio.gather(
                rik.get_json(_company_path(), service="general_data"),
                rik.get_json(_company_path(), service="general_data"),
            )

        asyncio.run(contended())
        asyncio.run(contended())


def test_a_soap_call_refused_the_slot_says_why_and_sends_nothing(configured) -> None:
    from opencheck.sources import REGISTRY

    _settings(
        configured, ARIREGISTER_USERNAME="user", ARIREGISTER_PASSWORD="pass", ARIREGISTER_DAILY_BUDGET="0"
    )
    with respx.mock(assert_all_called=False) as router:
        soap = router.post("https://ariregxmlv6.rik.ee/")
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(CODE))
    assert soap.call_count == 0
    assert data == {
        "registry_code": CODE,
        "detail_xml": None,
        "bo_xml": None,
        "unavailable": {"detail": "budget", "bo": "budget"},
    }


def test_a_soap_error_is_counted_by_outcome(configured) -> None:
    from opencheck.sources import REGISTRY

    _settings(configured, ARIREGISTER_USERNAME="user", ARIREGISTER_PASSWORD="pass")
    with respx.mock() as router:
        router.post("https://ariregxmlv6.rik.ee/").mock(
            side_effect=[httpx.Response(503), httpx.ReadTimeout("slow")]
        )
        data = asyncio.run(REGISTRY["ariregister"].fetch_timeline_data(CODE))
    assert data["detail_xml"] is None and data["bo_xml"] is None
    assert set(data["unavailable"].values()) == {"server_error", "timeout"}
    outcomes = rik.stats()["outcomes"]
    assert sum(outcomes.values()) == 2


def test_the_history_says_a_spent_allowance_not_a_silent_service(configured) -> None:
    from opencheck.sources import ariregister_history as hist

    _settings(configured, ARIREGISTER_USERNAME="user", ARIREGISTER_PASSWORD="pass")

    class _Adapter:
        async def fetch_timeline_data(self, code: str) -> dict[str, Any]:
            return {"registry_code": code, "detail_xml": None, "bo_xml": None,
                    "unavailable": {"detail": "budget", "bo": "budget"}}

    async def go() -> tuple[dict[str, Any], list[Any]]:
        with degradation.recording() as found:
            h = await hist.read_history(_Adapter(), CODE, {})
        return h, list(found)

    h, found = asyncio.run(go())
    assert h["read"] is False and h["reason"] == hist.NOT_READ_LIMITED
    (d,) = found
    assert d.reason == "rate_limited" and d.check == "source_read"
    assert rik.DETAILS["budget"] in d.detail


# --------------------------------------------------------------------------- #
# Provenance and secrets
# --------------------------------------------------------------------------- #


def test_a_rest_read_records_live_provenance_before_the_request(configured) -> None:
    seen: list[int] = []

    def answer(request: httpx.Request) -> httpx.Response:
        recorder = provenance.current_recorder()
        seen.append(len(recorder.observations) if recorder else -1)
        return httpx.Response(200, json=_fixture("general_data_10421629"))

    with respx.mock() as router:
        _login_route(router)
        router.get(f"{BASE}{_company_path()}").mock(side_effect=answer)

        async def go() -> Any:
            with provenance.recording() as rec:
                await rik.get_json(_company_path(), service="general_data")
            return rec

        rec = asyncio.run(go())
    assert seen and seen[0] >= 1, "recorded before the request left"
    assert rec.resolve(is_stub=False).liveness == "live"


def test_neither_key_nor_token_reaches_an_error_a_log_or_the_stats(configured, caplog) -> None:
    caplog.set_level(logging.DEBUG)
    token = "SECRET-TOKEN-abcdefghij"
    with respx.mock() as router:
        router.get(f"{BASE}/keyLogin/").mock(
            return_value=httpx.Response(200, json={"access_token": token, "token_type": "bearer"})
        )
        router.get(f"{BASE}{_company_path()}").mock(side_effect=httpx.ConnectError(f"boom {KEY} {token}"))
        with pytest.raises(rik.RikUnavailable) as caught:
            asyncio.run(rik.get_json(_company_path(), service="general_data"))
    exposed = " ".join(
        [str(caught.value), repr(caught.value), caplog.text, json.dumps(rik.stats())]
    )
    assert KEY not in exposed and token not in exposed
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


def test_the_key_setting_is_scrubbed_by_name(configured) -> None:
    from opencheck.secret_scrub import scrub

    assert KEY not in scrub(f"failed with {KEY}")


def test_stats_hold_only_closed_vocabulary(configured) -> None:
    with respx.mock() as router:
        _login_route(router)
        router.get(url__regex=rf"{BASE}/company/\d+/generalData").mock(
            side_effect=[httpx.Response(404, json=_fixture("not_found")), httpx.Response(503)]
        )

        async def go() -> None:
            for code in ("99999990", "12345678"):
                with pytest.raises(rik.RikError):
                    await rik.get_json(f"/company/{code}/generalData", service="general_data")

        asyncio.run(go())
    s = rik.stats()
    text = json.dumps(s)
    assert "99999990" not in text and "12345678" not in text
    for key in list(s["outcomes"]) + list(s["refused"]):
        service, what = key.split("|")
        assert service in rik.SERVICES and (what in rik.OUTCOMES or what in rik.REFUSALS)


def test_signalstats_serves_the_section(configured) -> None:
    from opencheck import signalstats

    section = signalstats.stats()["ariregister_rik"]
    assert section["rest_configured"] is True
    assert section["contract_daily_answers"] == 50_000
    assert section["daily_budget"] == 40_000
    assert section["sent_today"] == 0


# --------------------------------------------------------------------------- #
# Personal data
# --------------------------------------------------------------------------- #


def test_personal_data_paths_names_codes_and_contacts_never_values() -> None:
    body = {
        "registryCode": 10421629,
        "relatedPersons": [
            {"firstName": "A", "identityCode": "38001010000", "role": "JUHL"},
            {"legalPersonName": "B OÜ", "isikukood_registrikood": "12345678"},
            {"name": "C", "isikukood_registrikood": "49001010000"},
        ],
        "shareholders": [{"email": "someone@example.ee"}],
        "notes": "succeeded by a Latvian company, registry code 40003000000",
        "empty": {"identityCode": None},
    }
    paths = list(rik.personal_data_paths(body))
    assert paths == [
        "/relatedPersons/0/identityCode",
        "/relatedPersons/2/isikukood_registrikood",
        "/shareholders/0/email",
    ]
    assert not any("38001010000" in p for p in paths)


def test_the_real_company_answer_carries_no_personal_data() -> None:
    assert list(rik.personal_data_paths(_fixture("general_data_10421629"))) == []
