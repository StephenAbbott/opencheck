"""An Estonian index miss answered by RIK's REST ``GET /company/{code}``.

Stephen's decisions (17 Oct 2026): every code the open-data index does not
hold goes to REST when the key is set — a deleted company (the files carry
none) or one entered after the cut; a code the index holds never does. A
deleted company says "Deleted" in the verdict.

Fixtures ``rest_company_10078240.json`` (Osaühing Pogi, merged into Enefit
Taastuvenergia in 2017) and ``rest_company_11336737.json`` (Eesti Energia
Õlitööstus AS, merged in 2017, one former name) are real answers of
17 Oct 2026 with every personal identification code replaced by a fake of
the same shape and every person's address and e-mail removed.
"""

from __future__ import annotations

import importlib.util
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
from opencheck.findings import MAX_FINDING_CHARS, finding_ariregister
from opencheck.sources import ariregister
from opencheck.sources import ariregister_rest as rik
from opencheck.sources.ariregister_rest_company import bundle_from_rest, scrub_text
from opencheck.verdict import build_verdict, status_sentence
from tests.test_ariregister_index import ABSENT, CUT, EESTI, _fetch, _page_url, index  # noqa: F401

FIXTURES = Path(__file__).parent / "fixtures" / "ariregister_rest"
POGI = "10078240"
OLI = "11336737"
KEY = "rest-key-1234567890-secret"
_FAKE_CODE = re.compile(r"(?<!\d)[1-6]\d{10}(?!\d)")


def _body(code: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"rest_company_{code}.json").read_text(encoding="utf-8"))


def _company_url(code: str) -> str:
    return f"{rik.REST_BASE}/company/{code}"


@pytest.fixture()
def rest(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.setenv("ARIREGISTER_REST_API_KEY", KEY)
    monkeypatch.setenv("ARIREGISTER_QUEUE_WAIT_S", "5")
    get_settings.cache_clear()
    rik.reset_for_tests()
    yield
    get_settings.cache_clear()


def _login(router: respx.MockRouter) -> respx.Route:
    return router.get(f"{rik.REST_BASE}/keyLogin/").mock(
        return_value=httpx.Response(200, json={"access_token": "T", "token_type": "bearer"})
    )


# --------------------------------------------------------------------------- #
# The bundle
# --------------------------------------------------------------------------- #


def test_the_fixtures_hold_no_real_personal_code() -> None:
    for code in (POGI, OLI):
        text = (FIXTURES / f"rest_company_{code}.json").read_text(encoding="utf-8")
        assert all(m.startswith("3") for m in _FAKE_CODE.findall(text) if m != code)


@pytest.mark.parametrize("code", [POGI, OLI])
def test_the_bundle_carries_no_personal_data(code: str) -> None:
    bundle = bundle_from_rest(_body(code), code)
    assert list(rik.personal_data_paths(bundle)) == []
    text = json.dumps(bundle, ensure_ascii=False)
    for m in _FAKE_CODE.findall(text):
        assert m == code or not m.startswith("3"), m
    assert "@" not in text


def test_a_code_in_the_answer_is_not_copied_into_the_bundle() -> None:
    body = _body(OLI)
    body["generalData"]["successions"][0]["identityCode"] = "38001010011"
    assert "38001010011" not in json.dumps(bundle_from_rest(body, OLI))


def test_a_bundle_carrying_a_personal_code_is_refused(monkeypatch) -> None:
    from opencheck.sources import ariregister_rest_company as rc

    monkeypatch.setattr(rc, "_pledges", lambda body, today: [{"pledgee": {"code": "38001010011"}}])
    with pytest.raises(ValueError):
        bundle_from_rest(_body(OLI), OLI)


def test_a_deleted_company_is_dated_and_named() -> None:
    bundle = bundle_from_rest(_body(OLI), OLI)
    assert bundle["tier"] == "rest" and bundle["is_stub"] is False
    assert bundle["status_code"] == "K" and bundle["status"] == "Deleted"
    assert bundle["dissolution_date"] == "2017-01-10"
    assert bundle["registration_date"] == "2007-04-19"
    assert bundle["name"] == "Eesti Energia Õlitööstus AS"
    assert bundle["former_names"] == [
        {"name": "Aktsiaselts Narva Õlitehas", "from": "2007-04-19", "until": "2009-05-28"},
    ]
    assert bundle["bo_not_read"] is True and bundle["beneficial_owners"] == []


def test_a_repeated_name_is_not_a_former_name() -> None:
    assert bundle_from_rest(_body(POGI), POGI)["former_names"] == []


def test_every_ended_shareholding_is_history_and_nothing_is_current() -> None:
    bundle = bundle_from_rest(_body(POGI), POGI)
    history = bundle["history"]
    assert history["read"] is True and history["bo_read"] is False
    assert len(history["former_shareholders"]) == 46
    assert bundle["shareholders"] == [] and bundle["officers"] == []
    people = [r for r in history["former_shareholders"] if r["isiku_tyyp"] == "F"]
    companies = [r for r in history["former_shareholders"] if r["isiku_tyyp"] == "J"]
    assert people and companies
    assert all(r["history_key"].startswith("rest-") for r in people)
    assert {r["history_join"] for r in people} <= {"register", "former", "row"}
    # A legal person is keyed on its registry code, with no join note.
    assert all("history_key" not in r and "history_join" not in r for r in companies)
    for r in history["former_shareholders"]:
        assert r["lopp_kpv"]
        assert "isikukood_registrikood" not in r or re.fullmatch(r"\d{8}", r["isikukood_registrikood"])


def test_founders_are_carried_without_an_end_date() -> None:
    founders = [r for r in bundle_from_rest(_body(POGI), POGI)["off_card_persons"] if r["isiku_roll"] in {"A", "B"}]
    assert founders and all(not r.get("lopp_kpv") for r in founders)


def test_succession_names_the_partner_not_the_company_itself() -> None:
    bundle = bundle_from_rest(_body(OLI), OLI)
    codes = {c for s in bundle["general"]["succession"] for c in s["codes"]}
    assert codes == {"10579981"}
    pogi = bundle_from_rest(_body(POGI), POGI)
    assert {c for s in pogi["general"]["succession"] for c in s["codes"]} == {"11184032"}


def test_a_deleted_company_is_not_said_to_owe_a_bo_filing() -> None:
    assert "bo_obliged" not in bundle_from_rest(_body(OLI), OLI)["general"] or \
        bundle_from_rest(_body(OLI), OLI)["general"]["bo_obliged"] is None


def test_scrub_text_agrees_with_the_builder() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "build_ariregister_index.py"
    spec = importlib.util.spec_from_file_location("_b_scrub", script)
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    for text in (
        "Pankrot 38001010011 kohta",
        "registrikood 38001010011 (Läti)",
        "reg. nr 40003000000, isik 49912310000",
        "nothing  here\n at all",
    ):
        assert scrub_text(text) == builder.scrub_text(text)


# --------------------------------------------------------------------------- #
# The adapter
# --------------------------------------------------------------------------- #


async def test_an_index_miss_is_answered_by_rest(index, rest) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        company = router.get(_company_url(ABSENT)).mock(return_value=httpx.Response(200, json=_body(OLI)))
        page = router.get(_page_url(ABSENT))
        with provenance.recording() as rec:
            bundle = await _fetch(ABSENT, "EESTI ENERGIA ÕLITÖÖSTUS AS")
    assert company.called and not page.called
    assert company.calls[0].request.url.params["onlyValid"] == "false"
    assert KEY not in str(company.calls[0].request.url)
    assert bundle["tier"] == "rest" and bundle["index_miss"] is True and bundle["cut"] == CUT
    assert bundle["retrieved_at"]
    assert rec.resolve(is_stub=False).liveness == "live"


async def test_a_code_the_index_holds_is_never_read_as_a_rest_record(index, rest) -> None:
    """The index answers the record; REST is asked only for the ownership
    history (the REST-history phase) — without rulings or pledges, which
    is how the record read is told apart."""
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        company = router.get(_company_url(EESTI)).mock(
            return_value=httpx.Response(200, json={"relatedPersons": {}}),
        )
        bundle = await _fetch(EESTI)
    assert bundle["tier"] == "index"
    assert company.call_count == 1
    params = company.calls[0].request.url.params
    assert "includeRulings" not in params and params["includeRelatedPersons"] == "true"


async def test_a_second_read_is_served_from_the_cache(index, rest) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        company = router.get(_company_url(ABSENT)).mock(return_value=httpx.Response(200, json=_body(OLI)))
        await _fetch(ABSENT)
        with provenance.recording() as rec:
            bundle = await _fetch(ABSENT)
    assert company.call_count == 1 and bundle["tier"] == "rest"
    assert rec.resolve(is_stub=False).liveness == "cached"


async def test_rest_not_found_is_a_note_card_not_a_degradation(index, rest) -> None:
    body = {"message": f"Registry code {ABSENT} did not return any matches", "code": 4043}
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        router.get(_company_url(ABSENT)).mock(return_value=httpx.Response(404, json=body))
        with degradation.recording() as degraded:
            bundle = await _fetch(ABSENT, "GHOST OÜ")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert degraded == []


async def test_a_rest_failure_is_a_note_card_and_a_degradation(index, rest) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        router.get(_company_url(ABSENT)).mock(return_value=httpx.Response(503))
        with degradation.recording() as degraded:
            bundle = await _fetch(ABSENT, "NEW OÜ")
    assert bundle["not_found"] is True
    assert [d.source_id for d in degraded] == ["ariregister"]
    # A failure is never remembered as "no record" (Phase 262).
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        again = router.get(_company_url(ABSENT)).mock(return_value=httpx.Response(200, json=_body(OLI)))
        bundle = await _fetch(ABSENT)
    assert again.called and bundle["tier"] == "rest"


async def test_a_spent_budget_is_rate_limited_and_sends_nothing(index, rest, monkeypatch) -> None:
    monkeypatch.setenv("ARIREGISTER_DAILY_BUDGET", "0")
    get_settings.cache_clear()
    with respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=rik.REST_BASE)
        with degradation.recording() as degraded:
            bundle = await _fetch(ABSENT)
    assert not route.called and bundle["not_found"] is True
    assert degraded and degraded[0].reason == degradation.REASON_RATE_LIMITED


async def test_without_a_key_an_index_miss_still_goes_to_the_page(index) -> None:
    with respx.mock(assert_all_called=False) as router:
        rest_route = router.get(url__startswith=rik.REST_BASE)
        router.get(_page_url(ABSENT)).mock(return_value=httpx.Response(429))
        bundle = await _fetch(ABSENT)
    assert not rest_route.called and bundle["not_found"] is True


# --------------------------------------------------------------------------- #
# BODS, the finding and the verdict
# --------------------------------------------------------------------------- #


def _statements(code: str) -> list[dict[str, Any]]:
    bundle = bundle_from_rest(_body(code), code)
    bundle["retrieved_at"] = "2026-10-17T10:00:00+00:00"
    with provenance.recording() as rec:
        provenance.record_live("test")
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        return list(map_ariregister(bundle))


def test_the_deleted_company_statement_says_so() -> None:
    statements = _statements(OLI)
    subject = next(
        s for s in statements
        if s["recordType"] == "entity" and s["recordDetails"].get("name") == "Eesti Energia Õlitööstus AS"
    )
    rd = subject["recordDetails"]
    assert rd["dissolutionDate"] == "2017-01-10"
    assert "Aktsiaselts Narva Õlitehas" in rd.get("alternateNames", [])
    notes = " ".join(a.get("description", "") for a in subject.get("annotations", []))
    assert "Beneficial owners were not read" in notes
    assert "Aktsiaselts Narva Õlitehas" in notes


def test_every_ownership_record_of_a_deleted_company_is_closed() -> None:
    rels = [s for s in _statements(POGI) if s["recordType"] == "relationship"]
    assert rels and all(s["recordStatus"] == "closed" for s in rels)


def test_the_finding_leads_with_the_deletion() -> None:
    finding = finding_ariregister(bundle_from_rest(_body(POGI), POGI)) or ""
    assert finding.startswith("Deleted from the register on 18 August 2017")
    assert "ended ownership records" in finding and "beneficial owners not read" in finding
    assert len(finding) <= MAX_FINDING_CHARS


def test_a_company_newer_than_the_cut_says_so() -> None:
    body = _body(OLI)
    body["generalData"]["status"] = {"status": "R", "statusAsText": "Entered into the register"}
    body["generalData"]["deletionDate"] = None
    body["generalData"]["firstEntryTime"] = "2026-10-15T09:00:00"
    for n in body["generalData"]["legalEntityNames"]:
        if n.get("endDate") == "2017-01-10":
            n["endDate"] = None
    bundle = bundle_from_rest(body, OLI)
    bundle["cut"] = CUT
    bundle["index_miss"] = True
    assert bundle["dissolution_date"] is None
    finding = finding_ariregister(bundle) or ""
    assert finding.startswith("Entered on 15 October 2026, after the open data's cut;")
    assert "beneficial owners not read" in finding


def test_the_verdict_opens_with_deleted_for_a_deleted_company() -> None:
    status = {"liveness": "terminal", "raw": "Deleted", "since": "2017-01-10", "source_id": "ariregister"}
    sentence = status_sentence(status)
    assert sentence is not None and sentence.startswith("Deleted on 10 Jan")
    assert sentence.endswith(".")
    verdict = build_verdict([], [], register_status=status) or ""
    assert verdict.startswith(sentence)


def test_any_other_terminal_status_says_dissolved() -> None:
    status = {"liveness": "terminal", "raw": "dissolved", "since": "2019-04-03", "source_id": "companies_house"}
    assert (status_sentence(status) or "").startswith("Dissolved on 3 Apr")


@pytest.mark.parametrize("status", [
    None,
    {"liveness": "live", "raw": "Entered into the register"},
    {"liveness": "winding_down", "raw": "In liquidation"},
])
def test_a_company_that_has_not_ended_gets_no_status_sentence(status) -> None:
    assert status_sentence(status) is None
    assert build_verdict([], [], register_status=status) == build_verdict([], [])
