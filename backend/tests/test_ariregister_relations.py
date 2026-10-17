"""Every company a legal person has held a role in (the legal-person-relations
ticket): RIK's REST ``companyRelations``, on demand, legal persons only.

Fixtures are real answers of 17 Oct 2026: Eesti Energia AS's relations (25
companies, 13 deleted, 113 ended relations — the company's e-mail, phone, web
site and address kept on the first row only, to prove they are dropped) and
the first five of Nasdaq CSD SE's 8,562 (Latvia, a foreign code). Neither
answer carries a personal code.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.sources import ariregister_relations as rel
from opencheck.sources import ariregister_rest as rik

F = Path(__file__).parent / "fixtures" / "ariregister_rest"
KEY = "rest-key-1234567890-secret"
URL = f"{rik.REST_BASE}/company/companyRelations"


def _body(name: str) -> dict[str, Any]:
    return json.loads((F / name).read_text(encoding="utf-8"))


EE = "relations_10421629.json"
LVA = "relations_lva_40003242879.json"


@pytest.fixture()
def live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data-root"))
    monkeypatch.setenv("ARIREGISTER_REST_API_KEY", KEY)
    monkeypatch.setenv("ARIREGISTER_QUEUE_WAIT_S", "5")
    get_settings.cache_clear()
    rik.reset_for_tests()
    yield monkeypatch
    get_settings.cache_clear()


def _login(router: respx.MockRouter) -> None:
    router.get(f"{rik.REST_BASE}/keyLogin/").mock(
        return_value=httpx.Response(200, json={"access_token": "T", "token_type": "bearer"})
    )


# --------------------------------------------------------------------------- #
# The reduced answer
# --------------------------------------------------------------------------- #


def test_eesti_energia_matches_the_evaluation() -> None:
    out = rel.reduce_answer(_body(EE))
    companies = out["companies"]
    assert out["total"] == len(companies) == 25 and out["truncated"] is False
    assert sum(1 for c in companies if c["status_code"] == "K") == 13
    assert sum(1 for c in companies for r in c["relations"] if r["ended"]) == 113


def test_only_the_shown_fields_are_kept() -> None:
    text = json.dumps(rel.reduce_answer(_body(EE)), ensure_ascii=False)
    for dropped in ("info@energia.ee", "+372", "www.energia.ee", "Laki 24", "infoSystemUrl", "postalCode"):
        assert dropped not in text


def test_a_deleted_company_says_deleted_and_is_never_current() -> None:
    companies = {c["name"]: c for c in rel.reduce_answer(_body(EE))["companies"]}
    pogi = companies["Osaühing Pogi"]
    assert pogi["status"] == "Deleted" and pogi["current"] is False
    assert pogi["legal_form"] == "Private limited company (OÜ)"


def test_roles_read_in_the_mappers_words() -> None:
    companies = {c["name"]: c for c in rel.reduce_answer(_body(EE))["companies"]}
    roles = {r["role"] for r in companies["Strantum Infra OÜ"]["relations"]}
    assert roles == {"Shareholder (osanik)", "Founder (asutaja)"}
    first = companies["Strantum Infra OÜ"]["relations"]
    holding = next(r for r in first if r["since"] == "2012-02-07")
    assert holding["share_percent"] == "55" and holding["share_size"] == "1397"
    assert holding["share_currency"] == "Euro" and holding["until"] == "2016-08-19" and holding["ended"]


def test_a_founder_is_history_never_a_current_role() -> None:
    companies = {c["name"]: c for c in rel.reduce_answer(_body(EE))["companies"]}
    # Strantum: its holdings ended in 2016; only the founder row is open.
    strantum = companies["Strantum Infra OÜ"]
    founder = next(r for r in strantum["relations"] if r["role_code"] == "A")
    assert founder["founder"] and founder["until"] is None
    assert strantum["current"] is False


def test_companies_with_a_current_role_come_first() -> None:
    companies = rel.reduce_answer(_body(EE))["companies"]
    flags = [c["current"] for c in companies]
    assert flags == sorted(flags, reverse=True) and flags[0] is True


def test_an_unconfirmed_legal_form_code_is_left_unworded() -> None:
    body = _body(EE)
    body["list"][0]["legalForm"] = "12"
    assert rel.reduce_answer(body)["companies"][-1]["legal_form"] in (None, *rel.LEGAL_FORMS.values())
    assert all(
        c["legal_form"] is None for c in rel.reduce_answer(body)["companies"] if c["name"] == body["list"][0]["legalEntityName"]
    )


def test_a_long_list_is_cut_and_says_so() -> None:
    out = rel.reduce_answer(_body(LVA))
    assert out["total"] == 8562 and len(out["companies"]) == 5 and out["truncated"] is True


def test_a_personal_code_shaped_value_is_refused() -> None:
    body = _body(EE)
    body["list"][0]["legalEntityName"] = "Tamm 38001010011"
    with pytest.raises(ValueError):
        rel.reduce_answer(body)


@pytest.mark.parametrize("code,country,expected", [
    ("10421629", None, ("10421629", "EST")),
    ("421629", "est", ("00421629", "EST")),
    (" 40003242879 ", "LVA", ("40003242879", "LVA")),
])
def test_codes_are_normalised(code: str, country: str | None, expected: tuple[str, str]) -> None:
    assert rel.normalise(code, country) == expected


@pytest.mark.parametrize("code,country", [
    ("123456789", "EST"), ("", "EST"), ("abc", "EST"), ("1234", "LV"), ("<script>", "LVA"),
])
def test_a_bad_code_is_refused(code: str, country: str) -> None:
    with pytest.raises(rel.BadCode):
        rel.normalise(code, country)


# --------------------------------------------------------------------------- #
# The call
# --------------------------------------------------------------------------- #


def test_one_call_with_the_right_parameters_then_the_cache(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        route = router.get(URL).mock(return_value=httpx.Response(200, json=_body(EE)))
        first = asyncio.run(rel.legal_person_relations("10421629"))
        second = asyncio.run(rel.legal_person_relations("10421629", "EST"))
    assert route.call_count == 1
    params = dict(route.calls[0].request.url.params)
    assert params == {
        "legalPersonCode": "10421629", "legalPersonCodeCountry": "EST",
        "onlyValid": "false", "pageSize": str(rel.PAGE_SIZE),
    }
    assert KEY not in str(route.calls[0].request.url)
    assert first["cached"] is False and second["cached"] is True
    assert first["total"] == second["total"] == 25


def test_no_relations_is_an_empty_list(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        router.get(URL).mock(return_value=httpx.Response(204))
        out = asyncio.run(rel.legal_person_relations("99999999"))
    assert out["total"] == 0 and out["companies"] == []


def test_a_failure_is_raised_and_never_cached(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        router.get(URL).mock(return_value=httpx.Response(503))
        with pytest.raises(rik.RikUnavailable):
            asyncio.run(rel.legal_person_relations("10421629"))
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        again = router.get(URL).mock(return_value=httpx.Response(200, json=_body(EE)))
        asyncio.run(rel.legal_person_relations("10421629"))
    assert again.called


def test_it_is_counted_as_its_own_service(live) -> None:
    with respx.mock(assert_all_called=False) as router:
        _login(router)
        router.get(URL).mock(return_value=httpx.Response(200, json=_body(EE)))
        asyncio.run(rel.legal_person_relations("10421629"))
    assert rik.stats()["outcomes"].get("company_relations|answered") == 1


# --------------------------------------------------------------------------- #
# The route and the MCP tool
# --------------------------------------------------------------------------- #


def test_the_route_says_when_it_was_not_read(monkeypatch) -> None:
    monkeypatch.setenv("ARIREGISTER_REST_API_KEY", "")
    get_settings.cache_clear()
    r = TestClient(app).get("/legal-person-relations", params={"code": "10421629"})
    assert r.status_code == 200
    assert r.json()["status"] == "not_configured" and r.json()["companies"] == []


def test_the_route_lists_the_companies(monkeypatch) -> None:
    async def fake(code: str, country: str | None = "EST") -> dict[str, Any]:
        return {**rel.reduce_answer(_body(EE)), "code": code, "country": country, "retrieved_at": "2026-10-17T10:00:00+00:00"}

    monkeypatch.setattr(rel, "legal_person_relations", fake)
    r = TestClient(app).get("/legal-person-relations", params={"code": "10421629"})
    body = r.json()
    assert r.status_code == 200 and body["status"] == "ok" and body["total"] == 25
    assert "deleted companies included" in body["caveat"]


def test_the_route_says_why_rik_did_not_answer(monkeypatch) -> None:
    async def refused(code: str, country: str | None = "EST") -> dict[str, Any]:
        raise rik.RikUnavailable("budget")

    monkeypatch.setattr(rel, "legal_person_relations", refused)
    body = TestClient(app).get("/legal-person-relations", params={"code": "10421629"}).json()
    assert body["status"] == "unavailable" and body["reason"] == "rate_limited" and body["detail"]


def test_the_route_refuses_a_bad_code() -> None:
    r = TestClient(app).get("/legal-person-relations", params={"code": "123456789"})
    assert r.status_code == 400


async def test_the_mcp_tool_takes_the_legal_person_schemes(monkeypatch) -> None:
    from opencheck.mcp.server import opencheck_person_positions

    seen: list[tuple[str, str | None]] = []

    async def fake(code: str, country: str | None = "EST") -> dict[str, Any]:
        seen.append((code, country))
        return {**rel.reduce_answer(_body(LVA)), "code": code, "country": country}

    monkeypatch.setattr(rel, "legal_person_relations", fake)
    out = await opencheck_person_positions(scheme="EE-ARIREGISTER-FOREIGN", id="40003242879", country="LVA")
    assert out["status"] == "ok" and out["total"] == 8562 and out["truncated"] is True
    assert seen == [("40003242879", "LVA")]
    assert "error" in await opencheck_person_positions(scheme="EE-ARIREGISTER-FOREIGN", id="40003242879")
    out = await opencheck_person_positions(scheme="ee-ariregister", id="10421629")
    assert seen[-1] == ("10421629", "EST")
