"""OpenCorporates officers come from the company response — Phase 195.

The adapter fetched them from ``/companies/{juris}/{number}/officers``, an
endpoint that **404s** (checked live 2026-09-09 on gb/00002065 and gb/00102498
with a working key). It was fetched through ``_get_optional``, which turns a
404 into "this source has nothing", so ``bundle["officers"]`` was empty for
every company OpenCheck ever looked up — 27 cached officer payloads on disk,
every one ``null`` — while the company response beside it carried 116 officers
for Lloyds Bank PLC.

Numbers here are that company's, live: 116 officers, 100 ended, 16 serving,
each nested under an ``officer`` key.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources.opencorporates import OpenCorporatesAdapter

_API = "https://api.opencorporates.com/v0.4"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCORPORATES_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _officer(name: str, *, end_date: str | None = None, position: str = "director") -> dict:
    inner: dict = {
        "id": 206915123,
        "name": name,
        "position": position,
        "start_date": "2009-01-16",
        "opencorporates_url": "https://opencorporates.com/officers/206915123",
    }
    if end_date:
        inner["end_date"] = end_date
        inner["inactive"] = True
    return {"officer": inner}


def _mock_company(httpx_mock: HTTPXMock, officers: list[dict]) -> None:
    httpx_mock.add_response(
        url=f"{_API}/companies/gb/00002065?api_token=test-key",
        json={
            "results": {
                "company": {
                    "name": "LLOYDS BANK PLC",
                    "company_number": "00002065",
                    "jurisdiction_code": "gb",
                    "current_status": "Active",
                    "officers": officers,
                }
            }
        },
    )
    # The network call is optional and answers empty for most companies.
    httpx_mock.add_response(
        url=f"{_API}/companies/gb/00002065/network?api_token=test-key",
        json={"results": {}},
    )


async def test_the_company_response_supplies_the_officers(httpx_mock: HTTPXMock) -> None:
    _mock_company(
        httpx_mock,
        [_officer("HAROLD FRANCIS BAINES", end_date="2012-08-01"), _officer("KELLY BRIAN BENNETT")],
    )

    bundle = await OpenCorporatesAdapter().fetch("gb/00002065")

    assert len(bundle["officers"]) == 2
    assert bundle["officers"][0]["officer"]["name"] == "HAROLD FRANCIS BAINES"


async def test_the_officers_child_endpoint_is_not_asked_for(httpx_mock: HTTPXMock) -> None:
    """It 404s, and asking cost a round trip to be told nothing on every
    lookup — which is how the officer list stayed empty without a failure."""
    _mock_company(httpx_mock, [_officer("KELLY BRIAN BENNETT")])

    await OpenCorporatesAdapter().fetch("gb/00002065")

    paths = [r.url.path for r in httpx_mock.get_requests()]
    assert not any(p.endswith("/officers") for p in paths), paths


async def test_ended_officers_still_reach_the_bundle(httpx_mock: HTTPXMock) -> None:
    """The bundle is the register's answer, whole. Deciding who counts as
    serving belongs to the finding and the mapper, not to the fetch."""
    _mock_company(
        httpx_mock,
        [_officer("A", end_date="2012-08-01"), _officer("B", end_date="2015-01-01"), _officer("C")],
    )

    bundle = await OpenCorporatesAdapter().fetch("gb/00002065")

    assert len(bundle["officers"]) == 3
